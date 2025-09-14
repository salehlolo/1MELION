"""
⚠️ تنبيه مهم
- هذا السكربت تعليمي فقط. التداول بالعقود الآجلة والرافعة المالية عالي المخاطر. اختبر على حساب Demo أولًا.
- يعمل على **OKX** (Demo افتراضيًا)، ويراقب **أقل 40 زوجًا** سيولةً (Bottom-40) من USDT-SWAP بشرط ألا تكون السيولة معدومة.
- معيار الاختيار: أصغر *quoteVolume* بالدولار (أو baseVolume*last كfallback) مع حد أدنى سيولة لضمان أنها "فيها سيولة".
- الدخول **فوري** عند تنفيذ Aggressive بقيمة **≥ 1,000,000$** (قابلة للتغيير).
- حجم الدخول ثابت: **100$ مارجن × 10x = 1000$** اسمي لكل صفقة. صفقة واحدة فقط + Flip على نفس الأداة.
- الهدف/الوقف: **صافي ±2%** من سعر الدخول بعد الرسوم والانزلاق.
- تيليجرام منظم: رسالة بدء، رسالة فتح تجمع سبب الإشارة + تفاصيل التنفيذ، تقرير كل ساعة، رسالة إغلاق بنتيجة الربح/الخسارة.
 - تحسينات: قفل تزامني لمنع السباقات، اشتراكات WS على دفعات، وضبط رافعة تكيفية.

المتطلبات
---------
pip install ccxt websockets requests python-dotenv

ملف .env (مثال)
---------------
OKX_API_KEY=...
OKX_API_SECRET=...
OKX_API_PASSPHRASE=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=1266351161
USE_DEMO=true

# شرط الإشارة
BIG_TRADE_USD=1000000

# إدارة حجم الصفقة
MARGIN_PER_TRADE_USD=100
LEVERAGE=10

# TP/SL (net after fees & slippage)
TAKE_PROFIT_NET_BPS=200
STOP_LOSS_NET_BPS=200
TAKER_FEE_BPS_PER_SIDE=5
SLIPPAGE_BPS_ENTRY=2
SLIPPAGE_BPS_EXIT=2

# سلوك الدخول
COOLDOWN_BETWEEN_TRADES_SEC=0
FLIP_ALLOWED=true

# اختيار الأزواج (Bottom-40 مع حد أدنى للسيولة)
TOP_N=40
MIN_QUOTE_VOL_USD=1000000   # الحد الأدنى للسيولة بالدولار (يمكن خفضه/رفعه)
WS_SUB_CHUNK=20
ENABLE_MULTI_EXCHANGE=true
SIGNAL_EXCHANGES=okx,binance,bybit
EXTERNAL_SOURCES=spot,swap
MX_DEBOUNCE_MS=500
"""

import asyncio
import json
import math
import os
import time
import logging
import random
from collections import deque
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

import ccxt
import websockets
import requests

# ==========================
# تحميل .env
# ==========================
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ==========================
# إعدادات / من .env
# ==========================
OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")

USE_DEMO = os.getenv("USE_DEMO", "true").lower() in ("1", "true", "yes")

# ≥ 1M$ (قابلة للتعديل من .env)
BIG_TRADE_USD = float(os.getenv("BIG_TRADE_USD", "1000000"))

# دخول ثابت: 100$ مارجن × 10 = 1000$ قيمة اسمية
MARGIN_PER_TRADE_USD = float(os.getenv("MARGIN_PER_TRADE_USD", "100"))
LEVERAGE = int(os.getenv("LEVERAGE", "10"))

# الهدف/الوقف (صافي بعد الرسوم والانزلاق)
TAKE_PROFIT_NET_BPS = int(os.getenv("TAKE_PROFIT_NET_BPS", "200"))   # 2.00% net
STOP_LOSS_NET_BPS = int(os.getenv("STOP_LOSS_NET_BPS", "200"))       # 2.00% net
TAKER_FEE_BPS_PER_SIDE = float(os.getenv("TAKER_FEE_BPS_PER_SIDE", "5"))
SLIPPAGE_BPS_ENTRY = float(os.getenv("SLIPPAGE_BPS_ENTRY", "2"))
SLIPPAGE_BPS_EXIT = float(os.getenv("SLIPPAGE_BPS_EXIT", "2"))

# مصادر الإشارات وخرائط السبوت→السواب
SIGNAL_SOURCES = [s.strip().lower() for s in os.getenv("SIGNAL_SOURCES", "swap").split(",") if s.strip()]
MAP_SPOT_TO_SWAP = os.getenv("MAP_SPOT_TO_SWAP", "true").lower() in ("1", "true", "yes")

# التحويلات على السلاسل (Transfers)
ENABLE_TRANSFERS = os.getenv("ENABLE_TRANSFERS", "false").lower() in ("1", "true", "yes")
TRANSFERS_MIN_USD = float(os.getenv("TRANSFERS_MIN_USD", "1000000"))
TRANSFERS_POLL_SEC = float(os.getenv("TRANSFERS_POLL_SEC", "1"))
TRANSFERS_EXCHANGES = [s.strip().lower() for s in os.getenv("TRANSFERS_EXCHANGES", "").split(",") if s.strip()]
TRANSFERS_PROVIDER = os.getenv("TRANSFERS_PROVIDER", "whalealert").lower()
WHALE_ALERT_API_KEY = os.getenv("WHALE_ALERT_API_KEY", "")
TRANSFERS_EXCHANGES_SET = set(TRANSFERS_EXCHANGES)

# إشارات متعددة المنصات
ENABLE_MULTI_EXCHANGE = os.getenv("ENABLE_MULTI_EXCHANGE", "false").lower() in ("1", "true", "yes")
SIGNAL_EXCHANGES = [s.strip().lower() for s in os.getenv("SIGNAL_EXCHANGES", "okx").split(",") if s.strip()]
EXTERNAL_SOURCES = [s.strip().lower() for s in os.getenv("EXTERNAL_SOURCES", "spot,swap").split(",") if s.strip()]
MX_DEBOUNCE_MS = int(os.getenv("MX_DEBOUNCE_MS", "500"))

# downsizing
ALLOW_AUTO_DOWNSIZE = os.getenv("ALLOW_AUTO_DOWNSIZE", "true").lower() in ("1", "true", "yes")
MARGIN_SAFETY_FACTOR = float(os.getenv("MARGIN_SAFETY_FACTOR", "1.05"))

# فوري: لا تأخير بين الصفقات (لكن نمنع فتح أكثر من صفقة في آن واحد)
COOLDOWN_BETWEEN_TRADES_SEC = int(os.getenv("COOLDOWN_BETWEEN_TRADES_SEC", "0"))

# تردد فحص السعر لإدارة الوقف/الهدف
TICKER_POLL_SEC = float(os.getenv("TICKER_POLL_SEC", "1.0"))

# تقارير تيليجرام
HOURLY_REPORT_SEC = int(os.getenv("HOURLY_REPORT_SEC", "3600"))

# Flip عند إشارة معاكسة قوية *على نفس الأداة فقط*
FLIP_ALLOWED = os.getenv("FLIP_ALLOWED", "true").lower() in ("1", "true", "yes")

# اختيار أقل أزواج سيولةً
TOP_N = int(os.getenv("TOP_N", "40"))
MIN_QUOTE_VOL_USD = float(os.getenv("MIN_QUOTE_VOL_USD", "1000000"))  # حد أدنى: 1M$ افتراضيًا
WS_SUB_CHUNK = int(os.getenv("WS_SUB_CHUNK", "20"))

# تيليجرام
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# ==========================
# لوج
# ==========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("okx-bottomN-liq-bot-v4")

# ==========================
# كيانات وحالة
# ==========================
@dataclass
class Position:
    inst_id: str
    symbol: str
    side: str            # "long" أو "short"
    entry_price: float
    contracts: float
    notional: float
    tp_price: float
    sl_price: float


@dataclass
class TransferEvent:
    id: str
    base: str
    usd: float
    ts: int
    src: str
    dst: str
    chain: str


@dataclass
class TradeEvent:
    exchange: str
    market_type: str  # "spot" أو "swap"
    base: str
    side: str  # "buy" أو "sell"
    px: float
    sz: float
    notional_usd: float
    ts: int

class State:
    def __init__(self):
        self.position: Optional[Position] = None
        self.last_trade_ts: float = 0.0
        self.daily_pnl_usd: float = 0.0
        self.cumulative_pnl_usd: float = 0.0
        self.daily_reset_day: int = time.gmtime().tm_yday
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0
        # معلومات السوق لكل أداة: contractSize, amount_min, amount_precision
        self.mk: Dict[str, dict] = {}
        self.watch_insts: List[str] = []      # swap instIds
        self.watch_spot_insts: List[str] = [] # spot instIds
        self.spot_map: Dict[str, str] = {}    # base -> spot instId
        self.swap_map: Dict[str, str] = {}    # base -> swap instId
        self.last_signal_ts_by_inst: Dict[str, int] = {}
        self.effective_leverage_by_symbol: Dict[str, int] = {}
        self.mx_buffers: Dict[Tuple[str, str], dict] = {}

state = State()

# قفل يمنع السباقات
trade_lock = asyncio.Lock()

# ==========================
# تيليجرام
# ==========================
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage" if TELEGRAM_BOT_TOKEN else None

async def tg_send(text: str, disable_web_page_preview: bool = True):
    if not TELEGRAM_API or not TELEGRAM_CHAT_ID:
        return
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": disable_web_page_preview,
    }
    try:
        await asyncio.to_thread(requests.post, TELEGRAM_API, data=payload, timeout=10)
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")

# ==========================
# OKX عبر CCXT
# ==========================
exchange = ccxt.okx({
    "apiKey": OKX_API_KEY,
    "secret": OKX_API_SECRET,
    "password": OKX_API_PASSPHRASE,
    "enableRateLimit": True,
    "options": {"defaultType": "swap"},
})
exchange.set_sandbox_mode(USE_DEMO)

def inst_to_symbol(inst_id: str) -> str:
    return f"{inst_id.split('-')[0]}/USDT:USDT"

def _quote_volume_usd(t: dict) -> float:
    """حساب حجم التداول المقوّم بالدولار من التيكر (quoteVolume أو baseVolume*last)."""
    if not t:
        return 0.0
    vq = t.get("quoteVolume")
    if vq is not None:
        try:
            return float(vq) or 0.0
        except Exception:
            pass
    # fallback
    try:
        base = float(t.get("baseVolume") or 0.0)
        last = float(t.get("last") or 0.0)
        return base * last
    except Exception:
        return 0.0


class WhaleAlertPoller:
    BASE_URL = "https://api.whale-alert.io/v1/transactions"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.cursor: Optional[str] = None
        self.last_call: float = 0.0
        self.call_times: deque[float] = deque()
        self.rate429: int = 15

    async def _throttle(self):
        min_interval = 6.0
        now = time.time()
        # ensure at least 6s between calls
        elapsed = now - self.last_call
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
            now = time.time()
        # ensure no more than 10 calls per minute
        while self.call_times and now - self.call_times[0] > 60:
            self.call_times.popleft()
        if len(self.call_times) >= 10:
            wait = 60 - (now - self.call_times[0])
            await asyncio.sleep(max(wait, 0))
            now = time.time()
            while self.call_times and now - self.call_times[0] > 60:
                self.call_times.popleft()
        self.last_call = now
        self.call_times.append(now)

    async def fetch_recent_transfers(self, since_ts: Optional[int] = None) -> List[TransferEvent]:
        if not self.api_key:
            logger.warning("WhaleAlert API key missing; returning no transfers")
            return []

        await self._throttle()

        params = {
            "api_key": self.api_key,
            "start": int(time.time()) - 90,
            "min_value": int(TRANSFERS_MIN_USD),
            "limit": 100,
        }
        if self.cursor:
            params["cursor"] = self.cursor
        try:
            resp = await asyncio.to_thread(requests.get, self.BASE_URL, params=params, timeout=10)
        except Exception as e:
            delay = random.randint(10, 30)
            logger.warning(f"WhaleAlert fetch failed: {e}; retrying in {delay}s")
            await asyncio.sleep(delay)
            return []

        status = resp.status_code
        if status == 200:
            try:
                data = resp.json()
            except Exception:
                logger.warning("WhaleAlert invalid JSON")
                return []
            self.cursor = data.get("cursor") or self.cursor
            events: List[TransferEvent] = []
            for tx in data.get("transactions", []):
                try:
                    events.append(
                        TransferEvent(
                            id=str(tx.get("id") or tx.get("hash")),
                            base=(tx.get("symbol") or "").upper(),
                            usd=float(tx.get("amount_usd") or 0.0),
                            ts=int(tx.get("timestamp") or 0),
                            src=(tx.get("from", {}) or {}).get("owner", "").lower(),
                            dst=(tx.get("to", {}) or {}).get("owner", "").lower(),
                            chain=(tx.get("blockchain") or "").lower(),
                        )
                    )
                except Exception:
                    continue
            return events
        elif status in (401, 403):
            logger.warning("WhaleAlert auth error")
            await asyncio.sleep(60)
            return []
        elif status == 429:
            retry_after = int(resp.headers.get("Retry-After", 0) or 0)
            wait = max(retry_after, self.rate429)
            logger.warning(f"WhaleAlert rate limited; retrying in {wait}s")
            await asyncio.sleep(wait)
            self.rate429 = min(self.rate429 * 2, 60)
            return []
        elif 500 <= status < 600:
            delay = random.randint(10, 30)
            logger.warning(f"WhaleAlert server error {status}; retrying in {delay}s")
            await asyncio.sleep(delay)
            return []
        else:
            logger.warning(f"WhaleAlert unexpected status {status}")
            return []

async def build_universe_bottomN() -> Tuple[List[Tuple[str, str, float]], List[Tuple[str, str, float]]]:
    """
    يبني قائمة Bottom-N تلقائيًا من أسواق USDT-SWAP مع إمكانية ربط SPOT المقابل.
    يعيد قائمتين: [ (swap_instId, swap_symbol, vol), ... ] و [ (spot_instId, spot_symbol, vol), ... ].
    """
    await asyncio.to_thread(exchange.load_markets)

    bases: Dict[str, dict] = {}
    for m in exchange.markets.values():
        try:
            if m.get("type") == "swap" and m.get("quote") == "USDT" and m.get("linear"):
                base = m.get("base")
                inst_id = m.get("id")
                sym = m.get("symbol")
                if base and inst_id and sym:
                    info = bases.setdefault(base, {})
                    info["swap_inst"] = inst_id
                    info["swap_symbol"] = sym
                    spot_symbol = f"{base}/USDT"
                    spot_m = exchange.markets.get(spot_symbol)
                    if spot_m:
                        info["spot_inst"] = spot_m.get("id")
                        info["spot_symbol"] = spot_symbol
        except Exception:
            continue

    if not bases:
        return [], []

    swap_symbols = [info["swap_symbol"] for info in bases.values()]
    spot_symbols = [info["spot_symbol"] for info in bases.values() if "spot_symbol" in info]
    try:
        tickers = await asyncio.to_thread(exchange.fetch_tickers, swap_symbols + spot_symbols)
    except Exception as e:
        logger.warning(f"fetch_tickers failed ({e}); using zeros for volumes")
        tickers = {}

    scored: List[Tuple[float, str, dict]] = []
    for base, info in bases.items():
        vol_swap = _quote_volume_usd(tickers.get(info["swap_symbol"], {}))
        vol_spot = _quote_volume_usd(tickers.get(info.get("spot_symbol", ""), {}))
        total = vol_swap + vol_spot
        scored.append((total, base, info))
    scored.sort(key=lambda x: x[0])

    filtered = [(base, info, vol) for (vol, base, info) in scored if vol >= MIN_QUOTE_VOL_USD]
    if len(filtered) >= TOP_N:
        chosen = filtered[:TOP_N]
    else:
        remainder = [(base, info, vol) for (vol, base, info) in scored if (base, info, vol) not in filtered and vol > 0]
        needed = TOP_N - len(filtered)
        chosen = filtered + remainder[:max(0, needed)]
    if len(chosen) < TOP_N:
        chosen = [(base, info, vol) for (vol, base, info) in scored[:TOP_N]]

    selected_swap: List[Tuple[str, str, float]] = []
    selected_spot: List[Tuple[str, str, float]] = []
    state.spot_map.clear()
    state.swap_map.clear()
    for base, info, vol in chosen:
        selected_swap.append((info["swap_inst"], info["swap_symbol"], vol))
        state.swap_map[base] = info["swap_inst"]
        if info.get("spot_inst") and info.get("spot_symbol"):
            selected_spot.append((info["spot_inst"], info["spot_symbol"], vol))
            state.spot_map[base] = info["spot_inst"]

    return selected_swap, selected_spot

async def ensure_markets(selected: List[Tuple[str, str, float]]):
    """
    يحفظ ميتاداتا الأسواق المختارة في state.mk
    """
    for inst, symbol, vol in selected:
        if symbol not in exchange.markets:
            logger.warning(f"Symbol not in markets (env mismatch?): {symbol}")
            continue
        m = exchange.market(symbol)
        cs = float(m.get("contractSize") or m.get("info", {}).get("ctVal", 0) or 0)
        amount_min = m.get("limits", {}).get("amount", {}).get("min", 1)
        amt_prec = m.get("precision", {}).get("amount")
        state.mk[inst] = {
            "symbol": symbol,
            "contractSize": cs if cs > 0 else 1.0,
            "amount_min": amount_min,
            "amount_precision": amt_prec,
        }
        logger.info(f"{inst} -> symbol={symbol}, contractSize={state.mk[inst]['contractSize']}, quoteVol≈${vol:,.0f}")

async def set_leverage_adaptive(symbol: str, target: int) -> int:
    if not hasattr(exchange, "set_leverage"):
        state.effective_leverage_by_symbol[symbol] = 1
        return 1
    candidates = [target]
    for x in (7, 5, 3, 2, 1):
        if x not in candidates:
            candidates.append(x)
    delay = 0.4
    for lev in candidates:
        try:
            await asyncio.to_thread(exchange.set_leverage, lev, symbol, {"mgnMode": "cross"})
            logger.info(f"Leverage set to {lev}x for {symbol}")
            state.effective_leverage_by_symbol[symbol] = lev
            return lev
        except Exception as e:
            s = str(e)
            if "Too Many Requests" in s or "50011" in s:
                logger.warning(f"Rate limited on set_leverage for {symbol}. retry in {delay:.1f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 4.0)
                continue
            logger.warning(f"Leverage {lev}x not allowed on {symbol}; trying lower...")
            await asyncio.sleep(0.05)
            continue
    logger.warning(f"All leverage attempts failed for {symbol}; defaulting to 1x.")
    state.effective_leverage_by_symbol[symbol] = 1
    return 1

async def fetch_price(symbol: str) -> float:
    ticker = await asyncio.to_thread(exchange.fetch_ticker, symbol)
    return float(ticker.get("last") or ticker.get("info", {}).get("last", 0) or 0)

async def fetch_equity_usdt() -> float:
    bal = await asyncio.to_thread(exchange.fetch_balance)
    usdt = bal.get("USDT") or {}
    return float(usdt.get("total") or usdt.get("free") or 0.0)

def get_effective_leverage(symbol: str) -> int:
    lev = state.effective_leverage_by_symbol.get(symbol)
    return max(1, int(lev or 1))

def notional_to_contracts(inst_id: str, notional_usd: float, price: float) -> float:
    mk = state.mk.get(inst_id)
    if not mk or price <= 0:
        return 0.0
    cs = float(mk["contractSize"]) or 1.0
    return max(notional_usd / (price * cs), 0.0)


def compute_tp_sl_prices(entry_price: float, side: str) -> Tuple[float, float]:
    d_tp = TAKE_PROFIT_NET_BPS / 10_000
    d_sl = STOP_LOSS_NET_BPS / 10_000
    fee_in = TAKER_FEE_BPS_PER_SIDE / 10_000
    fee_out = TAKER_FEE_BPS_PER_SIDE / 10_000
    slip_in = SLIPPAGE_BPS_ENTRY / 10_000
    slip_out = SLIPPAGE_BPS_EXIT / 10_000
    c_in = fee_in + slip_in
    c_out = fee_out + slip_out

    if side == "long":
        a = (d_tp + c_in + c_out) / (1 - c_out)
        b = 1 - (1 + c_in - d_sl) / (1 - c_out)
        tp = entry_price * (1 + a)
        sl = entry_price * (1 - b)
    else:
        g = (d_tp + c_in + c_out) / (1 + c_out)
        u = (d_sl + c_in + c_out) / (1 + c_out)
        tp = entry_price * (1 - g)
        sl = entry_price * (1 + u)
    return tp, sl

def round_contracts(inst_id: str, contracts: float) -> float:
    mk = state.mk.get(inst_id, {})
    amt_prec = mk.get("amount_precision")
    if amt_prec is not None:
        factor = 10 ** amt_prec
        contracts = math.floor(contracts * factor) / factor
    amount_min = mk.get("amount_min", 1)
    return contracts if contracts >= amount_min else 0.0


async def handle_trade_event(ev: TradeEvent):
    key = (ev.base, ev.side)
    buf = state.mx_buffers.get(key)
    if not buf:
        buf = {"notional": 0.0, "sources": set(), "px": ev.px, "sz": ev.sz, "ts": ev.ts}
        state.mx_buffers[key] = buf
        asyncio.create_task(flush_mx_buffer(ev.base, ev.side))
    buf["notional"] += ev.notional_usd
    buf["sources"].add(f"{ev.exchange.upper()}-{ev.market_type.upper()}")
    buf["px"] = ev.px
    buf["sz"] = ev.sz
    buf["ts"] = ev.ts


async def flush_mx_buffer(base: str, side: str):
    await asyncio.sleep(MX_DEBOUNCE_MS / 1000.0)
    buf = state.mx_buffers.pop((base, side), None)
    if not buf:
        return
    total = buf.get("notional", 0.0)
    if total < BIG_TRADE_USD:
        return
    target_inst = state.swap_map.get(base)
    if not target_inst:
        return
    sources = list(buf.get("sources", []))
    src_label = (
        f"MULTI (Σ عبر {MX_DEBOUNCE_MS}ms)" if len(sources) > 1 else (sources[0] if sources else "UNKNOWN")
    )
    trigger = {
        "side": side,
        "px": buf.get("px"),
        "sz": buf.get("sz"),
        "notional": total,
        "ts": buf.get("ts"),
        "source": src_label,
    }
    signal_side = "long" if side == "buy" else "short"
    if state.position is None:
        eff_lev = get_effective_leverage(state.mk[target_inst]["symbol"])
        notional_to_use = MARGIN_PER_TRADE_USD * eff_lev
        await open_position(target_inst, signal_side, notional_to_use, trigger)
    else:
        if FLIP_ALLOWED and state.position.inst_id == target_inst and signal_side != state.position.side:
            await close_position("flip")
            eff_lev = get_effective_leverage(state.mk[target_inst]["symbol"])
            notional_to_use = MARGIN_PER_TRADE_USD * eff_lev
            await open_position(target_inst, signal_side, notional_to_use, trigger)

# ==========================
# أوامر ومراكز (محمية بالقفل)
# ==========================
async def open_position(inst_id: str, side: str, notional_usd: float, trigger: dict | None = None):
    async with trade_lock:
        assert side in ("long", "short")
        if COOLDOWN_BETWEEN_TRADES_SEC > 0 and time.time() - state.last_trade_ts < COOLDOWN_BETWEEN_TRADES_SEC:
            logger.info("Ignored signal due to cooldown.")
            return

        if state.position is not None:
            logger.info("Position already open; ignoring new signal.")
            return

        mk = state.mk.get(inst_id)
        if not mk:
            logger.warning(f"Market meta missing for {inst_id}")
            return

        symbol = mk["symbol"]
        eff_lev = get_effective_leverage(symbol)
        price = await fetch_price(symbol)
        if price <= 0:
            return

        initial_margin_needed = notional_usd / eff_lev
        equity = await fetch_equity_usdt()
        if equity < initial_margin_needed * MARGIN_SAFETY_FACTOR:
            warn = f"⚠️ *Skipped signal* — Equity too low. Need ~${initial_margin_needed:,.0f}, have ~${equity:,.0f}."
            logger.warning(warn)
            await tg_send(warn)
            return

        contracts = round_contracts(inst_id, notional_to_contracts(inst_id, notional_usd, price))
        if contracts <= 0:
            warn = "⚠️ *Skipped signal* — Min notional exceeds budget."
            logger.warning("Calculated contracts <= 0; aborting open_position")
            await tg_send(warn)
            return

        ccxt_side = "buy" if side == "long" else "sell"
        params = {"tdMode": "cross"}

        logger.info(
            f"Opening {side.upper()} {inst_id} | notional ≈ ${notional_usd:,.0f} | contracts ≈ {contracts} @ ~{price}"
        )
        try:
            await asyncio.to_thread(exchange.create_order, symbol, "market", ccxt_side, contracts, None, params)
        except Exception as e:
            if "51008" in str(e) and ALLOW_AUTO_DOWNSIZE:
                equity = await fetch_equity_usdt()
                max_notional = equity / MARGIN_SAFETY_FACTOR * eff_lev
                new_contracts = round_contracts(inst_id, notional_to_contracts(inst_id, max_notional, price))
                if new_contracts > 0:
                    await asyncio.to_thread(exchange.create_order, symbol, "market", ccxt_side, new_contracts, None, params)
                    contracts = new_contracts
                    notional_usd = price * new_contracts * float(mk.get("contractSize", 1.0))
                    initial_margin_needed = notional_usd / eff_lev
                else:
                    warn = "⚠️ *Skipped signal* — Equity too low even after downsizing."
                    logger.warning(warn)
                    await tg_send(warn)
                    return
            else:
                raise

        tp_price, sl_price = compute_tp_sl_prices(price, side)
        state.position = Position(
            inst_id=inst_id,
            symbol=symbol,
            side=side,
            entry_price=price,
            contracts=contracts,
            notional=notional_usd,
            tp_price=tp_price,
            sl_price=sl_price,
        )
        state.last_trade_ts = time.time()
        state.total_trades += 1

    # رسالة بعد تحرير القفل
    trigger_text = ""
    if trigger:
        t_ts = trigger.get('ts')
        src = trigger.get('source', 'SWAP')
        reason_ar = (
            f"السبب: دخول مستثمر/متداول بشراء ≥ {BIG_TRADE_USD:,.0f}$."
            if trigger.get('side') == 'buy'
            else f"السبب: خروج/بيع ≥ {BIG_TRADE_USD:,.0f}$ من السوق."
        )
        if src.upper() == 'TRANSFER':
            direction = trigger.get('dir', '')
            usd_val = trigger.get('usd', 0)
            exch = trigger.get('exchange', '')
            base = trigger.get('base', '')
            chain = trigger.get('chain', '')
            t_fmt = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(t_ts)) if t_ts else ''
            trigger_text = (
                "📣 *Signal*\n"
                f"السبب: (WhaleAlert) {direction} {exch}\n"
                f"{reason_ar}\n"
                f"• Transfer: ~${usd_val:,.0f} {base} on {chain}\n"
                f"• Time: {t_fmt} UTC"
            )
        else:
            t_side = 'BUY' if trigger.get('side') == 'buy' else 'SELL'
            t_notional = trigger.get('notional', 0)
            t_px = trigger.get('px')
            t_sz = trigger.get('sz')
            trigger_text = (
                "📣 *Signal*\n"
                f"مصدر الإشارة: {src}\n"
                f"{reason_ar}\n"
                f"• Cause: Aggressive {t_side} ≥ ${BIG_TRADE_USD:,}\n"
                f"• Observed: {t_side} sz={t_sz} @ {t_px} → notional ≈ ${t_notional:,.0f}\n"
                f"• Time: {t_ts}"
            )

    exec_text = (
        f"🟢 *Opened {side.upper()}* `{inst_id}`\n"
        f"• Entry: {state.position.entry_price if state.position else price}\n"
        f"• Notional used: ~${notional_usd:,.0f} (margin ${MARGIN_PER_TRADE_USD} x{eff_lev})\n"
        f"• Contracts: {state.position.contracts if state.position else contracts}\n"
        f"• Protections: TP +{TAKE_PROFIT_NET_BPS/100:.2f}% (net), SL -{STOP_LOSS_NET_BPS/100:.2f}% (net)\n"
        f"• Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC"
    )
    await tg_send((trigger_text + "\n\n" + exec_text) if trigger_text else exec_text)

async def close_position(reason: str):
    async with trade_lock:
        if not state.position:
            return

        pos = state.position
        side_to_close = "sell" if pos.side == "long" else "buy"
        price_now = await fetch_price(pos.symbol)
        pnl = (price_now - pos.entry_price) / pos.entry_price * pos.notional if pos.side == "long" else (pos.entry_price - price_now) / pos.entry_price * pos.notional

        params = {"tdMode": "cross", "reduceOnly": True}
        logger.info(f"Closing position ({reason}) | est PnL ≈ ${pnl:,.2f}")
        try:
            await asyncio.to_thread(exchange.create_order, pos.symbol, "market", side_to_close, pos.contracts, None, params)
        except Exception:
            await asyncio.to_thread(exchange.create_order, pos.symbol, "market", side_to_close, pos.contracts)

        today = time.gmtime().tm_yday
        if today != state.daily_reset_day:
            state.daily_reset_day = today
            state.daily_pnl_usd = 0.0

        state.daily_pnl_usd += pnl
        state.cumulative_pnl_usd += pnl

        win = pnl >= 0
        if win:
            state.winning_trades += 1
        else:
            state.losing_trades += 1

        state.position = None
        state.last_trade_ts = time.time()

    await tg_send(
        f"🔴 *Closed* ({reason})\n"
        f"• Realized PnL: ${pnl:,.2f} {'✅' if win else '❌'}\n"
        f"• Daily PnL: ${state.daily_pnl_usd:,.2f}\n"
        f"• Total PnL: ${state.cumulative_pnl_usd:,.2f}"
    )

# ==========================
# إدارة المخاطر
# ==========================
async def manage_risk_loop():
    while True:
        try:
            today = time.gmtime().tm_yday
            if today != state.daily_reset_day:
                state.daily_reset_day = today
                state.daily_pnl_usd = 0.0
                logger.info("Daily PnL reset.")

            if state.position:
                pos = state.position
                price_now = await fetch_price(pos.symbol)
                if pos.side == "long":
                    if price_now <= pos.sl_price:
                        await close_position("stop loss")
                    elif price_now >= pos.tp_price:
                        await close_position("take profit")
                else:
                    if price_now >= pos.sl_price:
                        await close_position("stop loss")
                    elif price_now <= pos.tp_price:
                        await close_position("take profit")

            await asyncio.sleep(TICKER_POLL_SEC)
        except Exception as e:
            logger.error(f"Risk loop error: {e}")
            await asyncio.sleep(0.5)

# ==========================
# تقرير تيليجرام كل ساعة
# ==========================
async def hourly_report_loop():
    while True:
        try:
            if state.position:
                pos = state.position
                price = await fetch_price(pos.symbol)
                open_pnl = (price - pos.entry_price) / pos.entry_price * pos.notional if pos.side == "long" else (pos.entry_price - price) / pos.entry_price * pos.notional
                pos_text = f"{pos.side.upper()} `{pos.inst_id}` @ {pos.entry_price} | Open PnL: ${open_pnl:,.2f}"
            else:
                pos_text = "No open position"

            wins = state.winning_trades
            losses = state.losing_trades
            total = max(1, wins + losses)
            win_rate = wins / total * 100

            try:
                equity = await fetch_equity_usdt()
            except Exception as e:
                equity = 0.0
                logger.warning(f"Equity fetch failed: {e}")

            await tg_send(
                "⏱️ *Hourly Report*\n"
                f"• {pos_text}\n"
                f"• Daily Realized: ${state.daily_pnl_usd:,.2f}\n"
                f"• Total Realized: ${state.cumulative_pnl_usd:,.2f}\n"
                f"• Trades: {wins+losses} (Win% {win_rate:.1f}%)\n"
                f"• Equity (USDT): ${equity:,.2f}"
            )
        except Exception as e:
            logger.error(f"Hourly report error: {e}")
        finally:
            await asyncio.sleep(HOURLY_REPORT_SEC)


async def transfers_listener():
    poller = WhaleAlertPoller(WHALE_ALERT_API_KEY)
    seen: Dict[str, float] = {}
    poll_interval = max(6.0, TRANSFERS_POLL_SEC)
    while True:
        try:
            events = await poller.fetch_recent_transfers()
            now = time.time()
            for k, v in list(seen.items()):
                if now - v > 300:
                    del seen[k]
            for ev in events:
                if ev.id in seen:
                    continue
                seen[ev.id] = now
                usd = ev.usd
                if usd < TRANSFERS_MIN_USD:
                    continue
                direction = None
                exchange_name = None
                if ev.dst in TRANSFERS_EXCHANGES_SET:
                    direction = "in"
                    exchange_name = ev.dst
                elif ev.src in TRANSFERS_EXCHANGES_SET:
                    direction = "out"
                    exchange_name = ev.src
                if not direction:
                    continue
                base = ev.base.upper()
                inst_id = state.swap_map.get(base)
                if not inst_id:
                    continue
                ts_int = int(ev.ts)
                last_ts = state.last_signal_ts_by_inst.get(inst_id, 0)
                if ts_int and ts_int == last_ts:
                    continue
                if ts_int:
                    state.last_signal_ts_by_inst[inst_id] = ts_int
                side = "long" if direction == "in" else "short"
                eff_lev = get_effective_leverage(state.mk[inst_id]["symbol"])
                notional_to_use = MARGIN_PER_TRADE_USD * eff_lev
                trigger = {
                    "source": "TRANSFER",
                    "dir": direction,
                    "usd": usd,
                    "ts": ev.ts,
                    "exchange": exchange_name,
                    "base": base,
                    "chain": ev.chain,
                    "side": "buy" if side == "long" else "sell",
                }
                if state.position is None:
                    await open_position(inst_id, side, notional_to_use, trigger)
                else:
                    if FLIP_ALLOWED and state.position.inst_id == inst_id and side != state.position.side:
                        await close_position("flip")
                        await open_position(inst_id, side, notional_to_use, trigger)
        except Exception as e:
            logger.error(f"Transfers poller error: {e}")
        await asyncio.sleep(poll_interval)


# ==========================
# إشارات متعددة المنصات
# ==========================

def _ext_symbols_for_exchange(exchange_name: str) -> List[str]:
    bases = list(state.swap_map.keys())
    if exchange_name == "binance":
        return [f"{b.lower()}usdt" for b in bases]
    if exchange_name == "bybit":
        return [f"{b.upper()}USDT" for b in bases]
    return []


async def binance_ws(market_type: str):
    syms = _ext_symbols_for_exchange("binance")
    if not syms:
        return
    stream = "/".join(f"{s}@trade" for s in syms)
    url = (
        "wss://stream.binance.com:9443/stream?streams="
        if market_type == "spot"
        else "wss://fstream.binance.com/stream?streams="
    ) + stream
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                logger.info(f"Connected Binance {market_type} WS")
                async for msg in ws:
                    obj = json.loads(msg)
                    data = obj.get("data")
                    if not data:
                        continue
                    px = float(data.get("p", 0) or 0)
                    qty = float(data.get("q", 0) or 0)
                    ts = int(data.get("T", 0) or 0)
                    if px <= 0 or qty <= 0:
                        continue
                    side = "sell" if data.get("m") else "buy"
                    symbol = (data.get("s") or obj.get("stream", "").split("@")[0]).upper()
                    base = symbol.replace("USDT", "")
                    await handle_trade_event(
                        TradeEvent(
                            exchange="binance",
                            market_type=market_type,
                            base=base,
                            side=side,
                            px=px,
                            sz=qty,
                            notional_usd=px * qty,
                            ts=ts,
                        )
                    )
        except Exception as e:
            logger.error(f"Binance {market_type} WS error: {e}")
            await asyncio.sleep(1)


async def bybit_ws(market_type: str):
    syms = _ext_symbols_for_exchange("bybit")
    if not syms:
        return
    endpoint = (
        "wss://stream.bybit.com/v5/public/spot"
        if market_type == "spot"
        else "wss://stream.bybit.com/v5/public/linear"
    )
    args = [f"publicTrade.{s}" for s in syms]
    sub = json.dumps({"op": "subscribe", "args": args})
    while True:
        try:
            async with websockets.connect(endpoint, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(sub)
                logger.info(f"Connected Bybit {market_type} WS")
                async for msg in ws:
                    obj = json.loads(msg)
                    topic = obj.get("topic", "")
                    if not topic.startswith("publicTrade"):
                        continue
                    symbol = topic.split(".")[1].upper()
                    base = symbol.replace("USDT", "")
                    for t in obj.get("data", []):
                        px = float(t.get("p", 0) or 0)
                        qty = float(t.get("v", 0) or 0)
                        ts = int(t.get("T") or t.get("t") or 0)
                        side = str(t.get("S", "")).lower()
                        if px <= 0 or qty <= 0 or side not in ("buy", "sell"):
                            continue
                        await handle_trade_event(
                            TradeEvent(
                                exchange="bybit",
                                market_type=market_type,
                                base=base,
                                side=side,
                                px=px,
                                sz=qty,
                                notional_usd=px * qty,
                                ts=ts,
                            )
                        )
        except Exception as e:
            logger.error(f"Bybit {market_type} WS error: {e}")
            await asyncio.sleep(1)


async def multi_exchange_listener():
    tasks = []
    if "binance" in SIGNAL_EXCHANGES:
        if "spot" in EXTERNAL_SOURCES:
            tasks.append(asyncio.create_task(binance_ws("spot")))
        if "swap" in EXTERNAL_SOURCES:
            tasks.append(asyncio.create_task(binance_ws("swap")))
    if "bybit" in SIGNAL_EXCHANGES:
        if "spot" in EXTERNAL_SOURCES:
            tasks.append(asyncio.create_task(bybit_ws("spot")))
        if "swap" in EXTERNAL_SOURCES:
            tasks.append(asyncio.create_task(bybit_ws("swap")))
    if tasks:
        await asyncio.gather(*tasks)

# ==========================
# WebSocket للـ Bottom-N
# ==========================
def chunked(lst: List[dict], size: int) -> List[List[dict]]:
    return [lst[i:i+size] for i in range(0, len(lst), size)]

async def trades_listener():
    ws_url = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999" if USE_DEMO else "wss://ws.okx.com:8443/ws/v5/public"

    while True:
        args = [{"channel": "trades", "instId": inst} for inst in state.watch_insts]
        if "spot" in SIGNAL_SOURCES:
            args.extend([{"channel": "trades", "instId": inst} for inst in state.watch_spot_insts])
        if not args:
            await asyncio.sleep(2)
            continue
        try:
            async with websockets.connect(ws_url, ping_interval=15, ping_timeout=20) as ws:
                logger.info(f"Connected WS: {ws_url}")
                # اشترك على دفعات لتجنب حدود الحجم
                for chunk in chunked(args, max(1, WS_SUB_CHUNK)):
                    await ws.send(json.dumps({"op": "subscribe", "args": chunk}))
                    await asyncio.sleep(0.05)
                logger.info(f"Subscribed to {len(args)} instruments.")

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("event") in ("subscribe", "error"):
                        # يمكن تسجيل الأخطاء هنا إن وجدت
                        continue
                    data = msg.get("data")
                    if not data:
                        continue
                    for trade in data:
                        inst = trade.get("instId")
                        if inst not in state.mk:
                            continue
                        try:
                            px = float(trade.get("px", 0.0))
                            sz = float(trade.get("sz", 0.0))
                        except Exception:
                            continue
                        side = trade.get("side")
                        ts  = trade.get("ts")
                        if px <= 0 or sz <= 0 or side not in ("buy", "sell"):
                            continue

                        cs = float(state.mk[inst]["contractSize"]) or 1.0
                        notional = px * sz * cs
                        if notional < BIG_TRADE_USD:
                            continue

                        if inst in state.watch_insts:
                            source = "SWAP"
                            target_inst = inst
                        else:
                            source = "SPOT"
                            if not MAP_SPOT_TO_SWAP:
                                continue
                            base = inst.split('-')[0]
                            target_inst = state.swap_map.get(base)
                            if not target_inst:
                                continue

                        # إزالة الازدواجية لنفس الطابع الزمني على الأداة المنفذة
                        try:
                            ts_int = int(ts) if ts else 0
                            last_ts = state.last_signal_ts_by_inst.get(target_inst, 0)
                            if ts_int and ts_int == last_ts:
                                continue
                            if ts_int:
                                state.last_signal_ts_by_inst[target_inst] = ts_int
                        except Exception:
                            pass

                        signal_side = "long" if side == "buy" else "short"
                        trigger = {"side": side, "px": px, "sz": sz, "notional": notional, "ts": ts, "source": source}

                        if state.position is None:
                            eff_lev = get_effective_leverage(state.mk[target_inst]["symbol"])
                            notional_to_use = MARGIN_PER_TRADE_USD * eff_lev
                            await open_position(target_inst, signal_side, notional_to_use, trigger)
                        else:
                            if FLIP_ALLOWED and state.position.inst_id == target_inst and signal_side != state.position.side:
                                await close_position("flip")
                                eff_lev = get_effective_leverage(state.mk[target_inst]["symbol"])
                                notional_to_use = MARGIN_PER_TRADE_USD * eff_lev
                                await open_position(target_inst, signal_side, notional_to_use, trigger)
        except Exception as e:
            logger.error(f"WS error: {e}. Reconnecting in 0.6s…")
            await asyncio.sleep(0.6)

# ==========================
# التشغيل
# ==========================
async def main():
    if not (OKX_API_KEY and OKX_API_SECRET and OKX_API_PASSPHRASE):
        logger.error("OKX API credentials missing. Set them in .env or directly in the file.")
        return

    # بناء قائمة "Bottom-N مع سيولة دنيا" مع ربط السبوت
    selected_swap, selected_spot = await build_universe_bottomN()
    state.watch_insts = [inst for inst, sym, vol in selected_swap]
    state.watch_spot_insts = [inst for inst, sym, vol in selected_spot]
    if len(state.watch_insts) < TOP_N:
        logger.warning(f"Available instruments: {len(state.watch_insts)} < desired TOP_N={TOP_N} (Demo قد لا يدعم كل الأزواج).")

    # تحميل الميتاداتا للأسواق المختارة (swap + spot)
    await ensure_markets(selected_swap + selected_spot)

    # ضبط الرافعة بأسلوب تكيفي
    for inst in state.watch_insts:
        symbol = state.mk.get(inst, {}).get("symbol")
        if symbol:
            await set_leverage_adaptive(symbol, LEVERAGE)
            await asyncio.sleep(0.05)

    # رسالة بدء
    listed = ", ".join(state.watch_insts[:10]) + ("..." if len(state.watch_insts) > 10 else "")
    await tg_send(
        "🚀 *Bot Started (OKX v4 — BottomN)*\n"
        f"• Environment: {'Demo' if USE_DEMO else 'Live'}\n"
        f"• Watching (bottom): {len(state.watch_insts)} instruments (target {TOP_N})\n"
        f"• Min quote vol: ${int(MIN_QUOTE_VOL_USD):,}\n"
        f"• Entry per trade: margin ${MARGIN_PER_TRADE_USD} (effective lev per symbol up to {LEVERAGE}x)\n"
        f"• Leverage varies by symbol (adaptive)\n"
        f"• Signal threshold: ≥ ${int(BIG_TRADE_USD):,}\n"
        f"• TP/SL: ±{TAKE_PROFIT_NET_BPS/100:.2f}% net\n"
        f"• Flip: {'Enabled' if FLIP_ALLOWED else 'Disabled'}\n"
        f"• Examples: {listed}\n"
        f"• Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC"
    )

    tasks = [
        asyncio.create_task(manage_risk_loop()),
        asyncio.create_task(trades_listener()),
        asyncio.create_task(hourly_report_loop()),
    ]
    if ENABLE_TRANSFERS and "transfers" in SIGNAL_SOURCES:
        tasks.append(asyncio.create_task(transfers_listener()))
    if ENABLE_MULTI_EXCHANGE:
        tasks.append(asyncio.create_task(multi_exchange_listener()))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down…")
