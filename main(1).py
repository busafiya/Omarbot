
import os
import time
import json
import math
import threading
import traceback
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

VERSION = "Alpha Fusion Final v10 TV Webhook Manager"
NY = ZoneInfo("America/New_York")

UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMD", "TSLA", "META", "AMZN", "GOOGL", "GOOG", "NFLX", "AVGO", "ARM", "ASML", "TSM", "MU", "QCOM", "INTC", "MRVL", "AMAT", "LRCX", "KLAC", "MCHP", "ON", "STM", "TXN", "ADI", "MPWR", "NXPI", "SWKS", "TER", "COHR", "SMCI", "DELL", "HPE", "HPQ", "IBM", "ORCL", "CRM", "ADBE", "NOW", "SNOW", "DDOG", "NET", "CRWD", "PANW", "ZS", "OKTA", "MDB", "TEAM", "U", "PLTR", "AI", "PATH", "RBLX", "SHOP", "SQ", "PYPL", "HOOD", "COIN", "MSTR", "MARA", "RIOT", "CLSK", "HUT", "IREN", "CAN", "BTBT", "WULF", "CIFR", "BITF", "SOFI", "UPST", "AFRM", "LC", "RKT", "NIO", "LI", "XPEV", "BABA", "JD", "PDD", "BIDU", "TME", "NTES", "FUTU", "TIGR", "BEKE", "YMM", "ZTO", "BILI", "IQ", "WB", "EDU", "TAL", "LU", "FINV", "QFIN", "LX", "DAO", "ZLAB", "BGNE", "YSG", "DADA", "HUYA", "DOYU", "VNET", "ATHM", "MOMO", "TUYA", "KC", "GDS", "VIPS", "FENG", "QH", "ZH", "TCOM", "SPY", "QQQ", "IWM", "DIA", "SMH", "XLK", "XLF", "XLE", "XLY", "XLC", "XLI", "XLP", "XLV", "XLU", "ARKK", "SOXL", "TQQQ", "SQQQ", "UVXY", "VXX", "JPM", "BAC", "WFC", "C", "GS", "MS", "CBOE", "CME", "ICE", "SCHW", "BX", "BLK", "TROW", "AXP", "COF", "DFS", "USB", "PNC", "TFC", "FITB", "HBAN", "KEY", "RF", "CFG", "MTB", "CMA", "EWBC", "WAL", "FHN", "ZION", "ALL", "TRV", "CB", "AIG", "MET", "PRU", "AFL", "HIG", "PGR", "UNH", "ELV", "CI", "HUM", "CNC", "CVS", "WBA", "MCK", "COR", "CAH", "ABC", "AMGN", "GILD", "REGN", "BIIB", "VRTX", "MRNA", "BNTX", "PFE", "MRK", "LLY", "BMY", "JNJ", "ABBV", "ISRG", "SYK", "MDT", "BSX", "EW", "DXCM", "HOLX", "TMO", "DHR", "A", "ILMN", "GEHC", "ZBH", "PODD", "ALGN", "RMD", "XOM", "CVX", "COP", "EOG", "SLB", "HAL", "OXY", "DVN", "MRO", "APA", "FANG", "PXD", "HES", "VLO", "MPC", "PSX", "KMI", "WMB", "LNG", "EQT", "CTRA", "CHK", "RIG", "NOV", "BKR", "CAT", "DE", "CMI", "ETN", "EMR", "GE", "HON", "MMM", "BA", "LMT", "RTX", "NOC", "GD", "TDG", "TXT", "URI", "PCAR", "FAST", "GWW", "PH", "ROK", "DOV", "IR", "ITW", "CARR", "OTIS", "JCI", "TT", "WM", "RSG", "CTAS", "CPRT", "WMT", "COST", "TGT", "HD", "LOW", "TJX", "ROST", "DG", "DLTR", "FIVE", "KR", "SFM", "WSM", "BBY", "M", "DPZ", "SBUX", "MCD", "CMG", "YUM", "DRI", "TXRH", "CAVA", "SHAK", "NKE", "LULU", "DECK", "ONON", "UAA", "VFC", "TPR", "CPRI", "KO", "PEP", "MNST", "CELH", "KDP", "STZ", "TAP", "BUD", "PM", "MO", "BTI", "CL", "PG", "KMB", "CLX", "EL", "KVUE", "GIS", "K", "HC", "MDLZ", "HSY", "TSN", "CAG", "CPB", "SJM", "HRL", "DIS", "PARA", "WBD", "ROKU", "SPOT", "LYV", "TKO", "EA", "TTWO", "SNAP", "PINS", "MTCH", "DUOL", "CART", "DASH", "UBER", "LYFT", "ABNB", "BKNG", "EXPE", "MAR", "HLT", "RCL", "CCL", "NCLH", "UAL", "DAL", "AAL", "LUV", "JBLU", "ALK", "F", "GM", "RIVN", "LCID", "TM", "HMC", "STLA", "MBLY", "APTV", "BWA", "LEA", "MGA", "QS", "CHPT", "BLNK", "EVGO", "BE", "NKLA", "SPR", "HEI", "V", "MA", "FIS", "FISV", "GPN", "ADP", "PAYX", "INTU", "BILL", "FOUR", "FLT", "FI", "ZM", "DOCU", "TWLO", "FSLY", "ESTC", "SPLK"]

SCAN_LOCK = threading.Lock()
SCAN_STATE = {
    "version": VERSION,
    "running": False,
    "last_started_at": None,
    "last_finished_at": None,
    "last_error": None,
    "last_checked": 0,
    "last_qualified": 0,
    "last_sent": 0,
    "last_message": "not_started",
    "last_results": [],
}
DEDUP_CACHE = {}
SCHEDULER_STARTED = False

# -----------------------------
# Helpers
# -----------------------------
def env_bool(name, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")

def env_int(name, default):
    try:
        return int(float(os.getenv(name, default)))
    except Exception:
        return default

def env_float(name, default):
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default

def now_ny():
    return datetime.now(NY)

def now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def in_market_scan_window():
    n = now_ny()
    if n.weekday() >= 5:
        return False
    t = n.time()
    return (dtime(9, 35) <= t <= dtime(11, 30)) or (dtime(15, 0) <= t <= dtime(15, 50))

def telegram_send(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return {"ok": False, "error": "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID"}
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=20)
        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "text": r.text}
        return data
    except Exception as e:
        return {"ok": False, "error": str(e)}

def safe_float(x, default=0.0):
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.upper() == "AUTO":
            return default
        return float(x)
    except Exception:
        return default

def fmt(x):
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)

# -----------------------------
# Data + signal engine
# -----------------------------
def fetch_chart(symbol):
    # Yahoo chart endpoint, free/no key. It may be delayed; suitable for MVP testing, not guaranteed institutional data.
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": "5d", "interval": "5m", "includePrePost": "false"}
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, params=params, headers=headers, timeout=12)
    data = r.json()
    result = data.get("chart", {}).get("result") or []
    if not result:
        return None
    res = result[0]
    timestamps = res.get("timestamp") or []
    quote = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    opens = quote.get("open") or []
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    volumes = quote.get("volume") or []
    rows = []
    for i, ts in enumerate(timestamps):
        try:
            o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], volumes[i]
            if None in (o, h, l, c) or v is None:
                continue
            dt = datetime.fromtimestamp(ts, NY)
            rows.append({"t": dt, "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": float(v)})
        except Exception:
            continue
    return rows if len(rows) >= 30 else None

def ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    out = []
    prev = values[0]
    for v in values:
        prev = v * k + prev * (1 - k)
        out.append(prev)
    return out

def compute_vwap(rows):
    # Session VWAP based on last trading day in returned rows.
    last_date = rows[-1]["t"].date()
    session = [r for r in rows if r["t"].date() == last_date]
    pv = 0.0
    vv = 0.0
    vwaps = []
    for r in session:
        typical = (r["high"] + r["low"] + r["close"]) / 3
        pv += typical * r["volume"]
        vv += r["volume"]
        vwaps.append(pv / vv if vv else r["close"])
    return session, vwaps

def compute_atr_like(session, lookback=14):
    if len(session) < 2:
        return max(session[-1]["close"] * 0.01, 0.05) if session else 0.05
    trs = []
    prev = session[0]["close"]
    for r in session[1:]:
        tr = max(r["high"] - r["low"], abs(r["high"] - prev), abs(r["low"] - prev))
        trs.append(tr)
        prev = r["close"]
    vals = trs[-lookback:] if len(trs) >= lookback else trs
    return sum(vals) / len(vals) if vals else max(session[-1]["close"] * 0.01, 0.05)

def analyze_symbol(symbol):
    try:
        rows = fetch_chart(symbol)
        if not rows:
            return None
        session, vwaps = compute_vwap(rows)
        if len(session) < 8:
            return None
        last = session[-1]
        close = last["close"]
        if close < 5:
            return None
        vwap = vwaps[-1]
        atr = compute_atr_like(session)
        recent_vols = [r["volume"] for r in session[-21:-1] if r.get("volume") is not None]
        avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else max(last["volume"], 1)
        rvol = last["volume"] / avg_vol if avg_vol else 0
        closes = [r["close"] for r in session]
        macd = 0.0
        macd_sig = 0.0
        hist = 0.0
        if len(closes) >= 35:
            ema12 = ema(closes, 12)
            ema26 = ema(closes, 26)
            macd_series = [a-b for a, b in zip(ema12, ema26)]
            sig_series = ema(macd_series, 9)
            macd, macd_sig = macd_series[-1], sig_series[-1]
            hist = macd - macd_sig
        macd_bull = macd > macd_sig and hist > 0
        above_vwap = close > vwap
        candle_range = max(last["high"] - last["low"], 0.0001)
        close_near_high = (close - last["low"]) / candle_range >= 0.62
        bull_candle = close > last["open"]
        # Opening range from 9:30 bar(s) through first 5 min if available
        or_bars = [r for r in session if r["t"].hour == 9 and 30 <= r["t"].minute <= 34]
        or_high = max([r["high"] for r in or_bars], default=None)
        or_low = min([r["low"] for r in or_bars], default=None)
        orb = bool(or_high and close > or_high and above_vwap and rvol >= 1.8 and bull_candle)
        touch_vwap = last["low"] <= vwap * 1.003 and close >= vwap
        pullback = bool(above_vwap and touch_vwap and rvol >= 1.25 and bull_candle)
        vol_exp = bool(above_vwap and rvol >= 2.3 and bull_candle and close_near_high)
        setup = None
        score = 0
        if orb:
            setup = "ORB_BREAKOUT"
            score = 86
        if pullback and score < 84:
            setup = "VWAP_PULLBACK"
            score = 84
        if vol_exp and score < 88:
            setup = "VOLUME_EXPLOSION"
            score = 88
        if not setup:
            return None
        if macd_bull: score += 4
        if rvol >= 2.0: score += 3
        if rvol >= 3.0: score += 3
        if close_near_high: score += 2
        if above_vwap: score += 2
        score = min(100, int(round(score)))
        threshold = env_int("SCORE_THRESHOLD", 88)
        if score < threshold:
            return None
        risk = max(atr * 0.35, close * 0.003, 0.03)
        if setup == "ORB_BREAKOUT" and or_high and or_low:
            risk = max((or_high - or_low) * 0.60, atr * 0.25, close * 0.003)
        entry = close
        stop = entry - risk
        t1 = entry + risk
        t2 = entry + risk * 2
        t3 = entry + risk * 3
        risk_budget = env_float("RISK_PER_TRADE_USD", 12)
        equity = env_float("ACCOUNT_EQUITY", 1500)
        shares_by_risk = math.floor(risk_budget / max(risk, 0.01))
        shares_by_cap = math.floor((equity * 0.90) / entry)
        shares = max(1, min(shares_by_risk, shares_by_cap))
        return {
            "symbol": symbol,
            "setup": setup,
            "direction": "LONG",
            "score": score,
            "entry": entry,
            "entry_zone": f"{entry*0.9995:.2f} - {entry*1.0005:.2f}",
            "stop": stop,
            "target1": t1,
            "target2": t2,
            "target3": t3,
            "risk_share": risk,
            "risk_budget": risk_budget,
            "shares": shares,
            "rr": 2.0,
            "rvol": rvol,
            "vwap": vwap,
            "atr": atr,
            "reason": f"{setup}: VWAP agreement + RVOL {rvol:.2f} + MACD {'bullish' if macd_bull else 'neutral'} + strict score {score}/100",
        }
    except Exception:
        return None

def format_signal(s):
    return (
        "🚨 <b>Alpha Fusion Signal</b>\n\n"
        f"<b>Symbol:</b> {s['symbol']}\n"
        f"<b>Setup:</b> {s['setup']}\n"
        f"<b>Direction:</b> {s['direction']}\n"
        f"<b>Score:</b> {s['score']}/100\n\n"
        f"<b>Entry Zone:</b> {s['entry_zone']}\n"
        f"<b>Suggested Entry:</b> {fmt(s['entry'])}\n"
        f"<b>Stop Loss:</b> {fmt(s['stop'])}\n\n"
        f"<b>Target 1:</b> {fmt(s['target1'])}\n"
        f"<b>Target 2:</b> {fmt(s['target2'])}\n"
        f"<b>Target 3:</b> {fmt(s['target3'])}\n\n"
        f"<b>Risk/Share:</b> ${fmt(s['risk_share'])}\n"
        f"<b>Risk Budget:</b> ${fmt(s['risk_budget'])}\n"
        f"<b>Suggested Shares:</b> {s['shares']}\n"
        f"<b>RR to T2:</b> {fmt(s['rr'])}\n\n"
        f"<b>RVOL:</b> {s['rvol']:.2f}\n"
        f"<b>VWAP:</b> {fmt(s['vwap'])}\n"
        f"<b>ATR:</b> {fmt(s['atr'])}\n\n"
        f"<b>Reason:</b>\n{s['reason']}\n\n"
        "⚠️ Manual execution only. This is not financial advice."
    )

def should_send(symbol, setup):
    key = f"{symbol}:{setup}"
    dedup = env_int("DEDUP_MINUTES", 45) * 60
    last = DEDUP_CACHE.get(key, 0)
    if time.time() - last < dedup:
        return False
    DEDUP_CACHE[key] = time.time()
    return True

def run_scan(limit=None, notify=True, force=False, workers=None):
    with SCAN_LOCK:
        if SCAN_STATE["running"]:
            return {"ok": False, "message": "scan_already_running", "state": SCAN_STATE.copy()}
        SCAN_STATE.update({"running": True, "last_started_at": now_iso(), "last_error": None, "last_message": "running"})
    started = time.time()
    try:
        if env_bool("SCAN_DURING_MARKET_ONLY", True) and not force and not in_market_scan_window():
            msg = "market_closed_or_outside_scan_window"
            with SCAN_LOCK:
                SCAN_STATE.update({"running": False, "last_finished_at": now_iso(), "last_checked": 0, "last_qualified": 0, "last_sent": 0, "last_message": msg, "last_results": []})
            return {"ok": True, "message": msg, "market_window": False}
        if limit is None:
            limit = env_int("AUTO_SCAN_LIMIT", 400)
        limit = max(1, min(int(limit), len(UNIVERSE)))
        workers = int(workers or env_int("SCAN_WORKERS", 12))
        workers = max(1, min(workers, 24))
        symbols = UNIVERSE[:limit]
        results = []
        checked = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(analyze_symbol, s): s for s in symbols}
            for fut in as_completed(futures):
                checked += 1
                res = None
                try:
                    res = fut.result()
                except Exception:
                    res = None
                if res:
                    results.append(res)
        results.sort(key=lambda x: x.get("score", 0), reverse=True)
        max_sigs = env_int("MAX_SIGNALS_PER_SCAN", 3)
        sendable = results[:max_sigs]
        sent = 0
        if sendable:
            for s in sendable:
                if should_send(s["symbol"], s["setup"]):
                    telegram_send(format_signal(s))
                    sent += 1
        elif notify or env_bool("AUTO_NOTIFY_EMPTY", False):
            telegram_send(f"🔎 <b>{VERSION}</b>\nChecked {checked} symbols.\nNo qualified {env_int('SCORE_THRESHOLD', 88)}+ setup found right now.")
        with SCAN_LOCK:
            SCAN_STATE.update({
                "running": False,
                "last_finished_at": now_iso(),
                "last_checked": checked,
                "last_qualified": len(results),
                "last_sent": sent,
                "last_message": "done",
                "last_runtime_sec": round(time.time() - started, 2),
                "last_results": [{k: v for k, v in r.items() if k in ("symbol", "setup", "score", "entry", "stop", "target1", "target2", "rvol")} for r in results[:10]],
            })
        return {"ok": True, "checked": checked, "qualified": len(results), "sent": sent, "runtime_sec": round(time.time()-started, 2), "top": SCAN_STATE["last_results"]}
    except Exception as e:
        with SCAN_LOCK:
            SCAN_STATE.update({"running": False, "last_finished_at": now_iso(), "last_error": str(e), "last_message": "error"})
        return {"ok": False, "error": str(e), "trace": traceback.format_exc()[-1200:]}

def trigger_background_scan(limit=None, notify=True, force=False, workers=None):
    with SCAN_LOCK:
        if SCAN_STATE["running"]:
            return False
    t = threading.Thread(target=run_scan, kwargs={"limit": limit, "notify": notify, "force": force, "workers": workers}, daemon=True)
    t.start()
    return True

# -----------------------------
# Scheduler
# -----------------------------
def scheduler_loop():
    time.sleep(8)
    telegram_send(f"✅ <b>{VERSION} Connected</b>\nAuto-scan enabled: {env_bool('AUTO_SCAN_ENABLED', False)}\nUniverse: {len(UNIVERSE)} symbols.")
    while True:
        try:
            if env_bool("AUTO_SCAN_ENABLED", False):
                if (not env_bool("SCAN_DURING_MARKET_ONLY", True)) or in_market_scan_window():
                    trigger_background_scan(limit=env_int("AUTO_SCAN_LIMIT", 400), notify=env_bool("AUTO_NOTIFY_EMPTY", False), force=False)
            time.sleep(max(60, env_int("SCAN_INTERVAL_SECONDS", 300)))
        except Exception:
            time.sleep(60)

def start_scheduler_once():
    global SCHEDULER_STARTED
    if not SCHEDULER_STARTED:
        SCHEDULER_STARTED = True
        threading.Thread(target=scheduler_loop, daemon=True).start()

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home():
    return jsonify({"ok": True, "service": VERSION, "routes": ["/healthz", "/test-telegram", "/universe", "/scan-trigger", "/scan-status", "/scan-now", "/demo-signal", "/webhook"]})

@app.route("/healthz")
def healthz():
    return jsonify({"ok": True, "status": "healthy", "service": VERSION, "time": now_iso()})

@app.route("/test-telegram")
def test_telegram():
    res = telegram_send(f"✅ <b>{VERSION} Connected</b>\nRender → Telegram يعمل بنجاح.\nUniverse: {len(UNIVERSE)} symbols.")
    return jsonify({"ok": bool(res.get("ok")), "telegram": res, "version": VERSION})

@app.route("/universe")
def universe():
    limit = env_int("limit", 400)
    try:
        limit = int(request.args.get("limit", 400))
    except Exception:
        limit = 400
    limit = max(1, min(limit, len(UNIVERSE)))
    return jsonify({"ok": True, "version": VERSION, "count": limit, "total_available": len(UNIVERSE), "symbols": UNIVERSE[:limit]})

@app.route("/scan-status")
def scan_status():
    with SCAN_LOCK:
        st = SCAN_STATE.copy()
    return jsonify({
        "ok": True,
        "version": VERSION,
        "auto_scan_enabled": env_bool("AUTO_SCAN_ENABLED", False),
        "auto_scan_limit": env_int("AUTO_SCAN_LIMIT", 400),
        "scan_interval_seconds": env_int("SCAN_INTERVAL_SECONDS", 300),
        "market_only": env_bool("SCAN_DURING_MARKET_ONLY", True),
        "in_market_window_now": in_market_scan_window(),
        "score_threshold": env_int("SCORE_THRESHOLD", 88),
        "state": st,
    })

@app.route("/scan-trigger")
def scan_trigger():
    limit = request.args.get("limit")
    limit = int(limit) if limit else env_int("AUTO_SCAN_LIMIT", 400)
    notify = request.args.get("notify", "true").lower() in ("1", "true", "yes")
    force = request.args.get("force", "false").lower() in ("1", "true", "yes")
    workers = request.args.get("workers")
    workers = int(workers) if workers else None
    started = trigger_background_scan(limit=limit, notify=notify, force=force, workers=workers)
    return jsonify({"ok": True, "started": started, "message": "scan_started_in_background" if started else "scan_already_running", "status_url": "/scan-status"})

@app.route("/scan-now")
def scan_now():
    # Compatibility route. It now triggers background scan to avoid Render timeout.
    return scan_trigger()

@app.route("/demo-signal")
def demo_signal():
    s = {
        "symbol": "AMD", "setup": "VWAP_PULLBACK", "direction": "LONG", "score": 92,
        "entry_zone": "170.78 - 171.25", "entry": 171.01, "stop": 170.43,
        "target1": 171.59, "target2": 172.17, "target3": 172.75,
        "risk_share": 0.58, "risk_budget": env_float("RISK_PER_TRADE_USD", 12), "shares": 7,
        "rr": 2.0, "rvol": 3.2, "vwap": 170.95, "atr": 2.4,
        "reason": "Demo only: VWAP + RVOL + pullback confirmation",
    }
    res = telegram_send(format_signal(s))
    return jsonify({"ok": bool(res.get("ok")), "telegram": res})

def normalize_symbol(value):
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    # TradingView may send NASDAQ:AMD, NYSE:UBER, etc.
    if ":" in raw:
        raw = raw.split(":")[-1]
    return raw.replace(" ", "")


def data_float(data, key, default=0.0):
    val = data.get(key)
    if val is None:
        return default
    if isinstance(val, str):
        txt = val.strip()
        if txt == "" or txt.lower() in ("null", "none", "na", "nan"):
            return default
        # Some TradingView placeholders may arrive literally if the alert snapshot is wrong.
        if txt.startswith("{{") and txt.endswith("}}"):
            return default
    return safe_float(val, default)


def build_trade_plan_from_webhook(data):
    symbol = normalize_symbol(data.get("symbol") or data.get("ticker"))
    if not symbol:
        return None

    tv_price = data_float(data, "price", 0.0)
    tv_entry = data_float(data, "entry", 0.0)
    tv_stop = data_float(data, "stop_loss", 0.0)
    tv_t1 = data_float(data, "target1", 0.0)
    tv_t2 = data_float(data, "target2", 0.0)
    tv_t3 = data_float(data, "target3", 0.0)
    tv_score = data_float(data, "score", 0.0)
    tv_rvol = data_float(data, "rvol", 0.0)
    tv_vwap_distance = data_float(data, "vwap_distance_pct", 0.0)
    tv_dollar_volume = data_float(data, "dollar_volume", 0.0)
    setup_code = str(data.get("setup_code") or "NA")
    session_code = str(data.get("session_code") or "NA")

    rows = fetch_chart(symbol)
    session = []
    vwaps = []
    last = None
    vwap = 0.0
    atr = 0.0
    rvol = tv_rvol

    if rows:
        session, vwaps = compute_vwap(rows)
        if session:
            last = session[-1]
            vwap = vwaps[-1] if vwaps else 0.0
            atr = compute_atr_like(session)
            recent_vols = [r["volume"] for r in session[-21:-1] if r.get("volume") is not None]
            avg_vol = sum(recent_vols) / len(recent_vols) if recent_vols else max(last.get("volume", 1), 1)
            rvol = last.get("volume", 0) / avg_vol if avg_vol else tv_rvol

    entry = tv_entry or tv_price or (last["close"] if last else 0.0)
    if entry <= 0:
        return None

    if tv_stop > 0 and tv_stop < entry:
        stop = tv_stop
    else:
        candle_low = last["low"] if last else entry
        effective_vwap = vwap if vwap > 0 else entry
        effective_atr = atr if atr > 0 else max(entry * 0.01, 0.05)
        risk_buffer = max(effective_atr * 0.25, entry * 0.002, 0.02)
        default_risk = max(effective_atr * 0.60, entry * 0.005, 0.03)
        structural_stop = min(candle_low, effective_vwap) - risk_buffer
        stop = structural_stop if structural_stop < entry else entry - default_risk

    risk_share = entry - stop
    if risk_share <= 0:
        risk_share = max(entry * 0.005, 0.03)
        stop = entry - risk_share

    max_risk_pct = env_float("MAX_WEBHOOK_RISK_PCT", 2.0)
    risk_pct = risk_share / entry * 100.0 if entry > 0 else 999.0
    if risk_pct > max_risk_pct:
        risk_share = entry * (max_risk_pct / 100.0)
        stop = entry - risk_share
        risk_pct = max_risk_pct

    target1 = tv_t1 if tv_t1 > entry else entry + risk_share * 1.0
    target2 = tv_t2 if tv_t2 > entry else entry + risk_share * 2.0
    target3 = tv_t3 if tv_t3 > entry else entry + risk_share * 3.0

    risk_budget = env_float("RISK_PER_TRADE_USD", 12)
    equity = env_float("ACCOUNT_EQUITY", 1500)
    shares_by_risk = math.floor(risk_budget / max(risk_share, 0.01))
    shares_by_cap = math.floor((equity * 0.90) / entry)
    shares = max(1, min(shares_by_risk, shares_by_cap))

    score = int(tv_score) if tv_score > 0 else 0
    rr_to_t2 = (target2 - entry) / risk_share if risk_share > 0 else 0.0

    reason_parts = [
        "TradingView Alpha Long trigger received.",
        "Render built the trade plan using webhook price plus latest 5m chart data when available.",
        f"Setup code: {setup_code}.",
        f"Session code: {session_code}.",
        f"Risk capped at {risk_pct:.2f}%.",
    ]
    if tv_vwap_distance:
        reason_parts.append(f"TV VWAP distance: {tv_vwap_distance:.2f}%.")
    if tv_dollar_volume:
        reason_parts.append(f"TV dollar volume: {tv_dollar_volume:.0f}.")

    return {
        "symbol": symbol,
        "setup": f"ALPHA_LONG / code {setup_code}",
        "direction": "LONG",
        "score": score,
        "entry": entry,
        "entry_zone": f"{entry * 0.9995:.2f} - {entry * 1.0005:.2f}",
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "target3": target3,
        "risk_share": risk_share,
        "risk_budget": risk_budget,
        "shares": shares,
        "rr": rr_to_t2,
        "rvol": rvol,
        "vwap": vwap if vwap > 0 else entry,
        "atr": atr if atr > 0 else max(entry * 0.01, 0.05),
        "reason": " ".join(reason_parts),
    }


@app.route("/webhook", methods=["POST", "GET"])
def webhook():
    if request.method == "GET":
        return jsonify({"ok": True, "message": "webhook alive", "version": VERSION})

    data = request.get_json(silent=True) or {}
    print("WEBHOOK_RAW:", json.dumps(data, ensure_ascii=False), flush=True)

    setup = str(data.get("setup") or "").strip().upper()
    source = str(data.get("source") or "").strip().lower()

    if setup == "ALPHA_LONG" or "alpha fusion" in source:
        signal = build_trade_plan_from_webhook(data)
        if signal:
            res = telegram_send(format_signal(signal))
            return jsonify({
                "ok": bool(res.get("ok")),
                "mode": "alpha_long_trade_plan",
                "received": data,
                "signal": signal,
                "telegram": res,
            })

        text = (
            "⚠️ <b>Alpha Long received, but Render could not calculate trade plan</b>\n\n"
            + "\n".join([f"<b>{k}:</b> {v}" for k, v in data.items()])
        )
        res = telegram_send(text)
        return jsonify({"ok": bool(res.get("ok")), "mode": "alpha_long_no_plan", "received": data, "telegram": res})

    text = "🚨 <b>TradingView Webhook</b>\n" + "\n".join([f"<b>{k}:</b> {v}" for k, v in data.items()])
    res = telegram_send(text)
    return jsonify({"ok": bool(res.get("ok")), "mode": "raw_webhook", "received": data, "telegram": res})

start_scheduler_once()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
