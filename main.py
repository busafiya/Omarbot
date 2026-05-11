import os, time, requests, math

BASE = "https://www.binance.com"
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SCAN_EVERY = 300
MIN_SCORE = 8
MAX_COINS = 120

def tg(msg):
    requests.post(
        f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
        json={"chat_id": CHAT_ID, "text": msg},
        timeout=10
    )

def get_json(path, params=None):
    r = requests.get(BASE + path, params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()

def find_list(x):
    if isinstance(x, list):
        return x
    if isinstance(x, dict):
        for v in x.values():
            found = find_list(v)
            if found:
                return found
    return []

def alpha_tokens():
    data = get_json("/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list")
    rows = find_list(data)
    out = []
    for t in rows:
        if not isinstance(t, dict):
            continue
        name = t.get("symbol") or t.get("tokenSymbol") or t.get("baseAsset")
        aid = t.get("alphaId") or t.get("tokenId") or t.get("id")
        if name and aid:
            out.append(f"{name}_{aid}USDT")
        elif name and name.endswith("USDT"):
            out.append(name)
    return list(dict.fromkeys(out))[:MAX_COINS]

def klines(symbol, interval, limit=120):
    data = get_json(
        "/bapi/defi/v1/public/alpha-trade/klines",
        {"symbol": symbol, "interval": interval, "limit": limit}
    )
    rows = find_list(data)
    clean = []
    for x in rows:
        try:
            clean.append({
                "open": float(x[1]),
                "high": float(x[2]),
                "low": float(x[3]),
                "close": float(x[4]),
                "volume": float(x[5]),
            })
        except:
            pass
    return clean

def ema(vals, n):
    if len(vals) < n:
        return None
    k = 2 / (n + 1)
    e = vals[0]
    for v in vals[1:]:
        e = v * k + e * (1 - k)
    return e

def rsi(vals, n=14):
    if len(vals) < n + 1:
        return 50
    gains, losses = [], []
    for i in range(1, len(vals)):
        d = vals[i] - vals[i-1]
        gains.append(max(d, 0))
        losses.append(abs(min(d, 0)))
    ag = sum(gains[-n:]) / n
    al = sum(losses[-n:]) / n
    if al == 0:
        return 100
    rs = ag / al
    return 100 - (100 / (1 + rs))

def analyze(symbol):
    k15 = klines(symbol, "15m")
    k1h = klines(symbol, "1h")
    k4h = klines(symbol, "4h")

    if len(k15) < 50 or len(k1h) < 40:
        return None

    c15 = [x["close"] for x in k15]
    v15 = [x["volume"] for x in k15]
    c1h = [x["close"] for x in k1h]
    c4h = [x["close"] for x in k4h] if len(k4h) >= 30 else c1h

    price = c15[-1]
    ema7 = ema(c15[-40:], 7)
    ema25 = ema(c15[-60:], 25)
    ema99 = ema(c1h[-100:], 50)

    r = rsi(c15)
    vol_now = v15[-1]
    vol_avg = sum(v15[-20:]) / 20
    high20 = max(x["high"] for x in k15[-20:-1])
    low10 = min(x["low"] for x in k15[-10:])

    score = 0
    reasons = []

    if ema7 and ema25 and price > ema7 > ema25:
        score += 2
        reasons.append("ترند 15m صاعد")

    if ema99 and price > ema99:
        score += 2
        reasons.append("فوق متوسط 1h")

    if price >= high20 * 0.995:
        score += 2
        reasons.append("قريب من كسر مقاومة")

    if vol_now > vol_avg * 1.4:
        score += 2
        reasons.append("فوليوم دخول قوي")

    if 48 <= r <= 72:
        score += 1
        reasons.append("RSI صحي")

    if c15[-1] > c15[-2] and c15[-2] >= c15[-3]:
        score += 1
        reasons.append("زخم شموع إيجابي")

    if len(c4h) >= 20 and c4h[-1] > ema(c4h[-30:], 20):
        score += 1
        reasons.append("4h داعم")

    stop = min(low10, price * 0.965)
    risk = price - stop
    if risk <= 0:
        return None

    tp1 = price + risk * 1.2
    tp2 = price + risk * 2.0
    tp3 = price + risk * 3.0

    return score, price, stop, tp1, tp2, tp3, reasons

def main():
    tg("✅ O4 Alpha الحقيقي اشتغل: يسحب عملات Alpha من Binance ويفحصها")

    sent = {}

    while True:
        try:
            symbols = alpha_tokens()
            print("Alpha count:", len(symbols))

            for s in symbols:
                try:
                    result = analyze(s)
                    if not result:
                        continue

                    score, price, stop, tp1, tp2, tp3, reasons = result

                    if score >= MIN_SCORE:
                        key = f"{s}-{round(price, 8)}"
                        if sent.get(s) == key:
                            continue
                        sent[s] = key

                        tg(f"""🔥 O4 Alpha فرصة

العملة: {s}
القوة: {score}/10

الدخول: {price:.8f}
الوقف: {stop:.8f}

هدف 1: {tp1:.8f}
هدف 2: {tp2:.8f}
هدف 3: {tp3:.8f}

الأسباب:
- {chr(10).join(reasons)}

ملاحظة: هذه إشارة فقط، لا يوجد شراء تلقائي.""")
                except Exception as e:
                    print("skip", s, e)

        except Exception as e:
            tg(f"⚠️ خطأ في السحب أو الفحص: {e}")

        time.sleep(SCAN_EVERY)

main()