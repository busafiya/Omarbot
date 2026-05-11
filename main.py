import os
import time
import requests

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

WATCHLIST = [
    "DOGEUSDT",
    "SHIBUSDT",
    "FARTCOINUSDT",
    "NOTUSDT",
    "LUNCUSDT",
    "ARBUSDT",
]

INTERVAL = "15m"


def send_msg(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": text})


def get_klines(symbol, interval="15m", limit=80):
    url = "https://api.binance.com/api/v3/klines"
    r = requests.get(url, params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()


def ema(values, period):
    k = 2 / (period + 1)
    result = values[0]
    for v in values[1:]:
        result = v * k + result * (1 - k)
    return result


def rsi(closes, period=14):
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def score_symbol(symbol):
    data = get_klines(symbol, INTERVAL)
    closes = [float(x[4]) for x in data]
    volumes = [float(x[5]) for x in data]

    price = closes[-1]
    ema7 = ema(closes[-30:], 7)
    ema25 = ema(closes[-50:], 25)
    r = rsi(closes)

    vol_now = volumes[-1]
    vol_avg = sum(volumes[-20:]) / 20

    score = 0
    reasons = []

    if price > ema7:
        score += 2
        reasons.append("فوق EMA7")

    if price > ema25:
        score += 2
        reasons.append("فوق EMA25")

    if vol_now > vol_avg * 1.3:
        score += 2
        reasons.append("فوليوم قوي")

    if 50 <= r <= 75:
        score += 2
        reasons.append("RSI صحي")

    if closes[-1] > closes[-2] > closes[-3]:
        score += 2
        reasons.append("شموع صاعدة")

    entry = price
    stop = price * 0.965
    tp1 = price * 1.035
    tp2 = price * 1.07
    tp3 = price * 1.12

    return score, price, stop, tp1, tp2, tp3, reasons


def main():
    send_msg("✅ O4 Alpha Scanner اشتغل بنجاح")

    while True:
        for symbol in WATCHLIST:
            try:
                score, price, stop, tp1, tp2, tp3, reasons = score_symbol(symbol)

                if score >= 7:
                    msg = f"""🔥 O4 فرصة قوية

العملة: {symbol}
التقييم: {score}/10

الدخول: {price:.8f}
الوقف: {stop:.8f}

TP1: {tp1:.8f}
TP2: {tp2:.8f}
TP3: {tp3:.8f}

الأسباب:
- {chr(10).join(reasons)}
"""
                    send_msg(msg)

            except Exception as e:
                print(symbol, e)

        time.sleep(300)


main()