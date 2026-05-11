import os

print("omar alpha bot running")

api = os.getenv("BINANCE_API_KEY")
secret = os.getenv("BINANCE_SECRET_KEY")

print("API Loaded:", bool(api))
print("SECRET Loaded:", bool(secret))