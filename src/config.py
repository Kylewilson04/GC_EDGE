import os
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
SITE_URL = os.getenv("SITE_URL", "http://localhost")
SITE_NAME = os.getenv("SITE_NAME", "GoldSovereign")

# Yahoo Finance tickers
SYMBOLS = {
    "gold": "GC=F",      # Gold Futures
    "dxy": "DX-Y.NYB",   # US Dollar Index
    "us10y": "^TNX"      # 10-Year Treasury Yield
}
OPENROUTER_MODEL = "anthropic/claude-3.5-sonnet"
VOLATILITY_LOOKBACK = 20

