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

# GhostKing Protocol symbols
GHOSTKING_SYMBOLS = {
    "es": "ES=F",        # S&P 500 E-mini Futures
    "us10y": "^TNX",     # 10-Year Treasury Yield
    "us02y": "2YY=F",    # 2-Year Treasury Yield Futures
}

# FRED series for liquidity data
FRED_SERIES = {
    "walcl": "WALCL",         # Federal Reserve Total Assets
    "wtregen": "WTREGEN",     # Treasury General Account
    "rrpontsyd": "RRPONTSYD", # Overnight Reverse Repo
}

OPENROUTER_MODEL = "x-ai/grok-4.1-fast"
VOLATILITY_LOOKBACK = 20
LIQUIDITY_EMA_PERIOD = 20     # EMA period for Net Liquidity trend

