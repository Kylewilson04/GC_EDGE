import os
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
SITE_URL = os.getenv("SITE_URL", "http://localhost")
SITE_NAME = os.getenv("SITE_NAME", "GoldSovereign")

SYMBOLS = ["C:XAUUSD", "I:DXY", "I:US10Y"]
OPENROUTER_MODEL = "anthropic/claude-3.5-sonnet"
VOLATILITY_LOOKBACK = 20

