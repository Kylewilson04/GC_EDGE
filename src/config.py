import os
from pathlib import Path
from dotenv import load_dotenv

def _load_env_manual(env_file: Path) -> dict:
    """Manually parse .env file as fallback."""
    env_vars = {}
    try:
        # Try different encodings
        for encoding in ['utf-8', 'utf-8-sig', 'utf-16', 'latin-1']:
            try:
                with open(env_file, 'r', encoding=encoding) as f:
                    content = f.read()
                    print(f"  Read {len(content)} chars with {encoding}")
                    # Strip BOM if present
                    content = content.lstrip('\ufeff')
                    for line in content.splitlines():
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            key = key.strip().lstrip('\ufeff')  # Strip BOM from key
                            value = value.strip().strip('"').strip("'")
                            env_vars[key] = value
                            os.environ[key] = value
                            print(f"  Found: {key}={value[:20]}..." if len(value) > 20 else f"  Found: {key}={value}")
                    if env_vars:
                        break
            except UnicodeDecodeError:
                continue
    except Exception as e:
        print(f"⚠ Manual .env parse failed: {e}")
    return env_vars

# Load .env from the project root directory
env_path = Path(__file__).resolve().parent.parent / ".env"

# Try dotenv first
load_dotenv(dotenv_path=str(env_path), override=True)

# Check if it worked
_api_key = os.getenv("OPENROUTER_API_KEY", "")

# If not, try manual parsing
if not _api_key and env_path.exists():
    print(f"dotenv failed, trying manual parse: {env_path}")
    parsed = _load_env_manual(env_path)
    print(f"  Manual parse returned {len(parsed)} keys: {list(parsed.keys())}")
    # Force set from parsed dict as backup
    if "OPENROUTER_API_KEY" in parsed:
        os.environ["OPENROUTER_API_KEY"] = parsed["OPENROUTER_API_KEY"]

# Re-read after manual parse  
_api_key = os.getenv("OPENROUTER_API_KEY", "")
print(f"  os.getenv returned: '{_api_key[:30]}...'" if _api_key else "  os.getenv returned empty")

# Final status
if _api_key:
    print(f"✓ API key loaded ({len(_api_key)} chars)")
else:
    print(f"⚠ API key NOT found. .env exists: {env_path.exists()}, path: {env_path}")

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

OPENROUTER_MODEL = "x-ai/grok-4.1-fast"
VOLATILITY_LOOKBACK = 20

