# Gold_Sovereign_AI

Production-ready trading intelligence system for Gold Futures (GC) analysis with local data processing and LLM synthesis via OpenRouter.

## Features

- **Atomic Data Sovereignty**: All calculations run locally; only anonymized synthesis tokens sent to LLM
- **Async I/O**: Full asyncio implementation for concurrent data fetching
- **Vectorized Math**: NumPy/Pandas for efficient volatility and correlation calculations
- **Market Structure Analysis**: VPOC identification and regime detection (Trend/Balance/Compressed)
- **Volatility Levels**: Sigma-based support/resistance levels (1σ, 2σ)
- **COT Positioning**: Weekly CFTC Commitment of Traders data for speculator/commercial positioning
- **Economic Calendar**: FOMC, NFP, CPI event awareness with risk warnings
- **LLM Synthesis**: Claude 3.5 Sonnet via OpenRouter for institutional-grade reports
- **Discord Integration**: Automated webhook delivery with automatic chunking

## Prerequisites

- Python 3.10+
- API Keys:
  - `OPENROUTER_API_KEY` - OpenRouter API key
  - `DISCORD_WEBHOOK_URL` - Discord webhook URL
- Note: Market data from Yahoo Finance (free, no API key needed)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd GCedge
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create `.env` file:
```bash
OPENROUTER_API_KEY=your_openrouter_key
DISCORD_WEBHOOK_URL=your_discord_webhook_url
SITE_URL=http://localhost
SITE_NAME=GoldSovereign
RUN_INTERVAL_MINUTES=60
```

## Usage

Run the pipeline:
```bash
python main.py
```

The system will:
1. Fetch Gold Futures, DXY, and US10Y data concurrently
2. Calculate correlations and volatility levels
3. Analyze market structure (VPOC, regime)
4. Generate LLM report via OpenRouter
5. Send report to Discord webhook

Logs are written to `gold_sovereign_ai.log` and stdout.

## Architecture

```
src/
├── config.py           # Environment variables and constants
├── data_engine.py      # MarketData: Yahoo Finance fetching, correlations, volatility
├── analysis_engine.py  # LocalAnalyst: VPOC, market regime
├── cot_data.py         # COTAnalyzer: CFTC positioning data
├── economic_calendar.py # EconomicCalendar: FOMC, NFP, CPI events
├── llm_synthesis.py    # ReasoningCore: OpenRouter/Claude integration
└── messenger.py        # DiscordBot: Webhook delivery with chunking

main.py                 # Orchestration pipeline
```

## Report Format

The system generates institutional-grade reports with:

1. **Executive Summary** - Bias, confidence, key level
2. **Market Structure** - VPOC, Value Area, Regime
3. **Volatility Levels** - 1σ/2σ bands with context
4. **Macro Correlations** - DXY, Yields analysis
5. **COT Positioning** - Speculator/Commercial positioning
6. **Event Risk** - Upcoming FOMC/NFP/CPI warnings
7. **Game Theory Scenarios** - Bull/Bear cases with targets

## Configuration

Edit `src/config.py` to modify:
- `SYMBOLS`: Market symbols to analyze
- `VOLATILITY_LOOKBACK`: Days for volatility calculation
- `OPENROUTER_MODEL`: LLM model selection

## Error Handling

- Automatic retries (3 attempts) for all API calls
- Exponential backoff on failures
- Comprehensive logging for audit trail
- Graceful degradation on partial data failures

## Railway Deployment

1. Create account at [railway.app](https://railway.app)

2. Install Railway CLI:
```bash
npm install -g @railway/cli
railway login
```

3. Initialize and deploy:
```bash
railway init
railway up
```

4. Add environment variables in Railway dashboard:
   - `OPENROUTER_API_KEY`
   - `DISCORD_WEBHOOK_URL`
   - `RUN_INTERVAL_MINUTES` (e.g., `60` for hourly reports)

5. Deploy will auto-start. Check logs in Railway dashboard.

### Scheduling

Set `RUN_INTERVAL_MINUTES` to control how often reports run:
- `60` = every hour
- `240` = every 4 hours
- `1440` = daily
- `0` = run once and exit

## License

Proprietary - Internal use only

