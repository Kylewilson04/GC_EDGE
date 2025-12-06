import asyncio
import logging
import numpy as np
import pandas as pd
import yfinance as yf
from typing import Dict, Optional
from functools import wraps
from src.config import SYMBOLS, VOLATILITY_LOOKBACK

logger = logging.getLogger(__name__)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """Decorator for retrying async functions on failure."""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying...")
                    await asyncio.sleep(delay * (attempt + 1))
            return None
        return wrapper
    return decorator


class MarketData:
    def __init__(self):
        pass

    @retry_on_failure(max_retries=3, delay=1.0)
    async def fetch_ohlcv(self, symbol: str, period: str = "1mo", interval: str = "1h") -> Optional[pd.DataFrame]:
        """Fetch OHLCV data from Yahoo Finance."""
        def _fetch_sync():
            ticker = yf.Ticker(symbol)
            df = ticker.history(period=period, interval=interval)
            return df

        df = await asyncio.to_thread(_fetch_sync)

        if df is None or df.empty:
            logger.warning(f"No data fetched for {symbol}")
            return None

        df.columns = df.columns.str.lower()
        df.index.name = "timestamp"
        return df

    async def get_correlations(self) -> pd.DataFrame:
        """Fetch Gold, DXY, US10Y and return correlation matrix with aligned timestamps."""
        tasks = [self.fetch_ohlcv(symbol) for symbol in SYMBOLS.values()]
        dataframes = await asyncio.gather(*tasks)

        valid_data = [
            (name, df) 
            for (name, symbol), df in zip(SYMBOLS.items(), dataframes) 
            if df is not None and not df.empty
        ]
        
        if len(valid_data) < 2:
            logger.error("Insufficient data for correlation calculation")
            return pd.DataFrame()

        closes = pd.DataFrame()
        for name, df in valid_data:
            closes[name.upper()] = df["close"]

        if closes.empty:
            return pd.DataFrame()

        closes = closes.dropna()
        
        if len(closes) < 10:
            logger.warning("Insufficient aligned data points for correlation")
            return pd.DataFrame()

        correlation_matrix = closes.corr()
        return correlation_matrix

    def calc_volatility_levels(self, price_data: pd.Series) -> Dict[str, float]:
        """Calculate volatility-based sigma levels."""
        if price_data.empty or len(price_data) < VOLATILITY_LOOKBACK:
            logger.warning("Insufficient data for volatility calculation")
            return {}

        daily_returns = price_data.pct_change().dropna()
        
        if len(daily_returns) < VOLATILITY_LOOKBACK:
            lookback = len(daily_returns)
        else:
            lookback = VOLATILITY_LOOKBACK

        recent_returns = daily_returns.tail(lookback)
        daily_vol = np.std(recent_returns)
        annualized_vol = daily_vol * np.sqrt(252)

        current_price = float(price_data.iloc[-1])
        
        daily_range_1sigma = current_price * daily_vol
        daily_range_2sigma = current_price * (2 * daily_vol)

        levels = {
            "2_sigma_up": round(current_price + daily_range_2sigma, 2),
            "1_sigma_up": round(current_price + daily_range_1sigma, 2),
            "pivot": round(current_price, 2),
            "1_sigma_down": round(current_price - daily_range_1sigma, 2),
            "2_sigma_down": round(current_price - daily_range_2sigma, 2),
            "annualized_volatility": round(annualized_vol * 100, 2)
        }

        return levels
