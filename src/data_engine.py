import asyncio
import logging
import numpy as np
import pandas as pd
from polygon import RESTClient
from typing import Dict, List, Optional
from functools import wraps
from src.config import POLYGON_API_KEY, SYMBOLS, VOLATILITY_LOOKBACK

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
        self.client = RESTClient(POLYGON_API_KEY) if POLYGON_API_KEY else None

    @retry_on_failure(max_retries=3, delay=1.0)
    async def fetch_ohlcv(self, symbol: str, limit: int = 5000) -> Optional[pd.DataFrame]:
        """Fetch OHLCV data from Polygon API with retry logic."""
        if not self.client:
            logger.error("Polygon API key not configured")
            return None

        def _fetch_sync():
            from datetime import datetime, timedelta
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            aggs_list = self.client.list_aggs(
                ticker=symbol,
                multiplier=1,
                timespan="minute",
                from_=start_date.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
                limit=limit
            )
            
            aggs = []
            for agg in aggs_list:
                aggs.append({
                    "timestamp": pd.Timestamp(agg.timestamp, unit="ms"),
                    "open": agg.open,
                    "high": agg.high,
                    "low": agg.low,
                    "close": agg.close,
                    "volume": agg.volume
                })
            return aggs

        aggs = await asyncio.to_thread(_fetch_sync)

        if not aggs:
            logger.warning(f"No data fetched for {symbol}")
            return None

        df = pd.DataFrame(aggs)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        return df

    async def get_correlations(self) -> pd.DataFrame:
        """Fetch Gold, DXY, US10Y and return correlation matrix with aligned timestamps."""
        tasks = [self.fetch_ohlcv(symbol) for symbol in SYMBOLS]
        dataframes = await asyncio.gather(*tasks)

        valid_data = [(symbol, df) for symbol, df in zip(SYMBOLS, dataframes) if df is not None and not df.empty]
        
        if len(valid_data) < 2:
            logger.error("Insufficient data for correlation calculation")
            return pd.DataFrame()

        closes = pd.DataFrame()
        for symbol, df in valid_data:
            closes[symbol] = df["close"]

        if closes.empty:
            return pd.DataFrame()

        closes = closes.resample('5T').last().dropna()
        
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
            "2_sigma_up": current_price + daily_range_2sigma,
            "1_sigma_up": current_price + daily_range_1sigma,
            "pivot": current_price,
            "1_sigma_down": current_price - daily_range_1sigma,
            "2_sigma_down": current_price - daily_range_2sigma,
            "annualized_volatility": annualized_vol
        }

        return levels

