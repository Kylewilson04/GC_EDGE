import asyncio
import logging
import numpy as np
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from functools import wraps
from zoneinfo import ZoneInfo
from src.config import SYMBOLS, VOLATILITY_LOOKBACK

logger = logging.getLogger(__name__)

# CME Gold Futures Session Times (US/Eastern)
CME_SESSION_START_HOUR = 18  # 6:00 PM ET (previous day)
CME_SESSION_END_HOUR = 17    # 5:00 PM ET (current day)
ET_TZ = ZoneInfo("America/New_York")


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
    """Market data fetcher with proper CME session alignment."""
    
    def __init__(self):
        pass

    def _get_last_completed_session(self, now_et: datetime) -> Tuple[datetime, datetime]:
        """
        Calculate the last completed CME session window.
        CME Session: 18:00 ET (Prev Day) to 17:00 ET (Current Day)
        """
        current_hour = now_et.hour
        
        if current_hour >= CME_SESSION_END_HOUR:
            # After 5 PM - last session ended today at 17:00
            session_end = now_et.replace(hour=CME_SESSION_END_HOUR, minute=0, second=0, microsecond=0)
            session_start = (now_et - timedelta(days=1)).replace(hour=CME_SESSION_START_HOUR, minute=0, second=0, microsecond=0)
        else:
            # Before 5 PM - last session ended yesterday at 17:00
            session_end = (now_et - timedelta(days=1)).replace(hour=CME_SESSION_END_HOUR, minute=0, second=0, microsecond=0)
            session_start = (now_et - timedelta(days=2)).replace(hour=CME_SESSION_START_HOUR, minute=0, second=0, microsecond=0)
        
        return session_start, session_end

    @retry_on_failure(max_retries=3, delay=1.0)
    async def fetch_session_ohlcv(self, symbol: str = "GC=F") -> Optional[Dict]:
        """
        Fetch and aggregate 5m data into a proper CME session candle.
        Returns dict with Open, High, Low, Close, Volume, VWAP for the last completed session.
        """
        def _fetch_and_aggregate():
            # Download 5-minute data for the past 5 days
            df = yf.download(tickers=symbol, interval="5m", period="5d", progress=False)
            
            if df is None or df.empty:
                logger.warning(f"No data fetched for {symbol}")
                return None
            
            # Handle MultiIndex columns (yfinance returns (Price, Ticker) format)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            # Standardize column names
            df.columns = df.columns.str.lower()
            
            # Forward fill NaN values
            df = df.ffill()
            
            # Convert index to Eastern Time
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            df.index = df.index.tz_convert("America/New_York")
            
            # Get current time in ET
            now_et = datetime.now(ET_TZ)
            
            # Calculate last completed session window
            session_start, session_end = self._get_last_completed_session(now_et)
            
            # Make session times timezone-aware
            session_start = session_start.replace(tzinfo=ET_TZ)
            session_end = session_end.replace(tzinfo=ET_TZ)
            
            logger.info(f"Session window: {session_start} to {session_end}")
            
            # Filter to session window
            session_df = df[(df.index >= session_start) & (df.index <= session_end)]
            
            if session_df.empty:
                logger.warning(f"No data in session window for {symbol}")
                return None
            
            # Aggregate session data
            session_open = float(session_df["open"].iloc[0])
            session_high = float(session_df["high"].max())
            session_low = float(session_df["low"].min())
            session_close = float(session_df["close"].iloc[-1])
            session_volume = float(session_df["volume"].sum())
            
            # Calculate VWAP: Sum(Typical Price * Volume) / Sum(Volume)
            typical_price = (session_df["high"] + session_df["low"] + session_df["close"]) / 3
            if session_volume > 0:
                vwap = float((typical_price * session_df["volume"]).sum() / session_volume)
            else:
                vwap = float(typical_price.mean())
            
            # Calculate Pivot Point (H + L + C) / 3
            pivot = (session_high + session_low + session_close) / 3
            
            # Get the actual close bar timestamp for verification
            close_bar_time = session_df.index[-1]
            open_bar_time = session_df.index[0]
            
            # Log verification data
            logger.info("=" * 40)
            logger.info("🔍 CME SESSION VERIFICATION DATA")
            logger.info("=" * 40)
            logger.info(f"  Session Date: {session_end.strftime('%Y-%m-%d')}")
            logger.info(f"  First Bar:  {open_bar_time} → Open: ${session_open:.2f}")
            logger.info(f"  Last Bar:   {close_bar_time} → Close: ${session_close:.2f}")
            logger.info(f"  High: ${session_high:.2f} | Low: ${session_low:.2f}")
            logger.info(f"  Pivot (H+L+C)/3: ${pivot:.2f}")
            logger.info("=" * 40)
            logger.info("⚠️  VERIFY: Compare 'Close' above with CME 'Prior Settle'")
            logger.info("   https://www.cmegroup.com/markets/metals/precious/gold.settlements.html")
            logger.info("=" * 40)
            
            return {
                "symbol": symbol,
                "session_start": session_start.isoformat(),
                "session_end": session_end.isoformat(),
                "session_date": session_end.strftime('%Y-%m-%d'),
                "open": round(session_open, 2),
                "high": round(session_high, 2),
                "low": round(session_low, 2),
                "close": round(session_close, 2),
                "volume": round(session_volume, 0),
                "vwap": round(vwap, 2),
                "pivot": round(pivot, 2),
                "bars_in_session": len(session_df),
                "first_bar_time": str(open_bar_time),
                "last_bar_time": str(close_bar_time)
            }
        
        return await asyncio.to_thread(_fetch_and_aggregate)

    @retry_on_failure(max_retries=3, delay=1.0)
    async def fetch_ohlcv(self, symbol: str, period: str = "1mo", interval: str = "1h") -> Optional[pd.DataFrame]:
        """Fetch OHLCV data from Yahoo Finance (legacy method for compatibility)."""
        def _fetch_sync():
            df = yf.download(tickers=symbol, interval=interval, period=period, progress=False)
            
            if df is None or df.empty:
                return None
            
            # Handle MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.columns = df.columns.str.lower()
            df = df.ffill()
            df.index.name = "timestamp"
            return df

        df = await asyncio.to_thread(_fetch_sync)

        if df is None or df.empty:
            logger.warning(f"No data fetched for {symbol}")
            return None

        return df

    async def get_correlations(self) -> pd.DataFrame:
        """Fetch Gold, DXY, US10Y and return correlation matrix with aligned timestamps."""
        tasks = [self.fetch_ohlcv(symbol, period="5d", interval="1h") for symbol in SYMBOLS.values()]
        dataframes = await asyncio.gather(*tasks)

        valid_data = [
            (name, df) 
            for (name, symbol), df in zip(SYMBOLS.items(), dataframes) 
            if df is not None and not df.empty
        ]
        
        if len(valid_data) < 2:
            logger.warning("Insufficient data for correlation calculation")
            return pd.DataFrame()

        closes = pd.DataFrame()
        for name, df in valid_data:
            closes[name.upper()] = df["close"]

        if closes.empty:
            return pd.DataFrame()

        # Align timestamps and drop NaN
        closes = closes.dropna()
        
        if len(closes) < 10:
            logger.warning(f"Insufficient aligned data points for correlation ({len(closes)} points)")
            return pd.DataFrame()

        correlation_matrix = closes.corr()
        return correlation_matrix

    def calc_volatility_levels(self, session_data: Dict) -> Dict[str, float]:
        """Calculate volatility-based sigma levels from session data."""
        if not session_data:
            logger.warning("No session data for volatility calculation")
            return {}

        session_high = session_data.get("high", 0)
        session_low = session_data.get("low", 0)
        session_close = session_data.get("close", 0)
        pivot = session_data.get("pivot", session_close)
        
        if session_close == 0:
            return {}
        
        # Calculate session range as volatility proxy
        session_range = session_high - session_low
        
        # Use session range to estimate 1-sigma move
        # Typically 1 sigma ≈ 0.5 * daily range for normal distribution
        sigma_1 = session_range * 0.5
        sigma_2 = session_range * 1.0
        
        # Alternative: ATR-based calculation using the range
        # This gives more realistic intraday levels
        
        levels = {
            "2_sigma_up": round(pivot + sigma_2, 1),
            "1_sigma_up": round(pivot + sigma_1, 1),
            "pivot": round(pivot, 1),
            "1_sigma_down": round(pivot - sigma_1, 1),
            "2_sigma_down": round(pivot - sigma_2, 1),
            "session_high": round(session_high, 1),
            "session_low": round(session_low, 1),
            "vwap": round(session_data.get("vwap", pivot), 1),
            "session_range": round(session_range, 1)
        }

        return levels

    def calc_volatility_levels_from_series(self, price_data: pd.Series) -> Dict[str, float]:
        """Calculate volatility-based sigma levels from price series (legacy method)."""
        if price_data.empty or len(price_data) < VOLATILITY_LOOKBACK:
            logger.warning("Insufficient data for volatility calculation")
            return {}

        daily_returns = price_data.pct_change().dropna()
        
        lookback = min(len(daily_returns), VOLATILITY_LOOKBACK)
        recent_returns = daily_returns.tail(lookback)
        daily_vol = np.std(recent_returns)
        annualized_vol = daily_vol * np.sqrt(252)

        current_price = float(price_data.iloc[-1])
        
        daily_range_1sigma = current_price * daily_vol
        daily_range_2sigma = current_price * (2 * daily_vol)

        levels = {
            "2_sigma_up": round(current_price + daily_range_2sigma, 1),
            "1_sigma_up": round(current_price + daily_range_1sigma, 1),
            "pivot": round(current_price, 1),
            "1_sigma_down": round(current_price - daily_range_1sigma, 1),
            "2_sigma_down": round(current_price - daily_range_2sigma, 1),
            "annualized_volatility": round(annualized_vol * 100, 2)
        }

        return levels
