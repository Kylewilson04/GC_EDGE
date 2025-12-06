import logging
import numpy as np
import pandas as pd
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class LocalAnalyst:
    def __init__(self):
        pass

    def analyze_market_structure(self, df: pd.DataFrame, bins: int = 50) -> Dict[str, float]:
        """Identify VPOC (Volume Point of Control) via volume profile histogram."""
        if df.empty or "volume" not in df.columns or "close" not in df.columns:
            logger.warning("Insufficient data for VPOC analysis")
            return {"vpoc": None}

        price_min = df["close"].min()
        price_max = df["close"].max()
        
        if price_min == price_max:
            logger.warning("No price variation for VPOC calculation")
            return {"vpoc": float(price_min)}

        price_bins = np.linspace(price_min, price_max, bins + 1)
        df["price_bin"] = pd.cut(df["close"], bins=price_bins, include_lowest=True)
        
        volume_profile = df.groupby("price_bin", observed=True)["volume"].sum()
        
        if volume_profile.empty:
            return {"vpoc": float(df["close"].iloc[-1])}

        vpoc_bin = volume_profile.idxmax()
        vpoc_price = vpoc_bin.mid if hasattr(vpoc_bin, 'mid') else float(vpoc_bin.left + vpoc_bin.right) / 2

        return {
            "vpoc": float(vpoc_price),
            "max_volume": float(volume_profile.max())
        }

    def get_market_regime(self, df: pd.DataFrame, period: int = 20) -> str:
        """Determine market regime: Trend, Balance, or Compressed."""
        if df.empty or len(df) < period:
            logger.warning("Insufficient data for regime analysis")
            return "Unknown"

        if "close" not in df.columns or "high" not in df.columns or "low" not in df.columns:
            logger.warning("Missing required columns for regime analysis")
            return "Unknown"

        close = df["close"]
        high = df["high"]
        low = df["low"]

        atr_values = []
        for i in range(period, len(df)):
            tr1 = high.iloc[i] - low.iloc[i]
            tr2 = abs(high.iloc[i] - close.iloc[i-1])
            tr3 = abs(low.iloc[i] - close.iloc[i-1])
            atr = max(tr1, tr2, tr3)
            atr_values.append(atr)

        if not atr_values:
            return "Unknown"

        atr = np.mean(atr_values)
        current_atr = atr_values[-1] if atr_values else atr

        sma = close.rolling(window=period).mean()
        std = close.rolling(window=period).std()
        
        if len(sma) < period or len(std) < period:
            return "Unknown"

        bb_width = (2 * std.iloc[-1]) / sma.iloc[-1] if sma.iloc[-1] != 0 else 0

        atr_ratio = current_atr / atr if atr > 0 else 1.0

        if bb_width < 0.02 and atr_ratio < 0.8:
            return "Compressed"
        elif atr_ratio > 1.2:
            return "Trend"
        else:
            return "Balance"

