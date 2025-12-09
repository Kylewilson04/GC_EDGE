"""
GhostKing Protocol - Macro Liquidity & Regime Analysis for ES1! Trading

This module fetches macro data, calculates proprietary indicators,
and generates a "Daily Battle Plan" for trading S&P 500 Futures.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

try:
    import pandas_datareader.data as web
    FRED_AVAILABLE = True
except ImportError:
    FRED_AVAILABLE = False
    
logger = logging.getLogger(__name__)
ET_TZ = ZoneInfo("America/New_York")

# Yahoo Finance tickers for yields
YIELD_SYMBOLS = {
    "US10Y": "^TNX",    # 10-Year Treasury Yield
    "US02Y": "^IRX",    # 2-Year approximation (13-week T-Bill) - note: ^TYX is 30Y
}

# FRED series for liquidity data
FRED_SERIES = {
    "WALCL": "WALCL",         # Federal Reserve Total Assets
    "WTREGEN": "WTREGEN",     # Treasury General Account
    "RRPONTSYD": "RRPONTSYD", # Overnight Reverse Repo
}

# ES Futures symbol
ES_SYMBOL = "ES=F"


class GhostKingProtocol:
    """
    Macro Liquidity Regime Analysis Engine.
    
    Calculates:
    - Yield Curve Spread (US10Y - US02Y)
    - Net Liquidity (WALCL - WTREGEN - RRPONTSYD)
    - Liquidity Trend (20-day EMA comparison)
    
    Determines regime and generates actionable trading plan.
    """
    
    def __init__(self, liquidity_ema_period: int = 20):
        self.liquidity_ema_period = liquidity_ema_period
        self._cache = {}
        
    async def fetch_yield_data(self) -> Dict[str, Optional[float]]:
        """Fetch current Treasury yields from Yahoo Finance."""
        def _fetch_sync():
            yields = {}
            
            try:
                # Fetch 10-Year yield
                us10y = yf.download("^TNX", period="5d", interval="1d", progress=False)
                if us10y is not None and not us10y.empty:
                    if isinstance(us10y.columns, pd.MultiIndex):
                        us10y.columns = us10y.columns.get_level_values(0)
                    yields["US10Y"] = float(us10y["Close"].iloc[-1])
                else:
                    yields["US10Y"] = None
                    
                # Fetch 2-Year yield (^IRX is 13-week, use different approach)
                # Try fetching 2-Year from alternate source
                us02y = yf.download("2YY=F", period="5d", interval="1d", progress=False)
                if us02y is not None and not us02y.empty:
                    if isinstance(us02y.columns, pd.MultiIndex):
                        us02y.columns = us02y.columns.get_level_values(0)
                    yields["US02Y"] = float(us02y["Close"].iloc[-1])
                else:
                    # Fallback: try ^IRX and adjust
                    irx = yf.download("^IRX", period="5d", interval="1d", progress=False)
                    if irx is not None and not irx.empty:
                        if isinstance(irx.columns, pd.MultiIndex):
                            irx.columns = irx.columns.get_level_values(0)
                        # Use 13-week as proxy (typically lower than 2Y)
                        yields["US02Y"] = float(irx["Close"].iloc[-1])
                    else:
                        yields["US02Y"] = None
                        
            except Exception as e:
                logger.error(f"Error fetching yield data: {e}")
                yields["US10Y"] = None
                yields["US02Y"] = None
                
            return yields
            
        return await asyncio.to_thread(_fetch_sync)
    
    async def fetch_fred_data(self) -> Dict[str, Optional[pd.Series]]:
        """
        Fetch FRED liquidity data series.
        Returns dict with series data for WALCL, WTREGEN, RRPONTSYD.
        """
        if not FRED_AVAILABLE:
            logger.warning("pandas_datareader not available - using fallback liquidity estimates")
            return self._get_fallback_liquidity_data()
            
        def _fetch_sync():
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)  # 90 days for EMA calculation
            
            data = {}
            
            for name, series_id in FRED_SERIES.items():
                try:
                    df = web.DataReader(series_id, "fred", start_date, end_date)
                    if df is not None and not df.empty:
                        # FRED data is in millions, convert to trillions for readability
                        data[name] = df[series_id] / 1_000_000  # Now in trillions
                    else:
                        data[name] = None
                except Exception as e:
                    logger.warning(f"Failed to fetch FRED series {series_id}: {e}")
                    data[name] = None
                    
            return data
            
        return await asyncio.to_thread(_fetch_sync)
    
    def _get_fallback_liquidity_data(self) -> Dict[str, Optional[pd.Series]]:
        """
        Provide approximate fallback values when FRED data unavailable.
        These are rough estimates based on typical ranges.
        """
        logger.info("Using fallback liquidity estimates (FRED unavailable)")
        
        # Create a simple series with approximate current values (in trillions)
        dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
        
        # Approximate values as of late 2024 (in trillions)
        walcl_approx = 6.9  # Fed balance sheet ~$6.9T
        wtregen_approx = 0.8  # TGA ~$800B
        rrp_approx = 0.2  # RRP has dropped significantly
        
        return {
            "WALCL": pd.Series([walcl_approx] * 30, index=dates),
            "WTREGEN": pd.Series([wtregen_approx] * 30, index=dates),
            "RRPONTSYD": pd.Series([rrp_approx] * 30, index=dates),
        }
    
    async def fetch_es_price(self) -> Optional[float]:
        """Fetch current ES1! (S&P 500 Futures) price."""
        def _fetch_sync():
            try:
                es = yf.download(ES_SYMBOL, period="5d", interval="1d", progress=False)
                if es is not None and not es.empty:
                    if isinstance(es.columns, pd.MultiIndex):
                        es.columns = es.columns.get_level_values(0)
                    return float(es["Close"].iloc[-1])
            except Exception as e:
                logger.error(f"Error fetching ES price: {e}")
            return None
            
        return await asyncio.to_thread(_fetch_sync)
    
    def calculate_yield_curve_spread(self, yields: Dict[str, Optional[float]]) -> Optional[float]:
        """Calculate Yield Curve Spread: US10Y - US02Y"""
        us10y = yields.get("US10Y")
        us02y = yields.get("US02Y")
        
        if us10y is not None and us02y is not None:
            return us10y - us02y
        return None
    
    def calculate_net_liquidity(
        self, 
        walcl: Optional[pd.Series], 
        wtregen: Optional[pd.Series], 
        rrp: Optional[pd.Series]
    ) -> Tuple[Optional[pd.Series], Optional[float], Optional[float], str]:
        """
        Calculate Net Liquidity: WALCL - WTREGEN - RRPONTSYD
        
        Returns:
            - net_liquidity_series: Full time series
            - current_net_liquidity: Latest value
            - liquidity_ema: 20-day EMA
            - trend: "Rising" or "Falling"
        """
        if walcl is None or wtregen is None or rrp is None:
            return None, None, None, "UNKNOWN"
            
        # Align indices
        common_dates = walcl.index.intersection(wtregen.index).intersection(rrp.index)
        
        if len(common_dates) == 0:
            return None, None, None, "UNKNOWN"
            
        walcl_aligned = walcl.loc[common_dates].ffill()
        wtregen_aligned = wtregen.loc[common_dates].ffill()
        rrp_aligned = rrp.loc[common_dates].ffill()
        
        # Calculate net liquidity
        net_liquidity = walcl_aligned - wtregen_aligned - rrp_aligned
        
        if net_liquidity.empty:
            return None, None, None, "UNKNOWN"
            
        current_net_liquidity = float(net_liquidity.iloc[-1])
        
        # Calculate EMA
        if len(net_liquidity) >= self.liquidity_ema_period:
            liquidity_ema = net_liquidity.ewm(span=self.liquidity_ema_period, adjust=False).mean()
            current_ema = float(liquidity_ema.iloc[-1])
            
            # Determine trend by comparing current vs EMA and slope
            ema_slope = liquidity_ema.iloc[-1] - liquidity_ema.iloc[-5] if len(liquidity_ema) >= 5 else 0
            
            if current_net_liquidity > current_ema and ema_slope > 0:
                trend = "Rising"
            elif current_net_liquidity < current_ema and ema_slope < 0:
                trend = "Falling"
            else:
                trend = "Flat"
        else:
            current_ema = current_net_liquidity
            trend = "INSUFFICIENT_DATA"
            
        return net_liquidity, current_net_liquidity, current_ema, trend
    
    def determine_regime(
        self, 
        yield_curve_spread: Optional[float],
        net_liquidity: Optional[float],
        liquidity_ema: Optional[float],
        liquidity_trend: str
    ) -> Dict[str, str]:
        """
        Determine the macro regime state.
        
        Returns:
            {
                "macro_state": "RED (RECESSION WARNING)" or "BLUE (NORMAL)",
                "liquidity_bias": "BEARISH (FUEL CUT)" or "BULLISH (FUEL PUMPING)" or "NEUTRAL/CHOP",
                "yield_curve_status": "INVERTED" or "NORMAL",
                "combined_signal": summary
            }
        """
        regime = {}
        
        # Macro State based on yield curve
        if yield_curve_spread is not None:
            if yield_curve_spread < 0:
                regime["macro_state"] = "RED (RECESSION WARNING)"
                regime["yield_curve_status"] = "INVERTED"
            else:
                regime["macro_state"] = "BLUE (NORMAL)"
                regime["yield_curve_status"] = "NORMAL"
        else:
            regime["macro_state"] = "UNKNOWN"
            regime["yield_curve_status"] = "UNKNOWN"
            
        # Liquidity Bias
        if net_liquidity is not None and liquidity_ema is not None:
            if net_liquidity < liquidity_ema and liquidity_trend in ["Falling", "Flat"]:
                regime["liquidity_bias"] = "BEARISH (FUEL CUT)"
            elif net_liquidity > liquidity_ema and liquidity_trend == "Rising":
                regime["liquidity_bias"] = "BULLISH (FUEL PUMPING)"
            else:
                regime["liquidity_bias"] = "NEUTRAL/CHOP"
        else:
            regime["liquidity_bias"] = "NEUTRAL/CHOP"
            
        # Combined signal
        if regime["liquidity_bias"] == "BEARISH (FUEL CUT)":
            if regime["macro_state"] == "RED (RECESSION WARNING)":
                regime["combined_signal"] = "⚠️ MAXIMUM CAUTION - Bear regime + Recession signal"
            else:
                regime["combined_signal"] = "🔻 SHORT BIAS - Liquidity contracting"
        elif regime["liquidity_bias"] == "BULLISH (FUEL PUMPING)":
            if regime["macro_state"] == "RED (RECESSION WARNING)":
                regime["combined_signal"] = "⚡ CONFLICTING - Liquidity up but curve inverted"
            else:
                regime["combined_signal"] = "🔺 LONG BIAS - Full risk-on conditions"
        else:
            regime["combined_signal"] = "⏸️ NEUTRAL - Range-bound conditions expected"
            
        return regime
    
    def generate_battle_plan(self, regime: Dict[str, str]) -> str:
        """
        Generate the Daily Battle Plan based on regime analysis.
        Hardcoded Ghost King trading logic.
        """
        liquidity_bias = regime.get("liquidity_bias", "NEUTRAL/CHOP")
        
        if "BEARISH" in liquidity_bias:
            return """**STRATEGY: THE TRAP & DROP** 💀

> * **The Bias:** SHORT. Do not chase Longs. The fuel line is cut.
> * **The Setup:** Watch for price to rally into the High Volume Node (Resistance). If Liquidity Line remains flat/down during the rally, initiate SHORT.
> * **The Kill Switch:** If Price > POC (Point of Control) AND Liquidity makes a Higher High, stand down.
> * **Execution:** Target the "Air Pocket" (Low Volume Node) below current price for the flush."""

        elif "BULLISH" in liquidity_bias:
            return """**STRATEGY: RIDE THE WAVE** 🌊

> * **The Bias:** LONG. Buying dips is permitted.
> * **The Setup:** Wait for price to reclaim the POC (Point of Control).
> * **Execution:** Long on retest of POC. Target the next High Volume Node above."""

        else:  # NEUTRAL/CHOP
            return """**STRATEGY: KILL ZONE** ⚔️

> * **The Bias:** FLAT. Do not swing trade.
> * **The Setup:** Range bound trade only. Short the top of the Volume Node, Long the bottom.
> * **Warning:** Expect choppy price action. Tight stops mandatory."""

    async def run_analysis(self) -> Dict:
        """
        Execute full GhostKing Protocol analysis.
        
        Returns comprehensive analysis dict with all data and battle plan.
        """
        logger.info("=" * 50)
        logger.info("👻 GHOSTKING PROTOCOL: Initiating Analysis")
        logger.info("=" * 50)
        
        # Fetch all data in parallel
        yield_task = self.fetch_yield_data()
        fred_task = self.fetch_fred_data()
        es_task = self.fetch_es_price()
        
        yields, fred_data, es_price = await asyncio.gather(yield_task, fred_task, es_task)
        
        # Log data retrieval status
        logger.info(f"  ✓ US10Y: {yields.get('US10Y', 'N/A')}")
        logger.info(f"  ✓ US02Y: {yields.get('US02Y', 'N/A')}")
        logger.info(f"  ✓ ES Price: ${es_price:,.2f}" if es_price else "  ⚠ ES Price unavailable")
        
        # Calculate Yield Curve Spread
        yield_curve_spread = self.calculate_yield_curve_spread(yields)
        if yield_curve_spread is not None:
            logger.info(f"  ✓ Yield Curve Spread: {yield_curve_spread:.3f}%")
        
        # Calculate Net Liquidity
        net_liquidity_series, current_net_liquidity, liquidity_ema, liquidity_trend = \
            self.calculate_net_liquidity(
                fred_data.get("WALCL"),
                fred_data.get("WTREGEN"),
                fred_data.get("RRPONTSYD")
            )
            
        if current_net_liquidity is not None:
            logger.info(f"  ✓ Net Liquidity: ${current_net_liquidity:.2f}T")
            logger.info(f"  ✓ Liquidity EMA: ${liquidity_ema:.2f}T")
            logger.info(f"  ✓ Liquidity Trend: {liquidity_trend}")
        
        # Determine Regime
        regime = self.determine_regime(
            yield_curve_spread,
            current_net_liquidity,
            liquidity_ema,
            liquidity_trend
        )
        
        logger.info(f"  ✓ Macro State: {regime['macro_state']}")
        logger.info(f"  ✓ Liquidity Bias: {regime['liquidity_bias']}")
        logger.info(f"  ✓ Combined Signal: {regime['combined_signal']}")
        
        # Generate Battle Plan
        battle_plan = self.generate_battle_plan(regime)
        
        logger.info("👻 GHOSTKING PROTOCOL: Analysis Complete")
        logger.info("=" * 50)
        
        return {
            "timestamp": datetime.now(ET_TZ).strftime("%Y-%m-%d"),
            "yields": {
                "US10Y": yields.get("US10Y"),
                "US02Y": yields.get("US02Y"),
                "spread": yield_curve_spread
            },
            "liquidity": {
                "walcl": float(fred_data["WALCL"].iloc[-1]) if fred_data.get("WALCL") is not None else None,
                "wtregen": float(fred_data["WTREGEN"].iloc[-1]) if fred_data.get("WTREGEN") is not None else None,
                "rrp": float(fred_data["RRPONTSYD"].iloc[-1]) if fred_data.get("RRPONTSYD") is not None else None,
                "net_liquidity": current_net_liquidity,
                "ema_20": liquidity_ema,
                "trend": liquidity_trend
            },
            "es_price": es_price,
            "regime": regime,
            "battle_plan": battle_plan
        }
    
    def format_report(self, analysis: Dict) -> str:
        """
        Format the analysis into a clean Markdown report.
        """
        timestamp = analysis.get("timestamp", datetime.now(ET_TZ).strftime("%Y-%m-%d"))
        
        # Yield Curve Data
        yields = analysis.get("yields", {})
        spread = yields.get("spread")
        spread_str = f"{spread:.3f}%" if spread is not None else "N/A"
        
        regime = analysis.get("regime", {})
        macro_state = regime.get("macro_state", "UNKNOWN")
        
        # Liquidity Data
        liquidity = analysis.get("liquidity", {})
        net_liq = liquidity.get("net_liquidity")
        net_liq_str = f"${net_liq:.2f}" if net_liq is not None else "N/A"
        trend = liquidity.get("trend", "UNKNOWN")
        
        # Battle Plan
        battle_plan = analysis.get("battle_plan", "Analysis unavailable")
        
        # Combined Signal emoji
        combined_signal = regime.get("combined_signal", "")
        
        report = f"""
---
## 👻 GHOST KING PROTOCOL: {timestamp}

**Engine (Yield Curve):** {spread_str} ({macro_state})
**Fuel (Net Liquidity):** {net_liq_str} Trillion ({trend})

**Signal:** {combined_signal}

### 💀 DAILY BATTLE PLAN

{battle_plan}

---
*GhostKing Protocol v1.0 | ES1! Macro Regime Analysis*
"""
        return report


async def get_ghostking_report() -> str:
    """
    Convenience function to run analysis and get formatted report.
    """
    protocol = GhostKingProtocol()
    analysis = await protocol.run_analysis()
    return protocol.format_report(analysis)


async def get_ghostking_analysis() -> Dict:
    """
    Convenience function to run analysis and get raw data dict.
    """
    protocol = GhostKingProtocol()
    return await protocol.run_analysis()

