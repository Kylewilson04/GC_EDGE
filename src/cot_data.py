import asyncio
import logging
import pandas as pd
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# CFTC COT Report URL - Futures Only
COT_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"


class COTAnalyzer:
    """Commitment of Traders data analyzer for Gold Futures positioning."""
    
    def __init__(self):
        pass
    
    async def fetch_cot_data(self) -> Optional[pd.DataFrame]:
        """Fetch latest COT data from CFTC."""
        def _fetch_sync():
            try:
                df = pd.read_csv(COT_URL, low_memory=False)
                return df
            except Exception as e:
                logger.error(f"Error fetching COT data: {e}")
                return None
        
        return await asyncio.to_thread(_fetch_sync)
    
    async def get_gold_positioning(self) -> Dict:
        """Get Gold futures positioning from COT report."""
        df = await self.fetch_cot_data()
        
        if df is None or df.empty:
            logger.warning("No COT data available")
            return self._empty_positioning()
        
        try:
            # Clean column names (remove extra spaces)
            df.columns = df.columns.str.strip()
            
            # Find Gold - search in market name column
            name_col = None
            for col in df.columns:
                if 'Market' in col and 'Name' in col:
                    name_col = col
                    break
            
            if name_col is None:
                name_col = df.columns[0]
            
            # Filter for Gold futures
            gold_df = df[df[name_col].str.contains('GOLD', case=False, na=False)]
            
            if gold_df.empty:
                logger.warning("Gold contract not found in COT data")
                return self._empty_positioning()
            
            latest = gold_df.iloc[-1]
            
            # Find correct column names dynamically
            def find_col(patterns):
                for col in df.columns:
                    col_lower = col.lower()
                    if all(p.lower() in col_lower for p in patterns):
                        return col
                return None
            
            # Non-Commercial (Speculators)
            nc_long_col = find_col(['noncomm', 'long']) or find_col(['non-commercial', 'long'])
            nc_short_col = find_col(['noncomm', 'short']) or find_col(['non-commercial', 'short'])
            
            # Commercial (Hedgers)  
            comm_long_col = find_col(['comm', 'long'])
            comm_short_col = find_col(['comm', 'short'])
            
            # Open Interest
            oi_col = find_col(['open', 'interest'])
            
            # Extract values with fallbacks
            noncomm_long = int(latest.get(nc_long_col, 0)) if nc_long_col else 0
            noncomm_short = int(latest.get(nc_short_col, 0)) if nc_short_col else 0
            noncomm_net = noncomm_long - noncomm_short
            
            comm_long = int(latest.get(comm_long_col, 0)) if comm_long_col else 0
            comm_short = int(latest.get(comm_short_col, 0)) if comm_short_col else 0
            comm_net = comm_long - comm_short
            
            open_interest = int(latest.get(oi_col, 0)) if oi_col else 0
            
            # Calculate percentages
            if open_interest > 0:
                noncomm_long_pct = round((noncomm_long / open_interest) * 100, 1)
                noncomm_short_pct = round((noncomm_short / open_interest) * 100, 1)
            else:
                noncomm_long_pct = 0
                noncomm_short_pct = 0
            
            # Determine bias
            if noncomm_net > 0:
                spec_bias = "NET LONG"
                bias_strength = "Strong" if abs(noncomm_net) > 100000 else "Moderate"
            else:
                spec_bias = "NET SHORT"
                bias_strength = "Strong" if abs(noncomm_net) > 100000 else "Moderate"
            
            return {
                "report_date": "Latest",
                "speculators": {
                    "long": noncomm_long,
                    "short": noncomm_short,
                    "net": noncomm_net,
                    "long_pct": noncomm_long_pct,
                    "short_pct": noncomm_short_pct,
                    "bias": spec_bias,
                    "strength": bias_strength
                },
                "commercials": {
                    "long": comm_long,
                    "short": comm_short,
                    "net": comm_net
                },
                "open_interest": open_interest,
                "available": True
            }
            
        except Exception as e:
            logger.error(f"Error parsing COT data: {e}")
            return self._empty_positioning()
    
    def _empty_positioning(self) -> Dict:
        """Return empty positioning structure."""
        return {
            "report_date": "N/A",
            "speculators": {
                "long": 0,
                "short": 0,
                "net": 0,
                "long_pct": 0,
                "short_pct": 0,
                "bias": "UNKNOWN",
                "strength": "N/A"
            },
            "commercials": {
                "long": 0,
                "short": 0,
                "net": 0
            },
            "open_interest": 0,
            "available": False
        }
