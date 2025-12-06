import asyncio
import logging
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# CFTC COT Report URLs
COT_LEGACY_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"


class COTAnalyzer:
    """Commitment of Traders data analyzer for Gold Futures positioning."""
    
    def __init__(self):
        self.gold_contract_code = "088691"  # CFTC code for Gold
    
    async def fetch_cot_data(self) -> Optional[pd.DataFrame]:
        """Fetch latest COT data from CFTC."""
        def _fetch_sync():
            try:
                df = pd.read_csv(
                    COT_LEGACY_URL,
                    low_memory=False
                )
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
            # Filter for Gold futures
            gold_df = df[df['CFTC_Contract_Market_Code'].astype(str) == self.gold_contract_code]
            
            if gold_df.empty:
                # Try alternative: search by name
                gold_df = df[df['Market_and_Exchange_Names'].str.contains('GOLD', case=False, na=False)]
            
            if gold_df.empty:
                logger.warning("Gold contract not found in COT data")
                return self._empty_positioning()
            
            latest = gold_df.iloc[-1]
            
            # Commercial (Hedgers) - typically producers/consumers
            comm_long = int(latest.get('Comm_Positions_Long_All', 0))
            comm_short = int(latest.get('Comm_Positions_Short_All', 0))
            comm_net = comm_long - comm_short
            
            # Non-Commercial (Speculators) - hedge funds, CTAs
            noncomm_long = int(latest.get('NonComm_Positions_Long_All', 0))
            noncomm_short = int(latest.get('NonComm_Positions_Short_All', 0))
            noncomm_net = noncomm_long - noncomm_short
            
            # Open Interest
            open_interest = int(latest.get('Open_Interest_All', 0))
            
            # Report date
            report_date = str(latest.get('As_of_Date_In_Form_YYMMDD', 'Unknown'))
            
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
                "report_date": report_date,
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

