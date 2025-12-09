import asyncio
import logging
import pandas as pd
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# CFTC COT Report URL - Futures Only (Disaggregated)
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
                logger.info(f"COT data fetched: {len(df)} rows, {len(df.columns)} columns")
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
            
            # Log available columns for debugging
            logger.info(f"COT columns available: {list(df.columns[:10])}...")
            
            # Find the market name column
            name_col = None
            for col in df.columns:
                if 'Market_and_Exchange_Names' in col or 'Market and Exchange Names' in col:
                    name_col = col
                    break
            
            if name_col is None:
                # Fallback: first column often contains market names
                name_col = df.columns[0]
            
            logger.info(f"Using name column: {name_col}")
            
            # Filter for Gold futures (COMEX Gold)
            gold_mask = df[name_col].str.contains('GOLD', case=False, na=False)
            gold_df = df[gold_mask]
            
            if gold_df.empty:
                logger.warning("Gold contract not found in COT data")
                logger.info(f"Sample market names: {df[name_col].head(10).tolist()}")
                return self._empty_positioning()
            
            logger.info(f"Found {len(gold_df)} Gold entries")
            
            # Get the latest entry (last row)
            latest = gold_df.iloc[-1]
            
            # Log the row for debugging
            logger.info(f"Gold market: {latest[name_col]}")
            
            # CFTC column names (standard format)
            # Try multiple possible column name formats
            column_mappings = {
                'noncomm_long': [
                    'NonComm_Positions_Long_All',
                    'Noncommercial Positions-Long (All)',
                    'Non-Commercial Long',
                ],
                'noncomm_short': [
                    'NonComm_Positions_Short_All', 
                    'Noncommercial Positions-Short (All)',
                    'Non-Commercial Short',
                ],
                'comm_long': [
                    'Comm_Positions_Long_All',
                    'Commercial Positions-Long (All)',
                    'Commercial Long',
                ],
                'comm_short': [
                    'Comm_Positions_Short_All',
                    'Commercial Positions-Short (All)',
                    'Commercial Short',
                ],
                'open_interest': [
                    'Open_Interest_All',
                    'Open Interest (All)',
                    'Open Interest',
                ],
                'report_date': [
                    'As_of_Date_In_Form_YYMMDD',
                    'Report_Date_as_YYYY-MM-DD',
                    'As of Date in Form YYMMDD',
                ]
            }
            
            def get_value(key_list, default=0):
                for key in key_list:
                    if key in latest.index:
                        val = latest[key]
                        if pd.notna(val):
                            try:
                                return int(float(val))
                            except:
                                return str(val)
                return default
            
            # Extract values
            noncomm_long = get_value(column_mappings['noncomm_long'])
            noncomm_short = get_value(column_mappings['noncomm_short'])
            comm_long = get_value(column_mappings['comm_long'])
            comm_short = get_value(column_mappings['comm_short'])
            open_interest = get_value(column_mappings['open_interest'])
            report_date = get_value(column_mappings['report_date'], 'Unknown')
            
            # Log extracted values
            logger.info(f"COT Values - NC Long: {noncomm_long}, NC Short: {noncomm_short}, OI: {open_interest}")
            
            # If still zero, try to find columns containing these keywords
            if noncomm_long == 0 and noncomm_short == 0:
                logger.warning("Standard columns not found, searching dynamically...")
                for col in latest.index:
                    col_lower = col.lower()
                    if 'noncomm' in col_lower and 'long' in col_lower and 'all' in col_lower:
                        val = latest[col]
                        if pd.notna(val) and val != 0:
                            noncomm_long = int(float(val))
                            logger.info(f"Found NC Long in column: {col} = {noncomm_long}")
                    if 'noncomm' in col_lower and 'short' in col_lower and 'all' in col_lower:
                        val = latest[col]
                        if pd.notna(val) and val != 0:
                            noncomm_short = int(float(val))
                            logger.info(f"Found NC Short in column: {col} = {noncomm_short}")
            
            noncomm_net = noncomm_long - noncomm_short
            comm_net = comm_long - comm_short
            
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
            elif noncomm_net < 0:
                spec_bias = "NET SHORT"
                bias_strength = "Strong" if abs(noncomm_net) > 100000 else "Moderate"
            else:
                spec_bias = "NEUTRAL"
                bias_strength = "N/A"
            
            result = {
                "report_date": str(report_date),
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
                "available": noncomm_long > 0 or noncomm_short > 0
            }
            
            logger.info(f"COT Result: {spec_bias} ({noncomm_net:,} contracts)")
            return result
            
        except Exception as e:
            logger.error(f"Error parsing COT data: {e}", exc_info=True)
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
