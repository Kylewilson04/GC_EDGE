import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

ET_TZ = ZoneInfo("America/New_York")

# High-impact events for Gold (2025)
# Event codes must match: CPI, NFP, FOMC_RATE_DECISION, PCE_CORE

FOMC_DATES_2025 = [
    "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
    "2025-07-30", "2025-09-17", "2025-11-05", "2025-12-17"
]

NFP_DATES_2025 = [
    "2025-01-10", "2025-02-07", "2025-03-07", "2025-04-04",
    "2025-05-02", "2025-06-06", "2025-07-03", "2025-08-01",
    "2025-09-05", "2025-10-03", "2025-11-07", "2025-12-05"
]

CPI_DATES_2025 = [
    "2025-01-15", "2025-02-12", "2025-03-12", "2025-04-10",
    "2025-05-13", "2025-06-11", "2025-07-11", "2025-08-12",
    "2025-09-11", "2025-10-10", "2025-11-13", "2025-12-10"
]

PCE_DATES_2025 = [
    "2025-01-31", "2025-02-28", "2025-03-28", "2025-04-25",
    "2025-05-30", "2025-06-27", "2025-07-25", "2025-08-29",
    "2025-09-26", "2025-10-31", "2025-11-26", "2025-12-19"
]

# Event Volatility Multipliers (K-Factors)
# Derived from Q4 2024-2025 Gold ADR analysis
# Normal Day ADR: ~1.2%, Event ADR: ~2.3%
EVENT_K_FACTORS = {
    "CPI": 1.92,                # Inflation shock - Fat tail multiplier
    "NFP": 1.55,                # Jobs data - softer impact
    "FOMC_RATE_DECISION": 2.10, # Fed Rate - Maximum expansion
    "PCE_CORE": 1.92            # CPI proxy - same multiplier
}


class EconomicCalendar:
    """Economic calendar for high-impact events affecting Gold."""
    
    def __init__(self):
        self.events = self._build_event_calendar()
    
    def _build_event_calendar(self) -> Dict[str, Dict]:
        """Build a dictionary of high-impact events with event codes."""
        calendar = {}
        
        for date in FOMC_DATES_2025:
            calendar[date] = {
                "event": "FOMC Rate Decision",
                "event_code": "FOMC_RATE_DECISION",
                "impact": "EXTREME",
                "k_factor": EVENT_K_FACTORS["FOMC_RATE_DECISION"],
                "asset_impact": "Gold extremely sensitive to rate decisions",
                "typical_move": "30-60+ points"
            }
        
        for date in NFP_DATES_2025:
            if date not in calendar:
                calendar[date] = {
                    "event": "Non-Farm Payrolls (NFP)",
                    "event_code": "NFP",
                    "impact": "HIGH",
                    "k_factor": EVENT_K_FACTORS["NFP"],
                    "asset_impact": "Strong jobs = USD up = Gold down (typically)",
                    "typical_move": "20-40 points"
                }
        
        for date in CPI_DATES_2025:
            if date not in calendar:
                calendar[date] = {
                    "event": "CPI Inflation Data",
                    "event_code": "CPI",
                    "impact": "EXTREME",
                    "k_factor": EVENT_K_FACTORS["CPI"],
                    "asset_impact": "Hot CPI = hawkish Fed = Gold pressure",
                    "typical_move": "25-50 points"
                }
        
        for date in PCE_DATES_2025:
            if date not in calendar:
                calendar[date] = {
                    "event": "PCE Core Inflation",
                    "event_code": "PCE_CORE",
                    "impact": "HIGH",
                    "k_factor": EVENT_K_FACTORS["PCE_CORE"],
                    "asset_impact": "Fed's preferred inflation gauge",
                    "typical_move": "20-40 points"
                }
        
        return calendar
    
    def get_upcoming_events(self, days_ahead: int = 7) -> List[Dict]:
        """Get high-impact events in the next N days."""
        today = datetime.now(ET_TZ).date()
        upcoming = []
        
        for i in range(days_ahead + 1):
            check_date = (today + timedelta(days=i)).strftime("%Y-%m-%d")
            if check_date in self.events:
                event = self.events[check_date].copy()
                event["date"] = check_date
                event["days_until"] = i
                upcoming.append(event)
        
        return upcoming
    
    def is_high_impact_day(self) -> Dict:
        """Check if today or tomorrow is a high-impact event day."""
        today = datetime.now(ET_TZ).date().strftime("%Y-%m-%d")
        tomorrow = (datetime.now(ET_TZ).date() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        result = {
            "today_event": None,
            "tomorrow_event": None,
            "risk_warning": None,
            "event_code": None,
            "k_factor": None
        }
        
        if today in self.events:
            result["today_event"] = self.events[today]
            result["event_code"] = self.events[today]["event_code"]
            result["k_factor"] = self.events[today]["k_factor"]
            result["risk_warning"] = f"🚨 EVENT DAY: {self.events[today]['event']} (K={self.events[today]['k_factor']}x)"
        
        if tomorrow in self.events:
            result["tomorrow_event"] = self.events[tomorrow]
            if not result["risk_warning"]:
                result["risk_warning"] = f"⚠️ Tomorrow: {self.events[tomorrow]['event']} (K={self.events[tomorrow]['k_factor']}x)"
        
        return result
    
    def get_event_context(self) -> Dict:
        """Get full event context for report generation."""
        upcoming = self.get_upcoming_events(days_ahead=7)
        today_check = self.is_high_impact_day()
        
        # Find next major event
        next_event = upcoming[0] if upcoming else None
        
        return {
            "upcoming_events": upcoming,
            "today_event": today_check["today_event"],
            "tomorrow_event": today_check["tomorrow_event"],
            "risk_warning": today_check["risk_warning"],
            "event_code": today_check["event_code"],
            "k_factor": today_check["k_factor"],
            "next_major_event": next_event,
            "events_this_week": len(upcoming),
            "is_event_day": today_check["event_code"] is not None
        }
    
    def get_event_volatility_bands(self, current_price: float, daily_atr: float, event_code: str) -> Optional[Dict]:
        """
        Calculate expanded volatility bands for high-impact events.
        Standard deviation bands are insufficient during liquidity shocks.
        
        CRITICAL: Only triggers for CPI, NFP, FOMC, PCE events.
        """
        HIGH_IMPACT_EVENTS = ["CPI", "NFP", "FOMC_RATE_DECISION", "PCE_CORE"]
        
        if event_code not in HIGH_IMPACT_EVENTS:
            return None  # Return None for standard bands
        
        multiplier = EVENT_K_FACTORS.get(event_code, 1.0)
        implied_move = daily_atr * multiplier
        
        upper_band_extreme = current_price + implied_move
        lower_band_extreme = current_price - implied_move
        
        return {
            "regime": f"EVENT_VOLATILITY ({event_code})",
            "event_code": event_code,
            "multiplier_used": multiplier,
            "normal_atr": round(daily_atr, 1),
            "expanded_range": round(implied_move, 1),
            "upper_band": round(upper_band_extreme, 1),
            "lower_band": round(lower_band_extreme, 1),
            "note": "⚠️ Liquidity gaps likely. Use expanded bands for stops."
        }
