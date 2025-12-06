import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# High-impact events for Gold
# Updated periodically - these are 2024-2025 key dates
# In production, you'd fetch from an API like Trading Economics or Forex Factory

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


class EconomicCalendar:
    """Economic calendar for high-impact events affecting Gold."""
    
    def __init__(self):
        self.events = self._build_event_calendar()
    
    def _build_event_calendar(self) -> Dict[str, Dict]:
        """Build a dictionary of high-impact events."""
        calendar = {}
        
        for date in FOMC_DATES_2025:
            calendar[date] = {
                "event": "FOMC Rate Decision",
                "impact": "HIGH",
                "asset_impact": "Gold extremely sensitive to rate decisions",
                "typical_move": "20-50+ points"
            }
        
        for date in NFP_DATES_2025:
            if date not in calendar:
                calendar[date] = {
                    "event": "Non-Farm Payrolls (NFP)",
                    "impact": "HIGH",
                    "asset_impact": "Strong jobs = USD up = Gold down (typically)",
                    "typical_move": "15-30 points"
                }
        
        for date in CPI_DATES_2025:
            if date not in calendar:
                calendar[date] = {
                    "event": "CPI Inflation Data",
                    "impact": "HIGH",
                    "asset_impact": "Hot CPI = hawkish Fed = Gold pressure",
                    "typical_move": "15-40 points"
                }
        
        return calendar
    
    def get_upcoming_events(self, days_ahead: int = 7) -> List[Dict]:
        """Get high-impact events in the next N days."""
        today = datetime.now().date()
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
        today = datetime.now().date().strftime("%Y-%m-%d")
        tomorrow = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        result = {
            "today_event": None,
            "tomorrow_event": None,
            "risk_warning": None
        }
        
        if today in self.events:
            result["today_event"] = self.events[today]
            result["risk_warning"] = f"⚠️ HIGH IMPACT TODAY: {self.events[today]['event']}"
        
        if tomorrow in self.events:
            result["tomorrow_event"] = self.events[tomorrow]
            if not result["risk_warning"]:
                result["risk_warning"] = f"📅 Tomorrow: {self.events[tomorrow]['event']}"
        
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
            "next_major_event": next_event,
            "events_this_week": len(upcoming)
        }

