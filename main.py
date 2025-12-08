import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from src.config import SYMBOLS
from src.data_engine import MarketData
from src.analysis_engine import LocalAnalyst
from src.cot_data import COTAnalyzer
from src.economic_calendar import EconomicCalendar
from src.llm_synthesis import ReasoningCore
from src.messenger import DiscordBot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Timezone for scheduling
ET_TZ = ZoneInfo("America/New_York")

# Target execution time (05:00 ET Pre-Market)
TARGET_HOUR = 5
TARGET_MINUTE = 0


def get_seconds_until_target(target_hour: int = TARGET_HOUR, target_minute: int = TARGET_MINUTE) -> float:
    """Calculate seconds until the next valid trading day at target time (05:00 ET)."""
    now = datetime.now(ET_TZ)
    
    # Create target time for today
    target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    # If we've already passed today's target, schedule for tomorrow
    if now >= target_time:
        target_time = target_time + timedelta(days=1)
    
    # Skip Saturday only (Saturday=5)
    # CME Gold Futures: Friday session ends 17:00 ET, reopens Sunday 18:00 ET
    # Sunday morning report reviews Friday's session before markets reopen
    # Reports run: Sunday, Monday, Tuesday, Wednesday, Thursday, Friday
    while target_time.weekday() == 5:  # Saturday only
        target_time = target_time + timedelta(days=1)
    
    seconds_until = (target_time - now).total_seconds()
    return seconds_until, target_time


async def run_pipeline():
    """Execute the full trading intelligence pipeline."""
    logger.info("=" * 50)
    logger.info("Starting Gold_Sovereign_AI pipeline")
    logger.info("=" * 50)

    try:
        # Initialize all components
        data_engine = MarketData()
        analyst = LocalAnalyst()
        cot_analyzer = COTAnalyzer()
        calendar = EconomicCalendar()
        llm = ReasoningCore()
        messenger = DiscordBot()

        # === DATA LAYER ===
        logger.info("[1/6] Fetching CME session data...")
        gold_symbol = SYMBOLS["gold"]
        
        # Fetch proper CME session-aligned data
        session_data = await data_engine.fetch_session_ohlcv(gold_symbol)
        
        if session_data is None:
            logger.error("Failed to fetch gold session data - aborting pipeline")
            return False

        logger.info(f"  ✓ Session: {session_data['session_start']} to {session_data['session_end']}")
        logger.info(f"  ✓ OHLC: O={session_data['open']} H={session_data['high']} L={session_data['low']} C={session_data['close']}")
        logger.info(f"  ✓ Bars in session: {session_data['bars_in_session']}")

        # Also fetch hourly data for analysis engine (VPOC, regime)
        gold_hourly = await data_engine.fetch_ohlcv(gold_symbol, period="5d", interval="1h")

        # === MATH LAYER ===
        logger.info("[2/6] Computing correlations...")
        correlation_matrix = await data_engine.get_correlations()
        if not correlation_matrix.empty:
            logger.info(f"  ✓ Correlations computed")
        else:
            logger.info(f"  ⚠ Correlations unavailable")

        logger.info("[3/6] Calculating volatility levels...")
        volatility_levels = data_engine.calc_volatility_levels(session_data)
        logger.info(f"  ✓ Pivot: {volatility_levels.get('pivot', 'N/A')}")
        logger.info(f"  ✓ Session Range: {volatility_levels.get('session_range', 'N/A')} pts")

        # === ANALYSIS LAYER ===
        logger.info("[4/6] Analyzing market structure...")
        if gold_hourly is not None and not gold_hourly.empty:
            market_structure = analyst.analyze_market_structure(gold_hourly)
            market_regime = analyst.get_market_regime(gold_hourly)
            logger.info(f"  ✓ Regime: {market_regime}")
            logger.info(f"  ✓ VPOC: {market_structure.get('vpoc', 'N/A')}")
        else:
            market_structure = {"vpoc": session_data.get("vwap")}
            market_regime = "Unknown"
            logger.info(f"  ⚠ Using VWAP as VPOC proxy: {session_data.get('vwap')}")

        # === POSITIONING LAYER ===
        logger.info("[5/6] Fetching COT positioning & calendar...")
        cot_positioning = await cot_analyzer.get_gold_positioning()
        event_context = calendar.get_event_context()
        
        if cot_positioning.get("available"):
            spec_bias = cot_positioning["speculators"]["bias"]
            spec_net = cot_positioning["speculators"]["net"]
            logger.info(f"  ✓ COT: Speculators {spec_bias} ({spec_net:,} contracts)")
        else:
            logger.info("  ⚠ COT data not available")
        
        if event_context.get("risk_warning"):
            logger.info(f"  ⚠ {event_context['risk_warning']}")

        # === BUILD DATA PACKAGE ===
        market_data_dict = {
            "timestamp": datetime.now(ET_TZ).strftime("%Y-%m-%d %H:%M ET"),
            "symbol": gold_symbol,
            "session_data": {
                "open": session_data["open"],
                "high": session_data["high"],
                "low": session_data["low"],
                "close": session_data["close"],
                "volume": session_data["volume"],
                "vwap": session_data["vwap"],
                "pivot": session_data["pivot"],
                "session_start": session_data["session_start"],
                "session_end": session_data["session_end"]
            },
            "current_price": session_data["close"],
            "correlations": correlation_matrix.to_dict() if not correlation_matrix.empty else {},
            "volatility_levels": volatility_levels,
            "market_structure": {
                "vpoc": market_structure.get("vpoc"),
                "max_volume_node": market_structure.get("max_volume"),
                "regime": market_regime
            },
            "cot_positioning": cot_positioning,
            "event_calendar": event_context,
            "data_quality": {
                "bars_in_session": session_data["bars_in_session"],
                "data_source": "Yahoo Finance (CME Session Aligned)",
                "session_alignment": "18:00 ET to 17:00 ET"
            }
        }

        # === LLM SYNTHESIS ===
        logger.info("[6/6] Generating LLM report...")
        report = await llm.generate_report(market_data_dict)
        logger.info(f"  ✓ Report generated ({len(report)} chars)")

        # === DELIVERY ===
        logger.info("Sending report to Discord...")
        success = messenger.send_report(report)
        
        if success:
            logger.info("=" * 50)
            logger.info("✅ Pipeline completed successfully")
            logger.info("=" * 50)
            return True
        else:
            logger.error("❌ Failed to send report to Discord")
            return False

    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}", exc_info=True)
        return False


async def daily_scheduler():
    """Run the pipeline once daily at 05:00 ET (Pre-Market Brief) on trading days only."""
    logger.info("=" * 50)
    logger.info("Gold_Sovereign_AI Daily Scheduler Started")
    logger.info(f"Target execution time: {TARGET_HOUR:02d}:{TARGET_MINUTE:02d} ET (Sun-Fri, skip Saturday)")
    logger.info("=" * 50)
    
    while True:
        try:
            # Calculate time until next execution
            seconds_until, target_time = get_seconds_until_target()
            hours_until = seconds_until / 3600
            
            logger.info(f"🛌 Hypersleep engaged. Waking up in {hours_until:.1f} hours for the 05:00 ET Pre-Market Brief.")
            logger.info(f"   Next execution: {target_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # Sleep until target time
            await asyncio.sleep(seconds_until)
            
            # Execute the pipeline
            logger.info("⏰ Wake up! Executing Pre-Market Brief...")
            try:
                await run_pipeline()
            except Exception as e:
                logger.error(f"❌ Pipeline execution failed: {e}", exc_info=True)
                logger.info("Pipeline failed but scheduler will continue. Sleeping until tomorrow...")
            
            # Small delay to ensure we don't double-execute
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"❌ Scheduler error: {e}", exc_info=True)
            logger.info("Scheduler encountered an error. Retrying in 5 minutes...")
            await asyncio.sleep(300)


async def main():
    """Main entry point - run once or schedule based on environment."""
    run_once = os.getenv("RUN_ONCE", "false").lower() == "true"
    
    if run_once:
        # Single execution mode (for testing)
        logger.info("Running in single execution mode (RUN_ONCE=true)")
        await run_pipeline()
    else:
        # Daily scheduled mode (production)
        await daily_scheduler()


if __name__ == "__main__":
    asyncio.run(main())
