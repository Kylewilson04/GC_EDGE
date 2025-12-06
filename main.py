import asyncio
import logging
import os
from datetime import datetime
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

RUN_INTERVAL_MINUTES = int(os.getenv("RUN_INTERVAL_MINUTES", "0"))


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
        logger.info("[1/6] Fetching market data...")
        gold_symbol = SYMBOLS["gold"]
        gold_data = await data_engine.fetch_ohlcv(gold_symbol)
        
        if gold_data is None or gold_data.empty:
            logger.error("Failed to fetch gold data - aborting pipeline")
            return False

        logger.info(f"  ✓ Gold data: {len(gold_data)} bars")

        # === MATH LAYER ===
        logger.info("[2/6] Computing correlations...")
        correlation_matrix = await data_engine.get_correlations()
        logger.info(f"  ✓ Correlations computed")

        logger.info("[3/6] Calculating volatility levels...")
        volatility_levels = data_engine.calc_volatility_levels(gold_data["close"])
        logger.info(f"  ✓ Volatility: {volatility_levels.get('annualized_volatility', 'N/A')}% annualized")

        # === ANALYSIS LAYER ===
        logger.info("[4/6] Analyzing market structure...")
        market_structure = analyst.analyze_market_structure(gold_data)
        market_regime = analyst.get_market_regime(gold_data)
        logger.info(f"  ✓ Regime: {market_regime}")
        logger.info(f"  ✓ VPOC: {market_structure.get('vpoc', 'N/A')}")

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
        current_price = float(gold_data["close"].iloc[-1])
        prev_close = float(gold_data["close"].iloc[-2]) if len(gold_data) > 1 else current_price
        daily_change = round(((current_price - prev_close) / prev_close) * 100, 2)

        market_data_dict = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
            "symbol": gold_symbol,
            "current_price": current_price,
            "daily_change_pct": daily_change,
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
                "bars_analyzed": len(gold_data),
                "data_source": "Yahoo Finance (15-min delayed)"
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


async def scheduled_runner():
    """Run the pipeline on a schedule."""
    logger.info(f"Starting scheduled runner with {RUN_INTERVAL_MINUTES} minute interval")
    
    while True:
        await run_pipeline()
        logger.info(f"Next run in {RUN_INTERVAL_MINUTES} minutes...")
        await asyncio.sleep(RUN_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    if RUN_INTERVAL_MINUTES > 0:
        asyncio.run(scheduled_runner())
    else:
        asyncio.run(run_pipeline())
