import asyncio
import logging
import os
from datetime import datetime
from src.config import SYMBOLS
from src.data_engine import MarketData
from src.analysis_engine import LocalAnalyst
from src.llm_synthesis import ReasoningCore
from src.messenger import DiscordBot

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

RUN_INTERVAL_MINUTES = int(os.getenv("RUN_INTERVAL_MINUTES", "0"))

logger = logging.getLogger(__name__)


async def main():
    """Orchestrate the trading intelligence pipeline."""
    logger.info("Starting Gold_Sovereign_AI pipeline")

    try:
        data_engine = MarketData()
        analyst = LocalAnalyst()
        llm = ReasoningCore()
        messenger = DiscordBot()

        logger.info("Fetching market data...")
        gold_symbol = SYMBOLS["gold"]
        gold_data = await data_engine.fetch_ohlcv(gold_symbol)
        
        if gold_data is None or gold_data.empty:
            logger.error("Failed to fetch gold data")
            return

        logger.info("Computing correlations...")
        correlation_matrix = await data_engine.get_correlations()

        logger.info("Calculating volatility levels...")
        volatility_levels = data_engine.calc_volatility_levels(gold_data["close"])

        logger.info("Analyzing market structure...")
        market_structure = analyst.analyze_market_structure(gold_data)
        market_regime = analyst.get_market_regime(gold_data)

        logger.info("Preparing market data dictionary...")
        market_data_dict = {
            "timestamp": datetime.now().isoformat(),
            "symbol": gold_symbol,
            "current_price": float(gold_data["close"].iloc[-1]),
            "correlations": correlation_matrix.to_dict() if not correlation_matrix.empty else {},
            "volatility_levels": volatility_levels,
            "market_structure": market_structure,
            "market_regime": market_regime,
            "data_points": len(gold_data)
        }

        logger.info("Generating LLM report...")
        report = await llm.generate_report(market_data_dict)

        logger.info("Sending report to Discord...")
        success = messenger.send_report(report)
        
        if success:
            logger.info("Pipeline completed successfully")
        else:
            logger.error("Failed to send report to Discord")

    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)


async def scheduled_runner():
    """Run the pipeline on a schedule."""
    logger.info(f"Starting scheduled runner with {RUN_INTERVAL_MINUTES} minute interval")
    
    while True:
        await main()
        logger.info(f"Sleeping for {RUN_INTERVAL_MINUTES} minutes...")
        await asyncio.sleep(RUN_INTERVAL_MINUTES * 60)


if __name__ == "__main__":
    if RUN_INTERVAL_MINUTES > 0:
        asyncio.run(scheduled_runner())
    else:
        asyncio.run(main())

