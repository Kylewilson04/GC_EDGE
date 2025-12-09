"""
Quick test script for GhostKing Protocol module.
Run: python test_ghostking.py
"""
import asyncio
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

from src.ghostking_protocol import GhostKingProtocol

async def main():
    print("\n" + "=" * 60)
    print("👻 GHOSTKING PROTOCOL - TEST RUN")
    print("=" * 60 + "\n")
    
    protocol = GhostKingProtocol()
    
    # Run the full analysis
    analysis = await protocol.run_analysis()
    
    # Generate and print the formatted report
    report = protocol.format_report(analysis)
    
    print("\n" + "=" * 60)
    print("📋 FORMATTED REPORT OUTPUT")
    print("=" * 60)
    print(report)
    
    # Also print raw data for debugging
    print("\n" + "=" * 60)
    print("🔧 RAW ANALYSIS DATA")
    print("=" * 60)
    print(f"\nYields:")
    print(f"  US10Y: {analysis['yields'].get('US10Y', 'N/A')}")
    print(f"  US02Y: {analysis['yields'].get('US02Y', 'N/A')}")
    print(f"  Spread: {analysis['yields'].get('spread', 'N/A')}")
    
    print(f"\nLiquidity:")
    print(f"  WALCL: {analysis['liquidity'].get('walcl', 'N/A')}T")
    print(f"  WTREGEN: {analysis['liquidity'].get('wtregen', 'N/A')}T")
    print(f"  RRP: {analysis['liquidity'].get('rrp', 'N/A')}T")
    print(f"  Net Liquidity: {analysis['liquidity'].get('net_liquidity', 'N/A')}T")
    print(f"  EMA(20): {analysis['liquidity'].get('ema_20', 'N/A')}T")
    print(f"  Trend: {analysis['liquidity'].get('trend', 'N/A')}")
    
    print(f"\nES Price: ${analysis.get('es_price', 'N/A'):,.2f}" if analysis.get('es_price') else "\nES Price: N/A")
    
    print(f"\nRegime:")
    regime = analysis.get('regime', {})
    print(f"  Macro State: {regime.get('macro_state', 'N/A')}")
    print(f"  Liquidity Bias: {regime.get('liquidity_bias', 'N/A')}")
    print(f"  Combined Signal: {regime.get('combined_signal', 'N/A')}")
    
    print("\n" + "=" * 60)
    print("✅ TEST COMPLETE")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    asyncio.run(main())

