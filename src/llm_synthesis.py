import logging
import json
import asyncio
from openai import AsyncOpenAI
from typing import Dict
from src.config import OPENROUTER_API_KEY, OPENROUTER_MODEL, SITE_URL, SITE_NAME

logger = logging.getLogger(__name__)


class ReasoningCore:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_API_KEY,
            default_headers={
                "HTTP-Referer": SITE_URL,
                "X-Title": SITE_NAME
            }
        ) if OPENROUTER_API_KEY else None

    async def generate_report(self, market_data_dict: Dict) -> str:
        """Generate trading intelligence report using Claude 3.5 Sonnet."""
        if not self.client:
            logger.error("OpenRouter API key not configured")
            return "# Error: API key not configured"

        system_prompt = """You are a ruthless institutional gold trader. No fluff. Focus on liquidity traps, correlation divergences, and sigma levels. Use strictly gold futures terminology. Provide actionable insights in a concise markdown format."""

        user_prompt = f"""Analyze the following market data and generate a comprehensive trading intelligence report:

{json.dumps(market_data_dict, indent=2)}

Structure the report with:
1. Executive Synthesis
2. Macro Correlations
3. Market Structure
4. Volatility Levels
5. Game Theory Scenarios

Be direct, quantitative, and focus on actionable intelligence."""

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await self.client.chat.completions.create(
                    model=OPENROUTER_MODEL,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.7,
                    max_tokens=2000
                )

                report = response.choices[0].message.content
                return report if report else "# Error: Empty response from LLM"

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Error generating report after {max_retries} attempts: {e}")
                    return f"# Error generating report: {str(e)}"
                logger.warning(f"LLM request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                await asyncio.sleep(2 * (attempt + 1))
        
        return "# Error: Failed to generate report after retries"

