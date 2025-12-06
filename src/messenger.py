import logging
import requests
from typing import Optional
from src.config import DISCORD_WEBHOOK_URL

logger = logging.getLogger(__name__)

DISCORD_MAX_LENGTH = 2000


class DiscordBot:
    def __init__(self):
        self.webhook_url = DISCORD_WEBHOOK_URL

    def send_report(self, markdown_text: str) -> bool:
        """Send report to Discord webhook, chunking if necessary."""
        if not self.webhook_url:
            logger.error("Discord webhook URL not configured")
            return False

        if len(markdown_text) <= DISCORD_MAX_LENGTH:
            return self._send_chunk(markdown_text)

        chunks = self._chunk_text(markdown_text)
        success = True
        for i, chunk in enumerate(chunks):
            if not self._send_chunk(chunk, chunk_num=i+1, total=len(chunks)):
                success = False
        return success

    def _chunk_text(self, text: str) -> list:
        """Split text into chunks that fit within Discord's limit."""
        chunks = []
        current_chunk = ""
        
        lines = text.split('\n')
        for line in lines:
            if len(current_chunk) + len(line) + 1 <= DISCORD_MAX_LENGTH:
                current_chunk += line + '\n'
            else:
                if current_chunk:
                    chunks.append(current_chunk.rstrip())
                if len(line) > DISCORD_MAX_LENGTH:
                    for i in range(0, len(line), DISCORD_MAX_LENGTH):
                        chunks.append(line[i:i+DISCORD_MAX_LENGTH])
                    current_chunk = ""
                else:
                    current_chunk = line + '\n'
        
        if current_chunk:
            chunks.append(current_chunk.rstrip())
        
        return chunks

    def _send_chunk(self, content: str, chunk_num: Optional[int] = None, total: Optional[int] = None) -> bool:
        """Send a single chunk to Discord with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if chunk_num and total:
                    header = f"**Part {chunk_num}/{total}**\n\n"
                    content_with_header = header + content
                else:
                    content_with_header = content

                payload = {"content": content_with_header}
                response = requests.post(self.webhook_url, json=payload, timeout=10)
                response.raise_for_status()
                logger.info(f"Successfully sent message to Discord (chunk {chunk_num}/{total if chunk_num else 'single'})")
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Error sending message to Discord after {max_retries} attempts: {e}")
                    return False
                logger.warning(f"Discord send failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying...")
                import time
                time.sleep(1 * (attempt + 1))
        return False

