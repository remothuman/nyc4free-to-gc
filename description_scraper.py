import logging
import time
from functools import lru_cache
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.0.0 Safari/537.36"
    )
}


class EventDescriptionScraper:
    """Fetch and parse NYC for Free event detail pages to extract descriptions."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 20.0,
        request_delay: float = 0.25,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.request_delay = max(0.0, request_delay)
        self.session = session or requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get_description(self, url_path: str) -> str:
        """Public entry point: resolve url_path, fetch HTML, extract text."""
        full_url = self._normalize_url(url_path)
        if not full_url:
            return ""
        return self._cached_fetch(full_url)

    def _normalize_url(self, url_path: str) -> str:
        if not url_path:
            return ""
        if url_path.startswith(("http://", "https://")):
            return url_path
        return urljoin(f"{self.base_url}/", url_path.lstrip("/"))

    @lru_cache(maxsize=512)
    def _cached_fetch(self, full_url: str) -> str:
        try:
            response = self.session.get(full_url, timeout=self.timeout)
            response.raise_for_status()
            return self._extract_description(response.text)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch description from %s: %s", full_url, exc)
            return ""
        finally:
            if self.request_delay:
                time.sleep(self.request_delay)

    @staticmethod
    def _extract_description(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        post_body = soup.select_one('[data-layout-label="Post Body"]')
        if post_body:
            paragraphs = []
            for block in post_body.select(".sqs-block.html-block .sqs-block-content"):
                text = block.get_text(separator="\n", strip=True)
                if text:
                    paragraphs.append(text)
            cleaned = _normalize_whitespace("\n\n".join(paragraphs))
            if cleaned:
                return cleaned

        for meta_name in (
            ("property", "og:description"),
            ("name", "description"),
        ):
            meta_tag = soup.find("meta", attrs={meta_name[0]: meta_name[1]})
            if meta_tag and meta_tag.get("content"):
                return meta_tag["content"].strip()

        return ""


def _normalize_whitespace(value: str) -> str:
    """Remove extra whitespace and normalize line breaks."""
    # Replace multiple spaces with single space
    import re
    value = re.sub(r'[ \t]+', ' ', value)
    
    # Split into lines and strip each
    lines = [line.strip() for line in value.splitlines()]
    
    # Remove empty lines and collapse consecutive blank lines
    collapsed = []
    for line in lines:
        if line:  # Only keep non-empty lines
            collapsed.append(line)
    
    # Join with single line breaks and add spacing between paragraphs
    return ' '.join(collapsed).strip()


