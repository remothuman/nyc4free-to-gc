import logging
import re
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple
from urllib.parse import urljoin

import html2text
import requests
from bs4 import BeautifulSoup, Tag


logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.0.0 Safari/537.36"
    )
}


@dataclass(frozen=True)
class EventDetails:
    description: str = ""
    external_url: str = ""
    external_label: str = ""
    poster_image_url: str = ""


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

    def get_details(self, url_path: str) -> EventDetails:
        """Public entry point: resolve url_path, fetch HTML, extract details."""
        full_url = self._normalize_url(url_path)
        if not full_url:
            return EventDetails()
        return self._cached_fetch(full_url)

    def get_description(self, url_path: str) -> str:
        """Backward-compatible helper returning only the description text."""
        return self.get_details(url_path).description

    def _normalize_url(self, url_path: str) -> str:
        if not url_path:
            return ""
        if url_path.startswith(("http://", "https://")):
            return url_path
        return urljoin(f"{self.base_url}/", url_path.lstrip("/"))

    @lru_cache(maxsize=512)
    def _cached_fetch(self, full_url: str) -> EventDetails:
        try:
            response = self.session.get(full_url, timeout=self.timeout)
            response.raise_for_status()
            return self._extract_details(response.text)
        except requests.RequestException as exc:
            logger.warning("Failed to fetch description from %s: %s", full_url, exc)
            return EventDetails()
        finally:
            if self.request_delay:
                time.sleep(self.request_delay)

    def _extract_details(self, html: str) -> EventDetails:
        soup = BeautifulSoup(html, "html.parser")

        post_body = soup.select_one('[data-layout-label="Post Body"]')
        description = ""
        if post_body:
            fragments = []
            for block in post_body.select(".sqs-block.html-block .sqs-block-content"):
                # Convert each block's HTML to text using html2text
                block_html = str(block)
                rendered = _html_to_text(block_html)
                if rendered:
                    fragments.append(rendered)
            if fragments:
                description = _cleanup_text("\n\n".join(fragments))

        if not description:
            for meta_name in (
                ("property", "og:description"),
                ("name", "description"),
            ):
                meta_tag = soup.find("meta", attrs={meta_name[0]: meta_name[1]})
                if meta_tag and meta_tag.get("content"):
                    description = meta_tag["content"].strip()
                    break

        external_url, external_label = self._extract_external_link(post_body or soup)
        poster_image_url = self._extract_poster_image(soup)

        return EventDetails(
            description=description,
            external_url=external_url,
            external_label=external_label,
            poster_image_url=poster_image_url,
        )

    def _extract_external_link(self, scope: Tag) -> Tuple[str, str]:
        """Look for a button-styled external link (official site/RSVP).
        
        Only matches links inside Squarespace button blocks, not inline links
        within the body text.
        """
        # Only look for actual button elements - these appear as standalone
        # rectangular outlined buttons on the page
        for anchor in scope.select(".sqs-block-button a"):
                href = (anchor.get("href") or "").strip()
                normalized = self._normalize_href(href)
                if not normalized:
                    continue
                if normalized.startswith(("mailto:", "tel:")):
                    continue
                if normalized.startswith(self.base_url):
                    continue
                label = anchor.get_text(strip=True) or "Official Link"
                return normalized, label

        return "", ""

    def _normalize_href(self, href: str) -> str:
        if not href:
            return ""
        return urljoin(f"{self.base_url}/", href)

    def _extract_poster_image(self, soup: BeautifulSoup) -> str:
        """Extract the main poster/banner image URL from the event page."""
        # Try og:image meta tag first (most reliable for main event image)
        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image and og_image.get("content"):
            return og_image["content"].strip()

        # Fallback: look for main banner image in the page structure
        # Squarespace often uses this for event banner images
        banner_img = soup.select_one(".banner-thumbnail-wrapper img")
        if banner_img:
            src = banner_img.get("data-src") or banner_img.get("src")
            if src:
                return src.strip()

        return ""


def _create_html2text_converter() -> html2text.HTML2Text:
    """Create a configured html2text converter."""
    h = html2text.HTML2Text()
    h.ignore_links = False
    h.ignore_images = True
    h.ignore_emphasis = True
    h.body_width = 0  # Don't wrap lines
    h.ul_item_mark = "-"  # Use dash for bullet points
    h.single_line_break = True  # Use single newlines instead of double
    return h


def _html_to_text(html: str) -> str:
    """Convert HTML to readable plaintext using html2text."""
    if not html:
        return ""
    converter = _create_html2text_converter()
    text = converter.handle(html)
    return _cleanup_text(text)


def _cleanup_text(value: str) -> str:
    """Light cleanup: normalize line endings and reduce excessive blank lines."""
    if not value:
        return ""
    # Normalize Windows/Mac line endings
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    # Reduce 3+ blank lines to 2
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


