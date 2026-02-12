"""HTTP client for Kickstarter with Cloudflare bypass.

Two backends:
  1. curl_cffi — fast, impersonates browser TLS fingerprint
  2. Playwright — fallback headless browser for when curl_cffi gets blocked

Rate-limited with exponential backoff.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

BASE_URL = "https://www.kickstarter.com"


class RateLimiter:
    """Simple rate limiter."""

    def __init__(self, rps: float = 1.0):
        self._interval = 1.0 / rps
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self._last + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()


class KickstarterAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class KickstarterClient:
    """Kickstarter client with curl_cffi (fast) + Playwright (fallback)."""

    def __init__(
        self,
        rate_limit_rps: float = 1.0,
        timeout: float = 30.0,
        max_retries: int = 3,
        user_agent: str | None = None,
    ):
        self._rate_limiter = RateLimiter(rps=rate_limit_rps)
        self._timeout = timeout
        self._max_retries = max_retries
        self._browser = None  # lazy Playwright init
        self._page = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        if self._browser:
            await self._browser.close()
            self._browser = None

    def _curl_get(self, url: str) -> tuple[int, str]:
        """Synchronous curl_cffi request impersonating Chrome."""
        from curl_cffi import requests as curl_requests

        resp = curl_requests.get(
            url,
            impersonate="chrome",
            timeout=self._timeout,
        )
        return resp.status_code, resp.text

    async def _curl_get_async(self, url: str) -> tuple[int, str]:
        """Run curl_cffi in a thread to keep async."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._curl_get, url)

    async def _playwright_get(self, url: str) -> str:
        """Fallback: use Playwright headless browser."""
        if not self._browser:
            from playwright.async_api import async_playwright

            pw = await async_playwright().start()
            self._browser = await pw.chromium.launch(headless=True)
            self._page = await self._browser.new_page()

        await self._page.goto(url, wait_until="networkidle", timeout=self._timeout * 1000)
        return await self._page.content()

    async def _request_json(self, url: str) -> dict:
        """Fetch a URL and parse JSON. Retries with backoff on 403/5xx."""
        await self._rate_limiter.acquire()

        for attempt in range(self._max_retries):
            try:
                status, body = await self._curl_get_async(url)
                if status == 200:
                    return json.loads(body)
                if status == 403:
                    wait = 15 * (attempt + 1)
                    logger.warning(f"403 blocked, waiting {wait}s (attempt {attempt + 1})")
                    await asyncio.sleep(wait)
                    continue
                if status >= 500:
                    logger.warning(f"Server error {status}, retry {attempt + 1}")
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise KickstarterAPIError(status, body[:200])
            except json.JSONDecodeError:
                logger.warning(f"Non-JSON response, retry {attempt + 1}")
                await asyncio.sleep(5)
                continue
            except KickstarterAPIError:
                raise
            except Exception as e:
                if attempt == self._max_retries - 1:
                    raise KickstarterAPIError(0, str(e))
                await asyncio.sleep(2 ** attempt)

        raise KickstarterAPIError(403, "Max retries exceeded")

    async def _request_html(self, url: str) -> str:
        """Fetch a URL and return HTML content."""
        await self._rate_limiter.acquire()

        # Try curl_cffi first
        try:
            status, body = await self._curl_get_async(url)
            if status == 200:
                return body
        except Exception:
            pass

        # Fallback: Playwright
        return await self._playwright_get(url)

    async def discover(
        self,
        term: str | None = None,
        category_id: int | None = None,
        state: str = "all",
        sort: str = "newest",
        page: int = 1,
    ) -> dict:
        """Search/discover projects via /discover/advanced.json."""
        params: dict[str, Any] = {"sort": sort, "page": page}
        if term:
            params["term"] = term
        if category_id:
            params["category_id"] = category_id
        if state and state != "all":
            params["state"] = state

        url = f"{BASE_URL}/discover/advanced.json?{urlencode(params)}"
        logger.debug(f"Discover: {url}")
        return await self._request_json(url)

    async def get_project(self, slug: str) -> dict:
        """Fetch full project detail by slug."""
        url = f"{BASE_URL}/projects/{slug}.json"
        logger.debug(f"Project detail: {url}")
        return await self._request_json(url)

    async def discover_all_pages(
        self,
        term: str | None = None,
        category_id: int | None = None,
        state: str = "all",
        sort: str = "newest",
        max_pages: int = 100,
        page_delay: float = 1.5,
    ) -> list[dict]:
        """Paginate through all discover results."""
        all_projects = []
        page = 1

        while page <= max_pages:
            data = await self.discover(
                term=term, category_id=category_id, state=state, sort=sort, page=page,
            )

            projects = data.get("projects", [])
            if not projects:
                logger.info(f"No more results at page {page}")
                break

            all_projects.extend(projects)
            total = data.get("total_hits", len(all_projects))
            logger.info(
                f"Page {page}: {len(projects)} projects ({len(all_projects)}/{total} total)"
            )

            if len(all_projects) >= total:
                break

            page += 1
            if page_delay > 0:
                await asyncio.sleep(page_delay)

        return all_projects
