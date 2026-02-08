"""HTTP client for Kickstarter's internal API endpoints.

Kickstarter exposes JSON data via:
  - Discovery API: /discover/advanced.json (search + paginate projects)
  - Project detail: /projects/{slug}.json (full project data)
  - Rewards: embedded in project detail or via dedicated endpoint

Rate-limited with exponential backoff.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.kickstarter.com"

# Kickstarter returns 429 if you're too fast
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class RateLimiter:
    """Token-bucket rate limiter."""

    def __init__(self, rps: float = 1.0):
        self._interval = 1.0 / rps
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            wait = self._last_request + self._interval - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request = asyncio.get_event_loop().time()


class KickstarterAPIError(Exception):
    """Raised on non-retryable API errors."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class KickstarterClient:
    """Async HTTP client for Kickstarter endpoints."""

    def __init__(
        self,
        rate_limit_rps: float = 1.0,
        max_retries: int = 3,
        timeout: float = 30.0,
        user_agent: str = "KickstarterResearchBot/0.1",
        proxy: Optional[str] = None,
    ):
        self._rate_limiter = RateLimiter(rps=rate_limit_rps)
        self._max_retries = max_retries
        self._timeout = timeout

        transport_kwargs: dict[str, Any] = {}
        if proxy:
            transport_kwargs["proxy"] = proxy

        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=httpx.Timeout(timeout),
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
            },
            follow_redirects=True,
            **transport_kwargs,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def close(self):
        await self._client.aclose()

    @retry(
        retry=retry_if_exception_type(httpx.HTTPStatusError),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=1, max=30),
        reraise=True,
    )
    async def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make a rate-limited request with retry logic."""
        await self._rate_limiter.acquire()

        resp = await self._client.request(method, path, **kwargs)

        if resp.status_code in RETRYABLE_STATUS:
            logger.warning(f"Retryable status {resp.status_code} for {path}")
            resp.raise_for_status()

        if resp.status_code >= 400:
            raise KickstarterAPIError(resp.status_code, resp.text[:200])

        return resp.json()

    async def discover(
        self,
        term: Optional[str] = None,
        category_id: Optional[int] = None,
        state: str = "all",
        sort: str = "newest",
        page: int = 1,
        per_page: int = 20,
    ) -> dict:
        """Search/discover projects.

        Uses Kickstarter's /discover/advanced.json endpoint.

        Args:
            term: Search query string.
            category_id: Filter by category ID.
            state: Project state filter (all/live/successful/failed).
            sort: Sort order (newest/popularity/end_date/most_funded/most_backed).
            page: Page number (1-indexed).
            per_page: Results per page (max ~20 from Kickstarter).

        Returns:
            Raw JSON response with 'projects' list and pagination info.
        """
        params: dict[str, Any] = {
            "sort": sort,
            "page": page,
            "per_page": per_page,
        }
        if term:
            params["term"] = term
        if category_id:
            params["category_id"] = category_id
        if state and state != "all":
            params["state"] = state

        logger.debug(f"Discover: term={term}, category={category_id}, page={page}")
        return await self._request("GET", "/discover/advanced.json", params=params)

    async def get_project(self, slug: str) -> dict:
        """Fetch full project detail by slug.

        Args:
            slug: Project URL slug (e.g., 'my-ai-project').

        Returns:
            Raw project JSON from Kickstarter.
        """
        logger.debug(f"Fetching project: {slug}")
        return await self._request("GET", f"/projects/{slug}.json")

    async def discover_all_pages(
        self,
        term: Optional[str] = None,
        category_id: Optional[int] = None,
        state: str = "all",
        sort: str = "newest",
        max_pages: int = 100,
        page_delay: float = 1.5,
    ) -> list[dict]:
        """Paginate through all discover results.

        Args:
            term: Search query.
            category_id: Category filter.
            state: State filter.
            sort: Sort order.
            max_pages: Safety limit on pages to fetch.
            page_delay: Extra delay between pages.

        Yields all project dicts across pages.
        """
        all_projects = []
        page = 1

        while page <= max_pages:
            data = await self.discover(
                term=term,
                category_id=category_id,
                state=state,
                sort=sort,
                page=page,
            )

            projects = data.get("projects", [])
            if not projects:
                logger.info(f"No more results at page {page}")
                break

            all_projects.extend(projects)
            total = data.get("total_hits", len(all_projects))
            logger.info(
                f"Page {page}: got {len(projects)} projects "
                f"({len(all_projects)}/{total} total)"
            )

            # Check if we've gotten everything
            if len(all_projects) >= total:
                break

            page += 1
            if page_delay > 0:
                await asyncio.sleep(page_delay)

        return all_projects
