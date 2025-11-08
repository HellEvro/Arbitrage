from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp

log = logging.getLogger(__name__)


class HttpClientFactory:
    def __init__(self, timeout: float = 10.0, user_agent: str | None = None) -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        # Use realistic browser User-Agent to avoid blocking
        self._user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[aiohttp.ClientSession]:
        if self._session and not self._session.closed:
            yield self._session
            return

        async with self._lock:
            if not self._session or self._session.closed:
                headers = {
                    "User-Agent": self._user_agent,
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors",
                    "Sec-Fetch-Site": "same-origin",
                }
                # Create connector with SSL verification but allow more flexible SSL
                connector = aiohttp.TCPConnector(ssl=True, limit=100)
                self._session = aiohttp.ClientSession(
                    timeout=self._timeout,
                    headers=headers,
                    connector=connector,
                )
        try:
            yield self._session  # type: ignore[misc]
        finally:
            ...

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_json(self, url: str, params: dict[str, Any] | None = None, max_retries: int = 3, extra_headers: dict[str, str] | None = None, cookies: dict[str, str] | None = None) -> Any:
        """
        Make GET request to public API endpoint (no authentication required).
        Handles rate limiting (429) with exponential backoff retry.
        
        Args:
            url: URL to request
            params: Query parameters
            max_retries: Maximum retry attempts
            extra_headers: Additional headers to include (e.g., Referer, Origin for Cloudflare)
            cookies: Cookies to include in request (for Cloudflare protection)
        """
        log.debug("GET %s with params: %s", url, params)
        retry_count = 0
        async with self.session() as session:
            # Merge extra headers if provided
            request_headers = dict(extra_headers) if extra_headers else {}
            # Add cookies if provided
            request_cookies = dict(cookies) if cookies else {}
            while retry_count < max_retries:
                try:
                    async with session.get(url, params=params, headers=request_headers, cookies=request_cookies) as response:
                        if response.status == 429:
                            # Rate limit exceeded - wait and retry
                            try:
                                retry_after = int(response.headers.get("Retry-After", "2"))
                            except (ValueError, TypeError):
                                retry_after = 2
                            wait_time = min(retry_after * (2 ** retry_count), 10)  # Max 10 seconds
                            log.warning("Rate limit exceeded (429) for %s, waiting %d seconds", url, wait_time)
                            await asyncio.sleep(wait_time)
                            retry_count += 1
                            continue
                        response.raise_for_status()
                        data = await response.json()
                        log.debug("Response status: %d, size: %d bytes", response.status, len(str(data)))
                        return data
                except aiohttp.ClientResponseError as e:
                    if e.status == 429 and retry_count < max_retries - 1:
                        wait_time = min(2 ** retry_count, 10)
                        log.warning("Rate limit error (429) for %s, retrying after %d seconds", url, wait_time)
                        await asyncio.sleep(wait_time)
                        retry_count += 1
                        continue
                    if e.status == 403:
                        # Don't retry on 403 - IP blocked or API changed
                        # Логируем только как debug, чтобы не засорять логи
                        log.debug("Access forbidden (403) for %s - may be IP blocked or API changed. Not retrying.", url)
                        raise
                    raise
        raise aiohttp.ClientResponseError(
            request_info=None,
            history=None,
            status=429,
            message="Rate limit exceeded after retries",
        )

