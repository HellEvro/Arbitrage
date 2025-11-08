from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aiohttp

log = logging.getLogger(__name__)


class HttpClientFactory:
    def __init__(self, timeout: float = 10.0, user_agent: str = "ArbitrageBot/0.1") -> None:
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._user_agent = user_agent
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[aiohttp.ClientSession]:
        if self._session and not self._session.closed:
            yield self._session
            return

        async with self._lock:
            if not self._session or self._session.closed:
                headers = {"User-Agent": self._user_agent}
                self._session = aiohttp.ClientSession(timeout=self._timeout, headers=headers)
        try:
            yield self._session  # type: ignore[misc]
        finally:
            ...

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_json(self, url: str, params: dict[str, Any] | None = None, max_retries: int = 3) -> Any:
        """
        Make GET request to public API endpoint (no authentication required).
        Handles rate limiting (429) with exponential backoff retry.
        """
        log.debug("GET %s with params: %s", url, params)
        retry_count = 0
        async with self.session() as session:
            while retry_count < max_retries:
                try:
                    async with session.get(url, params=params) as response:
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
                    raise
        raise aiohttp.ClientResponseError(
            request_info=None,
            history=None,
            status=429,
            message="Rate limit exceeded after retries",
        )

