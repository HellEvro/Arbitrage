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

    async def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        log.debug("GET %s with params: %s", url, params)
        async with self.session() as session:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                log.debug("Response status: %d, size: %d bytes", response.status, len(str(data)))
                return data

