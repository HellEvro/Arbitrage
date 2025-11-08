from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator, Protocol, Sequence

from arbitrage_bot.core.http import HttpClientFactory


@dataclass(slots=True)
class ExchangeMarket:
    symbol: str
    base_asset: str
    quote_asset: str


@dataclass(slots=True)
class ExchangeQuote:
    symbol: str
    bid: float
    ask: float
    timestamp_ms: int


class ExchangeAdapter(Protocol):
    name: str

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        ...

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        ...

    async def close(self) -> None:
        ...


class BaseAdapter:
    name: str

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        self._closed = asyncio.Event()
        self._http = http_factory
        self._poll_interval = poll_interval
        self._log = logging.getLogger(f"arbitrage_bot.exchanges.{self.name}")

    async def close(self) -> None:
        self._log.info("Closing adapter")
        self._closed.set()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    async def wait_interval(self) -> None:
        await asyncio.sleep(self._poll_interval)

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

