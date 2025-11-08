from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from arbitrage_bot.core.exceptions import DiscoveryError
from arbitrage_bot.exchanges import ExchangeAdapter
from arbitrage_bot.services.schemas import MarketInfo

log = logging.getLogger(__name__)


class MarketDiscoveryService:
    def __init__(self, adapters: Sequence[ExchangeAdapter], refresh_interval_sec: float = 300.0) -> None:
        self._adapters = adapters
        self._refresh_interval_sec = refresh_interval_sec
        self._cache: list[MarketInfo] = []
        self._lock = asyncio.Lock()

    async def refresh(self) -> list[MarketInfo]:
        log.info("Refreshing market discovery for %d exchanges", len(self._adapters))
        try:
            markets_per_exchange = await asyncio.gather(*(adapter.fetch_markets() for adapter in self._adapters))
        except Exception as exc:  # pragma: no cover - unexpected network errors
            log.exception("Failed to fetch markets from exchanges")
            raise DiscoveryError(f"Failed to fetch markets: {exc}") from exc

        symbol_map: dict[str, dict[str, str]] = {}
        for adapter, markets in zip(self._adapters, markets_per_exchange, strict=False):
            for market in markets:
                if market.quote_asset.upper() != "USDT":
                    continue
                canonical_symbol = f"{market.base_asset.upper()}{market.quote_asset.upper()}"
                exchange_symbols = symbol_map.setdefault(canonical_symbol, {})
                exchange_symbols[adapter.name] = market.symbol.upper()

        intersection: list[MarketInfo] = []
        for canonical, exchanges in symbol_map.items():
            if len(exchanges) != len(self._adapters):
                continue
            intersection.append(
                MarketInfo(
                    symbol=canonical,
                    exchanges=sorted(exchanges.keys()),
                    exchange_symbols=dict(exchanges),
                )
            )

        async with self._lock:
            self._cache = sorted(intersection, key=lambda info: info.symbol)

        log.info("Found %d intersecting markets across all exchanges", len(self._cache))
        return list(self._cache)

    async def get_cached(self) -> list[MarketInfo]:
        async with self._lock:
            return list(self._cache)

    @property
    def refresh_interval(self) -> float:
        return self._refresh_interval_sec

