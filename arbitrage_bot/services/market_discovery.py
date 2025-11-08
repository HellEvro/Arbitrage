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
            # Use return_exceptions=True to continue even if some exchanges fail
            results = await asyncio.gather(
                *(adapter.fetch_markets() for adapter in self._adapters),
                return_exceptions=True
            )
            markets_per_exchange = []
            for adapter, result in zip(self._adapters, results, strict=False):
                if isinstance(result, Exception):
                    log.error("Failed to fetch markets from %s: %s", adapter.name, result)
                    markets_per_exchange.append([])  # Empty list for failed exchange
                else:
                    markets_per_exchange.append(result)
        except Exception as exc:  # pragma: no cover - unexpected network errors
            # Don't raise if we have at least 2 exchanges working
            successful_exchanges = sum(1 for markets in markets_per_exchange if len(markets) > 0)
            if successful_exchanges >= 2:
                log.warning("Some exchanges failed, but %d exchanges are still working: %s", successful_exchanges, exc)
            else:
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

        # Count successful exchanges (non-empty market lists)
        successful_exchanges = sum(1 for markets in markets_per_exchange if len(markets) > 0)
        log.info("Successfully fetched markets from %d out of %d exchanges", successful_exchanges, len(self._adapters))
        
        # Minimum 2 exchanges required for arbitrage
        MIN_EXCHANGES_REQUIRED = 2
        if successful_exchanges < MIN_EXCHANGES_REQUIRED:
            log.warning(
                "Only %d exchanges available (minimum %d required). System will continue but may have limited opportunities.",
                successful_exchanges,
                MIN_EXCHANGES_REQUIRED
            )
        
        intersection: list[MarketInfo] = []
        for canonical, exchanges in symbol_map.items():
            # Require symbol to be on at least 2 exchanges for arbitrage
            if len(exchanges) < MIN_EXCHANGES_REQUIRED:
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

