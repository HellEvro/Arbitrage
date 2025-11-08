from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from arbitrage_bot.core.exceptions import AggregationError
from arbitrage_bot.exchanges import ExchangeAdapter
from arbitrage_bot.services.quote_store import QuoteStore
from arbitrage_bot.services.schemas import MarketInfo

log = logging.getLogger(__name__)


class QuoteAggregator:
    def __init__(self, adapters: Sequence[ExchangeAdapter], quote_store: QuoteStore, markets: Sequence[MarketInfo]) -> None:
        self._adapters = adapters
        self._quote_store = quote_store
        self._markets = list(markets)
        self._tasks: list[asyncio.Task[None]] = []
        self._reverse_map: dict[tuple[str, str], str] = {}
        self._symbols_by_exchange: dict[str, list[str]] = {}
        self._rebuild_mappings()

    async def start(self) -> None:
        if self._tasks:
            log.warning("Quote aggregator already started")
            return
        
        # Count adapters with symbols
        active_adapters = sum(
            1 for adapter in self._adapters
            if self._symbols_by_exchange.get(adapter.name, [])
        )
        
        MIN_EXCHANGES_REQUIRED = 2
        if active_adapters < MIN_EXCHANGES_REQUIRED:
            log.warning(
                "Only %d adapters have symbols configured (minimum %d required). "
                "System will continue but may have limited opportunities.",
                active_adapters,
                MIN_EXCHANGES_REQUIRED
            )
        
        log.info("Starting quote aggregator for %d adapters (%d with symbols)", len(self._adapters), active_adapters)
        for adapter in self._adapters:
            symbols = self._symbols_by_exchange.get(adapter.name, [])
            if not symbols:
                log.warning("No symbols configured for adapter %s; skipping", adapter.name)
                continue
            log.info("Starting quote stream for %s with %d symbols", adapter.name, len(symbols))
            task = asyncio.create_task(self._run_adapter(adapter, symbols))
            task.set_name(f"quote-aggregator-{adapter.name}")
            self._tasks.append(task)

    async def stop(self) -> None:
        log.info("Stopping quote aggregator")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        log.info("Quote aggregator stopped")

    async def _run_adapter(self, adapter: ExchangeAdapter, symbols: Sequence[str]) -> None:
        """Run adapter with automatic retry on failures.
        
        Continues trying to reconnect even if adapter fails, ensuring system
        works with minimum 2 exchanges. Uses exponential backoff for retries.
        """
        quote_count = 0
        retry_delay = 5.0  # Start with 5 seconds
        max_retry_delay = 300.0  # Max 5 minutes
        consecutive_failures = 0
        
        while not adapter.closed:
            try:
                log.info("Starting quote stream for %s (attempt %d)", adapter.name, consecutive_failures + 1)
                async for quote in adapter.quote_stream(symbols):
                    # Reset retry delay on successful quote
                    if consecutive_failures > 0:
                        consecutive_failures = 0
                        retry_delay = 5.0
                        log.info("Quote stream recovered for %s", adapter.name)
                    
                    canonical = self._reverse_map.get((adapter.name, quote.symbol.upper()))
                    if not canonical:
                        continue
                    mid_price = (quote.bid + quote.ask) / 2
                    await self._quote_store.upsert(
                        canonical,
                        adapter.name,
                        mid_price,
                        timestamp_ms=quote.timestamp_ms,
                        native_symbol=quote.symbol.upper(),
                    )
                    quote_count += 1
                    if quote_count % 100 == 0:
                        log.debug("Received %d quotes from %s", quote_count, adapter.name)
            except asyncio.CancelledError:
                log.info("Quote stream cancelled for %s (received %d quotes)", adapter.name, quote_count)
                raise
            except Exception as exc:
                consecutive_failures += 1
                log.warning(
                    "Quote stream failed for %s (failure #%d, received %d quotes): %s. Retrying in %.1f seconds...",
                    adapter.name,
                    consecutive_failures,
                    quote_count,
                    exc,
                    retry_delay
                )
                # Exponential backoff with max limit
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, max_retry_delay)

    def update_markets(self, markets: Sequence[MarketInfo]) -> None:
        self._markets = list(markets)
        self._rebuild_mappings()

    async def refresh_markets(self, markets: Sequence[MarketInfo]) -> None:
        new_markets = list(markets)
        if self._markets == new_markets:
            return
        await self.stop()
        self.update_markets(new_markets)
        await self.start()

    def _rebuild_mappings(self) -> None:
        reverse: dict[tuple[str, str], str] = {}
        by_exchange: dict[str, list[str]] = {}
        for market in self._markets:
            for exchange, symbol in market.exchange_symbols.items():
                reverse[(exchange, symbol.upper())] = market.symbol
                by_exchange.setdefault(exchange, []).append(symbol.upper())
        self._reverse_map = reverse
        self._symbols_by_exchange = by_exchange

