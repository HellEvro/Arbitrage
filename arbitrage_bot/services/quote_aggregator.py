from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass

from arbitrage_bot.core.exceptions import AggregationError
from arbitrage_bot.exchanges import ExchangeAdapter
from arbitrage_bot.services.quote_store import QuoteStore
from arbitrage_bot.services.schemas import MarketInfo

log = logging.getLogger(__name__)


@dataclass
class ExchangeStatus:
    """Status information for an exchange."""
    name: str
    connected: bool
    last_update_ms: int | None
    quote_count: int
    error_count: int
    last_error: str | None


class QuoteAggregator:
    def __init__(self, adapters: Sequence[ExchangeAdapter], quote_store: QuoteStore, markets: Sequence[MarketInfo]) -> None:
        self._adapters = adapters
        self._quote_store = quote_store
        self._markets = list(markets)
        self._tasks: list[asyncio.Task[None]] = []
        self._reverse_map: dict[tuple[str, str], str] = {}
        self._symbols_by_exchange: dict[str, list[str]] = {}
        # Mapping canonical symbol -> (base_asset, quote_asset)
        self._symbol_to_assets: dict[str, tuple[str, str]] = {}
        # Exchange status tracking
        self._exchange_status: dict[str, ExchangeStatus] = {}
        self._status_lock = asyncio.Lock()
        # Track unique symbols per exchange (for quote_count)
        self._exchange_symbols: dict[str, set[str]] = {}
        self._rebuild_mappings()
        
        # Initialize status for all adapters
        for adapter in adapters:
            self._exchange_status[adapter.name] = ExchangeStatus(
                name=adapter.name,
                connected=False,
                last_update_ms=None,
                quote_count=0,
                error_count=0,
                last_error=None,
            )
            self._exchange_symbols[adapter.name] = set()

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
                    # Получаем base_asset и quote_asset для этого символа
                    base_asset, quote_asset = self._symbol_to_assets.get(canonical, (None, None))
                    await self._quote_store.upsert(
                        canonical,
                        adapter.name,
                        mid_price,
                        timestamp_ms=quote.timestamp_ms,
                        native_symbol=quote.symbol.upper(),
                        base_asset=base_asset,
                        quote_asset=quote_asset,
                    )
                    
                    # Track unique symbols for this exchange
                    async with self._status_lock:
                        self._exchange_symbols[adapter.name].add(canonical)
                        status = self._exchange_status[adapter.name]
                        status.connected = True
                        status.last_update_ms = quote.timestamp_ms
                        status.quote_count = len(self._exchange_symbols[adapter.name])  # Count unique symbols
                        status.error_count = 0
                        status.last_error = None
                    
                    unique_count = len(self._exchange_symbols[adapter.name])
                    if unique_count > 0 and unique_count % 10 == 0:
                        log.debug("Tracking %d unique symbols from %s", unique_count, adapter.name)
            except asyncio.CancelledError:
                async with self._status_lock:
                    unique_count = len(self._exchange_symbols[adapter.name])
                log.info("Quote stream cancelled for %s (tracking %d unique symbols)", adapter.name, unique_count)
                raise
            except Exception as exc:
                consecutive_failures += 1
                error_msg = str(exc)[:100]  # Limit error message length
                
                # Update exchange status
                async with self._status_lock:
                    unique_count = len(self._exchange_symbols[adapter.name])
                    status = self._exchange_status[adapter.name]
                    status.connected = False
                    status.error_count = consecutive_failures
                    status.last_error = error_msg
                
                # Check if it's a 403 error (IP blocked)
                is_403 = hasattr(exc, 'status') and exc.status == 403
                if is_403:
                    # For 403 errors, wait longer before retry
                    retry_delay_403 = min(retry_delay * 3, max_retry_delay)
                    log.warning(
                        "Quote stream failed for %s with 403 Forbidden (failure #%d, tracking %d symbols). "
                        "IP may be blocked. Waiting %.1f seconds before retry...",
                        adapter.name,
                        consecutive_failures,
                        unique_count,
                        retry_delay_403
                    )
                    await asyncio.sleep(retry_delay_403)
                    retry_delay = min(retry_delay * 1.5, max_retry_delay)
                else:
                    log.warning(
                        "Quote stream failed for %s (failure #%d, tracking %d symbols): %s. Retrying in %.1f seconds...",
                        adapter.name,
                        consecutive_failures,
                        unique_count,
                        exc,
                        retry_delay
                    )
                    # Exponential backoff with max limit
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(retry_delay * 1.5, max_retry_delay)

    def update_markets(self, markets: Sequence[MarketInfo]) -> None:
        self._markets = list(markets)
        self._rebuild_mappings()
        # Reset symbol tracking when markets change (without lock - called from sync context)
        for exchange_name in self._exchange_symbols:
            self._exchange_symbols[exchange_name].clear()

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
        symbol_to_assets: dict[str, tuple[str, str]] = {}
        for market in self._markets:
            # Извлекаем base_asset и quote_asset из canonical symbol (например, "GAMEUSDT" -> "GAME", "USDT")
            canonical = market.symbol.upper()
            if canonical.endswith("USDT"):
                base_asset = canonical[:-4]  # Убираем "USDT"
                quote_asset = "USDT"
            else:
                # Fallback: пытаемся разделить по последним 4 символам
                base_asset = canonical
                quote_asset = "USDT"
            symbol_to_assets[canonical] = (base_asset, quote_asset)
            
            for exchange, symbol in market.exchange_symbols.items():
                reverse[(exchange, symbol.upper())] = market.symbol
                by_exchange.setdefault(exchange, []).append(symbol.upper())
        self._reverse_map = reverse
        self._symbols_by_exchange = by_exchange
        self._symbol_to_assets = symbol_to_assets
    
    async def get_exchange_status(self) -> dict[str, ExchangeStatus]:
        """Get current status of all exchanges."""
        async with self._status_lock:
            # Check for stale connections (no update in last 5 seconds)
            now_ms = int(time.time() * 1000)
            stale_threshold_ms = 5000
            
            result = {}
            for name, status in self._exchange_status.items():
                # If connected but no update in last 5 seconds, mark as disconnected
                if status.connected and status.last_update_ms:
                    if now_ms - status.last_update_ms > stale_threshold_ms:
                        status.connected = False
                
                # Update quote_count from current unique symbols count
                current_quote_count = len(self._exchange_symbols.get(name, set()))
                
                result[name] = ExchangeStatus(
                    name=status.name,
                    connected=status.connected,
                    last_update_ms=status.last_update_ms,
                    quote_count=current_quote_count,  # Use current count of unique symbols
                    error_count=status.error_count,
                    last_error=status.last_error,
                )
            return result

