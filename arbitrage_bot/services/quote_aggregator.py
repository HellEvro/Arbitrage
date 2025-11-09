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
    def __init__(self, adapters: Sequence[ExchangeAdapter], quote_store: QuoteStore, markets: Sequence[MarketInfo], exchange_enabled: dict[str, bool] | None = None) -> None:
        self._adapters = adapters
        self._quote_store = quote_store
        self._markets = list(markets)
        self._tasks: list[asyncio.Task[None]] = []
        # Mapping exchange name -> task for dynamic start/stop
        self._exchange_tasks: dict[str, asyncio.Task[None]] = {}
        self._reverse_map: dict[tuple[str, str], str] = {}
        self._symbols_by_exchange: dict[str, list[str]] = {}
        # Mapping canonical symbol -> (base_asset, quote_asset)
        self._symbol_to_assets: dict[str, tuple[str, str]] = {}
        # Exchange status tracking
        self._exchange_status: dict[str, ExchangeStatus] = {}
        self._status_lock = asyncio.Lock()
        # Track unique symbols per exchange (for quote_count)
        self._exchange_symbols: dict[str, set[str]] = {}
        # Exchange enabled/disabled state
        self._exchange_enabled: dict[str, bool] = exchange_enabled or {}
        # Store reference to the main event loop (set when start() is called)
        self._main_loop: asyncio.AbstractEventLoop | None = None
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
            # Initialize enabled state (default True if not specified)
            if adapter.name not in self._exchange_enabled:
                self._exchange_enabled[adapter.name] = True

    async def start(self) -> None:
        if self._tasks:
            log.warning("Quote aggregator already started")
            return
        
        # Сохраняем ссылку на основной event loop
        try:
            self._main_loop = asyncio.get_running_loop()
        except RuntimeError:
            # Если нет running loop, попробуем получить текущий
            try:
                self._main_loop = asyncio.get_event_loop()
            except RuntimeError:
                self._main_loop = None
        
        # Count enabled adapters with symbols
        active_adapters = sum(
            1 for adapter in self._adapters
            if self._exchange_enabled.get(adapter.name, True) and self._symbols_by_exchange.get(adapter.name, [])
        )
        
        MIN_EXCHANGES_REQUIRED = 2
        if active_adapters < MIN_EXCHANGES_REQUIRED:
            log.warning(
                "Only %d enabled adapters have symbols configured (minimum %d required). "
                "System will continue but may have limited opportunities.",
                active_adapters,
                MIN_EXCHANGES_REQUIRED
            )
        
        log.info("Starting quote aggregator for %d adapters (%d enabled with symbols)", len(self._adapters), active_adapters)
        for adapter in self._adapters:
            # Skip disabled exchanges
            if not self._exchange_enabled.get(adapter.name, True):
                log.info("Skipping disabled exchange: %s", adapter.name)
                continue
                
            symbols = self._symbols_by_exchange.get(adapter.name, [])
            if not symbols:
                log.warning("No symbols configured for adapter %s; skipping", adapter.name)
                continue
            log.info("Starting quote stream for %s with %d symbols", adapter.name, len(symbols))
            task = asyncio.create_task(self._run_adapter(adapter, symbols))
            task.set_name(f"quote-aggregator-{adapter.name}")
            self._tasks.append(task)
            self._exchange_tasks[adapter.name] = task

    async def stop(self) -> None:
        log.info("Stopping quote aggregator")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self._exchange_tasks.clear()
        log.info("Quote aggregator stopped")
    
    async def start_exchange(self, exchange_name: str) -> bool:
        """Start quote stream for a specific exchange."""
        adapter = next((a for a in self._adapters if a.name == exchange_name), None)
        if not adapter:
            log.warning("Exchange %s not found", exchange_name)
            return False
        
        if exchange_name in self._exchange_tasks:
            log.warning("Exchange %s already running", exchange_name)
            return False
        
        symbols = self._symbols_by_exchange.get(exchange_name, [])
        if not symbols:
            log.warning("No symbols configured for exchange %s", exchange_name)
            return False
        
        self._exchange_enabled[exchange_name] = True
        log.info("Starting quote stream for %s with %d symbols", exchange_name, len(symbols))
        
        # Используем основной loop для создания задачи
        loop = self._main_loop
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
        
        task = loop.create_task(self._run_adapter(adapter, symbols))
        task.set_name(f"quote-aggregator-{exchange_name}")
        self._tasks.append(task)
        self._exchange_tasks[exchange_name] = task
        return True
    
    async def stop_exchange(self, exchange_name: str) -> bool:
        """Stop quote stream for a specific exchange."""
        task = self._exchange_tasks.get(exchange_name)
        if not task:
            log.warning("Exchange %s not running", exchange_name)
            return False
        
        self._exchange_enabled[exchange_name] = False
        log.info("Stopping quote stream for %s", exchange_name)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        
        # Remove task from lists if it exists
        if task in self._tasks:
            self._tasks.remove(task)
        if exchange_name in self._exchange_tasks:
            del self._exchange_tasks[exchange_name]
        
        # Удаляем данные этой биржи из QuoteStore
        await self._quote_store.remove_exchange(exchange_name)
        
        # Clear quotes for this exchange
        async with self._status_lock:
            self._exchange_symbols[exchange_name].clear()
            status = self._exchange_status[exchange_name]
            status.connected = False
            status.quote_count = 0
        
        log.info("Exchange %s stopped and data removed from QuoteStore", exchange_name)
        return True
    
    def update_exchange_enabled(self, exchange_enabled: dict[str, bool]) -> None:
        """Update exchange enabled/disabled state."""
        self._exchange_enabled.update(exchange_enabled)

    async def _run_adapter(self, adapter: ExchangeAdapter, symbols: Sequence[str]) -> None:
        """Run adapter with automatic retry on failures.
        
        Continues trying to reconnect even if adapter fails, ensuring system
        works with minimum 2 exchanges. Uses fixed retry delay (5-10 seconds).
        """
        retry_delay = 5.0  # Fixed retry delay: 5 seconds
        retry_delay_403 = 10.0  # Fixed delay for 403 errors: 10 seconds
        consecutive_failures = 0
        
        while not adapter.closed:
            try:
                log.info("Starting quote stream for %s (attempt %d)", adapter.name, consecutive_failures + 1)
                async for quote in adapter.quote_stream(symbols):
                    # Reset failure counter on successful quote
                    if consecutive_failures > 0:
                        consecutive_failures = 0
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
                    log.warning(
                        "Quote stream failed for %s with 403 Forbidden (failure #%d, tracking %d symbols). "
                        "IP may be blocked. Waiting %.1f seconds before retry...",
                        adapter.name,
                        consecutive_failures,
                        unique_count,
                        retry_delay_403
                    )
                    await asyncio.sleep(retry_delay_403)
                else:
                    log.warning(
                        "Quote stream failed for %s (failure #%d, tracking %d symbols): %s. Retrying in %.1f seconds...",
                        adapter.name,
                        consecutive_failures,
                        unique_count,
                        exc,
                        retry_delay
                    )
                    # Fixed retry delay (no exponential backoff)
                    await asyncio.sleep(retry_delay)

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

