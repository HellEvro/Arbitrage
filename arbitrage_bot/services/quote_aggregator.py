from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass

from arbitrage_bot.core.exceptions import AggregationError
from arbitrage_bot.exchanges import ExchangeAdapter, ExchangeQuote
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
        # Очередь для передачи котировок от воркеров бирж к воркеру обработки
        self._quote_queue: asyncio.Queue[tuple[str, ExchangeQuote]] = asyncio.Queue(maxsize=10000)
        # Воркер для обработки котировок
        self._processor_task: asyncio.Task[None] | None = None
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
        
        # Запускаем воркер для обработки котировок
        self._processor_task = asyncio.create_task(self._process_quotes_worker())
        self._processor_task.set_name("quote-processor")
        
        # Запускаем отдельный воркер для каждой биржи (только получение данных)
        for adapter in self._adapters:
            symbols = self._symbols_by_exchange.get(adapter.name, [])
            if not symbols:
                log.warning("No symbols configured for adapter %s; skipping", adapter.name)
                continue
            log.info("Starting quote stream worker for %s with %d symbols", adapter.name, len(symbols))
            task = asyncio.create_task(self._exchange_worker(adapter, symbols))
            task.set_name(f"exchange-worker-{adapter.name}")
            self._tasks.append(task)

    async def stop(self) -> None:
        log.info("Stopping quote aggregator")
        # Останавливаем воркер обработки
        if self._processor_task:
            self._processor_task.cancel()
        # Останавливаем воркеры бирж
        for task in self._tasks:
            task.cancel()
        # Ждем завершения всех задач
        tasks_to_wait = list(self._tasks)
        if self._processor_task:
            tasks_to_wait.append(self._processor_task)
        await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        self._tasks.clear()
        self._processor_task = None
        log.info("Quote aggregator stopped")
    
    async def _exchange_worker(self, adapter: ExchangeAdapter, symbols: Sequence[str]) -> None:
        """Отдельный воркер для каждой биржи - только получает данные и кладет в очередь.
        
        Не блокирует другие биржи - просто получает котировки и передает их в очередь.
        Система продолжает работать даже если биржа не работает.
        """
        retry_delay = 5.0  # Fixed retry delay: 5 seconds
        retry_delay_403 = 10.0  # Fixed delay for 403 errors: 10 seconds
        consecutive_failures = 0
        
        # Все биржи работают одинаково - через обычный async worker
        while not adapter.closed:
            try:
                log.info("Starting quote stream for %s (attempt %d)", adapter.name, consecutive_failures + 1)
                quote_count = 0
                async for quote in adapter.quote_stream(symbols):
                    quote_count += 1
                    # Reset failure counter on successful quote
                    if consecutive_failures > 0:
                        consecutive_failures = 0
                        log.info("Quote stream recovered for %s", adapter.name)
                    
                    # Просто кладем котировку в очередь - обработка будет в отдельном воркере
                    try:
                        # Неблокирующая попытка положить в очередь
                        try:
                            self._quote_queue.put_nowait((adapter.name, quote))
                        except asyncio.QueueFull:
                            # Если очередь переполнена - пропускаем эту котировку (не критично)
                            log.debug("Quote queue full for %s, skipping quote", adapter.name)
                            continue
                    except Exception as e:
                        log.debug("Failed to put quote in queue for %s: %s (non-critical)", adapter.name, e)
                        # Продолжаем работу даже при ошибке
                        continue
                    
            except asyncio.CancelledError:
                async with self._status_lock:
                    unique_count = len(self._exchange_symbols[adapter.name])
                log.info("Quote stream cancelled for %s (tracking %d unique symbols)", adapter.name, unique_count)
                raise
            except Exception as exc:
                consecutive_failures += 1
                error_msg = str(exc)[:100]  # Limit error message length
                
                # Update exchange status (не блокируем систему!)
                try:
                    async with self._status_lock:
                        unique_count = len(self._exchange_symbols[adapter.name])
                        status = self._exchange_status[adapter.name]
                        status.connected = False
                        status.error_count = consecutive_failures
                        status.last_error = error_msg
                except Exception as status_error:
                    log.debug("Failed to update status for %s: %s (non-critical)", adapter.name, status_error)
                
                # Check if it's a 403 error (IP blocked)
                is_403 = hasattr(exc, 'status') and exc.status == 403
                if is_403:
                    log.warning(
                        "Quote stream failed for %s with 403 Forbidden (failure #%d). "
                        "IP may be blocked. Waiting %.1f seconds before retry...",
                        adapter.name,
                        consecutive_failures,
                        retry_delay_403
                    )
                    # Ждем меньшими порциями, чтобы не блокировать
                    waited = 0.0
                    while waited < retry_delay_403 and not adapter.closed:
                        sleep_time = min(2.0, retry_delay_403 - waited)
                        await asyncio.sleep(sleep_time)
                        waited += sleep_time
                else:
                    log.warning(
                        "Quote stream failed for %s (failure #%d): %s. Retrying in %.1f seconds...",
                        adapter.name,
                        consecutive_failures,
                        exc,
                        retry_delay
                    )
                    # Ждем меньшими порциями, чтобы не блокировать
                    waited = 0.0
                    while waited < retry_delay and not adapter.closed:
                        sleep_time = min(2.0, retry_delay - waited)
                        await asyncio.sleep(sleep_time)
                        waited += sleep_time
                
                # Продолжаем работу - система должна работать даже без этой биржи!
                continue
    
    async def _process_quotes_worker(self) -> None:
        """Отдельный воркер для обработки котировок из очереди.
        
        Берет котировки из очереди и обрабатывает их (маппинг, сохранение, обновление статуса).
        Работает постоянно, даже если очередь пустая или биржи не работают.
        Обрабатывает котировки пакетами, чтобы не блокировать event loop.
        """
        log.info("Starting quote processor worker")
        processed_count = 0
        
        while True:
            try:
                # Собираем все доступные котировки из очереди БЕЗ ограничений
                batch = []
                try:
                    # Собираем все что есть в очереди за один раз
                    while True:
                        exchange_name, quote = self._quote_queue.get_nowait()
                        batch.append((exchange_name, quote))
                except asyncio.QueueEmpty:
                    pass
                
                # Если очередь пустая, ждем немного
                if not batch:
                    if all(adapter.closed for adapter in self._adapters):
                        log.info("All adapters closed, stopping processor worker")
                        return
                    await asyncio.sleep(0.001)  # Минимальная задержка только если очередь пустая
                    continue
                
                # Обрабатываем весь батч БЕЗ задержек - максимально быстро!
                updates: list[tuple[str, str, float, int, str, str | None, str | None]] = []
                status_updates: dict[str, tuple[int, set[str]]] = {}
                
                # Быстрая синхронная обработка всего батча
                for exchange_name, quote in batch:
                    canonical = self._reverse_map.get((exchange_name, quote.symbol))
                    if not canonical:
                        continue
                    
                    mid_price = (quote.bid + quote.ask) * 0.5
                    base_asset, quote_asset = self._symbol_to_assets.get(canonical, (None, None))
                    
                    updates.append((
                        canonical,
                        exchange_name,
                        mid_price,
                        quote.timestamp_ms,
                        quote.symbol,
                        base_asset,
                        quote_asset,
                    ))
                    
                    status_data = status_updates.get(exchange_name)
                    if status_data is None:
                        status_updates[exchange_name] = (quote.timestamp_ms, {canonical})
                    else:
                        last_ms, symbols_set = status_data
                        symbols_set.add(canonical)
                        status_updates[exchange_name] = (max(last_ms, quote.timestamp_ms), symbols_set)
                    
                    processed_count += 1
                
                # Пакетно обновляем QuoteStore - БЕЗ задержек!
                if updates:
                    await self._quote_store.upsert_batch(updates)
                
                # Пакетно обновляем статусы бирж
                async with self._status_lock:
                    for exchange_name, (last_ms, symbols_set) in status_updates.items():
                        self._exchange_symbols[exchange_name].update(symbols_set)
                        status = self._exchange_status[exchange_name]
                        status.connected = True
                        status.last_update_ms = last_ms
                        status.quote_count = len(self._exchange_symbols[exchange_name])
                        status.error_count = 0
                        status.last_error = None
                
                if processed_count % 500 == 0:
                    log.debug("Processed %d quotes total", processed_count)
                        
            except asyncio.CancelledError:
                log.info("Quote processor worker cancelled")
                break
            except Exception as exc:
                log.warning("Unexpected error in quote processor worker: %s (continuing)", exc)
                await asyncio.sleep(0.1)
        
        log.info("Quote processor worker stopped (processed %d quotes)", processed_count)

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

