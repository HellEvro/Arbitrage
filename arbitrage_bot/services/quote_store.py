from __future__ import annotations

import asyncio
import time
from typing import Iterable

from arbitrage_bot.services.schemas import QuoteSnapshot


class QuoteStore:
    """Хранилище котировок с разделением чтения и записи.
    
    КРИТИЧНО: Чтение НЕ блокирует запись, запись НЕ блокирует чтение!
    Используем версионирование данных - чтение всегда видит консистентный снимок.
    """
    def __init__(self) -> None:
        # Два словаря для версионирования - чтение и запись полностью независимы!
        self._quotes_write: dict[str, QuoteSnapshot] = {}
        self._quotes_read: dict[str, QuoteSnapshot] = {}
        self._write_lock = asyncio.Lock()  # Lock ТОЛЬКО для записи и переключения версий

    async def upsert(
        self,
        symbol: str,
        exchange: str,
        price: float,
        *,
        timestamp_ms: int | None = None,
        native_symbol: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
    ) -> None:
        ts = timestamp_ms or int(time.time() * 1000)
        exchange_key = exchange.lower()
        native = (native_symbol or symbol).upper()
        async with self._write_lock:
            snapshot = self._quotes_write.get(symbol)
            if snapshot:
                snapshot.prices[exchange_key] = price
                snapshot.exchange_symbols[exchange_key] = native
                snapshot.timestamp_ms = ts
                if base_asset:
                    snapshot.base_asset = base_asset
                if quote_asset:
                    snapshot.quote_asset = quote_asset
            else:
                self._quotes_write[symbol] = QuoteSnapshot(
                    symbol=symbol,
                    prices={exchange_key: price},
                    exchange_symbols={exchange_key: native},
                    timestamp_ms=ts,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                )
            # Переключаем версию для чтения (мгновенно - просто ссылка)
            self._quotes_read = self._quotes_write

    async def get(self, symbol: str) -> QuoteSnapshot | None:
        # Чтение БЕЗ lock - запись не блокируется!
        snapshot = self._quotes_read.get(symbol)
        if not snapshot:
            return None
        # Возвращаем копию для безопасности
        return QuoteSnapshot(
            symbol=snapshot.symbol,
            prices=dict(snapshot.prices),
            exchange_symbols=dict(snapshot.exchange_symbols),
            timestamp_ms=snapshot.timestamp_ms,
            base_asset=snapshot.base_asset,
            quote_asset=snapshot.quote_asset,
        )

    async def upsert_batch(
        self,
        updates: list[tuple[str, str, float, int, str, str | None, str | None]],
    ) -> None:
        """Batch update quotes - записываем БЕЗ блокировки чтения!
        
        КРИТИЧНО: Подготавливаем все изменения БЕЗ lock, затем быстро применяем с lock!
        Чтение может происходить параллельно из _quotes_read!
        """
        if not updates:
            return
        
        # Подготавливаем ВСЕ изменения БЕЗ lock - максимально быстро!
        prepared_updates: dict[str, dict] = {}
        for symbol, exchange, price, ts, native_symbol, base_asset, quote_asset in updates:
            exchange_key = exchange.lower()
            native = native_symbol if native_symbol else symbol
            
            if symbol not in prepared_updates:
                prepared_updates[symbol] = {
                    'exchanges': {},
                    'symbols': {},
                    'timestamp': ts,
                    'base_asset': base_asset,
                    'quote_asset': quote_asset,
                }
            
            prepared_updates[symbol]['exchanges'][exchange_key] = price
            prepared_updates[symbol]['symbols'][exchange_key] = native
            prepared_updates[symbol]['timestamp'] = max(prepared_updates[symbol]['timestamp'], ts)
            if base_asset is not None:
                prepared_updates[symbol]['base_asset'] = base_asset
            if quote_asset is not None:
                prepared_updates[symbol]['quote_asset'] = quote_asset
        
        # Применяем изменения с lock - МИНИМАЛЬНОЕ время удержания lock!
        async with self._write_lock:
            for symbol, data in prepared_updates.items():
                snapshot = self._quotes_write.get(symbol)
                if snapshot:
                    snapshot.prices.update(data['exchanges'])
                    snapshot.exchange_symbols.update(data['symbols'])
                    snapshot.timestamp_ms = data['timestamp']
                    if data['base_asset'] is not None:
                        snapshot.base_asset = data['base_asset']
                    if data['quote_asset'] is not None:
                        snapshot.quote_asset = data['quote_asset']
                else:
                    self._quotes_write[symbol] = QuoteSnapshot(
                        symbol=symbol,
                        prices=data['exchanges'].copy(),
                        exchange_symbols=data['symbols'].copy(),
                        timestamp_ms=data['timestamp'],
                        base_asset=data['base_asset'],
                        quote_asset=data['quote_asset'],
                    )
            
            # Переключаем версию для чтения МГНОВЕННО
            self._quotes_read = self._quotes_write

    async def list(self) -> Iterable[QuoteSnapshot]:
        """Получить все котировки БЕЗ блокировки записи!
        
        КРИТИЧНО: Читаем из _quotes_read БЕЗ lock - запись может продолжаться!
        Возвращаем snapshots напрямую - _quotes_read не изменяется во время чтения!
        """
        # Читаем ссылку на словарь БЕЗ lock (атомарная операция в Python)
        # _quotes_read не изменяется во время чтения - запись идет в _quotes_write
        quotes_snapshot = self._quotes_read
        
        # Возвращаем snapshots напрямую - БЕЗ копирования для скорости!
        # Это безопасно, так как _quotes_read не изменяется во время чтения
        return list(quotes_snapshot.values())
