# Логика системы пошагово

## Поток данных:

### 1. Exchange Adapters (mexc.py, bybit.py и т.д.)
- Получают котировки через REST API в `quote_stream()`
- MEXC получает 935 символов каждые 3 секунды
- Возвращают `AsyncIterator[ExchangeQuote]`

### 2. QuoteAggregator._exchange_worker (для каждой биржи)
- Вызывает `adapter.quote_stream(symbols)`
- Получает котировки через `async for quote in adapter.quote_stream(symbols)`
- Кладет их в очередь: `self._quote_queue.put_nowait((adapter.name, quote))`
- Очередь: `asyncio.Queue(maxsize=10000)`
- Если очередь переполнена - пропускает котировку

### 3. QuoteAggregator._process_quotes_worker (один воркер для всех бирж)
- Берет котировки из очереди пакетами по 100
- Для каждой котировки:
  - Делает маппинг символа
  - Вызывает `await self._quote_store.upsert(...)` ← **БЛОКИРУЕТСЯ НА asyncio.Lock()**
  - Обновляет статус биржи через `async with self._status_lock:`

### 4. QuoteStore.upsert()
- Использует `async with self._lock:` ← **БЛОКИРУЕТСЯ**
- Обновляет `self._quotes[symbol]`

### 5. ArbitrageEngine.evaluate() (вызывается каждую секунду)
- Вызывает `await self._quote_store.list()` ← **БЛОКИРУЕТСЯ НА asyncio.Lock()**
- QuoteStore.list() использует `async with self._lock:`
- Вычисляет возможности
- Сохраняет в `self._latest` через `async with self._lock:`

### 6. Web app.emit_loop() (отдельный поток, отдельный event loop)
- Вызывает `loop.run_until_complete(arbitrage_engine.get_latest())` ← **БЛОКИРУЕТСЯ НА asyncio.Lock()**
- ArbitrageEngine.get_latest() использует `async with self._lock:`
- Отправляет через WebSocket

## ПРОБЛЕМА:

Когда MEXC получает 935 котировок:
1. `_exchange_worker` быстро кладет их в очередь (put_nowait)
2. `_process_quotes_worker` начинает обрабатывать пакет из 100 котировок
3. Для каждой котировки вызывает `quote_store.upsert()` который держит `QuoteStore._lock`
4. Пока обрабатывается пакет, `ArbitrageEngine.evaluate()` не может вызвать `quote_store.list()` - блокируется на том же lock!
5. `emit_loop` тоже блокируется при вызове `get_latest()`

## РЕШЕНИЕ:

Нужно минимизировать время удержания lock в QuoteStore:
- Обрабатывать пакет котировок БЕЗ lock
- Брать lock только для финального обновления словаря
- Или использовать threading.Lock вместо asyncio.Lock для чтения

