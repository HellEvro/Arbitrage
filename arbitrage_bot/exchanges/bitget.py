from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class BitgetAdapter(BaseAdapter):
    """
    Bitget exchange adapter using public REST API endpoints.
    Simple implementation - just like MEXC.
    """
    name = "bitget"
    _REST_BASE = "https://api.bitget.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from Bitget")
        try:
            data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/public/products")
            items = data.get("data", [])
            markets: list[ExchangeMarket] = []
            for item in items:
                if item.get("quoteCoin", "").upper() != "USDT":
                    continue
                # Bitget: /api/spot/v1/public/products возвращает только спотовые рынки
                # Проверяем статус - только "online" рынки активны
                status = item.get("status", "").lower()
                if status != "online":
                    continue
                symbol = item.get("symbol", "")
                if not symbol:
                    continue
                # Bitget products with _SPBL suffix are NOT spot products (they're margin/futures/etc)
                # Skip them entirely - they don't exist on spot trading
                if symbol.endswith("_SPBL"):
                    continue
                
                base_coin = item.get("baseCoin", "").upper()
                
                # КРИТИЧНО: На Bitget есть два разных ZK токена:
                # - ZKUSDT (0.07139) - это НЕ ZKSync
                # - ZKSYNCUSDT (0.05508) - это ZKSync (правильный токен)
                # Для canonical symbol "ZKUSDT" нужно использовать "ZKSYNCUSDT" на Bitget
                # Это специальный случай маппинга
                if base_coin == "ZK" and symbol.upper() == "ZKUSDT":
                    # Пропускаем ZKUSDT, используем только ZKSYNCUSDT для ZK
                    continue
                
                markets.append(
                    ExchangeMarket(
                        symbol=symbol.upper(),
                        base_asset=base_coin,
                        quote_asset="USDT",
                    )
                )
            self._log.info("Fetched %d USDT markets from Bitget", len(markets))
            return markets
        except Exception as e:
            self._log.error("Failed to fetch Bitget markets: %s", e)
            return []

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        
        while not self.closed:
            try:
                data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/market/tickers")
                
                entries = data.get("data", [])
                if not entries:
                    self._log.warning("Bitget API returned empty data array")
                    await self.wait_interval()
                    continue
                
                ts = int(time.time() * 1000)
                
                for item in entries:
                    symbol_raw = item.get("symbol")
                    if not symbol_raw:
                        continue
                    symbol = symbol_raw.upper()
                    
                    # Process ONLY watched symbols
                    if symbol not in watched:
                        continue
                    
                    # Bitget API uses "buyOne" (bid) and "sellOne" (ask) fields
                    bid = self._to_float(item.get("buyOne"))
                    ask = self._to_float(item.get("sellOne"))
                    
                    if bid > 0 and ask > 0:
                        yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
                    
            except Exception as e:
                self._log.warning("Failed to fetch quotes from Bitget: %s (will retry)", e)
            
            await self.wait_interval()
