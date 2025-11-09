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
            # Сначала получаем список всех доступных tickers для проверки
            # Это гарантирует, что мы добавляем только те рынки, которые реально торгуются на споте
            ticker_data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/market/tickers")
            available_tickers: set[str] = set()
            if isinstance(ticker_data, dict) and ticker_data.get("code") == "00000":
                ticker_items = ticker_data.get("data", [])
                available_tickers = {item.get("symbol", "").upper() for item in ticker_items if item.get("symbol")}
                self._log.debug("Found %d available tickers for verification", len(available_tickers))
            
            data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/public/products")
            
            # Проверяем структуру ответа
            if not isinstance(data, dict):
                self._log.error("Bitget API returned invalid response type: %s", type(data))
                return []
            
            # Проверяем код ответа
            code = data.get("code")
            if code and code != "00000":
                msg = data.get("msg", "Unknown error")
                self._log.error("Bitget API returned error code %s: %s", code, msg)
                return []
            
            items = data.get("data", [])
            if not items:
                self._log.warning("Bitget API returned empty data array")
                return []
            
            self._log.debug("Bitget API returned %d total items", len(items))
            
            markets: list[ExchangeMarket] = []
            filtered_by_quote = 0
            filtered_by_status = 0
            filtered_by_spbl = 0
            filtered_by_zk = 0
            filtered_by_ticker = 0
            
            for item in items:
                quote_coin = item.get("quoteCoin", "").upper()
                if quote_coin != "USDT":
                    filtered_by_quote += 1
                    continue
                
                # Bitget: /api/spot/v1/public/products возвращает только спотовые рынки
                # Проверяем статус - только "online" рынки активны
                status = item.get("status", "").lower()
                if status != "online":
                    filtered_by_status += 1
                    continue
                
                symbol = item.get("symbol", "")
                if not symbol:
                    continue
                
                # Bitget products API возвращает символы с суффиксом _SPBL для спотовых рынков
                # Но ticker API возвращает символы БЕЗ суффикса _SPBL
                # Удаляем суффикс для совместимости с ticker API
                if symbol.endswith("_SPBL"):
                    symbol = symbol[:-5]  # Удаляем "_SPBL"
                    filtered_by_spbl += 1  # Считаем как обработанные (не отфильтрованные)
                
                symbol_upper = symbol.upper()
                
                # КРИТИЧНО: Проверяем, что символ существует в ticker API
                # Это гарантирует, что рынок реально торгуется на споте
                if available_tickers and symbol_upper not in available_tickers:
                    filtered_by_ticker += 1
                    continue
                
                base_coin = item.get("baseCoin", "").upper()
                
                # КРИТИЧНО: На Bitget есть два разных ZK токена:
                # - ZKUSDT (0.07139) - это НЕ ZKSync
                # - ZKSYNCUSDT (0.05508) - это ZKSync (правильный токен)
                # Для canonical symbol "ZKUSDT" нужно использовать "ZKSYNCUSDT" на Bitget
                # Это специальный случай маппинга
                if base_coin == "ZK" and symbol_upper == "ZKUSDT":
                    filtered_by_zk += 1
                    continue
                
                markets.append(
                    ExchangeMarket(
                        symbol=symbol_upper,
                        base_asset=base_coin,
                        quote_asset="USDT",
                    )
                )
            
            self._log.info(
                "Fetched %d USDT markets from Bitget (filtered: %d by quote, %d by status, %d processed _SPBL suffix removal, %d not in tickers, %d by ZK)",
                len(markets),
                filtered_by_quote,
                filtered_by_status,
                filtered_by_spbl,
                filtered_by_ticker,
                filtered_by_zk,
            )
            return markets
        except Exception as e:
            self._log.error("Failed to fetch Bitget markets: %s", e, exc_info=True)
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
                
                # Проверяем структуру ответа
                if not isinstance(data, dict):
                    self._log.error("Bitget API returned invalid response type: %s", type(data))
                    await self.wait_interval()
                    continue
                
                # Проверяем код ответа (Bitget API может возвращать код ошибки в поле "code")
                code = data.get("code")
                if code and code != "00000":  # "00000" означает успех в Bitget API
                    msg = data.get("msg", "Unknown error")
                    self._log.error("Bitget API returned error code %s: %s", code, msg)
                    await self.wait_interval()
                    continue
                
                entries = data.get("data", [])
                if not entries:
                    self._log.warning("Bitget API returned empty data array")
                    await self.wait_interval()
                    continue
                
                ts = int(time.time() * 1000)
                quotes_yielded = 0
                
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
                        quotes_yielded += 1
                
                if quotes_yielded == 0:
                    self._log.debug("No quotes yielded for watched symbols (watched: %d, total entries: %d)", len(watched), len(entries))
                    
            except Exception as e:
                self._log.warning("Failed to fetch quotes from Bitget: %s (will retry)", e, exc_info=True)
                # Пробрасываем исключение дальше, чтобы quote_aggregator мог обработать его
                raise
            
            await self.wait_interval()
