from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class MexcAdapter(BaseAdapter):
    """
    MEXC exchange adapter using public REST API endpoints.
    Simple implementation - just like other exchanges.
    """
    name = "mexc"
    _REST_BASE = "https://api.mexc.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from MEXC")
        try:
            data = await self._http.get_json(f"{self._REST_BASE}/api/v3/exchangeInfo")
            symbols = data.get("symbols", [])
            markets: list[ExchangeMarket] = []
            for item in symbols:
                status = item.get("status")
                quote_asset = item.get("quoteAsset", "").upper()
                is_spot_trading = item.get("isSpotTradingAllowed", False)
                if status != "1" or not is_spot_trading:
                    continue
                if quote_asset != "USDT":
                    continue
                markets.append(
                    ExchangeMarket(
                        symbol=item.get("symbol", "").upper(),
                        base_asset=item.get("baseAsset", "").upper(),
                        quote_asset="USDT",
                    )
                )
            self._log.info("Fetched %d USDT markets from MEXC", len(markets))
            return markets
        except Exception as e:
            self._log.error("Failed to fetch MEXC markets: %s", e)
            # Fallback: use ticker endpoint
            try:
                tickers = await self._http.get_json(f"{self._REST_BASE}/api/v3/ticker/24hr")
                markets: list[ExchangeMarket] = []
                seen_symbols = set()
                for ticker in tickers:
                    symbol = ticker.get("symbol", "").upper()
                    if not symbol or symbol in seen_symbols:
                        continue
                    if not symbol.endswith("USDT"):
                        continue
                    seen_symbols.add(symbol)
                    base = symbol.replace("USDT", "")
                    markets.append(
                        ExchangeMarket(
                            symbol=symbol,
                            base_asset=base,
                            quote_asset="USDT",
                        )
                    )
                self._log.info("Fetched %d USDT markets from MEXC (via ticker fallback)", len(markets))
                return markets
            except Exception as e2:
                self._log.error("Both exchangeInfo and ticker fallback failed: %s", e2)
                return []

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        
        while not self.closed:
            try:
                data = await self._http.get_json(f"{self._REST_BASE}/api/v3/ticker/24hr")
                
                if not isinstance(data, list):
                    self._log.warning("Unexpected data format from MEXC: %s", type(data))
                    await self.wait_interval()
                    continue
                
                ts = int(time.time() * 1000)
                
                for item in data:
                    symbol_raw = item.get("symbol")
                    if not symbol_raw:
                        continue
                    symbol = symbol_raw.upper()
                    
                    # Process ONLY watched symbols
                    if symbol not in watched:
                        continue
                    
                    bid = self._to_float(item.get("bidPrice"))
                    ask = self._to_float(item.get("askPrice"))
                    
                    if bid > 0 and ask > 0:
                        close_time = item.get("closeTime")
                        if close_time and isinstance(close_time, (int, float)):
                            ts = int(close_time)
                        yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
                    
            except Exception as e:
                self._log.warning("Failed to fetch quotes from MEXC: %s (will retry)", e)
            
            await self.wait_interval()
