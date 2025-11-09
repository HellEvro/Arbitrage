from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class KucoinAdapter(BaseAdapter):
    """
    KuCoin exchange adapter using public REST API endpoints.
    Simple implementation - just like MEXC.
    """
    name = "kucoin"
    _REST_BASE = "https://api.kucoin.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from KuCoin")
        try:
            data = await self._http.get_json(f"{self._REST_BASE}/api/v1/symbols")
            items = data.get("data", [])
            markets: list[ExchangeMarket] = []
            for item in items:
                if item.get("quoteCurrency", "").upper() != "USDT":
                    continue
                if item.get("enableTrading") is not True:
                    continue
                markets.append(
                    ExchangeMarket(
                        symbol=item.get("symbol", "").upper(),
                        base_asset=item.get("baseCurrency", "").upper(),
                        quote_asset="USDT",
                    )
                )
            self._log.info("Fetched %d USDT markets from KuCoin", len(markets))
            return markets
        except Exception as e:
            self._log.error("Failed to fetch KuCoin markets: %s", e)
            return []

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        # Symbols are already in KuCoin format ("ACE-USDT") from quote_aggregator
        watched_kucoin = {symbol.upper() for symbol in symbols}
        
        if not watched_kucoin:
            self._log.warning("No symbols to watch")
            return
        
        self._log.info("Starting quote stream for %d KuCoin symbols", len(watched_kucoin))
        
        while not self.closed:
            try:
                data = await self._http.get_json(f"{self._REST_BASE}/api/v1/market/allTickers")
                
                entries = data.get("data", {}).get("ticker", [])
                if not entries:
                    self._log.warning("KuCoin API returned empty ticker array")
                    await self.wait_interval()
                    continue
                
                ts = int(data.get("data", {}).get("time", time.time() * 1000))
                
                for item in entries:
                    symbol_kucoin = item.get("symbol", "").upper()
                    if symbol_kucoin not in watched_kucoin:
                        continue
                    
                    # KuCoin returns prices as strings
                    bid = self._to_float(item.get("buy"))
                    ask = self._to_float(item.get("sell"))
                    
                    if bid > 0 and ask > 0:
                        # Yield with KuCoin format symbol - quote_aggregator will map it correctly
                        yield ExchangeQuote(symbol=symbol_kucoin, bid=bid, ask=ask, timestamp_ms=ts)
                    
            except Exception as e:
                self._log.warning("Failed to fetch quotes from KuCoin: %s (will retry)", e)
            
            await self.wait_interval()
