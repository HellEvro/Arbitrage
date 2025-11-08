from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class BitgetAdapter(BaseAdapter):
    """
    Bitget exchange adapter using public REST API endpoints.
    No authentication required for Public/Market endpoints (tickers, order books, candles).
    Rate limits: IP-based (e.g., 20 requests/sec for tickers).
    Public endpoints: /api/spot/v1/public/products, /api/spot/v1/market/tickers
    """
    name = "bitget"
    _REST_BASE = "https://api.bitget.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from Bitget")
        data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/public/products")
        items = data.get("data", [])
        markets: list[ExchangeMarket] = []
        for item in items:
            if item.get("quoteCoin", "").upper() != "USDT":
                continue
            symbol = item.get("symbol", "")
            if not symbol:
                continue
            markets.append(
                ExchangeMarket(
                    symbol=symbol.upper(),
                    base_asset=item.get("baseCoin", "").upper(),
                    quote_asset="USDT",
                )
            )
        self._log.info("Fetched %d USDT markets from Bitget", len(markets))
        return markets

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        while not self.closed:
            data = await self._http.get_json(f"{self._REST_BASE}/api/spot/v1/market/tickers")
            entries = data.get("data", [])
            ts = int(time.time() * 1000)
            for item in entries:
                symbol = item.get("symbol", "").upper()
                if symbol not in watched:
                    continue
                bid = self._to_float(item.get("bidPr"))
                ask = self._to_float(item.get("askPr"))
                if bid <= 0 or ask <= 0:
                    continue
                yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
            await self.wait_interval()

