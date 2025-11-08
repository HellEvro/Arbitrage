from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class OkxAdapter(BaseAdapter):
    name = "okx"
    _REST_BASE = "https://www.okx.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from OKX")
        data = await self._http.get_json(f"{self._REST_BASE}/api/v5/public/instruments", params={"instType": "SPOT"})
        instruments = data.get("data", [])
        markets: list[ExchangeMarket] = []
        for item in instruments:
            if item.get("quoteCcy", "").upper() != "USDT":
                continue
            markets.append(
                ExchangeMarket(
                    symbol=item.get("instId", "").upper(),
                    base_asset=item.get("baseCcy", "").upper(),
                    quote_asset="USDT",
                )
            )
        self._log.info("Fetched %d USDT markets from OKX", len(markets))
        return markets

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        while not self.closed:
            data = await self._http.get_json(f"{self._REST_BASE}/api/v5/market/tickers", params={"instType": "SPOT"})
            entries = data.get("data", [])
            ts = int(time.time() * 1000)
            for item in entries:
                symbol = item.get("instId", "").upper()
                if symbol not in watched:
                    continue
                bid = self._to_float(item.get("bidPx"))
                ask = self._to_float(item.get("askPx"))
                if bid <= 0 or ask <= 0:
                    continue
                yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
            await self.wait_interval()

