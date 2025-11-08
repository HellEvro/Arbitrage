from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class BybitAdapter(BaseAdapter):
    """
    Bybit exchange adapter using public REST API endpoints.
    No authentication required for market data (tickers, order books, trades, candles).
    Public endpoints: /v5/market/tickers, /v5/market/instruments-info
    """
    name = "bybit"
    _REST_BASE = "https://api.bybit.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from Bybit")
        data = await self._http.get_json(f"{self._REST_BASE}/v5/market/instruments-info", params={"category": "spot"})
        result = data.get("result", {}) or {}
        instruments = result.get("list", []) if result else []
        markets: list[ExchangeMarket] = []
        for item in instruments:
            symbol = item.get("symbol")
            base = item.get("baseCoin")
            quote = item.get("quoteCoin")
            if not symbol or not base or not quote:
                continue
            if quote.upper() != "USDT":
                continue
            markets.append(ExchangeMarket(symbol=symbol.upper(), base_asset=base.upper(), quote_asset=quote.upper()))
        self._log.info("Fetched %d USDT markets from Bybit", len(markets))
        return markets

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        while not self.closed:
            data = await self._http.get_json(
                f"{self._REST_BASE}/v5/market/tickers",
                params={"category": "spot"},
            )
            result = data.get("result", {}) or {}
            entries = result.get("list", []) if result else []
            raw_time = result.get("time")
            try:
                ts = int(raw_time)
            except (TypeError, ValueError):
                ts = int(time.time() * 1000)
            for item in entries:
                symbol = item.get("symbol", "").upper()
                if symbol not in watched:
                    continue
                bid = self._to_float(item.get("bid1Price"))
                ask = self._to_float(item.get("ask1Price"))
                if bid <= 0 or ask <= 0:
                    continue
                yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
            await self.wait_interval()

