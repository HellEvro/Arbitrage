from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class MexcAdapter(BaseAdapter):
    """
    MEXC exchange adapter using public REST API endpoints.
    No authentication required for Market Data endpoints (ping, time, tickers, candles, etc.).
    Public endpoints: /api/v3/exchangeInfo, /api/v3/ticker/bookTicker
    """
    name = "mexc"
    _REST_BASE = "https://api.mexc.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from MEXC")
        data = await self._http.get_json(f"{self._REST_BASE}/api/v3/exchangeInfo")
        symbols = data.get("symbols", [])
        markets: list[ExchangeMarket] = []
        for item in symbols:
            status = item.get("status")
            quote_asset = item.get("quoteAsset", "").upper()
            is_spot_trading = item.get("isSpotTradingAllowed", False)
            # MEXC uses status "1" for TRADING, and we also check isSpotTradingAllowed
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

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        while not self.closed:
            data = await self._http.get_json(f"{self._REST_BASE}/api/v3/ticker/bookTicker")
            ts = int(time.time() * 1000)
            for item in data:
                symbol = item.get("symbol", "").upper()
                if symbol not in watched:
                    continue
                bid = self._to_float(item.get("bidPrice"))
                ask = self._to_float(item.get("askPrice"))
                if bid <= 0 or ask <= 0:
                    continue
                yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
            await self.wait_interval()

