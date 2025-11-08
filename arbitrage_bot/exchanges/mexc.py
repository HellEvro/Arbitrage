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
    # Try alternative domains if main API is blocked
    _REST_BASE = "https://api.mexc.com"
    _REST_ALT_BASE = "https://www.mexc.com/api/v3"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from MEXC")
        # MEXC uses Cloudflare protection - add browser-like headers and delay
        import asyncio
        await asyncio.sleep(0.5)  # Small delay to avoid rapid requests
        
        mexc_headers = {
            "Referer": "https://www.mexc.com/",
            "Origin": "https://www.mexc.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        
        # Try multiple endpoints/domains
        endpoints = [
            f"{self._REST_BASE}/api/v3/exchangeInfo",
            f"{self._REST_ALT_BASE}/exchangeInfo",
        ]
        
        data = None
        last_error = None
        for endpoint in endpoints:
            try:
                self._log.debug("Trying MEXC endpoint: %s", endpoint)
                data = await self._http.get_json(endpoint, extra_headers=mexc_headers)
                break  # Success
            except Exception as e:
                last_error = e
                self._log.debug("Endpoint %s failed: %s", endpoint, e)
                if endpoint == endpoints[-1]:
                    # Last endpoint failed, try fallback
                    break
                await asyncio.sleep(0.5)  # Delay between retries
                continue
        
        if data is None:
            # All endpoints failed, try ticker24hr fallback
            if last_error:
                self._log.warning("All MEXC endpoints failed, trying ticker24hr fallback: %s", last_error)
            try:
                tickers = await self._http.get_json(
                    f"{self._REST_BASE}/api/v3/ticker/24hr",
                    extra_headers=mexc_headers
                )
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
                self._log.info("Fetched %d USDT markets from MEXC (via ticker24hr fallback)", len(markets))
                return markets
            except Exception as e2:
                self._log.error("Both exchangeInfo and ticker24hr failed for MEXC: %s", e2)
                raise
        
        # Process exchangeInfo data
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
        # MEXC uses Cloudflare protection - add browser-like headers and delay
        import asyncio
        await asyncio.sleep(0.5)  # Initial delay
        
        mexc_headers = {
            "Referer": "https://www.mexc.com/",
            "Origin": "https://www.mexc.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        }
        while not self.closed:
            try:
                data = await self._http.get_json(
                    f"{self._REST_BASE}/api/v3/ticker/bookTicker",
                    extra_headers=mexc_headers
                )
            except Exception as e:
                self._log.error("Failed to fetch quotes from MEXC: %s", e)
                await asyncio.sleep(self._poll_interval)
                continue
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

