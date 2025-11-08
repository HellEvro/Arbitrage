from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class KucoinAdapter(BaseAdapter):
    """
    KuCoin exchange adapter using public REST API endpoints.
    No authentication required for Market Data endpoints (Get All Tickers, Get Ticker, etc.).
    Public endpoints: /api/v1/symbols, /api/v1/market/allTickers
    """
    name = "kucoin"
    _REST_BASE = "https://api.kucoin.com"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        self._log.info("Fetching markets from KuCoin")
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

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        # KuCoin uses format "ACE-USDT" but we receive canonical "ACEUSDT"
        # Create mapping: canonical -> KuCoin format
        watched_canonical = {symbol.upper() for symbol in symbols}
        watched_kucoin = set()
        # Convert canonical symbols to KuCoin format (add hyphen)
        for canonical in watched_canonical:
            if canonical.endswith("USDT") and len(canonical) > 4:
                base = canonical[:-4]  # Remove "USDT"
                kucoin_symbol = f"{base}-USDT"
                watched_kucoin.add(kucoin_symbol)
        
        if not watched_kucoin:
            self._log.warning("No symbols to watch after conversion")
            return
        self._log.info("Starting quote stream for %d symbols (%d KuCoin format)", len(watched_canonical), len(watched_kucoin))
        quote_yielded = 0
        while not self.closed:
            try:
                data = await self._http.get_json(f"{self._REST_BASE}/api/v1/market/allTickers")
                entries = data.get("data", {}).get("ticker", [])
                if not entries:
                    self._log.warning("KuCoin API returned empty ticker array")
                    await self.wait_interval()
                    continue
                
                ts = int(data.get("data", {}).get("time", time.time() * 1000))
                matched_count = 0
                for item in entries:
                    symbol_kucoin = item.get("symbol", "").upper()
                    if symbol_kucoin not in watched_kucoin:
                        continue
                    # KuCoin returns prices as strings, parse them carefully
                    buy_str = item.get("buy", "")
                    sell_str = item.get("sell", "")
                    bid = self._to_float(buy_str)
                    ask = self._to_float(sell_str)
                    if bid <= 0 or ask <= 0:
                        self._log.debug("Invalid price for %s: bid=%s, ask=%s", symbol_kucoin, buy_str, sell_str)
                        continue
                    # Yield with KuCoin format symbol - quote_aggregator will map it correctly
                    yield ExchangeQuote(symbol=symbol_kucoin, bid=bid, ask=ask, timestamp_ms=ts)
                    quote_yielded += 1
                    matched_count += 1
                
                if matched_count == 0:
                    # Log first few watched symbols for debugging
                    sample_watched = list(watched_kucoin)[:5]
                    sample_entries = [item.get("symbol", "").upper() for item in entries[:10]]
                    self._log.warning(
                        "KuCoin: No matching symbols found. Watched: %s (total: %d), "
                        "Sample from API: %s (total entries: %d)",
                        sample_watched,
                        len(watched_kucoin),
                        sample_entries,
                        len(entries)
                    )
                elif quote_yielded % 100 == 0:
                    self._log.debug("KuCoin: yielded %d quotes total, %d in this batch", quote_yielded, matched_count)
            except Exception as e:
                self._log.error("KuCoin quote stream error: %s", e, exc_info=True)
                raise
            await self.wait_interval()

