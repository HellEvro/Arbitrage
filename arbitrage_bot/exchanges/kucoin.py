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
        # KuCoin uses format "ACE-USDT"
        # Symbols from quote_aggregator are already in KuCoin format (from fetch_markets)
        # So we should use them as-is, not convert from canonical format
        watched_kucoin = {symbol.upper() for symbol in symbols}
        
        # However, if we receive canonical format (without hyphen), convert it
        # But check if symbols already have hyphen first
        processed_kucoin = set()
        for symbol in watched_kucoin:
            if "-" in symbol:
                # Already in KuCoin format
                processed_kucoin.add(symbol)
            elif symbol.endswith("USDT") and len(symbol) > 4:
                # Canonical format, convert to KuCoin format
                base = symbol[:-4]  # Remove "USDT"
                processed_kucoin.add(f"{base}-USDT")
            else:
                # Unknown format, use as-is
                processed_kucoin.add(symbol)
        
        if not processed_kucoin:
            self._log.warning("No symbols to watch after processing")
            return
        self._log.info("Starting quote stream for %d symbols (processed %d KuCoin format)", len(watched_kucoin), len(processed_kucoin))
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
                    if symbol_kucoin not in processed_kucoin:
                        continue
                    # Find the original symbol format (might have hyphen or not)
                    original_symbol = None
                    for sym in watched_kucoin:
                        if sym == symbol_kucoin:
                            original_symbol = sym
                            break
                        elif sym.endswith("USDT") and len(sym) > 4:
                            # Check if canonical format matches
                            base = sym[:-4]
                            if f"{base}-USDT" == symbol_kucoin:
                                original_symbol = sym
                                break
                    if not original_symbol:
                        # Use KuCoin format as fallback
                        original_symbol = symbol_kucoin
                    # KuCoin returns prices as strings, parse them carefully
                    buy_str = item.get("buy", "")
                    sell_str = item.get("sell", "")
                    bid = self._to_float(buy_str)
                    ask = self._to_float(sell_str)
                    if bid <= 0 or ask <= 0:
                        self._log.debug("Invalid price for %s: bid=%s, ask=%s", original_symbol, buy_str, sell_str)
                        continue
                    # Yield with original symbol format so quote_aggregator can map it correctly
                    yield ExchangeQuote(symbol=original_symbol, bid=bid, ask=ask, timestamp_ms=ts)
                    quote_yielded += 1
                    matched_count += 1
                
                if matched_count == 0:
                    # Log first few watched symbols for debugging
                    sample_watched = list(processed_kucoin)[:5]
                    sample_entries = [item.get("symbol", "").upper() for item in entries[:10]]
                    self._log.warning(
                        "KuCoin: No matching symbols found. Watched (processed): %s (total: %d), "
                        "Sample from API: %s (total entries: %d)",
                        sample_watched,
                        len(processed_kucoin),
                        sample_entries,
                        len(entries)
                    )
                elif quote_yielded % 100 == 0:
                    self._log.debug("KuCoin: yielded %d quotes total, %d in this batch", quote_yielded, matched_count)
            except Exception as e:
                self._log.error("KuCoin quote stream error: %s", e, exc_info=True)
                raise
            await self.wait_interval()

