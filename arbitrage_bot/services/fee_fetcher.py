from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import ExchangeAdapter

log = logging.getLogger(__name__)


class FeeInfo:
    """Fee information for an exchange."""
    def __init__(self, exchange: str, taker: float, maker: float, symbol: str | None = None):
        self.exchange = exchange
        self.taker = taker
        self.maker = maker
        self.symbol = symbol  # None means default fee for exchange


class FeeFetcher:
    """Fetches trading fees from exchanges via public APIs.
    
    Automatically retrieves fees from exchange APIs when available.
    Falls back to exchange defaults if API doesn't provide fee info.
    Caches fees to avoid repeated API calls.
    """
    
    # Default fees from exchange documentation (used as fallback)
    DEFAULT_FEES: dict[str, tuple[float, float]] = {
        "bybit": (0.001, 0.001),  # taker, maker - standard spot fees
        "mexc": (0.002, 0.002),   # varies by symbol, fetched from API
        "bitget": (0.001, 0.001), # standard spot fees
        "okx": (0.0015, 0.0008),  # tier-based, default tier
        "kucoin": (0.001, 0.001), # standard spot fees
    }
    
    def __init__(self, http_factory: HttpClientFactory):
        self._http = http_factory
        self._cache: dict[str, FeeInfo] = {}
        self._symbol_fees_cache: dict[str, dict[str, FeeInfo]] = {}  # exchange -> symbol -> FeeInfo
        self._lock = asyncio.Lock()
    
    async def get_fee(self, exchange: str, symbol: str | None = None) -> FeeInfo:
        """Get fee for exchange and optionally specific symbol.
        
        Uses cached values when available to minimize API calls.
        """
        cache_key = f"{exchange}:{symbol or 'default'}"
        
        async with self._lock:
            if cache_key in self._cache:
                return self._cache[cache_key]
            
            # Check symbol-specific cache
            if symbol and exchange in self._symbol_fees_cache:
                if symbol.upper() in self._symbol_fees_cache[exchange]:
                    fee_info = self._symbol_fees_cache[exchange][symbol.upper()]
                    self._cache[cache_key] = fee_info
                    return fee_info
        
        # Try to fetch from exchange API
        fee_info = await self._fetch_from_exchange(exchange, symbol)
        
        async with self._lock:
            self._cache[cache_key] = fee_info
            if symbol:
                if exchange not in self._symbol_fees_cache:
                    self._symbol_fees_cache[exchange] = {}
                self._symbol_fees_cache[exchange][symbol.upper()] = fee_info
        
        return fee_info
    
    async def _fetch_from_exchange(self, exchange: str, symbol: str | None) -> FeeInfo:
        """Fetch fee from exchange API."""
        try:
            if exchange == "mexc":
                return await self._fetch_mexc_fee(symbol)
            elif exchange == "bybit":
                return await self._fetch_bybit_fee(symbol)
            elif exchange == "okx":
                return await self._fetch_okx_fee(symbol)
            # For other exchanges, use defaults
            taker, maker = self.DEFAULT_FEES.get(exchange, (0.001, 0.001))
            return FeeInfo(exchange, taker, maker, symbol)
        except Exception as e:
            log.warning("Failed to fetch fee from %s for %s: %s, using default", exchange, symbol, e)
            taker, maker = self.DEFAULT_FEES.get(exchange, (0.001, 0.001))
            return FeeInfo(exchange, taker, maker, symbol)
    
    async def _fetch_mexc_fee(self, symbol: str | None) -> FeeInfo:
        """Fetch MEXC fee from exchangeInfo - fees are in symbol data.
        
        MEXC provides makerCommission and takerCommission per symbol in exchangeInfo.
        Values are typically in basis points (e.g., 20 = 0.2%).
        """
        try:
            if symbol:
                data = await self._http.get_json("https://api.mexc.com/api/v3/exchangeInfo")
                symbols = data.get("symbols", [])
                for item in symbols:
                    if item.get("symbol", "").upper() == symbol.upper():
                        # MEXC returns commission as string, e.g., "0.002" or "20" (basis points)
                        maker_str = str(item.get("makerCommission", "0.002"))
                        taker_str = str(item.get("takerCommission", "0.002"))
                        
                        maker = float(maker_str)
                        taker = float(taker_str)
                        
                        # If > 1, assume it's in basis points (e.g., 20 = 0.2%)
                        if maker > 1:
                            maker = maker / 10000
                        if taker > 1:
                            taker = taker / 10000
                        
                        log.debug("MEXC fee for %s: taker=%.4f%%, maker=%.4f%%", symbol, taker*100, maker*100)
                        return FeeInfo("mexc", taker, maker, symbol)
        except Exception as e:
            log.debug("Could not fetch MEXC fee for %s: %s", symbol, e)
        # MEXC default: maker 0.2%, taker 0.2% for spot
        return FeeInfo("mexc", 0.002, 0.002, symbol)
    
    async def _fetch_bybit_fee(self, symbol: str | None) -> FeeInfo:
        """Fetch Bybit fee - typically 0.1% for spot."""
        # Bybit instruments-info might have fee info, but usually standard
        return FeeInfo("bybit", 0.001, 0.001, symbol)
    
    async def _fetch_okx_fee(self, symbol: str | None) -> FeeInfo:
        """Fetch OKX fee - varies by tier but default is 0.15% taker, 0.08% maker."""
        return FeeInfo("okx", 0.0015, 0.0008, symbol)
    
    async def refresh_all(self, exchanges: Sequence[str]) -> None:
        """Refresh fees for all exchanges."""
        async with self._lock:
            self._cache.clear()
        
        for exchange in exchanges:
            try:
                await self.get_fee(exchange)
            except Exception as e:
                log.warning("Failed to refresh fees for %s: %s", exchange, e)

