from __future__ import annotations

import time
from typing import AsyncIterator, Sequence

from arbitrage_bot.core.http import HttpClientFactory
from arbitrage_bot.exchanges.base import BaseAdapter, ExchangeMarket, ExchangeQuote


class MexcAdapter(BaseAdapter):
    """
    MEXC exchange adapter using public REST API endpoints.
    No authentication required for Market Data endpoints (ping, time, tickers, candles, etc.).
    Public endpoints: /api/v3/exchangeInfo, /api/v3/ticker/24hr
    Note: MEXC uses Cloudflare protection - we need to visit main page first to get cookies.
    """
    name = "mexc"
    # Try alternative domains if main API is blocked
    _REST_BASE = "https://api.mexc.com"
    _REST_ALT_BASE = "https://www.mexc.com/api/v3"
    _MAIN_PAGE = "https://www.mexc.com/"

    def __init__(self, http_factory: HttpClientFactory, poll_interval: float = 1.0) -> None:
        super().__init__(http_factory, poll_interval=poll_interval)
        self._cookies: dict[str, str] = {}
        self._cookies_initialized = False
        self._markets_cache: list[ExchangeMarket] | None = None
        self._markets_cache_time: float = 0.0
        self._markets_cache_ttl: float = 600.0  # Кэш на 10 минут
        self._last_403_time: float = 0.0
        self._403_cooldown: float = 60.0  # 60 секунд после 403
        self._request_delay: float = 3.0  # Задержка между запросами (увеличено для избежания блокировок)
        self._successful_requests: int = 0  # Счетчик успешных запросов

    async def _ensure_cookies(self) -> None:
        """Visit main page to get cookies for Cloudflare protection."""
        if self._cookies_initialized:
            return
        
        import asyncio
        import aiohttp
        
        self._log.debug("Getting cookies from MEXC main page")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._MAIN_PAGE,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        # Extract cookies
                        for cookie_name, cookie_obj in resp.cookies.items():
                            self._cookies[cookie_name] = cookie_obj.value if hasattr(cookie_obj, 'value') else str(cookie_obj)
                        self._cookies_initialized = True
                        self._log.debug("Got %d cookies from MEXC: %s", len(self._cookies), list(self._cookies.keys())[:5])
                    else:
                        self._log.warning("Failed to get cookies, status: %d", resp.status)
        except Exception as e:
            self._log.warning("Failed to get cookies: %s", e)
        
        await asyncio.sleep(1.0)  # Wait after getting cookies

    async def fetch_markets(self) -> Sequence[ExchangeMarket]:
        import time
        current_time = time.time()
        
        # Проверяем кэш
        if self._markets_cache is not None and (current_time - self._markets_cache_time) < self._markets_cache_ttl:
            self._log.debug("Using cached MEXC markets (%d markets)", len(self._markets_cache))
            return self._markets_cache
        
        # Проверяем cooldown после 403
        if (current_time - self._last_403_time) < self._403_cooldown:
            # ВАЖНО: Возвращаем кэш даже если он старый - лучше старые данные чем никаких
            if self._markets_cache is not None:
                self._log.debug("MEXC still in 403 cooldown, returning cached markets (%d markets)", len(self._markets_cache))
                return self._markets_cache
            self._log.warning("MEXC in cooldown and no cache available - returning empty")
            return []
        
        self._log.info("Fetching markets from MEXC")
        # MEXC uses Cloudflare protection - get cookies first
        await self._ensure_cookies()
        
        import asyncio
        await asyncio.sleep(self._request_delay)  # Задержка для избежания блокировок
        
        # Правильные заголовки для обхода Cloudflare
        mexc_headers = {
            "Referer": "https://www.mexc.com/exchange/BTC_USDT",
            "Origin": "https://www.mexc.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        }
        
        # Try multiple endpoints/domains - альтернативный первым
        endpoints = [
            f"{self._REST_ALT_BASE}/exchangeInfo",  # Альтернативный домен первым
            f"{self._REST_BASE}/api/v3/exchangeInfo",
        ]
        
        data = None
        last_error = None
        for endpoint in endpoints:
            try:
                self._log.debug("Trying MEXC endpoint: %s", endpoint)
                data = await self._http.get_json(endpoint, extra_headers=mexc_headers, max_retries=1, cookies=self._cookies if self._cookies else None)  # Don't retry on 403
                break  # Success
            except Exception as e:
                last_error = e
                # Пробуем все endpoints даже при 403 - может альтернативный домен работает
                if hasattr(e, 'status') and e.status == 403:
                    self._log.debug("MEXC endpoint %s returned 403 Forbidden - trying next endpoint", endpoint)
                    # Продолжаем пробовать другие endpoints
                    if endpoint == endpoints[-1]:
                        # Это был последний endpoint
                        break
                    await asyncio.sleep(0.5)  # Delay between retries
                    continue
                self._log.debug("Endpoint %s failed: %s", endpoint, e)
                if endpoint == endpoints[-1]:
                    # Last endpoint failed, try fallback
                    break
                await asyncio.sleep(0.5)  # Delay between retries
                continue
        
        if data is None:
            # All endpoints failed, try ticker24hr fallback
            # But skip if last error was 403 (IP blocked)
            if last_error and hasattr(last_error, 'status') and last_error.status == 403:
                import time
                self._last_403_time = time.time()
                self._log.warning("MEXC API returned 403 Forbidden - IP may be blocked. Skipping ticker24hr fallback. Cooldown: %d seconds", self._403_cooldown)
                # ВАЖНО: Возвращаем кэш даже если он старый - лучше старые данные чем никаких
                if self._markets_cache is not None:
                    self._log.info("Returning cached MEXC markets (%d markets) despite 403 error", len(self._markets_cache))
                    return self._markets_cache
                # Если кэша нет, возвращаем пустой список - market_discovery пропустит MEXC
                self._log.warning("No cached markets available for MEXC - will be excluded from discovery")
                return []
            
            if last_error:
                self._log.warning("All MEXC endpoints failed, trying ticker24hr fallback: %s", last_error)
            try:
                tickers = await self._http.get_json(
                    f"{self._REST_BASE}/api/v3/ticker/24hr",
                    extra_headers=mexc_headers,
                    max_retries=1,  # Don't retry on 403
                    cookies=self._cookies if self._cookies else None
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
                # Сохраняем в кэш
                import time
                self._markets_cache = markets
                self._markets_cache_time = time.time()
                self._log.info("Fetched %d USDT markets from MEXC (via ticker24hr fallback, cached for %d seconds)", len(markets), int(self._markets_cache_ttl))
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
        # Сохраняем в кэш
        import time
        self._markets_cache = markets
        self._markets_cache_time = time.time()
        self._log.info("Fetched %d USDT markets from MEXC (cached for %d seconds)", len(markets), int(self._markets_cache_ttl))
        return markets

    async def quote_stream(self, symbols: Sequence[str]) -> AsyncIterator[ExchangeQuote]:
        watched = {symbol.upper() for symbol in symbols}
        if not watched:
            self._log.warning("No symbols to watch")
            return
        self._log.info("Starting quote stream for %d symbols", len(watched))
        # MEXC uses Cloudflare protection - get cookies first
        await self._ensure_cookies()
        
        import asyncio
        await asyncio.sleep(self._request_delay)  # Initial delay
        
        # Минимальные заголовки для quote stream
        mexc_headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }
        consecutive_403_errors = 0
        max_403_errors = 3  # After 3 consecutive 403 errors, wait longer
        
        # Use ticker/24hr endpoint - request all tickers at once
        # Note: This endpoint works with cookies from main page visit
        quote_endpoint = f"{self._REST_BASE}/api/v3/ticker/24hr"
        
        # Convert watched set to list for iteration
        watched_list = list(watched)
        
        while not self.closed:
            try:
                # Check cooldown after 403
                import time
                current_time = time.time()
                if (current_time - self._last_403_time) < self._403_cooldown:
                    wait_time = self._403_cooldown - (current_time - self._last_403_time)
                    self._log.debug("MEXC still in 403 cooldown, waiting %.1f seconds", wait_time)
                    # Ждем меньшими порциями, чтобы не блокировать поток слишком долго
                    # И периодически проверяем, не закрыт ли адаптер
                    waited = 0.0
                    while waited < wait_time and not self.closed:
                        sleep_time = min(5.0, wait_time - waited)  # Максимум 5 секунд за раз
                        await asyncio.sleep(sleep_time)
                        waited += sleep_time
                    if self.closed:
                        break
                    continue
                
                # Ensure cookies are fresh before each request
                if not self._cookies_initialized:
                    await self._ensure_cookies()
                
                if self._cookies:
                    self._log.debug("Requesting quotes with %d cookies", len(self._cookies))
                else:
                    self._log.warning("No cookies available, request may fail")
                
                # Add delay before request to avoid rate limiting
                # Longer delay helps avoid Cloudflare blocks
                # Увеличиваем задержку после каждого успешного запроса для снижения частоты
                current_delay = self._request_delay
                if self._successful_requests > 0:
                    # После первых запросов увеличиваем задержку еще больше
                    current_delay = self._request_delay * 1.5
                await asyncio.sleep(current_delay)
                
                # Request all tickers - MEXC supports this endpoint
                # Don't pass cookies - let HttpClientFactory handle headers
                data = await self._http.get_json(
                    quote_endpoint,
                    extra_headers=mexc_headers,
                    max_retries=1  # Don't retry on 403
                )
                consecutive_403_errors = 0  # Reset counter on success
                self._successful_requests += 1
                
                # После каждых 10 успешных запросов увеличиваем базовую задержку
                if self._successful_requests % 10 == 0:
                    self._request_delay = min(self._request_delay * 1.2, 5.0)  # Максимум 5 секунд
                    self._log.debug("MEXC: Increased request delay to %.1f seconds after %d successful requests", self._request_delay, self._successful_requests)
                
                if isinstance(data, list):
                    self._log.debug("Successfully received %d tickers from MEXC", len(data))
                else:
                    self._log.warning("Unexpected data format from MEXC: %s", type(data))
                    await self.wait_interval()
                    continue
            except Exception as e:
                # Check if it's a 403 error
                is_403 = hasattr(e, 'status') and e.status == 403
                if is_403:
                    import time
                    self._last_403_time = time.time()
                    consecutive_403_errors += 1
                    self._successful_requests = 0  # Reset success counter on 403
                    
                    # Увеличиваем базовую задержку при 403, но не слишком сильно
                    self._request_delay = min(self._request_delay * 1.2, 5.0)  # Максимум 5 секунд
                    
                    # Refresh cookies on 403 - they might have expired
                    try:
                        self._cookies_initialized = False
                        await self._ensure_cookies()
                    except Exception as cookie_error:
                        self._log.debug("Failed to refresh cookies: %s (non-critical)", cookie_error)
                    
                    if consecutive_403_errors >= max_403_errors:
                        self._log.warning(
                            "MEXC returned %d consecutive 403 errors - IP may be blocked. " 
                            "Cooldown: %d seconds, request delay increased to %.1f seconds",
                            consecutive_403_errors,
                            int(self._403_cooldown),
                            self._request_delay
                        )
                        # Don't wait here - let cooldown check handle it
                        consecutive_403_errors = 0  # Reset after reaching max
                    else:
                        wait_time = min(self._request_delay * (consecutive_403_errors + 1), 10.0)  # Максимум 10 секунд
                        self._log.warning(
                            "MEXC returned 403 Forbidden (consecutive: %d/%d). Request delay increased to %.1f seconds. Waiting %.1f seconds.",
                            consecutive_403_errors,
                            max_403_errors,
                            self._request_delay,
                            wait_time
                        )
                        # Ждем меньшими порциями, чтобы не блокировать поток слишком долго
                        waited = 0.0
                        while waited < wait_time and not self.closed:
                            sleep_time = min(2.0, wait_time - waited)  # Максимум 2 секунды за раз
                            await asyncio.sleep(sleep_time)
                            waited += sleep_time
                        if self.closed:
                            break
                else:
                    consecutive_403_errors = 0  # Reset on non-403 errors
                    self._log.warning("Failed to fetch quotes from MEXC: %s (will retry)", e)
                    # Небольшая задержка перед повтором при не-403 ошибках
                    await asyncio.sleep(min(self._poll_interval, 2.0))
                continue
            # ticker/24hr returns a list of tickers (2348 тикеров)
            items = data if isinstance(data, list) else []
            
            # КРИТИЧНО: Фильтруем ТОЛЬКО watched символы! Не все спотовые пары!
            quote_count = 0
            for item in items:
                symbol_raw = item.get("symbol")
                if not symbol_raw:
                    continue
                symbol = symbol_raw.upper()
                
                # КРИТИЧНО: Обрабатываем ТОЛЬКО watched символы!
                if symbol not in watched:
                    continue
                
                bid = self._to_float(item.get("bidPrice"))
                ask = self._to_float(item.get("askPrice"))
                
                if bid > 0 and ask > 0:
                    close_time = item.get("closeTime")
                    if close_time and isinstance(close_time, (int, float)):
                        ts = int(close_time)
                    else:
                        ts = int(time.time() * 1000)
                    
                    yield ExchangeQuote(symbol=symbol, bid=bid, ask=ask, timestamp_ms=ts)
                    quote_count += 1
            
            if quote_count > 0:
                self._log.debug("MEXC: processed %d quotes from %d tickers (watched: %d)", quote_count, len(items), len(watched))
            
            await self.wait_interval()

