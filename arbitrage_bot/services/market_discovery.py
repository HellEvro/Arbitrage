from __future__ import annotations

import asyncio
import logging
from typing import Sequence

from arbitrage_bot.core.exceptions import DiscoveryError
from arbitrage_bot.exchanges import ExchangeAdapter
from arbitrage_bot.services.schemas import MarketInfo

log = logging.getLogger(__name__)


class MarketDiscoveryService:
    def __init__(self, adapters: Sequence[ExchangeAdapter], refresh_interval_sec: float = 300.0) -> None:
        self._adapters = adapters
        self._refresh_interval_sec = refresh_interval_sec
        self._cache: list[MarketInfo] = []
        self._lock = asyncio.Lock()

    async def refresh(self) -> list[MarketInfo]:
        log.info("Refreshing market discovery for %d exchanges", len(self._adapters))
        try:
            # Use return_exceptions=True to continue even if some exchanges fail
            results = await asyncio.gather(
                *(adapter.fetch_markets() for adapter in self._adapters),
                return_exceptions=True
            )
            markets_per_exchange = []
            for adapter, result in zip(self._adapters, results, strict=False):
                if isinstance(result, Exception):
                    log.error("Failed to fetch markets from %s: %s", adapter.name, result)
                    markets_per_exchange.append([])  # Empty list for failed exchange
                else:
                    markets_per_exchange.append(result)
        except Exception as exc:  # pragma: no cover - unexpected network errors
            # Don't raise if we have at least 2 exchanges working
            successful_exchanges = sum(1 for markets in markets_per_exchange if len(markets) > 0)
            if successful_exchanges >= 2:
                log.warning("Some exchanges failed, but %d exchanges are still working: %s", successful_exchanges, exc)
            else:
                log.exception("Failed to fetch markets from exchanges")
                raise DiscoveryError(f"Failed to fetch markets: {exc}") from exc

        # Функция для поиска наибольшего общего префикса между двумя строками
        def longest_common_prefix(str1: str, str2: str) -> str:
            """Находит наибольший общий префикс между двумя строками."""
            min_len = min(len(str1), len(str2))
            for i in range(min_len):
                if str1[i] != str2[i]:
                    return str1[:i]
            return str1[:min_len]
        
        # Функция для извлечения базового корня из base_asset
        # Использует комбинацию: убирает известные суффиксы И ищет общий префикс с другими монетами
        def extract_base_root(base_asset: str, all_base_assets: set[str]) -> str:
            """Извлекает базовый корень, используя суффиксы и общие префиксы."""
            base = base_asset.upper()
            
            # Сначала пробуем убрать известные суффиксы
            known_suffixes = ["SYNC", "WASM", "V2", "V3", "VIRTUAL", "2", "3", "L", "C"]
            known_suffixes.sort(key=len, reverse=True)
            
            for suffix in known_suffixes:
                if base.endswith(suffix):
                    root = base[:-len(suffix)]
                    if len(root) >= 2:
                        # Проверяем, есть ли другие монеты с таким же корнем
                        for other_base in all_base_assets:
                            if other_base != base and other_base.startswith(root):
                                return root
                        return root
            
            # Если суффиксы не помогли, ищем наибольший общий префикс с другими монетами
            # КРИТИЧНО: Не группируем монеты с общим префиксом, если разница в длине слишком большая
            # Например, STRAX (5 символов) и STRAWBERRYAI (12 символов) - это разные монеты
            # даже если у них общий префикс "STRA" (4 символа)
            best_root = base
            best_match_len = 0
            
            for other_base in all_base_assets:
                if other_base != base:
                    lcp = longest_common_prefix(base, other_base)
                    # Минимальная длина префикса - 3 символа
                    if len(lcp) >= 3:
                        # Проверяем, что общий префикс составляет значительную часть обоих названий
                        # И что разница в длине не слишком большая (не более чем в 2 раза)
                        base_len = len(base)
                        other_len = len(other_base)
                        lcp_len = len(lcp)
                        
                        # Общий префикс должен быть хотя бы 50% от более короткого названия
                        min_len = min(base_len, other_len)
                        if lcp_len >= min_len * 0.5:
                            # И разница в длине не должна быть больше чем в 2 раза
                            max_len = max(base_len, other_len)
                            if max_len <= min_len * 2:
                                if lcp_len > best_match_len:
                                    best_root = lcp
                                    best_match_len = lcp_len
            
            # Если нашли общий префикс, который соответствует критериям, используем его
            if best_match_len >= 3:
                return best_root
            
            return base
        
        # Сначала собираем все base_asset для определения общих префиксов
        all_base_assets_set: set[str] = set()
        for markets in markets_per_exchange:
            for market in markets:
                if market.quote_asset.upper() == "USDT":
                    all_base_assets_set.add(market.base_asset.upper())
        
        # КРИТИЧНО: Используем ПОЛНОЕ название base_asset по умолчанию, а не извлеченный корень!
        # Корень используется только для специальных случаев (например, ZKSYNC -> ZK)
        # Структура: {base_asset: {exchange: [(base_asset, symbol, canonical)]}}
        asset_groups: dict[str, dict[str, list[tuple[str, str, str]]]] = {}
        
        for adapter, markets in zip(self._adapters, markets_per_exchange, strict=False):
            for market in markets:
                if market.quote_asset.upper() != "USDT":
                    continue
                
                base_asset = market.base_asset.upper()
                symbol = market.symbol.upper()
                
                # Специальный случай для Bitget ZKSYNCUSDT -> мапим к ZKUSDT
                if adapter.name == "bitget" and symbol == "ZKSYNCUSDT":
                    canonical_symbol = "ZKUSDT"
                    # Используем "ZK" как ключ для группировки
                    group_key = "ZK"
                else:
                    canonical_symbol = f"{base_asset}{market.quote_asset.upper()}"
                    # Используем ПОЛНОЕ название base_asset как ключ для группировки
                    group_key = base_asset
                
                if group_key not in asset_groups:
                    asset_groups[group_key] = {}
                if adapter.name not in asset_groups[group_key]:
                    asset_groups[group_key][adapter.name] = []
                
                asset_groups[group_key][adapter.name].append((base_asset, symbol, canonical_symbol))
        
        # Теперь создаем финальный маппинг
        # КРИТИЧНО: Используем ПОЛНОЕ название base_asset для группировки!
        # Каждый base_asset получает свой canonical символ - никакого объединения по корням!
        symbol_map: dict[str, dict[str, str]] = {}
        
        for group_key, exchanges_data in asset_groups.items():
            # Собираем все уникальные base_asset для этой группы
            # В большинстве случаев будет только один base_asset (так как мы используем полное название)
            all_base_assets = set()
            for exchange_markets in exchanges_data.values():
                for base_asset, _, _ in exchange_markets:
                    all_base_assets.add(base_asset)
            
            # Если только один base_asset - простая ситуация (обычный случай)
            if len(all_base_assets) == 1:
                base_asset = list(all_base_assets)[0]
                canonical = f"{base_asset}USDT"
                for exchange_name, exchange_markets in exchanges_data.items():
                    for _, symbol, _ in exchange_markets:
                        if canonical not in symbol_map:
                            symbol_map[canonical] = {}
                        symbol_map[canonical][exchange_name] = symbol
            else:
                # Несколько base_asset в одной группе (может быть только для специального случая ZK)
                # Создаем отдельные canonical для каждого base_asset
                for exchange_name, exchange_markets in exchanges_data.items():
                    for base_asset, symbol, canonical_symbol in exchange_markets:
                        if canonical_symbol not in symbol_map:
                            symbol_map[canonical_symbol] = {}
                        symbol_map[canonical_symbol][exchange_name] = symbol

        # Count successful exchanges (non-empty market lists)
        successful_exchanges = sum(1 for markets in markets_per_exchange if len(markets) > 0)
        log.info("Successfully fetched markets from %d out of %d exchanges", successful_exchanges, len(self._adapters))
        
        # Minimum 2 exchanges required for arbitrage
        MIN_EXCHANGES_REQUIRED = 2
        if successful_exchanges < MIN_EXCHANGES_REQUIRED:
            log.warning(
                "Only %d exchanges available (minimum %d required). System will continue but may have limited opportunities.",
                successful_exchanges,
                MIN_EXCHANGES_REQUIRED
            )
        
        intersection: list[MarketInfo] = []
        for canonical, exchanges in symbol_map.items():
            # Require symbol to be on at least 2 exchanges for arbitrage
            if len(exchanges) < MIN_EXCHANGES_REQUIRED:
                continue
            intersection.append(
                MarketInfo(
                    symbol=canonical,
                    exchanges=sorted(exchanges.keys()),
                    exchange_symbols=dict(exchanges),
                )
            )

        async with self._lock:
            self._cache = sorted(intersection, key=lambda info: info.symbol)

        log.info("Found %d intersecting markets across all exchanges", len(self._cache))
        return list(self._cache)

    async def get_cached(self) -> list[MarketInfo]:
        async with self._lock:
            return list(self._cache)

    @property
    def refresh_interval(self) -> float:
        return self._refresh_interval_sec


