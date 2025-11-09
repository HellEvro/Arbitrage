from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Iterable

from arbitrage_bot.config.models import Settings
from arbitrage_bot.services.fee_fetcher import FeeFetcher
from arbitrage_bot.services.quote_store import QuoteStore
from arbitrage_bot.services.schemas import ArbitrageOpportunity, QuoteSnapshot

log = logging.getLogger(__name__)


class ArbitrageEngine:
    def __init__(
        self, 
        quote_store: QuoteStore, 
        settings: Settings, 
        fee_fetcher: FeeFetcher | None = None,
        top_n: int = 1000,  # Большое значение по умолчанию - фильтрация на клиенте
    ) -> None:
        self._quote_store = quote_store
        self._settings = settings
        self._fee_fetcher = fee_fetcher
        self._top_n = top_n
        self._lock = asyncio.Lock()
        self._latest: list[ArbitrageOpportunity] = []
        # История стабильности: ключ = (symbol, buy_exchange, sell_exchange), значение = deque с временными метками
        self._stability_history: dict[tuple[str, str, str], deque[int]] = {}
        # Используем настройки из конфига
        self._update_filtering_settings()
    
    def _update_filtering_settings(self) -> None:
        """Обновить настройки фильтрации из settings."""
        self._stable_window_ms = int(self._settings.filtering.stable_window_minutes * 60 * 1000)
    
    def reload_settings(self, new_settings: Settings) -> None:
        """Перезагрузить настройки без перезапуска."""
        old_notional = self._settings.notional_usdt_default if self._settings else 0
        old_slippage = self._settings.slippage_bps if self._settings else 0
        old_min_profit = self._settings.thresholds.min_profit_usdt if self._settings else 0
        old_min_spread = self._settings.thresholds.min_spread_pct if self._settings else 0
        
        old_settings = self._settings.filtering.model_dump() if self._settings else {}
        self._settings = new_settings
        self._update_filtering_settings()
        new_settings_dict = new_settings.filtering.model_dump()
        
        log.info(
            "Settings reloaded from config. "
            "Notional: %.0f -> %.0f USDT, "
            "Slippage: %.1f -> %.1f bps, "
            "Min profit: %.2f -> %.2f USDT, "
            "Min spread: %.2f -> %.2f%%, "
            "Same coin ratio: %.2f -> %.2f, "
            "Price ratio threshold: %.2f -> %.2f",
            old_notional,
            new_settings.notional_usdt_default,
            old_slippage,
            new_settings.slippage_bps,
            old_min_profit,
            new_settings.thresholds.min_profit_usdt,
            old_min_spread,
            new_settings.thresholds.min_spread_pct,
            old_settings.get("same_coin_ratio", 0),
            new_settings_dict.get("same_coin_ratio", 0),
            old_settings.get("price_ratio_threshold", 0),
            new_settings_dict.get("price_ratio_threshold", 0),
        )

    async def evaluate(self) -> list[ArbitrageOpportunity]:
        snapshots = await self._quote_store.list()
        snapshot_list = list(snapshots)
        log.debug("Evaluating %d quote snapshots", len(snapshot_list))
        
        opportunities = await self._compute_opportunities(snapshot_list)

        async with self._lock:
            self._latest = opportunities

        if opportunities:
            log.info("Found %d arbitrage opportunities (from %d snapshots)", len(opportunities), len(snapshot_list))
            if len(opportunities) > 0:
                top_opp = opportunities[0]
                log.debug(
                    "Top opportunity: %s - Gross: %.2f USDT, Fees: %.2f USDT, Net: %.2f USDT (%.3f%%)",
                    top_opp.symbol,
                    top_opp.gross_profit_usdt,
                    top_opp.total_fees_usdt,
                    top_opp.spread_usdt,
                    top_opp.spread_pct,
                )
        else:
            log.debug("No arbitrage opportunities found (snapshots: %d)", len(snapshot_list))
        return opportunities

    async def get_latest(self) -> list[ArbitrageOpportunity]:
        async with self._lock:
            return list(self._latest)

    async def _compute_opportunities(self, snapshots: Iterable[QuoteSnapshot]) -> list[ArbitrageOpportunity]:
        """Compute arbitrage opportunities.
        
        Requires at least 2 exchanges per symbol for arbitrage.
        System continues working even if some exchanges are unavailable.
        
        Также группирует монеты с одинаковым корнем и сравнивает цены:
        - Если цены близкие - это одна монета с разными названиями
        - Если цены сильно различаются - это разные монеты
        """
        results: list[ArbitrageOpportunity] = []
        now_ms = int(time.time() * 1000)
        stale_threshold_ms = self._settings.thresholds.stale_ms
        
        snapshot_list = list(snapshots)
        total_snapshots = len(snapshot_list)
        filtered_by_exchanges = 0
        filtered_by_stale = 0
        filtered_by_price = 0
        
        # Получаем пороги из настроек для фильтрации
        MIN_PRICE_THRESHOLD = self._settings.filtering.min_price_threshold
        PRICE_RATIO_THRESHOLD = self._settings.filtering.price_ratio_threshold
        
        # Функция для извлечения базового корня из base_asset
        def extract_base_root(base_asset: str | None) -> str:
            """Извлекает базовый корень, убирая известные суффиксы."""
            if not base_asset:
                return ""
            base = base_asset.upper()
            # Известные суффиксы
            known_suffixes = ["SYNC", "WASM", "V2", "V3", "VIRTUAL", "2", "3", "L", "C"]
            known_suffixes.sort(key=len, reverse=True)
            for suffix in known_suffixes:
                if base.endswith(suffix):
                    root = base[:-len(suffix)]
                    if len(root) >= 2:
                        return root
            return base
        
        # Функция для поиска наибольшего общего префикса
        def longest_common_prefix(str1: str, str2: str) -> str:
            min_len = min(len(str1), len(str2))
            for i in range(min_len):
                if str1[i] != str2[i]:
                    return str1[:i]
            return str1[:min_len]
        
        # Группируем snapshots по базовому корню для сравнения цен
        # Структура: {base_root: [snapshots]}
        root_groups: dict[str, list[QuoteSnapshot]] = {}
        for snapshot in snapshot_list:
            base_asset = snapshot.base_asset
            if not base_asset:
                # Если base_asset нет, пытаемся извлечь из symbol
                if snapshot.symbol.endswith("USDT"):
                    base_asset = snapshot.symbol[:-4]
                else:
                    continue
            
            base_root = extract_base_root(base_asset)
            if base_root not in root_groups:
                root_groups[base_root] = []
            root_groups[base_root].append(snapshot)
        
        # Для каждой группы корня проверяем цены и объединяем/разделяем
        processed_snapshots: list[QuoteSnapshot] = []
        # Пороги для определения одной монеты vs разных монет (из настроек):
        SAME_COIN_RATIO = self._settings.filtering.same_coin_ratio
        LIKELY_SAME_COIN_RATIO = self._settings.filtering.likely_same_coin_ratio
        DIFFERENT_COIN_RATIO = self._settings.filtering.different_coin_ratio
        
        for base_root, group_snapshots in root_groups.items():
            if len(group_snapshots) == 1:
                # Только один snapshot с этим корнем - обрабатываем как обычно
                processed_snapshots.extend(group_snapshots)
                continue
            
            # Несколько snapshots с одним корнем - сравниваем цены
            # Вычисляем среднюю цену для каждого snapshot
            snapshot_avg_prices: dict[str, float] = {}
            for snapshot in group_snapshots:
                prices = [p for p in snapshot.prices.values() if p > 0]
                if prices:
                    snapshot_avg_prices[snapshot.symbol] = sum(prices) / len(prices)
            
            if not snapshot_avg_prices:
                # Нет цен - обрабатываем все отдельно
                processed_snapshots.extend(group_snapshots)
                continue
            
            # Группируем snapshots по ценовым диапазонам
            # Применяем разные пороги в зависимости от того, идентичны ли названия или только корень
            price_groups: list[list[QuoteSnapshot]] = []
            used_symbols: set[str] = set()
            
            # Функция для извлечения base_asset из symbol
            def get_base_from_symbol(symbol: str) -> str:
                if symbol.endswith("USDT"):
                    return symbol[:-4].upper()
                return symbol.upper()
            
            for snapshot in group_snapshots:
                if snapshot.symbol in used_symbols:
                    continue
                
                avg_price = snapshot_avg_prices.get(snapshot.symbol, 0)
                if avg_price == 0:
                    processed_snapshots.append(snapshot)
                    used_symbols.add(snapshot.symbol)
                    continue
                
                snapshot_base = snapshot.base_asset or get_base_from_symbol(snapshot.symbol)
                snapshot_base_len = len(snapshot_base)
                
                # Ищем другие snapshots с близкими ценами
                price_group = [snapshot]
                used_symbols.add(snapshot.symbol)
                
                for other_snapshot in group_snapshots:
                    if other_snapshot.symbol in used_symbols:
                        continue
                    
                    other_avg_price = snapshot_avg_prices.get(other_snapshot.symbol, 0)
                    if other_avg_price == 0:
                        continue
                    
                    other_base = other_snapshot.base_asset or get_base_from_symbol(other_snapshot.symbol)
                    other_base_len = len(other_base)
                    
                    # Вычисляем ratio
                    price_ratio = max(avg_price, other_avg_price) / min(avg_price, other_avg_price) if min(avg_price, other_avg_price) > 0 else float('inf')
                    
                    # Определяем порог в зависимости от того, идентичны ли названия
                    if snapshot_base == other_base:
                        # Названия идентичны - НО это могут быть разные монеты на разных биржах!
                        # КРИТИЧНО: Если canonical symbols разные (snapshot.symbol != other_snapshot.symbol),
                        # значит market_discovery уже разделил их как разные монеты - НЕ объединяем!
                        # Если canonical symbols одинаковые, значит это одна монета с разными ценами на биржах
                        if snapshot.symbol != other_snapshot.symbol:
                            # Разные canonical symbols - это разные монеты, не объединяем
                            continue
                        # Одинаковые canonical symbols - это одна монета, но раздвижка может быть большой
                        # Используем стандартный порог для арбитражных возможностей
                        threshold = LIKELY_SAME_COIN_RATIO  # 1.5 - раздвижка может быть большой для арбитража
                    elif snapshot_base_len == other_base_len:
                        # Одинаковая длина, но разные символы - проверяем внимательнее
                        # Может быть одна монета с небольшими вариациями названия
                        threshold = LIKELY_SAME_COIN_RATIO  # 1.5
                    else:
                        # Разная длина названия, но одинаковый корень
                        # Используем мягкий порог арбитражных погрешностей
                        threshold = LIKELY_SAME_COIN_RATIO  # 1.5 - арбитражные погрешности
                    
                    # Если цены близкие в пределах порога - это одна монета, объединяем
                    if price_ratio < threshold:
                        price_group.append(other_snapshot)
                        used_symbols.add(other_snapshot.symbol)
                
                price_groups.append(price_group)
            
            # Обрабатываем каждую группу
            for price_group in price_groups:
                if len(price_group) == 1:
                    # Один snapshot - обрабатываем как обычно
                    processed_snapshots.append(price_group[0])
                else:
                    # Несколько snapshots с близкими ценами - это одна монета
                    # Используем первый snapshot как основной и добавляем цены из других
                    main_snapshot = price_group[0]
                    # Объединяем цены из всех snapshots в группе
                    merged_snapshot = QuoteSnapshot(
                        symbol=main_snapshot.symbol,
                        prices=dict(main_snapshot.prices),
                        exchange_symbols=dict(main_snapshot.exchange_symbols),
                        timestamp_ms=max(s.timestamp_ms for s in price_group),
                        base_asset=main_snapshot.base_asset,
                        quote_asset=main_snapshot.quote_asset,
                    )
                    # Добавляем цены из других snapshots
                    for other_snapshot in price_group[1:]:
                        for exchange, price in other_snapshot.prices.items():
                            if exchange not in merged_snapshot.prices:
                                merged_snapshot.prices[exchange] = price
                                merged_snapshot.exchange_symbols[exchange] = other_snapshot.exchange_symbols.get(exchange, other_snapshot.symbol)
                    
                    processed_snapshots.append(merged_snapshot)

        # Теперь обрабатываем объединенные snapshots
        for snapshot in processed_snapshots:
            # Minimum 2 exchanges required for arbitrage
            if len(snapshot.prices) < 2:
                filtered_by_exchanges += 1
                continue

            if now_ms - snapshot.timestamp_ms > stale_threshold_ms:
                filtered_by_stale += 1
                continue

            # Проверяем ВСЕ возможные пары бирж для этой монеты
            # Это позволит найти все арбитражные возможности, а не только min/max
            exchanges_list = list(snapshot.prices.items())
            
            # Определяем диапазон цен для этого символа на всех биржах
            all_prices = [price for _, price in exchanges_list if price > 0]
            if len(all_prices) < 2:
                filtered_by_exchanges += 1
                continue
            
            min_price_all = min(all_prices)
            max_price_all = max(all_prices)
            # Проверяем, не являются ли это разные монеты (слишком большая разница в ценах)
            # Пороги уже определены в начале функции
            
            # Если одна цена очень маленькая, а другая нормальная - это разные монеты
            has_near_zero = min_price_all < MIN_PRICE_THRESHOLD and max_price_all >= MIN_PRICE_THRESHOLD
            # Если цены слишком сильно различаются - это разные монеты
            price_ratio_all = max_price_all / min_price_all if min_price_all > 0 else float('inf')
            
            # КРИТИЧНО: Если общий диапазон цен для символа слишком большой - это разные монеты
            # Пропускаем весь символ, не создавая никаких возможностей
            if has_near_zero or price_ratio_all > PRICE_RATIO_THRESHOLD:
                filtered_by_price += len(exchanges_list) * (len(exchanges_list) - 1)  # Примерная оценка отфильтрованных пар
                log.info(
                    "[FILTER] Skipping symbol %s entirely: price range %.8f - %.8f (ratio=%.2f > %.2f, has_near_zero=%s) - different coins",
                    snapshot.symbol,
                    min_price_all,
                    max_price_all,
                    price_ratio_all,
                    PRICE_RATIO_THRESHOLD,
                    has_near_zero,
                )
                continue
            
            for i, (buy_exchange, buy_price) in enumerate(exchanges_list):
                if buy_price <= 0:
                    filtered_by_price += 1
                    continue
                
                for j, (sell_exchange, sell_price) in enumerate(exchanges_list):
                    # Пропускаем одинаковые биржи
                    if buy_exchange == sell_exchange:
                        continue
                    
                    if sell_price <= 0:
                        continue
                    
                    # Пропускаем если цена продажи не выше цены покупки
                    if sell_price <= buy_price:
                        continue
                    
                    # КРИТИЧНО: Если цены слишком сильно различаются - это разные монеты, пропускаем
                    # Это предотвращает создание арбитражных возможностей между разными проектами
                    pair_price_ratio = sell_price / buy_price if buy_price > 0 else float('inf')
                    buy_is_near_zero = buy_price < MIN_PRICE_THRESHOLD
                    sell_is_near_zero = sell_price < MIN_PRICE_THRESHOLD
                    
                    # Если одна цена очень маленькая, а другая нормальная - это разные монеты
                    if (buy_is_near_zero and not sell_is_near_zero) or (not buy_is_near_zero and sell_is_near_zero):
                        filtered_by_price += 1
                        log.info(
                            "[FILTER] Filtered opportunity %s: %s@%.8f -> %s@%.8f (one price near zero: buy=%.8f < %.8f, sell=%.8f < %.8f)",
                            snapshot.symbol,
                            buy_exchange,
                            buy_price,
                            sell_exchange,
                            sell_price,
                            buy_price,
                            MIN_PRICE_THRESHOLD,
                            sell_price,
                            MIN_PRICE_THRESHOLD,
                        )
                        continue
                    
                    # Если цена в 1.5+ раза больше - это разные монеты (строгая фильтрация)
                    # Это предотвращает арбитраж между разными проектами с одинаковыми названиями
                    # Порог 1.5x выбран для максимальной строгости - даже небольшие различия могут указывать на разные монеты
                    if pair_price_ratio > PRICE_RATIO_THRESHOLD:
                        filtered_by_price += 1
                        log.info(
                            "[FILTER] Filtered opportunity %s: %s@%.8f -> %s@%.8f (ratio=%.2f > %.2f, different coins)",
                            snapshot.symbol,
                            buy_exchange,
                            buy_price,
                            sell_exchange,
                            sell_price,
                            pair_price_ratio,
                            PRICE_RATIO_THRESHOLD,
                        )
                        continue
                    
                    spread = sell_price - buy_price
                    spread_pct = (spread / buy_price) * 100.0

                    notional = self._settings.notional_usdt_default
                    quantity = notional / buy_price

                    # Get fees - use config first (fast, no blocking), then default
                    # КРИТИЧНО: Используем fees из настроек для скорости - fee_fetcher может блокировать!
                    if buy_exchange in self._settings.fees:
                        fee_buy_rate = self._settings.fees[buy_exchange].taker
                        fee_sell_rate = self._settings.fees[sell_exchange].taker if sell_exchange in self._settings.fees else 0.001
                        buy_fee_pct = fee_buy_rate * 100
                        sell_fee_pct = fee_sell_rate * 100
                    else:
                        fee_buy_rate = 0.001
                        fee_sell_rate = 0.001
                        buy_fee_pct = 0.1
                        sell_fee_pct = 0.1

                    fees_buy = notional * fee_buy_rate
                    fees_sell = (quantity * sell_price) * fee_sell_rate
                    total_fees = fees_buy + fees_sell

                    slippage = self._settings.slippage_bps / 10000.0 * notional

                    gross_profit = (sell_price - buy_price) * quantity
                    net_profit = gross_profit - total_fees - slippage

                    # Фильтруем по минимальной прибыли и спреду
                    if net_profit < self._settings.thresholds.min_profit_usdt:
                        continue
                    if spread_pct < self._settings.thresholds.min_spread_pct:
                        continue

                    buy_symbol = snapshot.exchange_symbols.get(buy_exchange, snapshot.symbol)
                    sell_symbol = snapshot.exchange_symbols.get(sell_exchange, snapshot.symbol)

                    # Проверяем стабильность арбитражной возможности
                    is_stable = self._check_stability(
                        snapshot.symbol,
                        buy_exchange,
                        sell_exchange,
                        snapshot.timestamp_ms,
                    )

                    results.append(
                        ArbitrageOpportunity(
                            symbol=snapshot.symbol,
                            buy_exchange=buy_exchange,
                            buy_price=buy_price,
                            buy_symbol=buy_symbol,
                            buy_fee_pct=buy_fee_pct,
                            sell_exchange=sell_exchange,
                            sell_price=sell_price,
                            sell_symbol=sell_symbol,
                            sell_fee_pct=sell_fee_pct,
                            spread_usdt=net_profit,
                            spread_pct=spread_pct,
                            gross_profit_usdt=gross_profit,
                            total_fees_usdt=total_fees,
                            timestamp_ms=snapshot.timestamp_ms,
                            is_stable=is_stable,
                        )
                    )

        results.sort(key=lambda item: item.spread_usdt, reverse=True)
        
        # Логируем статистику фильтрации
        valid_snapshots = total_snapshots - filtered_by_exchanges - filtered_by_stale - filtered_by_price
        log.info(
            "[FILTER STATS] Snapshot filtering: total=%d, valid=%d, "
            "filtered_by_exchanges=%d (<2 exchanges), filtered_by_stale=%d (>%dms old), filtered_by_price=%d (price ratio > %.2f or near zero), "
            "opportunities=%d",
            total_snapshots,
            valid_snapshots,
            filtered_by_exchanges,
            filtered_by_stale,
            stale_threshold_ms,
            filtered_by_price,
            PRICE_RATIO_THRESHOLD,
            len(results),
        )
        
        # Возвращаем ВСЕ результаты - фильтрация происходит на клиенте через UI
        # top_n больше не используется для ограничения, только для совместимости
        return results

    def _check_stability(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        timestamp_ms: int,
    ) -> bool:
        """Проверяет, является ли арбитражная возможность стабильной.
        
        Возможность считается стабильной, если цена на sell_exchange выше цены на buy_exchange
        в течение последних 5 минут (stable_window_minutes).
        
        Это означает, что монета стабильно дороже на одной бирже, чем на другой,
        что дает время для арбитража: купить на дешевой бирже (buy_exchange),
        перебросить на дорогую биржу (sell_exchange) и продать там.
        
        Args:
            symbol: Символ монеты
            buy_exchange: Биржа для покупки (дешевая)
            sell_exchange: Биржа для продажи (дорогая)
            timestamp_ms: Текущая временная метка в миллисекундах
            
        Returns:
            True если возможность стабильна (цена выше на sell_exchange в течение 5+ минут)
        """
        key = (symbol, buy_exchange, sell_exchange)
        
        # Получаем или создаем историю для этой пары
        if key not in self._stability_history:
            self._stability_history[key] = deque(maxlen=1000)  # Ограничиваем размер истории
        
        history = self._stability_history[key]
        
        # Добавляем текущую временную метку
        history.append(timestamp_ms)
        
        # Очищаем старые записи (старше окна стабильности)
        cutoff_time = timestamp_ms - self._stable_window_ms
        while history and history[0] < cutoff_time:
            history.popleft()
        
        # Проверяем стабильность: если самая старая запись в истории существует
        # и разница между текущим временем и самой старой записью >= окна стабильности,
        # значит возможность существует уже достаточно долго (5 минут)
        if len(history) > 0:
            oldest_timestamp = history[0]
            # Если разница между текущим временем и самой старой записью >= окна стабильности
            # значит возможность стабильна (существует уже 5+ минут)
            if timestamp_ms - oldest_timestamp >= self._stable_window_ms:
                return True
        
        return False

