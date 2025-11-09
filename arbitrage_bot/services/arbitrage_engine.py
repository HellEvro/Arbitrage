from __future__ import annotations

import asyncio
import logging
import time
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
        top_n: int = 1000  # Большое значение по умолчанию - фильтрация на клиенте
    ) -> None:
        self._quote_store = quote_store
        self._settings = settings
        self._fee_fetcher = fee_fetcher
        self._top_n = top_n
        self._lock = asyncio.Lock()
        self._latest: list[ArbitrageOpportunity] = []

    async def evaluate(self) -> list[ArbitrageOpportunity]:
        # КРИТИЧНО: list() теперь НЕ блокируется - чтение полностью независимо от записи!
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
        """
        results: list[ArbitrageOpportunity] = []
        now_ms = int(time.time() * 1000)
        stale_threshold_ms = self._settings.thresholds.stale_ms
        
        snapshot_list = list(snapshots)
        total_snapshots = len(snapshot_list)
        filtered_by_exchanges = 0
        filtered_by_stale = 0
        filtered_by_price = 0

        for snapshot in snapshot_list:
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
                    
                    spread = sell_price - buy_price
                    spread_pct = (spread / buy_price) * 100.0

                    notional = self._settings.notional_usdt_default
                    quantity = notional / buy_price

                    # Get fees - prefer fee_fetcher, fallback to config, then default
                    if self._fee_fetcher:
                        buy_fee_info = await self._fee_fetcher.get_fee(buy_exchange, snapshot.symbol)
                        sell_fee_info = await self._fee_fetcher.get_fee(sell_exchange, snapshot.symbol)
                        fee_buy_rate = buy_fee_info.taker
                        fee_sell_rate = sell_fee_info.taker
                        buy_fee_pct = fee_buy_rate * 100  # Convert to percentage for display
                        sell_fee_pct = fee_sell_rate * 100
                    elif buy_exchange in self._settings.fees:
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
                        )
                    )

        results.sort(key=lambda item: item.spread_usdt, reverse=True)
        
        # Логируем статистику фильтрации
        valid_snapshots = total_snapshots - filtered_by_exchanges - filtered_by_stale - filtered_by_price
        log.debug(
            "Snapshot filtering stats: total=%d, valid=%d, "
            "filtered_by_exchanges=%d (<2 exchanges), filtered_by_stale=%d (>%dms old), filtered_by_price=%d (invalid), "
            "opportunities=%d",
            total_snapshots,
            valid_snapshots,
            filtered_by_exchanges,
            filtered_by_stale,
            stale_threshold_ms,
            filtered_by_price,
            len(results),
        )
        
        # Возвращаем ВСЕ результаты - фильтрация происходит на клиенте через UI
        # top_n больше не используется для ограничения, только для совместимости
        return results

