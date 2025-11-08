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
        top_n: int = 20
    ) -> None:
        self._quote_store = quote_store
        self._settings = settings
        self._fee_fetcher = fee_fetcher
        self._top_n = top_n
        self._lock = asyncio.Lock()
        self._latest: list[ArbitrageOpportunity] = []

    async def evaluate(self) -> list[ArbitrageOpportunity]:
        snapshots = await self._quote_store.list()
        snapshot_list = list(snapshots)
        log.debug("Evaluating %d quote snapshots", len(snapshot_list))
        
        opportunities = await self._compute_opportunities(snapshot_list)

        async with self._lock:
            self._latest = opportunities

        if opportunities:
            log.info("Found %d arbitrage opportunities", len(opportunities))
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

        for snapshot in snapshots:
            # Minimum 2 exchanges required for arbitrage
            if len(snapshot.prices) < 2:
                continue

            if now_ms - snapshot.timestamp_ms > stale_threshold_ms:
                continue

            min_exchange, min_price = min(snapshot.prices.items(), key=lambda item: item[1])
            max_exchange, max_price = max(snapshot.prices.items(), key=lambda item: item[1])

            if min_price <= 0 or max_price <= 0:
                continue

            spread = max_price - min_price
            spread_pct = (spread / min_price) * 100.0

            notional = self._settings.notional_usdt_default
            quantity = notional / min_price

            # Get fees - prefer fee_fetcher, fallback to config, then default
            if self._fee_fetcher:
                buy_fee_info = await self._fee_fetcher.get_fee(min_exchange, snapshot.symbol)
                sell_fee_info = await self._fee_fetcher.get_fee(max_exchange, snapshot.symbol)
                fee_buy_rate = buy_fee_info.taker
                fee_sell_rate = sell_fee_info.taker
                buy_fee_pct = fee_buy_rate * 100  # Convert to percentage for display
                sell_fee_pct = fee_sell_rate * 100
            elif min_exchange in self._settings.fees:
                fee_buy_rate = self._settings.fees[min_exchange].taker
                fee_sell_rate = self._settings.fees[max_exchange].taker if max_exchange in self._settings.fees else 0.001
                buy_fee_pct = fee_buy_rate * 100
                sell_fee_pct = fee_sell_rate * 100
            else:
                fee_buy_rate = 0.001
                fee_sell_rate = 0.001
                buy_fee_pct = 0.1
                sell_fee_pct = 0.1

            fees_buy = notional * fee_buy_rate
            fees_sell = (quantity * max_price) * fee_sell_rate
            total_fees = fees_buy + fees_sell

            slippage = self._settings.slippage_bps / 10000.0 * notional

            gross_profit = (max_price - min_price) * quantity
            net_profit = gross_profit - total_fees - slippage

            if net_profit < self._settings.thresholds.min_profit_usdt:
                continue
            if spread_pct < self._settings.thresholds.min_spread_pct:
                continue

            buy_symbol = snapshot.exchange_symbols.get(min_exchange, snapshot.symbol)
            sell_symbol = snapshot.exchange_symbols.get(max_exchange, snapshot.symbol)

            results.append(
                ArbitrageOpportunity(
                    symbol=snapshot.symbol,
                    buy_exchange=min_exchange,
                    buy_price=min_price,
                    buy_symbol=buy_symbol,
                    buy_fee_pct=buy_fee_pct,
                    sell_exchange=max_exchange,
                    sell_price=max_price,
                    sell_symbol=sell_symbol,
                    sell_fee_pct=sell_fee_pct,
                    spread_usdt=net_profit,
                    spread_pct=spread_pct,
                    timestamp_ms=snapshot.timestamp_ms,
                )
            )

        results.sort(key=lambda item: item.spread_usdt, reverse=True)
        return results[: self._top_n]

