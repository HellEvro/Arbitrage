from __future__ import annotations

import time

import pytest

from arbitrage_bot.config.models import Settings
from arbitrage_bot.services.arbitrage_engine import ArbitrageEngine
from arbitrage_bot.services.quote_store import QuoteStore


@pytest.mark.asyncio
async def test_arbitrage_engine_detects_opportunity() -> None:
    settings = Settings.model_validate(
        {
            "exchanges": ["bybit", "mexc"],
            "fees": {
                "bybit": {"taker": 0.001, "maker": 0.001},
                "mexc": {"taker": 0.001, "maker": 0.001},
            },
            "thresholds": {"min_profit_usdt": 0.1, "min_spread_pct": 0.0, "stale_ms": 1500},
            "telegram": {"enabled": False},
            "notional_usdt_default": 1000,
        }
    )

    store = QuoteStore()
    now_ms = int(time.time() * 1000)
    await store.upsert("BTCUSDT", "bybit", 100.0, timestamp_ms=now_ms)
    await store.upsert("BTCUSDT", "mexc", 105.0, timestamp_ms=now_ms)

    engine = ArbitrageEngine(store, settings, top_n=5)
    opportunities = await engine.evaluate()

    assert len(opportunities) == 1
    opportunity = opportunities[0]
    assert opportunity.symbol == "BTCUSDT"
    assert opportunity.buy_exchange == "bybit"
    assert opportunity.sell_exchange == "mexc"
    assert opportunity.spread_usdt > 0
    assert opportunity.buy_symbol == "BTCUSDT"
    assert opportunity.sell_symbol == "BTCUSDT"
    assert opportunity.buy_fee_pct >= 0
    assert opportunity.sell_fee_pct >= 0
    assert opportunity.gross_profit_usdt > 0
    assert opportunity.total_fees_usdt >= 0
    # Чистая прибыль = валовая прибыль - комиссии - проскальзывание
    # Проверяем что чистая прибыль меньше или равна валовой минус комиссии
    assert opportunity.spread_usdt <= opportunity.gross_profit_usdt - opportunity.total_fees_usdt

