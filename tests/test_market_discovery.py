from __future__ import annotations

import pytest

from arbitrage_bot.exchanges.base import ExchangeMarket
from arbitrage_bot.services.market_discovery import MarketDiscoveryService


class DummyAdapter:
    def __init__(self, name: str, markets: list[ExchangeMarket]) -> None:
        self.name = name
        self._markets = markets

    async def fetch_markets(self) -> list[ExchangeMarket]:
        return self._markets

    async def quote_stream(self, symbols):  # pragma: no cover - not used in tests
        raise NotImplementedError

    async def close(self) -> None:  # pragma: no cover - not used in tests
        return


@pytest.mark.asyncio
async def test_market_discovery_intersection() -> None:
    bybit = DummyAdapter(
        "bybit",
        [
            ExchangeMarket(symbol="BTCUSDT", base_asset="BTC", quote_asset="USDT"),
            ExchangeMarket(symbol="ETHUSDT", base_asset="ETH", quote_asset="USDT"),
        ],
    )
    okx = DummyAdapter(
        "okx",
        [
            ExchangeMarket(symbol="BTC-USDT", base_asset="BTC", quote_asset="USDT"),
            ExchangeMarket(symbol="LTC-USDT", base_asset="LTC", quote_asset="USDT"),
        ],
    )

    service = MarketDiscoveryService([bybit, okx])

    result = await service.refresh()
    assert len(result) == 1
    market = result[0]
    assert market.symbol == "BTCUSDT"
    assert market.exchange_symbols == {"bybit": "BTCUSDT", "okx": "BTC-USDT"}

    cached = await service.get_cached()
    assert cached == result

