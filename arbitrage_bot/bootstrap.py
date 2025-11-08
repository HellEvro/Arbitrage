from __future__ import annotations

from typing import Sequence

from arbitrage_bot.config import Settings, load_settings
from arbitrage_bot.core import HttpClientFactory, configure_logging
from arbitrage_bot.exchanges.base import ExchangeAdapter
from arbitrage_bot.exchanges.bitget import BitgetAdapter
from arbitrage_bot.exchanges.bybit import BybitAdapter
from arbitrage_bot.exchanges.kucoin import KucoinAdapter
from arbitrage_bot.exchanges.mexc import MexcAdapter
from arbitrage_bot.exchanges.okx import OkxAdapter
from arbitrage_bot.services.arbitrage_engine import ArbitrageEngine
from arbitrage_bot.services.market_discovery import MarketDiscoveryService
from arbitrage_bot.services.quote_aggregator import QuoteAggregator
from arbitrage_bot.services.quote_store import QuoteStore
from arbitrage_bot.services.telegram_notifier import TelegramNotifier


def create_adapters(settings: Settings, http_factory: HttpClientFactory) -> Sequence[ExchangeAdapter]:
    adapters: list[ExchangeAdapter] = []
    for exchange in settings.exchanges:
        match exchange:
            case "bybit":
                adapters.append(BybitAdapter(http_factory, poll_interval=1.0))
            case "mexc":
                adapters.append(MexcAdapter(http_factory, poll_interval=1.0))
            case "bitget":
                adapters.append(BitgetAdapter(http_factory, poll_interval=1.0))
            case "okx":
                adapters.append(OkxAdapter(http_factory, poll_interval=1.0))
            case "kucoin":
                adapters.append(KucoinAdapter(http_factory, poll_interval=1.0))
            case _:
                raise ValueError(f"Unsupported exchange: {exchange}")
    return adapters


async def build_app_components(config_path: str | None = None) -> tuple[
    Settings,
    HttpClientFactory,
    Sequence[ExchangeAdapter],
    MarketDiscoveryService,
    QuoteStore,
    QuoteAggregator,
    ArbitrageEngine,
    TelegramNotifier,
]:
    settings = load_settings(config_path)
    configure_logging(settings.logging)

    http_factory = HttpClientFactory()
    adapters = create_adapters(settings, http_factory)
    discovery = MarketDiscoveryService(adapters)
    quote_store = QuoteStore()
    arbitrage_engine = ArbitrageEngine(quote_store, settings)
    notifier = TelegramNotifier(settings)

    markets = await discovery.refresh()
    aggregator = QuoteAggregator(adapters, quote_store, markets)

    return settings, http_factory, adapters, discovery, quote_store, aggregator, arbitrage_engine, notifier

