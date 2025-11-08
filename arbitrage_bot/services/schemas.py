from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(slots=True)
class MarketInfo:
    symbol: str
    exchanges: Sequence[str]
    exchange_symbols: dict[str, str]


@dataclass(slots=True)
class QuoteSnapshot:
    symbol: str
    prices: dict[str, float]
    timestamp_ms: int
    exchange_symbols: dict[str, str]


@dataclass(slots=True)
class ArbitrageOpportunity:
    symbol: str
    buy_exchange: str
    buy_price: float
    buy_symbol: str
    sell_exchange: str
    sell_price: float
    sell_symbol: str
    spread_usdt: float
    spread_pct: float
    timestamp_ms: int

