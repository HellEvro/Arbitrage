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
    base_asset: str | None = None  # Базовая валюта (например, "GAME")
    quote_asset: str | None = None  # Котируемая валюта (например, "USDT")


@dataclass(slots=True)
class ArbitrageOpportunity:
    symbol: str
    buy_exchange: str
    buy_price: float
    buy_symbol: str
    buy_fee_pct: float  # Fee percentage for buy exchange
    sell_exchange: str
    sell_price: float
    sell_symbol: str
    sell_fee_pct: float  # Fee percentage for sell exchange
    spread_usdt: float  # Net profit after fees and slippage
    spread_pct: float
    gross_profit_usdt: float  # Gross profit before fees
    total_fees_usdt: float  # Total fees (buy + sell)
    timestamp_ms: int
    base_asset: str | None = None  # Базовая валюта (например, "GAME")
    quote_asset: str | None = None  # Котируемая валюта (например, "USDT")
    is_stable: bool = False  # Стабильная возможность (цена выше на одной бирже в течение 5 минут)

