from __future__ import annotations

import asyncio
import time
from typing import Iterable

from arbitrage_bot.services.schemas import QuoteSnapshot


class QuoteStore:
    def __init__(self) -> None:
        self._quotes: dict[str, QuoteSnapshot] = {}
        self._lock = asyncio.Lock()

    async def upsert(
        self,
        symbol: str,
        exchange: str,
        price: float,
        *,
        timestamp_ms: int | None = None,
        native_symbol: str | None = None,
        base_asset: str | None = None,
        quote_asset: str | None = None,
    ) -> None:
        ts = timestamp_ms or int(time.time() * 1000)
        exchange_key = exchange.lower()
        native = (native_symbol or symbol).upper()
        async with self._lock:
            snapshot = self._quotes.get(symbol)
            if snapshot:
                snapshot.prices[exchange_key] = price
                snapshot.exchange_symbols[exchange_key] = native
                snapshot.timestamp_ms = ts
                # Обновляем base_asset и quote_asset если они предоставлены
                if base_asset:
                    snapshot.base_asset = base_asset
                if quote_asset:
                    snapshot.quote_asset = quote_asset
            else:
                self._quotes[symbol] = QuoteSnapshot(
                    symbol=symbol,
                    prices={exchange_key: price},
                    exchange_symbols={exchange_key: native},
                    timestamp_ms=ts,
                    base_asset=base_asset,
                    quote_asset=quote_asset,
                )

    async def get(self, symbol: str) -> QuoteSnapshot | None:
        async with self._lock:
            snapshot = self._quotes.get(symbol)
            if not snapshot:
                return None
            return QuoteSnapshot(
                symbol=snapshot.symbol,
                prices=dict(snapshot.prices),
                exchange_symbols=dict(snapshot.exchange_symbols),
                timestamp_ms=snapshot.timestamp_ms,
                base_asset=snapshot.base_asset,
                quote_asset=snapshot.quote_asset,
            )

    async def list(self) -> Iterable[QuoteSnapshot]:
        async with self._lock:
            return [
                QuoteSnapshot(
                    symbol=qs.symbol,
                    prices=dict(qs.prices),
                    exchange_symbols=dict(qs.exchange_symbols),
                    timestamp_ms=qs.timestamp_ms,
                    base_asset=qs.base_asset,
                    quote_asset=qs.quote_asset,
                )
                for qs in self._quotes.values()
            ]

