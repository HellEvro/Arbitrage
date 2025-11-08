from __future__ import annotations

import asyncio
import logging
import time
from typing import Sequence

from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.utils.token import TokenValidationError

from arbitrage_bot.config.models import Settings
from arbitrage_bot.core.exceptions import NotificationError
from arbitrage_bot.services.schemas import ArbitrageOpportunity

log = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._last_sent_symbol: str | None = None
        self._last_sent_ts: float = 0.0
        self._lock = asyncio.Lock()
        self._bot: Bot | None = None
        self._enabled_override: bool | None = None

    async def notify(self, opportunities: Sequence[ArbitrageOpportunity]) -> None:
        if not self._is_enabled():
            return
        if not opportunities:
            return

        async with self._lock:
            now = time.time()
            interval = self._settings.telegram.notify_interval_sec
            top = opportunities[0]
            if top.spread_usdt < self._settings.telegram.min_profit_usdt:
                return
            if self._last_sent_symbol == top.symbol and now - self._last_sent_ts < interval:
                return

            message = self._format_message(top)
            await self._send_message(message)
            self._last_sent_symbol = top.symbol
            self._last_sent_ts = now

    async def _send_message(self, message: str) -> None:
        try:
            bot = await self._get_bot()
        except NotificationError as exc:
            log.warning("Telegram notifier misconfigured: %s", exc)
            return

        if not bot:
            log.info("Telegram notification skipped: bot disabled")
            return

        chat_id = self._settings.telegram.chat_id
        if not chat_id:
            log.warning("Telegram chat_id not set; skipping notification")
            return

        try:
            await bot.send_message(chat_id=chat_id, text=message, disable_web_page_preview=True)
            log.info("Telegram notification sent", extra={"chat_id": chat_id})
        except TelegramAPIError as exc:
            log.exception("Failed to send telegram notification: %s", exc)
            raise NotificationError(str(exc)) from exc

    def _format_message(self, opportunity: ArbitrageOpportunity) -> str:
        buy_exchange_cap = opportunity.buy_exchange.capitalize()
        sell_exchange_cap = opportunity.sell_exchange.capitalize()
        return (
            f"🔔 Арбитраж: {opportunity.symbol}\n"
            f"Купи на {buy_exchange_cap} по {opportunity.buy_price:.1f} (комиссия {opportunity.buy_fee_pct:.3f}%) → "
            f"Продай на {sell_exchange_cap} по {opportunity.sell_price:.1f} (комиссия {opportunity.sell_fee_pct:.3f}%)\n"
            f"Разница: +{opportunity.spread_usdt:.1f} USDT ({opportunity.spread_pct:.3f}%)"
        )

    async def _get_bot(self) -> Bot | None:
        if self._bot:
            return self._bot
        token = self._settings.telegram.bot_token.strip()
        if not token or token == "<YOUR_TOKEN>":
            raise NotificationError("bot_token is empty or not configured")
        try:
            self._bot = Bot(token=token)
            return self._bot
        except TokenValidationError as e:
            log.warning("Invalid Telegram bot token: %s", e)
            raise NotificationError(f"Invalid bot token: {e}") from e

    def _is_enabled(self) -> bool:
        if self._enabled_override is not None:
            return self._enabled_override
        return self._settings.telegram.enabled

    def set_enabled(self, enabled: bool) -> None:
        self._enabled_override = enabled
        state = "enabled" if enabled else "disabled"
        log.info("Telegram notifier %s via override", state)

    def is_enabled(self) -> bool:
        return self._is_enabled()

    async def close(self) -> None:
        if self._bot:
            await self._bot.session.close()
            self._bot = None

