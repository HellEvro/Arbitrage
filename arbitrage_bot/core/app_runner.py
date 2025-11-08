from __future__ import annotations

import asyncio
import logging
import signal
import threading
import webbrowser
from typing import Any

from flask import Flask
from flask_socketio import SocketIO

from arbitrage_bot.config import Settings
from arbitrage_bot.services.arbitrage_engine import ArbitrageEngine
from arbitrage_bot.services.market_discovery import MarketDiscoveryService
from arbitrage_bot.services.quote_aggregator import QuoteAggregator
from arbitrage_bot.services.telegram_notifier import TelegramNotifier

log = logging.getLogger("arbitrage_bot.system")


class AppRunner:
    """Manages application lifecycle: loops, server, and shutdown."""

    def __init__(
        self,
        settings: Settings,
        aggregator: QuoteAggregator,
        engine: ArbitrageEngine,
        discovery: MarketDiscoveryService,
        notifier: TelegramNotifier,
        app: Flask,
        socketio: SocketIO,
    ) -> None:
        self._settings = settings
        self._aggregator = aggregator
        self._engine = engine
        self._discovery = discovery
        self._notifier = notifier
        self._app = app
        self._socketio = socketio
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()

        def _handle_stop(*_: Any) -> None:
            log.info("Received shutdown signal")
            self._stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle_stop)
            except NotImplementedError:
                # Windows doesn't support all signals
                pass

    async def start(self) -> None:
        """Start all application components."""
        log.info("Starting application runner")
        self.setup_signal_handlers()

        await self._aggregator.start()
        log.info("Quote aggregator started")

        self._tasks.append(asyncio.create_task(self._evaluation_loop(), name="evaluation-loop"))
        self._tasks.append(asyncio.create_task(self._discovery_loop(), name="discovery-loop"))
        self._tasks.append(asyncio.create_task(self._run_flask_server(), name="flask-server"))

    async def _evaluation_loop(self) -> None:
        """Continuously evaluate arbitrage opportunities."""
        try:
            while not self._stop_event.is_set():
                opportunities = await self._engine.evaluate()
                await self._notifier.notify(opportunities)
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.exception("Error in evaluation loop: %s", e)

    async def _discovery_loop(self) -> None:
        """Periodically refresh market discovery."""
        try:
            while not self._stop_event.is_set():
                markets = await self._discovery.refresh()
                await self._aggregator.refresh_markets(markets)
                await asyncio.sleep(self._discovery.refresh_interval)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.exception("Error in discovery loop: %s", e)

    async def _run_flask_server(self) -> None:
        """Run Flask server in a separate thread."""
        def run_socketio() -> None:
            self._socketio.run(
                self._app,
                host=self._settings.web.host,
                port=self._settings.web.port,
                allow_unsafe_werkzeug=True,
                use_reloader=False,
            )

        flask_thread = threading.Thread(target=run_socketio, daemon=True)
        flask_thread.start()
        log.info("Web server starting on %s:%d", self._settings.web.host, self._settings.web.port)

        async def open_browser() -> None:
            await asyncio.sleep(1.5)
            url = f"http://localhost:{self._settings.web.port}"
            log.info("Opening browser at %s", url)
            webbrowser.open(url)

        asyncio.create_task(open_browser(), name="open-browser")

        await self._stop_event.wait()
        flask_thread.join(timeout=1.0)

    async def stop(self) -> None:
        """Stop all application components gracefully."""
        log.info("Stopping application runner")
        self._stop_event.set()

        for task in self._tasks:
            task.cancel()

        await self._aggregator.stop()
        await self._notifier.close()

        await asyncio.gather(*self._tasks, return_exceptions=True)
        log.info("Application runner stopped")

    async def wait(self) -> None:
        """Wait for shutdown signal."""
        await self._stop_event.wait()

