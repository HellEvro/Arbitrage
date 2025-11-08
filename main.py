from __future__ import annotations

import asyncio
import logging
import signal
import webbrowser
from typing import Any

import uvicorn

from arbitrage_bot.bootstrap import build_app_components
from arbitrage_bot.web import create_app

log = logging.getLogger("arbitrage_bot.system")


async def main() -> None:
    log.info("Starting arbitrage bot system")
    (
        settings,
        http_factory,
        adapters,
        discovery,
        quote_store,
        aggregator,
        engine,
        notifier,
    ) = await build_app_components()

    log.info("Application components initialized")

    _app, socketio = create_app(
        settings,
        engine,
        discovery=discovery,
        quote_store=quote_store,
        notifier=notifier,
    )

    stop_event = asyncio.Event()

    def _handle_stop(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except NotImplementedError:
            pass

    await aggregator.start()
    log.info("Quote aggregator started")

    async def evaluation_loop() -> None:
        try:
            while not stop_event.is_set():
                opportunities = await engine.evaluate()
                await notifier.notify(opportunities)
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            return

    async def discovery_loop() -> None:
        try:
            while not stop_event.is_set():
                markets = await discovery.refresh()
                await aggregator.refresh_markets(markets)
                await asyncio.sleep(discovery.refresh_interval)
        except asyncio.CancelledError:
            return

    evaluation_task = asyncio.create_task(evaluation_loop(), name="evaluation-loop")
    discovery_task = asyncio.create_task(discovery_loop(), name="discovery-loop")

    config = uvicorn.Config(
        socketio.asgi_app,
        host=settings.web.host,
        port=settings.web.port,
        loop="asyncio",
        lifespan="off",
        log_config=None,
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve(), name="uvicorn-server")

    def _on_server_done(_: asyncio.Future[Any]) -> None:
        stop_event.set()

    server_task.add_done_callback(_on_server_done)

    async def open_browser() -> None:
        await asyncio.sleep(1.5)
        url = f"http://localhost:{settings.web.port}"
        log.info("Opening browser at %s", url)
        webbrowser.open(url)

    asyncio.create_task(open_browser(), name="open-browser")
    log.info("Web server starting on %s:%d", settings.web.host, settings.web.port)

    await stop_event.wait()
    server.should_exit = True

    evaluation_task.cancel()
    discovery_task.cancel()

    await aggregator.stop()
    await notifier.close()
    await asyncio.gather(*(adapter.close() for adapter in adapters), return_exceptions=True)
    await http_factory.close()

    if not server_task.done():
        await server.shutdown()
    await asyncio.gather(evaluation_task, discovery_task, return_exceptions=True)
    await server_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        ...

