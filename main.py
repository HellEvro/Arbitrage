from __future__ import annotations

import asyncio
import logging

from arbitrage_bot.bootstrap import build_app_components
from arbitrage_bot.core.app_runner import AppRunner
from arbitrage_bot.web import create_app

log = logging.getLogger("arbitrage_bot.system")


async def main() -> None:
    log.info("Starting arbitrage bot system")
    
    # Build all application components
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

    # Create Flask app and SocketIO
    app, socketio = create_app(
        settings,
        engine,
        discovery=discovery,
        quote_store=quote_store,
        notifier=notifier,
    )

    # Create and start application runner
    runner = AppRunner(
        settings=settings,
        aggregator=aggregator,
        engine=engine,
        discovery=discovery,
        notifier=notifier,
        app=app,
        socketio=socketio,
    )

    try:
        await runner.start()
        await runner.wait()
    finally:
        # Cleanup
        await runner.stop()
        await asyncio.gather(*(adapter.close() for adapter in adapters), return_exceptions=True)
        await http_factory.close()
        log.info("Application shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        ...

