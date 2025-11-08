from __future__ import annotations

import asyncio
import logging
import os
import sys

from arbitrage_bot.bootstrap import build_app_components
from arbitrage_bot.config import load_settings
from arbitrage_bot.core.app_runner import AppRunner
from arbitrage_bot.core.port_cleanup import cleanup_port, find_process_on_port, is_python_process, is_process_running
from arbitrage_bot.web import create_app
import time

# Setup basic logging before anything else
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)

log = logging.getLogger("arbitrage_bot.system")


async def main() -> None:
    log.info("Starting arbitrage bot system")
    
    # Load settings first to get port configuration
    settings = load_settings()
    port = settings.web.port
    
    # Clean up any existing processes on the port - MUST succeed before continuing
    log.info("Checking and cleaning up port %d (from config)", port)
    max_cleanup_attempts = 3
    cleanup_success = False
    
    for attempt in range(max_cleanup_attempts):
        if cleanup_port(port, wait_timeout=20.0):
            cleanup_success = True
            log.info("Port %d successfully cleaned up on attempt %d", port, attempt + 1)
            break
        else:
            log.warning("Port cleanup attempt %d failed, retrying...", attempt + 1)
            if attempt < max_cleanup_attempts - 1:
                time.sleep(2.0)  # Wait before retry
    
    if not cleanup_success:
        log.error("FAILED to clean up port %d after %d attempts. Aborting startup!", port, max_cleanup_attempts)
        raise RuntimeError(f"Port {port} is still in use and could not be freed. Please manually terminate processes.")
    
    # Final verification - port must be free (excluding current process)
    current_pid = os.getpid()
    final_check = find_process_on_port(port)
    if final_check:
        # Исключаем текущий процесс из проверки
        final_check_filtered = [pid for pid in final_check if pid > 0 and pid != current_pid]
        if final_check_filtered:
            # Проверяем, действительно ли процессы еще работают
            final_python = [pid for pid in final_check_filtered if is_python_process(pid) and is_process_running(pid)]
            if final_python:
                log.error("Port %d verification failed - still has running Python processes: %s", port, final_python)
                raise RuntimeError(f"Port {port} verification failed - processes still running: {final_python}")
            else:
                log.info("Port %d has PIDs but they are not running Python processes (likely TIME_WAIT): %s", port, final_check_filtered)
        else:
            log.debug("Port %d final check: only current process found (OK)", port)
    
    log.info("Port %d verified free - proceeding with startup", port)
    
    # Build all application components (will reload settings, but that's OK)
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
        aggregator=aggregator,
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

