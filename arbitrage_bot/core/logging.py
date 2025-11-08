from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import structlog

from arbitrage_bot.config.models import LoggingConfig

_MAX_LOG_SIZE = 10 * 1024 * 1024
_BACKUP_COUNT = 5


def _create_file_handler(log_file: Path, level: str) -> RotatingFileHandler:
    handler = RotatingFileHandler(
        log_file,
        maxBytes=_MAX_LOG_SIZE,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    return handler


def _setup_logger(logger_name: str, log_file: str, level: str, logs_dir: Path) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_path = logs_dir / log_file
    file_handler = _create_file_handler(file_path, level)
    logger.addHandler(file_handler)

    return logger


def configure_logging(config: LoggingConfig) -> None:
    shared_processors: list[structlog.types.Processor] = [
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: structlog.types.Processor
    if config.json:
        renderer = structlog.processors.JSONRenderer(sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(config.level)),
        cache_logger_on_first_use=True,
    )

    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    level = config.level

    _setup_logger("arbitrage_bot.system", "system.log", level, logs_dir)
    _setup_logger("arbitrage_bot.exchanges.bybit", "bybit.log", level, logs_dir)
    _setup_logger("arbitrage_bot.exchanges.mexc", "mexc.log", level, logs_dir)
    _setup_logger("arbitrage_bot.exchanges.bitget", "bitget.log", level, logs_dir)
    _setup_logger("arbitrage_bot.exchanges.okx", "okx.log", level, logs_dir)
    _setup_logger("arbitrage_bot.exchanges.kucoin", "kucoin.log", level, logs_dir)
    _setup_logger("arbitrage_bot.services.market_discovery", "market_discovery.log", level, logs_dir)
    _setup_logger("arbitrage_bot.services.quote_aggregator", "quote_aggregator.log", level, logs_dir)
    _setup_logger("arbitrage_bot.services.arbitrage_engine", "arbitrage_engine.log", level, logs_dir)
    _setup_logger("arbitrage_bot.services.telegram_notifier", "telegram_notifier.log", level, logs_dir)
    _setup_logger("arbitrage_bot.web", "web.log", level, logs_dir)
    _setup_logger("arbitrage_bot.core.http", "http.log", level, logs_dir)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

