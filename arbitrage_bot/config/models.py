from __future__ import annotations

from typing import Literal, Sequence

from pydantic import BaseModel, Field, PositiveFloat

ExchangeName = Literal["bybit", "mexc", "bitget", "okx", "kucoin"]


class ExchangeConfig(BaseModel):
    name: ExchangeName
    rest_base_url: str
    websocket_url: str | None = None
    rate_limit_per_sec: PositiveFloat = Field(default=5.0)


class FeeConfig(BaseModel):
    taker: float = Field(default=0.001, ge=0.0)
    maker: float = Field(default=0.001, ge=0.0)


class TelegramConfig(BaseModel):
    enabled: bool = True
    bot_token: str = Field(default="", min_length=0)
    chat_id: str = Field(default="", min_length=0)
    notify_interval_sec: PositiveFloat = Field(default=60)
    min_profit_usdt: PositiveFloat = Field(default=1.0)


class ThresholdsConfig(BaseModel):
    min_profit_usdt: float = Field(default=0.5, ge=0.0)
    min_spread_pct: float = Field(default=0.05, ge=0.0)
    stale_ms: int = Field(default=1500, ge=0)


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 5152
    cors_origins: Sequence[str] = Field(default_factory=lambda: ["*"])


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO")
    json: bool = Field(default=False)


class RedisConfig(BaseModel):
    enabled: bool = False
    url: str = "redis://localhost:6379/0"
    namespace: str = "arbitrage_bot"


class Settings(BaseModel):
    exchanges: Sequence[ExchangeName] = Field(default_factory=lambda: ["bybit", "mexc", "bitget", "okx", "kucoin"])
    notional_usdt_default: PositiveFloat = Field(default=1000)
    fees: dict[ExchangeName, FeeConfig] = Field(default_factory=dict)
    slippage_bps: float = Field(default=3.0, ge=0.0)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    telegram: TelegramConfig = Field(default_factory=TelegramConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    exchange_overrides: dict[ExchangeName, ExchangeConfig] = Field(default_factory=dict)

