from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .models import Settings

DEFAULT_CONFIG_PATHS = (
    Path("config.yaml"),
    Path("config/config.yaml"),
    Path("config/config.example.yaml"),
)


def load_settings(path: str | Path | None = None, overrides: dict[str, Any] | None = None) -> Settings:
    """Load settings from provided path, optionally apply overrides, and return validated Settings."""
    data: dict[str, Any] = {}

    candidates = [Path(path)] if path else list(DEFAULT_CONFIG_PATHS)
    for candidate in candidates:
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as fp:
                data = yaml.safe_load(fp) or {}
            break

    if overrides:
        data.update(overrides)

    try:
        return Settings.model_validate(data)
    except ValidationError as exc:
        raise RuntimeError(f"Invalid configuration: {exc}") from exc


def find_config_path() -> Path | None:
    """Find the actual config file path being used."""
    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            return candidate
    return None


def save_filtering_config(filtering_config: dict[str, Any]) -> None:
    """Save filtering configuration to config.yaml file."""
    config_path = find_config_path()
    if not config_path:
        raise RuntimeError("Config file not found")
    
    # Load current config
    with config_path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    
    # Update filtering section
    if "filtering" not in data:
        data["filtering"] = {}
    data["filtering"].update(filtering_config)
    
    # Save back to file
    with config_path.open("w", encoding="utf-8") as fp:
        yaml.dump(data, fp, default_flow_style=False, allow_unicode=True, sort_keys=False)


def save_profit_config(profit_config: dict[str, Any]) -> None:
    """Save profit calculation configuration to config.yaml file."""
    config_path = find_config_path()
    if not config_path:
        raise RuntimeError("Config file not found")
    
    # Load current config
    with config_path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    
    # Update profit-related sections
    if "notional_usdt_default" in profit_config:
        data["notional_usdt_default"] = profit_config["notional_usdt_default"]
    if "slippage_bps" in profit_config:
        data["slippage_bps"] = profit_config["slippage_bps"]
    if "thresholds" not in data:
        data["thresholds"] = {}
    if "min_profit_usdt" in profit_config:
        data["thresholds"]["min_profit_usdt"] = profit_config["min_profit_usdt"]
    if "min_spread_pct" in profit_config:
        data["thresholds"]["min_spread_pct"] = profit_config["min_spread_pct"]
    
    # Save back to file
    with config_path.open("w", encoding="utf-8") as fp:
        yaml.dump(data, fp, default_flow_style=False, allow_unicode=True, sort_keys=False)

