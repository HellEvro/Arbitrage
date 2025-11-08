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

