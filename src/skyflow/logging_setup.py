"""Application logging. Config-driven; no secrets."""

from __future__ import annotations

import logging
from logging.config import dictConfig
from pathlib import Path

import yaml


def configure_logging(config_path: str | Path | None = None, level: str | None = None) -> None:
    path = Path(config_path) if config_path else Path("config/logging.yaml")
    if path.is_file():
        with path.open("r", encoding="utf-8") as handle:
            dictConfig(yaml.safe_load(handle))
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        )
    if level:
        logging.getLogger().setLevel(level.upper())
