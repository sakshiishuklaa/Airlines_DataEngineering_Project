"""Load YAML + environment configuration. Credentials never live in code."""

from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

SCALE_PRESETS: dict[str, dict[str, Any]] = {
    "demo": {
        "airlines": 18,
        "airports": 60,
        "aircraft": 140,
        "routes": 200,
        "customers": 4_000,
        "flights": 2_500,
        "mean_load_factor": 0.62,
        "feedback_rate": 0.12,
        "baggage_rate": 0.85,
        "payment_success_rate": 0.94,
    },
    "interview": {
        "airlines": 25,
        "airports": 80,
        "aircraft": 800,
        "routes": 900,
        "customers": 80_000,
        "flights": 100_000,
        "mean_load_factor": 0.80,
        "feedback_rate": 0.10,
        "baggage_rate": 0.85,
        "payment_success_rate": 0.93,
    },
    "large": {
        "airlines": 30,
        "airports": 90,
        "aircraft": 2_000,
        "routes": 1_400,
        "customers": 250_000,
        "flights": 500_000,
        "mean_load_factor": 0.81,
        "feedback_rate": 0.08,
        "baggage_rate": 0.86,
        "payment_success_rate": 0.93,
    },
    "xl": {
        "airlines": 35,
        "airports": 100,
        "aircraft": 3_500,
        "routes": 1_800,
        "customers": 600_000,
        "flights": 1_000_000,
        "mean_load_factor": 0.82,
        "feedback_rate": 0.06,
        "baggage_rate": 0.86,
        "payment_success_rate": 0.93,
    },
}


def load_env(env_file: str | Path | None = None) -> None:
    candidate = Path(env_file) if env_file else Path(".env")
    if candidate.is_file():
        load_dotenv(candidate, override=False)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")
    return data


def load_generator_config(path: str | Path) -> dict[str, Any]:
    """Merge YAML, scale preset, and environment overrides."""
    load_env()
    cfg = deepcopy(_read_yaml(Path(path)))

    scale = cfg.setdefault("scale", {})
    preset_name = str(scale.get("preset") or os.getenv("SKYFLOW_SCALE_PRESET") or "demo")
    if preset_name not in SCALE_PRESETS:
        raise ValueError(f"Unknown scale preset '{preset_name}'. Choose from: {sorted(SCALE_PRESETS)}")
    merged_scale = {**SCALE_PRESETS[preset_name], **{k: v for k, v in scale.items() if v is not None}}
    merged_scale["preset"] = preset_name
    cfg["scale"] = merged_scale

    run = cfg.setdefault("run", {})
    run["seed"] = int(os.getenv("SKYFLOW_RANDOM_SEED", run.get("seed", 42)))
    run["output_root"] = os.getenv("SKYFLOW_OUTPUT_ROOT", run.get("output_root", "data/lake/raw"))
    run["output_format"] = os.getenv("SKYFLOW_OUTPUT_FORMAT", run.get("output_format", "parquet"))
    return cfg
