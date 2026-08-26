"""CLI for Module 2 airline source-system extracts."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from skyflow.config import SCALE_PRESETS, load_generator_config, load_sources_config
from skyflow.logging_setup import configure_logging
from skyflow.sources.pipeline import run_source_layer

LOGGER = logging.getLogger("skyflow.sources.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Land synthetic airline data as multi-system source extracts (local; no S3)."
    )
    parser.add_argument("--config", default="config/sources.yaml", help="Path to sources YAML.")
    parser.add_argument("--generator-config", help="Override generator YAML path.")
    parser.add_argument("--preset", choices=sorted(SCALE_PRESETS), help="Override generator scale preset.")
    parser.add_argument("--mode", choices=["full", "incremental", "window"], help="Extract mode.")
    parser.add_argument(
        "--extract-dates",
        help="Comma-separated YYYY-MM-DD dates, e.g. 2026-08-23,2026-08-24,2026-08-25.",
    )
    parser.add_argument("--extract-date", help="Single extract date (full or one incremental day).")
    parser.add_argument("--output-root", help="Override source landing root (default data/sources).")
    parser.add_argument("--seed", type=int, help="Override RNG seed.")
    parser.add_argument("--no-defects", action="store_true", help="Disable simulated source-system data issues.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def _parse_dates(raw: str) -> list[str]:
    dates = [part.strip() for part in raw.split(",") if part.strip()]
    if not dates:
        raise ValueError("extract dates must be a non-empty comma-separated list")
    for item in dates:
        datetime_parse(item)
    return dates


def datetime_parse(value: str) -> None:
    from datetime import date

    date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(level=args.log_level)
    try:
        source_cfg = load_sources_config(args.config)
        gen_path = args.generator_config or source_cfg["run"].get("generator_config") or "config/generator.yaml"
        generator_cfg = load_generator_config(gen_path)

        if args.preset:
            generator_cfg["scale"] = {**SCALE_PRESETS[args.preset], "preset": args.preset}
        if args.seed is not None:
            generator_cfg["run"]["seed"] = args.seed
        if args.output_root:
            source_cfg["run"]["output_root"] = args.output_root
        if args.mode:
            source_cfg["run"]["mode"] = args.mode
        if args.extract_date and args.extract_dates:
            raise ValueError("Pass either --extract-date or --extract-dates, not both")
        if args.extract_date:
            source_cfg["run"]["extract_dates"] = [args.extract_date]
        if args.extract_dates:
            source_cfg["run"]["extract_dates"] = _parse_dates(args.extract_dates)
        if args.no_defects:
            source_cfg["run"]["apply_defects"] = False

        # Source layer must not write the Module 1 lake; collector is in-memory.
        generator_cfg["run"]["output_root"] = str(Path(source_cfg["run"]["output_root"]) / "_unused_lake")

        manifest = run_source_layer(source_cfg, generator_cfg)
        LOGGER.info("Success. Batches: %s", manifest["batches"])
        LOGGER.info("Source root: %s", Path(source_cfg["run"]["output_root"]).resolve())
        return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception:
        LOGGER.exception("Unhandled source-layer failure")
        return 2


if __name__ == "__main__":
    sys.exit(main())
