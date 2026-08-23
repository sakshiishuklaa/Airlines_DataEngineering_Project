"""CLI for Module 1 synthetic source generation."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from skyflow.config import SCALE_PRESETS, load_generator_config
from skyflow.generator.engine import FlightOpsGenerator
from skyflow.generator.writers import LakeWriter
from skyflow.logging_setup import configure_logging

LOGGER = logging.getLogger("skyflow.generator.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate relational synthetic airline source datasets into a local raw data lake."
    )
    parser.add_argument("--config", default="config/generator.yaml", help="Path to generator YAML.")
    parser.add_argument("--preset", choices=sorted(SCALE_PRESETS), help="Override scale.preset.")
    parser.add_argument("--flights", type=int, help="Override flight count.")
    parser.add_argument("--customers", type=int, help="Override customer count.")
    parser.add_argument("--output-root", help="Override raw lake root directory.")
    parser.add_argument("--format", choices=["parquet", "csv"], dest="output_format", help="File format.")
    parser.add_argument("--seed", type=int, help="Override RNG seed.")
    parser.add_argument("--ingestion-date", help="Partition date YYYY-MM-DD.")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(level=args.log_level)
    try:
        cfg = load_generator_config(args.config)
        if args.preset:
            cfg["scale"] = {**SCALE_PRESETS[args.preset], "preset": args.preset}
        if args.flights is not None:
            cfg["scale"]["flights"] = args.flights
        if args.customers is not None:
            cfg["scale"]["customers"] = args.customers
        if args.output_root:
            cfg["run"]["output_root"] = args.output_root
        if args.output_format:
            cfg["run"]["output_format"] = args.output_format
        if args.seed is not None:
            cfg["run"]["seed"] = args.seed
        if args.ingestion_date:
            cfg["run"]["ingestion_date"] = args.ingestion_date

        generator = FlightOpsGenerator(cfg)
        writer = LakeWriter(
            output_root=Path(cfg["run"]["output_root"]),
            ingestion_date=generator.ingestion_date,
            output_format=cfg["run"]["output_format"],
        )
        counts = generator.generate(writer)
        LOGGER.info("Success. Row counts: %s", counts)
        LOGGER.info("Raw lake root: %s", Path(cfg["run"]["output_root"]).resolve())
        return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        LOGGER.error("%s", exc)
        return 1
    except Exception:
        LOGGER.exception("Unhandled generator failure")
        return 2


if __name__ == "__main__":
    sys.exit(main())
