"""Orchestrate Module 1 generation into Module 2 multi-system source extracts."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from skyflow.generator.engine import FlightOpsGenerator
from skyflow.sources.catalog import SOURCE_SYSTEMS
from skyflow.sources.cdc import annotate_cdc, plan_batches, slice_entity
from skyflow.sources.collector import FrameCollector
from skyflow.sources.defects import apply_defects
from skyflow.sources.writers import apply_export_conventions, utc_now_iso, write_extract, write_json

LOGGER = logging.getLogger(__name__)


def _batch_identity(extract_date: str) -> tuple[str, str]:
    token = uuid.uuid4().hex[:8].upper()
    compact = extract_date.replace("-", "")
    batch_id = f"SKY{compact}-{token}"
    return batch_id, token


def _ingestion_ts(extract_date: str, offset: str) -> str:
    return datetime.fromisoformat(f"{extract_date}T{offset}").replace(tzinfo=timezone.utc).isoformat()


def run_source_layer(source_cfg: dict[str, Any], generator_cfg: dict[str, Any]) -> dict[str, Any]:
    output_root = Path(source_cfg["run"]["output_root"])
    env = str(source_cfg["run"].get("env", "PROD"))
    mode = str(source_cfg["run"]["mode"])
    extract_dates = list(source_cfg["run"]["extract_dates"])
    apply_issues = bool(source_cfg["run"].get("apply_defects", True))
    holdback = float(source_cfg.get("cdc", {}).get("holdback_frac", 0.12))
    update_frac = float(source_cfg.get("cdc", {}).get("update_frac", 0.08))
    seed = int(generator_cfg["run"]["seed"])

    if mode == "full":
        holdback = 0.0
        update_frac = 0.0

    plans = plan_batches(mode, extract_dates)
    LOGGER.info(
        "Source layer start mode=%s dates=%s defects=%s output=%s",
        mode,
        extract_dates,
        apply_issues,
        output_root.resolve(),
    )

    collector = FrameCollector()
    generator = FlightOpsGenerator(generator_cfg)
    counts = generator.generate(collector)
    frames = collector.to_frames()
    frames = annotate_cdc(frames, extract_dates, seed=seed, holdback_frac=holdback, update_frac=update_frac)

    run_files: list[dict[str, Any]] = []
    date_summaries: list[dict[str, Any]] = []

    for plan in plans:
        batch_id, token = _batch_identity(plan.extract_date)
        day_files: list[dict[str, Any]] = []
        day_counts: dict[str, int] = {}
        day_defects: dict[str, list[str]] = {}

        for system in SOURCE_SYSTEMS:
            ingest_ts = _ingestion_ts(plan.extract_date, system.landing_offset)
            directory = output_root / system.code / f"extract_date={plan.extract_date}"
            for dataset in system.datasets:
                sliced = slice_entity(frames[dataset.entity], dataset.entity, plan)
                exported = apply_export_conventions(sliced, dataset)
                dirty, notes = apply_defects(dataset.entity, exported, seed=seed, enabled=apply_issues)
                path = write_extract(
                    directory,
                    system,
                    dataset,
                    dirty,
                    env=env,
                    extract_date=plan.extract_date,
                    batch_id=batch_id,
                    batch_token=token,
                    ingestion_timestamp=ingest_ts,
                )
                record = {
                    "source_system": system.name,
                    "source_system_code": system.code,
                    "entity": dataset.entity,
                    "extract_mode": plan.mode,
                    "extract_date": plan.extract_date,
                    "format": dataset.format,
                    "path": str(path),
                    "file_name": path.name,
                    "row_count": int(len(dirty)),
                    "batch_id": batch_id,
                    "defects": notes,
                }
                day_files.append(record)
                run_files.append(record)
                day_counts[dataset.entity] = int(len(dirty))
                if notes:
                    day_defects[dataset.entity] = notes

            write_json(
                directory / "_extract_manifest.json",
                {
                    "source_system": system.name,
                    "source_system_code": system.code,
                    "extract_date": plan.extract_date,
                    "extract_mode": plan.mode,
                    "batch_id": batch_id,
                    "ingestion_timestamp": ingest_ts,
                    "files": [f for f in day_files if f["source_system_code"] == system.code],
                },
            )

        date_summaries.append(
            {
                "extract_date": plan.extract_date,
                "extract_mode": plan.mode,
                "batch_id": batch_id,
                "row_counts": day_counts,
                "defects": day_defects,
            }
        )

    run_manifest = {
        "generated_at_utc": utc_now_iso(),
        "mode": mode,
        "env": env,
        "extract_dates": extract_dates,
        "output_root": str(output_root),
        "generator_row_counts": counts,
        "batches": date_summaries,
        "files": run_files,
        "module": 2,
        "s3": False,
    }
    write_json(output_root / "_run_manifest.json", run_manifest)
    write_json(
        output_root / "_cdc_state.json",
        {
            "last_extract_date": extract_dates[-1],
            "mode": mode,
            "extract_dates": extract_dates,
            "updated_at_utc": utc_now_iso(),
        },
    )
    LOGGER.info("Source layer complete. Manifest: %s", output_root / "_run_manifest.json")
    return run_manifest
