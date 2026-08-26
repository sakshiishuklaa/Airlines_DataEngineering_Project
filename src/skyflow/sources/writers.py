"""Write source-system extract files (csv / json / jsonl / parquet) plus manifests."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from skyflow.sources.catalog import DatasetExtract, SourceSystem, filename_for

LOGGER = logging.getLogger(__name__)


def to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        ts = value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")
        return ts.isoformat()
    try:
        if pd.isna(value):
            return None
    except (ValueError, TypeError):
        pass
    if isinstance(value, np.generic):
        return value.item()
    if hasattr(value, "isoformat") and not isinstance(value, (str, bytes)):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


def records_for_json(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rec in frame.to_dict(orient="records"):
        rows.append({k: to_jsonable(v) for k, v in rec.items()})
    return rows


def add_metadata(
    frame: pd.DataFrame,
    *,
    source_system: str,
    ingestion_timestamp: str,
    batch_id: str,
    file_name: str,
) -> pd.DataFrame:
    out = frame.copy()
    out["source_system"] = source_system
    out["ingestion_timestamp"] = ingestion_timestamp
    out["batch_id"] = batch_id
    out["file_name"] = file_name
    return out


def apply_export_conventions(frame: pd.DataFrame, dataset: DatasetExtract) -> pd.DataFrame:
    out = frame.copy()
    mapping = {c.canonical: c.source for c in dataset.columns}
    missing = [c.canonical for c in dataset.columns if c.canonical not in out.columns]
    if missing:
        raise ValueError(f"{dataset.entity} missing canonical columns before extract: {missing}")
    renamed = out.rename(columns=mapping)
    source_cols = [c.source for c in dataset.columns]
    extras = [c for c in renamed.columns if c not in source_cols]
    renamed = renamed.loc[:, source_cols + extras]
    for canonical in dataset.bool_as_yn:
        source = mapping[canonical]
        if source in renamed.columns:
            renamed[source] = renamed[source].map(lambda v: "Y" if bool(v) else "N")
    return renamed


def write_extract(
    directory: Path,
    system: SourceSystem,
    dataset: DatasetExtract,
    frame: pd.DataFrame,
    *,
    env: str,
    extract_date: str,
    batch_id: str,
    batch_token: str,
    ingestion_timestamp: str,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    name = filename_for(env, system, dataset, extract_date, batch_token)
    target = directory / name
    payload = add_metadata(
        frame,
        source_system=system.name,
        ingestion_timestamp=ingestion_timestamp,
        batch_id=batch_id,
        file_name=name,
    )
    if dataset.format == "parquet":
        payload.to_parquet(target, index=False, engine="pyarrow")
    elif dataset.format == "csv":
        payload.to_csv(target, index=False)
    elif dataset.format == "jsonl":
        lines = [json.dumps(row, default=str) for row in records_for_json(payload)]
        target.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    elif dataset.format == "json" and dataset.json_style == "envelope":
        document = {
            "source_system": system.name,
            "ingestion_timestamp": ingestion_timestamp,
            "batch_id": batch_id,
            "file_name": name,
            "record_count": int(len(payload)),
            "transactions": records_for_json(payload),
        }
        target.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
    elif dataset.format == "json":
        target.write_text(json.dumps(records_for_json(payload), indent=2, default=str), encoding="utf-8")
    else:
        raise ValueError(f"Unsupported extract format {dataset.format} / {dataset.json_style}")
    LOGGER.info("Wrote %s rows=%s path=%s", dataset.entity, f"{len(payload):,}", target)
    return target


def write_json(path: Path, document: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
    return path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
