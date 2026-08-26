"""Discover and read Module 2 source extracts. Module 3 ingestion should use this API."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from skyflow.sources.catalog import (
    DATASET_BY_ENTITY,
    METADATA_COLUMNS,
    DatasetExtract,
    source_to_canonical,
)

FILENAME_RE = re.compile(
    r"^(?P<env>[A-Z0-9]+)_(?P<sys>[A-Z]+)_(?P<token>[A-Z]+)_(?P<ymd>\d{8})_(?P<batch>[A-Z0-9]+)\.(?P<ext>csv|json|jsonl|parquet)$"
)


@dataclass(frozen=True, slots=True)
class SourceFile:
    path: Path
    source_system_code: str
    extract_date: str
    entity: str
    format: str
    json_style: str | None
    file_name: str


def _entity_for_token(token: str) -> str:
    for entity, (_sys, dataset) in DATASET_BY_ENTITY.items():
        if dataset.filename_token == token:
            return entity
    raise ValueError(f"Unknown filename token: {token}")


def discover_extracts(root: str | Path) -> list[SourceFile]:
    """Walk data/sources and return every extract file (excludes manifests)."""
    root = Path(root)
    found: list[SourceFile] = []
    if not root.is_dir():
        return found
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.startswith("_"):
            continue
        match = FILENAME_RE.match(path.name)
        if not match:
            continue
        entity = _entity_for_token(match.group("token"))
        _sys, dataset = DATASET_BY_ENTITY[entity]
        ymd = match.group("ymd")
        extract_date = f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"
        found.append(
            SourceFile(
                path=path,
                source_system_code=match.group("sys").lower(),
                extract_date=extract_date,
                entity=entity,
                format=dataset.format,
                json_style=dataset.json_style,
                file_name=path.name,
            )
        )
    return found


def read_extract(path: str | Path, dataset: DatasetExtract | None = None) -> pd.DataFrame:
    """Read a source file as-emitted (source column names + metadata)."""
    path = Path(path)
    suffix = path.suffix.lower().lstrip(".")
    if suffix == "parquet":
        return pd.read_parquet(path)
    if suffix == "csv":
        return pd.read_csv(path)
    if suffix == "jsonl":
        if path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_json(path, lines=True)
    if suffix == "json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "transactions" in payload:
            return pd.DataFrame(payload["transactions"])
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        raise ValueError(f"Unrecognized JSON extract shape: {path}")
    raise ValueError(f"Unsupported extract suffix: {path}")


def to_canonical_columns(entity: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Rename source columns to Module 1 lake names. Leaves metadata and extras as-is."""
    mapping = source_to_canonical(entity)
    rename = {src: canon for src, canon in mapping.items() if src in frame.columns}
    return frame.rename(columns=rename)


def metadata_columns() -> tuple[str, ...]:
    return METADATA_COLUMNS
