"""Lake writers: Hive-style raw partitions, parquet/csv, batch manifest."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from skyflow.generator.schemas import SOURCE_SCHEMAS, column_names

LOGGER = logging.getLogger(__name__)

_DTYPE_MAP = {
    "int64": "Int64",
    "string": "string",
    "float64": "float64",
    "timestamp": "datetime64[us, UTC]",
    "date": "object",
    "bool": "boolean",
}


def _align_schema(entity: str, frame: pd.DataFrame) -> pd.DataFrame:
    expected = column_names(entity)
    missing = [c for c in expected if c not in frame.columns]
    extra = [c for c in frame.columns if c not in expected]
    if missing or extra:
        raise ValueError(f"{entity} schema mismatch. missing={missing} extra={extra}")
    ordered = frame.loc[:, expected].copy()
    if ordered.empty:
        ordered = pd.DataFrame(
            {col.name: pd.Series(dtype=_DTYPE_MAP[col.dtype]) for col in SOURCE_SCHEMAS[entity]}
        )
    return ordered


class LakeWriter:
    def __init__(self, output_root: str | Path, ingestion_date: str, output_format: str = "parquet") -> None:
        if output_format not in {"parquet", "csv"}:
            raise ValueError("output_format must be parquet or csv")
        self.output_root = Path(output_root)
        self.ingestion_date = ingestion_date
        self.output_format = output_format
        self.written: dict[str, list[str]] = {}

    def entity_dir(self, entity: str) -> Path:
        path = self.output_root / entity / f"ingestion_date={self.ingestion_date}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_frame(self, entity: str, frame: pd.DataFrame, part: int = 0) -> Path:
        ordered = _align_schema(entity, frame)
        directory = self.entity_dir(entity)
        suffix = "parquet" if self.output_format == "parquet" else "csv"
        target = directory / f"{entity}_{part:03d}.{suffix}"
        if self.output_format == "parquet":
            ordered.to_parquet(target, index=False, engine="pyarrow")
        else:
            ordered.to_csv(target, index=False)
        self.written.setdefault(entity, []).append(str(target))
        LOGGER.info("Wrote %s rows=%s path=%s", entity, f"{len(ordered):,}", target)
        return target

    def write_manifest(self, payload: dict[str, Any]) -> Path:
        self.output_root.mkdir(parents=True, exist_ok=True)
        path = self.output_root / "_batch_manifest.json"
        document = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "ingestion_date": self.ingestion_date,
            "output_format": self.output_format,
            "files": self.written,
            **payload,
        }
        path.write_text(json.dumps(document, indent=2, default=str), encoding="utf-8")
        LOGGER.info("Wrote batch manifest %s", path)
        return path
