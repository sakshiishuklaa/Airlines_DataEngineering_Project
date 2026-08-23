from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from skyflow.config import SCALE_PRESETS
from skyflow.generator.engine import FlightOpsGenerator
from skyflow.generator.integrity import validate_frames
from skyflow.generator.schemas import ENTITY_ORDER, SOURCE_SCHEMAS, column_names
from skyflow.generator.writers import LakeWriter


def _tiny_config(tmp_path: Path) -> dict:
    return {
        "project": {"name": "skyflow-test", "source_system": "skyflow_ops_v1"},
        "run": {
            "seed": 7,
            "ingestion_date": "2026-08-23",
            "output_format": "parquet",
            "output_root": str(tmp_path / "raw"),
        },
        "scale": {
            **SCALE_PRESETS["demo"],
            "preset": "unit",
            "airlines": 6,
            "airports": 15,
            "aircraft": 24,
            "routes": 30,
            "customers": 50,
            "flights": 80,
            "mean_load_factor": 0.45,
            "feedback_rate": 0.4,
            "baggage_rate": 0.7,
            "payment_success_rate": 0.9,
        },
        "calendar": {"start_date": "2024-06-01", "end_date": "2024-08-31", "timezone": "UTC"},
        "operations": {
            "on_time_rate": 0.70,
            "delayed_rate": 0.20,
            "cancelled_rate": 0.10,
            "max_delay_minutes": 180,
        },
    }


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> dict[str, pd.DataFrame]:
    tmp_path = tmp_path_factory.mktemp("lake")
    cfg = _tiny_config(tmp_path)
    generator = FlightOpsGenerator(cfg)
    writer = LakeWriter(
        output_root=cfg["run"]["output_root"],
        ingestion_date=cfg["run"]["ingestion_date"],
        output_format="parquet",
    )
    generator.generate(writer)
    frames: dict[str, pd.DataFrame] = {}
    root = Path(cfg["run"]["output_root"])
    for entity in ENTITY_ORDER:
        files = sorted((root / entity / "ingestion_date=2026-08-23").glob("*.parquet"))
        frames[entity] = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    frames["_root"] = root  # type: ignore[assignment]
    return frames


def test_schema_columns_match_contract(generated: dict[str, pd.DataFrame]) -> None:
    for entity in ENTITY_ORDER:
        assert list(generated[entity].columns) == column_names(entity)
        assert {c.name for c in SOURCE_SCHEMAS[entity]} == set(generated[entity].columns)


def test_referential_integrity(generated: dict[str, pd.DataFrame]) -> None:
    payload = {k: v for k, v in generated.items() if k != "_root"}
    validate_frames(payload)


def test_cancelled_flights_have_null_actuals(generated: dict[str, pd.DataFrame]) -> None:
    flights = generated["flights"]
    cancelled = flights["status"].eq("cancelled")
    assert cancelled.any()
    assert flights.loc[cancelled, "actual_departure_ts"].isna().all()
    assert flights.loc[cancelled, "cancellation_reason"].notna().all()


def test_booking_volume_respects_capacity(generated: dict[str, pd.DataFrame]) -> None:
    cap = generated["aircraft"].set_index("aircraft_id")["capacity"]
    flights = generated["flights"].copy()
    flights["capacity"] = flights["aircraft_id"].map(cap)
    counts = generated["bookings"].groupby("flight_id").size()
    joined = flights.set_index("flight_id")["capacity"]
    assert (counts <= joined.reindex(counts.index)).all()


def test_feedback_requires_real_booking(generated: dict[str, pd.DataFrame]) -> None:
    fb = generated["customer_feedback"]
    assert not fb.empty
    keys = set(
        zip(
            generated["bookings"]["customer_id"],
            generated["bookings"]["flight_id"],
            generated["bookings"]["booking_id"],
        )
    )
    got = set(zip(fb["customer_id"], fb["flight_id"], fb["booking_id"]))
    assert got.issubset(keys)


def test_manifest_and_partition_layout(generated: dict[str, pd.DataFrame]) -> None:
    root = generated["_root"]
    assert (root / "_batch_manifest.json").is_file()
    for entity in ENTITY_ORDER:
        assert (root / entity / "ingestion_date=2026-08-23").is_dir()


def test_watermarks_are_ordered(generated: dict[str, pd.DataFrame]) -> None:
    for entity in ENTITY_ORDER:
        frame = generated[entity]
        created = pd.to_datetime(frame["created_at"], utc=True)
        updated = pd.to_datetime(frame["updated_at"], utc=True)
        assert (updated >= created).all()
