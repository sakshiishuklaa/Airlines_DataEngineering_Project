from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from skyflow.config import SCALE_PRESETS
from skyflow.generator.schemas import ENTITY_ORDER, column_names
from skyflow.sources.catalog import DATASET_BY_ENTITY, METADATA_COLUMNS, SOURCE_SYSTEMS, filename_for
from skyflow.sources.consumer import discover_extracts, read_extract, to_canonical_columns
from skyflow.sources.pipeline import run_source_layer


def _tiny_generator_cfg() -> dict:
    return {
        "project": {"name": "skyflow-test", "source_system": "skyflow_ops_v1"},
        "run": {
            "seed": 11,
            "ingestion_date": "2026-08-23",
            "output_format": "parquet",
            "output_root": "unused",
        },
        "scale": {
            **SCALE_PRESETS["demo"],
            "preset": "unit",
            "airlines": 6,
            "airports": 12,
            "aircraft": 16,
            "routes": 20,
            "customers": 40,
            "flights": 50,
            "mean_load_factor": 0.40,
            "feedback_rate": 0.35,
            "baggage_rate": 0.65,
            "payment_success_rate": 0.9,
        },
        "calendar": {"start_date": "2024-06-01", "end_date": "2024-07-15", "timezone": "UTC"},
        "operations": {
            "on_time_rate": 0.70,
            "delayed_rate": 0.20,
            "cancelled_rate": 0.10,
            "max_delay_minutes": 180,
        },
    }


def _source_cfg(tmp_path: Path, mode: str, dates: list[str], defects: bool) -> dict:
    return {
        "run": {
            "output_root": str(tmp_path / "sources"),
            "env": "PROD",
            "mode": mode,
            "apply_defects": defects,
            "extract_dates": dates,
        },
        "cdc": {"holdback_frac": 0.20, "update_frac": 0.15},
    }


@pytest.fixture(scope="module")
def window_run(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("srcwin")
    dates = ["2026-08-23", "2026-08-24", "2026-08-25"]
    manifest = run_source_layer(_source_cfg(root, "window", dates, defects=False), _tiny_generator_cfg())
    manifest["_root"] = Path(manifest["output_root"])
    return manifest


def test_five_source_systems_and_all_entities_mapped() -> None:
    assert {s.name for s in SOURCE_SYSTEMS} == {
        "flight_ops",
        "booking",
        "customer",
        "payment",
        "master_data",
    }
    assert set(DATASET_BY_ENTITY) == set(ENTITY_ORDER)


def test_window_folder_structure_and_naming(window_run: dict) -> None:
    root = window_run["_root"]
    assert (root / "_run_manifest.json").is_file()
    assert (root / "_cdc_state.json").is_file()
    for code in ("fos", "pss", "crm", "pay", "mdm"):
        for day in window_run["extract_dates"]:
            folder = root / code / f"extract_date={day}"
            assert folder.is_dir()
            assert (folder / "_extract_manifest.json").is_file()
    files = discover_extracts(root)
    assert files
    for item in files:
        parsed = filename_for(
            "PROD",
            next(s for s in SOURCE_SYSTEMS if s.code == item.source_system_code),
            DATASET_BY_ENTITY[item.entity][1],
            item.extract_date,
            item.file_name.split("_")[-1].split(".")[0],
        )
        assert item.file_name == parsed


def test_formats_match_catalog(window_run: dict) -> None:
    files = discover_extracts(window_run["_root"])
    by_entity_ext = {(f.entity, f.path.suffix.lstrip(".")) for f in files}
    expected = {
        ("flights", "parquet"),
        ("baggage", "jsonl"),
        ("bookings", "csv"),
        ("customers", "csv"),
        ("customer_feedback", "json"),
        ("payments", "json"),
        ("airlines", "csv"),
        ("airports", "csv"),
        ("aircraft", "parquet"),
        ("routes", "csv"),
    }
    assert expected.issubset(by_entity_ext)


def test_metadata_columns_present(window_run: dict) -> None:
    flights = next(f for f in discover_extracts(window_run["_root"]) if f.entity == "flights" and f.extract_date == "2026-08-23")
    frame = read_extract(flights.path)
    for col in METADATA_COLUMNS:
        assert col in frame.columns
        assert frame[col].notna().all()
    assert (frame["source_system"] == "flight_ops").all()
    assert (frame["file_name"] == flights.file_name).all()


def test_first_day_is_full_later_days_incremental(window_run: dict) -> None:
    by_date = {b["extract_date"]: b for b in window_run["batches"]}
    assert by_date["2026-08-23"]["extract_mode"] == "full"
    assert by_date["2026-08-24"]["extract_mode"] == "incremental"
    assert by_date["2026-08-25"]["extract_mode"] == "incremental"
    full_bookings = by_date["2026-08-23"]["row_counts"]["bookings"]
    d1 = by_date["2026-08-24"]["row_counts"]["bookings"]
    d2 = by_date["2026-08-25"]["row_counts"]["bookings"]
    assert full_bookings > d1
    assert full_bookings > d2
    assert d1 > 0 and d2 > 0


def test_mdm_snapshot_grows_or_holds_as_holdback_lands(window_run: dict) -> None:
    counts = {b["extract_date"]: b["row_counts"]["airports"] for b in window_run["batches"]}
    assert counts["2026-08-23"] <= counts["2026-08-24"] <= counts["2026-08-25"]
    assert counts["2026-08-25"] == window_run["generator_row_counts"]["airports"]


def test_source_to_canonical_roundtrip(window_run: dict) -> None:
    sample = next(f for f in discover_extracts(window_run["_root"]) if f.entity == "bookings")
    raw = read_extract(sample.path)
    canon = to_canonical_columns("bookings", raw)
    for name in column_names("bookings"):
        assert name in canon.columns
    for meta in METADATA_COLUMNS:
        assert meta in canon.columns


def test_payment_envelope_shape(window_run: dict) -> None:
    pay = next(f for f in discover_extracts(window_run["_root"]) if f.entity == "payments" and f.extract_date == "2026-08-23")
    payload = json.loads(pay.path.read_text(encoding="utf-8"))
    assert payload["source_system"] == "payment"
    assert "transactions" in payload
    assert payload["record_count"] == len(payload["transactions"])
    assert payload["batch_id"]
    assert payload["file_name"] == pay.file_name


def test_full_mode_emits_entire_universe(tmp_path: Path) -> None:
    dates = ["2026-08-23"]
    manifest = run_source_layer(_source_cfg(tmp_path, "full", dates, defects=False), _tiny_generator_cfg())
    gen = manifest["generator_row_counts"]
    landed = manifest["batches"][0]["row_counts"]
    for entity in ENTITY_ORDER:
        assert landed[entity] == gen[entity]


def test_defects_are_visible_when_enabled(tmp_path: Path) -> None:
    dates = ["2026-08-23"]
    manifest = run_source_layer(_source_cfg(tmp_path, "full", dates, defects=True), _tiny_generator_cfg())
    noted = manifest["batches"][0]["defects"]
    assert "flights" in noted
    assert "bookings" in noted
    assert "customers" in noted
    bookings = next(
        f for f in discover_extracts(manifest["output_root"]) if f.entity == "bookings"
    )
    frame = read_extract(bookings.path)
    pnr = frame["PNR"].astype(str)
    assert pnr.str.endswith(" ").any() or frame["PAX_ID"].eq(9_999_999).any() or frame["PNR_STATUS"].astype(str).str.contains(r"[A-Z]{3,}").any()


def test_incremental_mode_writes_requested_dates_only(tmp_path: Path) -> None:
    dates = ["2026-08-24", "2026-08-25"]
    manifest = run_source_layer(_source_cfg(tmp_path, "incremental", dates, defects=False), _tiny_generator_cfg())
    assert [b["extract_date"] for b in manifest["batches"]] == dates
    assert all(b["extract_mode"] == "incremental" for b in manifest["batches"])
    assert not (Path(manifest["output_root"]) / "fos" / "extract_date=2026-08-23").exists()
