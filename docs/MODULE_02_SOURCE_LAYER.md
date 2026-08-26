# Module 2 — Airline Source Data Layer

## What this module is

A **local operational landing zone** that treats Module 1 output as *canonical airline data*, then emits it the way five source systems actually would: different folders, filenames, formats, load styles, and messy extracts.

This is **not** the data lake. Module 1 still owns `data/lake/raw/{entity}/ingestion_date=...`. Module 3 will **read these source files** and land cleaned, canonical parquet into that lake. **S3 is not implemented.**

## Source architecture

```
Module 1 generator (in-memory)
        │
        ▼
 ┌──────────────────────────────────────────────────────────┐
 │  Module 2 source landing   data/sources/   (local only)  │
 │                                                          │
 │  mdm/   Airport/Aircraft Master Data   snapshot CSV/PQ   │
 │  crm/   Customer System                CDC CSV + JSON    │
 │  fos/   Flight Operations System       CDC parquet+jsonl │
 │  pss/   Booking System (PSS)           CDC CSV           │
 │  pay/   Payment System                 CDC JSON envelope │
 └──────────────────────────────────────────────────────────┘
        │
        │  Module 3 (not built): discover → parse → map → DQ
        ▼
 data/lake/raw/{entity}/ingestion_date=YYYY-MM-DD/   (locked Module 1 layout)
```

| Source system | Code | Datasets | Format | Load style |
|---------------|------|----------|--------|------------|
| Airport/Aircraft Master Data System | `mdm` | airlines, airports, aircraft, routes | CSV, CSV, Parquet, CSV | Daily **full snapshot** (as-of extract date) |
| Customer System | `crm` | customers, customer_feedback | CSV, JSON array | **CDC** (new + changed) |
| Flight Operations System | `fos` | flights, baggage | Parquet, NDJSON | **CDC** |
| Booking System | `pss` | bookings | CSV | **CDC** |
| Payment System | `pay` | payments | JSON envelope (`transactions[]`) | **CDC** |

Systems land at staggered UTC times (MDM 01:10, CRM 01:45, FOS 02:15, PSS 02:40, PAY 03:05) so later ingestion can talk about source SLAs and late files.

Column names in extracts are **source-native**. Canonical lake names stay in `src/skyflow/generator/schemas.py`. Mapping: [SOURCE_TO_TARGET.md](SOURCE_TO_TARGET.md).

## Folder structure

```
data/sources/
  _run_manifest.json
  _cdc_state.json
  mdm/extract_date=2026-08-23/
    PROD_MDM_AIRLINES_20260823_<TOKEN>.csv
    PROD_MDM_AIRPORTS_20260823_<TOKEN>.csv
    PROD_MDM_AIRCRAFT_20260823_<TOKEN>.parquet
    PROD_MDM_ROUTES_20260823_<TOKEN>.csv
    _extract_manifest.json
  crm/extract_date=2026-08-23/
    PROD_CRM_CUSTOMERS_20260823_<TOKEN>.csv
    PROD_CRM_FEEDBACK_20260823_<TOKEN>.json
    _extract_manifest.json
  fos/extract_date=2026-08-23/
    PROD_FOS_FLIGHTS_20260823_<TOKEN>.parquet
    PROD_FOS_BAGGAGE_20260823_<TOKEN>.jsonl
    _extract_manifest.json
  pss/extract_date=2026-08-23/
    PROD_PSS_BOOKINGS_20260823_<TOKEN>.csv
    _extract_manifest.json
  pay/extract_date=2026-08-23/
    PROD_PAY_SETTLEMENT_20260823_<TOKEN>.json
    _extract_manifest.json
  … same tree for 2026-08-24 and 2026-08-25
```

Hive-style `extract_date=` is the **source arrival date**, not the Module 1 lake `ingestion_date=` partition. Module 3 may set lake `ingestion_date` equal to `extract_date` or to the pipeline run date; that is a Module 3 decision.

## File naming convention

```
{ENV}_{SYSTEM}_{ENTITY}_{YYYYMMDD}_{BATCHTOKEN}.{ext}
```

Example: `PROD_FOS_FLIGHTS_20260823_A1B2C3D4.parquet`

| Token | Meaning |
|-------|---------|
| ENV | `PROD` (configurable) |
| SYSTEM | `FOS` `PSS` `CRM` `PAY` `MDM` |
| ENTITY | `FLIGHTS`, `BOOKINGS`, `SETTLEMENT`, … |
| YYYYMMDD | extract date |
| BATCHTOKEN | 8 hex chars; **shared by all files for that extract date** |

## Ingestion metadata

Every extract row includes:

- `source_system` — catalog name (`flight_ops`, `booking`, …)
- `ingestion_timestamp` — UTC timestamp when that system’s file is imagined to land
- `batch_id` — `SKY{YYYYMMDD}-{TOKEN}` (same across systems for one day)
- `file_name` — the file the row was written into

Payment JSON **also** repeats these fields on the envelope header.

## Batch strategy

| Mode | CLI | Behavior |
|------|-----|----------|
| `window` (default) | `--mode window --extract-dates 2026-08-23,2026-08-24,2026-08-25` | Day 1 **full** CDC universe (minus holdback). Later days **incremental** inserts + updates. MDM is a snapshot as of each day. |
| `full` | `--mode full --extract-date 2026-08-23` | One-shot initial load: every generated row in every system. |
| `incremental` | `--mode incremental --extract-dates 2026-08-24,2026-08-25` | Only CDC/snapshot slices for those dates (no day-1 full dump). |

CDC simulation (window):

- `holdback_frac` (default 0.12): rows first appear on a later extract date (late-arriving / new production data).
- `update_frac` (default 0.08): day-1 rows re-emitted later with a mutated business field (customer `loyalty_tier`, flight delay, booking status).

Watermarks `created_at` / `updated_at` from Module 1 are **preserved on the extracts** (under source names). They remain the planned warehouse incremental keys. Extract `extract_date` is the file-arrival grain.

## Implementation

| Path | Role |
|------|------|
| `config/sources.yaml` | Dates, mode, CDC fractions, output root |
| `src/skyflow/sources/catalog.py` | Systems, formats, names, column maps |
| `src/skyflow/sources/collector.py` | In-memory capture of Module 1 frames |
| `src/skyflow/sources/cdc.py` | Full vs incremental slicing |
| `src/skyflow/sources/defects.py` | Realistic source issues |
| `src/skyflow/sources/writers.py` | CSV / JSON / JSONL / Parquet + metadata |
| `src/skyflow/sources/pipeline.py` | Orchestration |
| `src/skyflow/sources/consumer.py` | Discovery/read API for Module 3 |
| `src/skyflow/sources/cli.py` | CLI |

Module 1 CLI and lake writer are unchanged.

## How to run

```bash
cd /Users/sakshishukla/Documents/Skyflow_Airlines_DE
source .venv/bin/activate
pip install -e .

# 3-day window: full 2026-08-23, incremental 24 and 25 (demo generator scale)
python -m skyflow.sources.cli --config config/sources.yaml

# Initial load only, clean extracts
python -m skyflow.sources.cli --mode full --extract-date 2026-08-23 --no-defects

# Incremental days only
python -m skyflow.sources.cli --mode incremental --extract-dates 2026-08-24,2026-08-25
```

## Expected output

- `_run_manifest.json` with per-day `extract_mode`, `row_counts`, `batch_id`, `defects`
- `_cdc_state.json` with `last_extract_date`
- Per-system `_extract_manifest.json`
- Data files matching the tree above

## Sample files

Tiny committed examples (not a full batch): `docs/samples/source_layer/`.

Inspect a real run:

```bash
python - <<'PY'
from pathlib import Path
from skyflow.sources.consumer import discover_extracts, read_extract, to_canonical_columns

root = Path("data/sources")
files = discover_extracts(root)
print(len(files), "extract files")
for f in files[:8]:
    print(f.extract_date, f.source_system_code, f.entity, f.file_name)
bookings = next(x for x in files if x.entity == "bookings" and x.extract_date == "2026-08-23")
raw = read_extract(bookings.path)
print("source columns", list(raw.columns)[:8])
print(to_canonical_columns("bookings", raw)[["booking_id", "booking_ref", "source_system", "batch_id"]].head())
PY
```

## Validation

```bash
pytest
```

Contract checks in `tests/test_sources.py`: system mapping, folder + filename convention, formats, metadata, window full vs incremental, MDM snapshot, source-to-canonical rename, payment envelope, full-load completeness, defects enabled, incremental-only dates.

## Common errors

| Symptom | Cause | Fix |
|---------|-------|-----|
| Empty `data/sources` | Command not run / wrong cwd | Run from repo root |
| `Config file not found` | Missing YAML | `--config config/sources.yaml` |
| Huge PSS CSV | Demo scale bookings are large | Use test volumes or a smaller generator override |
| Incremental day has 0 bookings | `holdback_frac=0` and single full date only | Use `window` with 2+ dates |
| Expecting lake parquet | Module 2 does not write `data/lake/raw` | Module 1 CLI still does that; Module 3 will ingest sources → lake |

## Interview talking points

- Sources are **heterogeneous** (CSV vs JSON vs Parquet, host names vs camelCase vs envelope JSON), which is why a source-to-target map exists before Glue.
- **Snapshot vs CDC** is a property of the system, not of the warehouse.
- File-level **batch_id / source_system / file_name** is lineage you can carry into audit tables.
- Defects are **intentional** (orphan PAX_ID, `$` fares, mixed status case, trailing IATA spaces, undeclared JSON fields) so DQ/quarantine in a later module has something real to catch.
- Lake layout from Module 1 is unchanged: this module is the **producer contract** Module 3 will consume.

## Exact next step (Module 3)

**Ingest source extracts into the locked local lake** (still no AWS unless that module explicitly adds S3):

1. `discover_extracts(data/sources)`
2. Parse by format (`csv` / `parquet` / `jsonl` / JSON array / payment envelope)
3. Apply [SOURCE_TO_TARGET.md](SOURCE_TO_TARGET.md) (trim, type casts, Y/N → bool, strip `$`)
4. Keep or persist metadata into an ingest audit table/manifest
5. Write canonical columns only to `data/lake/raw/{entity}/ingestion_date={extract_date}/`
6. Quarantine rows that fail FK/null/type checks — that can wait for the DQ module if you keep Module 3 to landing + mapping only
