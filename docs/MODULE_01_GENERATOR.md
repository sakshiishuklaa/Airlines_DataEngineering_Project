# Module 1 — Synthetic source generator

## What this module is

A production-style, config-driven generator that emits **10 related airline datasets** into a **Hive-partitioned local raw lake**. This is the source system for every later module (S3, Glue, warehouse).

## How to run

```bash
cd /Users/sakshishukla/Documents/Skyflow_Airlines_DE
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
cp .env.example .env

# Demo scale (~2.5K flights; bookings derived from aircraft capacity × load factor)
python -m skyflow.generator.cli --config config/generator.yaml

# Interview scale (100K flights)
python -m skyflow.generator.cli --config config/generator.yaml --preset interview

# Custom volume
python -m skyflow.generator.cli --flights 100000 --customers 80000 --seed 42

# CSV instead of parquet
python -m skyflow.generator.cli --format csv --output-root data/lake/raw
```

Without an editable install, run with `PYTHONPATH=src`.

## Expected output

```
data/lake/raw/_batch_manifest.json
data/lake/raw/airlines/ingestion_date=<today>/airlines_000.parquet
data/lake/raw/airports/ingestion_date=<today>/airports_000.parquet
data/lake/raw/aircraft/ingestion_date=<today>/aircraft_000.parquet
data/lake/raw/routes/ingestion_date=<today>/routes_000.parquet
data/lake/raw/customers/ingestion_date=<today>/customers_000.parquet
data/lake/raw/flights/ingestion_date=<today>/flights_000.parquet
data/lake/raw/bookings/ingestion_date=<today>/bookings_000.parquet
data/lake/raw/payments/ingestion_date=<today>/payments_000.parquet
data/lake/raw/baggage/ingestion_date=<today>/baggage_000.parquet
data/lake/raw/customer_feedback/ingestion_date=<today>/customer_feedback_000.parquet
```

Logs include row counts. Manifest `row_counts` is the audit record for the batch.

Demo-scale order of magnitude (seed 42, approximate):

- airlines 18, airports 60, aircraft 140, routes 200, customers 4,000, flights 2,500
- bookings ~150K–250K (capacity × load factor)
- payments = bookings
- baggage < bookings
- feedback ≪ bookings

## Validation

```bash
pytest
```

Manual checks:

```bash
python - <<'PY'
import json
from pathlib import Path
import pandas as pd

m = json.loads(Path("data/lake/raw/_batch_manifest.json").read_text())
print(m["row_counts"])
date = m["ingestion_date"]
flights = pd.read_parquet(next((Path("data/lake/raw") / "flights" / f"ingestion_date={date}").glob("*.parquet")))
print(flights["status"].value_counts())
print(flights[["scheduled_departure_ts", "actual_departure_ts", "status"]].head())
PY
```

## Common errors

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ModuleNotFoundError: skyflow` | Package not on `PYTHONPATH` | `pip install -e .` or `PYTHONPATH=src` |
| Config file not found | Wrong working directory | Run from repo root |
| Unknown `--preset` | Typo | Use `demo`, `interview`, `large`, `xl` |
| Route generation produced 0 rows | Too few airports | Raise `airports` (practically ≥10) |
| Slow `xl` / 1M flights | Millions of bookings on a laptop | Use `--preset interview` first |
| Parquet engine errors | pyarrow not installed | `pip install -r requirements.txt` |

## Interview talking points

- Relational synthetic data (FKs, hub-and-spoke-ish routes via sampling, fares from distance, delays affecting NPS) vs independently random tables
- Lake-ready Hive partitions and a batch manifest (lineage/audit)
- Watermark columns designed before warehouse work (`updated_at`)
- SCD2 candidate (`customers.loyalty_tier`) chosen up front without implementing SCD yet
- Scale presets and chunked booking writes so 100K+ flights is a config change, not a rewrite

AWS console steps and `.env` keys: [AWS_SETUP.md](AWS_SETUP.md).

## Exact next step (done in Module 2)

Multi-system local source landing (`data/sources/…`). See [MODULE_02_SOURCE_LAYER.md](MODULE_02_SOURCE_LAYER.md). S3 is **not** part of Module 2.
