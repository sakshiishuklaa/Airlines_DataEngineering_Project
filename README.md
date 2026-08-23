# Skyflow — Airline Flight Operations Data Platform

End-to-end data platform for airline operations: synthetic source generation, S3 data lake, PySpark/Glue ETL, data quality, PostgreSQL dimensional warehouse, Airflow orchestration, event-driven AWS processing, and Power BI analytics.

This repository is built **incrementally**. **Module 1 (this release) is the synthetic source generator and project foundation.** Later AWS/Glue/Airflow/warehouse modules are specified here but not implemented yet.

## Why this architecture

| Component | Role | Why it belongs in a production-style DE portfolio |
|-----------|------|-----------------------------------------------------|
| Faker + NumPy | Synthetic OLTP-like sources | Repeatable, relational, scale-configurable data without needing a real airline feed |
| S3 (later) | Data lake | Cheap durable landing zone; decouples producers from processors |
| Glue Catalog/Crawler (later) | Technical metadata | Schema discovery and Spark job reuse |
| PySpark / Glue (later) | Distributed ETL | Volume (100K–1M+ flights, multi-million bookings) |
| Data quality + quarantine (later) | Trust | Bad records must not silently land in facts |
| PostgreSQL star schema (later) | Serving layer | BI-friendly grain, surrogate keys, SCD2 |
| Airflow (later) | Batch orchestration | Dependencies, retries, SLAs |
| EventBridge / SQS / Lambda / SNS (later) | Event-driven + alerting | File-arrival processing and operational notifications |
| Power BI (later) | Consumption | Data marts, not raw lake tables |

## Current module

**Module 1 — Project foundation and synthetic source data generator**

Produces 10 related datasets with foreign keys, operational realism (delays, cancellations, load factor, fares), `created_at`/`updated_at` watermarks, and Hive-style raw lake folders ready for a Glue crawler later.

## Quick start (Module 1)

```bash
cd /Users/sakshishukla/Documents/Skyflow_Airlines_DE
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

python -m skyflow.generator.cli --config config/generator.yaml
```

Expected: parquet files under `data/lake/raw/<entity>/ingestion_date=<date>/` plus `_batch_manifest.json`.

See [docs/MODULE_01_GENERATOR.md](docs/MODULE_01_GENERATOR.md) for validation, troubleshooting, and interview talking points.

AWS account prep (IAM profile + S3 bucket, no uploader yet): [docs/AWS_SETUP.md](docs/AWS_SETUP.md).

## Project layout

```
config/                 # YAML + env-driven configuration
docs/                   # Architecture, source contracts, module notes
src/skyflow/            # Installable package
  generator/            # Module 1
tests/
data/lake/raw/          # Generated (gitignored)
sql/                    # Reserved for warehouse DDL (future)
dags/                   # Reserved for Airflow (future)
glue/                   # Reserved for Glue jobs (future)
infra/                  # Reserved for AWS IaC/scripts (future)
```

## Module roadmap (do not skip ahead)

1. Synthetic generator + contracts — **done in this module**
2. Local-to-S3 raw landing and lake conventions
3. Glue crawler / Data Catalog (or local catalog equivalent)
4. PySpark bronze → silver ETL
5. Data quality, quarantine, audit tables
6. PostgreSQL warehouse DDL (staging / warehouse / marts)
7. Dimensional load: surrogate keys, SCD2, incremental watermark upserts
8. Data marts
9. Airflow DAGs
10. EventBridge + SQS + Lambda + SNS
11. Power BI semantic model
12. End-to-end runbook and interview narrative

## Running architecture (living)

```
Python/Faker  →  Synthetic sources  →  local lake RAW (Module 1)
                                         ↓
                              AWS S3 RAW (Module 2+)
                                         ↓
                         Glue Crawler → Glue Data Catalog
                                         ↓
                         Glue/PySpark ETL + DQ + quarantine
                                         ↓
                         S3 PROCESSED / CURATED
                                         ↓
                         PostgreSQL DWH (star schema + marts)
                                         ↓
                         Power BI

Cross-cutting (later): Airflow, EventBridge, SQS, Lambda, SNS
```

## Dependency list (current)

- Python 3.11+
- python-dotenv, PyYAML, Faker, NumPy, pandas, pyarrow
- pytest

Future (not installed yet): pyspark, boto3, apache-airflow, psycopg2/SQLAlchemy, great-expectations.

## License

Portfolio / educational use.
