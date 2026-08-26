# Skyflow — Airline Flight Operations Data Platform

End-to-end data platform for airline operations: synthetic source generation, S3 data lake, PySpark/Glue ETL, data quality, PostgreSQL dimensional warehouse, Airflow orchestration, event-driven AWS processing, and Power BI analytics.

This repository is built **incrementally**. **Modules 1–2 are implemented:** synthetic generator (canonical contracts + optional local lake) and a **multi-system operational source landing zone**. AWS/Glue/Airflow/warehouse modules are specified but not implemented yet.

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

## Current modules

**Module 1 — Project foundation and synthetic source data generator**

Produces 10 related datasets with foreign keys, operational realism, watermarks, and Hive-style raw lake folders.

**Module 2 — Airline source data layer**

Same datasets emitted as five operational systems (FOS, PSS, CRM, payments, MDM) with mixed formats, folder/naming conventions, full vs incremental batches, ingest metadata, and realistic data issues. Local only — **no S3**. See [docs/MODULE_02_SOURCE_LAYER.md](docs/MODULE_02_SOURCE_LAYER.md) and [docs/SOURCE_TO_TARGET.md](docs/SOURCE_TO_TARGET.md).

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

## Quick start (Module 2)

```bash
python -m skyflow.sources.cli --config config/sources.yaml
```

Expected: mixed CSV/JSON/Parquet extracts under `data/sources/{fos,pss,crm,pay,mdm}/extract_date=2026-08-23|24|25/` plus `_run_manifest.json`.

AWS account prep (IAM profile + S3 bucket, no uploader yet): [docs/AWS_SETUP.md](docs/AWS_SETUP.md).

## Project layout

```
config/                 # YAML + env-driven configuration
docs/                   # Architecture, source contracts, module notes
src/skyflow/            # Installable package
  generator/            # Module 1
  sources/              # Module 2 operational extracts
tests/
data/lake/raw/          # Module 1 lake (gitignored)
data/sources/           # Module 2 landing (gitignored)
sql/                    # Reserved for warehouse DDL (future)
dags/                   # Reserved for Airflow (future)
glue/                   # Reserved for Glue jobs (future)
infra/                  # Reserved for AWS IaC/scripts (future)
```

## Module roadmap (do not skip ahead)

1. Synthetic generator + contracts — **done**
2. Multi-system local source landing (formats, CDC, defects) — **done**
3. Ingest source extracts → locked local lake (mapping + parse); S3 later
4. Glue crawler / Data Catalog (or local catalog equivalent)
5. PySpark bronze → silver ETL
6. Data quality, quarantine, audit tables
7. PostgreSQL warehouse DDL (staging / warehouse / marts)
8. Dimensional load: surrogate keys, SCD2, incremental watermark upserts
9. Data marts
10. Airflow DAGs
11. EventBridge + SQS + Lambda + SNS
12. Power BI semantic model
13. End-to-end runbook and interview narrative

## Running architecture (living)

```
Python/Faker  →  canonical datasets (Module 1)
                    ↓
         operational extracts data/sources (Module 2)
                    ↓
         ingest → local lake RAW (Module 3; not built)
                    ↓
              AWS S3 RAW (later)
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
