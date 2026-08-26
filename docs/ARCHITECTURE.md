# Architecture — Airline Flight Operations Data Platform

## Business objective

Ingest airline operations data, land it in a data lake, apply distributed ETL and data quality, serve a dimensional warehouse for analytics (delays, cancellations, bookings, revenue), and operate the platform with batch orchestration plus event-driven alerts.

## Why each technology

| Technology | Responsibility | Why this instead of an alternative |
|------------|----------------|------------------------------------|
| Python 3.11 + Faker + NumPy | Synthetic source system | Repeatable, relational, scale-tunable OLTP-like extracts without a real PSS/DCS feed |
| Parquet on Hive-style folders | Raw lake files | Columnar, compressed, crawler-friendly; same layout as S3 prefixes |
| AWS S3 (later) | Durable lake | Decouples producers from compute; standard for Glue/EMR/Athena |
| Glue Crawler + Data Catalog (later) | Technical metadata | Spark jobs bind to catalog tables, not brittle hardcoded schemas only |
| PySpark / Glue (later) | ETL at volume | 100K–1M+ flights and multi-million bookings |
| Great Expectations or Spark DQ (later) | Quality gates | Quarantine invalid facts before they hit finance/ops marts |
| PostgreSQL (later) | Dimensional serving | Star schema, surrogate keys, SCD2, BI connectivity |
| Airflow (later) | Batch orchestration | Ordered tasks, retries, backfills, SLAs |
| EventBridge, SQS, Lambda (later) | Event-driven landing | React to object-created events without polling-only designs |
| SNS (later) | Alerting | DQ failure / pipeline failure fan-out |
| Power BI (later) | Consumption | Reads marts, not raw |

## Source → warehouse mapping (planned; warehouse not built yet)

| Source | Warehouse target | Grain / notes |
|--------|------------------|---------------|
| airlines | dim_airline | One row per airline (Type 1 unless alliance/status history is added) |
| airports | dim_airport | One row per airport |
| aircraft | dim_aircraft | One row per tail; airline_id as FK degenerate or bridge |
| routes | dim_route | Origin/destination pair per airline |
| customers | **dim_customer SCD2** | loyalty_tier, email, phone historically tracked |
| calendar | dim_date | Role-playing dates on facts |
| flights | fact_flight | Grain: one scheduled flight occurrence (`flight_id`) |
| bookings | fact_booking | Grain: one PNR/flight seat (`booking_id`) |
| payments | fact_revenue | Grain: one payment attempt; degenerate booking_id |

`updated_at` on every source table is the **incremental watermark**. Loads will be idempotent via natural keys + surrogate keys.

## Lake layout (locked in Module 1)

```
data/lake/raw/<entity>/ingestion_date=YYYY-MM-DD/<entity>_NNN.parquet
data/lake/raw/_batch_manifest.json
```

Later: `s3://$S3_LAKE_BUCKET/raw/...` with the same prefix shape so Glue crawlers do not require a redesign.

## Source landing (locked in Module 2)

```
data/sources/<fos|pss|crm|pay|mdm>/extract_date=YYYY-MM-DD/
```

Mixed CSV / JSON / JSONL / Parquet. Canonical mapping: [SOURCE_TO_TARGET.md](SOURCE_TO_TARGET.md). Local only (no S3).

## Processing principles (later modules)

- **Batch ingestion:** generator or S3 PutObject lands a dated partition.
- **Incremental:** `WHERE updated_at > watermark` into staging, then upsert.
- **Idempotency:** rerunning the same `ingestion_date` + business keys does not duplicate facts.
- **SCD2:** dim_customer expires prior row (`is_current`, `valid_from`, `valid_to`) when loyalty/contact change.
- **Quarantine:** DQ failures written to `rejected/` with reason codes; audit table records batch counts.

## Operational sources (Module 2, local)

Five systems land extracts under `data/sources/{code}/extract_date=YYYY-MM-DD/` with mixed CSV/JSON/Parquet, source-native column names, and ingest metadata. Mapping to the locked lake: [SOURCE_TO_TARGET.md](SOURCE_TO_TARGET.md). **No S3.**

## Module boundary

**Implemented now:** Module 1 generator + contracts + local raw lake layout; Module 2 multi-system source landing.

**Not implemented:** source→lake ingest, S3 upload, Glue, Spark ETL, DQ, PostgreSQL, Airflow, Lambda/SQS/SNS, Power BI.
