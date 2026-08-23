# Running dependency list

## Module 1 (installed)

| Package | Used for |
|---------|----------|
| python-dotenv | Load `.env` without putting secrets in code |
| PyYAML | Generator and logging config |
| Faker | Customer PII-like fields |
| NumPy | Vectorized sampling, distances, operational randomness |
| pandas | Tabular assembly |
| pyarrow | Parquet (Glue/S3-friendly) |
| pytest | Contract and integrity tests |

Python 3.11+ required.

## Planned (not installed yet)

| Package | Module |
|---------|--------|
| boto3 | S3 landing, SNS, SQS, EventBridge helpers |
| pyspark | Glue-compatible ETL locally |
| psycopg2 or SQLAlchemy | PostgreSQL warehouse loads |
| apache-airflow | Orchestration |
| great-expectations (optional) | DQ suite |

Never store AWS keys in git. Use `AWS_PROFILE` / instance roles. Console steps: [AWS_SETUP.md](AWS_SETUP.md).
