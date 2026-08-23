# AWS setup (console)

Do this **before Module 2**. This document does **not** implement the S3 uploader; it only tells you what to create and which `.env` keys to fill.

**Rule:** never put access keys in git. Use an IAM **profile** (`~/.aws/credentials`). `.env` only stores region, profile name, bucket, and prefixes.

**Cost:** Module 2 needs only S3 + IAM (pennies). Do **not** create Glue jobs or RDS until those modules. Stop unused RDS when you are done.

---

## Architecture reminder

```
Laptop generator → S3 raw  →  Glue (later) → S3 processed/curated
                                              ↓ JDBC / staging load
                                         PostgreSQL warehouse
                                              ↓
                                           Power BI
```

Glue **can** write to PostgreSQL **only if** the database is reachable from AWS (typically **RDS** in a VPC). Glue **cannot** use `POSTGRES_HOST=localhost` on your laptop.

Recommended path for this project:

1. Glue transforms **S3 → S3** (processed/curated).
2. A later job loads **staging** in PostgreSQL.
3. SQL does MERGE / SCD2 into `warehouse`.

---

## A. Now (Module 2) — IAM profile + S3 bucket

### A1. IAM user (or SSO role)

1. AWS Console → **IAM** → **Users** → **Create user**.
2. User name: `skyflow-dev` (or similar).
3. **Provide user access to the AWS Management Console**: optional (off is fine if you only use CLI).
4. Next → **Attach policies directly** → create an **inline policy** (least privilege) with S3 access to **one bucket** (create the bucket in A2 first, then come back and tighten the resource ARN). For a first pass you can attach `AmazonS3FullAccess` on a throwaway account, then restrict.

Minimum actions for Module 2:

- `s3:ListBucket` on `arn:aws:s3:::YOUR_BUCKET`
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `arn:aws:s3:::YOUR_BUCKET/*`

5. Create user → **Security credentials** → **Create access key** → use case **Command Line Interface (CLI)** → create.
6. Copy **Access key ID** and **Secret access key** once. They go into the **local AWS CLI**, not into the repo.

On your machine:

```bash
aws configure --profile skyflow
# AWS Access Key ID:     <paste>
# AWS Secret Access Key: <paste>
# Default region name:   us-east-1
# Default output format: json
```

Confirm:

```bash
aws sts get-caller-identity --profile skyflow
```

You should see an account ID and `skyflow-dev`.

Then in `.env`:

```
AWS_REGION=us-east-1
AWS_PROFILE=skyflow
```

Do **not** set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` in `.env` unless you have a strong reason. The profile is enough.

### A2. S3 lake bucket

1. Console → **S3** → **Create bucket**.
2. **Bucket name:** globally unique, e.g. `skyflow-airlines-lake-<your-initials>-<yyyymmdd>`.
3. **AWS Region:** same as `AWS_REGION` (example: `us-east-1`).
4. **Object Ownership:** ACLs disabled (recommended).
5. **Block Public Access:** leave **all four** blocks **on**.
6. **Bucket Versioning:** optional Off for a portfolio.
7. **Default encryption:** SSE-S3 (Amazon S3 managed keys).
8. Create bucket.

You do **not** need to create folders in the console. Keys like `raw/flights/ingestion_date=2026-08-23/...` appear when Module 2 uploads.

Optional empty prefixes (not required):

- `raw/`
- `processed/`
- `curated/`
- `rejected/`

Then in `.env`:

```
S3_LAKE_BUCKET=skyflow-airlines-lake-yourname-20260823
S3_RAW_PREFIX=raw
```

Smoke-check from CLI (replace names):

```bash
aws s3 ls s3://$S3_LAKE_BUCKET --profile skyflow --region us-east-1
```

Empty output with no error means the bucket exists and the profile can list it.

### A3. Copy env file

```bash
cp .env.example .env
```

Fill `AWS_REGION`, `AWS_PROFILE`, `S3_LAKE_BUCKET`. Leave `POSTGRES_*` as localhost until the warehouse module.

---

## B. Later — Glue (do not create yet)

When the Glue module starts:

1. **IAM** → **Roles** → **Create role** → trusted entity **AWS service** → **Glue**.
2. Attach: `AWSGlueServiceRole` plus S3 read/write on `YOUR_BUCKET` (and later Secrets Manager if you store the DB password there).
3. **AWS Glue** → **Data Catalog** → **Databases** → **Add database**, e.g. `skyflow_catalog`.
4. **Crawlers** → **Create crawler** → source `s3://YOUR_BUCKET/raw/` → IAM role from step 2 → target database `skyflow_catalog`.
5. **ETL jobs** → Glue 4.0 / Spark, same role, read catalog or `s3://.../raw`, write `s3://.../processed` and `s3://.../curated`.

`.env` keys to add then: `GLUE_DATABASE`, `GLUE_ROLE_ARN`, `S3_PROCESSED_PREFIX`, `S3_CURATED_PREFIX`, `S3_REJECTED_PREFIX`.

---

## C. Later — PostgreSQL warehouse (do not create RDS yet)

**Option 1 (recommended first):** Docker/local Postgres. Keep:

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=skyflow_dw
POSTGRES_USER=skyflow
POSTGRES_PASSWORD=...
POSTGRES_SSLMODE=prefer
```

Load from your laptop (Airflow/Python). Glue in AWS **cannot** use this host.

**Option 2 (Glue JDBC):** **RDS** → **Create database** → **PostgreSQL**.

Typical console choices for a cheap lab:

1. Engine: PostgreSQL 16.
2. Templates: **Free tier** if eligible, else `db.t3.micro` / `db.t4g.micro`.
3. DB instance identifier: `skyflow-dw`.
4. Master username: `skyflow` (match `.env`).
5. **VPC:** default is fine for a lab; **do not** make it public unless you understand the risk. Prefer **not publicly accessible**.
6. Security group: inbound **5432** from the Glue connection’s security group (not `0.0.0.0/0`).
7. Create database. Copy **endpoint** (hostname) into `POSTGRES_HOST`.
8. Set `POSTGRES_SSLMODE=require`.

Glue connection (same later module):

1. **Glue** → **Data connections** → **Create connection** → **JDBC**.
2. Type: PostgreSQL.
3. JDBC URL: `jdbc:postgresql://YOUR_RDS_ENDPOINT:5432/skyflow_dw`.
4. Store user/password in **AWS Secrets Manager** (preferred) or the connection form.
5. Network: same VPC/subnets/SG as RDS so Glue elastic network interfaces can reach port 5432.
6. **Test connection**.

`.env` then: `POSTGRES_HOST=<rds-endpoint>`, `GLUE_CONNECTION_NAME=skyflow-postgres`, optional `POSTGRES_SECRET_ARN=...`.

---

## D. Later — events and alerts (do not create yet)

| Console | What |
|---------|------|
| **Amazon EventBridge** | Rule on S3 `Object Created` for `raw/` |
| **Amazon SQS** | Queue for landing events |
| **AWS Lambda** | Consumer (validate / trigger Glue) |
| **Amazon SNS** | Topic for DQ / job failure emails |

---

## E. `.env` key map

| Key | Fill when | Example |
|-----|-----------|---------|
| `AWS_REGION` | Module 2 | `us-east-1` |
| `AWS_PROFILE` | Module 2 | `skyflow` |
| `S3_LAKE_BUCKET` | Module 2 | `skyflow-airlines-lake-...` |
| `S3_RAW_PREFIX` | Module 2 | `raw` |
| `S3_PROCESSED_PREFIX` | Glue ETL | `processed` |
| `S3_CURATED_PREFIX` | Glue ETL | `curated` |
| `S3_REJECTED_PREFIX` | DQ | `rejected` |
| `GLUE_DATABASE` | Catalog | `skyflow_catalog` |
| `GLUE_ROLE_ARN` | Glue jobs | `arn:aws:iam::123:role/SkyflowGlueRole` |
| `GLUE_CONNECTION_NAME` | Glue → RDS | `skyflow-postgres` |
| `POSTGRES_*` | Warehouse | localhost first; RDS endpoint if Glue JDBC |
| `POSTGRES_SECRET_ARN` | optional | Secrets Manager ARN |

Copy from `.env.example`. Never commit `.env`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|----------------|-----|
| `InvalidClientTokenId` / `ExpiredToken` | Wrong or rotated keys | Recreate access key; `aws configure --profile skyflow` |
| `AccessDenied` on `s3 ls` | Policy or wrong bucket/region | Match region; grant ListBucket on bucket ARN |
| `BucketAlreadyExists` | Name taken globally | Add a unique suffix |
| Glue job cannot reach Postgres | SG / VPC / `localhost` | Use RDS + Glue connection; not laptop Postgres |
| CLI uses the wrong account | Default profile, not `skyflow` | Always `--profile skyflow` or `AWS_PROFILE=skyflow` |

When Module 2 starts, the uploader will read these same keys and mirror `data/lake/raw/` → `s3://$S3_LAKE_BUCKET/$S3_RAW_PREFIX/`.
