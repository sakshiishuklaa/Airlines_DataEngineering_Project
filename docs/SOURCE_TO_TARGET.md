# Source-to-target mapping (Module 2 → lake)

**Source:** local operational extracts under `data/sources/{system}/extract_date=YYYY-MM-DD/`  
**Target:** locked Module 1 lake `data/lake/raw/{entity}/ingestion_date=YYYY-MM-DD/` plus canonical columns in `src/skyflow/generator/schemas.py`.

Module 3 should map **source column → canonical column**, apply the listed transforms, drop undeclared extras (or send them to a residual/quarantine path), and **not** store source-native names in the lake.

Lineage fields (`source_system`, `ingestion_timestamp`, `batch_id`, `file_name`) are **not** lake business columns. Persist them on an ingest audit/manifest, or as optional lake metadata columns if a later module adds them without renaming the locked 10-entity contracts.

## Systems → datasets → lake entities

| Source system | Extract file token | Format | Lake entity | Load |
|---------------|-------------------|--------|-------------|------|
| master_data (`mdm`) | AIRLINES | CSV | `airlines` | snapshot |
| master_data | AIRPORTS | CSV | `airports` | snapshot |
| master_data | AIRCRAFT | Parquet | `aircraft` | snapshot |
| master_data | ROUTES | CSV | `routes` | snapshot |
| customer (`crm`) | CUSTOMERS | CSV | `customers` | CDC |
| customer | FEEDBACK | JSON array | `customer_feedback` | CDC |
| flight_ops (`fos`) | FLIGHTS | Parquet | `flights` | CDC |
| flight_ops | BAGGAGE | JSONL | `baggage` | CDC |
| booking (`pss`) | BOOKINGS | CSV | `bookings` | CDC |
| payment (`pay`) | SETTLEMENT | JSON envelope | `payments` | CDC |

Warehouse (later, unchanged plan): airlines→dim_airline, airports→dim_airport, aircraft→dim_aircraft, routes→dim_route, customers→dim_customer SCD2, flights→fact_flight, bookings→fact_booking, payments→fact_revenue. Baggage and feedback remain source/lake entities until a later mart is defined.

## Column maps

Transforms are what Module 3 must implement. Module 2 only *produces* the source shape.

### airlines (MDM CSV)

| Source | Target | Transform |
|--------|--------|-----------|
| airline_id | airline_id | int |
| iata | iata_code | trim |
| icao | icao_code | trim |
| name | airline_name | trim |
| country | country | as-is |
| alliance | alliance | as-is |
| hq_city | headquarters_city | as-is |
| founded_year | founded_year | int |
| status | status | lower |
| created_at | created_at | timestamp UTC |
| updated_at | updated_at | timestamp UTC |

Defects: trailing space on `iata`.

### airports (MDM CSV)

| Source | Target | Transform |
|--------|--------|-----------|
| airport_id | airport_id | int |
| iata | iata_code | trim |
| icao | icao_code | trim |
| name | airport_name | as-is |
| city | city | as-is |
| country | country | as-is |
| region | region | as-is |
| tz | timezone | as-is |
| lat | latitude | float |
| lon | longitude | float |
| created_at / updated_at | created_at / updated_at | timestamp UTC |

Defects: trailing space on `iata`.

### aircraft (MDM Parquet)

Source names already match the lake (`aircraft_id`, `airline_id`, `tail_number`, `manufacturer`, `model`, `capacity`, `manufacture_year`, `status`, `created_at`, `updated_at`). Passthrough + types.

### routes (MDM CSV)

| Source | Target | Transform |
|--------|--------|-----------|
| ROUTE_ID | route_id | int |
| AL_ID | airline_id | int |
| ORIG_ID | origin_airport_id | int |
| DEST_ID | dest_airport_id | int |
| DIST_KM | distance_km | float |
| BLOCK_MIN | typical_duration_min | int |
| INTL_FLAG | is_international | `Y`→true, `N`→false; blank → quarantine |
| CREATED_AT / UPDATED_AT | created_at / updated_at | timestamp UTC |

### customers (CRM CSV)

| Source | Target | Transform |
|--------|--------|-----------|
| CustomerID | customer_id | int |
| FirstName | first_name | trim |
| LastName | last_name | trim |
| Email | email | lower; nulls → quarantine (unique in contract) |
| Phone | phone | strip dashes/spaces |
| DOB | date_of_birth | parse ISO **or** `MM/DD/YYYY` |
| Nationality | nationality | as-is |
| LoyaltyTier | loyalty_tier | lower |
| CreatedAt / UpdatedAt | created_at / updated_at | timestamp UTC |

Defects: null email, duplicate email, dashed phone, US date format.

### customer_feedback (CRM JSON array)

| Source | Target | Transform |
|--------|--------|-----------|
| feedbackId | feedback_id | int |
| customerId | customer_id | int |
| flightId | flight_id | int |
| bookingId | booking_id | int |
| rating | rating | int 1–5 |
| nps | nps_score | int 0–10 |
| comments | comments | as-is (nullable) |
| submittedAt | submitted_ts | timestamp UTC |
| createdAt / updatedAt | created_at / updated_at | timestamp UTC |

### flights (FOS Parquet)

| Source | Target | Transform |
|--------|--------|-----------|
| FLIGHT_ID | flight_id | int |
| FLIGHT_NO | flight_number | as-is |
| CARRIER_ID | airline_id | int |
| TAIL_AC_ID | aircraft_id | int |
| ROUTE_ID | route_id | int |
| ORIG_AIRPORT_ID | origin_airport_id | int |
| DEST_AIRPORT_ID | dest_airport_id | int |
| SKD_DEP_UTC | scheduled_departure_ts | timestamp UTC |
| SKD_ARR_UTC | scheduled_arrival_ts | timestamp UTC |
| ACT_DEP_UTC | actual_departure_ts | timestamp UTC, nullable |
| ACT_ARR_UTC | actual_arrival_ts | timestamp UTC, nullable |
| FLT_STATUS | status | lower; domain arrived\|delayed\|cancelled |
| DELAY_MIN | delay_minutes | int |
| CXL_REASON | cancellation_reason | lower, nullable |
| DIST_KM | distance_km | float |
| CREATED_TS / UPDATED_TS | created_at / updated_at | timestamp UTC |

Defects: `DELAYED` with `DELAY_MIN=0`; mixed-case status; arrived with null `ACT_DEP_UTC`.

### baggage (FOS JSONL)

| Source | Target | Transform |
|--------|--------|-----------|
| bagId | baggage_id | int |
| pnrBookingId | booking_id | int |
| bagTag | tag_number | as-is |
| pieces | piece_count | int; null → quarantine |
| weightKg | weight_kg | float |
| bagStatus | status | lower |
| createdAt / updatedAt | created_at / updated_at | timestamp UTC |

Extra field `stationCode` is **not** in the lake contract. Drop or land in a residual column store in a later DQ module.

### bookings (PSS CSV)

| Source | Target | Transform |
|--------|--------|-----------|
| BOOKING_ID | booking_id | int |
| PNR | booking_ref | trim |
| PAX_ID | customer_id | int; `9999999` is an intentional orphan |
| FLT_ID | flight_id | int |
| BOOKED_AT | booking_ts | timestamp UTC |
| CABIN | cabin_class | lower |
| SEAT | seat_number | as-is |
| FARE_USD | fare_amount | strip leading `$`, float |
| PNR_STATUS | booking_status | lower |
| CREATED_AT / UPDATED_AT | created_at / updated_at | timestamp UTC |

Defects: trailing PNR space, mixed-case status, `$` fares, orphan PAX_ID, duplicate rows (same PNR twice).

### payments (PAY JSON envelope)

File shape:

```json
{
  "source_system": "payment",
  "ingestion_timestamp": "...",
  "batch_id": "...",
  "file_name": "...",
  "record_count": 0,
  "transactions": [ { "...": "..." } ]
}
```

Read **`transactions`**, not the root object as a table.

| Source | Target | Transform |
|--------|--------|-----------|
| paymentId | payment_id | int |
| bookingId | booking_id | int |
| paidAt | payment_ts | timestamp UTC |
| amount | amount | float (may arrive as string) |
| currency | currency | upper |
| method | method | lower |
| status | status | lower |
| txnRef | transaction_ref | as-is; null → quarantine |
| createdAt / updatedAt | created_at / updated_at | timestamp UTC |

## How Module 3 should consume files

Use `skyflow.sources.consumer`:

```python
from skyflow.sources.consumer import discover_extracts, read_extract, to_canonical_columns

for item in discover_extracts("data/sources"):
    raw = read_extract(item.path)           # source names + metadata
    mapped = to_canonical_columns(item.entity, raw)  # rename only
    # then: type/transform/DQ from this document
    # then: LakeWriter.write_frame(item.entity, business_columns_only)
```

Suggested processing order per `extract_date`: **mdm → crm customers → fos flights → pss bookings → pay → fos baggage → crm feedback** (FK order).

Idempotency: replacing the same `ingestion_date` lake partition after a rerun of the same source `extract_date` + `batch_id` should be safe. Deduplicate bookings on `booking_id` / `booking_ref` because source files can contain duplicate rows.

Do **not** upload to S3 in Module 3 unless that module’s spec explicitly adds it; this mapping is valid on the local filesystem first.
