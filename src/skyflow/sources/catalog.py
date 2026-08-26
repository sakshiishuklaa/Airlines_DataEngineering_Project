"""Operational source-system catalog for Module 2.

Canonical lake column names stay in generator.schemas. This module records how
each airline system actually emits those attributes (names, format, path).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

FormatName = Literal["csv", "json", "jsonl", "parquet"]
JsonStyle = Literal["records", "ndjson", "envelope"]
ExtractStyle = Literal["snapshot", "cdc"]

METADATA_COLUMNS: tuple[str, ...] = (
    "source_system",
    "ingestion_timestamp",
    "batch_id",
    "file_name",
)


@dataclass(frozen=True, slots=True)
class ColumnMap:
    canonical: str
    source: str
    notes: str = ""


@dataclass(frozen=True, slots=True)
class DatasetExtract:
    entity: str
    format: FormatName
    filename_token: str
    columns: tuple[ColumnMap, ...]
    json_style: JsonStyle | None = None
    bool_as_yn: tuple[str, ...] = ()
    description: str = ""


@dataclass(frozen=True, slots=True)
class SourceSystem:
    code: str
    name: str
    display_name: str
    extract_style: ExtractStyle
    landing_offset: str
    datasets: tuple[DatasetExtract, ...]
    description: str


def _m(canonical: str, source: str, notes: str = "") -> ColumnMap:
    return ColumnMap(canonical=canonical, source=source, notes=notes)


FLIGHT_OPS = SourceSystem(
    code="fos",
    name="flight_ops",
    display_name="Flight Operations System",
    extract_style="cdc",
    landing_offset="02:15:00",
    description="FOC/DCS extract: daily flight movements and baggage tags.",
    datasets=(
        DatasetExtract(
            entity="flights",
            format="parquet",
            filename_token="FLIGHTS",
            description="High-volume movement messages; columnar extract.",
            columns=(
                _m("flight_id", "FLIGHT_ID"),
                _m("flight_number", "FLIGHT_NO"),
                _m("airline_id", "CARRIER_ID"),
                _m("aircraft_id", "TAIL_AC_ID"),
                _m("route_id", "ROUTE_ID"),
                _m("origin_airport_id", "ORIG_AIRPORT_ID"),
                _m("dest_airport_id", "DEST_AIRPORT_ID"),
                _m("scheduled_departure_ts", "SKD_DEP_UTC"),
                _m("scheduled_arrival_ts", "SKD_ARR_UTC"),
                _m("actual_departure_ts", "ACT_DEP_UTC"),
                _m("actual_arrival_ts", "ACT_ARR_UTC"),
                _m("status", "FLT_STATUS"),
                _m("delay_minutes", "DELAY_MIN"),
                _m("cancellation_reason", "CXL_REASON"),
                _m("distance_km", "DIST_KM"),
                _m("created_at", "CREATED_TS"),
                _m("updated_at", "UPDATED_TS"),
            ),
        ),
        DatasetExtract(
            entity="baggage",
            format="jsonl",
            json_style="ndjson",
            filename_token="BAGGAGE",
            description="Bag-tag events as newline-delimited JSON.",
            columns=(
                _m("baggage_id", "bagId"),
                _m("booking_id", "pnrBookingId"),
                _m("tag_number", "bagTag"),
                _m("piece_count", "pieces"),
                _m("weight_kg", "weightKg"),
                _m("status", "bagStatus"),
                _m("created_at", "createdAt"),
                _m("updated_at", "updatedAt"),
            ),
        ),
    ),
)

BOOKING = SourceSystem(
    code="pss",
    name="booking",
    display_name="Booking System",
    extract_style="cdc",
    landing_offset="02:40:00",
    description="Passenger service system (PSS) PNR extract.",
    datasets=(
        DatasetExtract(
            entity="bookings",
            format="csv",
            filename_token="BOOKINGS",
            description="Classic host extract: quoted CSV, host column names.",
            columns=(
                _m("booking_id", "BOOKING_ID"),
                _m("booking_ref", "PNR"),
                _m("customer_id", "PAX_ID"),
                _m("flight_id", "FLT_ID"),
                _m("booking_ts", "BOOKED_AT"),
                _m("cabin_class", "CABIN"),
                _m("seat_number", "SEAT"),
                _m("fare_amount", "FARE_USD"),
                _m("booking_status", "PNR_STATUS"),
                _m("created_at", "CREATED_AT"),
                _m("updated_at", "UPDATED_AT"),
            ),
        ),
    ),
)

CUSTOMER = SourceSystem(
    code="crm",
    name="customer",
    display_name="Customer System",
    extract_style="cdc",
    landing_offset="01:45:00",
    description="CRM profile export plus post-trip survey dump.",
    datasets=(
        DatasetExtract(
            entity="customers",
            format="csv",
            filename_token="CUSTOMERS",
            description="CRM nightly customer file.",
            columns=(
                _m("customer_id", "CustomerID"),
                _m("first_name", "FirstName"),
                _m("last_name", "LastName"),
                _m("email", "Email"),
                _m("phone", "Phone"),
                _m("date_of_birth", "DOB"),
                _m("nationality", "Nationality"),
                _m("loyalty_tier", "LoyaltyTier"),
                _m("created_at", "CreatedAt"),
                _m("updated_at", "UpdatedAt"),
            ),
        ),
        DatasetExtract(
            entity="customer_feedback",
            format="json",
            json_style="records",
            filename_token="FEEDBACK",
            description="Survey API JSON array.",
            columns=(
                _m("feedback_id", "feedbackId"),
                _m("customer_id", "customerId"),
                _m("flight_id", "flightId"),
                _m("booking_id", "bookingId"),
                _m("rating", "rating"),
                _m("nps_score", "nps"),
                _m("comments", "comments"),
                _m("submitted_ts", "submittedAt"),
                _m("created_at", "createdAt"),
                _m("updated_at", "updatedAt"),
            ),
        ),
    ),
)

PAYMENT = SourceSystem(
    code="pay",
    name="payment",
    display_name="Payment System",
    extract_style="cdc",
    landing_offset="03:05:00",
    description="Payment processor settlement file (JSON envelope).",
    datasets=(
        DatasetExtract(
            entity="payments",
            format="json",
            json_style="envelope",
            filename_token="SETTLEMENT",
            description="Wrapped settlement batch: header + transactions[].",
            columns=(
                _m("payment_id", "paymentId"),
                _m("booking_id", "bookingId"),
                _m("payment_ts", "paidAt"),
                _m("amount", "amount"),
                _m("currency", "currency"),
                _m("method", "method"),
                _m("status", "status"),
                _m("transaction_ref", "txnRef"),
                _m("created_at", "createdAt"),
                _m("updated_at", "updatedAt"),
            ),
        ),
    ),
)

MASTER_DATA = SourceSystem(
    code="mdm",
    name="master_data",
    display_name="Airport/Aircraft Master Data System",
    extract_style="snapshot",
    landing_offset="01:10:00",
    description="Reference data hub: airlines, airports, fleet, published routes. Daily full snapshot.",
    datasets=(
        DatasetExtract(
            entity="airlines",
            format="csv",
            filename_token="AIRLINES",
            description="IATA carrier reference.",
            columns=(
                _m("airline_id", "airline_id"),
                _m("iata_code", "iata"),
                _m("icao_code", "icao"),
                _m("airline_name", "name"),
                _m("country", "country"),
                _m("alliance", "alliance"),
                _m("headquarters_city", "hq_city"),
                _m("founded_year", "founded_year"),
                _m("status", "status"),
                _m("created_at", "created_at"),
                _m("updated_at", "updated_at"),
            ),
        ),
        DatasetExtract(
            entity="airports",
            format="csv",
            filename_token="AIRPORTS",
            description="IATA/ICAO airport reference.",
            columns=(
                _m("airport_id", "airport_id"),
                _m("iata_code", "iata"),
                _m("icao_code", "icao"),
                _m("airport_name", "name"),
                _m("city", "city"),
                _m("country", "country"),
                _m("region", "region"),
                _m("timezone", "tz"),
                _m("latitude", "lat"),
                _m("longitude", "lon"),
                _m("created_at", "created_at"),
                _m("updated_at", "updated_at"),
            ),
        ),
        DatasetExtract(
            entity="aircraft",
            format="parquet",
            filename_token="AIRCRAFT",
            description="Fleet registry extract (modern MDM; parquet).",
            columns=(
                _m("aircraft_id", "aircraft_id"),
                _m("airline_id", "airline_id"),
                _m("tail_number", "tail_number"),
                _m("manufacturer", "manufacturer"),
                _m("model", "model"),
                _m("capacity", "capacity"),
                _m("manufacture_year", "manufacture_year"),
                _m("status", "status"),
                _m("created_at", "created_at"),
                _m("updated_at", "updated_at"),
            ),
        ),
        DatasetExtract(
            entity="routes",
            format="csv",
            filename_token="ROUTES",
            bool_as_yn=("is_international",),
            description="Published city-pair file; international flag as Y/N.",
            columns=(
                _m("route_id", "ROUTE_ID"),
                _m("airline_id", "AL_ID"),
                _m("origin_airport_id", "ORIG_ID"),
                _m("dest_airport_id", "DEST_ID"),
                _m("distance_km", "DIST_KM"),
                _m("typical_duration_min", "BLOCK_MIN"),
                _m("is_international", "INTL_FLAG", "Y/N in source; boolean in lake."),
                _m("created_at", "CREATED_AT"),
                _m("updated_at", "UPDATED_AT"),
            ),
        ),
    ),
)

SOURCE_SYSTEMS: tuple[SourceSystem, ...] = (
    MASTER_DATA,
    CUSTOMER,
    FLIGHT_OPS,
    BOOKING,
    PAYMENT,
)

SYSTEM_BY_CODE = {s.code: s for s in SOURCE_SYSTEMS}
SYSTEM_BY_NAME = {s.name: s for s in SOURCE_SYSTEMS}
DATASET_BY_ENTITY: dict[str, tuple[SourceSystem, DatasetExtract]] = {
    ds.entity: (sys, ds) for sys in SOURCE_SYSTEMS for ds in sys.datasets
}


def source_columns(entity: str) -> list[str]:
    _sys, dataset = DATASET_BY_ENTITY[entity]
    return [c.source for c in dataset.columns]


def canonical_to_source(entity: str) -> dict[str, str]:
    _sys, dataset = DATASET_BY_ENTITY[entity]
    return {c.canonical: c.source for c in dataset.columns}


def source_to_canonical(entity: str) -> dict[str, str]:
    return {v: k for k, v in canonical_to_source(entity).items()}


def filename_for(
    env: str,
    system: SourceSystem,
    dataset: DatasetExtract,
    extract_date: str,
    batch_token: str,
) -> str:
    ymd = extract_date.replace("-", "")
    ext = {"jsonl": "jsonl", "json": "json", "csv": "csv", "parquet": "parquet"}[dataset.format]
    return f"{env}_{system.code.upper()}_{dataset.filename_token}_{ymd}_{batch_token}.{ext}"
