"""Locked source-dataset contracts for the Airline Flight Operations platform.

Warehouse, Glue, and Airflow modules must consume these names and grains.
Do not rename columns without a documented compatibility impact.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    dtype: str
    nullable: bool
    description: str
    pk: bool = False
    fk: str | None = None


def _cols(*columns: Column) -> tuple[Column, ...]:
    return columns


AUDIT = (
    Column("created_at", "timestamp", False, "Source record creation timestamp (UTC). Used as watermark."),
    Column("updated_at", "timestamp", False, "Source record last update timestamp (UTC). Primary incremental watermark."),
)

SOURCE_SCHEMAS: dict[str, tuple[Column, ...]] = {
    "airlines": _cols(
        Column("airline_id", "int64", False, "Source primary key.", pk=True),
        Column("iata_code", "string", False, "Unique 2-letter IATA airline code."),
        Column("icao_code", "string", False, "Unique 3-letter ICAO airline code."),
        Column("airline_name", "string", False, "Legal / marketing name."),
        Column("country", "string", False, "Country of registration."),
        Column("alliance", "string", False, "Star Alliance | oneworld | SkyTeam | none."),
        Column("headquarters_city", "string", False, "HQ city."),
        Column("founded_year", "int64", False, "Year founded."),
        Column("status", "string", False, "active | inactive."),
        *AUDIT,
    ),
    "airports": _cols(
        Column("airport_id", "int64", False, "Source primary key.", pk=True),
        Column("iata_code", "string", False, "Unique 3-letter IATA airport code."),
        Column("icao_code", "string", False, "Unique 4-letter ICAO airport code."),
        Column("airport_name", "string", False, "Airport name."),
        Column("city", "string", False, "City."),
        Column("country", "string", False, "Country."),
        Column("region", "string", False, "Geographic region for marts."),
        Column("timezone", "string", False, "IANA timezone."),
        Column("latitude", "float64", False, "Latitude."),
        Column("longitude", "float64", False, "Longitude."),
        *AUDIT,
    ),
    "aircraft": _cols(
        Column("aircraft_id", "int64", False, "Source primary key.", pk=True),
        Column("airline_id", "int64", False, "Owning airline.", fk="airlines.airline_id"),
        Column("tail_number", "string", False, "Unique registration / tail number."),
        Column("manufacturer", "string", False, "Airbus | Boeing | Embraer | Bombardier."),
        Column("model", "string", False, "Aircraft family/model."),
        Column("capacity", "int64", False, "Passenger seat capacity."),
        Column("manufacture_year", "int64", False, "Year of manufacture."),
        Column("status", "string", False, "active | maintenance | retired."),
        *AUDIT,
    ),
    "routes": _cols(
        Column("route_id", "int64", False, "Source primary key.", pk=True),
        Column("airline_id", "int64", False, "Operating airline.", fk="airlines.airline_id"),
        Column("origin_airport_id", "int64", False, "Origin airport.", fk="airports.airport_id"),
        Column("dest_airport_id", "int64", False, "Destination airport.", fk="airports.airport_id"),
        Column("distance_km", "float64", False, "Great-circle distance in kilometers."),
        Column("typical_duration_min", "int64", False, "Typical block time in minutes."),
        Column("is_international", "bool", False, "True if origin and destination countries differ."),
        *AUDIT,
    ),
    "customers": _cols(
        Column("customer_id", "int64", False, "Source primary key.", pk=True),
        Column("first_name", "string", False, "Given name."),
        Column("last_name", "string", False, "Family name."),
        Column("email", "string", False, "Unique email."),
        Column("phone", "string", False, "Phone number."),
        Column("date_of_birth", "date", False, "Date of birth."),
        Column("nationality", "string", False, "ISO-style nationality country."),
        Column("loyalty_tier", "string", False, "standard | silver | gold | platinum. SCD2 candidate in warehouse."),
        *AUDIT,
    ),
    "flights": _cols(
        Column("flight_id", "int64", False, "Source primary key.", pk=True),
        Column("flight_number", "string", False, "Airline code + number, e.g. DL1042."),
        Column("airline_id", "int64", False, "Operating airline.", fk="airlines.airline_id"),
        Column("aircraft_id", "int64", False, "Assigned aircraft.", fk="aircraft.aircraft_id"),
        Column("route_id", "int64", False, "Published route.", fk="routes.route_id"),
        Column("origin_airport_id", "int64", False, "Copied from route at schedule time.", fk="airports.airport_id"),
        Column("dest_airport_id", "int64", False, "Copied from route at schedule time.", fk="airports.airport_id"),
        Column("scheduled_departure_ts", "timestamp", False, "Scheduled departure UTC."),
        Column("scheduled_arrival_ts", "timestamp", False, "Scheduled arrival UTC."),
        Column("actual_departure_ts", "timestamp", True, "Actual departure UTC; null if cancelled."),
        Column("actual_arrival_ts", "timestamp", True, "Actual arrival UTC; null if cancelled."),
        Column("status", "string", False, "arrived | delayed | cancelled | diverted."),
        Column("delay_minutes", "int64", False, "Arrival delay vs schedule; 0 if on-time or cancelled."),
        Column("cancellation_reason", "string", True, "weather | mechanical | crew | atc | security | other."),
        Column("distance_km", "float64", False, "Copied from route at schedule time."),
        *AUDIT,
    ),
    "bookings": _cols(
        Column("booking_id", "int64", False, "Source primary key.", pk=True),
        Column("booking_ref", "string", False, "Unique PNR / booking reference."),
        Column("customer_id", "int64", False, "Passenger / booking owner.", fk="customers.customer_id"),
        Column("flight_id", "int64", False, "Booked flight.", fk="flights.flight_id"),
        Column("booking_ts", "timestamp", False, "Booking timestamp; always before scheduled departure."),
        Column("cabin_class", "string", False, "economy | premium_economy | business | first."),
        Column("seat_number", "string", False, "Seat label, e.g. 12A."),
        Column("fare_amount", "float64", False, "Ticket fare in USD before payment fees."),
        Column("booking_status", "string", False, "confirmed | cancelled | checked_in | no_show | refunded."),
        *AUDIT,
    ),
    "payments": _cols(
        Column("payment_id", "int64", False, "Source primary key.", pk=True),
        Column("booking_id", "int64", False, "Paid booking.", fk="bookings.booking_id"),
        Column("payment_ts", "timestamp", False, "Payment attempt timestamp."),
        Column("amount", "float64", False, "Charged amount in currency units."),
        Column("currency", "string", False, "ISO currency; generator uses USD."),
        Column("method", "string", False, "card | wallet | miles | bank_transfer."),
        Column("status", "string", False, "captured | failed | refunded | pending."),
        Column("transaction_ref", "string", False, "Unique payment processor reference."),
        *AUDIT,
    ),
    "baggage": _cols(
        Column("baggage_id", "int64", False, "Source primary key.", pk=True),
        Column("booking_id", "int64", False, "Owning booking.", fk="bookings.booking_id"),
        Column("tag_number", "string", False, "Unique bag tag."),
        Column("piece_count", "int64", False, "Number of pieces on this tag."),
        Column("weight_kg", "float64", False, "Total weight kilograms."),
        Column("status", "string", False, "checked | loaded | claimed | delayed | lost."),
        *AUDIT,
    ),
    "customer_feedback": _cols(
        Column("feedback_id", "int64", False, "Source primary key.", pk=True),
        Column("customer_id", "int64", False, "Reviewing customer.", fk="customers.customer_id"),
        Column("flight_id", "int64", False, "Reviewed flight.", fk="flights.flight_id"),
        Column("booking_id", "int64", False, "Booking that proves the customer flew.", fk="bookings.booking_id"),
        Column("rating", "int64", False, "1-5 satisfaction rating."),
        Column("nps_score", "int64", False, "0-10 net promoter score."),
        Column("comments", "string", True, "Free-text comment; may be null."),
        Column("submitted_ts", "timestamp", False, "Submitted after scheduled/actual arrival."),
        *AUDIT,
    ),
}

ENTITY_ORDER = (
    "airlines",
    "airports",
    "aircraft",
    "routes",
    "customers",
    "flights",
    "bookings",
    "payments",
    "baggage",
    "customer_feedback",
)


def column_names(entity: str) -> list[str]:
    return [c.name for c in SOURCE_SCHEMAS[entity]]
