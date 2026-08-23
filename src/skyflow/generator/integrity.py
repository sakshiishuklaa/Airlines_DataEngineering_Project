"""Referential integrity checks for generated source datasets."""

from __future__ import annotations

import pandas as pd


class IntegrityError(AssertionError):
    pass


def _assert(cond: bool, message: str) -> None:
    if not cond:
        raise IntegrityError(message)


def validate_frames(frames: dict[str, pd.DataFrame]) -> None:
    required = {
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
    }
    missing = required - set(frames)
    _assert(not missing, f"Missing datasets: {sorted(missing)}")

    airlines, airports, aircraft, routes, customers, flights, bookings, payments, baggage, feedback = (
        frames["airlines"],
        frames["airports"],
        frames["aircraft"],
        frames["routes"],
        frames["customers"],
        frames["flights"],
        frames["bookings"],
        frames["payments"],
        frames["baggage"],
        frames["customer_feedback"],
    )

    _assert(airlines["airline_id"].is_unique, "airlines.airline_id must be unique")
    _assert(airlines["iata_code"].is_unique, "airlines.iata_code must be unique")
    _assert(airports["airport_id"].is_unique, "airports.airport_id must be unique")
    _assert(aircraft["aircraft_id"].is_unique, "aircraft.aircraft_id must be unique")
    _assert(aircraft["tail_number"].is_unique, "aircraft.tail_number must be unique")
    _assert(routes["route_id"].is_unique, "routes.route_id must be unique")
    _assert(customers["customer_id"].is_unique, "customers.customer_id must be unique")
    _assert(customers["email"].is_unique, "customers.email must be unique")
    _assert(flights["flight_id"].is_unique, "flights.flight_id must be unique")
    _assert(bookings["booking_id"].is_unique, "bookings.booking_id must be unique")
    _assert(bookings["booking_ref"].is_unique, "bookings.booking_ref must be unique")
    _assert(payments["payment_id"].is_unique, "payments.payment_id must be unique")
    _assert(set(aircraft["airline_id"]).issubset(set(airlines["airline_id"])), "aircraft.airline_id FK failed")
    _assert(set(routes["airline_id"]).issubset(set(airlines["airline_id"])), "routes.airline_id FK failed")
    _assert(set(routes["origin_airport_id"]).issubset(set(airports["airport_id"])), "routes.origin FK failed")
    _assert(set(routes["dest_airport_id"]).issubset(set(airports["airport_id"])), "routes.dest FK failed")
    _assert((routes["origin_airport_id"] != routes["dest_airport_id"]).all(), "route origin must differ from dest")
    _assert(set(flights["airline_id"]).issubset(set(airlines["airline_id"])), "flights.airline_id FK failed")
    _assert(set(flights["aircraft_id"]).issubset(set(aircraft["aircraft_id"])), "flights.aircraft_id FK failed")
    _assert(set(flights["route_id"]).issubset(set(routes["route_id"])), "flights.route_id FK failed")
    _assert(set(bookings["customer_id"]).issubset(set(customers["customer_id"])), "bookings.customer_id FK failed")
    _assert(set(bookings["flight_id"]).issubset(set(flights["flight_id"])), "bookings.flight_id FK failed")
    _assert(set(payments["booking_id"]).issubset(set(bookings["booking_id"])), "payments.booking_id FK failed")
    if not baggage.empty:
        _assert(set(baggage["booking_id"]).issubset(set(bookings["booking_id"])), "baggage.booking_id FK failed")
    if not feedback.empty:
        _assert(set(feedback["booking_id"]).issubset(set(bookings["booking_id"])), "feedback.booking_id FK failed")
        _assert(set(feedback["flight_id"]).issubset(set(flights["flight_id"])), "feedback.flight_id FK failed")
        _assert(set(feedback["customer_id"]).issubset(set(customers["customer_id"])), "feedback.customer_id FK failed")

    cancelled = flights["status"].eq("cancelled")
    _assert(flights.loc[cancelled, "actual_departure_ts"].isna().all(), "cancelled flights must have null actual_departure_ts")
    _assert((flights["scheduled_arrival_ts"] > flights["scheduled_departure_ts"]).all(), "arrival must be after departure")
    _assert((bookings["booking_ts"] < bookings["flight_id"].map(flights.set_index("flight_id")["scheduled_departure_ts"])).all(), "booking_ts must precede scheduled departure")

    # Aircraft used on a flight must belong to the flight's airline
    ac_owner = aircraft.set_index("aircraft_id")["airline_id"]
    _assert((flights["aircraft_id"].map(ac_owner) == flights["airline_id"]).all(), "flight aircraft must belong to flight airline")
    route_owner = routes.set_index("route_id")["airline_id"]
    _assert((flights["route_id"].map(route_owner) == flights["airline_id"]).all(), "flight route must belong to flight airline")
