"""Relational synthetic data engine.

Entities are generated in FK order. Bookings and child events are derived from
flights (capacity × load factor), not sampled independently.
"""

from __future__ import annotations

import logging
import string
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

from skyflow.generator import reference_data as ref
from skyflow.generator.schemas import ENTITY_ORDER, column_names
from skyflow.generator.writers import LakeWriter

LOGGER = logging.getLogger(__name__)

SEAT_LETTERS = np.array(list("ABCDEF"))
REF_ALPHABET = string.ascii_uppercase + string.digits
FLIGHT_CHUNK = 8_000


def _haversine_km(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    radius = 6371.0
    p1, p2 = np.radians(lat1), np.radians(lat2)
    d_phi = np.radians(lat2 - lat1)
    d_lambda = np.radians(lon2 - lon1)
    a = np.sin(d_phi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(d_lambda / 2) ** 2
    return radius * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


def _duration_minutes(distance_km: np.ndarray) -> np.ndarray:
    block = 30.0 + (distance_km / 780.0) * 60.0
    return np.maximum(45, np.rint(block).astype(np.int64))


def _ids_to_refs(ids: np.ndarray) -> np.ndarray:
    """Unique 6-char base36 booking references from integer ids."""
    alphabet = REF_ALPHABET
    out = []
    for raw in ids:
        n = int(raw)
        chars = []
        for _ in range(6):
            n, rem = divmod(n, 36)
            chars.append(alphabet[rem])
        out.append("".join(reversed(chars)))
    return np.array(out)


class FlightOpsGenerator:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.scale = config["scale"]
        self.run = config["run"]
        self.calendar = config["calendar"]
        self.operations = config["operations"]
        self.seed = int(self.run["seed"])
        self.rng = np.random.default_rng(self.seed)
        self.faker = Faker()
        self.faker.seed_instance(self.seed)
        self.now = pd.Timestamp.now(tz="UTC")

        ingestion = self.run.get("ingestion_date")
        self.ingestion_date = ingestion or datetime.now(timezone.utc).date().isoformat()
        self.start_date = date.fromisoformat(str(self.calendar["start_date"]))
        self.end_date = date.fromisoformat(str(self.calendar["end_date"]))
        if self.end_date <= self.start_date:
            raise ValueError("calendar.end_date must be after start_date")

    def generate(self, writer: LakeWriter) -> dict[str, int]:
        LOGGER.info(
            "Starting generation preset=%s flights=%s seed=%s ingestion_date=%s",
            self.scale.get("preset"),
            f"{int(self.scale['flights']):,}",
            self.seed,
            self.ingestion_date,
        )
        airlines = self._airlines()
        airports = self._airports()
        aircraft = self._aircraft(airlines)
        routes = self._routes(airlines, airports)
        customers = self._customers()
        flights = self._flights(airlines, aircraft, routes)

        for name, frame in [
            ("airlines", airlines),
            ("airports", airports),
            ("aircraft", aircraft),
            ("routes", routes),
            ("customers", customers),
            ("flights", flights),
        ]:
            writer.write_frame(name, frame, part=0)

        counts = {
            "airlines": len(airlines),
            "airports": len(airports),
            "aircraft": len(aircraft),
            "routes": len(routes),
            "customers": len(customers),
            "flights": len(flights),
            "bookings": 0,
            "payments": 0,
            "baggage": 0,
            "customer_feedback": 0,
        }

        booking_id = 1
        payment_id = 1
        baggage_id = 1
        feedback_id = 1
        part = 0
        n_flights = len(flights)
        for start in range(0, n_flights, FLIGHT_CHUNK):
            chunk = flights.iloc[start : start + FLIGHT_CHUNK]
            bookings = self._bookings(chunk, aircraft, customers, routes, id_start=booking_id)
            payments = self._payments(bookings, id_start=payment_id)
            baggage = self._baggage(bookings, id_start=baggage_id)
            feedback = self._feedback(bookings, chunk, id_start=feedback_id)

            writer.write_frame("bookings", bookings, part=part)
            writer.write_frame("payments", payments, part=part)
            writer.write_frame("baggage", baggage, part=part)
            writer.write_frame("customer_feedback", feedback, part=part)

            counts["bookings"] += len(bookings)
            counts["payments"] += len(payments)
            counts["baggage"] += len(baggage)
            counts["customer_feedback"] += len(feedback)
            booking_id += len(bookings)
            payment_id += len(payments)
            baggage_id += len(baggage)
            feedback_id += len(feedback)
            part += 1
            LOGGER.info(
                "Chunk %s/%s flights %s-%s bookings_chunk=%s",
                part,
                int(np.ceil(n_flights / FLIGHT_CHUNK)),
                start + 1,
                start + len(chunk),
                f"{len(bookings):,}",
            )

        writer.write_manifest(
            {
                "seed": self.seed,
                "preset": self.scale.get("preset"),
                "source_system": self.config.get("project", {}).get("source_system", "skyflow_ops_v1"),
                "row_counts": counts,
                "calendar": {"start_date": self.start_date.isoformat(), "end_date": self.end_date.isoformat()},
            }
        )
        LOGGER.info("Generation complete: %s", counts)
        for entity in ENTITY_ORDER:
            if entity not in counts:
                raise RuntimeError(f"Missing count for {entity}")
        return counts

    def _audit_from_base(self, n: int, base: pd.Series | None = None, lag_frac: float = 0.0) -> tuple[pd.Series, pd.Series]:
        if base is None:
            offsets = self.rng.integers(30, 800, size=n)
            created = self.now - pd.to_timedelta(offsets, unit="D")
        else:
            created = pd.to_datetime(base, utc=True)
        lag = np.zeros(n, dtype=np.int64)
        if lag_frac > 0:
            mask = self.rng.random(n) < lag_frac
            lag[mask] = self.rng.integers(1, 400, size=int(mask.sum()))
        updated = created + pd.to_timedelta(lag, unit="D")
        updated = updated.where(updated <= self.now, self.now)
        return created, updated

    def _airlines(self) -> pd.DataFrame:
        n = min(int(self.scale["airlines"]), len(ref.AIRLINES))
        specs = ref.AIRLINES[:n]
        created, updated = self._audit_from_base(n)
        status = np.where(self.rng.random(n) < 0.03, "inactive", "active")
        frame = pd.DataFrame(
            {
                "airline_id": np.arange(1, n + 1, dtype=np.int64),
                "iata_code": [s[0] for s in specs],
                "icao_code": [s[1] for s in specs],
                "airline_name": [s[2] for s in specs],
                "country": [s[3] for s in specs],
                "alliance": [s[4] for s in specs],
                "headquarters_city": [s[5] for s in specs],
                "founded_year": [s[6] for s in specs],
                "status": status,
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated airlines=%s", len(frame))
        return frame.loc[:, column_names("airlines")]

    def _airports(self) -> pd.DataFrame:
        n = min(int(self.scale["airports"]), len(ref.AIRPORTS))
        specs = ref.AIRPORTS[:n]
        created, updated = self._audit_from_base(n)
        frame = pd.DataFrame(
            {
                "airport_id": np.arange(1, n + 1, dtype=np.int64),
                "iata_code": [s[0] for s in specs],
                "icao_code": [s[1] for s in specs],
                "airport_name": [s[2] for s in specs],
                "city": [s[3] for s in specs],
                "country": [s[4] for s in specs],
                "region": [s[5] for s in specs],
                "timezone": [s[6] for s in specs],
                "latitude": [s[7] for s in specs],
                "longitude": [s[8] for s in specs],
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated airports=%s", len(frame))
        return frame.loc[:, column_names("airports")]

    def _aircraft(self, airlines: pd.DataFrame) -> pd.DataFrame:
        n = int(self.scale["aircraft"])
        active = airlines.loc[airlines["status"].eq("active"), "airline_id"].to_numpy()
        if len(active) == 0:
            active = airlines["airline_id"].to_numpy()
        repeats = int(np.ceil(n / len(active)))
        airline_ids = np.tile(active, repeats)[:n]
        self.rng.shuffle(airline_ids)
        model_idx = self.rng.integers(0, len(ref.AIRCRAFT_MODELS), size=n)
        models = [ref.AIRCRAFT_MODELS[int(i)] for i in model_idx]
        created, updated = self._audit_from_base(n)
        tails = np.array([f"N{10000 + i:05d}{REF_ALPHABET[i % 36]}" for i in range(n)])
        frame = pd.DataFrame(
            {
                "aircraft_id": np.arange(1, n + 1, dtype=np.int64),
                "airline_id": airline_ids.astype(np.int64),
                "tail_number": tails,
                "manufacturer": [m[0] for m in models],
                "model": [m[1] for m in models],
                "capacity": np.array([m[2] for m in models], dtype=np.int64),
                "manufacture_year": self.rng.integers(2005, 2025, size=n, dtype=np.int64),
                "status": self.rng.choice(
                    np.array(["active", "maintenance", "retired"]),
                    size=n,
                    p=np.array([0.90, 0.07, 0.03]),
                ),
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated aircraft=%s", len(frame))
        return frame.loc[:, column_names("aircraft")]

    def _routes(self, airlines: pd.DataFrame, airports: pd.DataFrame) -> pd.DataFrame:
        n_target = int(self.scale["routes"])
        airport_ids = airports["airport_id"].to_numpy()
        lat = airports.set_index("airport_id")["latitude"].to_dict()
        lon = airports.set_index("airport_id")["longitude"].to_dict()
        country = airports.set_index("airport_id")["country"].to_dict()
        active_airlines = airlines.loc[airlines["status"].eq("active")]
        if active_airlines.empty:
            active_airlines = airlines
        airline_ids = active_airlines["airline_id"].to_numpy()

        origin = []
        dest = []
        carrier = []
        seen: set[tuple[int, int, int]] = set()
        guard = 0
        while len(origin) < n_target and guard < n_target * 40:
            guard += 1
            aid = int(self.rng.choice(airline_ids))
            pair = self.rng.choice(airport_ids, size=2, replace=False)
            o, d = int(pair[0]), int(pair[1])
            key = (aid, o, d)
            if key in seen:
                continue
            dist = float(_haversine_km(np.array([lat[o]]), np.array([lon[o]]), np.array([lat[d]]), np.array([lon[d]]))[0])
            if dist < 150:
                continue
            seen.add(key)
            origin.append(o)
            dest.append(d)
            carrier.append(aid)

        if not origin:
            raise RuntimeError("Route generation produced 0 rows. Increase airport count.")

        o_arr = np.array(origin, dtype=np.int64)
        d_arr = np.array(dest, dtype=np.int64)
        dist = _haversine_km(
            np.array([lat[i] for i in o_arr]),
            np.array([lon[i] for i in o_arr]),
            np.array([lat[i] for i in d_arr]),
            np.array([lon[i] for i in d_arr]),
        )
        created, updated = self._audit_from_base(len(o_arr))
        intl = np.array([country[int(o)] != country[int(d)] for o, d in zip(o_arr, d_arr)])
        frame = pd.DataFrame(
            {
                "route_id": np.arange(1, len(o_arr) + 1, dtype=np.int64),
                "airline_id": np.array(carrier, dtype=np.int64),
                "origin_airport_id": o_arr,
                "dest_airport_id": d_arr,
                "distance_km": np.round(dist, 1),
                "typical_duration_min": _duration_minutes(dist),
                "is_international": intl,
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated routes=%s (target=%s)", len(frame), n_target)
        return frame.loc[:, column_names("routes")]

    def _customers(self) -> pd.DataFrame:
        n = int(self.scale["customers"])
        first = [self.faker.first_name() for _ in range(n)]
        last = [self.faker.last_name() for _ in range(n)]
        created, updated = self._audit_from_base(n, lag_frac=0.12)
        frame = pd.DataFrame(
            {
                "customer_id": np.arange(1, n + 1, dtype=np.int64),
                "first_name": first,
                "last_name": last,
                "email": [f"{first[i].lower()}.{last[i].lower()}.{i + 1}@skyflow-mail.test" for i in range(n)],
                "phone": [self.faker.msisdn()[:15] for _ in range(n)],
                "date_of_birth": [self.faker.date_of_birth(minimum_age=12, maximum_age=88) for _ in range(n)],
                "nationality": [self.faker.country() for _ in range(n)],
                "loyalty_tier": self.rng.choice(np.array(ref.LOYALTY_TIERS), size=n, p=np.array(ref.LOYALTY_WEIGHTS)),
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated customers=%s", len(frame))
        return frame.loc[:, column_names("customers")]

    def _flights(self, airlines: pd.DataFrame, aircraft: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
        n = int(self.scale["flights"])
        on_time = float(self.operations["on_time_rate"])
        delayed = float(self.operations["delayed_rate"])
        cancelled = float(self.operations["cancelled_rate"])
        probs = np.array([on_time, delayed, cancelled], dtype=float)
        probs = probs / probs.sum()

        fleet = aircraft.loc[aircraft["status"].ne("retired")].copy()
        if fleet.empty:
            fleet = aircraft.copy()
        by_airline = {int(aid): grp["aircraft_id"].to_numpy() for aid, grp in fleet.groupby("airline_id")}
        usable_routes = routes[routes["airline_id"].isin(by_airline.keys())].reset_index(drop=True)
        if usable_routes.empty:
            raise RuntimeError("No routes with an available fleet. Increase aircraft count.")

        route_idx = self.rng.integers(0, len(usable_routes), size=n)
        picked = usable_routes.iloc[route_idx].reset_index(drop=True)
        airline_ids = picked["airline_id"].to_numpy(dtype=np.int64)

        aircraft_ids = np.empty(n, dtype=np.int64)
        for aid, ac_ids in by_airline.items():
            mask = airline_ids == aid
            k = int(mask.sum())
            if k:
                aircraft_ids[mask] = self.rng.choice(ac_ids, size=k, replace=True)

        iata = airlines.set_index("airline_id")["iata_code"]
        numbers = self.rng.integers(100, 8999, size=n)
        flight_number = [f"{iata.loc[int(aid)]}{int(num)}" for aid, num in zip(airline_ids, numbers)]

        span_days = (self.end_date - self.start_date).days + 1
        day_offset = self.rng.integers(0, span_days, size=n)
        hour = self.rng.choice(
            np.array([6, 7, 8, 9, 11, 13, 15, 17, 18, 19, 21]),
            size=n,
            p=np.array([0.08, 0.10, 0.12, 0.10, 0.07, 0.07, 0.08, 0.12, 0.12, 0.09, 0.05]),
        )
        minute = self.rng.choice(np.array([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]), size=n)
        sched_dep = (
            pd.Timestamp(self.start_date, tz="UTC")
            + pd.to_timedelta(day_offset, unit="D")
            + pd.to_timedelta(hour, unit="h")
            + pd.to_timedelta(minute, unit="m")
        )
        duration = np.maximum(40, picked["typical_duration_min"].to_numpy(dtype=np.int64) + self.rng.integers(-8, 12, size=n))
        sched_arr = sched_dep + pd.to_timedelta(duration, unit="m")

        outcome = self.rng.choice(np.array(["arrived", "delayed", "cancelled"]), size=n, p=probs)
        max_delay = int(self.operations.get("max_delay_minutes", 240))
        delay = np.zeros(n, dtype=np.int64)
        delayed_mask = outcome == "delayed"
        delay[delayed_mask] = self.rng.integers(16, max_delay + 1, size=int(delayed_mask.sum()))
        ontime_mask = outcome == "arrived"
        jitter = np.zeros(n, dtype=np.int64)
        jitter[ontime_mask] = self.rng.integers(-8, 15, size=int(ontime_mask.sum()))
        delay[ontime_mask] = np.maximum(0, jitter[ontime_mask])
        late_ontime = ontime_mask & (delay >= 15)
        outcome = outcome.copy()
        outcome[late_ontime] = "delayed"

        cancelled_mask = outcome == "cancelled"
        actual_dep = sched_dep + pd.to_timedelta(np.maximum(delay - 3, 0), unit="m")
        actual_arr = sched_arr + pd.to_timedelta(delay + self.rng.integers(0, 8, size=n), unit="m")
        actual_dep = actual_dep.where(~cancelled_mask, pd.NaT)
        actual_arr = actual_arr.where(~cancelled_mask, pd.NaT)
        delay = np.where(cancelled_mask, 0, delay)

        reasons = np.array([None] * n, dtype=object)
        if cancelled_mask.any():
            reasons[cancelled_mask] = self.rng.choice(np.array(ref.CANCELLATION_REASONS), size=int(cancelled_mask.sum()))

        created = sched_dep - pd.to_timedelta(90, unit="D")
        updated = pd.Series([self.now] * n)
        frame = pd.DataFrame(
            {
                "flight_id": np.arange(1, n + 1, dtype=np.int64),
                "flight_number": flight_number,
                "airline_id": airline_ids,
                "aircraft_id": aircraft_ids,
                "route_id": picked["route_id"].to_numpy(dtype=np.int64),
                "origin_airport_id": picked["origin_airport_id"].to_numpy(dtype=np.int64),
                "dest_airport_id": picked["dest_airport_id"].to_numpy(dtype=np.int64),
                "scheduled_departure_ts": sched_dep,
                "scheduled_arrival_ts": sched_arr,
                "actual_departure_ts": actual_dep,
                "actual_arrival_ts": actual_arr,
                "status": outcome,
                "delay_minutes": delay,
                "cancellation_reason": reasons,
                "distance_km": picked["distance_km"].to_numpy(dtype=np.float64),
                "created_at": created,
                "updated_at": updated,
            }
        )
        LOGGER.info("Generated flights=%s", len(frame))
        return frame.loc[:, column_names("flights")]

    def _bookings(
        self,
        flights: pd.DataFrame,
        aircraft: pd.DataFrame,
        customers: pd.DataFrame,
        routes: pd.DataFrame,
        id_start: int,
    ) -> pd.DataFrame:
        mean_lf = float(self.scale["mean_load_factor"])
        cap_map = aircraft.set_index("aircraft_id")["capacity"]
        capacities = flights["aircraft_id"].map(cap_map).to_numpy(dtype=np.int64)
        load = np.clip(self.rng.normal(mean_lf, 0.08, size=len(flights)), 0.35, 0.98)
        n_per = np.minimum(capacities, np.maximum(1, np.rint(capacities * load).astype(np.int64)))
        total = int(n_per.sum())
        if total == 0:
            return pd.DataFrame(columns=column_names("bookings"))

        flight_ids = np.repeat(flights["flight_id"].to_numpy(dtype=np.int64), n_per)
        seat_ord = np.concatenate([np.arange(k, dtype=np.int64) for k in n_per])
        booking_ids = np.arange(id_start, id_start + total, dtype=np.int64)
        customer_ids = self.rng.integers(1, len(customers) + 1, size=total, dtype=np.int64)

        loc = pd.Series(np.arange(len(flights)), index=flights["flight_id"].to_numpy())
        take = loc.loc[flight_ids].to_numpy()

        sched = pd.to_datetime(flights["scheduled_departure_ts"].to_numpy()[take], utc=True)
        status_f = flights["status"].to_numpy()[take]
        route_ids = flights["route_id"].to_numpy()[take]
        dist_map = routes.set_index("route_id")["distance_km"]
        distance = pd.Series(route_ids).map(dist_map).to_numpy(dtype=np.float64)

        lead_days = self.rng.integers(1, 120, size=total)
        lead_hours = self.rng.integers(0, 20, size=total)
        booking_ts = sched - pd.to_timedelta(lead_days, unit="D") - pd.to_timedelta(lead_hours, unit="h")

        draw = self.rng.random(total)
        cabins = np.where(draw < 0.02, "first", np.where(draw < 0.12, "business", np.where(draw < 0.28, "premium_economy", "economy")))
        mult = np.select(
            [cabins == "first", cabins == "business", cabins == "premium_economy"],
            [5.2, 3.1, 1.55],
            default=1.0,
        )
        fare = np.round(np.maximum(49.0, 0.11 * distance * mult * self.rng.uniform(0.82, 1.35, size=total)), 2)

        cancelled_flight = status_f == "cancelled"
        other = self.rng.choice(
            np.array(["confirmed", "checked_in", "no_show", "cancelled"]),
            size=total,
            p=np.array([0.62, 0.30, 0.05, 0.03]),
        )
        refund_or_cancel = self.rng.choice(np.array(["cancelled", "refunded"]), size=total, p=np.array([0.35, 0.65]))
        booking_status = np.where(cancelled_flight, refund_or_cancel, other)

        row_num = seat_ord // 6 + 1
        letter = SEAT_LETTERS[seat_ord % 6]
        seats = np.char.add(row_num.astype(str), letter.astype(str))

        created = booking_ts
        updated = booking_ts + pd.to_timedelta(self.rng.integers(0, 10, size=total), unit="D")
        frame = pd.DataFrame(
            {
                "booking_id": booking_ids,
                "booking_ref": _ids_to_refs(booking_ids),
                "customer_id": customer_ids,
                "flight_id": flight_ids,
                "booking_ts": booking_ts,
                "cabin_class": cabins,
                "seat_number": seats,
                "fare_amount": fare,
                "booking_status": booking_status,
                "created_at": created,
                "updated_at": updated,
            }
        )
        return frame.loc[:, column_names("bookings")]

    def _payments(self, bookings: pd.DataFrame, id_start: int) -> pd.DataFrame:
        n = len(bookings)
        if n == 0:
            return pd.DataFrame(columns=column_names("payments"))
        success = float(self.scale["payment_success_rate"])
        booking_ts = pd.to_datetime(bookings["booking_ts"], utc=True)
        payment_ts = booking_ts + pd.to_timedelta(self.rng.integers(1, 90, size=n), unit="m")
        amount = np.round(bookings["fare_amount"].to_numpy(dtype=np.float64) * self.rng.uniform(0.98, 1.06, size=n), 2)
        rand = self.rng.random(n)
        status = np.where(
            bookings["booking_status"].to_numpy() == "refunded",
            "refunded",
            np.where(rand < success, "captured", np.where(rand < success + 0.04, "pending", "failed")),
        )
        ids = np.arange(id_start, id_start + n, dtype=np.int64)
        frame = pd.DataFrame(
            {
                "payment_id": ids,
                "booking_id": bookings["booking_id"].to_numpy(dtype=np.int64),
                "payment_ts": payment_ts,
                "amount": amount,
                "currency": "USD",
                "method": self.rng.choice(np.array(ref.PAYMENT_METHODS), size=n, p=np.array([0.72, 0.14, 0.09, 0.05])),
                "status": status,
                "transaction_ref": np.array([f"PAY{int(i):012d}" for i in ids]),
                "created_at": payment_ts,
                "updated_at": payment_ts,
            }
        )
        return frame.loc[:, column_names("payments")]

    def _baggage(self, bookings: pd.DataFrame, id_start: int) -> pd.DataFrame:
        if bookings.empty:
            return pd.DataFrame(columns=column_names("baggage"))
        rate = float(self.scale["baggage_rate"])
        eligible = bookings["booking_status"].isin(["confirmed", "checked_in", "no_show"]).to_numpy()
        take = eligible & (self.rng.random(len(bookings)) < rate)
        subset = bookings.loc[take]
        n = len(subset)
        if n == 0:
            return pd.DataFrame(columns=column_names("baggage"))
        ids = np.arange(id_start, id_start + n, dtype=np.int64)
        created = pd.to_datetime(subset["booking_ts"], utc=True)
        frame = pd.DataFrame(
            {
                "baggage_id": ids,
                "booking_id": subset["booking_id"].to_numpy(dtype=np.int64),
                "tag_number": np.array([f"BAG{int(i):010d}" for i in ids]),
                "piece_count": self.rng.integers(1, 4, size=n, dtype=np.int64),
                "weight_kg": np.round(self.rng.uniform(8, 32, size=n), 1),
                "status": self.rng.choice(
                    np.array(["checked", "loaded", "claimed", "delayed", "lost"]),
                    size=n,
                    p=np.array([0.18, 0.22, 0.52, 0.06, 0.02]),
                ),
                "created_at": created,
                "updated_at": created + pd.to_timedelta(self.rng.integers(0, 20, size=n), unit="D"),
            }
        )
        return frame.loc[:, column_names("baggage")]

    def _feedback(self, bookings: pd.DataFrame, flights: pd.DataFrame, id_start: int) -> pd.DataFrame:
        empty = pd.DataFrame(columns=column_names("customer_feedback"))
        if bookings.empty:
            return empty
        rate = float(self.scale["feedback_rate"])
        merged = bookings.merge(
            flights[["flight_id", "status", "scheduled_arrival_ts", "actual_arrival_ts", "delay_minutes"]],
            on="flight_id",
            how="inner",
        )
        eligible = merged["booking_status"].isin(["confirmed", "checked_in"]) & merged["status"].ne("cancelled")
        pool = merged.loc[eligible]
        if pool.empty:
            return empty
        n_take = max(1, int(round(len(pool) * rate))) if rate > 0 else 0
        n_take = min(n_take, len(pool))
        if n_take == 0:
            return empty
        take = pool.sample(n=n_take, random_state=int(self.seed + id_start))
        n = len(take)
        delay = take["delay_minutes"].to_numpy(dtype=np.float64)
        base = 4.2 - np.minimum(delay, 180) / 90.0
        rating = np.clip(np.rint(base + self.rng.normal(0, 0.6, size=n)), 1, 5).astype(np.int64)
        nps = np.clip(np.rint((rating - 1) * 2.5 + self.rng.normal(0, 1.2, size=n)), 0, 10).astype(np.int64)
        arr = pd.to_datetime(take["actual_arrival_ts"].fillna(take["scheduled_arrival_ts"]), utc=True)
        submitted = arr + pd.to_timedelta(self.rng.integers(2, 72, size=n), unit="h")
        comments_pool = np.array(
            [
                "Smooth flight and on-time arrival.",
                "Crew was helpful but boarding was slow.",
                "Seat comfort could be better on this sector.",
                "Delay was poorly communicated.",
                "Great service in business cabin.",
                None,
                None,
            ],
            dtype=object,
        )
        ids = np.arange(id_start, id_start + n, dtype=np.int64)
        frame = pd.DataFrame(
            {
                "feedback_id": ids,
                "customer_id": take["customer_id"].to_numpy(dtype=np.int64),
                "flight_id": take["flight_id"].to_numpy(dtype=np.int64),
                "booking_id": take["booking_id"].to_numpy(dtype=np.int64),
                "rating": rating,
                "nps_score": nps,
                "comments": self.rng.choice(comments_pool, size=n),
                "submitted_ts": submitted,
                "created_at": submitted,
                "updated_at": submitted,
            }
        )
        return frame.loc[:, column_names("customer_feedback")]
