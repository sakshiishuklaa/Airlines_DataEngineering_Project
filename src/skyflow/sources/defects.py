"""Intentional source-system data issues (not lake-contract violations in Module 1)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from skyflow.sources.rng import entity_rng

LOGGER = logging.getLogger(__name__)


def _as_object(frame: pd.DataFrame, col: str) -> None:
    frame[col] = frame[col].astype(object)


def apply_defects(entity: str, frame: pd.DataFrame, seed: int, enabled: bool) -> tuple[pd.DataFrame, list[str]]:
    if not enabled or frame.empty:
        return frame, []
    rng = entity_rng(seed, "defects", entity)
    notes: list[str] = []
    out = frame.copy()

    if entity == "flights":
        out, notes = _flight_defects(out, rng, notes)
    elif entity == "bookings":
        out, notes = _booking_defects(out, rng, notes)
    elif entity == "customers":
        out, notes = _customer_defects(out, rng, notes)
    elif entity == "payments":
        out, notes = _payment_defects(out, rng, notes)
    elif entity == "baggage":
        out, notes = _baggage_defects(out, rng, notes)
    elif entity == "airlines":
        out, notes = _code_padding_defects(out, rng, notes, col="iata")
    elif entity == "airports":
        out, notes = _code_padding_defects(out, rng, notes, col="iata")
    elif entity == "routes":
        out, notes = _route_defects(out, rng, notes)
    return out, notes


def _pick(n: int, frac: float, rng: np.random.Generator) -> np.ndarray:
    k = int(np.floor(n * frac))
    if k <= 0 or n == 0:
        return np.array([], dtype=np.int64)
    return rng.choice(n, size=k, replace=False)


def _flight_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    delayed = frame["FLT_STATUS"].astype(str).str.lower().eq("delayed")
    delayed_idx = np.flatnonzero(delayed.to_numpy())
    if len(delayed_idx):
        k = max(1, int(np.floor(len(delayed_idx) * 0.08)))
        take = rng.choice(delayed_idx, size=min(k, len(delayed_idx)), replace=False)
        frame.loc[frame.index[take], "DELAY_MIN"] = 0
        notes.append("delayed_status_with_zero_delay")
    idx = _pick(n, 0.04, rng)
    if len(idx):
        mixed = frame.loc[frame.index[idx], "FLT_STATUS"].astype(str).str.upper()
        frame.loc[frame.index[idx], "FLT_STATUS"] = mixed
        notes.append("mixed_case_flight_status")
    arrived = frame["FLT_STATUS"].astype(str).str.lower().eq("arrived")
    arr_idx = np.flatnonzero(arrived.to_numpy())
    if len(arr_idx):
        k = max(1, int(np.floor(len(arr_idx) * 0.02)))
        take = rng.choice(arr_idx, size=min(k, len(arr_idx)), replace=False)
        frame.loc[frame.index[take], "ACT_DEP_UTC"] = pd.NaT
        notes.append("arrived_flight_null_actual_departure")
    return frame, notes


def _booking_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.03, rng)
    if len(idx):
        frame.loc[frame.index[idx], "PNR"] = frame.loc[frame.index[idx], "PNR"].astype(str) + " "
        notes.append("pnr_trailing_whitespace")
    idx = _pick(n, 0.04, rng)
    if len(idx):
        frame.loc[frame.index[idx], "PNR_STATUS"] = frame.loc[frame.index[idx], "PNR_STATUS"].astype(str).str.upper()
        notes.append("mixed_case_pnr_status")
    idx = _pick(n, 0.015, rng)
    if len(idx):
        _as_object(frame, "FARE_USD")
        fare = frame.loc[frame.index[idx], "FARE_USD"].map(lambda v: f"${float(v):.2f}")
        frame.loc[frame.index[idx], "FARE_USD"] = fare
        notes.append("fare_prefixed_with_dollar_sign")
    idx = _pick(n, 0.004, rng)
    if len(idx):
        frame.loc[frame.index[idx], "PAX_ID"] = 9_999_999
        notes.append("orphan_passenger_id")
    dup_idx = _pick(n, 0.002, rng)
    if len(dup_idx):
        extra = frame.iloc[dup_idx].copy()
        frame = pd.concat([frame, extra], ignore_index=True)
        notes.append("duplicate_booking_rows")
    return frame, notes


def _customer_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.012, rng)
    if len(idx):
        _as_object(frame, "Email")
        frame.loc[frame.index[idx], "Email"] = None
        notes.append("null_email")
    idx = _pick(n, 0.05, rng)
    if len(idx):
        phones = frame.loc[frame.index[idx], "Phone"].astype(str)
        frame.loc[frame.index[idx], "Phone"] = phones.str[:3] + "-" + phones.str[3:6] + "-" + phones.str[6:]
        notes.append("phone_with_dashes")
    idx = _pick(n, 0.06, rng)
    if len(idx):
        _as_object(frame, "DOB")
        dob = pd.to_datetime(frame.loc[frame.index[idx], "DOB"], errors="coerce")
        frame.loc[frame.index[idx], "DOB"] = dob.dt.strftime("%m/%d/%Y")
        notes.append("dob_us_date_format")
    idx = _pick(n, 0.003, rng)
    if len(idx) >= 1 and n >= 2:
        src = int(idx[0])
        dest = int((src + 1) % n)
        frame.loc[frame.index[dest], "Email"] = frame.loc[frame.index[src], "Email"]
        notes.append("duplicate_email")
    return frame, notes


def _payment_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.05, rng)
    if len(idx):
        _as_object(frame, "amount")
        frame.loc[frame.index[idx], "amount"] = frame.loc[frame.index[idx], "amount"].map(lambda v: f"{float(v):.2f}")
        notes.append("amount_as_string")
    idx = _pick(n, 0.08, rng)
    if len(idx):
        frame.loc[frame.index[idx], "currency"] = frame.loc[frame.index[idx], "currency"].astype(str).str.lower()
        notes.append("lowercase_currency")
    idx = _pick(n, 0.01, rng)
    if len(idx):
        _as_object(frame, "txnRef")
        frame.loc[frame.index[idx], "txnRef"] = None
        notes.append("missing_transaction_ref")
    return frame, notes


def _baggage_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.02, rng)
    if len(idx):
        _as_object(frame, "pieces")
        frame.loc[frame.index[idx], "pieces"] = None
        notes.append("null_piece_count")
    extra = pd.Series(rng.choice(np.array(["JFK", "ATL", "ORD", "LAX"]), size=n), index=frame.index)
    frame["stationCode"] = extra
    notes.append("undeclared_station_code_field")
    return frame, notes


def _code_padding_defects(
    frame: pd.DataFrame, rng: np.random.Generator, notes: list[str], col: str
) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.08, rng)
    if len(idx) and col in frame.columns:
        frame.loc[frame.index[idx], col] = frame.loc[frame.index[idx], col].astype(str) + " "
        notes.append(f"{col}_trailing_space")
    return frame, notes


def _route_defects(frame: pd.DataFrame, rng: np.random.Generator, notes: list[str]) -> tuple[pd.DataFrame, list[str]]:
    n = len(frame)
    idx = _pick(n, 0.02, rng)
    if len(idx):
        _as_object(frame, "INTL_FLAG")
        frame.loc[frame.index[idx], "INTL_FLAG"] = ""
        notes.append("blank_intl_flag")
    return frame, notes
