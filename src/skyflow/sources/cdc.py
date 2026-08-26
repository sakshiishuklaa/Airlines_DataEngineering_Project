"""Full-load vs incremental (CDC) slicing for source extracts."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from skyflow.generator.schemas import ENTITY_ORDER
from skyflow.sources.catalog import DATASET_BY_ENTITY
from skyflow.sources.rng import entity_rng

CDC_BIRTH = "_cdc_birth_date"
CDC_CHANGE = "_cdc_change_date"


@dataclass(frozen=True, slots=True)
class BatchPlan:
    extract_date: str
    mode: str  # full | incremental


def plan_batches(mode: str, extract_dates: list[str]) -> list[BatchPlan]:
    if not extract_dates:
        raise ValueError("At least one extract_date is required")
    if mode == "full":
        return [BatchPlan(d, "full") for d in extract_dates]
    if mode == "incremental":
        return [BatchPlan(d, "incremental") for d in extract_dates]
    if mode == "window":
        first, *rest = extract_dates
        return [BatchPlan(first, "full"), *[BatchPlan(d, "incremental") for d in rest]]
    raise ValueError(f"Unknown source mode '{mode}'. Use full, incremental, or window.")


def annotate_cdc(
    frames: dict[str, pd.DataFrame],
    extract_dates: list[str],
    seed: int,
    holdback_frac: float,
    update_frac: float,
) -> dict[str, pd.DataFrame]:
    """Assign simulated insert/update extract dates. Does not mutate source contracts.

    Window / full+incremental: most rows are born on the first date; a holdback
    appears on later dates as inserts; a sample of early rows is re-emitted as
    updates (loyalty / status-style CDC).
    """
    if not extract_dates:
        raise ValueError("extract_dates must be non-empty")
    first = extract_dates[0]
    later = extract_dates[1:]
    annotated: dict[str, pd.DataFrame] = {}

    for entity in ENTITY_ORDER:
        frame = frames[entity].copy()
        n = len(frame)
        birth = np.array([first] * n, dtype=object)
        change = np.array([None] * n, dtype=object)
        if n == 0 or not later or holdback_frac <= 0:
            frame[CDC_BIRTH] = birth
            frame[CDC_CHANGE] = change
            annotated[entity] = frame
            continue

        rng = entity_rng(seed, "cdc", entity)
        hold_n = int(np.floor(n * holdback_frac))
        if hold_n > 0:
            hold_idx = rng.choice(n, size=hold_n, replace=False)
            later_assign = rng.choice(np.array(later, dtype=object), size=hold_n)
            birth[hold_idx] = later_assign

        early_mask = birth == first
        early_idx = np.flatnonzero(early_mask)
        upd_n = int(np.floor(len(early_idx) * update_frac))
        if upd_n > 0:
            pick = rng.choice(early_idx, size=upd_n, replace=False)
            change[pick] = rng.choice(np.array(later, dtype=object), size=upd_n)
            frame = _apply_cdc_mutations(entity, frame, pick, rng)

        frame[CDC_BIRTH] = birth
        frame[CDC_CHANGE] = change
        annotated[entity] = frame
    return annotated


def _apply_cdc_mutations(
    entity: str,
    frame: pd.DataFrame,
    idx: np.ndarray,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Touch a business attribute so incremental files are not byte-identical copies."""
    if len(idx) == 0:
        return frame
    out = frame.copy()
    if entity == "customers":
        tiers = np.array(["standard", "silver", "gold", "platinum"])
        out.loc[out.index[idx], "loyalty_tier"] = rng.choice(tiers, size=len(idx))
    elif entity == "flights":
        out.loc[out.index[idx], "delay_minutes"] = (
            out.loc[out.index[idx], "delay_minutes"].to_numpy(dtype=np.int64) + rng.integers(1, 12, size=len(idx))
        )
    elif entity == "bookings":
        current = out.loc[out.index[idx], "booking_status"].to_numpy()
        flipped = np.where(current == "confirmed", "checked_in", current)
        out.loc[out.index[idx], "booking_status"] = flipped
    return out


def slice_entity(
    frame: pd.DataFrame,
    entity: str,
    plan: BatchPlan,
) -> pd.DataFrame:
    system, _dataset = DATASET_BY_ENTITY[entity]
    work = frame.copy()
    if CDC_BIRTH not in work.columns:
        drop = [c for c in (CDC_BIRTH, CDC_CHANGE) if c in work.columns]
        return work.drop(columns=drop, errors="ignore")

    birth = work[CDC_BIRTH].astype(str)
    change = work[CDC_CHANGE]

    if system.extract_style == "snapshot" or plan.mode == "full":
        mask = birth <= plan.extract_date
        sliced = work.loc[mask].copy()
    else:
        is_insert = birth == plan.extract_date
        is_update = change.notna() & (change.astype(str) == plan.extract_date)
        sliced = work.loc[is_insert | is_update].copy()

    return sliced.drop(columns=[CDC_BIRTH, CDC_CHANGE], errors="ignore")
