"""Collect Module 1 generator output in memory without writing the lake."""

from __future__ import annotations

from typing import Any

import pandas as pd

from skyflow.generator.schemas import ENTITY_ORDER


class FrameCollector:
    """Duck-typed writer compatible with FlightOpsGenerator.generate()."""

    def __init__(self) -> None:
        self.parts: dict[str, list[pd.DataFrame]] = {name: [] for name in ENTITY_ORDER}
        self.manifest: dict[str, Any] | None = None

    def write_frame(self, entity: str, frame: pd.DataFrame, part: int = 0) -> None:
        self.parts.setdefault(entity, []).append(frame.copy())

    def write_manifest(self, payload: dict[str, Any]) -> None:
        self.manifest = dict(payload)

    def to_frames(self) -> dict[str, pd.DataFrame]:
        out: dict[str, pd.DataFrame] = {}
        for entity, chunks in self.parts.items():
            if not chunks:
                out[entity] = pd.DataFrame()
            else:
                out[entity] = pd.concat(chunks, ignore_index=True)
        return out
