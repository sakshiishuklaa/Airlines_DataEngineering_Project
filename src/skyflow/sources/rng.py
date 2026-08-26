"""Deterministic RNGs that do not depend on PYTHONHASHSEED."""

from __future__ import annotations

import hashlib

import numpy as np


def entity_rng(seed: int, *parts: str) -> np.random.Generator:
    material = "|".join([str(seed), *parts]).encode("utf-8")
    n = int.from_bytes(hashlib.sha256(material).digest()[:8], "little") % (2**32)
    return np.random.default_rng(n)
