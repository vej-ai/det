"""Sibling helper module — imported by source.py with ``from .helper import ...``.

This file exists only to prove the engine loads the connector folder as a real
Python package (stage 11). If the relative import in ``source.py`` resolves,
the engine's load-as-package mechanism is working.
"""

from __future__ import annotations

# Two single-record batches, so the multi-batch path is exercised even with a
# minimal dataset. The IDs are deterministic so a test can assert on exact
# rows loaded.
FIXTURE_BATCHES: list[list[dict[str, object]]] = [
    [{"id": 1}],
    [{"id": 2}],
]
