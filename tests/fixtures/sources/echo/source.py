"""The echo fixture source — deterministic ``@stream`` generators for tests.

``echo`` is not a real connector: every stream yields a small, fixed dataset so
a test can assert on exact row counts and values. It exists to drive the engine
(stage 5) and the DuckDB destination through realistic motions:

* :func:`events` — an ``append`` stream with a declared schema and a ``JSON``
  column (``payload``). It yields the *same* data every run.
* :func:`items` — a ``merge`` stream with a ``primary_key`` and an
  ``incremental`` cursor. It **filters by the cursor** — only records whose
  ``updated_at`` is strictly greater than ``cursor.start_value()`` are yielded —
  and ``observe()``s each one. That is what makes incremental behavior
  *testable*: a re-run after a committed cursor yields fewer (or zero) rows.

docs/03 §3.1: a ``@stream`` generator yields *batches* (``list[dict]``), not
single records. Both streams here yield in two small batches so the
multi-batch path is exercised.
"""

from __future__ import annotations

from collections.abc import Iterator

from dtex import Batch, Cursor, stream

# --------------------------------------------------------------------------
# Fixed fixture data — deterministic, so tests assert on exact values.
# --------------------------------------------------------------------------

# The full ``events`` dataset — 4 records, yielded the same way every run.
# ``payload`` is a nested dict / list, destined for a JSON column.
_EVENTS: list[dict[str, object]] = [
    {"id": 1, "name": "alpha", "amount": 1.5, "active": True,
     "payload": {"tags": ["a", "b"], "score": 10}},
    {"id": 2, "name": "beta", "amount": 2.0, "active": False,
     "payload": {"tags": [], "score": 0}},
    {"id": 3, "name": "gamma", "amount": 3.25, "active": True,
     "payload": {"nested": {"deep": [1, 2, 3]}}},
    {"id": 4, "name": "delta", "amount": 4.0, "active": False,
     "payload": ["plain", "list", "payload"]},
]

# The full ``items`` dataset — 5 records with a monotonically increasing
# ``updated_at`` cursor (1..5). The fixture stream filters this list by the
# cursor, so the engine's committed-cursor value decides how many are yielded.
_ITEMS: list[dict[str, object]] = [
    {"id": 1, "label": "item-one", "updated_at": 1},
    {"id": 2, "label": "item-two", "updated_at": 2},
    {"id": 3, "label": "item-three", "updated_at": 3},
    {"id": 4, "label": "item-four", "updated_at": 4},
    {"id": 5, "label": "item-five", "updated_at": 5},
]


def _in_batches(records: list[dict[str, object]], size: int) -> Iterator[Batch]:
    """Yield ``records`` as successive batches of at most ``size`` — docs/03 §3.1."""
    for start in range(0, len(records), size):
        yield records[start : start + size]


@stream(name="events")
def events() -> Iterator[Batch]:
    """Yield the fixed ``events`` dataset — an ``append`` stream.

    Declares no injectables: an append fixture needs no config, state or
    cursor. Yields the same 4 records every run, in 2 batches of 2, so the
    multi-batch ``append`` path is covered.
    """
    yield from _in_batches(_EVENTS, size=2)


@stream(name="items")
def items(cursor: Cursor) -> Iterator[Batch]:
    """Yield ``items`` newer than the cursor — a ``merge`` + ``incremental`` stream.

    docs/03 §3.2: ``cursor.start_value()`` is the resume point (the last
    committed ``updated_at``, or the ``initial_value`` ``0`` on the first run).
    This stream yields only records strictly past that point and ``observe()``s
    each one's cursor value, so the engine can persist the new max.

    Consequence — the property the smoke test depends on: a first run (cursor
    ``0``) yields all 5 items; a re-run after the cursor is committed at ``5``
    yields 0. Incremental resume is therefore directly observable as a row
    count.
    """
    start = cursor.start_value()
    floor = 0 if start is None else int(start)

    fresh = [r for r in _ITEMS if int(r["updated_at"]) > floor]  # type: ignore[arg-type]
    for record in fresh:
        cursor.observe(record["updated_at"])

    yield from _in_batches(fresh, size=2)
