"""Date-window stepping — port of v2/main.py lines 363-376 / 351 lookback math.

The ShipHero GraphQL API has hard server-side limits on how large a single
`date_from`..`date_to` query may be. The reference connector solves this by
splitting the cursor → now range into fixed-width windows (default 10 days),
running one paginated GraphQL query per window. Each yielded record sits in
exactly one window, so per-window pagination is independent and a window's
failure does not corrupt earlier windows (the engine's per-stream transaction
rolls back this stream's load on failure, but each *batch* the destination
already wrote inside the window stays written until the engine signals abort).

This module owns two responsibilities:

* :func:`compute_start` — apply the ``lookback_days`` subtraction to the cursor
  resume value. The det engine deliberately does NOT do this (see the
  ``# NOTE`` below); the connector owns its own lookback policy.
* :func:`date_windows` — yield ``(date_from, date_to)`` tuples covering the
  range, each at most ``step_days`` wide. Implemented as a generator so a
  long backfill (years of windows) does not materialize the whole list.

# NOTE: docs/03 §3.2 + the ``Cursor`` docstring claim the engine "applies the
# lookback subtraction" before handing the value to the connector. The actual
# implementation in ``det/engine/runner.py::_seed_value`` returns the
# persisted ``cursor_value`` *verbatim* — no lookback math. Per CONTRIBUTING.md
# "code is source of truth", that means **the connector owns the lookback
# subtraction**. We mirror v2/main.py line 351's
# ``checkpoint['created_date'] - timedelta(days=LOOKBACK_DAYS)`` here.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta


def to_utc_dt(value: datetime | str | None) -> datetime | None:
    """Normalize a cursor value into a tz-aware UTC :class:`datetime`.

    The det engine seeds the cursor with one of two shapes depending on
    whether this is a first run or a resume:

    * **First run** — ``engine.runner._seed_value`` parses
      ``incremental.initial_value`` via ``datetime.fromisoformat``. That yields
      a *naive* datetime (``"2024-01-01"`` has no tz).
    * **Resume** — the persisted ``cursor_value`` round-trips through DuckDB's
      ``JSON`` column, which means we read it back as a string (the ISO-8601
      text we serialized).

    Either way we need a single, comparable, tz-aware UTC datetime to drive the
    window loop. Returns ``None`` unchanged so ``compute_start`` can detect "no
    resume point yet".
    """
    if value is None:
        return None
    if isinstance(value, str):
        # Strip trailing Z (datetime.fromisoformat doesn't accept it pre-3.11
        # consistently) and parse.
        text = value.replace("Z", "+00:00") if value.endswith("Z") else value
        dt = datetime.fromisoformat(text)
    else:
        dt = value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def compute_start(
    cursor_value: datetime | str | None,
    *,
    initial_value: str,
    lookback_days: int,
) -> datetime:
    """Compute the start of the date-window loop — port of main.py lines 343-361.

    Resume semantics, mirroring the reference:

    * If a persisted cursor exists, go back ``lookback_days`` from it (catches
      late-arriving rows).
    * Otherwise, start from ``initial_value`` (the manifest's
      ``incremental.initial_value`` or the connector's ``params.start_date``
      fallback).

    The returned datetime is always tz-aware UTC and floored to midnight, so
    successive runs produce stable window boundaries.
    """
    persisted = to_utc_dt(cursor_value)
    if persisted is not None:
        start = persisted - timedelta(days=lookback_days)
    else:
        start = to_utc_dt(initial_value) or datetime(2024, 1, 1, tzinfo=UTC)
    return start.replace(hour=0, minute=0, second=0, microsecond=0)


def date_windows(
    start: datetime,
    *,
    step_days: int,
    end: datetime | None = None,
) -> Iterator[tuple[datetime, datetime]]:
    """Yield ``(date_from, date_to)`` windows of at most ``step_days`` — main.py 374-454.

    A generator (not a list) — a multi-year backfill at the default 10-day step
    would otherwise materialize hundreds of tuples up front. Each window is a
    half-open interval ``[date_from, date_to)``; the final window is clipped to
    ``end`` (default: now, floored to midnight UTC, so windowing is reproducible
    within a single run).

    Raises :class:`ValueError` on a non-positive ``step_days`` — a zero step
    would loop forever.
    """
    if step_days <= 0:
        raise ValueError(f"step_days must be positive, got {step_days}")
    if end is None:
        end = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    current = start
    while current < end:
        nxt = min(current + timedelta(days=step_days), end)
        yield current, nxt
        current = nxt
