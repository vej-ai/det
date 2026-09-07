"""Singular Reporting API source — two @stream functions.

* ``network_reports`` — the broad daily fact: one row per
  (date × app × source × os × platform × country × campaign) with network
  metrics (cost, impressions, clicks, installs) and Singular's attributed
  cohort ``revenue`` (period ``actual``). No filters — every source
  including organic.

* ``agency_reports`` — the same report fetched once per configured agency
  (``agencies`` param), rows tagged with the agency value. Exists because
  Singular's Reporting API accepts ``agency_name`` as a FILTER but not as
  a GROUP-BY dimension, so agency membership (e.g. which rows belong to
  Example Agency) cannot be recovered from the unfiltered report. Downstream
  models join/filter on this table to attribute agency-run traffic.

Both streams share the incremental strategy:

  1. windows = (cursor − lookback_days | initial_since_date) → YESTERDAY,
     sliced into ``window_chunk_days`` chunks. Today is never fetched —
     it is a partial day that would land half-baked numbers; the daily
     schedule picks it up tomorrow.
  2. Each window is one async report (create → poll → download).
  3. ``merge`` on the dimensional PK upserts restated values in place —
     Singular numbers keep maturing (late network reporting, cohort
     revenue), which is exactly why every run re-covers the lookback.
  4. ``cursor.observe`` fires ONCE, after every window fetched cleanly,
     with the last window's end date. A failed window aborts the run
     before the cursor moves, so the next run re-covers the same ground.

The streams declare no ``incremental.lookback`` (the engine would subtract it
before the stream runs) — the connector-owned subtraction below, driven by
the ``lookback_days`` param, is what re-covers the maturing window.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

from dtex import Batch, Config, Cursor, stream

from .client import SingularClient

_BATCH_SIZE = 500

# Module-level projection contract — mirrors register.yaml's schema blocks.
# The report is requested with exactly these dimensions/metrics, and rows
# are built only from them, so no phantom API field reaches the destination.
_DIMENSIONS: tuple[str, ...] = (
    "app",
    "source",
    "os",
    "platform",
    "country_field",
    "adn_campaign_id",
    "adn_campaign_name",
)
_INT_METRICS: tuple[str, ...] = ("adn_impressions", "custom_clicks", "custom_installs")
_FLOAT_METRICS: tuple[str, ...] = ("adn_cost",)
_COHORT_METRICS: tuple[str, ...] = ("revenue",)
_COHORT_PERIOD = "actual"


def _client(config: Config) -> SingularClient:
    return SingularClient(
        api_key=config.secrets["api_key"],
        base_url=str(config.base_url),
        poll_timeout_sec=int(config.poll_timeout_sec),
        poll_interval_sec=float(config.poll_interval_sec),
    )


def _windows(
    cursor_value: object,
    *,
    initial_since_date: str,
    lookback_days: int,
    chunk_days: int,
    today: date | None = None,
) -> list[tuple[date, date]]:
    """Inclusive [start, end] date windows covering the fetch range.

    Range = (cursor − lookback, floored at initial_since_date) → yesterday.
    First run (no cursor) starts at initial_since_date. Returns [] when
    there is nothing complete to fetch (e.g. cursor already at yesterday
    and lookback 0, or initial date in the future).
    """
    now = today if today is not None else datetime.now(tz=UTC).date()
    yesterday = now - timedelta(days=1)
    floor = date.fromisoformat(str(initial_since_date)[:10])

    if cursor_value is None:
        start = floor
    else:
        if isinstance(cursor_value, datetime):
            cursor_anchor = cursor_value.date()
        elif isinstance(cursor_value, date):
            cursor_anchor = cursor_value
        else:
            cursor_anchor = date.fromisoformat(str(cursor_value)[:10])
        start = cursor_anchor - timedelta(days=int(lookback_days))
        if start < floor:
            start = floor

    if start > yesterday:
        return []

    span = max(1, int(chunk_days))
    windows: list[tuple[date, date]] = []
    window_start = start
    while window_start <= yesterday:
        window_end = min(window_start + timedelta(days=span - 1), yesterday)
        windows.append((window_start, window_end))
        window_start = window_end + timedelta(days=1)
    return windows


def _report_query(
    window_start: date, window_end: date, *, agency: str | None = None
) -> dict[str, str]:
    """The create_async_report payload for one window."""
    query = {
        "start_date": window_start.isoformat(),
        "end_date": window_end.isoformat(),
        "format": "json",
        "time_breakdown": "day",
        "dimensions": ",".join(_DIMENSIONS),
        "metrics": ",".join(_INT_METRICS + _FLOAT_METRICS),
        "cohort_metrics": ",".join(_COHORT_METRICS),
        "cohort_periods": _COHORT_PERIOD,
        "country_code_format": "iso3",
        # Alignment rows reconcile campaign-vs-creative stats; without
        # creative dimensions they are pure noise — keep them out.
        "display_alignment": "false",
    }
    if agency is not None:
        query["filters"] = json.dumps(
            [{"dimension": "agency_name", "operator": "in", "values": [agency]}]
        )
    return query


def _int_or_none(value: object) -> int | None:
    """Singular serializes numbers as strings and absent as '' — coerce."""
    if value is None or value == "":
        return None
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _cohort_value(value: object) -> float | None:
    """Cohort metrics arrive nested: {"actual": 12.3} (period-keyed)."""
    if isinstance(value, dict):
        value = value.get(_COHORT_PERIOD)
    return _float_or_none(value)


def _record(row: dict, *, agency: str | None = None) -> dict:
    """One flat, projected record from one raw report row.

    With ``time_breakdown=day`` every row carries per-day start_date ==
    end_date; start_date is the fact date. Dimension values are coerced
    to strings ('' for missing) because they are merge-key columns —
    NULLs never match in a MERGE and would duplicate rows forever.
    """
    record: dict = {"date": str(row.get("start_date") or "")[:10]}
    if agency is not None:
        record["agency"] = agency
    for name in _DIMENSIONS:
        value = row.get(name)
        record[name] = "" if value is None else str(value)
    for name in _INT_METRICS:
        record[name] = _int_or_none(row.get(name))
    for name in _FLOAT_METRICS:
        record[name] = _float_or_none(row.get(name))
    record["revenue"] = _cohort_value(row.get("revenue"))
    return record


def _extract(
    stream_name: str,
    windows: list[tuple[date, date]],
    agencies: list[str | None],
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """The shared walk — one report per (agency × window), observe once."""
    client = _client(config)
    batch: list[dict] = []
    rows_seen = 0

    for agency in agencies:
        for index, (window_start, window_end) in enumerate(windows, start=1):
            log.info(
                "singular.%s: window %d/%d %s → %s%s",
                stream_name,
                index,
                len(windows),
                window_start,
                window_end,
                f" agency={agency}" if agency else "",
            )
            rows = client.run_report(
                _report_query(window_start, window_end, agency=agency), log=log
            )
            for row in rows:
                rows_seen += 1
                record = _record(row, agency=agency)
                if not record["date"]:
                    # A row without a date cannot merge — skip loudly.
                    log.warning(
                        "singular.%s: dropped a dateless row: %r",
                        stream_name,
                        {k: row.get(k) for k in ("start_date", "source")},
                    )
                    continue
                batch.append(record)
                if len(batch) >= _BATCH_SIZE:
                    yield batch
                    batch = []

    if batch:
        yield batch

    # Advance the cursor exactly once, and only after EVERY window (for
    # every agency) fetched cleanly — a mid-run failure must leave the
    # cursor behind so the next run re-covers the same ground.
    last_complete = windows[-1][1]
    cursor.observe(last_complete)
    log.info(
        "singular.%s: %d rows across %d window(s); cursor → %s",
        stream_name,
        rows_seen,
        len(windows),
        last_complete,
    )


@stream(name="network_reports")
def network_reports(
    config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    windows = _windows(
        cursor.start_value(),
        initial_since_date=str(config.initial_since_date),
        lookback_days=int(config.lookback_days),
        chunk_days=int(config.window_chunk_days),
    )
    if not windows:
        log.info("singular.network_reports: no complete days to fetch")
        return
    yield from _extract("network_reports", windows, [None], config, cursor, log)


@stream(name="agency_reports")
def agency_reports(
    config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    agencies = [a.strip() for a in str(config.agencies).split(",") if a.strip()]
    if not agencies:
        log.info(
            "singular.agency_reports: no agencies configured — nothing to fetch "
            "(set params.agencies, e.g. 'agency-a')"
        )
        return
    windows = _windows(
        cursor.start_value(),
        initial_since_date=str(config.initial_since_date),
        lookback_days=int(config.lookback_days),
        chunk_days=int(config.window_chunk_days),
    )
    if not windows:
        log.info("singular.agency_reports: no complete days to fetch")
        return
    yield from _extract("agency_reports", windows, list(agencies), config, cursor, log)
