"""Pure helpers for the Meta source — no ``dtex`` and no HTTP imports.

Import-clean on purpose so the record projection and the window math are
unit-testable without the engine or a network stub.

``to_record`` keeps the Insights object essentially as Meta returns it
with three adjustments:

* the three primary-key columns are validated (a row without
  ``date_start`` / ``account_id`` / ``ad_id`` is unkeyable and dropped by
  the caller, counted and logged);
* ``extracted_at`` is stamped;
* the ``*_actions`` / ``conversions`` breakdown lists are passed through
  untouched so they land as JSON arrays of ``{action_type, value}`` —
  the shape downstream SQL unnests.

``to_hourly_record`` is the campaign-level, hour-of-day sibling for the
``insights_hourly`` stream: the ``hourly_stats_aggregated_by_advertiser_
time_zone`` breakdown arrives as the string ``"HH:00:00 - HH:59:59"``;
``parse_hour`` turns it into the INTEGER ``hour`` (0..23) that is part of
that stream's primary key, and the caller stamps the account's
``timezone_name`` / ``timezone_offset_hours_utc`` so downstream SQL can
convert the advertiser-local slot to any other zone.

Numeric scalars arrive as strings ("12.34"); the engine's NORMALIZE step
coerces them to the declared FieldType, so no casting happens here.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from typing import Any

# The Insights breakdown that buckets a day's metrics by hour in the AD
# ACCOUNT's timezone (not UTC, not the viewer's). Its value is the string
# "HH:00:00 - HH:59:59".
HOURLY_BREAKDOWN = "hourly_stats_aggregated_by_advertiser_time_zone"
_HOUR_RE = re.compile(r"^\s*(\d{1,2}):\d{2}:\d{2}\s*-\s*\d{1,2}:\d{2}:\d{2}\s*$")

# Fields Meta returns as lists of {action_type, value}. Anything in this set
# is landed as-is (JSON); an unexpected scalar here is left alone too — the
# declared JSON type only matters for the columns in register.yaml.
ARRAY_FIELDS: frozenset[str] = frozenset(
    {
        "actions",
        "action_values",
        "unique_actions",
        "conversions",
        "conversion_values",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p95_watched_actions",
        "video_p100_watched_actions",
        "video_time_watched_actions",
    }
)


def parse_accounts(raw: str) -> list[str]:
    """Comma-separated ad account ids -> clean list without ``act_`` prefixes.

    Order kept, duplicates dropped; raises on an empty result so a
    misconfigured pipeline fails at startup rather than walking nothing.
    """
    seen: list[str] = []
    for token in str(raw).split(","):
        acc = token.strip()
        if acc.startswith("act_"):
            acc = acc[4:]
        if acc and acc not in seen:
            seen.append(acc)
    if not seen:
        raise ValueError(
            "meta: the account_ids param is empty — set it to a comma-separated "
            "list of ad account ids (with or without the act_ prefix)"
        )
    return seen


def as_date(value: object) -> date:
    """Coerce a cursor/param value (date | datetime | ISO string) to a date."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def iter_windows(first_day: date, last_day: date, window_days: int) -> Iterator[tuple[date, date]]:
    """Inclusive ``[since, until]`` date windows covering first_day..last_day.

    Each window spans at most ``window_days`` calendar days; windows do not
    overlap (Insights ``time_range`` is inclusive on both ends).
    """
    if window_days < 1:
        raise ValueError(f"meta: window_days must be >= 1, got {window_days}")
    start = first_day
    while start <= last_day:
        end = min(start + timedelta(days=window_days - 1), last_day)
        yield start, end
        start = end + timedelta(days=1)


def to_record(row: dict[str, Any], extracted_at: datetime) -> dict[str, Any] | None:
    """One Insights object -> one landed record; ``None`` if unkeyable."""
    date_start = row.get("date_start")
    account_id = row.get("account_id")
    ad_id = row.get("ad_id")
    if not (date_start and account_id and ad_id):
        return None
    record: dict[str, Any] = dict(row)
    record["account_id"] = str(account_id).removeprefix("act_")
    record["ad_id"] = str(ad_id)
    record["extracted_at"] = extracted_at
    return record


def parse_hour(value: object) -> int | None:
    """``"13:00:00 - 13:59:59"`` -> ``13``; ``None`` if the value is not that shape.

    Meta documents the breakdown value as the hour range in the advertiser
    timezone; anything else (empty, ``"unknown"``, a stray int) is
    unkeyable for the hourly stream and dropped by the caller.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if 0 <= value <= 23 else None
    m = _HOUR_RE.match(str(value))
    if not m:
        return None
    hour = int(m.group(1))
    return hour if 0 <= hour <= 23 else None


def to_hourly_record(
    row: dict[str, Any],
    extracted_at: datetime,
    timezone_name: str | None,
    timezone_offset_hours_utc: float | None,
) -> dict[str, Any] | None:
    """One campaign-level hourly Insights object -> landed record; ``None`` if unkeyable.

    Keys: ``date_start``, ``hour`` (parsed from the breakdown), ``account_id``,
    ``campaign_id``. The raw breakdown string is kept alongside the parsed
    ``hour``; the per-account timezone is stamped on every row.
    """
    date_start = row.get("date_start")
    account_id = row.get("account_id")
    campaign_id = row.get("campaign_id")
    raw_hour = row.get(HOURLY_BREAKDOWN)
    hour = parse_hour(raw_hour)
    if not (date_start and account_id and campaign_id) or hour is None:
        return None
    record: dict[str, Any] = dict(row)
    record["account_id"] = str(account_id).removeprefix("act_")
    record["campaign_id"] = str(campaign_id)
    record["hour"] = hour
    record["hour_range"] = None if raw_hour is None else str(raw_hour)
    record.pop(HOURLY_BREAKDOWN, None)
    record["timezone_name"] = timezone_name
    record["timezone_offset_hours_utc"] = timezone_offset_hours_utc
    record["extracted_at"] = extracted_at
    return record
