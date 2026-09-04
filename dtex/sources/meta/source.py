"""Meta Marketing API source — two @streams walking (account × date window).

Both streams share one extraction loop (``_walk``):

* Resolve the walk floor: ``max(cursor - lookback_days, start_date)`` when
  a cursor exists, else ``start_date`` (virgin run = the backfill).
* Walk ``[floor, today]`` in inclusive windows of ``window_days``,
  oldest -> newest; inside each window loop every configured ad account,
  run one ASYNC Insights report job (``time_increment=1``, the stream's
  ``level`` / ``breakdowns``) and page its result — see client.py for why
  sync GETs are not used.
* Yield batches per page; after EVERY account of a window has been fully
  yielded, ``cursor.observe(window_end)``. The engine may flush cursor
  state after any durable batch write, so a day observed before its rows
  left our buffer could be committed as "done" without its data.

Streams:

* ``ads_insights`` — level=ad, no breakdown, merge on
  (date_start, account_id, ad_id).
* ``insights_hourly`` — level=campaign, breakdown
  ``hourly_stats_aggregated_by_advertiser_time_zone``, merge on
  (date_start, hour, account_id, campaign_id). The hour is in the AD
  ACCOUNT's timezone, so each account's ``timezone_name`` /
  ``timezone_offset_hours_utc`` is fetched ONCE per run
  (``GET /act_<id>?fields=timezone_name,timezone_offset_hours_utc``) and
  stamped on every row. Each stream has its own cursor, and per-stream
  ``params:`` in the config (window_days / start_date) size its backfill
  independently.

Today is included on purpose: it is partial, and the next run's lookback
re-pull (28 days by default — Meta's longest attribution restatement
window) overwrites it via merge on the natural grain. The same re-pull
absorbs late conversions and deleted/rejected-ad removals for recent
days; a restatement older than the lookback is only caught by a manual
re-pull (one-shot ``since:`` under streams:, or ``dtex state reset``).

A window whose async job keeps ending ``Job Failed`` / ``Job Skipped`` is
BISECTED: Meta sheds load this way for reports it deems too large, the
job fails before any page is fetched (nothing has been yielded), so the
sub-window is simply re-requested narrower. A 1-day window that still
fails is a real error and raises.

Every (window, account) is logged with its row + page counts and the
account's usage-header percentage — a silent run is a bug, not a feature.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from itertools import chain
from typing import Any

from dtex import Batch, Config, Cursor, stream

from .client import AsyncJobFailed, MetaClient
from .records import (
    HOURLY_BREAKDOWN,
    as_date,
    iter_windows,
    parse_accounts,
    to_hourly_record,
    to_record,
)

# Fields requested by the hourly stream — campaign grain only; the ad-level
# breakdown arrays (video, unique_*) are not needed for hour-of-day spend.
HOURLY_FIELDS = (
    "account_id,account_name,account_currency,campaign_id,campaign_name,"
    "date_start,spend,impressions,clicks,actions,action_values"
)
ACCOUNT_TZ_FIELDS = "timezone_name,timezone_offset_hours_utc"

Projector = Callable[[dict[str, Any], str], dict[str, Any] | None]


def _build_client(config: Config) -> MetaClient:
    return MetaClient(
        access_token=str(config.secrets["access_token"]),
        base_url=str(config.base_url),
        api_version=str(config.api_version),
        page_size=int(config.page_size),
        max_retries=int(config.max_retries),
        page_delay_seconds=float(config.page_delay_seconds),
        poll_interval_seconds=float(config.poll_interval_seconds),
        job_timeout_seconds=float(config.job_timeout_seconds),
    )


def _walk(
    *,
    name: str,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
    client: MetaClient,
    accounts: list[str],
    fields: str,
    level: str,
    breakdowns: str,
    project: Projector,
    key_desc: str,
) -> Iterator[Batch]:
    """Shared (window × account) async-report walk; see module docstring."""
    attribution = str(config.action_attribution_windows or "")
    run_ts = datetime.now(tz=UTC)
    today = run_ts.date()
    start_floor = as_date(config.start_date)

    start_value = cursor.start_value()
    if start_value is None:
        first_day = start_floor
        log.info("meta.%s: virgin state — backfilling from %s", name, first_day)
    else:
        first_day = max(
            as_date(start_value) - timedelta(days=int(config.lookback_days)),
            start_floor,
        )
        log.info(
            "meta.%s: resuming — cursor=%s, lookback %sd -> walking from %s",
            name,
            start_value,
            config.lookback_days,
            first_day,
        )

    if first_day > today:
        log.info("meta.%s: walk floor %s is in the future — nothing to do", name, first_day)
        return

    total_rows = 0
    total_windows = 0
    for win_start, win_end in iter_windows(first_day, today, int(config.window_days)):
        window_rows = 0
        for account in accounts:
            # Bisect on async-job failure: the job fails BEFORE any page is
            # fetched, so nothing has been yielded and the sub-window can be
            # re-requested narrower; a 1-day window that still fails raises.
            pending = [(win_start, win_end)]
            while pending:
                sub_start, sub_end = pending.pop(0)
                account_rows = 0
                pages_seen = 0
                skipped = 0
                try:
                    pages = client.iter_insights(
                        account,
                        sub_start,
                        sub_end,
                        fields,
                        attribution,
                        level=level,
                        breakdowns=breakdowns,
                    )
                    first_page = next(pages, None)
                except AsyncJobFailed as exc:
                    span_days = (sub_end - sub_start).days + 1
                    if span_days <= 1:
                        raise RuntimeError(
                            f"{exc} — even a 1-day window fails; trim the fields "
                            "param or check the account in Ads Manager"
                        ) from exc
                    mid = sub_start + timedelta(days=span_days // 2)
                    log.warning(
                        "meta.%s: act_%s %s..%s async job kept failing — bisecting at %s",
                        name,
                        account,
                        sub_start,
                        sub_end,
                        mid,
                    )
                    pending[:0] = [(sub_start, mid - timedelta(days=1)), (mid, sub_end)]
                    continue
                for page, rows in chain([first_page] if first_page else [], pages):
                    pages_seen = page
                    batch: list[dict[str, Any]] = []
                    for row in rows:
                        record = project(row, account)
                        if record is None:
                            skipped += 1
                            continue
                        record["extracted_at"] = run_ts
                        batch.append(record)
                    if batch:
                        yield batch
                    account_rows += len(batch)
                if skipped:
                    log.warning(
                        "meta.%s: act_%s %s..%s had %d object(s) without %s "
                        "— NOT landed (unkeyable)",
                        name,
                        account,
                        sub_start,
                        sub_end,
                        skipped,
                        key_desc,
                    )
                log.info(
                    "meta.%s: act_%s %s..%s -> %d row(s) in %d page(s), usage %.0f%%",
                    name,
                    account,
                    sub_start,
                    sub_end,
                    account_rows,
                    pages_seen,
                    client.last_usage_pct,
                )
                window_rows += account_rows

        # Every account of this window (and every bisected sub-window) has
        # been yielded — only now may the cursor pass it.
        cursor.observe(win_end)
        total_rows += window_rows
        total_windows += 1

    log.info(
        "meta.%s: %d row(s) across %d window(s) x %d account(s), walked %s..%s",
        name,
        total_rows,
        total_windows,
        len(accounts),
        first_day,
        today,
    )


@stream(name="ads_insights")
def ads_insights(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Ad-level daily insights for every account; merge on (day, account, ad)."""
    run_ts = datetime.now(tz=UTC)

    def project(row: dict[str, Any], _account: str) -> dict[str, Any] | None:
        return to_record(row, run_ts)

    yield from _walk(
        name="ads_insights",
        config=config,
        cursor=cursor,
        log=log,
        client=_build_client(config),
        accounts=parse_accounts(config.account_ids),
        fields=str(config.fields),
        level="ad",
        breakdowns="",
        project=project,
        key_desc="date_start/account_id/ad_id",
    )


@stream(name="insights_hourly")
def insights_hourly(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Campaign-level hour-of-day insights (advertiser tz).

    Merge on (day, hour, account, campaign).
    """
    client = _build_client(config)
    accounts = parse_accounts(config.account_ids)
    run_ts = datetime.now(tz=UTC)

    # Timezone per account, fetched once per run — the hourly breakdown is
    # expressed in it, so it must ride with every row for downstream SQL to
    # convert.
    tz_by_account: dict[str, tuple[str | None, float | None]] = {}
    for account in accounts:
        info = client.get_account(account, ACCOUNT_TZ_FIELDS)
        tz_name = info.get("timezone_name")
        raw_off = info.get("timezone_offset_hours_utc")
        try:
            tz_off = None if raw_off is None else float(raw_off)
        except (TypeError, ValueError):
            tz_off = None
        if not tz_name:
            log.warning(
                "meta.insights_hourly: act_%s returned no timezone_name — rows land "
                "with NULL timezone; downstream cannot convert them",
                account,
            )
        tz_by_account[account] = (str(tz_name) if tz_name else None, tz_off)
        log.info(
            "meta.insights_hourly: act_%s timezone %s (utc offset %s)", account, tz_name, raw_off
        )

    def project(row: dict[str, Any], account: str) -> dict[str, Any] | None:
        tz_name, tz_off = tz_by_account[account]
        return to_hourly_record(row, run_ts, tz_name, tz_off)

    yield from _walk(
        name="insights_hourly",
        config=config,
        cursor=cursor,
        log=log,
        client=client,
        accounts=accounts,
        fields=HOURLY_FIELDS,
        level="campaign",
        breakdowns=HOURLY_BREAKDOWN,
        project=project,
        key_desc=f"date_start/{HOURLY_BREAKDOWN}/account_id/campaign_id",
    )
