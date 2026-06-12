"""RevenueCat v2 source — three @stream functions.

* `customers` and `subscriptions` — NON-incremental. RC v2's /customers
  endpoint has no server-side date filter (verified June 2026 against
  the official docs, the airbyte issue 70315, and the RC community
  forum). Every run paginates the full customer list. We drop the
  client-side `if ts < since: continue` filter that v0.1 of this
  connector had — it was a data-loss bug, because RC doesn't guarantee
  ordering, so a high `last_seen_at` early in the response advanced
  the cursor past records the next run would skip. `write_disposition:
  merge` on `id` makes re-pulls upsert idempotently.

* `metrics_daily` — incremental, real. RC v2's /charts/{chart_name}
  endpoint takes server-side `start_date`/`end_date` filters AND tags
  each per-day value with `incomplete=true` when the day is still
  finalizing. We pull from `max(cursor, today - lookback_days)` up to
  today, then advance the cursor only past the most recent COMPLETE
  day so the next run re-pulls today (and any other still-incomplete
  recent day). Long-format output — one row per
  (cohort_date × chart × measure) — so adding charts is zero schema
  migration.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta

from dtex import Batch, Config, Cursor, stream

from .client import RevenueCatClient

_BATCH_SIZE = 500


def _ms_to_ts(ms: int | None) -> str | None:
    """Convert RC's millisecond-epoch timestamps to ISO-8601 UTC."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _cohort_to_date(cohort_unix_seconds: int) -> str:
    """Convert chart-cohort Unix seconds to an ISO date (UTC)."""
    return (
        datetime.fromtimestamp(cohort_unix_seconds, tz=UTC).date().isoformat()
    )


# ---------------------------------------------------------------------------
# customers — non-incremental, paginate everything every run, merge on `id`
# ---------------------------------------------------------------------------


@stream(name="customers")
def customers(config: Config, log: logging.Logger) -> Iterator[Batch]:
    client = RevenueCatClient(
        api_key=config.secrets["api_key"],
        project_id=config.project_id,
        base_url=config.base_url,
    )
    path = f"/projects/{config.project_id}/customers"
    page_size = int(config.page_size)

    log.info("revenuecat.customers: full sync (no server-side date filter on v2)")

    batch: list[dict] = []
    yielded = 0
    rows_seen = 0
    for row in client.paginate(path, {"limit": page_size}):
        rows_seen += 1
        # Progress every 5k rows so a stall is visible in the log rather
        # than inferred from `ps`. RC's edge can produce long quiet
        # stretches under rate-limit; this gives the operator something
        # to look at.
        if rows_seen % 5000 == 0:
            log.info("revenuecat.customers: paginated %d rows so far", rows_seen)
        batch.append(
            {
                "id": row["id"],
                "first_seen_at": _ms_to_ts(row.get("first_seen_at")),
                "last_seen_at": _ms_to_ts(row.get("last_seen_at")),
                "last_seen_app_version": row.get("last_seen_app_version"),
                "last_seen_country": row.get("last_seen_country"),
                "last_seen_platform": row.get("last_seen_platform"),
            }
        )
        if len(batch) >= _BATCH_SIZE:
            yielded += len(batch)
            yield batch
            batch = []
    if batch:
        yielded += len(batch)
        yield batch
    log.info("revenuecat.customers: yielded %d rows", yielded)


# ---------------------------------------------------------------------------
# subscriptions — per-customer fan-out, non-incremental, merge on `id`
# ---------------------------------------------------------------------------
#
# RC has no project-level /subscriptions endpoint, so this stream iterates
# /customers and then per-customer /subscriptions. For N customers it's
# O(N+1) HTTP calls per run. Cost the operator pays for the v2 API shape.


@stream(name="subscriptions")
def subscriptions(config: Config, log: logging.Logger) -> Iterator[Batch]:
    client = RevenueCatClient(
        api_key=config.secrets["api_key"],
        project_id=config.project_id,
        base_url=config.base_url,
    )
    project_id = config.project_id
    page_size = int(config.page_size)

    log.info(
        "revenuecat.subscriptions: per-customer fan-out — expect N+1 HTTP calls"
    )

    batch: list[dict] = []
    customers_seen = 0
    subs_seen = 0
    customer_path = f"/projects/{project_id}/customers"
    for customer in client.paginate(customer_path, {"limit": page_size}):
        customers_seen += 1
        # Same progress cadence as customers — without this a 50-min
        # subscriptions walk looks identical to a stall from outside.
        if customers_seen % 5000 == 0:
            log.info(
                "revenuecat.subscriptions: walked %d customers, %d subscriptions so far",
                customers_seen,
                subs_seen,
            )
        customer_last_seen = _ms_to_ts(customer.get("last_seen_at"))
        sub_path = (
            f"/projects/{project_id}/customers/{customer['id']}/subscriptions"
        )
        for row in client.paginate(sub_path, {"limit": page_size}):
            subs_seen += 1
            revenue = row.get("total_revenue_in_usd") or {}
            batch.append(
                {
                    "id": row["id"],
                    "customer_id": row.get("customer_id") or customer["id"],
                    "customer_last_seen_at": customer_last_seen,
                    "product_id": row.get("product_id"),
                    "status": row.get("status"),
                    "gives_access": row.get("gives_access"),
                    "auto_renewal_status": row.get("auto_renewal_status"),
                    "store": row.get("store"),
                    "store_subscription_identifier": row.get(
                        "store_subscription_identifier"
                    ),
                    "current_period_starts_at": _ms_to_ts(
                        row.get("current_period_starts_at")
                    ),
                    "current_period_ends_at": _ms_to_ts(
                        row.get("current_period_ends_at")
                    ),
                    "total_revenue_gross_usd": revenue.get("gross"),
                    "total_revenue_proceeds_usd": revenue.get("proceeds"),
                }
            )
            if len(batch) >= _BATCH_SIZE:
                yield batch
                batch = []
    if batch:
        yield batch
    log.info(
        "revenuecat.subscriptions: walked %d customers, yielded %d subscriptions",
        customers_seen,
        subs_seen,
    )


# ---------------------------------------------------------------------------
# metrics_daily — incremental on cohort_date, long format
# ---------------------------------------------------------------------------
#
# The strategy:
#   1. start_date = max(cursor.start_value() or initial_value,
#                       today - lookback_days)
#   2. end_date = today
#   3. For each chart in metrics_charts:
#        a. GET /projects/{id}/charts/{chart}?resolution=day
#                  &start_date=<>&end_date=<>
#        b. The response carries `measures[]` (label index) and
#           `values[]` ({cohort, incomplete, measure, value}).
#        c. Yield one row per value, joining measure_index → measure_name.
#   4. Observe the MAX cohort_date with `incomplete=false`. That becomes
#      the next run's cursor floor. Any incomplete days are left "below"
#      the cursor so they re-pull next run with the latest finalized value.


@stream(name="metrics_daily")
def metrics_daily(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    client = RevenueCatClient(
        api_key=config.secrets["api_key"],
        project_id=config.project_id,
        base_url=config.base_url,
    )

    charts = [c.strip() for c in str(config.metrics_charts).split(",") if c.strip()]
    if not charts:
        log.warning("revenuecat.metrics_daily: no charts configured — skipping")
        return

    today = datetime.now(tz=UTC).date()
    lookback_days = int(config.metrics_lookback_days)
    lookback_floor = today - timedelta(days=lookback_days)

    cursor_value = cursor.start_value()  # date | None
    # cursor.start_value() returns the typed initial_value (date) on first
    # run, or the last committed cursor on a resumed run.
    if cursor_value is None:
        cursor_value = date.fromisoformat(str(config.metrics_initial_since_date))
    if isinstance(cursor_value, str):
        cursor_value = date.fromisoformat(cursor_value)
    start_date = max(cursor_value, lookback_floor)
    end_date = today

    if start_date > end_date:
        log.info(
            "revenuecat.metrics_daily: start_date %s > end_date %s — no work to do",
            start_date,
            end_date,
        )
        return

    log.info(
        "revenuecat.metrics_daily: charts=%s start=%s end=%s lookback=%dd",
        charts,
        start_date,
        end_date,
        lookback_days,
    )

    pulled_at = datetime.now(tz=UTC).isoformat()
    batch: list[dict] = []
    max_complete_cohort: date | None = None

    for chart_name in charts:
        chart_path = f"/projects/{config.project_id}/charts/{chart_name}"
        data = client.get(
            chart_path,
            {
                "resolution": "day",
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )

        # measures[i].display_name is the label for measure_index i.
        measure_names: dict[int, str] = {}
        for idx, m in enumerate(data.get("measures") or []):
            measure_names[idx] = str(
                m.get("display_name") or m.get("name") or f"measure_{idx}"
            )

        values = data.get("values") or []
        log.info(
            "revenuecat.metrics_daily: chart=%s measures=%s rows=%d",
            chart_name,
            list(measure_names.values()),
            len(values),
        )

        for v in values:
            cohort_seconds = v.get("cohort")
            if cohort_seconds is None:
                continue
            cohort_date = _cohort_to_date(int(cohort_seconds))
            measure_idx = int(v.get("measure", 0))
            measure_name = measure_names.get(measure_idx, f"measure_{measure_idx}")
            incomplete = bool(v.get("incomplete", False))

            batch.append(
                {
                    "cohort_date": cohort_date,
                    "chart_name": chart_name,
                    "measure_name": measure_name,
                    "value": v.get("value"),
                    "incomplete": incomplete,
                    "pulled_at": pulled_at,
                }
            )

            # Only complete days advance the cursor — incomplete (today,
            # usually) get re-pulled on the next run for the corrected value.
            if not incomplete:
                cohort_d = date.fromisoformat(cohort_date)
                if max_complete_cohort is None or cohort_d > max_complete_cohort:
                    max_complete_cohort = cohort_d

            if len(batch) >= _BATCH_SIZE:
                yield batch
                batch = []

    if batch:
        yield batch

    # Advance the cursor exactly once at the end so partial-batch crashes
    # don't lock the cursor at a too-recent value before all charts pulled.
    if max_complete_cohort is not None:
        cursor.observe(max_complete_cohort)
        log.info(
            "revenuecat.metrics_daily: cursor advanced to %s (last complete day)",
            max_complete_cohort,
        )
    else:
        log.info("revenuecat.metrics_daily: no complete days observed — cursor unchanged")
