"""Chargebacks911 CBAPIv2 source — two @stream functions.

The two endpoints look symmetrical but behave differently, and the
difference drives this whole module:

``/clients/{id}/chargebacks`` HONOURS the server-side date filter
(``date_column`` + ``start_date`` + ``end_date``), but 503s when the
window is wide. So it is incremental, and the window is walked in bounded
CHUNKS (see ``_iter_windows``) — never as one 2.5-year request.

``/clients/{id}/alerts`` IGNORES the date filter entirely. It always
returns full account history in ``id`` order, at any date range. Verified
live 2026-08-03: a ``2019-01-01``→``2019-01-02`` window returned
present-day alerts, while ``limit``/``page`` demonstrably worked. The
pipeline's own bookkeeping had been showing this for a while — the alerts
stream re-extracted the full 3341 rows on every run, including runs whose
cursor was already at the newest ``dateUpdated``, when only 9 rows sat
past that cursor. So ``alerts`` is an honest FULL SWEEP: no date params
are sent, because sending them produced full history wearing a date label.

Shared shape for both streams:

1. Build the list of request windows (one ``{}`` for a full sweep, or N
   bounded date slices for a chunked incremental run).
2. For each window, paginate with ``limit`` + ``page`` until a short page.
3. Project each row onto the declared schema (phantom API fields never
   reach the destination), tracking the max cursor value seen.
4. ``cursor.observe(...)`` exactly ONCE at the very end, with that max.
   Observing per-row would be equivalent (the engine keeps the max), but
   the single observe at the end makes the commit semantics explicit:
   nothing advances unless the full walk completed and the final batch
   landed.

``write_disposition: merge`` on ``id`` makes re-pulled rows idempotent —
which is what keeps the alerts full sweep and the chargebacks lookback
overlap both safe.

``chargebacks.status_history`` is a nested array of objects; it is yielded
raw under a declared ``JSON`` column, the same convention shiphero and
stripe use for nested values (the destination JSON-encodes it natively).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

from dtex import Batch, Config, Cursor, stream

from .client import Chargebacks911Client

_BATCH_SIZE = 500

# Declared-schema projections — must mirror the `schema:` blocks in
# register.yaml. Kept in code because the @stream body does not receive
# the declared schema as an injectable (same trade-off as the shiphero
# connector); a column here but not in register.yaml is harmless (the
# engine infers it), while projecting guarantees no phantom API field
# ever reaches the destination.

_ALERT_FIELDS: tuple[str, ...] = (
    "id", "clientId", "completed", "isDisputed", "isRefund", "isValid",
    "level", "outcomeId", "currencyId", "cc_id", "amount", "refundAmount",
    "mid", "descriptor", "type", "caseId", "cbCaseId", "transId",
    "currency_name", "outcome_name", "ccType", "ccNum", "cc_name",
    "cc_prettyName", "cc_firstDigit", "cc_firstDigitInt", "caid",
    "refundId", "orderId", "customerName", "customerEmail", "firstName",
    "lastName", "issuerName", "arn", "authCode", "authDate", "reasonCode",
    "transactionType", "fileName", "alertAge", "analytics", "received",
    "expirationDate", "dateBilled", "dateCreated", "dateDisputed",
    "dateUpdated", "transDate", "completedDate", "confirmedDate",
    "outcomeDate", "processedDate", "refundDate",
)

_CHARGEBACK_FIELDS: tuple[str, ...] = (
    "id", "arn", "auth_no", "b_address", "b_city", "b_state", "b_zip",
    "bin", "card_bin", "card_last_four", "last_four", "case_no",
    "case_type", "cc_type", "currency", "chargeback_currency_code",
    "date_due", "date_post", "date_trans", "date_updated", "descriptor",
    "fname", "lname", "ip_address", "mid", "mid_alias", "order_id",
    "platform_name", "reason_category", "reason_code", "reference_number",
    "status", "uid", "chargeback_amount", "dispute_amount",
    "disputed_amount", "crm_gateway_id", "platform_id",
    "order_api_generated", "partial_chargeback", "status_history",
)


def _client(config: Config) -> Chargebacks911Client:
    return Chargebacks911Client(
        username=config.secrets["username"],
        password=config.secrets["password"],
        base_url=str(config.base_url),
    )


def _iter_windows(
    cursor: Cursor, lookback_days: int, chunk_days: int
) -> list[dict[str, str]]:
    """Bounded ``date_updated`` windows covering (cursor − lookback) → today.

    Returns a LIST (not a generator) so the caller can log how many
    requests the walk will take before issuing any.

    ``[{}]`` — a single unfiltered request — when there is no cursor value:
    the first-ever run, or ``--full-refresh``. The streams deliberately
    declare no ``initial_value``, and CB911 503s computing wide windows, so
    the unfiltered sweep is the only bootstrap that works.

    Otherwise the span is sliced into ``chunk_days``-wide windows. A single
    wide request 503s server-side (three of five production runs died on
    ``start_date=2023-12-25&end_date=2026-08-03``), so even a long catch-up
    after an outage now proceeds as many small, individually-retryable
    requests instead of one that cannot succeed.

    Windows are inclusive on both ends and adjacent windows share no day;
    ``merge`` on ``id`` would make an overlap harmless anyway.
    """
    start_value = cursor.start_value()
    if start_value is None:
        return [{}]

    # Cursor values are STRING dates, possibly with a time component — the
    # first 10 chars are the ISO date.
    start = date.fromisoformat(str(start_value)[:10]) - timedelta(days=lookback_days)
    today = datetime.now(tz=UTC).date()
    if start > today:
        # Clock skew, or a cursor from the future. One window ending today
        # is still the honest request; an empty list would silently sync
        # nothing at all.
        start = today

    span = max(1, int(chunk_days))
    windows: list[dict[str, str]] = []
    window_start = start
    while window_start <= today:
        window_end = min(window_start + timedelta(days=span - 1), today)
        windows.append(
            {
                "date_column": "date_updated",
                "start_date": window_start.isoformat(),
                "end_date": window_end.isoformat(),
            }
        )
        window_start = window_end + timedelta(days=1)
    return windows


def _extract(
    stream_name: str,
    path: str,
    fields: tuple[str, ...],
    cursor_field: str,
    windows: list[dict[str, str]],
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """The shared walk — paginate every window, project, observe once."""
    client = _client(config)

    batch: list[dict] = []
    rows_seen = 0
    max_cursor: str | None = None

    for index, window in enumerate(windows, start=1):
        params: dict[str, Any] = {"limit": int(config.page_size)}
        params.update(window)
        if window:
            log.info(
                "chargebacks911.%s: window %d/%d %s → %s",
                stream_name,
                index,
                len(windows),
                window["start_date"],
                window["end_date"],
            )
        else:
            log.info(
                "chargebacks911.%s: full sweep (no date filter)", stream_name
            )

        for row in client.paginate(path, params):
            rows_seen += 1
            if rows_seen % 5000 == 0:
                log.info(
                    "chargebacks911.%s: paginated %d rows so far",
                    stream_name,
                    rows_seen,
                )
            value = row.get(cursor_field)
            if value is not None:
                text = str(value)
                if max_cursor is None or text > max_cursor:
                    max_cursor = text
            batch.append({name: row.get(name) for name in fields})
            if len(batch) >= _BATCH_SIZE:
                yield batch
                batch = []

    if batch:
        yield batch

    # Observe exactly once, at the very end, with the max cursor value
    # seen — see the module docstring for why.
    if max_cursor is not None:
        cursor.observe(max_cursor)
        log.info(
            "chargebacks911.%s: yielded %d rows, cursor advanced to %s",
            stream_name,
            rows_seen,
            max_cursor,
        )
    else:
        log.info(
            "chargebacks911.%s: yielded %d rows, no cursor values observed — "
            "cursor unchanged",
            stream_name,
            rows_seen,
        )


@stream(name="alerts")
def alerts(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Full sweep every run — the endpoint has no working date filter.

    ``dateUpdated`` is still observed so ``_dtex_state`` shows real
    freshness, but it does NOT narrow the request: it cannot. Cost is
    bounded by account size, not by run frequency, and ``merge`` on ``id``
    makes the repeat pull idempotent. If this account ever grows enough for
    the sweep to hurt, the fix is a smaller ``page_size`` plus a descending
    walk that stops early — not a date filter the server ignores.
    """
    yield from _extract(
        "alerts",
        f"/clients/{config.client_id}/alerts",
        _ALERT_FIELDS,
        "dateUpdated",
        [{}],
        config,
        cursor,
        log,
    )


@stream(name="chargebacks")
def chargebacks(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Incremental on ``date_updated``, walked in bounded chunks.

    This endpoint's date filter genuinely works; the constraint is that a
    wide window 503s. See ``_iter_windows``.
    """
    yield from _extract(
        "chargebacks",
        f"/clients/{config.client_id}/chargebacks",
        _CHARGEBACK_FIELDS,
        "date_updated",
        _iter_windows(
            cursor,
            int(config.lookback_days),
            int(config.window_chunk_days),
        ),
        config,
        cursor,
        log,
    )
