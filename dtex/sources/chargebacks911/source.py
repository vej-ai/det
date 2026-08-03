"""Chargebacks911 CBAPIv2 source — two @stream functions.

Both streams share one shape:

1. Compute the pull window: ``start_date = (cursor date − lookback_days)``,
   ``end_date = today``. The cursor value is a STRING date (ISO-shaped, so
   string ordering is chronological). On a virgin run the engine hands back
   ``None`` (the streams deliberately declare no ``initial_value``) and the
   pull goes out UNFILTERED — CB911's server times out (503) computing wide
   ``date_column`` windows, so the bootstrap must be the full sweep.
2. On incremental runs, pass the endpoint's server-side filter —
   ``date_column=date_updated&start_date=...&end_date=...`` — on the first
   request; the client repeats it on every page.
3. Paginate with ``limit`` + ``page`` until a short page.
4. Project each row onto the declared schema (phantom API fields never
   reach the destination), tracking the max cursor value seen.
5. ``cursor.observe(...)`` exactly ONCE at the very end, with that max.
   Observing per-row would be equivalent (the engine keeps the max), but
   the single observe at the end makes the commit semantics explicit:
   nothing advances unless the full walk completed and the final batch
   landed.

The lookback overlap + ``write_disposition: merge`` on ``id`` make the
re-pulled window idempotent — late updates inside the window upsert.

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


def _date_params(cursor: Cursor, lookback_days: int) -> dict[str, str]:
    """The server-side date filter for one incremental run.

    ``start_date`` is the cursor value's DATE part minus ``lookback_days``
    (cursor values are STRING dates, possibly with a time component —
    the first 10 chars are the ISO date). ``end_date`` is today (UTC).
    Under ``--full-refresh`` the cursor has no start value and the filter
    is omitted entirely — a full pull.
    """
    start_value = cursor.start_value()
    if start_value is None:
        # First-ever run (no persisted state; the streams declare no
        # initial_value) or --full-refresh: pull WITHOUT date filters.
        # CB911's server 503s computing wide date_column windows — the
        # unfiltered sweep is the only bootstrap that works (verified live
        # 2026-08-03).
        return {}
    start = date.fromisoformat(str(start_value)[:10]) - timedelta(days=lookback_days)
    return {
        "date_column": "date_updated",
        "start_date": start.isoformat(),
        "end_date": datetime.now(tz=UTC).date().isoformat(),
    }


def _extract(
    stream_name: str,
    path: str,
    fields: tuple[str, ...],
    cursor_field: str,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """The shared walk — both streams differ only in path + projection."""
    client = _client(config)
    params: dict[str, Any] = {"limit": int(config.page_size)}
    date_params = _date_params(cursor, int(config.lookback_days))
    params.update(date_params)

    log.info(
        "chargebacks911.%s: window start_date=%s end_date=%s (lookback=%sd)",
        stream_name,
        date_params.get("start_date", "<full refresh>"),
        date_params.get("end_date", "<full refresh>"),
        config.lookback_days,
    )

    batch: list[dict] = []
    rows_seen = 0
    max_cursor: str | None = None
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
    yield from _extract(
        "alerts",
        f"/clients/{config.client_id}/alerts",
        _ALERT_FIELDS,
        "dateUpdated",
        config,
        cursor,
        log,
    )


@stream(name="chargebacks")
def chargebacks(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    yield from _extract(
        "chargebacks",
        f"/clients/{config.client_id}/chargebacks",
        _CHARGEBACK_FIELDS,
        "date_updated",
        config,
        cursor,
        log,
    )
