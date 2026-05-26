"""Stripe source — the ``@stream`` functions the engine discovers and runs.

One ``@stream`` per declared resource (``charges`` / ``invoices`` /
``customers`` / ``subscriptions``). Each is a thin wrapper over
:func:`_extract_stream`, the shared helper that:

1. builds a :class:`StripeClient` from ``config`` and ``config.secrets["api_key"]``;
2. translates the engine-supplied ``Cursor`` into a ``created[gte]=<ts>`` filter
   (omitting it on the first run / under ``--full-refresh``);
3. walks pages via :func:`paginate` — one ``list[dict]`` page per HTTP request;
4. for every record calls ``cursor.observe(record["created"])`` so the engine
   tracks the new high-water Unix timestamp;
5. yields each page as a batch — Stripe's page IS the natural batch size.

The v1 model is **resource-as-stream**, NOT Sigma query-as-stream — see
``docs/connectors/stripe-research.md`` §"Recommended connector design" for
the rationale and the locked decision. The Sigma SQL surface is preview +
paid + 3h-lagged; it is deferred to v2.

docs/03 §3.1 — a ``@stream`` generator yields batches (``list[dict]``), not
single records; the engine handles per-batch loading and checkpointing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping
from typing import Any

from detx import Batch, Config, Cursor, stream
from detx.sources.stripe.client import StripeClient
from detx.sources.stripe.pagination import paginate

# --------------------------------------------------------------------------
# Stream-name → endpoint resolution
# --------------------------------------------------------------------------
#
# The stream-level ``params.endpoint`` declared in register.yaml IS the
# source of truth (so an author can override per stream). This mapping is a
# fallback default used by tests and by sanity-checks here.
_DEFAULT_ENDPOINTS: dict[str, str] = {
    "charges": "/charges",
    "invoices": "/invoices",
    "customers": "/customers",
    "subscriptions": "/subscriptions",
}


def _build_client(
    config: Config, log: logging.Logger | logging.LoggerAdapter[Any]
) -> StripeClient:
    """Construct a :class:`StripeClient` from ``config`` — single construction site.

    All the connector-level params declared in ``register.yaml`` flow through
    here. ``config.secrets["api_key"]`` is the restricted Stripe key resolved
    by the engine; the client takes it once, places it on the Session header,
    and the key never appears in a log line again.
    """
    api_key = config.secrets["api_key"]
    return StripeClient(
        base_url=str(config.get("base_url", "https://api.stripe.com/v1")),
        api_key=api_key,
        api_version=str(config.get("api_version", "2024-12-18.acacia")),
        page_size=int(config.get("page_size", 100)),
        max_retries=int(config.get("max_retries", 5)),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.0)),
        requests_per_second=float(config.get("requests_per_second", 25.0)),
        timeout_seconds=float(config.get("timeout_seconds", 30.0)),
        log=log,
    )


def _decode_extra_query(raw: str | None) -> Mapping[str, Any]:
    """Decode the optional ``extra_query_params_json`` stream-scoped param.

    ParamSpec types are restricted to ``string|int|float|bool`` (docs/03 §2.4),
    so an "optional dict" param has to ride along as a JSON-encoded string.
    Empty / None means "no extras". A malformed value raises immediately —
    silent fallback would mask a typo'd Stripe ``expand[]`` filter.
    """
    if raw is None or raw == "":
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(
            f"extra_query_params_json must encode a JSON object, got {type(parsed).__name__}"
        )
    return parsed


def _extract_stream(
    stream_name: str,
    config: Config,
    cursor: Cursor,
    log: logging.Logger | logging.LoggerAdapter[Any],
) -> Iterator[Batch]:
    """Shared extract helper — drives one Stripe resource end to end.

    Every ``@stream`` function in this module is a four-line wrapper over
    this helper; the actual extract logic lives here so each resource gets the
    same pagination, cursor handling, and rate-limit semantics for free.

    Cursor semantics — docs/03 §3.2:

    * ``cursor.start_value()`` returns the last committed Unix timestamp (an
      ``int`` since ``cursor_type: int``), the typed ``initial_value`` on the
      first run, or ``None`` under ``--full-refresh``;
    * when present, it becomes the ``created[gte]=<ts>`` filter so Stripe
      returns only records created at or after the resume point;
    * when ``None``, the filter is omitted and the run is a full extract.

    Records are yielded one page per ``yield`` — Stripe's pages ARE the
    natural batch size (~100 records). Memory stays bounded; the engine
    can ``write_batch`` each page independently.

    Late-arriving rows: Stripe lists newest-first within the
    ``created[gte]`` window. With ``cursor.observe()`` taking the *max*, the
    final cursor is correct regardless of arrival order — and the engine
    commits the cursor only after every batch durably lands (docs/03 §3.2),
    so a crash mid-stream safely re-runs the whole window with no lost rows.
    """
    endpoint = str(config.get("endpoint") or _DEFAULT_ENDPOINTS.get(stream_name) or "")
    if not endpoint:
        raise ValueError(
            f"stripe: stream {stream_name!r} has no resolved endpoint — "
            f"declare `params.endpoint` in register.yaml"
        )

    extras = _decode_extra_query(config.get("extra_query_params_json"))

    page_size = int(config.get("page_size", 100))
    base_query: dict[str, Any] = {"limit": page_size}

    start_value = cursor.start_value()
    if start_value is not None:
        # ``cursor_type: int`` ⇒ start_value is already a Unix timestamp.
        # `created[gte]` is Stripe's documented incremental filter
        # (research note §B "Incremental filter").
        base_query["created[gte]"] = int(start_value)

    base_query.update(dict(extras))

    log.info(
        "stripe.%s: starting extract endpoint=%s page_size=%d start=%s%s",
        stream_name,
        endpoint,
        page_size,
        start_value,
        " (full_refresh)" if cursor.is_full_refresh else "",
    )

    pages = 0
    records = 0
    with _build_client(config, log) as client:
        for page in paginate(client, endpoint, base_query, log=log):
            pages += 1
            records += len(page)
            for record in page:
                created = record.get("created")
                if created is not None:
                    cursor.observe(int(created))
            yield page

    log.info(
        "stripe.%s: extract complete pages=%d records=%d",
        stream_name,
        pages,
        records,
    )


# --------------------------------------------------------------------------
# @stream functions — one per declared resource.
# --------------------------------------------------------------------------
#
# Each is the thinnest possible wrapper; the engine discovers them by name
# via the @stream decorator, matched against ``streams[].name`` in
# register.yaml. See docs/03 §3.1.


@stream(name="charges")
def charges(
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract ``/v1/charges`` incrementally — see :func:`_extract_stream`."""
    yield from _extract_stream("charges", config, cursor, log)


@stream(name="invoices")
def invoices(
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract ``/v1/invoices`` incrementally — see :func:`_extract_stream`."""
    yield from _extract_stream("invoices", config, cursor, log)


@stream(name="customers")
def customers(
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract ``/v1/customers`` incrementally — see :func:`_extract_stream`."""
    yield from _extract_stream("customers", config, cursor, log)


@stream(name="subscriptions")
def subscriptions(
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract ``/v1/subscriptions`` incrementally — see :func:`_extract_stream`."""
    yield from _extract_stream("subscriptions", config, cursor, log)
