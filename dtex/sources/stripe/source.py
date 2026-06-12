# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Stripe source — the ``@stream`` functions the engine discovers and runs.

The connector exposes TWO extraction surfaces in one source folder:

* **REST streams** (``charges`` / ``invoices`` / ``customers`` /
  ``subscriptions``) wrap :func:`_extract_stream`, which builds a
  :class:`StripeClient`, paginates the v1 REST endpoint, and yields each
  page as a batch. Cheap, GA, near-live data.

* **Sigma streams** (``charges_daily`` / ``subscriptions_active`` /
  ``invoices_paid``) wrap :func:`_extract_sigma_stream`, which builds a
  :class:`SigmaClient`, submits a Sigma SQL query (read from a .sql file
  whose path is declared on the stream's ``sigma:`` block in
  register.yaml), polls until the query run completes, downloads the CSV
  result, and yields batches of dicts. Sigma is a paid Stripe product
  with ~3-hour data lag; the upside is server-side SQL JOIN /
  aggregation across Stripe's internal tables, in one stream.

Each ``@stream`` function knows which surface it belongs to (REST or
Sigma) — the dispatch is by name + decorator, not a per-call branch.
The ``sigma:`` block in register.yaml is the metadata source of truth
for the Sigma path (the SQL filename); the function reads its own
:class:`StreamDef` (injected by the engine via ``stream_def``) and
loads the file from there. Adding a 4th Sigma stream is two lines: a
register.yaml entry with ``sigma: {query: queries/X.sql}`` and a
one-line ``@stream`` wrapper here.

docs/03 §3.1 — a ``@stream`` generator yields batches (``list[dict]``),
not single records; the engine handles per-batch loading and checkpointing.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dtex import Batch, Config, Cursor, StreamDef, stream
from dtex.sources.stripe.client import StripeClient
from dtex.sources.stripe.pagination import paginate
from dtex.sources.stripe.sigma_client import SigmaClient

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


# --------------------------------------------------------------------------
# Sigma SQL extraction — the second extraction surface (queries/*.sql)
# --------------------------------------------------------------------------

_SIGMA_BATCH_SIZE = 500
"""Records per yielded Sigma batch. The engine commits state after each batch
durably lands, so smaller batches = finer-grained recovery on failure but
more destination calls. 500 is a reasonable BigQuery load-job batch size."""

# Folder-relative paths in `sigma: {query: ...}` are resolved against the
# connector folder root — same convention every project-local connector
# uses for its sibling files.
_CONNECTOR_DIR = Path(__file__).parent

# `{name}` placeholder substitution in SQL bodies. Chosen over `:name`
# (Stripe's own preview syntax) because `:00` inside `00:00:00` collides
# with that form. No SQL dialect uses braces, so collision is impossible.
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


def _build_sigma_client(
    config: Config, log: logging.Logger | logging.LoggerAdapter[Any]
) -> SigmaClient:
    """Construct a :class:`SigmaClient` from `config` — single construction site."""
    return SigmaClient(
        base_url=str(config.get("sigma_base_url", "https://api.stripe.com")),
        api_key=config.secrets["api_key"],
        api_version=str(config.get("sigma_api_version", "2026-04-22.preview")),
        account_id=str(config.get("account_id") or ""),
        poll_interval_seconds=float(config.get("sigma_poll_interval_seconds", 2.0)),
        poll_timeout_seconds=int(config.get("sigma_poll_timeout_seconds", 600)),
        max_retries=int(config.get("max_retries", 5)),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.0)),
    )


def _extract_sigma_stream(
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor | None,
    log: logging.Logger | logging.LoggerAdapter[Any],
) -> Iterator[Batch]:
    """Shared Sigma extract — drives one SQL stream end to end.

    Reads the SQL body from `stream_def.sigma.query` (a path relative to
    the connector folder), substitutes `{since}` from the cursor floor
    (or the connector's `sigma_initial_since` fallback for non-incremental
    streams), submits the query to Sigma's Query Run API, streams the
    CSV result, batches records at 500, observes the cursor field per
    row, and yields each batch.
    """
    if stream_def.sigma is None:  # pragma: no cover — defensive guard
        raise RuntimeError(
            f"_extract_sigma_stream called on {stream_def.name!r} which has no "
            f"`sigma:` block in register.yaml"
        )
    sql_path = _CONNECTOR_DIR / stream_def.sigma.query
    sql_text = sql_path.read_text()
    sql_bound = _bind_sigma_placeholders(
        sql_text, since=_sigma_cursor_floor(cursor, config)
    )

    cursor_field = (
        stream_def.incremental.cursor_field if stream_def.incremental is not None else None
    )
    log.info(
        "stripe.sigma.%s: starting extract query=%s cursor_floor=%s",
        stream_def.name,
        stream_def.sigma.query,
        _sigma_cursor_floor(cursor, config),
    )

    client = _build_sigma_client(config, log)
    batch: list[dict[str, Any]] = []
    for row in client.run_query(sql_bound, log=log):
        # Sigma's CSV returns every cell as a string. The engine's NORMALIZE
        # step coerces against the stream's declared schema (timestamps
        # from ISO strings, ints from numeric strings, booleans), so we
        # pass the raw row through and just observe the cursor.
        if cursor is not None and cursor_field is not None:
            value = row.get(cursor_field)
            if value:
                cursor.observe(value)
        batch.append(row)
        if len(batch) >= _SIGMA_BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _sigma_cursor_floor(cursor: Cursor | None, config: Config) -> str:
    """Return the Presto TIMESTAMP literal to bind into `{since}`.

    Precedence:
      1. The stream's `cursor.start_value()` if it's an incremental stream
         with a value (covers both first-run `initial_value` and resumed
         state).
      2. The connector-level `sigma_initial_since` fallback for streams
         without a cursor (the SQL might still reference `{since}` for
         documentation or future-incremental use).

    Always returns a Presto-compatible ``YYYY-MM-DD HH:MM:SS`` literal
    body — Sigma rejects ISO-8601's `T` separator and timezone suffixes.
    """
    if cursor is not None:
        start = cursor.start_value()
        if start is not None:
            return _to_presto_timestamp(start)
    return _to_presto_timestamp(str(config.get("sigma_initial_since")))


def _to_presto_timestamp(value: Any) -> str:
    """Render any common timestamp shape as a Presto ``TIMESTAMP`` literal body.

    Presto's ``TIMESTAMP 'literal'`` form expects ``YYYY-MM-DD HH:MM:SS``
    (optionally with fractional seconds). It rejects the ``T`` separator,
    any timezone suffix (``Z``, ``+00:00``), and ``YYYY-MM-DDTHH:MM:SS.fffZ``
    JSON-style timestamps. Accepts datetime/date/ISO-8601 strings.
    """
    if isinstance(value, datetime):
        dt = value.astimezone(UTC) if value.tzinfo else value
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return f"{value.isoformat()} 00:00:00"
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1]
    if len(text) >= 6 and text[-6] in "+-" and text[-3] == ":":
        text = text[:-6]
    text = text.replace("T", " ")
    if "." in text:
        text = text.split(".", 1)[0]
    return text


def _bind_sigma_placeholders(sql: str, *, since: str) -> str:
    """Substitute `{name}` placeholders in `sql` with safely-quoted literals.

    Only `{since}` is supported — it becomes a single-quoted SQL string
    literal. Unknown placeholders raise immediately (better than Sigma
    failing with a parse error 10 minutes into a polled query run).
    """
    bindings = {"since": "'" + since.replace("'", "''") + "'"}

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        try:
            return bindings[name]
        except KeyError as exc:
            raise KeyError(
                f"sigma: SQL placeholder '{{{name}}}' is not bound. "
                f"Supported placeholders: {sorted(bindings)}"
            ) from exc

    return _PLACEHOLDER_RE.sub(replace, sql)


# --------------------------------------------------------------------------
# Sigma @stream functions — one per declared Sigma stream in register.yaml.
# --------------------------------------------------------------------------
#
# Each is a one-line wrapper over :func:`_extract_sigma_stream`. The function
# names match register.yaml's ``streams[].name`` — the engine routes by name.
# Adding a 4th Sigma stream is: a register entry with ``sigma: {query: ...}``,
# a `.sql` file under queries/, and a one-line wrapper here.


@stream(name="charges_daily")
def charges_daily(
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract every Stripe charge via Sigma SQL — see :func:`_extract_sigma_stream`."""
    yield from _extract_sigma_stream(stream_def, config, cursor, log)


@stream(name="subscriptions_active")
def subscriptions_active(
    stream_def: StreamDef,
    config: Config,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract currently-active subscriptions via Sigma SQL (full-refresh).

    No ``cursor`` arg — the stream has no ``incremental:`` block in
    register.yaml, so the engine doesn't inject one. Every run pulls the
    full set; ``write_disposition: replace`` cleans the prior load.
    """
    yield from _extract_sigma_stream(stream_def, config, cursor=None, log=log)


@stream(name="invoices_paid")
def invoices_paid(
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Extract paid invoices via Sigma SQL — see :func:`_extract_sigma_stream`."""
    yield from _extract_sigma_stream(stream_def, config, cursor, log)
