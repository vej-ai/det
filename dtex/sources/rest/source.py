# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Generic REST source — the @stream functions the engine discovers and runs.

This module pairs with ``register.yaml``. The contract (docs/03 §3.1, §7 rule 7)
requires one ``@stream`` function per declared stream — a *generic* connector
implements that by giving every declared stream a tiny ``@stream(name="X")``
wrapper that just forwards to :func:`extract_stream` with its endpoint and
extraction params.

Adding a stream to a Generic REST connector is therefore two coordinated edits:

1. Declare it under ``streams:`` in ``register.yaml`` (name / table /
   primary_key / write_disposition / incremental / schema). The YAML carries
   only what the dtex contract supports.
2. Write a ``@stream(name="X")`` function here that calls
   :func:`extract_stream` with the per-stream endpoint / record_path /
   pagination params. These are Python literals (lists, dicts) — they cannot
   live in ``register.yaml`` because :class:`~dtex.types.ParamSpec` is
   restricted to ``string|int|float|bool`` scalars.

# NOTE: per CONTRIBUTING.md "code is source of truth" — the task brief
# proposed putting per-stream API config (``record_path: ["data","items"]``,
# ``extra_query_params: {...}``) under ``streams[].params`` in YAML. The
# contract type :class:`~dtex.types.ParamSpec` only accepts scalar values,
# so a list/dict there would fail discovery before the connector ever runs.
# Modifying ``types.py`` is off-limits for connector builders. The strongest
# alternative — adopted here — is to keep per-stream API config in Python: it
# is testable in isolation, type-checked by mypy, and does not bend the YAML
# contract. The README explains the pattern.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from dtex import Batch, Config, Cursor, stream
from dtex.sources.rest.client import AuthSpec, build_client
from dtex.sources.rest.extractors import extract_records
from dtex.sources.rest.pagination import build_strategy


def extract_stream(
    *,
    config: Config,
    log: logging.Logger,
    cursor: Cursor | None = None,
    endpoint: str,
    record_path: Sequence[str],
    cursor_query_param: str | None = None,
    next_cursor_path: str | None = None,
    extra_query_params: Mapping[str, Any] | None = None,
    page_size_param: str | None = None,
    pagination_strategy: str | None = None,
) -> Iterator[Batch]:
    """The shared driver: paginate ``endpoint``, extract records, yield batches.

    Every ``@stream`` function in this module calls this with its own endpoint
    and extraction params. The driver:

    1. Builds the HTTP client from connector-level config (``base_url``, auth,
       retries, rate limit) — once per stream invocation, so the session +
       auth headers are paid once.
    2. Builds the pagination strategy from connector-level
       ``pagination_strategy`` (overridable per-stream via the kwarg).
    3. Iterates pages: send → :func:`extract_records` from the JSON body using
       ``record_path`` → on incremental streams, ``cursor.observe`` each
       record's cursor field → yield the page as one batch → ask the strategy
       for the next page's params.
    4. For incremental streams, sends the cursor's
       :meth:`~dtex.types.Cursor.start_value` as
       ``cursor_query_param=<value>`` on the *first* page. Pagination then takes
       over the query mechanics.

    Yields one batch per HTTP page — the engine load-streams (batches are
    durably written one-by-one), so a large extraction never buffers more than
    one page worth of records in memory.

    ``cursor`` is ``None`` for non-incremental streams; the engine omits it
    from injection. ``cursor_query_param`` may also be ``None`` for an
    incremental stream whose API does not take a delta param — the cursor is
    then used only to mark progress, with re-extraction relying on the
    destination's ``merge`` disposition for deduplication.
    """
    auth = AuthSpec(
        auth_type=config.get("auth_type", "none"),
        token=config.secrets.get("api_token", ""),
        header_name=config.get("auth_header_name", "Authorization"),
        query_param=config.get("auth_query_param", "api_key"),
    )
    client = build_client(
        base_url=str(config.base_url),
        auth=auth,
        max_retries=int(config.get("max_retries", 5)),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.0)),
        requests_per_second=float(config.get("requests_per_second", 0)),
        timeout_seconds=float(config.get("timeout_seconds", 30.0)),
        log=log,
    )

    strategy_name = pagination_strategy or str(
        config.get("pagination_strategy", "cursor")
    )
    page_size = int(config.get("page_size", 100))
    strategy = build_strategy(
        strategy_name,
        base_url=str(config.base_url).rstrip("/") + "/" + endpoint.lstrip("/"),
        page_size=page_size,
        record_path=tuple(record_path),
        cursor_query_param=cursor_query_param,
        next_cursor_path=next_cursor_path,
        page_size_param=page_size_param,
    )

    # Seed the initial query: caller's static extras, then (for an incremental
    # stream) the cursor's start value as the configured query parameter. Pages
    # 2..N never carry the start-value param — pagination state takes over.
    initial: dict[str, Any] = dict(extra_query_params or {})
    if cursor is not None and cursor_query_param and cursor.start_value() is not None:
        initial[cursor_query_param] = _serialize_cursor_value(cursor.start_value())

    params = strategy.prepare_first(initial)
    cursor_field = cursor.cursor_field if cursor is not None else None
    page_number = 0
    # Defence-in-depth cap: if a misbehaving API + strategy interact in a way
    # that never converges to ``None``, fail the run loud instead of hanging it.
    # 1e6 pages × `page_size`100 = 100M records — well past any real workload.
    max_pages = 1_000_000

    while params is not None:
        page_number += 1
        if page_number > max_pages:
            raise RuntimeError(
                f"REST source: refusing to fetch more than {max_pages} pages "
                f"from {endpoint!r} — pagination is not converging"
            )
        try:
            response = client.get(endpoint, params=params)
            payload = response.json()
        except ValueError as exc:
            # `response.json()` raises ``ValueError`` on a non-JSON body — make
            # the message actionable (which endpoint, which page).
            raise RuntimeError(
                f"REST source: page {page_number} of {endpoint!r} returned "
                f"non-JSON content (Content-Type may be wrong): {exc}"
            ) from exc

        records = extract_records(payload, list(record_path))
        if cursor is not None and cursor_field is not None:
            for record in records:
                value = record.get(cursor_field)
                if value is not None:
                    cursor.observe(value)
        if records:
            # An empty page is yielded as zero rows — no point handing the
            # destination an empty list (its write_batch is a no-op anyway).
            yield records

        params = strategy.update_after(payload, response.headers, params)


def _serialize_cursor_value(value: Any) -> Any:
    """Render a cursor value into a JSON/query-string friendly form.

    Most APIs accept the cursor value verbatim (a string token, an int id, an
    ISO 8601 timestamp). Python ``datetime``/``date`` values are not natively
    URL-encodable; render them as ISO 8601 here so the connector author does
    not have to remember to do it in every ``@stream``.
    """
    # Imports are local — keep the module import surface flat.
    from datetime import date, datetime

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


# --------------------------------------------------------------------------
# Stream functions — one per declared streams[] entry in register.yaml
# --------------------------------------------------------------------------
# The pattern:
#   @stream(name="<matches register.yaml>")
#   def <name>(config, cursor, log):
#       yield from extract_stream(
#           config=config,
#           cursor=cursor,
#           log=log,
#           endpoint="<path under base_url>",
#           record_path=["<path>", "<to>", "<records>"],
#           cursor_query_param="<query param to send the cursor as>",
#           next_cursor_path="<dotted JSON path to next-page cursor>",
#           extra_query_params={"static": "params"},
#       )
# A non-incremental stream omits ``cursor`` from its signature.


@stream(name="items")
def items(
    config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Extract items incrementally with cursor pagination.

    Example wiring against an API whose ``/items`` endpoint returns
    ``{"data": [...], "meta": {"next_cursor": "..."}}`` and supports an
    ``updated_since`` query param.
    """
    yield from extract_stream(
        config=config,
        cursor=cursor,
        log=log,
        endpoint="/items",
        record_path=["data"],
        cursor_query_param="updated_since",
        next_cursor_path="meta.next_cursor",
    )


@stream(name="events")
def events(config: Config, log: logging.Logger) -> Iterator[Batch]:
    """Extract events with cursor pagination — non-incremental.

    Example wiring against an API whose ``/events`` endpoint returns
    ``{"data": [...], "meta": {"next_cursor": "..."}}``. Re-runs re-fetch
    everything (no cursor); use ``write_disposition: merge`` on a stable id if
    you want at-most-once semantics, or accept duplicates with ``append``.
    """
    yield from extract_stream(
        config=config,
        log=log,
        endpoint="/events",
        record_path=["data"],
        cursor_query_param="cursor",
        next_cursor_path="meta.next_cursor",
    )
