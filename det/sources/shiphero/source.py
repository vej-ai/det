"""ShipHero source — the @stream entry points the engine discovers.

One @stream per declared stream in ``register.yaml``. Each is a thin wrapper
around the shared :func:`extract_stream` helper, which owns the per-window +
per-page extraction logic. The split keeps each @stream a single-purpose
function whose signature the engine introspects (docs/03 §3.1 injectables) and
that an author can unit-test in isolation (the decorator returns the function
unchanged when no registration scope is open).

Architecture (port of v2/main.py `sync_table` lines 329-465, restructured for
det's per-batch contract):

    @stream(name=...) ──▶ extract_stream(name, config, cursor, log)
                              │
                              ├── ShipHeroClient(api_url, refresh_token, ...)
                              │     └── refresh() lazily, retry on 401/429/5xx
                              │
                              ├── compute_start(cursor.start_value(), lookback)
                              │     # connector owns lookback subtraction
                              │
                              └── for window in date_windows(start, step_days):
                                       for record in paginate(...):
                                           batch.append(record)
                                           cursor.observe(record[cursor_field])
                                           if len(batch) >= batch_size:
                                               yield batch
                                               batch = []
                                  if batch: yield batch  # final partial

# NOTE: @stream's injectables are a strict whitelist
# ``{config, state, cursor, log}`` (``det.registry.STREAM_INJECTABLES``).
# Each per-stream wrapper here uses *exactly* those names — passing
# ``stream_name`` to ``extract_stream`` is hard-coded in the wrapper, not
# injected.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

from det import Batch, Config, Cursor, stream
from det.sources.shiphero.client import ShipHeroClient
from det.sources.shiphero.pagination import paginate
from det.sources.shiphero.queries import (
    STREAM_GRAPHQL_FIELDS,
    STREAM_QUERIES,
)
from det.sources.shiphero.windows import (
    compute_start,
    date_windows,
    to_utc_dt,
)

# Per-stream cursor field — matches ``incremental.cursor_field`` in
# register.yaml. Declared here so the engine's API contract (a single map of
# injectables) stays unaware of stream-internal field names.
_CURSOR_FIELD: dict[str, str] = {
    "shipments": "created_date",
    "orders": "order_date",
    "products": "updated_at",
}

# Per-stream allow-list of column names — must match the ``schema[]`` block in
# register.yaml. The connector projects each API record onto these columns
# before yielding the batch; any field the API returned that the declared
# schema does not list is dropped here, so the destination's INSERT never
# trips on a phantom column.
#
# NOTE: kept in code (not pulled from the manifest) because the @stream body
# does not receive the declared schema as an injectable. Drift between this
# constant and register.yaml is caught by the discovery validator: a column
# named in register.yaml is required to be in the destination table, but a
# column present in this set but absent from register.yaml is harmless (the
# destination will infer it on the first batch). The harder failure mode is
# the reverse — a declared column that the API can't produce — and that does
# not concern this projection.
_PROJECT_COLS: dict[str, frozenset[str]] = {
    "shipments": frozenset({
        "id", "legacy_id", "order_id", "user_id", "warehouse_id",
        "pending_shipment_id", "shipped_off_shiphero", "dropshipment",
        "created_date", "shipping_labels", "line_items",
    }),
    "orders": frozenset({
        "id", "legacy_id", "order_number", "shop_name",
        "fulfillment_status", "order_date", "total_price", "subtotal",
        "total_tax", "email", "profile", "line_items",
    }),
    "products": frozenset({
        "id", "legacy_id", "sku", "name", "barcode", "price", "value",
        "created_at", "updated_at", "kit", "kit_build", "warehouse_products",
    }),
}


# --------------------------------------------------------------------------
# The shared extraction helper — plain Python, called by each @stream wrapper.
# --------------------------------------------------------------------------


def extract_stream(
    stream_name: str,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
) -> Iterator[Batch]:
    """Yield batches of records for one ShipHero stream — main.py 329-465.

    Generic across streams: the per-stream specifics (GraphQL query, GraphQL
    field name, cursor field) come from the ``stream_name`` lookup tables in
    this module and in ``queries.py``. The control flow is identical for every
    stream — that uniformity is what justifies having a single helper instead
    of three near-duplicate @stream bodies.

    Drives the lifecycle:

    1. Construct the client (lazy token acquisition on first ``query``).
    2. Compute the windowed start point from ``cursor.start_value()``, with
       the connector's lookback subtraction applied (the engine does NOT do
       this — see ``windows.py`` NOTE).
    3. For each ``(date_from, date_to)`` window, paginate the GraphQL query
       with cursor-based pagination.
    4. For each record, ``observe`` its cursor value and append to a batch.
    5. Yield the batch each ``config.batch_size`` records.
    """
    query = STREAM_QUERIES[stream_name]
    graphql_field = STREAM_GRAPHQL_FIELDS[stream_name]
    cursor_field = _CURSOR_FIELD[stream_name]
    project_cols = _PROJECT_COLS[stream_name]

    client = ShipHeroClient(
        api_url=str(config.api_url),
        refresh_token=config.secrets["refresh_token"],
        max_retries=int(config.max_retries),
        retry_backoff_seconds=float(config.retry_backoff_seconds),
        log=log,
    )

    # Two field paths into the GraphQL response shape:
    #   records:   data.<field>.data.edges[*].node
    #   page info: data.<field>.data.pageInfo  (the {hasNextPage, endCursor} block)
    field_path_records = ["data", graphql_field, "data", "edges", "*", "node"]
    field_path_pageinfo = ["data", graphql_field, "data", "pageInfo"]

    start_value = cursor.start_value()
    start = compute_start(
        start_value,
        initial_value=str(config.start_date),
        lookback_days=int(config.lookback_days),
    )
    log.info(
        "shiphero.%s: resume from %s (lookback %dd applied)",
        stream_name,
        start.isoformat(),
        int(config.lookback_days),
    )

    batch: list[dict[str, Any]] = []
    page_size = int(config.page_size)
    batch_size = int(config.batch_size)

    # Defined once; closure variables (`client`, `query`) are loop-invariant.
    def _fetch_page(variables: dict[str, Any]) -> dict[str, Any]:
        return client.query(query, variables)

    for win_from, win_to in date_windows(start, step_days=int(config.step_days)):
        log.info(
            "shiphero.%s: window %s..%s",
            stream_name,
            win_from.isoformat(),
            win_to.isoformat(),
        )

        extra = {
            "dateFrom": win_from.strftime("%Y-%m-%dT%H:%M:%S"),
            "dateTo": win_to.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        for raw_record in paginate(
            fetch_page=_fetch_page,
            page_size=page_size,
            field_path_to_records=field_path_records,
            field_path_to_pageinfo=field_path_pageinfo,
            extra_variables=extra,
        ):
            # Project onto the declared schema's column set — drops any field
            # the API returned that register.yaml does not list.
            record = {k: v for k, v in raw_record.items() if k in project_cols}
            value = record.get(cursor_field)
            if value is not None:
                # Observe a ``datetime`` (not its ISO string): the DuckDB
                # destination's ``_det_state.cursor_value`` is a ``JSON``
                # column, and DuckDB's JSON binding accepts ``datetime`` but
                # rejects an un-quoted bare string. ``to_utc_dt`` normalizes
                # both naive and aware datetimes (and ISO-8601 string inputs
                # from a replayed payload) into the one tz-aware UTC form, so
                # ``Cursor.observe``'s internal ``>`` comparison stays
                # total-orderable across records.
                normalized = to_utc_dt(value)
                if normalized is not None:
                    cursor.observe(normalized)
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch


# --------------------------------------------------------------------------
# @stream entry points — one per declared stream in register.yaml.
# --------------------------------------------------------------------------


@stream(name="shipments")
def shipments(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Extract ShipHero shipments incrementally — register.yaml streams[0].

    Thin wrapper. The engine inspects this function's signature and injects
    ``config`` / ``cursor`` / ``log`` by name (docs/03 §3.1); ``stream_name``
    is fixed here, not injected.
    """
    yield from extract_stream("shipments", config, cursor, log)


@stream(name="orders")
def orders(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Extract ShipHero orders incrementally — register.yaml streams[1]."""
    yield from extract_stream("orders", config, cursor, log)


@stream(name="products")
def products(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Extract ShipHero products incrementally — register.yaml streams[2]."""
    yield from extract_stream("products", config, cursor, log)


