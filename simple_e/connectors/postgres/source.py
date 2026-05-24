"""Postgres source — ``@stream`` functions, one per declared example stream.

Each ``@stream`` function is a thin wrapper that delegates to a shared
extraction helper. The helper opens a connection lazily on first iteration,
paginates by keyset (``WHERE cursor_field > %s ORDER BY cursor_field, pk
LIMIT batch_size``) for incremental table streams, by ``DECLARE … CURSOR`` /
``FETCH FORWARD`` for non-incremental full scans, and by a wrapped subquery
for ``query`` mode — yielding ``batch_size`` records per ``yield`` and closing
the connection when the generator is exhausted or raises.

Per-stream Postgres details
---------------------------

# NOTE: per-stream knobs (``schema_name``, ``table_name``, ``query``,
# ``cursor_field``, ``primary_key``) are *hardcoded constants* per ``@stream``
# function below, NOT YAML ``params``. The reason is contract-level: the
# engine (simple_e/engine/config.py::build_config) constructs a single
# connector-level :class:`~simple_e.types.Config` and injects only that into a
# ``@stream`` function. The manifest parser accepts ``stream_def.params`` but
# the runner never merges it into the per-call Config. So per-stream
# configuration *must* live in code if the connector wants it. This matches
# the ShipHero example (docs/04): ``GRAPHQL_QUERY`` and ``FIELD_PATH`` live in
# ``schema.py`` / ``client.py``, not in YAML.
#
# The contract fields the engine *does* read from YAML — ``name``, ``table``,
# ``primary_key``, ``write_disposition``, ``incremental``, ``schema`` — are
# in ``register.yaml`` as required.

Cursor injection rules
----------------------

Both example streams declare an ``incremental`` block in ``register.yaml``, so
the engine injects a :class:`~simple_e.types.Cursor` (docs/03 §3.2). Both
``@stream`` signatures therefore uniformly declare ``(config, cursor, log)``.
For a non-incremental Postgres stream the YAML would simply omit
``incremental:`` and the corresponding ``@stream`` function would drop
``cursor`` from its signature — the engine then does not inject one and
:func:`_extract_table` is told to take the full-scan path.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from simple_e import Batch, Config, Cursor, stream
from simple_e.connectors.postgres.client import (
    close_cursor_sql,
    connect,
    declare_cursor_sql,
    fetch_forward_sql,
    full_scan_select_sql,
    keyset_select_sql,
    query_select_sql,
)

if TYPE_CHECKING:
    import psycopg


# ---------------------------------------------------------------------------
# The two example @stream functions — thin wrappers over the shared extractor
# ---------------------------------------------------------------------------


@stream(name="users")
def users(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Extract ``public.users`` incrementally on ``updated_at``.

    Declares ``incremental`` in ``register.yaml`` — so ``cursor`` is injected
    (docs/03 §3.2). Yields batches of ``config.batch_size`` records.
    """
    yield from extract_stream(
        stream_name="users",
        config=config,
        cursor=cursor,
        log=log,
        schema_name="public",
        table_name="users",
        cursor_field="updated_at",
        primary_key=("id",),
    )


@stream(name="events")
def events(config: Config, cursor: Cursor, log: logging.Logger) -> Iterator[Batch]:
    """Extract ``public.events`` incrementally on ``occurred_at``."""
    yield from extract_stream(
        stream_name="events",
        config=config,
        cursor=cursor,
        log=log,
        schema_name="public",
        table_name="events",
        cursor_field="occurred_at",
        primary_key=("id",),
    )


# ---------------------------------------------------------------------------
# The shared extractor — every @stream above is a thin call to this
# ---------------------------------------------------------------------------


def extract_stream(
    *,
    stream_name: str,
    config: Config,
    cursor: Cursor | None,
    log: logging.Logger,
    schema_name: str | None = None,
    table_name: str | None = None,
    query: str | None = None,
    cursor_field: str | None = None,
    primary_key: tuple[str, ...] = (),
) -> Iterator[Batch]:
    """Dispatch one stream's extraction to the right path and yield batches.

    Three mutually-exclusive shapes:

    * ``table_name`` + ``cursor`` (and ``cursor_field``) — keyset
      incremental table read. The fastest, most resumable shape.
    * ``query`` + ``cursor`` (and ``cursor_field``) — incremental over an
      author-written ``SELECT``. The query is wrapped as a subquery and the
      keyset WHERE / ORDER BY / LIMIT is applied around it.
    * ``table_name`` and *no* ``cursor`` — full-table scan via a server-side
      ``DECLARE … CURSOR`` / ``FETCH FORWARD`` loop. The cursor is None
      because the YAML stream omitted ``incremental:`` and the engine did not
      inject one (docs/03 §3.2).

    # NOTE: ``table_name`` and ``query`` are *mutually exclusive*. The
    # manifest parser cannot enforce this (the values live in source.py, not
    # YAML — see this module's docstring), so the check is enforced here at
    # the start of every call, with a clear message naming the offending
    # stream. ``query`` mode is incremental-only — a full-scan ``query``
    # stream is not supported because we'd have no PK / no ordering to
    # paginate by.
    """
    if table_name is None and query is None:
        raise ValueError(
            f"postgres stream {stream_name!r}: exactly one of table_name / query is required"
        )
    if table_name is not None and query is not None:
        raise ValueError(
            f"postgres stream {stream_name!r}: table_name and query are mutually exclusive"
        )

    if query is not None:
        if cursor is None or cursor_field is None:
            raise ValueError(
                f"postgres stream {stream_name!r}: 'query' mode is incremental-only — "
                f"declare an `incremental:` block and pass cursor + cursor_field"
            )
        yield from _extract_query(
            config=config, cursor=cursor, log=log, query=query, cursor_field=cursor_field,
            stream_name=stream_name,
        )
        return

    # table_name mode.
    assert table_name is not None
    if schema_name is None:
        schema_name = "public"

    if cursor is None:
        yield from _extract_full_scan(
            config=config, log=log, schema_name=schema_name, table_name=table_name,
            stream_name=stream_name,
        )
    else:
        if cursor_field is None:
            raise ValueError(
                f"postgres stream {stream_name!r}: incremental table read requires cursor_field"
            )
        yield from _extract_table_keyset(
            config=config, cursor=cursor, log=log, schema_name=schema_name,
            table_name=table_name, cursor_field=cursor_field, primary_key=primary_key,
            stream_name=stream_name,
        )


# ---------------------------------------------------------------------------
# The three extraction paths
# ---------------------------------------------------------------------------


def _extract_table_keyset(
    *,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
    schema_name: str,
    table_name: str,
    cursor_field: str,
    primary_key: tuple[str, ...],
    stream_name: str,
) -> Iterator[Batch]:
    """Keyset-paginate ``schema.table`` on ``cursor_field`` (+ PK) — incremental.

    The loop: bind the current floor to ``%s``, fetch up to ``batch_size``
    rows, yield them as a :class:`Batch`, advance the floor to the last
    cursor value seen. Terminates when a page comes back smaller than
    ``batch_size``.

    # NOTE: ``cursor.observe`` is called for every row whose ``cursor_field``
    # value is non-null. ``Cursor`` ignores ``None`` (see types.py), so a row
    # without the cursor field never drags the cursor backward.
    """
    batch_size = int(config.batch_size)
    floor: Any = cursor.start_value()
    sql_stmt = keyset_select_sql(schema_name, table_name, cursor_field, primary_key)
    log.info(
        "postgres %s: keyset table=%s.%s cursor_field=%s start=%r batch_size=%d",
        stream_name, schema_name, table_name, cursor_field, floor, batch_size,
    )

    with _with_connection(config) as conn, conn.cursor() as cur:
        while True:
            cur.execute(sql_stmt, (floor, batch_size))
            column_names = [d.name for d in (cur.description or [])]
            rows = cur.fetchall()
            if not rows:
                break
            batch: Batch = []
            for row in rows:
                record = dict(zip(column_names, row, strict=False))
                cval = record.get(cursor_field)
                cursor.observe(cval)
                batch.append(record)
            yield batch
            if len(rows) < batch_size:
                break
            # Advance the floor to the strictly-greater cursor of the last
            # row — the next page resumes from there. ``ORDER BY cursor, pk``
            # ensures monotone non-decreasing cursor values.
            new_floor = batch[-1].get(cursor_field)
            if new_floor is None or new_floor == floor:
                # No forward progress — typically a column that is entirely
                # NULL or a contract violation. Bail out so we don't loop.
                log.warning(
                    "postgres %s: cursor did not advance past %r — stopping",
                    stream_name, floor,
                )
                break
            floor = new_floor


def _extract_query(
    *,
    config: Config,
    cursor: Cursor,
    log: logging.Logger,
    query: str,
    cursor_field: str,
    stream_name: str,
) -> Iterator[Batch]:
    """Run a wrapped ``query`` incrementally — keyset-paginated subquery."""
    batch_size = int(config.batch_size)
    floor: Any = cursor.start_value()
    sql_stmt = query_select_sql(query, cursor_field)
    log.info(
        "postgres %s: query-mode cursor_field=%s start=%r batch_size=%d",
        stream_name, cursor_field, floor, batch_size,
    )

    with _with_connection(config) as conn, conn.cursor() as cur:
        while True:
            cur.execute(sql_stmt, (floor, batch_size))
            column_names = [d.name for d in (cur.description or [])]
            rows = cur.fetchall()
            if not rows:
                break
            batch: Batch = []
            for row in rows:
                record = dict(zip(column_names, row, strict=False))
                cval = record.get(cursor_field)
                cursor.observe(cval)
                batch.append(record)
            yield batch
            if len(rows) < batch_size:
                break
            new_floor = batch[-1].get(cursor_field)
            if new_floor is None or new_floor == floor:
                log.warning(
                    "postgres %s: cursor did not advance past %r — stopping",
                    stream_name, floor,
                )
                break
            floor = new_floor


def _extract_full_scan(
    *,
    config: Config,
    log: logging.Logger,
    schema_name: str,
    table_name: str,
    stream_name: str,
) -> Iterator[Batch]:
    """Stream every row of ``schema.table`` via a server-side cursor.

    Used when the YAML stream declares NO ``incremental:`` block — the engine
    injects no :class:`Cursor`, and the connector must read the whole table
    in batches without loading it all into memory. Postgres server-side
    cursors require a transaction; we open one explicitly with
    ``conn.transaction()``.
    """
    batch_size = int(config.batch_size)
    cursor_name = f"_simple_e_pg_{uuid.uuid4().hex[:12]}"
    body = full_scan_select_sql(schema_name, table_name)
    declare_stmt = declare_cursor_sql(cursor_name, body)
    fetch_stmt = fetch_forward_sql(cursor_name, batch_size)
    close_stmt = close_cursor_sql(cursor_name)
    log.info(
        "postgres %s: full-scan table=%s.%s server_cursor=%s batch_size=%d",
        stream_name, schema_name, table_name, cursor_name, batch_size,
    )

    with _with_connection(config) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(declare_stmt)
                try:
                    while True:
                        cur.execute(fetch_stmt)
                        column_names = [d.name for d in (cur.description or [])]
                        rows = cur.fetchall()
                        if not rows:
                            break
                        batch: Batch = [
                            dict(zip(column_names, row, strict=False)) for row in rows
                        ]
                        yield batch
                        if len(rows) < batch_size:
                            break
                finally:
                    cur.execute(close_stmt)


# ---------------------------------------------------------------------------
# Connection lifecycle — lazy open, deterministic close
# ---------------------------------------------------------------------------


class _ConnectionContext:
    """Context manager that opens a Postgres connection on enter and closes on exit.

    Trivially expressible as ``contextlib.contextmanager``, but a small class
    is easier to type and easier to substitute in tests (a fake context can
    yield a fake connection without touching ``contextmanager``).
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._conn: psycopg.Connection[Any] | None = None

    def __enter__(self) -> psycopg.Connection[Any]:
        self._conn = connect(self._config)
        return self._conn

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None


def _with_connection(config: Config) -> _ConnectionContext:
    """Return a context manager that opens / closes a Postgres connection.

    Used by every extractor above so the connection lifecycle matches the
    generator lifecycle: opened on the first ``yield from`` iteration, closed
    when the generator finishes or raises.
    """
    return _ConnectionContext(config)
