# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""CockroachDB extraction machinery — decorator-free, importable by projects.

Everything the ``@stream`` functions in :mod:`source` delegate to lives here:
:func:`extract_stream` and the four read paths (PK-keyset bootstrap, cursor
keyset, wrapped query, server-side-cursor full scan), plus the state-scalar
round-tripping helpers and the connection-lifecycle context.

# NOTE: this module exists *separately from* ``source.py`` so that a
# project-local connector can reuse the machinery without side effects.
# Importing ``source`` executes its example ``@stream`` decorators, and inside
# a project connector's registration scope those examples would register as
# the project's streams — a validation error ("@stream has no matching
# streams[] entry"). Import THIS module from project connectors:
#
#     from dtex.sources.cockroachdb.extract import extract_stream
#
# See the ``source`` module docstring for the read-path strategy rationale.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from dtex import Batch, Config, Cursor, CursorType, State
from dtex.sources.cockroachdb.client import (
    close_cursor_sql,
    connect,
    declare_cursor_sql,
    fetch_forward_sql,
    full_scan_select_sql,
    keyset_select_sql,
    pk_keyset_select_sql,
    query_select_sql,
    set_transaction_aost_sql,
)

if TYPE_CHECKING:
    import psycopg

# State keys used by the bootstrap path — see the ``source`` module docstring.
_STATE_BOOTSTRAPPED = "bootstrapped"
_STATE_BOOTSTRAP_LAST_PK = "bootstrap_last_pk"
_STATE_BOOTSTRAP_CURSOR_MAX = "bootstrap_cursor_max"


# ---------------------------------------------------------------------------
# The shared extractor — every @stream above is a thin call to this
# ---------------------------------------------------------------------------


def extract_stream(
    *,
    stream_name: str,
    config: Config,
    state: State | None = None,
    cursor: Cursor | None = None,
    log: logging.Logger,
    schema_name: str | None = None,
    table_name: str | None = None,
    query: str | None = None,
    cursor_field: str | None = None,
    primary_key: tuple[str, ...] = (),
) -> Iterator[Batch]:
    """Dispatch one stream's extraction to the right path and yield batches.

    Shapes, by argument combination:

    * ``table_name`` + ``cursor`` (and ``cursor_field``) — incremental table
      read. First sync (or after ``--full-refresh``) takes the PK-keyset
      bootstrap; steady state takes the cursor keyset. ``primary_key`` must be
      non-empty for the bootstrap.
    * ``query`` + ``cursor`` (and ``cursor_field``) — incremental over an
      author-written ``SELECT``, wrapped as a subquery. No bootstrap — a query
      has no primary index to sweep; if the underlying data is huge, express
      the stream as a table instead.
    * ``table_name`` and *no* ``cursor`` — full-table scan via a server-side
      cursor. The YAML stream omitted ``incremental:`` and the engine did not
      inject a cursor.
    """
    if table_name is None and query is None:
        raise ValueError(
            f"cockroachdb stream {stream_name!r}: exactly one of table_name / query is required"
        )
    if table_name is not None and query is not None:
        raise ValueError(
            f"cockroachdb stream {stream_name!r}: table_name and query are mutually exclusive"
        )

    if query is not None:
        if cursor is None or cursor_field is None:
            raise ValueError(
                f"cockroachdb stream {stream_name!r}: 'query' mode is incremental-only — "
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
            primary_key=primary_key, stream_name=stream_name,
        )
        return

    if cursor_field is None:
        raise ValueError(
            f"cockroachdb stream {stream_name!r}: incremental table read requires cursor_field"
        )
    if state is None:
        raise ValueError(
            f"cockroachdb stream {stream_name!r}: incremental table read requires state — "
            f"declare `state` in the @stream signature so the engine injects it"
        )
    if not primary_key:
        raise ValueError(
            f"cockroachdb stream {stream_name!r}: incremental table read requires a "
            f"non-empty primary_key for the bootstrap sweep"
        )

    if cursor.is_full_refresh:
        # --full-refresh restarts history: forget any bootstrap progress so
        # the PK sweep runs again from the top.
        state.set(_STATE_BOOTSTRAPPED, False)
        state.set(_STATE_BOOTSTRAP_LAST_PK, None)
        state.set(_STATE_BOOTSTRAP_CURSOR_MAX, None)

    if not state.get(_STATE_BOOTSTRAPPED):
        yield from _extract_bootstrap_pk(
            config=config, state=state, cursor=cursor, log=log,
            schema_name=schema_name, table_name=table_name,
            cursor_field=cursor_field, primary_key=primary_key,
            stream_name=stream_name,
        )
        return

    yield from _extract_table_keyset(
        config=config, cursor=cursor, log=log, schema_name=schema_name,
        table_name=table_name, cursor_field=cursor_field, primary_key=primary_key,
        stream_name=stream_name,
    )


# ---------------------------------------------------------------------------
# The extraction paths
# ---------------------------------------------------------------------------


def _extract_bootstrap_pk(
    *,
    config: Config,
    state: State,
    cursor: Cursor,
    log: logging.Logger,
    schema_name: str,
    table_name: str,
    cursor_field: str,
    primary_key: tuple[str, ...],
    stream_name: str,
) -> Iterator[Batch]:
    """First sync of an incremental stream — sweep the table in primary-key order.

    Pages with ``(pk...) > (last seen)`` — always a constrained primary-index
    scan. Progress (last PK tuple, running cursor max) is written to ``state``
    after every yielded batch, and the engine commits state when the stream's
    batches land — so a page-capped run (``bootstrap_max_pages``) or a crash
    resumes from the recorded PK instead of restarting a multi-hour sweep.

    # NOTE: the running cursor max is tracked in ``state`` across runs, not
    # just in this run's ``Cursor``. PK order is uncorrelated with cursor
    # order, so the *final* bootstrap run's own observations may be far below
    # the true table max — handing that to the engine would make the first
    # steady-state run re-read a huge span. On completion the stored global
    # max is fed through ``cursor.observe`` so the engine persists the right
    # resume point.
    """
    batch_size = int(config.batch_size)
    max_pages = int(config.bootstrap_max_pages)
    aost = str(config.as_of_system_time)
    last_pk_state = state.get(_STATE_BOOTSTRAP_LAST_PK)
    last_pk: tuple[Any, ...] | None = (
        tuple(last_pk_state) if isinstance(last_pk_state, (list, tuple)) else None
    )
    running_max: Any = _cursor_from_state(
        state.get(_STATE_BOOTSTRAP_CURSOR_MAX), cursor.cursor_type
    )
    pages = 0
    complete = False

    log.info(
        "cockroachdb %s: bootstrap pk-keyset table=%s.%s pk=%s resume_pk=%r "
        "batch_size=%d max_pages=%s aost=%r",
        stream_name, schema_name, table_name, ",".join(primary_key), last_pk,
        batch_size, max_pages or "unlimited", aost,
    )

    with _with_connection(config) as conn, conn.cursor() as cur:
        while True:
            if max_pages and pages >= max_pages:
                log.info(
                    "cockroachdb %s: bootstrap paused after %d page(s) at pk=%r — "
                    "resumes on the next run",
                    stream_name, pages, last_pk,
                )
                break
            sql_stmt = pk_keyset_select_sql(
                schema_name, table_name, primary_key,
                first_page=last_pk is None, as_of_system_time=aost,
            )
            params: tuple[Any, ...] = (
                (batch_size,) if last_pk is None else (*last_pk, batch_size)
            )
            cur.execute(sql_stmt, params)
            column_names = [d.name for d in (cur.description or [])]
            rows = cur.fetchall()
            if not rows:
                complete = True
                break
            batch: Batch = []
            for row in rows:
                record = dict(zip(column_names, row, strict=False))
                cval = record.get(cursor_field)
                cursor.observe(cval)
                if cval is not None and (running_max is None or cval > running_max):
                    running_max = cval
                batch.append(record)
            last_record = batch[-1]
            last_pk = tuple(last_record[k] for k in primary_key)
            yield batch
            # The engine has landed the batch once control returns here —
            # record the resume point *after* the hand-off, never before.
            state.set(_STATE_BOOTSTRAP_LAST_PK, [_to_state_scalar(v) for v in last_pk])
            state.set(_STATE_BOOTSTRAP_CURSOR_MAX, _to_state_scalar(running_max))
            pages += 1
            if len(rows) < batch_size:
                complete = True
                break

    if complete:
        state.set(_STATE_BOOTSTRAPPED, True)
        state.set(_STATE_BOOTSTRAP_LAST_PK, None)
        # Hand the engine the true global max — see the docstring NOTE.
        cursor.observe(running_max)
        log.info(
            "cockroachdb %s: bootstrap complete after %d page(s); cursor handover=%r",
            stream_name, pages, running_max,
        )


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
    """Cursor-keyset ``schema.table`` on ``cursor_field`` (+ PK) — steady state.

    The loop: bind the current floor to ``%s``, fetch up to ``batch_size``
    rows, yield them as a :class:`Batch`, advance the floor to the last
    cursor value seen. Terminates when a page comes back smaller than
    ``batch_size``.

    # NOTE: ``cursor.observe`` is called for every row whose ``cursor_field``
    # value is non-null. ``Cursor`` ignores ``None`` (see types.py), so a row
    # without the cursor field never drags the cursor backward.
    """
    batch_size = int(config.batch_size)
    aost = str(config.as_of_system_time)
    floor: Any = cursor.start_value()
    sql_stmt = keyset_select_sql(
        schema_name, table_name, cursor_field, primary_key, as_of_system_time=aost
    )
    log.info(
        "cockroachdb %s: keyset table=%s.%s cursor_field=%s start=%r batch_size=%d aost=%r",
        stream_name, schema_name, table_name, cursor_field, floor, batch_size, aost,
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
                    "cockroachdb %s: cursor did not advance past %r — stopping",
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
        "cockroachdb %s: query-mode cursor_field=%s start=%r batch_size=%d",
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
                    "cockroachdb %s: cursor did not advance past %r — stopping",
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
    primary_key: tuple[str, ...] = (),
    stream_name: str,
) -> Iterator[Batch]:
    """Stream every row of ``schema.table`` via a server-side cursor.

    Used when the YAML stream declares NO ``incremental:`` block — the engine
    injects no :class:`Cursor`, and the connector must read the whole table
    in batches without loading it all into memory. Server-side cursors
    require a transaction; we open one explicitly with ``conn.transaction()``
    and, when a follower read is configured, pin the whole transaction with
    ``SET TRANSACTION AS OF SYSTEM TIME`` as its first statement.
    """
    batch_size = int(config.batch_size)
    aost = str(config.as_of_system_time)
    cursor_name = f"_dtex_crdb_{uuid.uuid4().hex[:12]}"
    body = full_scan_select_sql(schema_name, table_name, primary_key)
    declare_stmt = declare_cursor_sql(cursor_name, body)
    fetch_stmt = fetch_forward_sql(cursor_name, batch_size)
    close_stmt = close_cursor_sql(cursor_name)
    aost_stmt = set_transaction_aost_sql(aost)
    log.info(
        "cockroachdb %s: full-scan table=%s.%s server_cursor=%s batch_size=%d aost=%r",
        stream_name, schema_name, table_name, cursor_name, batch_size, aost,
    )

    with _with_connection(config) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                if aost_stmt is not None:
                    cur.execute(aost_stmt)
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
# State-scalar round-tripping — bootstrap progress must be JSON-safe
# ---------------------------------------------------------------------------


def _to_state_scalar(value: Any) -> Any:
    """Convert one value into a JSON-safe shape for the ``state`` blob.

    Timestamps and dates become ISO-8601 strings (the same convention the
    engine uses for persisted cursor values); JSON-native scalars pass
    through; anything else (e.g. ``Decimal``) becomes ``str`` — CockroachDB
    casts a bound string back to the column type in a comparison, so the
    round-trip stays valid as a keyset resume point.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _cursor_from_state(value: Any, cursor_type: CursorType) -> Any:
    """Parse a state-stored cursor scalar back into its comparable form.

    Inverse of :func:`_to_state_scalar` for the ``bootstrap_cursor_max`` key:
    the live rows yield ``datetime`` / ``date`` / ``int`` values, so the
    stored max must come back in the same type to be comparable and to be a
    correct ``cursor.observe`` hand-off.
    """
    if value is None:
        return None
    if cursor_type is CursorType.TIMESTAMP and isinstance(value, str):
        return datetime.fromisoformat(value)
    if cursor_type is CursorType.DATE and isinstance(value, str):
        return date.fromisoformat(value)
    if cursor_type is CursorType.INT and isinstance(value, str):
        return int(value)
    return value


# ---------------------------------------------------------------------------
# Connection lifecycle — lazy open, deterministic close
# ---------------------------------------------------------------------------


class _ConnectionContext:
    """Context manager that opens a CockroachDB connection on enter and closes on exit.

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
    """Return a context manager that opens / closes a CockroachDB connection.

    Used by every extractor above so the connection lifecycle matches the
    generator lifecycle: opened on the first ``yield from`` iteration, closed
    when the generator finishes or raises.
    """
    return _ConnectionContext(config)
