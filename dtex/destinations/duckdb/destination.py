# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The DuckDB destination connector body — the ``@destination`` hooks.

docs/05 §2 catalog row: "DuckDB — Local ``.duckdb`` file; ``INSERT`` /
``INSERT ... ON CONFLICT``. Zero-config dev default. Tier A." DuckDB is the
v1 dev default and every contract test runs through it, so this is built as
genuinely production-quality code, not a stub.

Hook contract — docs/03 §3.4 / docs/05 §1 / docs/09 §4, exact signatures:

* ``capabilities() -> set[Capability]``
* ``open(config) -> conn``
* ``ensure_schema(conn, table, schema) -> None``
* ``write_batch(conn, table, batch, disposition) -> int``
* ``read_state(conn, connector) -> list[StateRecord]``
* ``commit_state(conn, run_id, records) -> None``
* ``transaction(conn, stream) -> contextmanager``  (Capability.TRANSACTIONAL_LOAD)
* ``write_run_record(conn, record) -> None``       (Capability.RUN_RECORDS)
* ``close(conn) -> None``

The engine drives them in the order
``open → read_state → [ensure_schema → write_batch ...]* → commit_state → write_run_record → close``
(docs/05 §1, docs/09 §4); ``close`` always runs.

The ``conn`` passed between hooks is a :class:`DuckConn` wrapper, **not** the
raw ``duckdb`` connection — see its docstring for why.

# NOTE: ``@destination.state_backend`` is deliberately NOT defined. DuckDB is
# Tier A (it declares ``Capability.STATE``), so per docs/05 §5.4 it *is* its
# own state backend; ``state_backend`` is the Tier-B-only hook. The registry's
# ``MANDATORY_DESTINATION_HOOKS`` does not include it, and the engine only
# requires it when ``Capability.STATE`` is absent.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import duckdb

from dtex import (
    Batch,
    Capability,
    Config,
    LeaseRecord,
    LeaseStatus,
    RunRecord,
    Schema,
    StateRecord,
    StreamMeta,
    WriteDisposition,
    destination,
)
from dtex.destinations.duckdb.ddl import (
    add_column_sql,
    create_table_sql,
    qualified_table,
    quote_identifier,
    validate_identifier,
)

# Default path for the ``.duckdb`` file — docs/05 §2 "Zero-config dev default".
# Project-local and dot-prefixed so it sorts away from project files and is
# easy to .gitignore.
_DEFAULT_DB_PATH = ".dtex/warehouse.duckdb"

# The engine-owned state table — docs/03 §3.5, docs/05 §5.1. One row per
# (connector, stream); prefixed ``_dtex_`` so it sorts away from user
# tables. Its columns mirror ``StateRecord.to_row()`` exactly (8 columns).
#
# NOTE: this table was renamed from ``_simple_e_state`` to ``_dtex_state`` in
# stage 8.A (project-wide rename simple_e → dtex). The forthcoming runs table
# (stage 8.A run-record landing) will follow the same prefix convention:
# ``_dtex_runs``. Keeping the underscore-prefixed ``_dtex_`` namespace
# consistent across all engine-owned tables.
_STATE_TABLE = "_dtex_state"

# The engine-owned run-record audit table — docs/09 §4. One row per run;
# primary key ``run_id``. Same ``_dtex_`` namespace as ``_dtex_state`` so both
# engine tables sort together and apart from user tables. Created lazily on
# first write_run_record call via the ``DuckConn.runs_table_ready`` flag.
_RUNS_TABLE = "_dtex_runs"

# The engine-owned stream-lease table — docs/05 §5.5. One row per
# (connector, stream); same ``_dtex_`` namespace. Created lazily on first
# lease op via ``DuckConn.lease_table_ready``.
_LEASE_TABLE = "_dtex_leases"


@dataclass
class DuckConn:
    """The handle passed between ``@destination`` hooks for one run.

    docs/05 §1 fixes ``write_batch(conn, table, batch, disposition)`` — the
    signature carries no per-run scratch space. But ``replace`` needs exactly
    that: "truncate the table, then load" means *truncate once per run on the
    first batch*, then plain-insert the rest. Returning the raw
    ``duckdb.DuckDBPyConnection`` from ``open`` would leave nowhere to record
    "this table was already truncated this run".

    So ``open`` returns this wrapper instead. It carries:

    * :attr:`conn` — the live DuckDB connection;
    * :attr:`dataset` — the optional schema name (the ``dataset`` routing
      param), applied to every table reference;
    * :attr:`replace_truncated` — the set of tables already truncated this run,
      so a ``replace`` stream truncates exactly once however many batches it
      yields;
    * :attr:`state_table_ready` — whether ``_dtex_state`` has been created
      this run, so it is created lazily at most once.
    """

    conn: duckdb.DuckDBPyConnection
    dataset: str | None = None
    replace_truncated: set[str] = field(default_factory=set)
    state_table_ready: bool = False
    runs_table_ready: bool = False
    lease_table_ready: bool = False


# --------------------------------------------------------------------------
# capabilities — docs/05 §1
# --------------------------------------------------------------------------


@destination.capabilities
def capabilities() -> set[Capability]:
    """Declare what the DuckDB destination can do — docs/05 §1.

    * ``STATE`` — DuckDB is a real database; it hosts the ``_dtex_state``
      table itself (Tier A, docs/05 §5). It therefore implements
      ``read_state`` / ``commit_state`` and *not* ``state_backend``.
    * ``MERGE`` — DuckDB supports ``INSERT ... ON CONFLICT (pk) DO UPDATE``,
      which is the ``merge`` write disposition (docs/05 §4).
    * ``SCHEMA_EVOLUTION`` — DuckDB supports ``ALTER TABLE ADD COLUMN`` for
      additive evolution (docs/05 §3.2).

    * ``TRANSACTIONAL_LOAD`` — DuckDB has full ACID on a single connection, so
      a stream's batch loads and its state commit are made atomic by the
      ``@destination.transaction`` hook below. The engine wraps each stream's
      ``[write_batch… → commit_state]`` block in that context; a crash
      mid-stream rolls back, so an ``append`` stream never leaves half-written
      duplicates (docs/05 §5.3).
    * ``RUN_RECORDS`` — DuckDB hosts the ``_dtex_runs`` audit table alongside
      ``_dtex_state``, so a run's structured record is queryable with plain
      SQL (docs/09 §4). The ``@destination.write_run_record`` hook below
      implements it.
    """
    return {
        Capability.STATE,
        Capability.MERGE,
        Capability.SCHEMA_EVOLUTION,
        Capability.TRANSACTIONAL_LOAD,
        Capability.RUN_RECORDS,
        Capability.LEASE,
    }


# --------------------------------------------------------------------------
# max_concurrent_writes — stage 8e (pipeline-level parallelism cap)
# --------------------------------------------------------------------------


@destination.max_concurrent_writes
def max_concurrent_writes(config: Config) -> int:
    """Cap concurrent writes to DuckDB at 1 — single-connection bound.

    DuckDB uses a file lock on the ``.duckdb`` database, and one dtex run
    holds a single ``duckdb.DuckDBPyConnection`` shared across all streams
    (see :class:`DuckConn`). Two streams writing through that one connection
    concurrently — or two writer connections on the same file — would corrupt
    the transaction / the file. Returning 1 makes BOTH engine parallelism
    paths serialize on DuckDB: ``dtex run --tag … --threads N`` (across
    configs) AND ``dtex run -p … --threads N`` (across streams within one
    config). Because the engine clamps stream concurrency to
    ``min(threads, max_concurrent_writes)``, this ``1`` is exactly why DuckDB
    needs no per-connection lock — streams never touch it concurrently, so
    the ``DuckConn`` scratch (``replace_truncated``, the ``*_ready`` flags)
    and the per-stream ``transaction`` BEGIN/COMMIT stay single-threaded by
    construction.

    ``config`` is ignored — DuckDB's file-lock model is destination-wide,
    not param-tunable. Accepting the argument keeps the hook signature
    uniform across destinations (BigQuery reads ``max_concurrent_writes``
    from its params; this one would silently ignore the same param if a
    user set it on a DuckDB target, which is the safer failure mode than
    raising — the engine still serializes correctly).
    """
    return 1


# --------------------------------------------------------------------------
# transaction — docs/05 §1, §5.3 (conditional on Capability.TRANSACTIONAL_LOAD)
# --------------------------------------------------------------------------


@destination.transaction
@contextmanager
def transaction(conn: DuckConn, stream: StreamMeta) -> Iterator[None]:
    """Wrap one stream's load + state commit in a DuckDB transaction — docs/05 §5.3.

    The engine enters this context per stream, *after* ``ensure_schema`` (DDL
    implicitly commits in DuckDB, so the table must already exist), around the
    ``write_batch`` loop and the ``commit_state`` call. On a clean exit the data
    and the advanced cursor flip atomically with ``COMMIT``; if any
    ``write_batch`` raises, ``ROLLBACK`` discards the partial load so a re-run
    starts the stream cleanly — the guarantee that matters for ``append``
    streams, which would otherwise duplicate rows on every crash.

    Per-stream scope matches dtex's per-stream commit model (docs/02
    §Commit granularity): each stream is independently atomic; an earlier
    stream that already committed keeps its progress.
    """
    conn.conn.execute("BEGIN TRANSACTION")
    try:
        yield
    except Exception:
        conn.conn.execute("ROLLBACK")
        # A rolled-back ``replace`` truncation never happened — clear the
        # per-run guard so a retry within the same run truncates again.
        conn.replace_truncated.discard(stream.table)
        raise
    else:
        conn.conn.execute("COMMIT")


# --------------------------------------------------------------------------
# open / close — docs/05 §1
# --------------------------------------------------------------------------


@destination.open
def open(config: Config) -> DuckConn:
    """Open a DuckDB connection from ``config`` — docs/05 §1.

    Reads two params declared in ``register.yaml``:

    * ``path`` — the ``.duckdb`` file path (default :data:`_DEFAULT_DB_PATH`);
    * ``dataset`` — an optional schema name; when set, every table (including
      ``_dtex_state``) is created and addressed inside that schema, and the
      schema is created if absent.

    Returns a :class:`DuckConn` wrapper (see its docstring) — the handle every
    later hook receives. Called once per run.
    """
    path = str(config.get("path", _DEFAULT_DB_PATH))
    dataset_raw = config.get("dataset")
    dataset = None if dataset_raw is None else str(dataset_raw)

    # Ensure the parent directory of a file-backed database exists. ``:memory:``
    # (used by tests / ephemeral runs) has no parent and is left alone.
    if path != ":memory:":
        from pathlib import Path

        Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(path)
    if dataset is not None:
        # Validate before interpolating — same identifier gate as every table.
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(dataset, kind='dataset')}")
    return DuckConn(conn=conn, dataset=dataset)


@destination.close
def close(conn: DuckConn) -> None:
    """Close the DuckDB connection — docs/05 §1.

    "Always called, even on failure" (docs/05 §1), so this must be safe to call
    on a half-open or already-closed handle: any error from ``conn.close()`` is
    swallowed, because ``close`` failing must not mask the run's real error.
    """
    try:
        conn.conn.close()
    except Exception:  # noqa: BLE001 — close must never raise; see docstring.
        pass


# --------------------------------------------------------------------------
# ensure_schema — docs/05 §3
# --------------------------------------------------------------------------


@destination.ensure_schema
def ensure_schema(conn: DuckConn, stream: StreamMeta) -> None:
    """Create the target table if absent; additively evolve it — docs/05 §3.

    docs/05 §3.1: translate the stream's :class:`Schema` into native DDL.
    docs/05 §3.2: additive evolution — a field present in ``stream.schema`` but
    absent from an existing table is added with ``ALTER TABLE ADD COLUMN``
    (nullable; existing rows get ``NULL``).

    The engine appends ``_dtex_synced_at`` to every record (docs/03
    §2.2.1); this hook calls :meth:`Schema.with_synced_at` so the physical
    table always carries that column — both on first ``CREATE`` and, for a
    pre-existing table that lacks it, via additive evolution.

    Locked decision: the default schema-evolution policy is ``evolve``
    (additive). This hook performs the additive ``ALTER``; the engine enforces
    the per-stream ``strict`` opt-in (a strict stream's schema diff fails the
    run *before* this hook is called), so ``ensure_schema`` itself is always
    additive — it never needs to know the contract.

    # NOTE: ``stream.partition`` is intentionally IGNORED by this hook.
    # DuckDB has no native table-partitioning concept (the closest is the
    # ``Hive partitioned`` Parquet writer for ``COPY ... TO`` exports, which
    # is an export-time concern, not a table-level one). The
    # :class:`~dtex.types.PartitionConfig` field is informational on
    # destinations without native partitioning — silently dropping it here
    # matches the contract's promise that the field is "ignored by
    # destinations that lack partitioning" (docs/05 §3.x). The engine still
    # resolves and logs the chosen partition in the JSONL ``stream_start``
    # event for cross-destination consistency, so an operator running the
    # same source against DuckDB and BigQuery sees the SAME stream_start
    # event shape; only this hook drops it.
    """
    table = stream.table
    full_schema = stream.schema.with_synced_at()
    validate_identifier(table, kind="table")

    existing = _table_columns(conn, table)
    if existing is None:
        # Table absent — create it whole from the declared schema.
        conn.conn.execute(create_table_sql(conn.dataset, table, full_schema))
        return

    # Table present — additively add any declared column it lacks (docs/05 §3.2).
    for f in full_schema.fields:
        if f.name not in existing:
            conn.conn.execute(add_column_sql(conn.dataset, table, f.name, f.type))


def _table_columns(conn: DuckConn, table: str) -> set[str] | None:
    """Return the column-name set of ``table``, or ``None`` if it does not exist.

    Uses DuckDB's ``information_schema.columns`` — a parameterized query, so the
    table/schema names are bound values, never string-interpolated.
    """
    if conn.dataset is None:
        rows = conn.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND table_schema = current_schema()",
            [table],
        ).fetchall()
    else:
        rows = conn.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? AND table_schema = ?",
            [table, conn.dataset],
        ).fetchall()
    if not rows:
        return None
    return {str(r[0]) for r in rows}


# --------------------------------------------------------------------------
# write_batch — docs/05 §4
# --------------------------------------------------------------------------


@destination.write_batch
def write_batch(conn: DuckConn, batch: Batch, stream: StreamMeta) -> int:
    """Persist one batch per its write disposition — docs/05 §4. Returns rows written.

    docs/05 §4 dispositions, as implemented for DuckDB:

    * ``append`` — plain ``INSERT``. Duplicates are the source's concern.
    * ``merge`` — ``INSERT ... ON CONFLICT (primary_key) DO UPDATE`` (upsert).
      DuckDB requires a ``UNIQUE``/``PRIMARY KEY`` constraint on the conflict
      target, so this hook ensures one exists (idempotently) before inserting.
    * ``replace`` — truncate the table on the *first* batch of the run, then
      ``INSERT`` (this and every later batch). The "truncated this run" flag
      lives on :class:`DuckConn` so a multi-batch ``replace`` stream truncates
      exactly once.

    The engine sets ``_dtex_synced_at`` on every record; this hook fills
    that column with the current UTC time for any record that does not already
    carry it, so a load timestamp is always present (docs/03 §2.2.1).

    All per-stream metadata — ``table``, ``write_disposition``,
    ``primary_key`` — arrives in the single :class:`StreamMeta` argument
    (docs/05 §1). New per-stream concerns become ``StreamMeta`` fields, never
    new hook parameters, so this signature stays stable as the engine grows.
    """
    table = stream.table
    validate_identifier(table, kind="table")
    wd = stream.write_disposition

    if not batch:
        # An empty batch is a valid no-op — but a ``replace`` stream that
        # yields nothing must still truncate (full-snapshot ⇒ empty snapshot).
        if wd is WriteDisposition.REPLACE:
            _truncate_once(conn, table)
        return 0

    # Stamp the engine load-timestamp column on records that lack it.
    stamped = _stamp_synced_at(batch)
    columns = _batch_columns(stamped)

    if wd is WriteDisposition.REPLACE:
        _truncate_once(conn, table)
        _insert_rows(conn, table, columns, stamped)
    elif wd is WriteDisposition.APPEND:
        _insert_rows(conn, table, columns, stamped)
    elif wd is WriteDisposition.MERGE:
        if not stream.primary_key:
            raise ValueError(
                f"write_batch: disposition 'merge' for table {table!r} requires a "
                f"primary_key (docs/05 §4)"
            )
        _merge_rows(conn, table, columns, stream.primary_key, stamped)
    else:  # pragma: no cover — WriteDisposition is a closed 3-member enum.
        raise ValueError(f"write_batch: unknown disposition {wd!r}")

    return len(stamped)


def _stamp_synced_at(batch: Batch) -> list[dict[str, Any]]:
    """Return a copy of ``batch`` with ``_dtex_synced_at`` set on every record.

    docs/03 §2.2.1: the engine appends this load-timestamp column. A record
    that already carries a value keeps it (a resumed/replayed batch stays
    stable); a record without one gets the current UTC time. The input batch
    is not mutated — connector code should see exactly the dicts it yielded.
    """
    now = datetime.now(UTC)
    out: list[dict[str, Any]] = []
    for record in batch:
        row = dict(record)
        if row.get(Schema.SYNCED_AT_COLUMN) is None:
            row[Schema.SYNCED_AT_COLUMN] = now
        out.append(row)
    return out


def _batch_columns(batch: list[dict[str, Any]]) -> tuple[str, ...]:
    """Return the union of column names across a batch, in first-seen order.

    A batch is a ``list[dict]`` (docs/04); records may be ragged. The insert
    must name a stable column set, so this collects every key any record uses;
    a record missing one of them binds ``NULL`` for it. Every name is validated
    as a safe identifier here, at the single choke point before SQL building.
    """
    seen: list[str] = []
    seen_set: set[str] = set()
    for record in batch:
        for key in record:
            if key not in seen_set:
                validate_identifier(key, kind="column")
                seen.append(key)
                seen_set.add(key)
    return tuple(seen)


def _encode_value(value: Any) -> Any:
    """Coerce a record value into something DuckDB's parameter binding accepts.

    Used by the *data* insert paths (``append`` / ``merge`` / ``replace``),
    where each bind targets a *typed* column (``VARCHAR`` / ``BIGINT`` /
    ``TIMESTAMP`` / …). ``dict`` / ``list`` values target a ``JSON`` column
    and are ``json.dumps``-serialized; every other value (scalars,
    ``datetime``, ``None``) is bound as-is — DuckDB handles those natively.

    Do *not* use this for binds that target a ``JSON`` column directly (e.g.
    ``_dtex_state.cursor_value`` / ``state_blob``) — a bare scalar string
    is not valid JSON text and DuckDB rejects it. Use
    :func:`_encode_json_column` instead.
    """
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def _encode_json_column(value: Any) -> Any:
    """Serialize *any* value for binding into a DuckDB ``JSON`` column.

    DuckDB's ``JSON`` type ingests a JSON-*text* string, so every non-``None``
    value — including bare scalars — must be ``json.dumps``-serialized first.
    Otherwise a string cursor like ``"2026-05-20T00:00:00"`` raises a
    ``ConversionException`` on commit (malformed JSON). ``None`` stays
    ``NULL``; ``datetime`` / ``date`` are serialized via ``default=str`` so
    they round-trip cleanly through :func:`_decode_json`.

    Used at every bind into ``_dtex_state.cursor_value`` and
    ``_dtex_state.state_blob``. The split from :func:`_encode_value` (the
    data-insert path) is deliberate — data binds go to typed columns; state
    binds go to JSON columns. Conflating them was a real bug surfaced by the
    stage-7 connector builds: a string-cursor source could not commit state.
    """
    if value is None:
        return None
    return json.dumps(value, default=str)


def _row_tuple(record: dict[str, Any], columns: tuple[str, ...]) -> list[Any]:
    """Build the positional bind values for one record over ``columns``.

    A column absent from this record binds ``NULL``; present values pass
    through :func:`_encode_value`. Order matches ``columns`` so it lines up
    with a parameterized ``INSERT``.
    """
    return [_encode_value(record.get(col)) for col in columns]


def _insert_rows(
    conn: DuckConn,
    table: str,
    columns: tuple[str, ...],
    batch: list[dict[str, Any]],
) -> None:
    """Run a parameterized multi-row ``INSERT`` — the ``append`` / ``replace`` path.

    Values are bound via ``?`` placeholders (``executemany``); column and table
    names are validated + quoted. No record value is ever string-formatted into
    SQL — the task's "parameterize SQL safely" bar.
    """
    qcols = ", ".join(quote_identifier(c, kind="column") for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    sql = f"INSERT INTO {qualified_table(conn.dataset, table)} ({qcols}) VALUES ({placeholders})"
    conn.conn.executemany(sql, [_row_tuple(r, columns) for r in batch])


def _merge_rows(
    conn: DuckConn,
    table: str,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
    batch: list[dict[str, Any]],
) -> None:
    """Upsert a batch via ``INSERT ... ON CONFLICT (pk) DO UPDATE`` — docs/05 §4.

    DuckDB's ``ON CONFLICT`` clause needs a ``UNIQUE`` / ``PRIMARY KEY``
    constraint on the conflict-target columns; :func:`_ensure_unique_index`
    creates one (idempotently) first. The ``DO UPDATE`` set overwrites every
    non-key column with the incoming value — "insert new rows, overwrite
    matched rows" (docs/05 §4).
    """
    for key in primary_key:
        validate_identifier(key, kind="column")
    _ensure_unique_index(conn, table, primary_key)

    qcols = ", ".join(quote_identifier(c, kind="column") for c in columns)
    placeholders = ", ".join("?" for _ in columns)
    conflict_cols = ", ".join(quote_identifier(k, kind="column") for k in primary_key)

    pk_set = set(primary_key)
    update_cols = [c for c in columns if c not in pk_set]
    if update_cols:
        # Overwrite every non-key column from the proposed (``EXCLUDED``) row.
        set_clause = ", ".join(
            f"{quote_identifier(c, kind='column')} = EXCLUDED.{quote_identifier(c, kind='column')}"
            for c in update_cols
        )
        conflict_action = f"DO UPDATE SET {set_clause}"
    else:
        # Every column is part of the key — a matched row is already identical;
        # there is nothing to update, so the conflict is simply ignored.
        conflict_action = "DO NOTHING"

    sql = (
        f"INSERT INTO {qualified_table(conn.dataset, table)} ({qcols}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_cols}) {conflict_action}"
    )
    conn.conn.executemany(sql, [_row_tuple(r, columns) for r in batch])


def _ensure_unique_index(conn: DuckConn, table: str, primary_key: tuple[str, ...]) -> None:
    """Create a ``UNIQUE INDEX`` on the merge key if one is not already present.

    ``INSERT ... ON CONFLICT`` requires the conflict target to be backed by a
    ``UNIQUE`` / ``PRIMARY KEY`` constraint. The index name is derived
    deterministically from the (validated) table + key names, so the
    ``IF NOT EXISTS`` makes this idempotent across batches and across runs.
    """
    index_name = f"_dtex_uq_{table}_{'_'.join(primary_key)}"
    validate_identifier(index_name, kind="index")
    key_cols = ", ".join(quote_identifier(k, kind="column") for k in primary_key)
    conn.conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {quote_identifier(index_name, kind='index')} "
        f"ON {qualified_table(conn.dataset, table)} ({key_cols})"
    )


def _truncate_once(conn: DuckConn, table: str) -> None:
    """Truncate ``table`` at most once per run — the ``replace`` disposition.

    docs/05 §4: ``replace`` is "truncate the table, then load". A ``replace``
    stream may yield several batches; only the *first* must truncate, or batches
    2..N would wipe their predecessors. :attr:`DuckConn.replace_truncated`
    records which tables this run already truncated.
    """
    if table in conn.replace_truncated:
        return
    conn.conn.execute(f"DELETE FROM {qualified_table(conn.dataset, table)}")
    conn.replace_truncated.add(table)


# --------------------------------------------------------------------------
# read_state / commit_state — the _dtex_state table — docs/05 §5
# --------------------------------------------------------------------------


def _ensure_state_table(conn: DuckConn) -> None:
    """Create ``_dtex_state`` lazily on first use — docs/05 §5.1.

    Eight columns, mirroring ``StateRecord.to_row()`` exactly: connector,
    stream, cursor_value, cursor_type, state_blob, last_run_id, rows_total,
    updated_at. ``cursor_value`` / ``state_blob`` are ``JSON``; the primary key
    is ``(connector, stream)``. Created at most once per run via the
    :attr:`DuckConn.state_table_ready` flag.
    """
    if conn.state_table_ready:
        return
    table = qualified_table(conn.dataset, _STATE_TABLE)
    conn.conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"  {quote_identifier('connector', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('stream', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('cursor_value', kind='column')} JSON, "
        f"  {quote_identifier('cursor_type', kind='column')} VARCHAR, "
        f"  {quote_identifier('state_blob', kind='column')} JSON, "
        f"  {quote_identifier('last_run_id', kind='column')} VARCHAR, "
        f"  {quote_identifier('rows_total', kind='column')} BIGINT NOT NULL DEFAULT 0, "
        f"  {quote_identifier('updated_at', kind='column')} TIMESTAMP, "
        f"  PRIMARY KEY ({quote_identifier('connector', kind='column')}, "
        f"{quote_identifier('stream', kind='column')})"
        f")"
    )
    conn.state_table_ready = True


@destination.read_state
def read_state(conn: DuckConn, connector: str) -> list[StateRecord]:
    """Load every prior :class:`StateRecord` for a connector — docs/05 §1, §5.

    Called once at run start (docs/05 §1 lifecycle). Returns one
    :class:`StateRecord` per ``_dtex_state`` row whose ``connector`` matches
    — the per-stream resume points. An empty list on the first ever run (the
    state table is created lazily, so it always exists by the time this reads).

    The ``cursor_value`` / ``state_blob`` JSON columns are deserialized from
    their JSON-text storage back into Python values, then handed to
    :meth:`StateRecord.from_row`, which re-types ``cursor_type`` and
    ``updated_at``.
    """
    _ensure_state_table(conn)
    table = qualified_table(conn.dataset, _STATE_TABLE)
    rows = conn.conn.execute(
        f"SELECT connector, stream, cursor_value, cursor_type, state_blob, "
        f"last_run_id, rows_total, updated_at "
        f"FROM {table} WHERE connector = ?",
        [connector],
    ).fetchall()

    records: list[StateRecord] = []
    for row in rows:
        records.append(
            StateRecord.from_row(
                {
                    "connector": row[0],
                    "stream": row[1],
                    "cursor_value": _decode_json(row[2]),
                    "cursor_type": row[3],
                    "state_blob": _decode_json(row[4]) or {},
                    "last_run_id": row[5],
                    "rows_total": row[6],
                    "updated_at": row[7],
                }
            )
        )
    return records


@destination.commit_state
def commit_state(conn: DuckConn, run_id: str, records: list[StateRecord]) -> None:
    """Upsert the run's :class:`StateRecord` set into ``_dtex_state`` — docs/05 §5.

    docs/05 §5.3: the non-negotiable rule — ``commit_state`` is called **only
    after all batches durably land**. It receives every stream's state record
    for the run; each is upserted on the ``(connector, stream)`` primary key
    via ``INSERT ... ON CONFLICT DO UPDATE``, so a stream's row is created on
    its first run and advanced in place thereafter.

    ``cursor_value`` / ``state_blob`` are JSON columns: the values go in as
    ``json.dumps`` text via :meth:`StateRecord.to_row` + :func:`_encode_value`.
    ``updated_at`` is stamped with the current UTC time when the record has
    not already set it, so every committed row carries a commit timestamp
    (docs/05 §5.1).

    # NOTE: ``updated_at`` lands in a DuckDB ``TIMESTAMP`` column, which is
    # timezone-naive (docs/05 §3.1 maps the ``timestamp`` logical type to
    # ``TIMESTAMP``, not ``TIMESTAMP_TZ``). The wall-clock instant round-trips
    # exactly; an aware ``datetime``'s offset is not stored. Stamping with UTC
    # makes that lossless in practice — the stored naive value *is* the UTC
    # wall-clock — and keeps every committed timestamp on one timeline.
    """
    if not records:
        return
    _ensure_state_table(conn)
    table = qualified_table(conn.dataset, _STATE_TABLE)
    now = datetime.now(UTC)

    sql = (
        f"INSERT INTO {table} "
        f"(connector, stream, cursor_value, cursor_type, state_blob, "
        f" last_run_id, rows_total, updated_at) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        f"ON CONFLICT (connector, stream) DO UPDATE SET "
        f"  cursor_value = EXCLUDED.cursor_value, "
        f"  cursor_type  = EXCLUDED.cursor_type, "
        f"  state_blob   = EXCLUDED.state_blob, "
        f"  last_run_id  = EXCLUDED.last_run_id, "
        f"  rows_total   = EXCLUDED.rows_total, "
        f"  updated_at   = EXCLUDED.updated_at"
    )
    params: list[list[Any]] = []
    for record in records:
        if record.last_run_id is None:
            record.last_run_id = run_id
        if record.updated_at is None:
            record.updated_at = now
        row = record.to_row()
        params.append(
            [
                row["connector"],
                row["stream"],
                _encode_json_column(row["cursor_value"]),
                row["cursor_type"],
                _encode_json_column(row["state_blob"]),
                row["last_run_id"],
                row["rows_total"],
                row["updated_at"],
            ]
        )
    conn.conn.executemany(sql, params)


def _decode_json(value: Any) -> Any:
    """Deserialize a value read back from a DuckDB ``JSON`` column.

    DuckDB returns a ``JSON`` column as its JSON-*text* string; this parses it
    back to a Python value. A value that is already a Python object (some
    DuckDB builds / paths hand one back directly) is returned unchanged, and
    ``None`` stays ``None``.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return json.loads(value)
    return value


# --------------------------------------------------------------------------
# Stream leasing — the _dtex_leases table — docs/05 §5.5 (Capability.LEASE)
# --------------------------------------------------------------------------


def _ensure_lease_table(conn: DuckConn) -> None:
    """Create ``_dtex_leases`` lazily on first use — docs/05 §5.5.

    Six columns mirroring :meth:`LeaseRecord.to_row`; primary key
    ``(connector, stream)`` — one lease per stream. ``acquired_at`` /
    ``heartbeat_at`` are ``TIMESTAMP`` (UTC wall-clock, same convention as
    ``_dtex_state.updated_at``).
    """
    if conn.lease_table_ready:
        return
    table = qualified_table(conn.dataset, _LEASE_TABLE)
    conn.conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"  {quote_identifier('connector', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('stream', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('run_id', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('status', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('acquired_at', kind='column')} TIMESTAMP, "
        f"  {quote_identifier('heartbeat_at', kind='column')} TIMESTAMP, "
        f"  PRIMARY KEY ({quote_identifier('connector', kind='column')}, "
        f"{quote_identifier('stream', kind='column')})"
        f")"
    )
    conn.lease_table_ready = True


@destination.read_leases
def read_leases(conn: DuckConn, connector: str) -> list[LeaseRecord]:
    """Load every ``_dtex_leases`` row for a connector — docs/05 §5.5.

    Called once at run start so the engine can decide which streams are held
    by a live lease. Liveness (fresh heartbeat vs. stale) is judged by the
    engine (:meth:`LeaseRecord.is_live`), not here — this hook just returns the
    raw rows.
    """
    _ensure_lease_table(conn)
    table = qualified_table(conn.dataset, _LEASE_TABLE)
    rows = conn.conn.execute(
        f"SELECT connector, stream, run_id, status, acquired_at, heartbeat_at "
        f"FROM {table} WHERE connector = ?",
        [connector],
    ).fetchall()
    return [
        LeaseRecord.from_row(
            {
                "connector": r[0],
                "stream": r[1],
                "run_id": r[2],
                "status": r[3],
                "acquired_at": r[4],
                "heartbeat_at": r[5],
            }
        )
        for r in rows
    ]


@destination.acquire_leases
def acquire_leases(conn: DuckConn, leases: Sequence[LeaseRecord]) -> set[str]:
    """Batched compare-and-set acquire — docs/05 §5.5. Returns the won streams.

    DuckDB is single-writer (its ``max_concurrent_writes`` is 1, so the engine
    never runs streams concurrently against it), so the batching here is to
    satisfy the batched hook contract, not to dodge a serialization error the
    way BigQuery must. The whole set is acquired in ONE transaction — either
    every eligible lease lands or none does — and the per-lease win rule is the
    same proven check-then-set as before: a candidate wins unless a *live*
    lease from another run already holds the stream.

    # NOTE: the win decision is made per-row against the state read at the top
    # of the transaction. Two candidates in the SAME set never contend (the
    # engine passes distinct streams), so intra-batch ordering is irrelevant.
    """
    if not leases:
        return set()
    _ensure_lease_table(conn)
    table = qualified_table(conn.dataset, _LEASE_TABLE)
    won: set[str] = set()

    conn.conn.execute("BEGIN TRANSACTION")
    try:
        for lease in leases:
            now = lease.heartbeat_at or datetime.now(UTC)
            cutoff_naive = _naive_utc(_stale_cutoff(now))
            existing = conn.conn.execute(
                f"SELECT run_id, status, heartbeat_at FROM {table} "
                f"WHERE connector = ? AND stream = ?",
                [lease.connector, lease.stream],
            ).fetchone()
            if existing is not None:
                ex_run, ex_status, ex_beat = existing[0], existing[1], existing[2]
                held_live = (
                    ex_status == LeaseStatus.RUNNING.value
                    and ex_run != lease.run_id
                    and ex_beat is not None
                    and ex_beat >= cutoff_naive
                )
                if held_live:
                    continue  # another run holds it live — skip, do not win
            conn.conn.execute(
                f"INSERT INTO {table} "
                f"(connector, stream, run_id, status, acquired_at, heartbeat_at) "
                f"VALUES (?, ?, ?, ?, ?, ?) "
                f"ON CONFLICT (connector, stream) DO UPDATE SET "
                f"  run_id = EXCLUDED.run_id, status = EXCLUDED.status, "
                f"  acquired_at = EXCLUDED.acquired_at, "
                f"  heartbeat_at = EXCLUDED.heartbeat_at",
                [
                    lease.connector,
                    lease.stream,
                    lease.run_id,
                    lease.status.value,
                    _naive_utc(lease.acquired_at or now),
                    _naive_utc(now),
                ],
            )
            won.add(lease.stream)
        conn.conn.execute("COMMIT")
        return won
    except Exception:
        conn.conn.execute("ROLLBACK")
        raise


@destination.heartbeat_leases
def heartbeat_leases(conn: DuckConn, leases: Sequence[LeaseRecord]) -> None:
    """Refresh every held lease's heartbeat — docs/05 §5.5. Batched no-DML-if-empty."""
    _update_leases(conn, leases)


@destination.release_leases
def release_leases(conn: DuckConn, leases: Sequence[LeaseRecord]) -> None:
    """Release every held lease with its per-stream terminal status — docs/05 §5.5."""
    _update_leases(conn, leases)


def _update_leases(conn: DuckConn, leases: Sequence[LeaseRecord]) -> None:
    """Shared batched status+heartbeat UPDATE for heartbeat / release.

    Each row's own ``status`` is written (so heartbeat keeps ``running`` and
    release writes ``done``/``failed``), ``run_id``-guarded so a refresh or
    release never stomps a lease another holder reclaimed. One transaction for
    the whole set.
    """
    if not leases:
        return
    _ensure_lease_table(conn)
    table = qualified_table(conn.dataset, _LEASE_TABLE)
    conn.conn.execute("BEGIN TRANSACTION")
    try:
        for lease in leases:
            now = lease.heartbeat_at or datetime.now(UTC)
            conn.conn.execute(
                f"UPDATE {table} SET status = ?, heartbeat_at = ? "
                f"WHERE connector = ? AND stream = ? AND run_id = ?",
                [
                    lease.status.value,
                    _naive_utc(now),
                    lease.connector,
                    lease.stream,
                    lease.run_id,
                ],
            )
        conn.conn.execute("COMMIT")
    except Exception:
        conn.conn.execute("ROLLBACK")
        raise


def _stale_cutoff(now: datetime) -> datetime:
    """The timestamp before which a heartbeat is stale — docs/05 §5.5."""
    from datetime import timedelta

    from dtex.types import LEASE_STALE_SECONDS

    return now - timedelta(seconds=LEASE_STALE_SECONDS)


def _naive_utc(value: datetime) -> datetime:
    """Convert a datetime to naive UTC wall-clock for a DuckDB TIMESTAMP column.

    # NOTE: this is load-bearing, not cosmetic. DuckDB binds a *timezone-aware*
    # Python datetime into a naive TIMESTAMP column by converting it to the
    # process's LOCAL time (verified: a 10:17 UTC value lands as 13:17 in a
    # UTC+3 process) and then hands it back naive — so a heartbeat written in
    # one timezone and compared against a UTC ``now`` is wrong by the local
    # offset, silently breaking staleness. Every timestamp this destination
    # writes to ``_dtex_leases`` therefore goes through here: aware values are
    # shifted to UTC then stripped, naive values are assumed already-UTC. The
    # stored wall-clock is thus always UTC, matching ``_dtex_state.updated_at``.
    """
    if value.tzinfo is not None:
        value = value.astimezone(UTC)
    return value.replace(tzinfo=None)


# --------------------------------------------------------------------------
# write_run_record — the _dtex_runs audit table — docs/09 §4 (Capability.RUN_RECORDS)
# --------------------------------------------------------------------------


def _ensure_runs_table(conn: DuckConn) -> None:
    """Create ``_dtex_runs`` lazily on first use — docs/09 §4.

    Columns (the canonical schema, destination-agnostic in the logical
    design; the DuckDB implementation maps them to native types here):

    * ``run_id``        TEXT, PRIMARY KEY — one row per run, upsertable.
    * ``config``        TEXT — the pipeline config name.
    * ``source``        TEXT — the source connector name.
    * ``destination``   TEXT — the destination connector name.
    * ``target``        TEXT — the profiles.yml target used.
    * ``status``        TEXT — ``succeeded`` / ``failed``.
    * ``started_at``    TIMESTAMP — run window open.
    * ``ended_at``      TIMESTAMP — run window close.
    * ``duration_s``    DOUBLE — computed seconds.
    * ``rows_loaded``   BIGINT — total rows across streams.
    * ``full_refresh``  BOOLEAN — whether state was discarded.
    * ``error_type``    TEXT, nullable — exception class name on failure.
    * ``error_message`` TEXT, nullable — exception message on failure.
    * ``streams_json``  JSON — per-stream array (StreamResult.to_dict each).

    # NOTE: ``duration_s`` is stored as a real column (not computed at read
    # time) so SQL ``ORDER BY duration_s DESC`` "show me the slowest runs"
    # works without a derivation expression — a common admin query, and the
    # value is trivially cheap to compute once at write time.
    """
    if conn.runs_table_ready:
        return
    table = qualified_table(conn.dataset, _RUNS_TABLE)
    conn.conn.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"  {quote_identifier('run_id', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('config', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('source', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('destination', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('target', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('status', kind='column')} VARCHAR NOT NULL, "
        f"  {quote_identifier('started_at', kind='column')} TIMESTAMP NOT NULL, "
        f"  {quote_identifier('ended_at', kind='column')} TIMESTAMP NOT NULL, "
        f"  {quote_identifier('duration_s', kind='column')} DOUBLE NOT NULL, "
        f"  {quote_identifier('rows_loaded', kind='column')} BIGINT NOT NULL DEFAULT 0, "
        f"  {quote_identifier('full_refresh', kind='column')} BOOLEAN NOT NULL DEFAULT FALSE, "
        f"  {quote_identifier('error_type', kind='column')} VARCHAR, "
        f"  {quote_identifier('error_message', kind='column')} VARCHAR, "
        f"  {quote_identifier('streams_json', kind='column')} JSON, "
        f"  PRIMARY KEY ({quote_identifier('run_id', kind='column')})"
        f")"
    )
    conn.runs_table_ready = True


@destination.write_run_record
def write_run_record(conn: DuckConn, record: RunRecord) -> None:
    """Persist one :class:`RunRecord` to ``_dtex_runs`` — docs/09 §4.

    Called once per run by the engine, after streams finish and before
    ``close`` (docs/05 §1 lifecycle, stage 8a addendum). The write is
    idempotent on ``run_id``: a duplicate write of the same record updates
    the row rather than duplicating it. The engine only writes once, but
    the upsert is the defensive guarantee — a future retry-on-transient
    cannot corrupt the audit chain.

    ``streams_json`` is a ``JSON`` column so per-stream detail (rows,
    cursor, status) survives in queryable form without a join.
    :func:`_encode_json_column` serializes it (the stage-7 split: data
    binds go to typed columns; JSON-column binds go through dumps).
    """
    _ensure_runs_table(conn)
    table = qualified_table(conn.dataset, _RUNS_TABLE)
    sql = (
        f"INSERT INTO {table} ("
        f"  run_id, config, source, destination, target, status, "
        f"  started_at, ended_at, duration_s, rows_loaded, full_refresh, "
        f"  error_type, error_message, streams_json"
        f") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        f"ON CONFLICT (run_id) DO UPDATE SET "
        f"  config        = EXCLUDED.config, "
        f"  source        = EXCLUDED.source, "
        f"  destination   = EXCLUDED.destination, "
        f"  target        = EXCLUDED.target, "
        f"  status        = EXCLUDED.status, "
        f"  started_at    = EXCLUDED.started_at, "
        f"  ended_at      = EXCLUDED.ended_at, "
        f"  duration_s    = EXCLUDED.duration_s, "
        f"  rows_loaded   = EXCLUDED.rows_loaded, "
        f"  full_refresh  = EXCLUDED.full_refresh, "
        f"  error_type    = EXCLUDED.error_type, "
        f"  error_message = EXCLUDED.error_message, "
        f"  streams_json  = EXCLUDED.streams_json"
    )
    conn.conn.execute(
        sql,
        [
            record.run_id,
            record.config,
            record.source,
            record.destination,
            record.target,
            record.status.value,
            record.started_at,
            record.ended_at,
            record.duration_s,
            record.rows_loaded,
            record.full_refresh,
            record.error_type,
            record.error_message,
            _encode_json_column(record.streams_json()),
        ],
    )
