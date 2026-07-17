# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""CockroachDB connection + safe-SQL helpers for the dtex CockroachDB connector.

Connection management, identifier quoting, and pagination-SQL construction —
the plumbing the ``@stream`` functions in :mod:`source` build on. Every
identifier flows through :class:`psycopg.sql.Identifier`; every value flows
through ``%s`` parameter binding. The connector never f-strings a table name
or a cursor value.

CockroachDB deltas vs the ``postgres`` connector's client:

* ``sslrootcert`` (default ``system``) — Cockroach Cloud presents certificates
  signed by a public CA, so the OS trust store verifies them; a bare libpq
  default would look for ``~/.postgresql/root.crt`` and fail.
* ``options`` — pass-through for ``--cluster=<routing-id>`` on multi-tenant
  Cockroach Cloud when the client stack cannot do SNI routing. Modern libpq
  (14+) negotiates SNI and does not need it.
* ``AS OF SYSTEM TIME`` fragments — follower reads make extraction contention-
  free and cheaper on Cockroach Cloud; see :func:`aost_clause`.
* Primary-key keyset pagination (:func:`pk_keyset_select_sql`) — the bootstrap
  read path; always index-backed by the primary key, never a sort.
* ``connect`` sets ``autocommit`` — each page-SELECT runs as its own implicit
  transaction, which is what allows a per-statement ``AS OF SYSTEM TIME``
  clause (CockroachDB rejects it inside a multi-statement transaction unless
  set via ``SET TRANSACTION``).

# NOTE: this module imports ``psycopg`` (v3) lazily *inside* functions, not at
# module top. The connector folder is imported at discovery time
# (dtex/engine/discovery.py), which runs before the engine has confirmed
# the run actually targets CockroachDB — a missing ``psycopg`` would otherwise
# break unrelated runs that merely scanned this folder. The driver is a
# declared runtime dependency (pyproject.toml ``psycopg[binary]>=3.1``); it is
# guaranteed to be installed when a run actually opens a connection, but only
# loaded at that moment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dtex import Config

if TYPE_CHECKING:
    import psycopg
    from psycopg import sql as psycopg_sql


# The one AS OF SYSTEM TIME expression allowed through as a SQL *function
# call* rather than a quoted literal. Anything else the operator configures
# (an interval like ``-10s``, an ISO timestamp) is composed as a Literal.
_AOST_FUNCTION_FORMS = frozenset({"follower_read_timestamp()"})


def _select_list(primary_key: tuple[str, ...]) -> Any:
    """The SELECT list: primary-key columns explicitly, then ``*``.

    # NOTE: this exists because of CockroachDB *hidden columns*. A REGIONAL BY
    # ROW table's ``crdb_region`` primary-key column is hidden — ``SELECT *``
    # omits it, which would strip it from extracted records (breaking merge on
    # the PK downstream) and leave the bootstrap unable to read its own resume
    # tuple. Naming a hidden column explicitly returns it. Visible PK columns
    # end up twice in the result set (once named, once via ``*``); the record
    # builder ``dict(zip(...))`` collapses duplicates to one equal value.
    """
    from psycopg import sql

    if not primary_key:
        return sql.SQL("*")
    parts = [sql.Identifier(k) for k in primary_key]
    return sql.SQL("{pks}, *").format(pks=sql.SQL(", ").join(parts))


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def connect(config: Config) -> psycopg.Connection[Any]:
    """Open a CockroachDB connection from a resolved :class:`Config` — lazy per stream.

    docs/03 §3: ``Config`` carries resolved params (``host``, ``port``,
    ``database``, ``user``, ``sslmode``, ``sslrootcert``, ``options``,
    ``application_name``, ``connect_timeout_seconds``) plus secrets
    (``password``). The connection is opened at the moment a ``@stream``
    generator first iterates, never at module import — the connector body owns
    the lifecycle and closes the connection when the generator is exhausted or
    raises (see :func:`source._with_connection`).

    The connection is put in ``autocommit`` mode: every extraction statement
    is a read, and autocommit is what lets a per-statement ``AS OF SYSTEM
    TIME`` clause through (CockroachDB rejects it inside an open multi-
    statement transaction). The full-scan path opens its explicit transaction
    with ``conn.transaction()``, which works unchanged under autocommit.

    # NOTE: the password is *never* logged. ``Config.secrets`` is read by
    # explicit subscript here (docs/03 §3) and handed straight to
    # ``psycopg.connect`` as a keyword argument; nothing in the path
    # stringifies the dict containing it.
    """
    import psycopg

    password = config.secrets["password"]
    kwargs: dict[str, Any] = {
        "host": config.host,
        "port": config.port,
        "dbname": config.database,
        "user": config.user,
        "password": password,
        "sslmode": config.sslmode,
        "application_name": config.application_name,
        "connect_timeout": config.connect_timeout_seconds,
        "autocommit": True,
    }
    # sslrootcert only matters for verify-* modes; libpq rejects an empty
    # string, so omit the kwarg entirely when unset.
    #
    # # NOTE: the special value "certifi" resolves to the certifi CA bundle
    # at runtime. libpq's "system" needs libpq >= 16 *with* a configured
    # OpenSSL default store — true for a distro libpq, false for the libpq
    # bundled in psycopg[binary] wheels, where "system" fails certificate
    # verification. "certifi" gives a machine-independent spelling that works
    # under the wheels; certifi ships transitively with dtex's requests
    # dependency.
    if config.sslrootcert == "certifi":
        import certifi

        kwargs["sslrootcert"] = certifi.where()
    elif config.sslrootcert:
        kwargs["sslrootcert"] = config.sslrootcert
    # options carries e.g. "--cluster=<routing-id>" for non-SNI clients on
    # multi-tenant Cockroach Cloud; empty means "not needed".
    if config.options:
        kwargs["options"] = config.options
    return psycopg.connect(**kwargs)


# ---------------------------------------------------------------------------
# Identifier quoting — psycopg.sql.Identifier is the *only* path
# ---------------------------------------------------------------------------


def quote_identifier(name: str) -> psycopg_sql.Identifier:
    """Wrap a SQL identifier so it is safe to compose into a statement.

    Delegates to :class:`psycopg.sql.Identifier`, which doubles embedded
    double-quotes and wraps the name in quotes — defeating the classic
    ``'; DROP TABLE x; --`` style injection by construction.

    docs/CONTRIBUTING: never f-string a table name. This function is the *only*
    way an identifier should reach a composed SQL statement in this connector.
    """
    from psycopg import sql

    return sql.Identifier(name)


def qualified_table(
    schema_name: str, table_name: str
) -> psycopg_sql.Composed | psycopg_sql.Identifier:
    """Return ``"schema"."table"`` as a safely-composed SQL fragment."""
    from psycopg import sql

    return sql.Identifier(schema_name, table_name)


# ---------------------------------------------------------------------------
# AS OF SYSTEM TIME — follower reads
# ---------------------------------------------------------------------------


def aost_clause(as_of_system_time: str) -> psycopg_sql.Composed | None:
    """Build an ``AS OF SYSTEM TIME <expr>`` fragment, or ``None`` when disabled.

    ``as_of_system_time`` is operator-written *config*, not user data, but it
    is still composed defensively: the only value allowed through verbatim (as
    a function call) is ``follower_read_timestamp()``; every other non-empty
    value — an interval like ``-10s``, an ISO timestamp — is composed as a
    quoted :class:`psycopg.sql.Literal`, which CockroachDB accepts for both
    forms. An empty string disables the clause entirely.
    """
    from psycopg import sql

    expr = as_of_system_time.strip()
    if not expr:
        return None
    if expr in _AOST_FUNCTION_FORMS:
        return sql.SQL("AS OF SYSTEM TIME follower_read_timestamp()").format()
    return sql.SQL("AS OF SYSTEM TIME {t}").format(t=sql.Literal(expr))


def set_transaction_aost_sql(as_of_system_time: str) -> psycopg_sql.Composed | None:
    """Build ``SET TRANSACTION AS OF SYSTEM TIME <expr>`` for the full-scan path.

    A server-side cursor lives inside an explicit transaction, where
    CockroachDB rejects per-statement ``AS OF SYSTEM TIME``; the sanctioned
    form is ``SET TRANSACTION AS OF SYSTEM TIME`` as the transaction's first
    statement. Same value handling as :func:`aost_clause`; ``None`` when
    disabled.
    """
    from psycopg import sql

    expr = as_of_system_time.strip()
    if not expr:
        return None
    if expr in _AOST_FUNCTION_FORMS:
        return sql.SQL("SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()").format()
    return sql.SQL("SET TRANSACTION AS OF SYSTEM TIME {t}").format(t=sql.Literal(expr))


def _maybe_aost(as_of_system_time: str) -> psycopg_sql.Composed | psycopg_sql.SQL:
    """The AOST fragment padded for in-query composition, or an empty fragment."""
    from psycopg import sql

    clause = aost_clause(as_of_system_time)
    if clause is None:
        return sql.SQL("")
    return sql.SQL(" ").join([sql.SQL(""), clause])


# ---------------------------------------------------------------------------
# Pagination SQL builders
# ---------------------------------------------------------------------------


def keyset_select_sql(
    schema_name: str,
    table_name: str,
    cursor_field: str,
    primary_key: tuple[str, ...],
    as_of_system_time: str = "",
) -> psycopg_sql.Composed:
    """Build the cursor-keyset SELECT for an incremental table read.

    The shape:

    .. code-block:: sql

        SELECT "pk1", "pk2", * FROM "schema"."table" [AS OF SYSTEM TIME ...]
        WHERE "cursor_field" > %s
        ORDER BY "cursor_field", "pk1", "pk2", ...
        LIMIT %s

    (the explicit PK columns pull hidden ones like ``crdb_region`` into the
    result — see :func:`_select_list`)

    Two ``%s`` placeholders — the cursor floor and the page size — bound by
    the caller. Identifiers (schema, table, cursor field, every primary-key
    column) flow through :class:`psycopg.sql.Identifier`; the value never does.

    This is the *steady-state* incremental read: after bootstrap the floor is
    recent, the WHERE prunes to a small span, and with ``LIMIT`` the
    CockroachDB optimizer takes an index-backed plan (top-K at worst) instead
    of the unbounded full-scan-plus-sort that exhausts a Cockroach Cloud
    tenant's SQL memory budget. The *first* read of a large table should not
    use this shape — see :func:`pk_keyset_select_sql`.

    ``primary_key`` may be empty, in which case the ORDER BY is just the
    cursor field; the keyset is then *not* uniquely deterministic and a row
    whose cursor value ties at the page boundary may be re-read on the next
    page. The caller's de-duplication (``write_disposition: merge`` on the PK)
    handles it for declared-PK streams.
    """
    from psycopg import sql

    order_columns = [sql.Identifier(cursor_field), *(sql.Identifier(k) for k in primary_key)]
    return sql.SQL(
        "SELECT {select_list} FROM {table}{aost} WHERE {cursor_col} > %s "
        "ORDER BY {order_by} LIMIT %s"
    ).format(
        select_list=_select_list(primary_key),
        table=qualified_table(schema_name, table_name),
        aost=_maybe_aost(as_of_system_time),
        cursor_col=sql.Identifier(cursor_field),
        order_by=sql.SQL(", ").join(order_columns),
    )


def pk_keyset_select_sql(
    schema_name: str,
    table_name: str,
    primary_key: tuple[str, ...],
    first_page: bool,
    as_of_system_time: str = "",
) -> psycopg_sql.Composed:
    """Build the primary-key keyset SELECT — the bootstrap read path.

    The shape (``first_page=False``):

    .. code-block:: sql

        SELECT "pk1", "pk2", * FROM "schema"."table" [AS OF SYSTEM TIME ...]
        WHERE ("pk1", "pk2") > (%s, %s)
        ORDER BY "pk1", "pk2"
        LIMIT %s

    and without the WHERE clause for the first page. The row-value comparison
    ``(pk...) > (...)`` is exactly the primary index's order, so every page is
    a constrained index scan — no sort, no cursor-field index required, bounded
    memory regardless of table size. This is what makes the first sync of a
    multi-million-row table safe on Cockroach Cloud's fixed SQL memory budget,
    where an ``ORDER BY cursor_field`` over the whole table is killed with
    "memory budget exceeded".

    Placeholders: one per primary-key column (the resume point) when
    ``first_page`` is ``False``, then the page size. ``primary_key`` must be
    non-empty — PK pagination without a PK is a contract violation the caller
    (``source.extract_stream``) rejects before composing SQL.
    """
    from psycopg import sql

    if not primary_key:
        raise ValueError("pk_keyset_select_sql: primary_key must be non-empty")

    pk_identifiers = [sql.Identifier(k) for k in primary_key]
    order_by = sql.SQL(", ").join(pk_identifiers)
    if first_page:
        return sql.SQL(
            "SELECT {select_list} FROM {table}{aost} ORDER BY {order_by} LIMIT %s"
        ).format(
            select_list=_select_list(primary_key),
            table=qualified_table(schema_name, table_name),
            aost=_maybe_aost(as_of_system_time),
            order_by=order_by,
        )
    placeholders = sql.SQL(", ").join(sql.SQL("%s") for _ in primary_key)
    return sql.SQL(
        "SELECT {select_list} FROM {table}{aost} WHERE ({pk_cols}) > ({placeholders}) "
        "ORDER BY {order_by} LIMIT %s"
    ).format(
        select_list=_select_list(primary_key),
        table=qualified_table(schema_name, table_name),
        aost=_maybe_aost(as_of_system_time),
        pk_cols=sql.SQL(", ").join(pk_identifiers),
        placeholders=placeholders,
        order_by=order_by,
    )


def query_select_sql(
    user_query: str, cursor_field: str
) -> psycopg_sql.Composed:
    """Wrap a user-authored SELECT for incremental keyset pagination.

    Same contract as the ``postgres`` connector: the author's complete
    ``SELECT ...`` is wrapped as a subquery and the incremental WHERE /
    ORDER BY / LIMIT applied around it. If the author wants a follower read,
    ``AS OF SYSTEM TIME`` belongs inside their query text — a subquery wrap
    cannot bolt it on without changing the statement's meaning.

    # NOTE: ``user_query`` is *not* parameter-bound — it is a Composed SQL
    # fragment because the contract is "SELECT statement authored by the
    # connector author in source.py". A baked source's queries are code, not
    # user input, so the injection surface is the connector author themselves.
    # Identifiers we *do* introduce (the cursor field) go through
    # :class:`psycopg.sql.Identifier`.
    """
    from psycopg import sql

    return sql.SQL(
        "SELECT * FROM ({user_query}) AS _dtex_sub "
        "WHERE {cursor_col} > %s ORDER BY {cursor_col} LIMIT %s"
    ).format(
        user_query=sql.SQL(user_query),
        cursor_col=sql.Identifier(cursor_field),
    )


def full_scan_select_sql(
    schema_name: str, table_name: str, primary_key: tuple[str, ...] = ()
) -> psycopg_sql.Composed:
    """Build the full-table SELECT for a non-incremental table.

    Used with a server-side ``DECLARE … CURSOR`` / ``FETCH FORWARD`` loop so a
    large table is streamed in fixed-size batches without loading every row
    into memory. No per-statement AOST here — inside the explicit transaction
    the follower read is set via :func:`set_transaction_aost_sql`. Pass
    ``primary_key`` so hidden PK columns (``crdb_region``) reach the records —
    see :func:`_select_list`.
    """
    from psycopg import sql

    return sql.SQL("SELECT {select_list} FROM {table}").format(
        select_list=_select_list(primary_key),
        table=qualified_table(schema_name, table_name),
    )


def declare_cursor_sql(
    cursor_name: str, body: psycopg_sql.Composed
) -> psycopg_sql.Composed:
    """Wrap a SELECT in a ``DECLARE … CURSOR FOR`` statement.

    Server-side cursors stream a result set in batches via successive
    ``FETCH FORWARD <n>`` calls. CockroachDB supports them (v22.1+) with the
    same transaction requirement as Postgres; the caller opens one with
    ``conn.transaction()`` before executing this DECLARE.
    """
    from psycopg import sql

    return sql.SQL("DECLARE {name} CURSOR FOR {body}").format(
        name=sql.Identifier(cursor_name),
        body=body,
    )


def fetch_forward_sql(
    cursor_name: str, batch_size: int
) -> psycopg_sql.Composed:
    """Build ``FETCH FORWARD <n> FROM "cursor"`` — one server-side cursor pull."""
    from psycopg import sql

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(
            f"fetch_forward_sql: batch_size must be a positive int, got {batch_size!r}"
        )
    return sql.SQL("FETCH FORWARD {n} FROM {name}").format(
        n=sql.Literal(batch_size),
        name=sql.Identifier(cursor_name),
    )


def close_cursor_sql(cursor_name: str) -> psycopg_sql.Composed:
    """Build ``CLOSE "cursor"`` for a server-side cursor."""
    from psycopg import sql

    return sql.SQL("CLOSE {name}").format(name=sql.Identifier(cursor_name))
