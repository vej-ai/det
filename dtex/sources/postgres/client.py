# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Postgres connection + safe-SQL helpers for the dtex Postgres connector.

Connection management, identifier quoting, and pagination-SQL construction —
the plumbing the ``@stream`` functions in :mod:`source` build on. Every
identifier flows through :class:`psycopg.sql.Identifier`; every value flows
through ``%s`` parameter binding. The connector never f-strings a table name
or a cursor value.

# NOTE: this module imports ``psycopg`` (v3) lazily *inside* functions, not at
# module top. The connector folder is imported at discovery time
# (dtex/engine/discovery.py), which runs before the engine has confirmed
# the run actually targets Postgres — a missing ``psycopg`` would otherwise
# break unrelated runs that merely scanned this folder. The driver is a
# declared runtime dependency (pyproject.toml ``psycopg[binary]>=3.1``); it is
# guaranteed to be installed when a Postgres run actually opens a connection,
# but only loaded at that moment.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dtex import Config

if TYPE_CHECKING:
    import psycopg
    from psycopg import sql as psycopg_sql


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------


def connect(config: Config) -> psycopg.Connection[Any]:
    """Open a Postgres connection from a resolved :class:`Config` — lazy per stream.

    docs/03 §3: ``Config`` carries resolved params (``host``, ``port``,
    ``database``, ``user``, ``sslmode``, ``application_name``,
    ``connect_timeout_seconds``) plus secrets (``password``). The connection is
    opened at the moment a ``@stream`` generator first iterates, never at
    module import — the connector body owns the lifecycle and closes the
    connection when the generator is exhausted or raises (see
    :func:`source._with_connection`).

    # NOTE: the password is *never* logged. ``Config.secrets`` is read by
    # explicit subscript here (docs/03 §3) and handed straight to
    # ``psycopg.connect`` as a keyword argument; nothing in the path stringifies
    # the dict containing it.
    """
    import psycopg

    password = config.secrets["password"]
    return psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=password,
        sslmode=config.sslmode,
        application_name=config.application_name,
        connect_timeout=config.connect_timeout_seconds,
    )


# ---------------------------------------------------------------------------
# Identifier quoting — psycopg.sql.Identifier is the *only* path
# ---------------------------------------------------------------------------


def quote_identifier(name: str) -> psycopg_sql.Identifier:
    """Wrap a Postgres identifier so it is safe to compose into a SQL statement.

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
    """Return ``"schema"."table"`` as a safely-composed SQL fragment.

    :class:`psycopg.sql.Identifier` accepts multiple parts and emits the
    correctly-quoted, dot-joined identifier — exactly what a Postgres
    ``schema.table`` qualifier needs and exactly what string-formatting cannot
    safely produce.
    """
    from psycopg import sql

    return sql.Identifier(schema_name, table_name)


# ---------------------------------------------------------------------------
# Pagination SQL builders
# ---------------------------------------------------------------------------


def keyset_select_sql(
    schema_name: str,
    table_name: str,
    cursor_field: str,
    primary_key: tuple[str, ...],
) -> psycopg_sql.Composed:
    """Build the keyset-pagination SELECT for an incremental table read.

    The shape:

    .. code-block:: sql

        SELECT * FROM "schema"."table"
        WHERE "cursor_field" > %s
        ORDER BY "cursor_field", "pk1", "pk2", ...
        LIMIT %s

    Two ``%s`` placeholders — the cursor floor and the page size — bound by
    the caller. Identifiers (schema, table, cursor field, every primary-key
    column) flow through :class:`psycopg.sql.Identifier`; the value never does.

    docs/03 §2.2: keyset (cursor + PK) pagination is the contract dtex
    promises for incremental Postgres reads — no ``LIMIT N OFFSET M`` (that
    scales O(N²) on large tables, as the task's quality bar fixes).

    ``primary_key`` may be empty, in which case the ORDER BY is just the
    cursor field; the keyset is then *not* uniquely deterministic and a row
    whose cursor value ties at the page boundary may be re-read on the next
    page. The caller's de-duplication (``write_disposition: merge`` on the PK)
    handles it for declared-PK streams; for a PK-less stream the caller must
    accept the at-most-once-plus-edge-duplicates property.
    """
    from psycopg import sql

    order_columns = [sql.Identifier(cursor_field), *(sql.Identifier(k) for k in primary_key)]
    return sql.SQL(
        "SELECT * FROM {table} WHERE {cursor_col} > %s "
        "ORDER BY {order_by} LIMIT %s"
    ).format(
        table=qualified_table(schema_name, table_name),
        cursor_col=sql.Identifier(cursor_field),
        order_by=sql.SQL(", ").join(order_columns),
    )


def query_select_sql(
    user_query: str, cursor_field: str
) -> psycopg_sql.Composed:
    """Wrap a user-authored SELECT for incremental keyset pagination.

    The user provides a complete ``SELECT ...`` statement; we wrap it as a
    subquery and apply the incremental WHERE / ORDER BY / LIMIT around it:

    .. code-block:: sql

        SELECT * FROM ({user_query}) AS _dtex_sub
        WHERE "cursor_field" > %s
        ORDER BY "cursor_field" LIMIT %s

    Wrapping (rather than appending ``WHERE`` to the user's text) lets the
    user's query carry its own ``ORDER BY``, ``GROUP BY``, ``UNION`` or
    aggregates without breaking the keyset clause we add. The requirement on
    the user: their ``SELECT`` must project ``cursor_field`` as a top-level
    column so the outer query can reference it.

    # NOTE: ``user_query`` is *not* parameter-bound — it is a Composed SQL
    # fragment because the contract is "Postgres SELECT statement authored by
    # the connector author in source.py". A baked source's queries are code,
    # not user input, so the injection surface is the connector author
    # themselves. Identifiers we *do* introduce (the cursor field) go through
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
    schema_name: str, table_name: str
) -> psycopg_sql.Composed:
    """Build the ``SELECT * FROM "schema"."table"`` for a non-incremental table.

    Used with a server-side ``DECLARE … CURSOR`` / ``FETCH FORWARD`` loop so a
    large table is streamed in fixed-size batches without loading every row
    into memory. The cursor name itself is generated by the caller (see
    :func:`declare_cursor_sql`) so the lifecycle stays explicit.
    """
    from psycopg import sql

    return sql.SQL("SELECT * FROM {table}").format(
        table=qualified_table(schema_name, table_name),
    )


def declare_cursor_sql(
    cursor_name: str, body: psycopg_sql.Composed
) -> psycopg_sql.Composed:
    """Wrap a SELECT in a ``DECLARE … CURSOR FOR`` statement.

    Server-side cursors stream a result set in batches via successive
    ``FETCH FORWARD <n>`` calls — the alternative to client-side
    materialisation. The cursor name itself is a Postgres identifier and so
    goes through :class:`psycopg.sql.Identifier`.

    Server-side cursors require an *open transaction* (Postgres rule); the
    caller opens one with ``conn.transaction()`` before executing this
    DECLARE.
    """
    from psycopg import sql

    return sql.SQL("DECLARE {name} CURSOR FOR {body}").format(
        name=sql.Identifier(cursor_name),
        body=body,
    )


def fetch_forward_sql(
    cursor_name: str, batch_size: int
) -> psycopg_sql.Composed:
    """Build ``FETCH FORWARD <n> FROM "cursor"`` — one server-side cursor pull.

    ``batch_size`` is composed as a SQL literal (it is an int, not user input);
    the cursor name is an identifier and goes through
    :class:`psycopg.sql.Identifier`.
    """
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
