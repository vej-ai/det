"""DDL + identifier helpers for the DuckDB destination — docs/05 §3.

Keeps ``destination.py`` focused on the lifecycle hooks. This module owns:

* the det :class:`~det.types.FieldType` → DuckDB native-type mapping
  (docs/05 §3.1);
* identifier validation + quoting, so a table or column name can never be a
  SQL-injection vector (the task's quality bar);
* the small DDL fragments ``ensure_schema`` emits (``CREATE TABLE`` /
  ``ALTER TABLE ADD COLUMN``).

The mapping is total over :class:`~det.types.FieldType`'s eight members.
"""

from __future__ import annotations

import re

from det.types import FieldType, Schema

# --------------------------------------------------------------------------
# Type mapping — docs/05 §3.1
# --------------------------------------------------------------------------

_FIELD_TYPE_TO_DUCKDB: dict[FieldType, str] = {
    FieldType.STRING: "VARCHAR",
    FieldType.INTEGER: "BIGINT",
    FieldType.FLOAT: "DOUBLE",
    FieldType.BOOLEAN: "BOOLEAN",
    FieldType.TIMESTAMP: "TIMESTAMP",
    FieldType.DATE: "DATE",
    FieldType.JSON: "JSON",
    FieldType.BYTES: "BLOB",
}
"""det :class:`FieldType` → DuckDB column type — docs/05 §3.1.

The full set of eight logical types; every member of ``FieldType`` has an
entry, so :func:`duckdb_type` is total.
"""


def duckdb_type(field_type: FieldType) -> str:
    """Return the DuckDB native type for a det :class:`FieldType` — docs/05 §3.1.

    Total over :class:`FieldType`; a member with no mapping (impossible while
    ``_FIELD_TYPE_TO_DUCKDB`` covers the enum) raises :class:`KeyError` rather
    than silently producing bad DDL.
    """
    return _FIELD_TYPE_TO_DUCKDB[field_type]


# --------------------------------------------------------------------------
# Identifier safety — validate, then quote
# --------------------------------------------------------------------------

# A safe SQL identifier: a letter or underscore, then letters/digits/underscores.
# Underscore-leading is allowed on purpose — the engine's own tables/columns
# (``_det_state``, ``_det_synced_at``) start with one.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str, *, kind: str = "identifier") -> str:
    """Reject any name that is not a safe SQL identifier — the task's quality bar.

    det table and column names come from ``register.yaml`` and from source
    records, so they are *untrusted* as far as SQL construction goes. This is
    the gate: a name that does not match :data:`_IDENTIFIER_RE` raises
    :class:`ValueError` before it ever reaches a SQL string. ``kind`` names the
    offending category (``table``, ``column``, ``dataset``) for the message.
    """
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"unsafe {kind} name {name!r}: a {kind} must match "
            f"[A-Za-z_][A-Za-z0-9_]* (letters, digits, underscore; no leading digit)"
        )
    return name


def quote_identifier(name: str, *, kind: str = "identifier") -> str:
    """Validate ``name`` then return it double-quoted for use in SQL.

    Two layers of defence: :func:`validate_identifier` rejects anything that is
    not an identifier at all, and the ``"…"`` quoting (with ``"`` doubled, the
    SQL-standard escape DuckDB honors) makes a name that *is* a valid
    identifier but collides with a reserved word — ``order``, ``select`` —
    safe to use as a table or column.
    """
    validate_identifier(name, kind=kind)
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def qualified_table(dataset: str | None, table: str) -> str:
    """Return the SQL-safe, optionally schema-qualified table reference.

    ``dataset`` is the DuckDB schema name (``register.yaml`` ``dataset`` param);
    when ``None`` the table lives in the connection's default schema. Both parts
    are validated + quoted by :func:`quote_identifier`.
    """
    qtable = quote_identifier(table, kind="table")
    if dataset is None:
        return qtable
    return f"{quote_identifier(dataset, kind='dataset')}.{qtable}"


# --------------------------------------------------------------------------
# DDL fragment builders
# --------------------------------------------------------------------------


def create_table_sql(dataset: str | None, table: str, schema: Schema) -> str:
    """Build the ``CREATE TABLE IF NOT EXISTS`` statement for a stream's schema.

    docs/05 §3.1: the destination translates a :class:`Schema` into native DDL.
    Each column name is validated + quoted; each type is mapped via
    :func:`duckdb_type`. ``IF NOT EXISTS`` makes first-run table creation
    idempotent across resumed runs.
    """
    cols = ", ".join(
        f"{quote_identifier(f.name, kind='column')} {duckdb_type(f.type)}"
        for f in schema.fields
    )
    return f"CREATE TABLE IF NOT EXISTS {qualified_table(dataset, table)} ({cols})"


def add_column_sql(dataset: str | None, table: str, column: str, field_type: FieldType) -> str:
    """Build an additive ``ALTER TABLE ADD COLUMN`` statement — docs/05 §3.2.

    The ``IF NOT EXISTS`` clause (DuckDB supports it) makes additive evolution
    idempotent: re-running against a table that already has the column is a
    no-op, not an error.
    """
    return (
        f"ALTER TABLE {qualified_table(dataset, table)} "
        f"ADD COLUMN IF NOT EXISTS {quote_identifier(column, kind='column')} "
        f"{duckdb_type(field_type)}"
    )
