# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""CockroachDB type → :class:`~dtex.types.FieldType` mapping + schema introspection.

docs/03 §2.2.1 fixes the dtex :class:`~dtex.types.FieldType` set; this
module is the CockroachDB-side translator. CockroachDB reports types through
``information_schema.columns`` with (mostly) Postgres names, so the mapping
extends the Postgres one with the shapes CockroachDB actually emits:

* ``ARRAY`` — CockroachDB reports every array column (e.g. ``VARCHAR[]``) as
  the single word ``ARRAY``. Arrays map to ``JSON``: the driver yields a
  Python list and dtex's JSON path carries it through to the destination
  verbatim (normalize.py deliberately does not coerce JSON values).
* ``USER-DEFINED`` — enums, notably the ``crdb_internal_region`` values in a
  REGIONAL BY ROW table's ``crdb_region`` column. The driver yields the enum
  label as text, so ``STRING``.
* ``inet`` / ``interval`` / the ``time`` family — text on the wire for our
  purposes; ``STRING``.

# NOTE: :func:`introspect_schema` is an **authoring-time / test helper**, not a
# runtime hook. dtex's contract gives a ``@stream`` function no way to hand
# a schema back to the engine — the engine reads the declared one from
# ``register.yaml`` or infers it from the first batch (runner.py
# ``_infer_schema``). Use it to generate an initial ``schema:`` block for a
# new CockroachDB table, or inside this connector's own tests.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dtex import Field, FieldMode, FieldType, Schema

if TYPE_CHECKING:
    import psycopg

# The complete, fixed mapping from a normalised CockroachDB type name to a
# dtex :class:`~dtex.types.FieldType`. Keys are the lower-cased
# ``information_schema.columns.data_type`` values; the match is exact (after
# lower-casing) so an unfamiliar type is an error the author is forced to
# address explicitly, not a silent ``STRING`` fallback.
_CRDB_TO_FIELD_TYPE: dict[str, FieldType] = {
    # text family
    "text": FieldType.STRING,
    "varchar": FieldType.STRING,
    "character varying": FieldType.STRING,
    "char": FieldType.STRING,
    "character": FieldType.STRING,
    "bpchar": FieldType.STRING,
    '"char"': FieldType.STRING,
    # integer family — CockroachDB INT is 64-bit (int8) by default
    "smallint": FieldType.INTEGER,
    "int2": FieldType.INTEGER,
    "integer": FieldType.INTEGER,
    "int": FieldType.INTEGER,
    "int4": FieldType.INTEGER,
    "bigint": FieldType.INTEGER,
    "int8": FieldType.INTEGER,
    "oid": FieldType.INTEGER,
    # float / numeric family
    "numeric": FieldType.FLOAT,
    "decimal": FieldType.FLOAT,
    "real": FieldType.FLOAT,
    "float4": FieldType.FLOAT,
    "double precision": FieldType.FLOAT,
    "float8": FieldType.FLOAT,
    # boolean
    "boolean": FieldType.BOOLEAN,
    "bool": FieldType.BOOLEAN,
    # date / time family
    "timestamp": FieldType.TIMESTAMP,
    "timestamp without time zone": FieldType.TIMESTAMP,
    "timestamptz": FieldType.TIMESTAMP,
    "timestamp with time zone": FieldType.TIMESTAMP,
    "date": FieldType.DATE,
    "time": FieldType.STRING,
    "time without time zone": FieldType.STRING,
    "time with time zone": FieldType.STRING,
    "interval": FieldType.STRING,
    # json
    "json": FieldType.JSON,
    "jsonb": FieldType.JSON,
    # CockroachDB-specific shapes
    "array": FieldType.JSON,
    "user-defined": FieldType.STRING,
    "inet": FieldType.STRING,
    # bytes
    "bytea": FieldType.BYTES,
    # uuid → STRING (no native UUID field type in dtex)
    "uuid": FieldType.STRING,
}


def cockroachdb_to_field_type(crdb_type: str) -> FieldType:
    """Map one CockroachDB type name to a dtex :class:`~dtex.types.FieldType`.

    The match is case-insensitive on a stripped name. An unknown type is a
    :class:`ValueError` with a clear message — never a silent ``STRING``
    fallback — so the connector author is forced to either add the type to
    :data:`_CRDB_TO_FIELD_TYPE` or declare the column explicitly in
    ``register.yaml`` (docs/03 §2.2.1).
    """
    if not isinstance(crdb_type, str):
        raise ValueError(
            f"cockroachdb_to_field_type expects a string, "
            f"got {type(crdb_type).__name__}: {crdb_type!r}"
        )
    key = crdb_type.strip().lower()
    if key in _CRDB_TO_FIELD_TYPE:
        return _CRDB_TO_FIELD_TYPE[key]
    known = ", ".join(sorted(_CRDB_TO_FIELD_TYPE))
    raise ValueError(
        f"unknown CockroachDB type {crdb_type!r}; declare the column explicitly "
        f"in register.yaml's schema:, or extend type_mapping._CRDB_TO_FIELD_TYPE. "
        f"Known types: {known}"
    )


def introspect_schema(
    conn: psycopg.Connection[Any],
    schema_name: str,
    table_name: str,
) -> Schema:
    """Build a :class:`Schema` for ``schema_name.table_name`` from ``information_schema``.

    Reads one row per column from ``information_schema.columns`` in the
    declared ordinal order, maps each ``data_type`` through
    :func:`cockroachdb_to_field_type`, and copies the ``is_nullable`` flag into
    a :class:`~dtex.types.FieldMode` (``NO`` → ``REQUIRED``, anything else →
    ``NULLABLE``).

    # NOTE: parameter binding (``%s``) — *not* identifier quoting — is correct
    # here because ``schema_name`` and ``table_name`` are bound as *values*
    # against the literal SQL columns ``table_schema`` / ``table_name``. The
    # query never interpolates them as SQL identifiers, so there is no
    # injection surface. The hidden ``crdb_region`` column of REGIONAL BY ROW
    # tables IS visible to ``information_schema`` and is included — it is part
    # of the primary key and belongs in the extracted schema.

    Raises :class:`ValueError` if the table has no columns visible to the
    connecting user (a missing table or a permissions issue surfaces as
    ``Schema(fields=())``, which the caller would otherwise silently accept).
    """
    sql = (
        "SELECT column_name, data_type, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (schema_name, table_name))
        rows = cur.fetchall()
    if not rows:
        raise ValueError(
            f"introspect_schema: no columns found for {schema_name}.{table_name} — "
            f"the table may be missing or the user may lack SELECT privileges"
        )
    fields: list[Field] = []
    for row in rows:
        column_name, data_type, is_nullable = row[0], row[1], row[2]
        mode = (
            FieldMode.REQUIRED
            if str(is_nullable).strip().upper() == "NO"
            else FieldMode.NULLABLE
        )
        fields.append(
            Field(
                name=str(column_name),
                type=cockroachdb_to_field_type(str(data_type)),
                mode=mode,
            )
        )
    return Schema(fields=tuple(fields))
