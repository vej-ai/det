"""Postgres type → :class:`~simple_e.types.FieldType` mapping + schema introspection.

docs/03 §2.2.1 fixes the simpl.E :class:`~simple_e.types.FieldType` set; this
module is the Postgres-side translator. Every column type emitted by
``information_schema.columns`` maps to one of those logical types so a stream
that omits ``schema:`` from ``register.yaml`` still has a deterministic schema
the destination can create — instead of the engine inferring it from the first
batch (docs/02 §Normalize), which cannot see nullable columns absent from the
sample.

# NOTE: :func:`introspect_schema` is an **authoring-time / test helper**, not a
# runtime hook. simpl.E's contract gives a ``@stream`` function no way to hand
# a schema back to the engine — the engine reads the declared one from
# ``register.yaml`` or infers it from the first batch (runner.py
# ``_infer_schema``). This helper is therefore useful when (a) generating an
# initial ``schema:`` block for a new Postgres table, or (b) inside this
# connector's own tests, to drive the type-mapping logic end to end without a
# fully decorated stream pipeline.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from simple_e import Field, FieldMode, FieldType, Schema

if TYPE_CHECKING:
    import psycopg

# The complete, fixed mapping from a normalised Postgres type name to a
# simpl.E :class:`~simple_e.types.FieldType`. Keys are the lower-cased
# ``information_schema.columns.data_type`` / ``pg_type.typname`` values; the
# match is exact (after lower-casing) so an unfamiliar type is an error the
# author is forced to address explicitly, not a silent ``STRING`` fallback.
#
# Coverage rationale (the task's list):
#   text / varchar / char         → STRING
#   integer / bigint / smallint / serial / bigserial → INTEGER
#   numeric / real / double precision / decimal → FLOAT
#   boolean                       → BOOLEAN
#   timestamp / timestamptz / "timestamp without time zone" / "timestamp with time zone" → TIMESTAMP
#   date                          → DATE
#   json / jsonb                  → JSON
#   bytea                         → BYTES
#   uuid                          → STRING
_PG_TO_FIELD_TYPE: dict[str, FieldType] = {
    # text family
    "text": FieldType.STRING,
    "varchar": FieldType.STRING,
    "character varying": FieldType.STRING,
    "char": FieldType.STRING,
    "character": FieldType.STRING,
    "bpchar": FieldType.STRING,
    # integer family
    "smallint": FieldType.INTEGER,
    "int2": FieldType.INTEGER,
    "integer": FieldType.INTEGER,
    "int": FieldType.INTEGER,
    "int4": FieldType.INTEGER,
    "bigint": FieldType.INTEGER,
    "int8": FieldType.INTEGER,
    "serial": FieldType.INTEGER,
    "serial4": FieldType.INTEGER,
    "bigserial": FieldType.INTEGER,
    "serial8": FieldType.INTEGER,
    "smallserial": FieldType.INTEGER,
    "serial2": FieldType.INTEGER,
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
    # json
    "json": FieldType.JSON,
    "jsonb": FieldType.JSON,
    # bytes
    "bytea": FieldType.BYTES,
    # uuid → STRING per the task spec (no native UUID field type in simpl.E)
    "uuid": FieldType.STRING,
}


def postgres_to_field_type(pg_type: str) -> FieldType:
    """Map one Postgres type name to a simpl.E :class:`~simple_e.types.FieldType`.

    The match is case-insensitive on a stripped name. An unknown type is a
    :class:`ValueError` with a clear message — never a silent ``STRING``
    fallback — so the connector author is forced to either add the type to
    :data:`_PG_TO_FIELD_TYPE` or declare the column explicitly in
    ``register.yaml`` (docs/03 §2.2.1).

    docs/03 §2.2.1: the logical types are deliberately portable; a destination
    maps each to its own native type (docs/05 §3.1). This function bridges in
    the opposite direction — *from* a Postgres native name *to* the logical
    type a destination can then ground out.
    """
    if not isinstance(pg_type, str):
        raise ValueError(
            f"postgres_to_field_type expects a string, got {type(pg_type).__name__}: {pg_type!r}"
        )
    key = pg_type.strip().lower()
    if key in _PG_TO_FIELD_TYPE:
        return _PG_TO_FIELD_TYPE[key]
    known = ", ".join(sorted(_PG_TO_FIELD_TYPE))
    raise ValueError(
        f"unknown Postgres type {pg_type!r}; declare the column explicitly "
        f"in register.yaml's schema:, or extend type_mapping._PG_TO_FIELD_TYPE. "
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
    :func:`postgres_to_field_type`, and copies the ``is_nullable`` flag into a
    :class:`~simple_e.types.FieldMode` (``NO`` → ``REQUIRED``, anything else →
    ``NULLABLE``).

    # NOTE: parameter binding (``%s``) — *not* identifier quoting — is correct
    # here because ``schema_name`` and ``table_name`` are bound as *values*
    # against the literal SQL columns ``table_schema`` / ``table_name``. The
    # query never interpolates them as SQL identifiers, so there is no
    # injection surface.

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
                type=postgres_to_field_type(str(data_type)),
                mode=mode,
            )
        )
    return Schema(fields=tuple(fields))
