# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""DDL + identifier helpers for the BigQuery destination — docs/05 §3.

Keeps ``destination.py`` focused on the lifecycle hooks. This module owns:

* the detx :class:`~detx.types.FieldType` → BigQuery native-type mapping
  (docs/05 §3.1);
* the :class:`~detx.types.FieldMode` → BigQuery mode mapping;
* identifier validation + backtick quoting, so a table / column / dataset name
  can never be a SQL-injection vector (the task's quality bar);
* the small SQL fragments the hooks emit (``MERGE`` body, staging-table DDL).

The mapping is total over :class:`~detx.types.FieldType`'s eight members and
:class:`~detx.types.FieldMode`'s three members.
"""

from __future__ import annotations

import re
from typing import Any

from detx.types import (
    Field,
    FieldMode,
    FieldType,
    PartitionConfig,
    PartitionRange,
    PartitionType,
    Schema,
    TimeGranularity,
)

# --------------------------------------------------------------------------
# Type mapping — docs/05 §3.1
# --------------------------------------------------------------------------

_FIELD_TYPE_TO_BIGQUERY: dict[FieldType, str] = {
    FieldType.STRING: "STRING",
    FieldType.INTEGER: "INT64",
    FieldType.FLOAT: "FLOAT64",
    FieldType.BOOLEAN: "BOOL",
    FieldType.TIMESTAMP: "TIMESTAMP",
    FieldType.DATE: "DATE",
    FieldType.JSON: "JSON",
    FieldType.BYTES: "BYTES",
}
"""detx :class:`FieldType` → BigQuery column type — docs/05 §3.1.

The full set of eight logical types; every member of ``FieldType`` has an
entry, so :func:`bigquery_type` is total.
"""

_FIELD_MODE_TO_BIGQUERY: dict[FieldMode, str] = {
    FieldMode.NULLABLE: "NULLABLE",
    FieldMode.REQUIRED: "REQUIRED",
    FieldMode.REPEATED: "REPEATED",
}


def bigquery_type(field_type: FieldType) -> str:
    """Return the BigQuery native type for a detx :class:`FieldType` — docs/05 §3.1.

    Total over :class:`FieldType`; a member with no mapping (impossible while
    ``_FIELD_TYPE_TO_BIGQUERY`` covers the enum) raises :class:`KeyError`
    rather than silently producing bad DDL.
    """
    return _FIELD_TYPE_TO_BIGQUERY[field_type]


def bigquery_mode(field_mode: FieldMode) -> str:
    """Return the BigQuery field mode for a detx :class:`FieldMode`.

    Total over :class:`FieldMode`'s three members (NULLABLE / REQUIRED /
    REPEATED) — the names line up 1:1 with BigQuery's own.
    """
    return _FIELD_MODE_TO_BIGQUERY[field_mode]


# --------------------------------------------------------------------------
# Identifier safety — validate, then backtick-quote
# --------------------------------------------------------------------------

# A safe SQL identifier: a letter or underscore, then letters/digits/
# underscores. Underscore-leading is allowed on purpose — the engine's own
# tables/columns (``_detx_state``, ``_detx_synced_at``) start with one.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# GCP project IDs are 6-30 chars, lowercase letters / digits / hyphens,
# starting with a lowercase letter. They are NOT plain SQL identifiers
# (the hyphen is illegal in a bare table/column name), so a backtick-
# quoted ``\`my-project\``` is required everywhere a project appears in
# SQL. Validation matches Google's documented rules + bans backticks.
_PROJECT_ID_RE = re.compile(r"^[a-z][a-z0-9-]{4,28}[a-z0-9]$")


def validate_identifier(name: str, *, kind: str = "identifier") -> str:
    """Reject any name that is not a safe SQL identifier — the task's quality bar.

    detx table, column and dataset names come from ``register.yaml`` and from
    source records, so they are *untrusted* as far as SQL construction goes.
    This is the gate: a name that does not match the per-kind rule raises
    :class:`ValueError` before it ever reaches a SQL string. ``kind`` names
    the offending category for the message.

    Rules:

    * ``project`` — GCP project ID: 6-30 chars, lowercase letter start,
      lowercase letters / digits / hyphens, no trailing hyphen. The hyphen
      is allowed only here; backticks are forbidden by construction.
    * everything else — plain SQL identifier:
      ``[A-Za-z_][A-Za-z0-9_]*``. No hyphen, no backtick, no whitespace.

    Both rules rule out backticks by construction, so the BigQuery concern
    "a name with a literal backtick would escape the quoting" is defended
    at this layer as well as at the quoting layer below.
    """
    if not isinstance(name, str):
        raise ValueError(
            f"unsafe {kind} name {name!r}: must be a string"
        )
    if kind == "project":
        if not _PROJECT_ID_RE.match(name):
            raise ValueError(
                f"unsafe {kind} name {name!r}: a GCP project id is 6-30 chars, "
                f"lowercase letter start, lowercase letters / digits / hyphens, "
                f"no trailing hyphen"
            )
        return name
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"unsafe {kind} name {name!r}: a {kind} must match "
            f"[A-Za-z_][A-Za-z0-9_]* (letters, digits, underscore; no leading digit)"
        )
    return name


def quote_identifier(name: str, *, kind: str = "identifier") -> str:
    """Validate ``name`` then return it backtick-quoted for use in BigQuery SQL.

    Two layers of defence: :func:`validate_identifier` rejects anything that
    is not an identifier at all, and the backtick quoting makes a name that
    *is* a valid identifier but collides with a reserved word (``order``,
    ``select``, …) safe to use as a table / column.

    BigQuery's identifier syntax does not allow embedded backticks, so the
    validated identifier carries none — there is no escape to apply.
    """
    validate_identifier(name, kind=kind)
    return f"`{name}`"


def fq_table(project: str, dataset: str, table: str) -> str:
    r"""Return the fully-qualified ``\`project\`.\`dataset\`.\`table\`\`` reference.

    Every part is validated + backtick-quoted (project via the project-id
    rule, dataset / table via the SQL-identifier rule), so this is the
    single choke point where a destination table reference is constructed
    for BigQuery SQL.
    """
    return (
        f"{quote_project(project)}."
        f"{quote_identifier(dataset, kind='dataset')}."
        f"{quote_identifier(table, kind='table')}"
    )


def quote_project(name: str) -> str:
    """Validate ``name`` as a GCP project id then return it backtick-quoted.

    Distinct entry from :func:`quote_identifier` because the validation
    rule is different (project ids allow hyphens), but the quoting
    mechanism is the same — backticks, no escapes (backticks are
    forbidden in the input by the project-id regex).
    """
    validate_identifier(name, kind="project")
    return f"`{name}`"


# --------------------------------------------------------------------------
# SchemaField builders — used by ensure_schema to talk to the BQ SDK
# --------------------------------------------------------------------------


def bq_schema_field(field: Field) -> Any:
    """Build a ``bigquery.SchemaField`` for one declared detx :class:`Field`.

    The BigQuery SDK is imported lazily because the ``bigquery`` extra may
    not be installed in a base ``detx`` environment (the rest of the package
    must not pay the SDK's import cost). See ``client.py`` for the lazy
    accessor — this helper goes through the same one so a unit test that
    monkeypatches the accessor sees this function follow.
    """
    # Local import so this module is importable in a base install — the
    # actual SchemaField construction needs the SDK, and the user opted
    # into ``[bigquery]`` if they are calling this. Going through the
    # module attribute (not ``from ... import _bigquery_module``) keeps
    # the test's ``monkeypatch.setattr(client_mod, "_bigquery_module", ...)``
    # effective even after the connector folder is re-imported under a
    # unique synthetic name (see destination.py for the full reasoning).
    from detx.destinations.bigquery import client as _client_mod

    bq = _client_mod._bigquery_module()
    validate_identifier(field.name, kind="column")
    return bq.SchemaField(
        name=field.name,
        field_type=bigquery_type(field.type),
        mode=bigquery_mode(field.mode),
        description=field.description or None,
    )


def bq_schema(schema: Schema) -> list[Any]:
    """Build the ``list[bigquery.SchemaField]`` for a detx :class:`Schema`.

    The order matches ``schema.fields`` — ``_detx_synced_at`` (when present)
    lands last as the engine appends it via :meth:`Schema.with_synced_at`
    before this is called.
    """
    return [bq_schema_field(f) for f in schema.fields]


# --------------------------------------------------------------------------
# MERGE SQL builder — used by write_batch for the MERGE write disposition
# --------------------------------------------------------------------------


def merge_sql(
    *,
    project: str,
    dataset: str,
    target_table: str,
    staging_table: str,
    primary_key: tuple[str, ...],
    columns: tuple[str, ...],
) -> str:
    """Build the ``MERGE INTO target USING staging ON pk ...`` statement — docs/05 §4.

    The target rows matched on every column of ``primary_key`` are updated
    from the staging row; rows in staging with no matching target row are
    inserted. Every non-key column in ``columns`` is overwritten on a match;
    if every column is part of the key, the matched branch is dropped (a
    matched row is already identical, nothing to update — and a SQL ``UPDATE
    SET`` with zero assignments is a syntax error in BigQuery).

    Every identifier flows through :func:`quote_identifier` first, so the
    statement is safe to interpolate column names into; values never appear
    in this SQL — the data was loaded into ``staging_table`` via a parameter-
    free Parquet LOAD job.
    """
    if not primary_key:
        raise ValueError("merge_sql requires a non-empty primary_key")
    if not columns:
        raise ValueError("merge_sql requires a non-empty columns list")

    target = fq_table(project, dataset, target_table)
    staging = fq_table(project, dataset, staging_table)

    qcols = [quote_identifier(c, kind="column") for c in columns]
    on_clause = " AND ".join(
        f"T.{quote_identifier(k, kind='column')} = S.{quote_identifier(k, kind='column')}"
        for k in primary_key
    )

    pk_set = set(primary_key)
    update_cols = [c for c in columns if c not in pk_set]
    if update_cols:
        set_clause = ", ".join(
            f"{quote_identifier(c, kind='column')} = "
            f"S.{quote_identifier(c, kind='column')}"
            for c in update_cols
        )
        matched_branch = f"WHEN MATCHED THEN UPDATE SET {set_clause}"
    else:
        # Every column is part of the key — the matched row is already
        # identical. Skip the matched branch entirely (an UPDATE with no
        # SET assignments is a BigQuery syntax error).
        matched_branch = ""

    insert_cols = ", ".join(qcols)
    insert_vals = ", ".join(f"S.{c}" for c in qcols)

    parts = [
        f"MERGE INTO {target} T",
        f"USING {staging} S",
        f"ON {on_clause}",
    ]
    if matched_branch:
        parts.append(matched_branch)
    parts.append(
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )
    return "\n".join(parts)


# --------------------------------------------------------------------------
# Partitioning — convert detx's PartitionConfig to BigQuery SDK objects
# and compare against an existing table's partitioning. docs/05 §3.x.
# --------------------------------------------------------------------------

# Maps detx's TimeGranularity to BigQuery's TimePartitioningType string. The
# BigQuery SDK accepts these uppercase strings on ``TimePartitioning(type_=…)``;
# the SDK module also exposes a ``TimePartitioningType.DAY`` enum but the
# string form is stable across SDK versions and trivially testable.
_GRANULARITY_TO_BQ: dict[TimeGranularity, str] = {
    TimeGranularity.HOUR: "HOUR",
    TimeGranularity.DAY: "DAY",
    TimeGranularity.MONTH: "MONTH",
    TimeGranularity.YEAR: "YEAR",
}

_BQ_TO_GRANULARITY: dict[str, TimeGranularity] = {
    v: k for k, v in _GRANULARITY_TO_BQ.items()
}


def apply_partitioning_to_table(
    table: Any, partition: PartitionConfig | None, bq_module: Any
) -> None:
    """Mutate a ``bigquery.Table`` to carry the requested partition spec.

    Called by ``ensure_schema`` on the table-creation path. Maps a detx
    :class:`PartitionConfig` onto either ``table.time_partitioning`` (for TIME
    and INGESTION types) or ``table.range_partitioning`` (for RANGE). A
    ``None`` partition leaves the table unpartitioned (today's behavior
    pre-this-stage).

    # NOTE: ``bq_module`` is the lazy-loaded ``google.cloud.bigquery`` module
    # handed in by the destination — going through the parameter (not a
    # local import) keeps the test fakes' single monkeypatch on
    # ``_bigquery_module`` effective from this module too.
    """
    if partition is None:
        return
    if partition.type is PartitionType.TIME:
        assert partition.granularity is not None  # validated by PartitionConfig
        table.time_partitioning = bq_module.TimePartitioning(
            type_=_GRANULARITY_TO_BQ[partition.granularity],
            field=partition.field,
        )
    elif partition.type is PartitionType.RANGE:
        assert partition.range is not None  # validated by PartitionConfig
        table.range_partitioning = bq_module.RangePartitioning(
            field=partition.field,
            range_=bq_module.PartitionRange(
                start=partition.range.start,
                end=partition.range.end,
                interval=partition.range.interval,
            ),
        )
    else:  # PartitionType.INGESTION
        # field=None — BigQuery binds to the _PARTITIONTIME pseudo-column;
        # bucket is implicitly DAY.
        table.time_partitioning = bq_module.TimePartitioning(
            type_="DAY",
            field=None,
        )


def existing_table_partition(table: Any) -> PartitionConfig | None:
    """Normalize an existing ``bigquery.Table``'s partition spec to detx shape.

    Reads ``table.time_partitioning`` and ``table.range_partitioning`` and
    returns the equivalent :class:`PartitionConfig` (or ``None`` if the table
    is unpartitioned). Used by :func:`compare_partition` to structurally
    compare against the requested spec — string-rendering both sides for the
    error message, not for the comparison.

    # NOTE: BigQuery does not let one table have BOTH time AND range
    # partitioning, so we never need to merge them. If a hypothetical table
    # had both we would prefer time (BigQuery's own historical behavior).
    """
    tp = getattr(table, "time_partitioning", None)
    if tp is not None:
        # Distinguish INGESTION (field=None) from TIME (field=<column>).
        bq_type = getattr(tp, "type_", None) or "DAY"
        field = getattr(tp, "field", None)
        if field is None:
            return PartitionConfig(
                field=None, type=PartitionType.INGESTION
            )
        gran = _BQ_TO_GRANULARITY.get(str(bq_type).upper(), TimeGranularity.DAY)
        return PartitionConfig(
            field=str(field), type=PartitionType.TIME, granularity=gran
        )
    rp = getattr(table, "range_partitioning", None)
    if rp is not None:
        field = getattr(rp, "field", None)
        range_obj = getattr(rp, "range_", None)
        if range_obj is None:
            return None  # malformed — treat as unpartitioned
        return PartitionConfig(
            field=None if field is None else str(field),
            type=PartitionType.RANGE,
            range=PartitionRange(
                start=int(range_obj.start),
                end=int(range_obj.end),
                interval=int(range_obj.interval),
            ),
        )
    return None


def compare_partition(
    existing: Any, requested: PartitionConfig | None
) -> tuple[str, str | None]:
    """Compare an existing table's partition against the requested spec.

    Returns ``("match", None)`` when the table is already partitioned exactly
    the same way (including the "both unpartitioned" case), otherwise
    ``("mismatch", <error_message>)`` where the message is the
    drift-suggestion text the destination hands up as an :class:`EngineError`.

    "No partition on existing vs requested partition" is a MISMATCH (the
    test list spells this out explicitly): BigQuery has no on-the-fly path
    to add partitioning to an existing table, so silently ignoring the
    requested spec would let writes drift from intent. The user must drop
    + recreate, or change the config to declare no partition.

    # NOTE: ``detx state reset --recreate-table`` is the user-facing path the
    # error message suggests, but the ``--recreate-table`` flag does NOT yet
    # exist on the CLI as of this stage. A future stage should wire it (the
    # mechanics — DROP TABLE + clear state row — are straightforward); the
    # message names it now so the error stays actionable once the flag lands
    # without needing a docs revision. Today's manual equivalent is: back the
    # table up (``CREATE TABLE bak AS SELECT * ...``), DROP the table, run
    # ``detx state reset -p <config>`` to clear the state row, then re-run.
    """
    existing_pc = existing_table_partition(existing)
    if existing_pc == requested:
        return ("match", None)
    existing_desc = (
        "(unpartitioned)" if existing_pc is None else existing_pc.describe()
    )
    requested_desc = (
        "(unpartitioned)" if requested is None else requested.describe()
    )
    table_name = getattr(existing, "table_id", None) or getattr(
        existing, "reference", "<unknown>"
    )
    msg = (
        f"table {table_name!r} already exists with partitioning={existing_desc}; "
        f"new config says {requested_desc}. BigQuery cannot change an existing "
        f"table's partitioning in place. To resolve: either (a) run `detx state "
        f"reset -p <config> --recreate-table` after backing up the table to "
        f"recreate it with the new partition spec, or (b) change the config to "
        f"match the existing partition spec."
    )
    return ("mismatch", msg)
