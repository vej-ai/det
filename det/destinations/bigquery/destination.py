"""The BigQuery destination connector body — the ``@destination`` hooks.

docs/05 §2 catalog row: "Batches staged as Parquet to a temp GCS prefix, then
``LOAD`` job; merge via ``MERGE`` statement. Tier A. v1." This is the
production warehouse det was always meant to land in — built to the same
contract as the DuckDB reference (see ``det/destinations/duckdb/``), with
BigQuery's primitives swapped in for DuckDB's:

* DuckDB ``INSERT ... VALUES (...)``        → GCS-staged Parquet + LOAD job
* DuckDB ``INSERT ... ON CONFLICT``         → LOAD into staging table + MERGE
* DuckDB ``DELETE FROM`` (replace)          → LOAD job with ``WRITE_TRUNCATE``
* DuckDB ``CREATE TABLE IF NOT EXISTS``     → ``client.create_table(exists_ok=True)``
* DuckDB ``ALTER TABLE ADD COLUMN``         → ``client.update_table(t, ['schema'])``

Hook contract — docs/03 §3.4 / docs/05 §1, exact signatures:

* ``capabilities() -> set[Capability]``
* ``open(config) -> conn``
* ``ensure_schema(conn, stream) -> None``
* ``write_batch(conn, batch, stream) -> int``
* ``read_state(conn, connector) -> list[StateRecord]``
* ``commit_state(conn, run_id, records) -> None``
* ``write_run_record(conn, record) -> None``       (Capability.RUN_RECORDS)
* ``close(conn) -> None``

The engine drives them in the order
``open → read_state → [ensure_schema → write_batch ...]* → commit_state →
write_run_record → close``; ``close`` always runs.

# NOTE: ``@destination.transaction`` is deliberately NOT defined, and the
# destination does NOT declare ``Capability.TRANSACTIONAL_LOAD``. BigQuery
# has no general BEGIN/COMMIT spanning multiple load jobs; the natural
# atomic unit is one ``LOAD`` / one ``MERGE`` (each commits or doesn't).
# Per-batch atomicity is BigQuery's real granularity here. To advertise
# TRANSACTIONAL_LOAD honestly we would need to buffer every batch of a
# stream into a single staging table and run one MERGE on the context-
# manager's ``__exit__`` — which breaks the streaming-load promise (no
# rows visible until the stream finishes). v1 chooses honesty: per-batch
# commit, no TRANSACTIONAL_LOAD claim, no ``transaction`` hook. The engine
# wraps each stream in ``nullcontext()`` accordingly (see
# ``det/engine/runner.py::_stream_transaction``). A future opt-in
# ``staged_merge: true`` param would buffer and is a v2 concern.

# NOTE: ``@destination.state_backend`` is deliberately NOT defined either.
# BigQuery is Tier A (it declares ``Capability.STATE``), so per docs/05 §5.4
# it *is* its own state backend; ``state_backend`` is the Tier-B-only hook.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

from det import (
    Batch,
    Capability,
    Config,
    RunRecord,
    Schema,
    StateRecord,
    StreamMeta,
    WriteDisposition,
    destination,
)
from det.destinations.bigquery import client as _client_mod
from det.destinations.bigquery.client import (
    BigQueryClient,
    BQConn,
    build_client,
    run_with_retries,
)
from det.destinations.bigquery.ddl import (
    apply_partitioning_to_table,
    bigquery_type,
    bq_schema,
    bq_schema_field,
    compare_partition,
    fq_table,
    merge_sql,
    validate_identifier,
)
from det.types import Field, FieldType


# # NOTE: ``PartitionDriftError`` is intentionally a destination-local
# exception class, not the engine's :class:`~det.engine.runner.EngineError`.
# The task spec asks for an EngineError on partition drift, but a
# destination importing from ``det.engine.runner`` would introduce a reverse
# dependency (the engine imports destinations, not the other way around).
# The engine catches every :class:`Exception` from a destination hook and
# folds it into a ``FAILED`` :class:`~det.types.RunResult`, so a
# destination-local subclass surfaces identically to the user — the
# ``status=FAILED`` RunResult carries this exact instance as its ``error``.
# The class is named to be search-friendly in stack traces and logs.
class PartitionDriftError(RuntimeError):
    """An existing table's partition spec does not match the requested one.

    Raised by :func:`ensure_schema` when the destination table exists and is
    partitioned differently from what the resolved
    :class:`~det.types.PartitionConfig` requests — see :func:`compare_partition`
    in ``ddl.py`` for the structural comparison rule. The error message
    includes both partition specs and the suggested operator action so the
    failure is actionable from the log alone.
    """


# Reach for the lazy SDK accessors through the client module each call —
# *not* via ``from ... import _bigquery_module`` at module load. The
# connector-folder harness imports the file under a unique synthetic name
# and removes it from ``sys.modules`` immediately, so a name imported into
# this module's namespace is unreachable from a test's
# ``monkeypatch.setattr`` on the canonical
# ``det.destinations.bigquery.client`` module. Going through the attribute
# on every call keeps the single substitution point on
# ``_client_mod._bigquery_module`` valid for both the engine and the tests.
def _bigquery_module() -> Any:
    """Look up the BigQuery SDK module via the canonical client module."""
    return _client_mod._bigquery_module()


def _pyarrow_modules() -> tuple[Any, Any]:
    """Look up the pyarrow modules via the canonical client module."""
    return _client_mod._pyarrow_modules()

logger = logging.getLogger(__name__)

# The engine-owned state and runs tables — same ``_det_`` namespace as the
# DuckDB destination so an admin / UI / future cross-warehouse tool can
# query both backends identically (the task's quality bar).
_STATE_TABLE = "_det_state"
_RUNS_TABLE = "_det_runs"


# --------------------------------------------------------------------------
# capabilities — docs/05 §1
# --------------------------------------------------------------------------


@destination.capabilities
def capabilities() -> set[Capability]:
    """Declare what the BigQuery destination can do — docs/05 §1.

    * ``STATE`` — BigQuery hosts the ``_det_state`` table itself (Tier A,
      docs/05 §5). Implements ``read_state`` / ``commit_state`` and *not*
      ``state_backend``.
    * ``MERGE`` — BigQuery supports ``MERGE INTO target USING staging ON pk
      WHEN MATCHED ... WHEN NOT MATCHED ...``, the ``merge`` write
      disposition (docs/05 §4).
    * ``SCHEMA_EVOLUTION`` — BigQuery supports additive column add via the
      ``client.update_table(table, ["schema"])`` SDK path (docs/05 §3.2).
    * ``RUN_RECORDS`` — BigQuery hosts the ``_det_runs`` audit table
      alongside ``_det_state``, queryable with plain SQL (docs/09 §4).

    # NOTE: ``TRANSACTIONAL_LOAD`` is intentionally *absent* — see the
    # module docstring NOTE for the reasoning. v1 chooses per-batch
    # atomicity (each LOAD / MERGE commits independently) over the
    # buffer-then-MERGE-once design that would break the streaming load.
    """
    return {
        Capability.STATE,
        Capability.MERGE,
        Capability.SCHEMA_EVOLUTION,
        Capability.RUN_RECORDS,
    }


# --------------------------------------------------------------------------
# open / close — docs/05 §1
# --------------------------------------------------------------------------


@destination.open
def open(config: Config) -> BQConn:  # noqa: A001 — hook name is fixed by the contract
    """Build the BQ + GCS clients from ``config`` and verify the staging bucket.

    Reads every param declared in ``register.yaml``:

    * ``project`` / ``dataset`` (required) — the BigQuery target.
    * ``location`` — dataset location at creation time (default ``"US"``).
    * ``staging_bucket`` (required) / ``staging_prefix`` — the GCS path
      where one-batch Parquet objects are staged before each LOAD job.
    * ``credentials_path`` — optional service-account JSON; empty = ADC.
    * ``job_timeout_seconds`` / ``retry_max_attempts`` / ``retry_backoff_seconds``
      — per-job timeout + transient-error retry policy.

    Creates the destination dataset if absent (lazy: ``create_dataset(..,
    exists_ok=True)`` in ``location``). Verifies the staging bucket exists
    — det never creates buckets, that is an operator concern (region,
    retention, lifecycle rules).
    """
    project = str(config.get("project") or "")
    dataset = str(config.get("dataset") or "")
    staging_bucket = str(config.get("staging_bucket") or "")
    if not project:
        raise ValueError("bigquery destination: 'project' param is required")
    if not dataset:
        raise ValueError("bigquery destination: 'dataset' param is required")
    if not staging_bucket:
        raise ValueError("bigquery destination: 'staging_bucket' param is required")

    validate_identifier(project, kind="project")
    validate_identifier(dataset, kind="dataset")

    location = str(config.get("location") or "US")
    staging_prefix = str(config.get("staging_prefix") or "det/staging")
    credentials_path = str(config.get("credentials_path") or "")
    job_timeout = int(config.get("job_timeout_seconds") or 300)
    retry_max = int(config.get("retry_max_attempts") or 5)
    retry_backoff = float(config.get("retry_backoff_seconds") or 1.0)

    client = build_client(
        project=project,
        dataset=dataset,
        location=location,
        staging_bucket=staging_bucket,
        staging_prefix=staging_prefix,
        credentials_path=credentials_path,
        job_timeout_seconds=job_timeout,
        retry_max_attempts=retry_max,
        retry_backoff_seconds=retry_backoff,
    )

    _ensure_dataset(client)
    _verify_staging_bucket(client)
    return BQConn(client=client)


def _ensure_dataset(client: BigQueryClient) -> None:
    """Create the destination dataset if absent — idempotent (``exists_ok=True``)."""
    bq = _bigquery_module()
    ref = bq.DatasetReference(client.project, client.dataset)
    dataset = bq.Dataset(ref)
    dataset.location = client.location
    client.bq.create_dataset(dataset, exists_ok=True)


def _verify_staging_bucket(client: BigQueryClient) -> None:
    """Raise a clear error if the staging bucket is missing — operator concern.

    det does not create the staging bucket: region / retention / lifecycle
    rules are operator decisions that vary per organization. A missing
    bucket here is the right place to fail loudly, before any batches
    arrive.
    """
    bucket = client.gcs.bucket(client.staging_bucket)
    if not bucket.exists():
        raise ValueError(
            f"bigquery destination: staging bucket "
            f"gs://{client.staging_bucket} does not exist or is not visible "
            f"to these credentials — create it (the destination never does) "
            f"and re-run"
        )


@destination.close
def close(conn: BQConn) -> None:
    """Release the BQ + GCS clients — docs/05 §1.

    "Always called, even on failure" (docs/05 §1), so this must be safe to
    call on a half-open or already-closed handle: any error from the
    underlying ``close`` is swallowed, because ``close`` failing must not
    mask the run's real error. The google-cloud clients do not require an
    explicit close; this hook is here purely so the engine's lifecycle
    stays uniform across destinations.
    """
    try:
        close_method = getattr(conn.client.bq, "close", None)
        if close_method is not None:
            close_method()
    except Exception:  # noqa: BLE001 — close must never raise; see docstring.
        pass
    try:
        close_method = getattr(conn.client.gcs, "close", None)
        if close_method is not None:
            close_method()
    except Exception:  # noqa: BLE001 — close must never raise; see docstring.
        pass


# --------------------------------------------------------------------------
# ensure_schema — docs/05 §3
# --------------------------------------------------------------------------


@destination.ensure_schema
def ensure_schema(conn: BQConn, stream: StreamMeta) -> None:
    """Create the target table if absent; additively evolve it — docs/05 §3.

    docs/05 §3.1: translate the stream's :class:`Schema` into native DDL —
    here a list of ``bigquery.SchemaField`` and ``client.create_table``.
    docs/05 §3.2: additive evolution — a field present in ``stream.schema``
    but absent from an existing table is added by mutating the table's
    schema list and calling ``client.update_table(table, ["schema"])``.

    The engine appends ``_det_synced_at`` to every record (docs/03 §2.2.1);
    this hook calls :meth:`Schema.with_synced_at` so the physical table
    always carries that column — both on first ``create_table`` and, for a
    pre-existing table that lacks it, via additive evolution.

    Locked decision: the default schema-evolution policy is ``evolve``
    (additive). This hook performs the additive ``update``; the engine
    enforces the per-stream ``strict`` opt-in (a strict stream's schema
    diff fails the run *before* this hook is called), so ``ensure_schema``
    itself is always additive — it never needs to know the contract.
    """
    bq = _bigquery_module()
    client = conn.client
    table_name = stream.table
    validate_identifier(table_name, kind="table")

    full_schema = stream.schema.with_synced_at()
    table_ref = bq.TableReference(
        bq.DatasetReference(client.project, client.dataset), table_name
    )

    existing = _get_table_or_none(conn, table_name)
    if existing is None:
        # Table absent — create it whole, with partitioning if requested.
        # docs/05 §3.x: ``stream.partition`` is the engine-resolved
        # PartitionConfig (None ⇒ unpartitioned, today's behavior).
        table = bq.Table(table_ref, schema=bq_schema(full_schema))
        apply_partitioning_to_table(table, stream.partition, bq)
        client.bq.create_table(table, exists_ok=True)
        return

    # Table present — validate partition drift BEFORE additive evolution.
    # BigQuery cannot change an existing table's partitioning in place, so
    # silently accepting a mismatch would let writes drift from intent.
    # docs/05 §3.x: hard error with an actionable message naming both
    # specs and the suggested fix.
    status, message = compare_partition(existing, stream.partition)
    if status == "mismatch":
        assert message is not None
        raise PartitionDriftError(message)

    # Additively add any declared field the table lacks (docs/05 §3.2).
    have = {f.name for f in existing.schema}
    new_fields: list[Any] = list(existing.schema)
    added = False
    for f in full_schema.fields:
        if f.name not in have:
            new_fields.append(bq_schema_field(f))
            added = True
    if added:
        existing.schema = new_fields
        client.bq.update_table(existing, ["schema"])


def _get_table_or_none(conn: BQConn, table_name: str) -> Any | None:
    """Return the ``bigquery.Table`` if it exists, else ``None``.

    BigQuery raises ``google.cloud.exceptions.NotFound`` for a missing
    table; we treat that as "create-time" and let any other exception
    surface — it means the lookup itself failed (auth / network), not that
    the table is simply absent.
    """
    bq = _bigquery_module()
    client = conn.client
    table_ref = bq.TableReference(
        bq.DatasetReference(client.project, client.dataset), table_name
    )
    try:
        return client.bq.get_table(table_ref)
    except Exception as exc:  # noqa: BLE001 — NotFound is the only expected miss
        # 404 / NotFound ⇒ absent. Anything else ⇒ a real lookup failure.
        if _status_code(exc) == 404 or _is_not_found(exc):
            return None
        raise


def _status_code(exc: BaseException) -> int | None:
    """Pull an HTTP status code off a Google API exception (duplicated for tests)."""
    for attr in ("code", "status_code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    if isinstance(code, int):
        return code
    return None


def _is_not_found(exc: BaseException) -> bool:
    """Is this exception class named like NotFound — used as a fallback check."""
    return type(exc).__name__ in ("NotFound", "_NotFound")


# --------------------------------------------------------------------------
# write_batch — docs/05 §4
# --------------------------------------------------------------------------


@destination.write_batch
def write_batch(conn: BQConn, batch: Batch, stream: StreamMeta) -> int:
    """Persist one batch per its write disposition — docs/05 §4. Returns rows written.

    The production load path, per docs/05 §2 (the catalog row): stage one
    batch as a Parquet object in GCS, trigger a BigQuery ``LOAD`` job into
    either the target (APPEND / REPLACE) or a per-batch staging table
    (MERGE), then on MERGE run the ``MERGE INTO ... ON pk`` statement and
    drop the staging table.

    docs/05 §4 dispositions, as implemented for BigQuery:

    * ``append`` — LOAD job with ``WRITE_APPEND`` into the target table.
      Duplicates are the source's concern.
    * ``merge`` — LOAD job with ``WRITE_TRUNCATE`` into a per-batch staging
      table ``{target}__staging_{run_suffix}_{uuid}``, then ``MERGE INTO
      target USING staging ON pk``, then drop the staging table in a
      ``finally`` (cleanup on both success and failure).
    * ``replace`` — first batch of the run uses ``WRITE_TRUNCATE`` (clears
      the table, loads); subsequent batches in the SAME run use
      ``WRITE_APPEND`` so a multi-batch replace stream truncates exactly
      once. Tracked via ``conn.replace_truncated`` — same pattern as DuckDB.
      An empty replace batch still truncates (full-snapshot ⇒ empty
      snapshot is a valid state).

    The engine sets ``_det_synced_at`` on every record; this hook fills
    that column with the current UTC time for any record that does not
    already carry it, so a load timestamp is always present (docs/03
    §2.2.1).

    GCS staging objects are cleaned up on success; on failure they are
    *left in place* for forensics (the task's quality bar). The MERGE
    staging *table* is dropped on both success and failure — leaving it
    in BigQuery would clutter the dataset with no diagnostic value
    (the load already wrote it cleanly; the failure is in the MERGE SQL,
    which is queryable from the job history).
    """
    table_name = stream.table
    validate_identifier(table_name, kind="table")
    wd = stream.write_disposition

    if not batch:
        # An empty batch is a valid no-op — but a ``replace`` stream that
        # yields nothing must still truncate (full-snapshot ⇒ empty snapshot).
        if wd is WriteDisposition.REPLACE:
            _truncate_replace_target(conn, stream)
        return 0

    stamped = _stamp_synced_at(batch)
    schema = stream.schema.with_synced_at()
    schema = _augment_schema_for_batch(schema, stamped)

    if wd is WriteDisposition.APPEND:
        _load_to_table(conn, table_name, stamped, schema, disposition="WRITE_APPEND")
    elif wd is WriteDisposition.REPLACE:
        if table_name in conn.replace_truncated:
            disposition = "WRITE_APPEND"
        else:
            disposition = "WRITE_TRUNCATE"
        _load_to_table(conn, table_name, stamped, schema, disposition=disposition)
        conn.replace_truncated.add(table_name)
    elif wd is WriteDisposition.MERGE:
        if not stream.primary_key:
            raise ValueError(
                f"write_batch: disposition 'merge' for table {table_name!r} "
                f"requires a primary_key (docs/05 §4)"
            )
        _merge_via_staging(conn, table_name, stamped, schema, stream.primary_key)
    else:  # pragma: no cover — WriteDisposition is a closed 3-member enum.
        raise ValueError(f"write_batch: unknown disposition {wd!r}")

    return len(stamped)


def _truncate_replace_target(conn: BQConn, stream: StreamMeta) -> None:
    """Run a zero-row LOAD with ``WRITE_TRUNCATE`` for an empty replace batch.

    docs/05 §4 ``replace`` semantics: a stream that yields no records
    still represents an empty snapshot, so the table must be empty after
    the run. A direct ``TRUNCATE`` / DML would also work; using a zero-row
    LOAD keeps the implementation path uniform with the populated case
    (one entry point, one retry wrapper).
    """
    if stream.table in conn.replace_truncated:
        return
    schema = stream.schema.with_synced_at()
    _load_to_table(conn, stream.table, [], schema, disposition="WRITE_TRUNCATE")
    conn.replace_truncated.add(stream.table)


def _stamp_synced_at(batch: Batch) -> list[dict[str, Any]]:
    """Return a copy of ``batch`` with ``_det_synced_at`` set on every record.

    docs/03 §2.2.1: the engine appends this load-timestamp column. A record
    that already carries a value keeps it; a record without one gets the
    current UTC time. The input batch is not mutated.
    """
    now = datetime.now(UTC)
    out: list[dict[str, Any]] = []
    for record in batch:
        row = dict(record)
        if row.get(Schema.SYNCED_AT_COLUMN) is None:
            row[Schema.SYNCED_AT_COLUMN] = now
        out.append(row)
    return out


def _augment_schema_for_batch(schema: Schema, batch: list[dict[str, Any]]) -> Schema:
    """Ensure every column the batch uses is in the schema (additive only).

    Authors may yield records carrying columns the declared schema did not
    name (the ``evolve`` default). Schema evolution at the table level is
    handled by ``ensure_schema``; here we only need a schema list that
    *covers* the columns of this batch so the Parquet writer + LOAD job
    have a place to bind each value. New columns are inferred as STRING
    (the safe fallback — matches the engine's own ``_infer_field_type``
    behavior for unknown values).
    """
    have = {f.name for f in schema.fields}
    fields = list(schema.fields)
    seen: set[str] = set()
    for record in batch:
        for key in record:
            if key in have or key in seen:
                continue
            validate_identifier(key, kind="column")
            fields.append(Field(name=key, type=FieldType.STRING))
            seen.add(key)
    if not seen:
        return schema
    return Schema(fields=tuple(fields))


# --------------------------------------------------------------------------
# Parquet staging + LOAD job — the shared write path
# --------------------------------------------------------------------------


def _records_to_parquet_bytes(
    batch: list[dict[str, Any]], schema: Schema
) -> bytes:
    """Serialize a batch to a Parquet blob using pyarrow — the LOAD source.

    The resulting bytes are uploaded to GCS as the LOAD source URI. JSON
    values are pre-serialized to text (BigQuery's PARQUET → JSON column
    path expects a JSON-text string, not a nested struct); ``datetime``
    and ``date`` values are preserved as native arrow timestamp/date32
    types so BigQuery sees them as TIMESTAMP / DATE on load.
    """
    pa, pq = _pyarrow_modules()

    # Build per-column arrow arrays. Even an empty batch needs the right
    # column set (so a zero-row LOAD still hits the right table shape).
    columns = [f.name for f in schema.fields]
    type_by_name = {f.name: f.type for f in schema.fields}

    arrays: dict[str, Any] = {}
    for col in columns:
        ft = type_by_name[col]
        py_values: list[Any] = [_encode_cell(record.get(col), ft) for record in batch]
        arrays[col] = pa.array(py_values, type=_arrow_type_for(ft, pa))

    table = pa.table(arrays)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _arrow_type_for(field_type: FieldType, pa: Any) -> Any:
    """Map a det :class:`FieldType` to a pyarrow type — the Parquet column type."""
    if field_type is FieldType.STRING:
        return pa.string()
    if field_type is FieldType.INTEGER:
        return pa.int64()
    if field_type is FieldType.FLOAT:
        return pa.float64()
    if field_type is FieldType.BOOLEAN:
        return pa.bool_()
    if field_type is FieldType.TIMESTAMP:
        # ``us`` precision, tz-naive — BigQuery TIMESTAMP is itself UTC, the
        # engine stamps ``_det_synced_at`` in UTC; aware datetimes carry the
        # same wall-clock either way.
        return pa.timestamp("us", tz="UTC")
    if field_type is FieldType.DATE:
        return pa.date32()
    if field_type is FieldType.JSON:
        # BigQuery's PARQUET-loaded JSON column wants a JSON-text string;
        # encoded by ``_encode_cell``.
        return pa.string()
    if field_type is FieldType.BYTES:
        return pa.binary()
    # Total over the enum — defensive fallback.
    return pa.string()  # pragma: no cover


def _encode_cell(value: Any, field_type: FieldType) -> Any:
    """Coerce a record value into the per-type representation the Parquet writer expects.

    Mirrors the DuckDB destination's ``_encode_value`` split: ``dict`` /
    ``list`` values targeting a JSON column are ``json.dumps``-serialized
    (a JSON column on BigQuery takes a JSON-text string); every other value
    passes through as-is so pyarrow handles the type. ``None`` stays
    ``None`` (NULL in BigQuery).
    """
    if value is None:
        return None
    if field_type is FieldType.JSON:
        if isinstance(value, str):
            return value  # already JSON text — pass through verbatim
        return json.dumps(value, default=str)
    return value


def _upload_blob(conn: BQConn, table_name: str, parquet_bytes: bytes) -> tuple[str, Any]:
    """Upload one batch's Parquet bytes to GCS; return (uri, blob).

    The blob is opaque-named (uuid) so an HTTP error message in the GCS
    layer never leaks record content or stream identity beyond the table
    name. The returned ``blob`` handle is used to delete the object on
    success (or to leave it in place for forensics on failure).
    """
    batch_uuid = uuid.uuid4().hex
    uri = conn.client.staging_uri(table_name, batch_uuid)
    blob_name = conn.client.staging_blob_name(table_name, batch_uuid)
    bucket = conn.client.gcs.bucket(conn.client.staging_bucket)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
    return uri, blob


def _load_to_table(
    conn: BQConn,
    table_name: str,
    batch: list[dict[str, Any]],
    schema: Schema,
    *,
    disposition: str,
) -> None:
    """Stage a Parquet object in GCS and run a LOAD job into ``table_name``.

    Used by APPEND, REPLACE and the staging side of MERGE. Cleans up the
    GCS object on success; leaves it on failure so an operator can inspect
    the source the failed LOAD pulled from (the task's quality bar).
    """
    bq = _bigquery_module()
    client = conn.client

    parquet_bytes = _records_to_parquet_bytes(batch, schema)
    uri, blob = _upload_blob(conn, table_name, parquet_bytes)

    table_ref = bq.TableReference(
        bq.DatasetReference(client.project, client.dataset), table_name
    )
    job_config = bq.LoadJobConfig(
        source_format=bq.SourceFormat.PARQUET,
        write_disposition=disposition,
        # Don't auto-detect — the table schema is the ground truth, set by
        # ``ensure_schema``. Auto-detect on a Parquet load would re-infer
        # types and could conflict with the declared schema.
        autodetect=False,
        schema=bq_schema(schema),
    )

    def _run() -> Any:
        job = client.bq.load_table_from_uri(
            uri, table_ref, job_config=job_config, location=client.location
        )
        return job.result(timeout=client.job_timeout_seconds)

    try:
        run_with_retries(
            _run,
            max_attempts=client.retry_max_attempts,
            backoff_seconds=client.retry_backoff_seconds,
        )
    except Exception:
        # Forensics: leave the staged Parquet for inspection. Log the URI
        # at WARNING so an operator scanning logs sees where to look.
        logger.warning(
            "bigquery LOAD failed; leaving staged parquet at %s for forensics",
            uri,
        )
        raise

    # Success — clean up the GCS object.
    try:
        blob.delete()
    except Exception as exc:  # noqa: BLE001 — cleanup must not mask success
        logger.warning("bigquery: failed to delete staging blob %s: %s", uri, exc)


def _merge_via_staging(
    conn: BQConn,
    target_table: str,
    batch: list[dict[str, Any]],
    schema: Schema,
    primary_key: tuple[str, ...],
) -> None:
    """LOAD a Parquet batch into a per-batch staging table, then MERGE into target.

    The staging table name embeds the run suffix and a per-batch uuid so
    two concurrent runs / two sibling batches never share one. ``MERGE``
    keys on every column of ``primary_key`` and overwrites every non-key
    column from the staging row; the SQL itself is built by
    :func:`~det.destinations.bigquery.ddl.merge_sql` and contains only
    backticked identifiers (no values), so it is safe by construction.

    The staging table is dropped in a ``finally``: a successful MERGE
    leaves nothing behind, and a failed MERGE leaves nothing behind in BQ
    either (the GCS Parquet stays, per the forensics rule). Without the
    drop a failed run would leak a per-batch staging table per attempt.
    """
    bq = _bigquery_module()
    client = conn.client

    for k in primary_key:
        validate_identifier(k, kind="column")

    staging_name = f"{target_table}__staging_{client.run_suffix}_{uuid.uuid4().hex[:8]}"
    validate_identifier(staging_name, kind="table")

    # Stage the batch — LOAD into the per-batch table with WRITE_TRUNCATE
    # (the staging table is fresh; this is purely defensive).
    _load_to_table(conn, staging_name, batch, schema, disposition="WRITE_TRUNCATE")

    try:
        columns = tuple(f.name for f in schema.fields)
        sql = merge_sql(
            project=client.project,
            dataset=client.dataset,
            target_table=target_table,
            staging_table=staging_name,
            primary_key=primary_key,
            columns=columns,
        )

        def _run() -> Any:
            job = client.bq.query(sql, location=client.location)
            return job.result(timeout=client.job_timeout_seconds)

        run_with_retries(
            _run,
            max_attempts=client.retry_max_attempts,
            backoff_seconds=client.retry_backoff_seconds,
        )
    finally:
        # Drop the staging table — both on success (clean up) and failure
        # (don't leak per-batch tables on retries). The drop is best-effort;
        # a failed drop is logged but does not mask the run's real error.
        staging_ref = bq.TableReference(
            bq.DatasetReference(client.project, client.dataset), staging_name
        )
        try:
            client.bq.delete_table(staging_ref, not_found_ok=True)
        except Exception as exc:  # noqa: BLE001 — cleanup must not mask
            logger.warning(
                "bigquery: failed to drop merge staging table %s: %s",
                staging_name,
                exc,
            )


# --------------------------------------------------------------------------
# read_state / commit_state — the _det_state table — docs/05 §5
# --------------------------------------------------------------------------


def _state_schema() -> Iterator[tuple[str, FieldType]]:
    """The canonical ``_det_state`` column set — mirrors ``StateRecord.to_row()``.

    Eight columns; same column names + types as the DuckDB destination so
    an admin / UI / cross-warehouse tool can query both backends
    identically (the task's quality bar).
    """
    yield "connector", FieldType.STRING
    yield "stream", FieldType.STRING
    yield "cursor_value", FieldType.JSON
    yield "cursor_type", FieldType.STRING
    yield "state_blob", FieldType.JSON
    yield "last_run_id", FieldType.STRING
    yield "rows_total", FieldType.INTEGER
    yield "updated_at", FieldType.TIMESTAMP


def _ensure_state_table(conn: BQConn) -> None:
    """Create ``_det_state`` lazily on first use — docs/05 §5.1.

    Same eight columns + same primary key as the DuckDB version.
    BigQuery's primary-key declaration is *informational* (not enforced),
    but declaring it is good hygiene and feeds downstream tools that read
    the table metadata.
    """
    if conn.state_table_ready:
        return
    bq = _bigquery_module()
    client = conn.client

    fields = [
        bq.SchemaField(
            name=name,
            field_type=bigquery_type(ftype),
            mode="REQUIRED" if name in ("connector", "stream") else "NULLABLE",
        )
        for name, ftype in _state_schema()
    ]

    table_ref = bq.TableReference(
        bq.DatasetReference(client.project, client.dataset), _STATE_TABLE
    )
    table = bq.Table(table_ref, schema=fields)
    client.bq.create_table(table, exists_ok=True)
    conn.state_table_ready = True


def _encode_json_column(value: Any) -> Any:
    """Serialize *any* value for binding into a BigQuery ``JSON`` column.

    The same rule as DuckDB's stage-7 fix: BigQuery's ``JSON`` type ingests
    JSON-*text* strings, so every non-``None`` value — including bare
    scalars — must be ``json.dumps``-serialized first. A bare cursor like
    ``"2026-05-20T00:00:00"`` is not valid JSON text and BigQuery rejects
    it. ``None`` stays ``None``; ``datetime`` / ``date`` are serialized
    via ``default=str`` so they round-trip cleanly through :func:`_decode_json`.
    """
    if value is None:
        return None
    return json.dumps(value, default=str)


def _decode_json(value: Any) -> Any:
    """Parse a value read back from a BigQuery ``JSON`` column.

    The BigQuery Python SDK returns a ``JSON`` column as a Python value
    already parsed (it deserializes the JSON text into a native Python
    object — dict / list / scalar) for most builds; some builds hand back
    the raw JSON-text string. Handle both shapes.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


@destination.read_state
def read_state(conn: BQConn, connector: str) -> list[StateRecord]:
    """Load every prior :class:`StateRecord` for a connector — docs/05 §1, §5.

    Called once at run start (docs/05 §1 lifecycle). Returns one
    :class:`StateRecord` per ``_det_state`` row whose ``connector`` matches
    — the per-stream resume points. An empty list on the first ever run
    (the state table is created lazily, so it always exists by the time
    this reads).

    The query is fully parameterized — the ``@connector`` placeholder
    binds the connector name via a ``ScalarQueryParameter``. No
    string-interpolation; identifiers are backticked.
    """
    _ensure_state_table(conn)
    bq = _bigquery_module()
    client = conn.client

    table = fq_table(client.project, client.dataset, _STATE_TABLE)
    sql = (
        f"SELECT connector, stream, cursor_value, cursor_type, state_blob, "
        f"last_run_id, rows_total, updated_at FROM {table} "
        f"WHERE connector = @connector"
    )
    job_config = bq.QueryJobConfig(
        query_parameters=[bq.ScalarQueryParameter("connector", "STRING", connector)]
    )
    job = client.bq.query(sql, job_config=job_config, location=client.location)
    rows = list(job.result(timeout=client.job_timeout_seconds))

    records: list[StateRecord] = []
    for row in rows:
        records.append(
            StateRecord.from_row(
                {
                    "connector": row["connector"],
                    "stream": row["stream"],
                    "cursor_value": _decode_json(row["cursor_value"]),
                    "cursor_type": row["cursor_type"],
                    "state_blob": _decode_json(row["state_blob"]) or {},
                    "last_run_id": row["last_run_id"],
                    "rows_total": row["rows_total"],
                    "updated_at": row["updated_at"],
                }
            )
        )
    return records


@destination.commit_state
def commit_state(conn: BQConn, run_id: str, records: list[StateRecord]) -> None:
    """Upsert the run's :class:`StateRecord` set into ``_det_state`` — docs/05 §5.

    docs/05 §5.3: ``commit_state`` runs **only after all batches durably
    land**. Each record upserts on the ``(connector, stream)`` key via a
    parameterized ``MERGE``. ``cursor_value`` / ``state_blob`` flow
    through :func:`_encode_json_column` so a bare scalar cursor like
    ``"2026-05-20T00:00:00"`` is valid JSON text (the stage-7 fix
    applies identically to BigQuery's JSON column).
    ``updated_at`` is stamped with the current UTC time when the record
    has not already set it.
    """
    if not records:
        return
    _ensure_state_table(conn)
    bq = _bigquery_module()
    client = conn.client
    now = datetime.now(UTC)

    table = fq_table(client.project, client.dataset, _STATE_TABLE)
    sql = (
        f"MERGE INTO {table} T "
        f"USING (SELECT @connector AS connector, @stream AS stream, "
        f"       @cursor_value AS cursor_value, @cursor_type AS cursor_type, "
        f"       @state_blob AS state_blob, @last_run_id AS last_run_id, "
        f"       @rows_total AS rows_total, @updated_at AS updated_at) S "
        f"ON T.connector = S.connector AND T.stream = S.stream "
        f"WHEN MATCHED THEN UPDATE SET "
        f"  cursor_value = S.cursor_value, cursor_type = S.cursor_type, "
        f"  state_blob = S.state_blob, last_run_id = S.last_run_id, "
        f"  rows_total = S.rows_total, updated_at = S.updated_at "
        f"WHEN NOT MATCHED THEN INSERT "
        f"  (connector, stream, cursor_value, cursor_type, state_blob, "
        f"   last_run_id, rows_total, updated_at) "
        f"VALUES (S.connector, S.stream, S.cursor_value, S.cursor_type, "
        f"        S.state_blob, S.last_run_id, S.rows_total, S.updated_at)"
    )

    for record in records:
        if record.last_run_id is None:
            record.last_run_id = run_id
        if record.updated_at is None:
            record.updated_at = now
        row = record.to_row()
        params = [
            bq.ScalarQueryParameter("connector", "STRING", row["connector"]),
            bq.ScalarQueryParameter("stream", "STRING", row["stream"]),
            bq.ScalarQueryParameter(
                "cursor_value", "JSON", _encode_json_column(row["cursor_value"])
            ),
            bq.ScalarQueryParameter("cursor_type", "STRING", row["cursor_type"]),
            bq.ScalarQueryParameter(
                "state_blob", "JSON", _encode_json_column(row["state_blob"])
            ),
            bq.ScalarQueryParameter("last_run_id", "STRING", row["last_run_id"]),
            bq.ScalarQueryParameter("rows_total", "INT64", row["rows_total"]),
            bq.ScalarQueryParameter(
                "updated_at", "TIMESTAMP", record.updated_at
            ),
        ]
        job_config = bq.QueryJobConfig(query_parameters=params)

        def _run(_sql: str = sql, _jc: Any = job_config) -> Any:
            job = client.bq.query(_sql, job_config=_jc, location=client.location)
            return job.result(timeout=client.job_timeout_seconds)

        run_with_retries(
            _run,
            max_attempts=client.retry_max_attempts,
            backoff_seconds=client.retry_backoff_seconds,
        )


# --------------------------------------------------------------------------
# write_run_record — the _det_runs audit table — docs/09 §4
# --------------------------------------------------------------------------


def _runs_schema() -> Iterator[tuple[str, FieldType, str]]:
    """The canonical ``_det_runs`` column set — mirrors the DuckDB destination.

    Same names and types so a cross-warehouse admin / UI can query both
    backends identically. ``run_id`` is REQUIRED (it is the upsert key);
    the per-run identity / window columns are REQUIRED; the error and
    streams JSON columns are NULLABLE.
    """
    yield "run_id", FieldType.STRING, "REQUIRED"
    yield "config", FieldType.STRING, "REQUIRED"
    yield "source", FieldType.STRING, "REQUIRED"
    yield "destination", FieldType.STRING, "REQUIRED"
    yield "target", FieldType.STRING, "REQUIRED"
    yield "status", FieldType.STRING, "REQUIRED"
    yield "started_at", FieldType.TIMESTAMP, "REQUIRED"
    yield "ended_at", FieldType.TIMESTAMP, "REQUIRED"
    yield "duration_s", FieldType.FLOAT, "REQUIRED"
    yield "rows_loaded", FieldType.INTEGER, "REQUIRED"
    yield "full_refresh", FieldType.BOOLEAN, "REQUIRED"
    yield "error_type", FieldType.STRING, "NULLABLE"
    yield "error_message", FieldType.STRING, "NULLABLE"
    yield "streams_json", FieldType.JSON, "NULLABLE"


def _ensure_runs_table(conn: BQConn) -> None:
    """Create ``_det_runs`` lazily on first use — docs/09 §4."""
    if conn.runs_table_ready:
        return
    bq = _bigquery_module()
    client = conn.client

    fields = [
        bq.SchemaField(name=name, field_type=bigquery_type(ftype), mode=mode)
        for name, ftype, mode in _runs_schema()
    ]
    table_ref = bq.TableReference(
        bq.DatasetReference(client.project, client.dataset), _RUNS_TABLE
    )
    table = bq.Table(table_ref, schema=fields)
    client.bq.create_table(table, exists_ok=True)
    conn.runs_table_ready = True


@destination.write_run_record
def write_run_record(conn: BQConn, record: RunRecord) -> None:
    """Persist one :class:`RunRecord` to ``_det_runs`` — docs/09 §4.

    Called once per run by the engine, after streams finish and before
    ``close`` (docs/05 §1 lifecycle, stage 8a addendum). The write is
    idempotent on ``run_id``: a duplicate write of the same record updates
    the row rather than duplicating it — the defensive guarantee for any
    future retry-on-transient. Streams detail lands in ``streams_json``
    (a JSON column) so per-stream rows / cursor / status survive without
    a join.
    """
    _ensure_runs_table(conn)
    bq = _bigquery_module()
    client = conn.client

    table = fq_table(client.project, client.dataset, _RUNS_TABLE)
    sql = (
        f"MERGE INTO {table} T "
        f"USING (SELECT @run_id AS run_id, @config AS config, "
        f"       @source AS source, @destination AS destination, "
        f"       @target AS target, @status AS status, "
        f"       @started_at AS started_at, @ended_at AS ended_at, "
        f"       @duration_s AS duration_s, @rows_loaded AS rows_loaded, "
        f"       @full_refresh AS full_refresh, "
        f"       @error_type AS error_type, @error_message AS error_message, "
        f"       @streams_json AS streams_json) S "
        f"ON T.run_id = S.run_id "
        f"WHEN MATCHED THEN UPDATE SET "
        f"  config = S.config, source = S.source, destination = S.destination, "
        f"  target = S.target, status = S.status, started_at = S.started_at, "
        f"  ended_at = S.ended_at, duration_s = S.duration_s, "
        f"  rows_loaded = S.rows_loaded, full_refresh = S.full_refresh, "
        f"  error_type = S.error_type, error_message = S.error_message, "
        f"  streams_json = S.streams_json "
        f"WHEN NOT MATCHED THEN INSERT "
        f"  (run_id, config, source, destination, target, status, "
        f"   started_at, ended_at, duration_s, rows_loaded, full_refresh, "
        f"   error_type, error_message, streams_json) "
        f"VALUES (S.run_id, S.config, S.source, S.destination, S.target, "
        f"        S.status, S.started_at, S.ended_at, S.duration_s, "
        f"        S.rows_loaded, S.full_refresh, S.error_type, "
        f"        S.error_message, S.streams_json)"
    )
    params = [
        bq.ScalarQueryParameter("run_id", "STRING", record.run_id),
        bq.ScalarQueryParameter("config", "STRING", record.config),
        bq.ScalarQueryParameter("source", "STRING", record.source),
        bq.ScalarQueryParameter("destination", "STRING", record.destination),
        bq.ScalarQueryParameter("target", "STRING", record.target),
        bq.ScalarQueryParameter("status", "STRING", record.status.value),
        bq.ScalarQueryParameter("started_at", "TIMESTAMP", record.started_at),
        bq.ScalarQueryParameter("ended_at", "TIMESTAMP", record.ended_at),
        bq.ScalarQueryParameter("duration_s", "FLOAT64", record.duration_s),
        bq.ScalarQueryParameter("rows_loaded", "INT64", record.rows_loaded),
        bq.ScalarQueryParameter("full_refresh", "BOOL", record.full_refresh),
        bq.ScalarQueryParameter("error_type", "STRING", record.error_type),
        bq.ScalarQueryParameter("error_message", "STRING", record.error_message),
        bq.ScalarQueryParameter(
            "streams_json", "JSON", _encode_json_column(record.streams_json())
        ),
    ]
    job_config = bq.QueryJobConfig(query_parameters=params)

    def _run() -> Any:
        job = client.bq.query(sql, job_config=job_config, location=client.location)
        return job.result(timeout=client.job_timeout_seconds)

    run_with_retries(
        _run,
        max_attempts=client.retry_max_attempts,
        backoff_seconds=client.retry_backoff_seconds,
    )
