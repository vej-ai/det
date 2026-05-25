"""Direct unit tests of the BigQuery destination hooks — docs/05.

Two paths:

* **Unit tests** (always run): substitute fake ``bigquery.Client`` /
  ``storage.Client`` classes via the module's lazy accessors
  (:func:`~det.destinations.bigquery.client._bigquery_module` /
  :func:`~det.destinations.bigquery.client._storage_module`). The fakes
  record calls and serve canned responses; no network, no live BigQuery.
* **Integration tests** (gated): exercise the destination against a real
  BigQuery dataset, enabled when ``BIGQUERY_TEST_PROJECT`` /
  ``BIGQUERY_TEST_DATASET`` / ``BIGQUERY_TEST_STAGING_BUCKET`` are set.
  Marked ``@pytest.mark.integration`` so the default ``pytest`` run skips
  them automatically (the marker is registered in ``pyproject.toml``).
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from det import (
    Capability,
    Config,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    PartitionConfig,
    PartitionRange,
    PartitionType,
    RunRecord,
    RunStatus,
    Schema,
    StateRecord,
    StreamMeta,
    StreamResult,
    TimeGranularity,
    WriteDisposition,
)
from det.destinations.bigquery import client as bq_client_mod
from det.destinations.bigquery.ddl import (
    bigquery_mode,
    bigquery_type,
    compare_partition,
    existing_table_partition,
    fq_table,
    merge_sql,
    quote_identifier,
    validate_identifier,
)

# # NOTE: ``PartitionDriftError`` itself is intentionally NOT imported here.
# The connector-folder harness reloads ``destination.py`` under a unique
# synthetic module name, so the class object that the hooks raise is NOT the
# same Python object as one imported from the canonical
# ``det.destinations.bigquery.destination`` module path. Tests check the
# raised exception via its parent type (``RuntimeError``) and its message
# substring — both stable across the harness's re-import.
from tests.conftest import LoadedConnector, load_connector

# --------------------------------------------------------------------------
# Path to the baked BigQuery destination folder
# --------------------------------------------------------------------------

_BIGQUERY_CONNECTOR_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "det" / "destinations" / "bigquery"
)


@pytest.fixture
def bigquery_destination() -> LoadedConnector:
    """The pre-baked BigQuery destination connector, loaded via the harness."""
    return load_connector(_BIGQUERY_CONNECTOR_DIR)


def _hooks(dest: LoadedConnector) -> dict[str, Callable[..., Any]]:
    """Return the destination's hook functions keyed by hook name."""
    return {name: dest.registry.hook(name).func for name in dest.registry.hook_names}  # type: ignore[union-attr]


# --------------------------------------------------------------------------
# Fakes — the unit-test substitutes for google-cloud-bigquery + storage
# --------------------------------------------------------------------------


class _FakeJob:
    """Stand-in for ``bigquery.job.LoadJob`` / ``QueryJob``.

    ``rows`` (for query jobs) is a list of dicts the test seeded; iterating
    a result hands back :class:`_FakeRow` objects with dict-like __getitem__.
    ``raise_on_result`` raises the given exception from ``result()`` (used
    for retry tests).
    """

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or []
        self.raise_on_result: Exception | None = None
        self.result_called = 0

    def result(self, timeout: float | None = None) -> Any:  # noqa: ARG002 — sdk shape
        self.result_called += 1
        if self.raise_on_result is not None:
            exc = self.raise_on_result
            # Reset so subsequent attempts succeed (retry-test friendly).
            self.raise_on_result = None
            raise exc
        return [_FakeRow(r) for r in self.rows]


class _FakeRow:
    """Stand-in for ``bigquery.Row`` — supports both ``row["col"]`` and ``row.col``."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Any:
        return iter(self._data)

    def values(self) -> Any:
        return self._data.values()


class _FakeBlob:
    """Stand-in for ``storage.Blob`` — records upload + delete calls."""

    def __init__(self, bucket: _FakeBucket, name: str) -> None:
        self.bucket = bucket
        self.name = name
        self.uploaded: bytes | None = None
        self.deleted = False

    def upload_from_string(self, data: bytes, content_type: str = "") -> None:
        self.uploaded = data
        self.bucket._uploads.append((self.name, len(data), content_type))

    def delete(self) -> None:
        self.deleted = True
        self.bucket._deletes.append(self.name)


class _FakeBucket:
    """Stand-in for ``storage.Bucket`` — hands out fake blobs, records ops."""

    def __init__(self, name: str, *, exists: bool = True) -> None:
        self.name = name
        self._exists = exists
        self._uploads: list[tuple[str, int, str]] = []
        self._deletes: list[str] = []
        self._blobs: dict[str, _FakeBlob] = {}

    def exists(self) -> bool:
        return self._exists

    def blob(self, name: str) -> _FakeBlob:
        if name not in self._blobs:
            self._blobs[name] = _FakeBlob(self, name)
        return self._blobs[name]


class _FakeStorageClient:
    """Stand-in for ``storage.Client``."""

    def __init__(self, project: str | None = None, credentials: Any = None) -> None:
        self.project = project
        self.credentials = credentials
        self._buckets: dict[str, _FakeBucket] = {}

    def bucket(self, name: str) -> _FakeBucket:
        if name not in self._buckets:
            self._buckets[name] = _FakeBucket(name)
        return self._buckets[name]


class _FakeNotFound(Exception):
    """Stand-in for ``google.cloud.exceptions.NotFound``."""

    code = 404


class _FakeSchemaField:
    """Stand-in for ``bigquery.SchemaField`` — value-equality on (name, type, mode)."""

    def __init__(
        self,
        name: str,
        field_type: str,
        mode: str = "NULLABLE",
        description: str | None = None,
    ) -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode
        self.description = description

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _FakeSchemaField):
            return NotImplemented
        return (
            self.name == other.name
            and self.field_type == other.field_type
            and self.mode == other.mode
        )

    def __repr__(self) -> str:
        return f"_FakeSchemaField({self.name!r}, {self.field_type!r}, {self.mode!r})"


class _FakeDatasetReference:
    def __init__(self, project: str, dataset: str) -> None:
        self.project = project
        self.dataset = dataset


class _FakeTableReference:
    def __init__(self, dataset_ref: _FakeDatasetReference, table_id: str) -> None:
        self.dataset_ref = dataset_ref
        self.table_id = table_id
        self.project = dataset_ref.project
        self.dataset = dataset_ref.dataset

    def __repr__(self) -> str:
        return f"<TableRef {self.project}.{self.dataset}.{self.table_id}>"


class _FakeTable:
    def __init__(
        self,
        ref: _FakeTableReference,
        schema: list[_FakeSchemaField] | None = None,
    ) -> None:
        self.reference = ref
        self.schema = list(schema or [])
        self.project = ref.project
        self.dataset_id = ref.dataset
        self.table_id = ref.table_id
        # Partition handles — set by apply_partitioning_to_table when the
        # destination requests one. Either time or range, never both.
        self.time_partitioning: _FakeTimePartitioning | None = None
        self.range_partitioning: _FakeRangePartitioning | None = None


class _FakeTimePartitioning:
    """Stand-in for ``bigquery.TimePartitioning``."""

    def __init__(self, type_: str = "DAY", field: str | None = None) -> None:
        self.type_ = type_
        self.field = field

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _FakeTimePartitioning):
            return NotImplemented
        return self.type_ == other.type_ and self.field == other.field

    def __repr__(self) -> str:
        return f"_FakeTimePartitioning({self.type_!r}, field={self.field!r})"


class _FakePartitionRange:
    """Stand-in for ``bigquery.PartitionRange``."""

    def __init__(self, start: int, end: int, interval: int) -> None:
        self.start = start
        self.end = end
        self.interval = interval


class _FakeRangePartitioning:
    """Stand-in for ``bigquery.RangePartitioning``."""

    def __init__(
        self, field: str | None = None, range_: _FakePartitionRange | None = None
    ) -> None:
        self.field = field
        self.range_ = range_


class _FakeDataset:
    def __init__(self, ref: _FakeDatasetReference) -> None:
        self.reference = ref
        self.location: str | None = None
        self.project = ref.project
        self.dataset_id = ref.dataset


class _FakeLoadJobConfig:
    def __init__(
        self,
        *,
        source_format: Any = None,
        write_disposition: str | None = None,
        autodetect: bool = False,
        schema: list[_FakeSchemaField] | None = None,
    ) -> None:
        self.source_format = source_format
        self.write_disposition = write_disposition
        self.autodetect = autodetect
        self.schema = schema


class _FakeScalarQueryParameter:
    def __init__(self, name: str, type_: str, value: Any) -> None:
        self.name = name
        self.type_ = type_
        self.value = value


class _FakeQueryJobConfig:
    def __init__(self, query_parameters: list[_FakeScalarQueryParameter] | None = None) -> None:
        self.query_parameters = query_parameters or []


class _FakeSourceFormat:
    PARQUET = "PARQUET"


class _FakeBigQueryClient:
    """The recording fake BQ client.

    Tracks every call the destination makes; serves canned LOAD / query
    job responses. ``query_responses`` maps SQL substrings to lists of
    pre-baked _FakeJob objects served in order; everything else gets a
    default empty job.
    """

    def __init__(
        self,
        project: str | None = None,
        credentials: Any = None,
        location: str | None = None,
        **kwargs: Any,  # noqa: ARG002 — match SDK kwargs
    ) -> None:
        self.project = project
        self.credentials = credentials
        self.location = location
        self.created_datasets: list[_FakeDataset] = []
        self.created_tables: list[_FakeTable] = []
        self.updated_tables: list[tuple[_FakeTable, list[str]]] = []
        self.deleted_tables: list[str] = []
        self.load_jobs: list[dict[str, Any]] = []
        self.queries: list[dict[str, Any]] = []
        # By-table-id store of canned table state for get_table().
        self._tables: dict[str, _FakeTable] = {}
        # Queue of canned query jobs, popped FIFO.
        self.query_job_queue: list[_FakeJob] = []
        self.load_job_factory: Callable[[], _FakeJob] = _FakeJob
        self.closed = False

    # -- table CRUD --
    def create_dataset(self, dataset: Any, exists_ok: bool = False) -> Any:
        self.created_datasets.append(dataset)
        return dataset

    def get_table(self, table_ref: _FakeTableReference) -> _FakeTable:
        key = table_ref.table_id
        if key not in self._tables:
            raise _FakeNotFound(f"table {key} not found")
        return self._tables[key]

    def create_table(self, table: _FakeTable, exists_ok: bool = False) -> _FakeTable:
        self.created_tables.append(table)
        # Record the table so a later get_table() finds it.
        self._tables[table.table_id] = table
        return table

    def update_table(self, table: _FakeTable, fields: list[str]) -> _FakeTable:
        self.updated_tables.append((table, list(fields)))
        # Persist the schema change.
        self._tables[table.table_id] = table
        return table

    def delete_table(self, table_ref: _FakeTableReference, not_found_ok: bool = False) -> None:
        self.deleted_tables.append(table_ref.table_id)
        self._tables.pop(table_ref.table_id, None)

    # -- jobs --
    def load_table_from_uri(
        self,
        uri: str,
        table_ref: _FakeTableReference,
        job_config: _FakeLoadJobConfig,
        location: str | None = None,
    ) -> _FakeJob:
        self.load_jobs.append(
            {
                "uri": uri,
                "table": table_ref.table_id,
                "write_disposition": job_config.write_disposition,
                "schema": job_config.schema,
                "location": location,
            }
        )
        # Default success.
        return self.load_job_factory()

    def query(
        self,
        sql: str,
        job_config: _FakeQueryJobConfig | None = None,
        location: str | None = None,
    ) -> _FakeJob:
        self.queries.append({"sql": sql, "job_config": job_config, "location": location})
        if self.query_job_queue:
            return self.query_job_queue.pop(0)
        return _FakeJob()

    def close(self) -> None:
        self.closed = True


class _FakeBigQueryModule:
    """The fake ``google.cloud.bigquery`` module surface the destination uses."""

    Client = _FakeBigQueryClient
    SchemaField = _FakeSchemaField
    DatasetReference = _FakeDatasetReference
    TableReference = _FakeTableReference
    Table = _FakeTable
    Dataset = _FakeDataset
    LoadJobConfig = _FakeLoadJobConfig
    QueryJobConfig = _FakeQueryJobConfig
    ScalarQueryParameter = _FakeScalarQueryParameter
    SourceFormat = _FakeSourceFormat
    # Partition SDK objects — added in stage 8c. The destination calls
    # ``bq.TimePartitioning(...)`` / ``bq.RangePartitioning(...)`` /
    # ``bq.PartitionRange(...)`` via the lazy module accessor; these fakes
    # let the unit tests assert on the resulting attributes.
    TimePartitioning = _FakeTimePartitioning
    RangePartitioning = _FakeRangePartitioning
    PartitionRange = _FakePartitionRange


@pytest.fixture
def fake_bq(monkeypatch: pytest.MonkeyPatch) -> _FakeBigQueryModule:
    """Substitute the lazy ``_bigquery_module()`` accessor with a fake module.

    Single swap point: ``destination.py`` and ``ddl.py`` both look up the
    SDK accessor via the canonical ``det.destinations.bigquery.client``
    module on every call (rather than ``from ... import _bigquery_module``
    at module load), so patching one attribute here covers every call site.
    This pattern is the connector-folder import harness's price of
    re-importing the folder under a unique synthetic name — the synthetic
    module is unreachable from monkeypatch.
    """
    fake = _FakeBigQueryModule()
    monkeypatch.setattr(bq_client_mod, "_bigquery_module", lambda: fake)
    return fake


@pytest.fixture
def fake_gcs(monkeypatch: pytest.MonkeyPatch) -> type[_FakeStorageClient]:
    """Substitute the lazy ``_storage_module()`` accessor with a fake."""
    class _FakeStorageModule:
        Client = _FakeStorageClient

    monkeypatch.setattr(bq_client_mod, "_storage_module", lambda: _FakeStorageModule)
    return _FakeStorageClient


@pytest.fixture
def fake_pyarrow(monkeypatch: pytest.MonkeyPatch) -> None:
    """No need to fake pyarrow — it ships with the [bigquery] extra and is fine to run real.

    Kept as an explicit fixture so a future test that wants to inject a
    failing pa.write_table has a documented patch point.
    """
    return None


def _open_with_fakes(
    bigquery_destination: LoadedConnector,
    *,
    project: str = "fake-project-id",
    dataset: str = "fake_ds",
    staging_bucket: str = "fake-bucket",
    bucket_exists: bool = True,
    **overrides: Any,
) -> Any:
    """Open the destination against the fake clients and return the BQConn."""
    hooks = _hooks(bigquery_destination)
    conn = hooks["open"](
        Config(
            params={
                "project": project,
                "dataset": dataset,
                "staging_bucket": staging_bucket,
                "retry_max_attempts": overrides.pop("retry_max_attempts", 3),
                "retry_backoff_seconds": overrides.pop("retry_backoff_seconds", 0.0),
                **overrides,
            }
        )
    )
    # If the test wants bucket_exists=False, force it after open (open
    # already verified existence, so this lets a follow-up test reach a
    # blob.upload path against a "missing" bucket — but right now nothing
    # uses it; included for symmetry).
    if not bucket_exists:  # pragma: no cover — unused but symmetric
        conn.client.gcs._buckets[staging_bucket]._exists = False
    return conn


def _events_meta(
    disposition: WriteDisposition = WriteDisposition.APPEND,
    primary_key: tuple[str, ...] = (),
) -> StreamMeta:
    """Build a StreamMeta for the ``events`` table — the hooks' one metadata arg."""
    schema = Schema(
        fields=(
            Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
            Field(name="name", type=FieldType.STRING),
            Field(name="payload", type=FieldType.JSON),
        )
    )
    return StreamMeta(
        table="events",
        write_disposition=disposition,
        schema=schema,
        primary_key=primary_key,
    )


# --------------------------------------------------------------------------
# capabilities — docs/05 §1
# --------------------------------------------------------------------------


def test_capabilities_declares_four_no_transactional_load(
    bigquery_destination: LoadedConnector,
) -> None:
    """BigQuery declares STATE, MERGE, SCHEMA_EVOLUTION, RUN_RECORDS — NOT TRANSACTIONAL_LOAD."""
    caps = _hooks(bigquery_destination)["capabilities"]()
    assert caps == {
        Capability.STATE,
        Capability.MERGE,
        Capability.SCHEMA_EVOLUTION,
        Capability.RUN_RECORDS,
    }
    # And specifically: no TRANSACTIONAL_LOAD.
    assert Capability.TRANSACTIONAL_LOAD not in caps


def test_mandatory_and_state_hooks_registered(
    bigquery_destination: LoadedConnector,
) -> None:
    """BigQuery is Tier A: it defines the state hooks, not state_backend; no transaction hook."""
    names = set(bigquery_destination.registry.hook_names)
    assert bigquery_destination.registry.missing_mandatory_hooks() == ()
    assert {"read_state", "commit_state", "write_run_record"} <= names
    assert "state_backend" not in names
    # TRANSACTIONAL_LOAD is not declared, so transaction hook is absent.
    assert "transaction" not in names


# --------------------------------------------------------------------------
# ddl helpers — type mapping, mode mapping, identifier safety, MERGE SQL
# --------------------------------------------------------------------------


def test_field_type_mapping_covers_every_type() -> None:
    """Every FieldType maps to its documented BigQuery type — docs/05 §3.1."""
    assert bigquery_type(FieldType.STRING) == "STRING"
    assert bigquery_type(FieldType.INTEGER) == "INT64"
    assert bigquery_type(FieldType.FLOAT) == "FLOAT64"
    assert bigquery_type(FieldType.BOOLEAN) == "BOOL"
    assert bigquery_type(FieldType.TIMESTAMP) == "TIMESTAMP"
    assert bigquery_type(FieldType.DATE) == "DATE"
    assert bigquery_type(FieldType.JSON) == "JSON"
    assert bigquery_type(FieldType.BYTES) == "BYTES"
    for ft in FieldType:
        assert isinstance(bigquery_type(ft), str)


def test_field_mode_mapping_covers_every_mode() -> None:
    """Every FieldMode maps to its BigQuery mode (NULLABLE/REQUIRED/REPEATED)."""
    assert bigquery_mode(FieldMode.NULLABLE) == "NULLABLE"
    assert bigquery_mode(FieldMode.REQUIRED) == "REQUIRED"
    assert bigquery_mode(FieldMode.REPEATED) == "REPEATED"


def test_identifier_validation_rejects_injection() -> None:
    """A non-identifier name (including backtick injection) is rejected before SQL."""
    bad_names = (
        "users`; DROP TABLE `evil",  # backtick injection attempt
        'has"quote',
        "has space",
        "1leading",
        "",
        "a-b",
        "drop;",
    )
    for bad in bad_names:
        with pytest.raises(ValueError, match="unsafe"):
            validate_identifier(bad, kind="table")


def test_identifier_validation_allows_underscore_prefixed() -> None:
    """Engine-owned names (_det_state, _det_synced_at) are valid."""
    assert validate_identifier("_det_state", kind="table") == "_det_state"
    assert validate_identifier("_det_synced_at", kind="column") == "_det_synced_at"


def test_quote_identifier_and_fq_table() -> None:
    """Quoting wraps in backticks; fq_table joins project.dataset.table."""
    assert quote_identifier("orders", kind="table") == "`orders`"
    assert fq_table("my-proj", "d", "orders") == "`my-proj`.`d`.`orders`"


def test_project_id_validation_allows_hyphens_rejects_underscores() -> None:
    """A GCP project id allows hyphens (real shape); a bare identifier does not."""
    # Allowed: a real-shape project id with hyphens.
    assert validate_identifier("my-gcp-project", kind="project") == "my-gcp-project"
    # Rejected: underscore in a project id, trailing hyphen, too short.
    for bad in ("my_project", "short", "trailing-", "Capitalized", "1leading"):
        with pytest.raises(ValueError, match="unsafe project"):
            validate_identifier(bad, kind="project")


def test_merge_sql_two_key_three_column_update() -> None:
    """MERGE SQL for a simple 2-key + 3-extra-column table is correct."""
    sql = merge_sql(
        project="my-proj",
        dataset="d",
        target_table="items",
        staging_table="items__staging_x",
        primary_key=("id", "tenant"),
        columns=("id", "tenant", "name", "amount", "ts"),
    )
    # Header — target + staging + ON clause.
    assert "MERGE INTO `my-proj`.`d`.`items` T" in sql
    assert "USING `my-proj`.`d`.`items__staging_x` S" in sql
    assert "ON T.`id` = S.`id` AND T.`tenant` = S.`tenant`" in sql
    # WHEN MATCHED updates the three non-key columns only.
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "`name` = S.`name`" in sql
    assert "`amount` = S.`amount`" in sql
    assert "`ts` = S.`ts`" in sql
    # Key columns are not in the UPDATE list.
    assert "`id` = S.`id`," not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "`tenant` = S.`tenant`," not in sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    # INSERT names every column.
    assert (
        "WHEN NOT MATCHED THEN INSERT (`id`, `tenant`, `name`, `amount`, `ts`) "
        "VALUES (S.`id`, S.`tenant`, S.`name`, S.`amount`, S.`ts`)"
    ) in sql


def test_merge_sql_all_key_columns_drops_matched_branch() -> None:
    """When every column is part of the PK there is nothing to update — branch dropped."""
    sql = merge_sql(
        project="my-proj",
        dataset="d",
        target_table="t",
        staging_table="t__stg",
        primary_key=("a", "b"),
        columns=("a", "b"),
    )
    assert "WHEN MATCHED" not in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql


# --------------------------------------------------------------------------
# open — dataset creation, bucket verification
# --------------------------------------------------------------------------


def test_open_creates_dataset_and_verifies_bucket(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """open() creates the dataset (exists_ok) and checks bucket.exists()."""
    conn = _open_with_fakes(bigquery_destination)
    bq = conn.client.bq
    assert len(bq.created_datasets) == 1
    assert bq.created_datasets[0].location == "US"
    # Bucket lookup happened (exists() returned True so no error).
    assert "fake-bucket" in conn.client.gcs._buckets


def test_open_raises_clear_error_on_missing_bucket(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing staging bucket fails open() with a clear operator-facing message."""
    class _MissingBucketStorageClient(_FakeStorageClient):
        def bucket(self, name: str) -> _FakeBucket:
            b = super().bucket(name)
            b._exists = False
            return b

    class _MissingBucketModule:
        Client = _MissingBucketStorageClient

    monkeypatch.setattr(
        bq_client_mod, "_storage_module", lambda: _MissingBucketModule
    )

    hooks = _hooks(bigquery_destination)
    with pytest.raises(ValueError, match="staging bucket"):
        hooks["open"](
            Config(
                params={
                    "project": "my-proj",
                    "dataset": "d",
                    "staging_bucket": "missing-bucket",
                }
            )
        )


def test_open_requires_project_dataset_staging_bucket(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """open() raises a clear error if any of the three required params is missing."""
    hooks = _hooks(bigquery_destination)
    with pytest.raises(ValueError, match="'project' param is required"):
        hooks["open"](Config(params={"dataset": "d", "staging_bucket": "b"}))
    with pytest.raises(ValueError, match="'dataset' param is required"):
        hooks["open"](Config(params={"project": "my-proj", "staging_bucket": "b"}))
    with pytest.raises(ValueError, match="'staging_bucket' param is required"):
        hooks["open"](Config(params={"project": "my-proj", "dataset": "d"}))


# --------------------------------------------------------------------------
# ensure_schema — table creation + additive evolution
# --------------------------------------------------------------------------


def test_ensure_schema_creates_table_with_synced_at(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """ensure_schema calls create_table with every declared field + _det_synced_at."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    bq = conn.client.bq
    assert len(bq.created_tables) == 1
    created = bq.created_tables[0]
    names = {f.name for f in created.schema}
    assert names == {"id", "name", "payload", Schema.SYNCED_AT_COLUMN}


def test_ensure_schema_additively_adds_new_column(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A new declared field is added via client.update_table(table, ['schema'])."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    # Seed the table with the initial schema.
    hooks["ensure_schema"](conn, _events_meta())
    # Now request an evolved schema (extra column).
    evolved_schema = Schema(
        fields=(
            *_events_meta().schema.fields,
            Field(name="amount", type=FieldType.FLOAT),
        )
    )
    evolved_meta = StreamMeta(
        table="events",
        write_disposition=WriteDisposition.APPEND,
        schema=evolved_schema,
    )
    hooks["ensure_schema"](conn, evolved_meta)

    bq = conn.client.bq
    assert len(bq.updated_tables) == 1
    updated_table, fields = bq.updated_tables[0]
    assert fields == ["schema"]
    assert any(f.name == "amount" for f in updated_table.schema)


def test_ensure_schema_idempotent_when_table_unchanged(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A second ensure_schema with the same schema does NOT call update_table."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())
    hooks["ensure_schema"](conn, _events_meta())  # no raise, no update_table call

    assert conn.client.bq.updated_tables == []


# --------------------------------------------------------------------------
# ensure_schema — partitioning (docs/05 §3.x)
# --------------------------------------------------------------------------


def _events_meta_with_partition(partition: PartitionConfig | None) -> StreamMeta:
    """Same shape as ``_events_meta`` but with a resolved partition spec."""
    base = _events_meta()
    return StreamMeta(
        table=base.table,
        write_disposition=base.write_disposition,
        schema=base.schema,
        primary_key=base.primary_key,
        partition=partition,
    )


def test_ensure_schema_creates_time_partitioned_table(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A TIME partition resolves to time_partitioning on the created table."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    # field name is from the schema; the BQ create path doesn't care about
    # the actual FieldType (the partition spec lives on table.time_partitioning).
    meta = _events_meta_with_partition(
        PartitionConfig(
            field="payload",
            type=PartitionType.TIME,
            granularity=TimeGranularity.DAY,
        )
    )
    hooks["ensure_schema"](conn, meta)

    created = conn.client.bq.created_tables[0]
    assert created.time_partitioning is not None
    assert created.time_partitioning.type_ == "DAY"
    assert created.time_partitioning.field == "payload"
    assert created.range_partitioning is None


def test_ensure_schema_creates_range_partitioned_table(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A RANGE partition resolves to range_partitioning on the created table."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    meta = _events_meta_with_partition(
        PartitionConfig(
            field="id",
            type=PartitionType.RANGE,
            range=PartitionRange(start=0, end=100, interval=10),
        )
    )
    hooks["ensure_schema"](conn, meta)

    created = conn.client.bq.created_tables[0]
    assert created.range_partitioning is not None
    assert created.range_partitioning.field == "id"
    assert created.range_partitioning.range_.start == 0
    assert created.range_partitioning.range_.end == 100
    assert created.range_partitioning.range_.interval == 10
    assert created.time_partitioning is None


def test_ensure_schema_creates_ingestion_partitioned_table(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """An INGESTION partition resolves to time_partitioning(field=None, DAY)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    meta = _events_meta_with_partition(
        PartitionConfig(field=None, type=PartitionType.INGESTION)
    )
    hooks["ensure_schema"](conn, meta)

    created = conn.client.bq.created_tables[0]
    assert created.time_partitioning is not None
    assert created.time_partitioning.type_ == "DAY"
    assert created.time_partitioning.field is None


def test_ensure_schema_unpartitioned_when_partition_none(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """``stream.partition = None`` creates an unpartitioned table (today's behavior)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    hooks["ensure_schema"](conn, _events_meta_with_partition(None))

    created = conn.client.bq.created_tables[0]
    assert created.time_partitioning is None
    assert created.range_partitioning is None


def test_ensure_schema_existing_table_matching_partition_succeeds(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """Re-running ensure_schema against a table with the same partition succeeds."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    partition = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.DAY,
    )
    hooks["ensure_schema"](conn, _events_meta_with_partition(partition))
    # The second call sees the now-existing table — same partition, should succeed.
    hooks["ensure_schema"](conn, _events_meta_with_partition(partition))


def test_ensure_schema_existing_table_mismatched_partition_raises(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """Partition drift raises with a message naming both specs + the fix suggestion."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    original = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.DAY,
    )
    hooks["ensure_schema"](conn, _events_meta_with_partition(original))

    # Now ask for a different granularity — drift.
    new = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.HOUR,
    )
    with pytest.raises(RuntimeError) as ei:
        hooks["ensure_schema"](conn, _events_meta_with_partition(new))
    msg = str(ei.value)
    # The message must include the existing spec, the requested spec, and the
    # actionable resolution suggestion.
    assert "TIME/DAY" in msg
    assert "TIME/HOUR" in msg
    assert "det state reset" in msg
    assert "BigQuery cannot change an existing table" in msg


def test_ensure_schema_existing_unpartitioned_table_vs_requested_partition_raises(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """An unpartitioned existing table + newly-declared partition is also drift."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    hooks["ensure_schema"](conn, _events_meta_with_partition(None))

    requested = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.DAY,
    )
    with pytest.raises(RuntimeError) as ei:
        hooks["ensure_schema"](conn, _events_meta_with_partition(requested))
    msg = str(ei.value)
    assert "(unpartitioned)" in msg
    assert "TIME/DAY" in msg


def test_ensure_schema_existing_partitioned_table_vs_no_partition_raises(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A partitioned existing table + newly-declared 'no partition' is also drift.

    The user removed the partition declaration — the engine refuses to silently
    accept the conflict; either restore the declaration or recreate the table.
    """
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    original = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.DAY,
    )
    hooks["ensure_schema"](conn, _events_meta_with_partition(original))

    with pytest.raises(RuntimeError) as ei:
        hooks["ensure_schema"](conn, _events_meta_with_partition(None))
    msg = str(ei.value)
    assert "TIME/DAY" in msg
    assert "(unpartitioned)" in msg


# --------------------------------------------------------------------------
# ddl.compare_partition — unit-tested directly (the drift comparison core)
# --------------------------------------------------------------------------


def test_compare_partition_unpartitioned_vs_unpartitioned_matches() -> None:
    """An existing table with no partition + no requested partition is a match."""

    fake = _FakeTable(_FakeTableReference(_FakeDatasetReference("p", "d"), "t"))
    status, msg = compare_partition(fake, None)
    assert status == "match"
    assert msg is None


def test_existing_table_partition_extracts_time_spec() -> None:
    """existing_table_partition correctly normalizes a fake's time_partitioning."""

    fake = _FakeTable(_FakeTableReference(_FakeDatasetReference("p", "d"), "t"))
    fake.time_partitioning = _FakeTimePartitioning(type_="HOUR", field="created")
    pc = existing_table_partition(fake)
    assert pc is not None
    assert pc.type is PartitionType.TIME
    assert pc.granularity is TimeGranularity.HOUR
    assert pc.field == "created"


def test_existing_table_partition_extracts_range_spec() -> None:
    """existing_table_partition correctly normalizes a fake's range_partitioning."""

    fake = _FakeTable(_FakeTableReference(_FakeDatasetReference("p", "d"), "t"))
    fake.range_partitioning = _FakeRangePartitioning(
        field="id",
        range_=_FakePartitionRange(start=0, end=100, interval=10),
    )
    pc = existing_table_partition(fake)
    assert pc is not None
    assert pc.type is PartitionType.RANGE
    assert pc.field == "id"
    assert pc.range == PartitionRange(start=0, end=100, interval=10)


def test_existing_table_partition_extracts_ingestion_spec() -> None:
    """existing_table_partition normalizes time_partitioning with field=None to INGESTION."""

    fake = _FakeTable(_FakeTableReference(_FakeDatasetReference("p", "d"), "t"))
    fake.time_partitioning = _FakeTimePartitioning(type_="DAY", field=None)
    pc = existing_table_partition(fake)
    assert pc is not None
    assert pc.type is PartitionType.INGESTION
    assert pc.field is None


# The ``PartitionDriftError`` class itself is exercised indirectly: every
# mismatch test above relies on it being a ``RuntimeError`` subclass (the
# class name is asserted via ``__class__.__name__`` on the raised instance).
def test_partition_drift_error_class_name(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """The exception raised on partition drift is named ``PartitionDriftError``."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    original = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.DAY,
    )
    hooks["ensure_schema"](conn, _events_meta_with_partition(original))

    new = PartitionConfig(
        field="payload",
        type=PartitionType.TIME,
        granularity=TimeGranularity.HOUR,
    )
    try:
        hooks["ensure_schema"](conn, _events_meta_with_partition(new))
    except RuntimeError as exc:
        assert type(exc).__name__ == "PartitionDriftError"
    else:
        pytest.fail("expected PartitionDriftError to be raised")


# --------------------------------------------------------------------------
# write_batch — APPEND
# --------------------------------------------------------------------------


def test_write_batch_append_loads_with_write_append(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """APPEND: build Parquet, upload to GCS, LOAD with WRITE_APPEND, delete blob on success."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    n = hooks["write_batch"](
        conn,
        [{"id": 1, "name": "a", "payload": {"k": "v"}}],
        _events_meta(),
    )
    assert n == 1

    bq = conn.client.bq
    assert len(bq.load_jobs) == 1
    job = bq.load_jobs[0]
    assert job["table"] == "events"
    assert job["write_disposition"] == "WRITE_APPEND"
    # URI shape: gs://bucket/prefix/run_suffix/events/batch-<uuid>.parquet
    assert job["uri"].startswith("gs://fake-bucket/det/staging/")
    assert "/events/batch-" in job["uri"]
    assert job["uri"].endswith(".parquet")
    # GCS: one upload, one delete (success path cleans up).
    bucket = conn.client.gcs._buckets["fake-bucket"]
    assert len(bucket._uploads) == 1
    assert len(bucket._deletes) == 1


# --------------------------------------------------------------------------
# write_batch — REPLACE — first batch truncates, rest append (same run)
# --------------------------------------------------------------------------


def test_write_batch_replace_truncates_first_appends_rest(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """REPLACE: first batch uses WRITE_TRUNCATE; subsequent batches use WRITE_APPEND."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    meta = _events_meta(WriteDisposition.REPLACE)
    hooks["write_batch"](conn, [{"id": 1, "name": "a"}], meta)
    hooks["write_batch"](conn, [{"id": 2, "name": "b"}], meta)
    hooks["write_batch"](conn, [{"id": 3, "name": "c"}], meta)

    dispositions = [job["write_disposition"] for job in conn.client.bq.load_jobs]
    assert dispositions == ["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_APPEND"]


def test_write_batch_replace_empty_batch_still_truncates(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """An empty REPLACE batch still triggers the truncate — full snapshot empty case."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    hooks["write_batch"](conn, [], _events_meta(WriteDisposition.REPLACE))

    jobs = conn.client.bq.load_jobs
    assert len(jobs) == 1
    assert jobs[0]["write_disposition"] == "WRITE_TRUNCATE"


# --------------------------------------------------------------------------
# write_batch — MERGE — staging table + MERGE + drop
# --------------------------------------------------------------------------


def test_write_batch_merge_loads_to_staging_runs_merge_drops_staging(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """MERGE: LOAD into a staging table, run MERGE INTO target, drop the staging table."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    meta = _events_meta(WriteDisposition.MERGE, primary_key=("id",))
    hooks["write_batch"](conn, [{"id": 1, "name": "a"}], meta)

    bq = conn.client.bq
    # One LOAD into a staging table.
    assert len(bq.load_jobs) == 1
    staging_table = bq.load_jobs[0]["table"]
    assert staging_table.startswith("events__staging_")
    assert bq.load_jobs[0]["write_disposition"] == "WRITE_TRUNCATE"
    # One MERGE query.
    assert len(bq.queries) == 1
    merge_sql_text = bq.queries[0]["sql"]
    assert "MERGE INTO `fake-project-id`.`fake_ds`.`events` T" in merge_sql_text
    assert f"USING `fake-project-id`.`fake_ds`.`{staging_table}` S" in merge_sql_text
    # Staging table dropped.
    assert staging_table in bq.deleted_tables


def test_write_batch_merge_drops_staging_even_on_merge_failure(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A MERGE failure still drops the staging table (cleanup in finally)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    # Seed a query job that raises a non-retryable error from result().
    fail_job = _FakeJob()
    fail_job.raise_on_result = _NonRetryableError("merge bad sql")
    conn.client.bq.query_job_queue.append(fail_job)

    meta = _events_meta(WriteDisposition.MERGE, primary_key=("id",))
    with pytest.raises(_NonRetryableError):
        hooks["write_batch"](conn, [{"id": 1, "name": "a"}], meta)

    # Staging LOAD happened (one), staging table dropped despite MERGE fail.
    bq = conn.client.bq
    assert len(bq.load_jobs) == 1
    staging_table = bq.load_jobs[0]["table"]
    assert staging_table in bq.deleted_tables


def test_write_batch_merge_requires_primary_key(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """MERGE without a primary_key fails fast with a clear message."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    with pytest.raises(ValueError, match="primary_key"):
        hooks["write_batch"](
            conn,
            [{"id": 1, "name": "a"}],
            _events_meta(WriteDisposition.MERGE),  # no primary_key
        )


# --------------------------------------------------------------------------
# Retry path — transient 503 retries, non-retryable 4xx raises
# --------------------------------------------------------------------------


class _RetryableHTTPError(Exception):
    code = 503


class _NonRetryableError(Exception):
    code = 403


def test_load_job_retries_on_transient_error(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A transient 503 from job.result() retries with backoff; succeeds on the second attempt."""
    conn = _open_with_fakes(bigquery_destination, retry_max_attempts=3, retry_backoff_seconds=0.0)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    attempts = {"n": 0}

    def factory() -> _FakeJob:
        attempts["n"] += 1
        job = _FakeJob()
        if attempts["n"] == 1:
            job.raise_on_result = _RetryableHTTPError("rate-limited")
        return job

    conn.client.bq.load_job_factory = factory

    n = hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())
    assert n == 1
    assert attempts["n"] == 2  # one failure, one success


def test_load_job_does_not_retry_on_non_retryable_error(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A non-retryable 4xx (e.g. 403 permission denied) surfaces immediately."""
    conn = _open_with_fakes(bigquery_destination, retry_max_attempts=5)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    attempts = {"n": 0}

    def factory() -> _FakeJob:
        attempts["n"] += 1
        job = _FakeJob()
        job.raise_on_result = _NonRetryableError("permission denied")
        return job

    conn.client.bq.load_job_factory = factory

    with pytest.raises(_NonRetryableError):
        hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())
    assert attempts["n"] == 1  # zero retries on a non-retryable error


def test_load_failure_leaves_staging_blob_for_forensics(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """On LOAD failure the GCS staging Parquet is NOT deleted (forensics rule)."""
    conn = _open_with_fakes(bigquery_destination, retry_max_attempts=1)
    hooks = _hooks(bigquery_destination)
    hooks["ensure_schema"](conn, _events_meta())

    def factory() -> _FakeJob:
        job = _FakeJob()
        job.raise_on_result = _NonRetryableError("schema mismatch")
        return job

    conn.client.bq.load_job_factory = factory

    with pytest.raises(_NonRetryableError):
        hooks["write_batch"](conn, [{"id": 1, "name": "a"}], _events_meta())

    bucket = conn.client.gcs._buckets["fake-bucket"]
    assert len(bucket._uploads) == 1
    # No delete — the blob is left for forensics.
    assert bucket._deletes == []


# --------------------------------------------------------------------------
# State table — read/commit round trip + string cursor + upsert
# --------------------------------------------------------------------------


def _seed_state_query_response(
    bq: _FakeBigQueryClient, rows: list[dict[str, Any]]
) -> None:
    """Queue a query response (the next SELECT will get these rows)."""
    bq.query_job_queue.append(_FakeJob(rows=rows))


def test_read_state_returns_empty_on_first_run(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """read_state on an empty connector returns [] (table created lazily)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    # No canned query response → default empty job.
    records = hooks["read_state"](conn, "echo")
    assert records == []
    # The state table was created lazily.
    table_names = [t.table_id for t in conn.client.bq.created_tables]
    assert "_det_state" in table_names


def test_commit_state_and_read_state_round_trip(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A StateRecord makes the commit → read round trip with every field intact."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    original = StateRecord(
        connector="echo",
        stream="items",
        cursor_value=5,
        cursor_type=CursorType.INT,
        state_blob={"page_token": "abc", "nested": {"k": [1, 2]}},
        last_run_id="run-001",
        rows_total=42,
        updated_at=datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC),
    )
    hooks["commit_state"](conn, "run-001", [original])

    # Now seed the read response with what BigQuery would return.
    import json
    _seed_state_query_response(
        conn.client.bq,
        [
            {
                "connector": "echo",
                "stream": "items",
                "cursor_value": json.dumps(5),
                "cursor_type": "int",
                "state_blob": json.dumps({"page_token": "abc", "nested": {"k": [1, 2]}}),
                "last_run_id": "run-001",
                "rows_total": 42,
                "updated_at": datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC),
            }
        ],
    )
    loaded = hooks["read_state"](conn, "echo")

    assert len(loaded) == 1
    rec = loaded[0]
    assert rec.connector == "echo"
    assert rec.stream == "items"
    assert rec.cursor_value == 5
    assert rec.cursor_type is CursorType.INT
    assert rec.state_blob == {"page_token": "abc", "nested": {"k": [1, 2]}}
    assert rec.last_run_id == "run-001"
    assert rec.rows_total == 42


def test_commit_state_string_cursor_value_is_json_encoded(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """A bare-string cursor_value commits without raising (stage-7 _encode_json_column fix)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    rec = StateRecord(
        connector="src",
        stream="rows",
        cursor_value="2026-05-20T00:00:00",
        cursor_type=CursorType.STRING,
        rows_total=7,
    )
    hooks["commit_state"](conn, "run-str", [rec])
    # The commit MERGE was issued; the cursor_value parameter went through
    # _encode_json_column → json.dumps, so the value bound to @cursor_value
    # is the JSON-text string '"2026-05-20T00:00:00"' (not the bare string).
    merge_query = conn.client.bq.queries[-1]
    params = {p.name: p.value for p in merge_query["job_config"].query_parameters}
    assert params["cursor_value"] == '"2026-05-20T00:00:00"'


def test_commit_state_stamps_run_id_when_unset(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """commit_state fills last_run_id from the run_id arg when the record left it None."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    hooks["commit_state"](
        conn, "run-xyz", [StateRecord(connector="echo", stream="events")]
    )
    merge_query = conn.client.bq.queries[-1]
    params = {p.name: p.value for p in merge_query["job_config"].query_parameters}
    assert params["last_run_id"] == "run-xyz"
    # updated_at was stamped to a datetime.
    assert isinstance(params["updated_at"], datetime)


# --------------------------------------------------------------------------
# write_run_record — _det_runs upsert on run_id
# --------------------------------------------------------------------------


def _build_record(run_id: str = "run-abc") -> RunRecord:
    started = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
    ended = started + timedelta(seconds=5)
    return RunRecord(
        run_id=run_id,
        config="echo_dev",
        source="echo",
        destination="bigquery",
        target="dev",
        status=RunStatus.SUCCEEDED,
        started_at=started,
        ended_at=ended,
        rows_loaded=9,
        streams=(StreamResult(name="events", rows_loaded=4),),
        full_refresh=False,
    )


def test_write_run_record_creates_table_and_upserts(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """write_run_record creates _det_runs lazily and issues a parameterized MERGE."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    hooks["write_run_record"](conn, _build_record())

    # Runs table created.
    assert any(t.table_id == "_det_runs" for t in conn.client.bq.created_tables)
    # One MERGE on run_id was issued, parameterized.
    assert len(conn.client.bq.queries) == 1
    q = conn.client.bq.queries[0]
    assert "MERGE INTO" in q["sql"]
    assert "T.run_id = S.run_id" in q["sql"]
    params = {p.name: p.value for p in q["job_config"].query_parameters}
    assert params["run_id"] == "run-abc"
    assert params["config"] == "echo_dev"
    assert params["status"] == "succeeded"
    assert params["rows_loaded"] == 9
    assert params["duration_s"] == 5.0
    # streams_json is JSON-encoded (it's not None — the record has streams).
    assert params["streams_json"] is not None


def test_write_run_record_is_idempotent_on_run_id(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """Writing the same run_id twice issues two MERGEs (upsert semantics — table dedups)."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)
    hooks["write_run_record"](conn, _build_record())
    hooks["write_run_record"](conn, _build_record())
    # Both calls go through; the SQL is a MERGE so the second is an update.
    assert len(conn.client.bq.queries) == 2


# --------------------------------------------------------------------------
# close — never raises
# --------------------------------------------------------------------------


def test_engine_resolves_destination_hooks_without_transaction(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """The engine accepts the BigQuery destination — 4 caps, no transaction hook.

    Proves the wiring end-to-end at the contract boundary: the engine's
    ``_resolve_destination_hooks`` is what the run loop calls, and a
    destination missing a mandatory or capability-required hook would
    raise here. A successful resolution returns the hook dict + the
    declared capability set.
    """
    from det.engine.runner import _resolve_destination_hooks

    hooks, caps = _resolve_destination_hooks(bigquery_destination)
    # The 4 declared capabilities, no TRANSACTIONAL_LOAD.
    assert caps == {
        Capability.STATE,
        Capability.MERGE,
        Capability.SCHEMA_EVOLUTION,
        Capability.RUN_RECORDS,
    }
    # Every required hook (core + state + run_records) is bound; no
    # transaction hook (since TRANSACTIONAL_LOAD is not declared).
    expected = {
        "capabilities",
        "open",
        "ensure_schema",
        "write_batch",
        "close",
        "read_state",
        "commit_state",
        "write_run_record",
    }
    assert set(hooks) == expected
    assert "transaction" not in hooks


def test_close_never_raises(
    bigquery_destination: LoadedConnector,
    fake_bq: _FakeBigQueryModule,
    fake_gcs: type[_FakeStorageClient],
) -> None:
    """close() is safe to call even when the underlying SDK raises."""
    conn = _open_with_fakes(bigquery_destination)
    hooks = _hooks(bigquery_destination)

    # Force the BQ client's close() to raise — the hook must swallow it.
    def boom() -> None:
        raise RuntimeError("close error")

    conn.client.bq.close = boom  # type: ignore[assignment]
    hooks["close"](conn)  # must not raise

    # Calling close twice in a row is also safe.
    hooks["close"](conn)


# --------------------------------------------------------------------------
# Integration tests (live BigQuery) — gated by env vars + the integration marker
# --------------------------------------------------------------------------


_INTEGRATION_ENV_VARS = (
    "BIGQUERY_TEST_PROJECT",
    "BIGQUERY_TEST_DATASET",
    "BIGQUERY_TEST_STAGING_BUCKET",
)


def _have_live_creds() -> bool:
    return all(os.getenv(v) for v in _INTEGRATION_ENV_VARS)


@pytest.mark.integration
@pytest.mark.skipif(not _have_live_creds(), reason="needs live BigQuery (set BIGQUERY_TEST_*)")
def test_integration_round_trip_against_live_bigquery(
    bigquery_destination: LoadedConnector,
) -> None:
    """End-to-end: open → ensure_schema → write_batch → read_state → close against live BQ.

    Only runs when ``BIGQUERY_TEST_PROJECT`` / ``BIGQUERY_TEST_DATASET`` /
    ``BIGQUERY_TEST_STAGING_BUCKET`` env vars are set. Drops the test
    table at the end.
    """
    project = os.environ["BIGQUERY_TEST_PROJECT"]
    dataset = os.environ["BIGQUERY_TEST_DATASET"]
    staging = os.environ["BIGQUERY_TEST_STAGING_BUCKET"]
    unique = uuid.uuid4().hex[:8]
    table = f"det_it_events_{unique}"

    hooks = _hooks(bigquery_destination)
    conn = hooks["open"](
        Config(
            params={
                "project": project,
                "dataset": dataset,
                "staging_bucket": staging,
            }
        )
    )
    try:
        meta = StreamMeta(
            table=table,
            write_disposition=WriteDisposition.APPEND,
            schema=Schema(
                fields=(
                    Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
                    Field(name="name", type=FieldType.STRING),
                )
            ),
        )
        hooks["ensure_schema"](conn, meta)
        n = hooks["write_batch"](
            conn, [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}], meta
        )
        assert n == 2

        # State round trip.
        hooks["commit_state"](
            conn,
            f"run-{unique}",
            [
                StateRecord(
                    connector="echo_it",
                    stream=table,
                    cursor_value=2,
                    cursor_type=CursorType.INT,
                    rows_total=2,
                )
            ],
        )
        records = hooks["read_state"](conn, "echo_it")
        assert any(r.stream == table and r.cursor_value == 2 for r in records)
    finally:
        # Best-effort cleanup of the test table; the SDK is available.
        try:
            from google.cloud import bigquery as bq  # noqa: PLC0415 — local cleanup
            client = bq.Client(project=project)
            client.delete_table(f"{project}.{dataset}.{table}", not_found_ok=True)
        except Exception:  # noqa: BLE001 — cleanup; logged below if it matters
            pass
        hooks["close"](conn)


@pytest.mark.integration
@pytest.mark.skipif(not _have_live_creds(), reason="needs live BigQuery (set BIGQUERY_TEST_*)")
def test_integration_partition_auto_default_and_drift_against_live_bigquery(
    bigquery_destination: LoadedConnector,
) -> None:
    """End-to-end partitioning: create with TIME+DAY, reread the spec, then trigger drift.

    Three phases:

    1. ``ensure_schema`` with a TIME+DAY partition spec → ``get_table().time_partitioning``
       must show DAY/<field>.
    2. ``write_batch`` lands rows into the partitioned table.
    3. ``ensure_schema`` with a DIFFERENT partition spec on the same table →
       raises ``PartitionDriftError`` (a ``RuntimeError`` subclass) with the
       expected message shape.

    Only runs when ``BIGQUERY_TEST_PROJECT`` / ``BIGQUERY_TEST_DATASET`` /
    ``BIGQUERY_TEST_STAGING_BUCKET`` env vars are set. Drops the test
    table at the end.
    """
    project = os.environ["BIGQUERY_TEST_PROJECT"]
    dataset = os.environ["BIGQUERY_TEST_DATASET"]
    staging = os.environ["BIGQUERY_TEST_STAGING_BUCKET"]
    unique = uuid.uuid4().hex[:8]
    table = f"det_it_part_{unique}"

    hooks = _hooks(bigquery_destination)
    conn = hooks["open"](
        Config(
            params={
                "project": project,
                "dataset": dataset,
                "staging_bucket": staging,
            }
        )
    )
    try:
        from datetime import UTC as _UTC  # noqa: PLC0415 — local
        from datetime import datetime as _dt

        meta = StreamMeta(
            table=table,
            write_disposition=WriteDisposition.APPEND,
            schema=Schema(
                fields=(
                    Field(name="id", type=FieldType.INTEGER, mode=FieldMode.REQUIRED),
                    Field(
                        name="created_at",
                        type=FieldType.TIMESTAMP,
                        mode=FieldMode.REQUIRED,
                    ),
                )
            ),
            partition=PartitionConfig(
                field="created_at",
                type=PartitionType.TIME,
                granularity=TimeGranularity.DAY,
            ),
        )
        hooks["ensure_schema"](conn, meta)
        # Re-read the table to verify the partition landed.
        from google.cloud import bigquery as bq  # noqa: PLC0415 — local
        client = bq.Client(project=project)
        table_ref = client.get_table(f"{project}.{dataset}.{table}")
        assert table_ref.time_partitioning is not None
        assert str(table_ref.time_partitioning.type_).upper() == "DAY"
        assert table_ref.time_partitioning.field == "created_at"

        n = hooks["write_batch"](
            conn,
            [
                {"id": 1, "created_at": _dt(2026, 1, 1, tzinfo=_UTC)},
                {"id": 2, "created_at": _dt(2026, 1, 2, tzinfo=_UTC)},
            ],
            meta,
        )
        assert n == 2

        # Drift: same table, different partition spec → expect RuntimeError.
        drift_meta = StreamMeta(
            table=table,
            write_disposition=WriteDisposition.APPEND,
            schema=meta.schema,
            partition=PartitionConfig(
                field="created_at",
                type=PartitionType.TIME,
                granularity=TimeGranularity.HOUR,
            ),
        )
        with pytest.raises(RuntimeError) as ei:
            hooks["ensure_schema"](conn, drift_meta)
        assert "TIME/DAY" in str(ei.value)
        assert "TIME/HOUR" in str(ei.value)
    finally:
        try:
            from google.cloud import bigquery as bq  # noqa: PLC0415 — local cleanup
            client = bq.Client(project=project)
            client.delete_table(f"{project}.{dataset}.{table}", not_found_ok=True)
        except Exception:  # noqa: BLE001 — cleanup
            pass
        hooks["close"](conn)
