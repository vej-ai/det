"""Unit tests for the det contract types — ``det/types.py``.

These exercise *behavior*: enum parsing from YAML scalars, schema lookup and
the engine-appended column, cursor observe/start_value semantics, Config
immutability and attribute access, manifest validation rules, and StateRecord
/ RunResult round-trips.
"""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta

import pytest

from det.types import (
    Batch,
    Capability,
    Config,
    ConnectorKind,
    ConnectorManifest,
    Cursor,
    CursorType,
    DestinationBinding,
    Field,
    FieldMode,
    FieldType,
    Incremental,
    ParamSpec,
    ParamType,
    Record,
    RunConfig,
    RunResult,
    RunStatus,
    Schema,
    SchemaContract,
    SecretRef,
    State,
    StateBackend,
    StateRecord,
    StreamDef,
    StreamResult,
    StreamStatus,
    WriteDisposition,
)

# ---------------------------------------------------------------------------
# Enum parsing
# ---------------------------------------------------------------------------


def test_enum_parse_accepts_canonical_value() -> None:
    """parse() returns the member for its canonical string value."""
    assert WriteDisposition.parse("merge") is WriteDisposition.MERGE
    assert FieldType.parse("STRING") is FieldType.STRING
    assert CursorType.parse("timestamp") is CursorType.TIMESTAMP


def test_enum_parse_is_case_insensitive() -> None:
    """A YAML author may write any casing — parsing is case-insensitive."""
    assert WriteDisposition.parse("MERGE") is WriteDisposition.MERGE
    assert WriteDisposition.parse("Merge") is WriteDisposition.MERGE
    assert FieldType.parse("string") is FieldType.STRING
    assert ConnectorKind.parse("SOURCE") is ConnectorKind.SOURCE


def test_enum_parse_accepts_member_name() -> None:
    """parse() also accepts the uppercase member name."""
    assert CursorType.parse("INT") is CursorType.INT
    assert ParamType.parse("BOOL") is ParamType.BOOL


def test_enum_parse_passes_through_existing_member() -> None:
    """Passing an already-parsed member returns it unchanged."""
    assert WriteDisposition.parse(WriteDisposition.APPEND) is WriteDisposition.APPEND


def test_enum_parse_rejects_unknown_value() -> None:
    """An unknown value raises ValueError listing the valid options."""
    with pytest.raises(ValueError, match="not a valid WriteDisposition"):
        WriteDisposition.parse("upsert")


def test_enum_parse_rejects_non_string() -> None:
    """A non-string scalar raises ValueError."""
    with pytest.raises(ValueError, match="expects a string"):
        FieldType.parse(42)


def test_capability_has_exactly_four_members() -> None:
    """Capability matches docs/05 §1 exactly — four members, no more."""
    assert {c.name for c in Capability} == {
        "STATE",
        "MERGE",
        "SCHEMA_EVOLUTION",
        "TRANSACTIONAL_LOAD",
    }


def test_schema_contract_default_is_evolve() -> None:
    """Locked decision: schema contract default is 'evolve'."""
    assert SchemaContract.parse("evolve") is SchemaContract.EVOLVE
    assert SchemaContract.parse("strict") is SchemaContract.STRICT


# ---------------------------------------------------------------------------
# Field / Schema
# ---------------------------------------------------------------------------


def test_field_from_dict_defaults() -> None:
    """Field.from_dict applies the documented defaults (NULLABLE mode, '' desc)."""
    f = Field.from_dict({"name": "id", "type": "STRING"})
    assert f.name == "id"
    assert f.type is FieldType.STRING
    assert f.mode is FieldMode.NULLABLE
    assert f.description == ""


def test_field_from_dict_rejects_unknown_key() -> None:
    """An unknown schema-field key is a hard error (docs/03 §7)."""
    with pytest.raises(ValueError, match="unknown schema field key"):
        Field.from_dict({"name": "id", "type": "STRING", "nulable": True})


def test_field_from_dict_requires_type() -> None:
    """A schema field without a 'type' is rejected."""
    with pytest.raises(ValueError, match="requires a 'type'"):
        Field.from_dict({"name": "id"})


def test_schema_field_lookup() -> None:
    """Schema.field looks up by column name; has() reports presence."""
    schema = Schema.from_list(
        [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "amount", "type": "FLOAT"},
        ]
    )
    assert schema is not None
    assert schema.field("amount").type is FieldType.FLOAT  # type: ignore[union-attr]
    assert schema.field("missing") is None
    assert schema.has("id")
    assert not schema.has("missing")
    assert schema.names == ("id", "amount")
    assert len(schema) == 2


def test_schema_from_list_none_returns_none() -> None:
    """A None schema list means 'infer' — from_list returns None, not empty."""
    assert Schema.from_list(None) is None


def test_schema_with_synced_at_appends_column() -> None:
    """with_synced_at appends the engine column without mutating the original."""
    schema = Schema.from_list([{"name": "id", "type": "STRING"}])
    assert schema is not None
    evolved = schema.with_synced_at()
    assert not schema.has(Schema.SYNCED_AT_COLUMN)  # original untouched
    assert evolved.has(Schema.SYNCED_AT_COLUMN)
    synced = evolved.field(Schema.SYNCED_AT_COLUMN)
    assert synced is not None
    assert synced.type is FieldType.TIMESTAMP
    assert synced.mode is FieldMode.NULLABLE


def test_schema_with_synced_at_is_idempotent() -> None:
    """Re-appending the synced_at column is a no-op (safe on resume)."""
    schema = Schema.from_list([{"name": "id", "type": "STRING"}])
    assert schema is not None
    once = schema.with_synced_at()
    twice = once.with_synced_at()
    assert len(twice) == len(once)
    assert twice is once  # idempotent path returns self


def test_schema_is_iterable() -> None:
    """A Schema iterates its fields in declared order."""
    schema = Schema.from_list([{"name": "a", "type": "STRING"}, {"name": "b", "type": "INTEGER"}])
    assert schema is not None
    assert [f.name for f in schema] == ["a", "b"]


# ---------------------------------------------------------------------------
# ParamSpec / SecretRef / Incremental
# ---------------------------------------------------------------------------


def test_param_spec_from_dict() -> None:
    """ParamSpec parses type, default and required from a YAML mapping."""
    ps = ParamSpec.from_dict({"type": "int", "default": 50, "description": "page size"})
    assert ps.type is ParamType.INT
    assert ps.default == 50
    assert ps.required is False
    ps2 = ParamSpec.from_dict({"type": "string", "required": True})
    assert ps2.required is True
    assert ps2.default is None


def test_param_type_distinct_from_field_type() -> None:
    """ParamType (lowercase knobs) is a different enum from FieldType (columns)."""
    assert ParamType.STRING.value == "string"
    assert FieldType.STRING.value == "STRING"
    assert ParamType is not FieldType


def test_secret_ref_accepts_env_and_profile_forms() -> None:
    """The two — and only two — resolver forms are accepted."""
    s1 = SecretRef.from_dict({"name": "tok", "ref": "${env.API_TOKEN}"})
    assert s1.ref == "${env.API_TOKEN}"
    s2 = SecretRef.from_dict({"name": "rt", "ref": "${profile.shiphero.refresh_token}"})
    assert s2.ref == "${profile.shiphero.refresh_token}"


def test_secret_ref_rejects_vault_form() -> None:
    """A ${secret...}/vault form is rejected — locked decision: only two resolvers."""
    with pytest.raises(ValueError, match="known resolver"):
        SecretRef.from_dict({"name": "tok", "ref": "${secret.API_TOKEN}"})
    with pytest.raises(ValueError, match="known resolver"):
        SecretRef.from_dict({"name": "tok", "ref": "literal-value"})


def test_secret_ref_is_valid_ref() -> None:
    """is_valid_ref is the discovery-time syntax check for resolver forms."""
    assert SecretRef.is_valid_ref("${env.X}")
    assert SecretRef.is_valid_ref("${profile.a.b}")
    assert not SecretRef.is_valid_ref("${vault.x}")
    assert not SecretRef.is_valid_ref("${env.X")  # missing closing brace


def test_incremental_from_dict_defaults() -> None:
    """Incremental defaults cursor_type to timestamp; lookback/initial optional."""
    inc = Incremental.from_dict({"cursor_field": "created_at"})
    assert inc.cursor_field == "created_at"
    assert inc.cursor_type is CursorType.TIMESTAMP
    assert inc.lookback is None
    assert inc.initial_value is None
    inc2 = Incremental.from_dict(
        {
            "cursor_field": "d",
            "cursor_type": "date",
            "lookback": "2d",
            "initial_value": "2024-01-01",
        }
    )
    assert inc2.cursor_type is CursorType.DATE
    assert inc2.lookback == "2d"


# ---------------------------------------------------------------------------
# StreamDef
# ---------------------------------------------------------------------------


def test_stream_def_from_dict_minimal() -> None:
    """A minimal stream defaults table to name and disposition to append."""
    sd = StreamDef.from_dict({"name": "orders"})
    assert sd.table == "orders"
    assert sd.write_disposition is WriteDisposition.APPEND
    assert sd.schema_contract is SchemaContract.EVOLVE
    assert sd.primary_key == ()
    assert not sd.is_incremental


def test_stream_def_primary_key_string_becomes_tuple() -> None:
    """A scalar primary_key is normalized to a one-element tuple."""
    sd = StreamDef.from_dict({"name": "s", "primary_key": "id", "write_disposition": "merge"})
    assert sd.primary_key == ("id",)
    sd2 = StreamDef.from_dict(
        {"name": "s2", "primary_key": ["date", "currency"], "write_disposition": "merge"}
    )
    assert sd2.primary_key == ("date", "currency")


def test_stream_def_merge_requires_primary_key() -> None:
    """write_disposition merge without a primary_key fails validation."""
    with pytest.raises(ValueError, match="requires a primary_key"):
        StreamDef.from_dict({"name": "s", "write_disposition": "merge"})


def test_stream_def_cursor_field_must_be_in_schema() -> None:
    """If both schema and incremental are declared, cursor_field must be a column."""
    with pytest.raises(ValueError, match="not in the declared schema"):
        StreamDef.from_dict(
            {
                "name": "s",
                "incremental": {"cursor_field": "created_at"},
                "schema": [{"name": "id", "type": "STRING"}],
            }
        )


def test_stream_def_cursor_field_in_schema_ok() -> None:
    """A cursor_field present in the schema passes validation."""
    sd = StreamDef.from_dict(
        {
            "name": "s",
            "incremental": {"cursor_field": "created_at", "cursor_type": "timestamp"},
            "schema": [
                {"name": "id", "type": "STRING"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
        }
    )
    assert sd.is_incremental
    assert sd.incremental is not None
    assert sd.incremental.cursor_field == "created_at"


def test_stream_def_rejects_unknown_key() -> None:
    """An unknown stream key catches typos like write_dispostion."""
    with pytest.raises(ValueError, match="unknown stream key"):
        StreamDef.from_dict({"name": "s", "write_dispostion": "append"})


# ---------------------------------------------------------------------------
# ConnectorManifest
# ---------------------------------------------------------------------------


def _source_manifest_dict() -> dict:
    """A minimal valid kind:source manifest dict."""
    return {
        "name": "exchange_rates",
        "kind": "source",
        "streams": [{"name": "rates", "table": "fx_rates"}],
    }


def test_manifest_source_parses() -> None:
    """A well-formed source manifest parses with documented defaults."""
    m = ConnectorManifest.from_dict(_source_manifest_dict())
    assert m.kind is ConnectorKind.SOURCE
    assert m.version == "0.1.0"
    assert len(m.streams) == 1
    assert m.stream("rates") is not None
    assert m.stream("missing") is None


def test_manifest_source_requires_streams() -> None:
    """kind:source with no streams fails (docs/03 §7 step 3)."""
    with pytest.raises(ValueError, match="requires a non-empty 'streams'"):
        ConnectorManifest.from_dict({"name": "x", "kind": "source"})


def test_manifest_destination_forbids_streams() -> None:
    """kind:destination must not declare streams."""
    with pytest.raises(ValueError, match="must not declare 'streams'"):
        ConnectorManifest.from_dict(
            {"name": "bq", "kind": "destination", "streams": [{"name": "s"}]}
        )


def test_manifest_destination_forbids_binding() -> None:
    """kind:destination must not declare a destination binding."""
    with pytest.raises(ValueError, match="must not declare a 'destination'"):
        ConnectorManifest.from_dict(
            {"name": "bq", "kind": "destination", "destination": {"connector": "other"}}
        )


def test_manifest_destination_parses() -> None:
    """A minimal destination manifest parses cleanly."""
    m = ConnectorManifest.from_dict({"name": "bigquery", "kind": "destination"})
    assert m.kind is ConnectorKind.DESTINATION
    assert m.streams == ()


def test_manifest_rejects_unknown_top_level_key() -> None:
    """An unknown top-level key is a hard error (docs/03 §7 step 2)."""
    d = _source_manifest_dict()
    d["destinaton"] = {"connector": "bq"}  # typo
    with pytest.raises(ValueError, match="unknown register.yaml key"):
        ConnectorManifest.from_dict(d)


def test_manifest_rejects_duplicate_stream_names() -> None:
    """Duplicate stream names fail validation (docs/03 §7 step 4)."""
    d = _source_manifest_dict()
    d["streams"] = [{"name": "rates"}, {"name": "rates"}]
    with pytest.raises(ValueError, match="duplicate stream name"):
        ConnectorManifest.from_dict(d)


def test_manifest_parses_secrets_and_destination() -> None:
    """Secrets, the destination binding and routing params parse correctly."""
    d = _source_manifest_dict()
    d["secrets"] = [{"name": "api_key", "ref": "${env.OPENRATES_API_KEY}"}]
    d["destination"] = {"connector": "bigquery", "dataset": "finance"}
    d["tags"] = ["finance", "rest"]
    m = ConnectorManifest.from_dict(d)
    assert len(m.secrets) == 1
    assert m.secrets[0].name == "api_key"
    assert m.destination is not None
    assert m.destination.connector == "bigquery"
    assert m.destination.routing == {"dataset": "finance"}
    assert m.tags == ("finance", "rest")


def test_destination_binding_requires_connector() -> None:
    """A destination binding without 'connector' is rejected."""
    with pytest.raises(ValueError, match="requires a 'connector'"):
        DestinationBinding.from_dict({"dataset": "finance"})


# ---------------------------------------------------------------------------
# Config — immutability + access
# ---------------------------------------------------------------------------


def test_config_attribute_access_reads_params() -> None:
    """config.foo is sugar for config.params['foo']."""
    cfg = Config(params={"page_size": 50, "start_date": "2025-01-01"})
    assert cfg.page_size == 50
    assert cfg.start_date == "2025-01-01"


def test_config_secrets_read_by_subscript() -> None:
    """Secrets are read via config.secrets['name'], never as attributes."""
    cfg = Config(params={}, secrets={"api_token": "tok-123"})
    assert cfg.secrets["api_token"] == "tok-123"
    assert cfg.has_secret("api_token")
    assert not cfg.has_secret("absent")


def test_config_missing_param_raises_attribute_error() -> None:
    """An unknown param raises AttributeError, so hasattr() works."""
    cfg = Config(params={"page_size": 50})
    with pytest.raises(AttributeError):
        _ = cfg.nonexistent
    assert not hasattr(cfg, "nonexistent")
    assert hasattr(cfg, "page_size")


def test_config_is_immutable() -> None:
    """Config is frozen — assigning to a field raises."""
    cfg = Config(params={"page_size": 50})
    with pytest.raises(FrozenInstanceError):
        cfg.params = {}  # type: ignore[misc]


def test_config_get_with_default() -> None:
    """Config.get returns a default for an absent param."""
    cfg = Config(params={"page_size": 50})
    assert cfg.get("page_size") == 50
    assert cfg.get("missing", 99) == 99


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def test_state_read_write_and_default() -> None:
    """State is mutable dict-like scratch space."""
    st = State({"token": "abc"})
    assert st["token"] == "abc"
    assert "token" in st
    st["page"] = 7
    assert st.get("page") == 7
    assert st.get("missing", 0) == 0
    st.set("via_set", True)
    assert st["via_set"] is True


def test_state_delete_and_len() -> None:
    """State supports deletion and length."""
    st = State({"a": 1, "b": 2})
    assert len(st) == 2
    del st["a"]
    assert "a" not in st
    assert len(st) == 1


def test_state_to_dict_is_a_copy() -> None:
    """to_dict returns a copy — mutating it does not affect the State."""
    st = State({"a": 1})
    snapshot = st.to_dict()
    snapshot["a"] = 999
    assert st["a"] == 1


def test_state_equality() -> None:
    """Two States with equal contents compare equal."""
    assert State({"a": 1}) == State({"a": 1})
    assert State({"a": 1}) != State({"a": 2})


# ---------------------------------------------------------------------------
# Cursor
# ---------------------------------------------------------------------------


def test_cursor_start_value_returns_resume_point() -> None:
    """start_value returns the engine-supplied resume point."""
    cur = Cursor("created_at", CursorType.TIMESTAMP, start_value="2025-01-01")
    assert cur.start_value() == "2025-01-01"
    assert cur.cursor_field == "created_at"
    assert cur.cursor_type is CursorType.TIMESTAMP
    assert not cur.is_full_refresh


def test_cursor_full_refresh_start_value_is_none() -> None:
    """Under --full-refresh, start_value is None regardless of the seed."""
    cur = Cursor("created_at", CursorType.TIMESTAMP, start_value="2025-01-01", is_full_refresh=True)
    assert cur.is_full_refresh
    assert cur.start_value() is None


def test_cursor_observe_tracks_max() -> None:
    """observe tracks the maximum across observed values."""
    cur = Cursor("created_at", CursorType.DATE)
    assert cur.observed_max is None
    cur.observe("2025-01-05")
    cur.observe("2025-01-03")
    cur.observe("2025-01-09")
    cur.observe("2025-01-07")
    assert cur.observed_max == "2025-01-09"


def test_cursor_observe_ignores_none() -> None:
    """A None cursor value is ignored — it must not drag the cursor back."""
    cur = Cursor("created_at", CursorType.INT)
    cur.observe(5)
    cur.observe(None)
    cur.observe(3)
    assert cur.observed_max == 5


def test_cursor_observe_integer_max() -> None:
    """observe works for integer cursors too."""
    cur = Cursor("id", CursorType.INT)
    for v in (10, 5, 200, 199):
        cur.observe(v)
    assert cur.observed_max == 200


# ---------------------------------------------------------------------------
# StateRecord — round-trips
# ---------------------------------------------------------------------------


def test_state_record_round_trip() -> None:
    """StateRecord survives a to_row / from_row round-trip."""
    now = datetime(2026, 5, 21, 3, 14, 7, tzinfo=UTC)
    rec = StateRecord(
        connector="shiphero",
        stream="shipments",
        cursor_value="2026-05-20T00:00:00Z",
        cursor_type=CursorType.TIMESTAMP,
        state_blob={"page_token": "xyz"},
        last_run_id="run-2026-05-21-abc",
        rows_total=7310,
        updated_at=now,
    )
    row = rec.to_row()
    assert row["cursor_type"] == "timestamp"
    assert row["last_run_id"] == "run-2026-05-21-abc"
    assert row["updated_at"] == now.isoformat()
    restored = StateRecord.from_row(row)
    assert restored == rec


def test_state_record_round_trip_with_nulls() -> None:
    """A full-refresh state row (no cursor) round-trips with Nones intact."""
    rec = StateRecord(connector="c", stream="s")
    row = rec.to_row()
    assert row["cursor_type"] is None
    assert row["last_run_id"] is None
    assert row["updated_at"] is None
    restored = StateRecord.from_row(row)
    assert restored == rec
    assert restored.rows_total == 0


def test_state_record_from_row_parses_cursor_type_string() -> None:
    """from_row parses a cursor_type string back into the enum."""
    rec = StateRecord.from_row(
        {"connector": "c", "stream": "s", "cursor_type": "int", "rows_total": 5}
    )
    assert rec.cursor_type is CursorType.INT
    assert rec.rows_total == 5


def test_state_record_is_mutable() -> None:
    """StateRecord is mutable — the engine advances counters in place."""
    rec = StateRecord(connector="c", stream="s")
    rec.rows_total += 100
    assert rec.rows_total == 100


# ---------------------------------------------------------------------------
# RunConfig / RunResult / StreamResult
# ---------------------------------------------------------------------------


def test_run_config_is_frozen_and_selects() -> None:
    """RunConfig is immutable; selects() honors the --select subset."""
    rc = RunConfig(
        run_id="a1b9f3",
        pipeline="stripe_prod",
        connector="stripe",
        target="prod",
        config=Config(params={"page_size": 100}),
        select=("charges", "invoices"),
    )
    with pytest.raises(FrozenInstanceError):
        rc.run_id = "other"  # type: ignore[misc]
    assert not rc.is_select_all
    assert rc.selects("charges")
    assert not rc.selects("customers")


def test_run_config_select_all_when_empty() -> None:
    """An empty select means every stream is in scope."""
    rc = RunConfig(
        run_id="r", pipeline="p", connector="c", target="dev", config=Config()
    )
    assert rc.is_select_all
    assert rc.selects("anything")


def test_stream_result_to_dict() -> None:
    """StreamResult serializes to the run-record per-stream shape."""
    sr = StreamResult(
        name="charges",
        rows_extracted=7310,
        rows_loaded=7310,
        cursor_before="2026-05-20T00:00:00Z",
        cursor_after="2026-05-21T00:00:00Z",
    )
    d = sr.to_dict()
    assert d["status"] == "succeeded"
    assert d["rows_loaded"] == 7310
    assert d["cursor_after"] == "2026-05-21T00:00:00Z"


def test_run_result_duration_and_lookup() -> None:
    """RunResult computes duration and looks up a stream by name."""
    start = datetime(2026, 5, 21, 3, 13, 26, tzinfo=UTC)
    end = start + timedelta(seconds=41, milliseconds=200)
    rr = RunResult(
        run_id="a1b9f3",
        config="stripe_prod",
        connector="stripe",
        target="prod",
        destination="bigquery",
        status=RunStatus.SUCCEEDED,
        started_at=start,
        ended_at=end,
        streams=[StreamResult(name="charges", rows_loaded=7310)],
        rows_loaded=7310,
    )
    assert rr.duration_s == pytest.approx(41.2)
    assert rr.stream("charges") is not None
    assert rr.stream("missing") is None


def test_run_result_to_dict_succeeded() -> None:
    """A succeeded RunResult serializes with error None."""
    start = datetime(2026, 5, 21, tzinfo=UTC)
    rr = RunResult(
        run_id="r1",
        config="stripe_prod",
        connector="stripe",
        target="prod",
        destination="bigquery",
        status=RunStatus.SUCCEEDED,
        started_at=start,
        ended_at=start + timedelta(seconds=10),
        streams=[StreamResult(name="charges", rows_loaded=5)],
        rows_loaded=5,
    )
    d = rr.to_dict()
    assert d["status"] == "succeeded"
    assert d["error"] is None
    assert d["destination"] == "bigquery"
    assert d["streams"][0]["name"] == "charges"


def test_run_result_raise_for_status_succeeded_returns_self() -> None:
    """raise_for_status on a succeeded run returns the result unchanged."""
    start = datetime(2026, 5, 21, tzinfo=UTC)
    rr = RunResult(
        run_id="r1",
        config="stripe_prod",
        connector="c",
        target="t",
        destination="d",
        status=RunStatus.SUCCEEDED,
        started_at=start,
        ended_at=start,
    )
    assert rr.raise_for_status() is rr


def test_run_result_raise_for_status_failed_raises() -> None:
    """raise_for_status on a failed run re-raises the original exception."""
    start = datetime(2026, 5, 21, tzinfo=UTC)
    err = RuntimeError("extract failed")
    rr = RunResult(
        run_id="r1",
        config="stripe_prod",
        connector="c",
        target="t",
        destination="d",
        status=RunStatus.FAILED,
        started_at=start,
        ended_at=start,
        error=err,
    )
    with pytest.raises(RuntimeError, match="extract failed"):
        rr.raise_for_status()


def test_run_result_to_dict_failed_renders_error() -> None:
    """A failed RunResult renders error as '<ExcType>: <message>'."""
    start = datetime(2026, 5, 21, tzinfo=UTC)
    rr = RunResult(
        run_id="r1",
        config="stripe_prod",
        connector="c",
        target="t",
        destination="d",
        status=RunStatus.FAILED,
        started_at=start,
        ended_at=start,
        error=ValueError("bad config"),
    )
    assert rr.to_dict()["error"] == "ValueError: bad config"


def test_stream_status_has_skipped() -> None:
    """StreamStatus carries the 'skipped' terminal state (docs/07 §4.1)."""
    assert StreamStatus.parse("skipped") is StreamStatus.SKIPPED


# ---------------------------------------------------------------------------
# StateBackend protocol + type aliases
# ---------------------------------------------------------------------------


def test_state_backend_is_runtime_checkable() -> None:
    """A class implementing read_state/commit_state satisfies StateBackend."""

    class _Backend:
        def read_state(self, connector: str) -> list[StateRecord]:
            return []

        def commit_state(self, run_id: str, records: list[StateRecord]) -> None:
            pass

    assert isinstance(_Backend(), StateBackend)
    assert not isinstance(object(), StateBackend)


def test_batch_and_record_aliases() -> None:
    """Batch is list[dict] and Record is dict — the source/destination envelope."""
    rec: Record = {"id": "ship_1", "created_date": "2025-12-14T09:31:00Z"}
    batch: Batch = [rec, {"id": "ship_2"}]
    assert isinstance(batch, list)
    assert isinstance(batch[0], dict)
