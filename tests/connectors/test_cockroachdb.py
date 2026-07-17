"""Tests for the pre-baked CockroachDB source connector.

Unit tests (always run, no CockroachDB needed):
    * type_mapping coverage — the Postgres set plus the CockroachDB shapes
      (``ARRAY``, ``USER-DEFINED``, ``inet``, ``interval``) — + unknown-type error
    * identifier quoting safety (the ``"; DROP TABLE x"`` test)
    * AS OF SYSTEM TIME composition — follower-read function form verbatim,
      everything else literal-quoted (injection-safe)
    * pk-keyset / keyset / query / full-scan SQL-construction shape + params
    * the bootstrap path: page-cap pause + resume from state, cursor-max
      hand-off across runs, full-refresh reset — via a stub connection
      injected with ``monkeypatch``.

Integration tests (gated by ``COCKROACHDB_TEST_URL``):
    * end-to-end ``dtex.run`` into a tmp DuckDB, asserting the bootstrap
      lands all rows on run 1 and the cursor keyset finds 0 new rows on
      run 2. Skipped when the env var is unset.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from dtex import (
    Config,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    State,
)
from dtex.sources.cockroachdb import client, extract, type_mapping

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CRDB_CONNECTOR_DIR = REPO_ROOT / "dtex" / "sources" / "cockroachdb"


def _config(**overrides: Any) -> Config:
    """A resolved CockroachDB Config with test-friendly defaults."""
    params: dict[str, Any] = {
        "host": "h", "port": 26257, "database": "x", "user": "u",
        "sslmode": "verify-full", "sslrootcert": "system", "options": "",
        "as_of_system_time": "", "application_name": "dtex",
        "connect_timeout_seconds": 30, "batch_size": 2,
        "bootstrap_max_pages": 0,
    }
    params.update(overrides)
    return Config(params=params, secrets={"password": "redacted"})


# ===========================================================================
# type_mapping — the CockroachDB additions + spot checks of the shared set.
# ===========================================================================


@pytest.mark.parametrize(
    "crdb_type,expected",
    [
        # spot-checks of the shared Postgres-shaped set
        ("text", FieldType.STRING),
        ("varchar", FieldType.STRING),
        ("character varying", FieldType.STRING),
        ("bigint", FieldType.INTEGER),
        ("int8", FieldType.INTEGER),
        ("numeric", FieldType.FLOAT),
        ("boolean", FieldType.BOOLEAN),
        ("timestamp with time zone", FieldType.TIMESTAMP),
        ("timestamptz", FieldType.TIMESTAMP),
        ("date", FieldType.DATE),
        ("jsonb", FieldType.JSON),
        ("bytea", FieldType.BYTES),
        ("uuid", FieldType.STRING),
        # the CockroachDB-specific shapes
        ("ARRAY", FieldType.JSON),
        ("array", FieldType.JSON),
        ("USER-DEFINED", FieldType.STRING),
        ("user-defined", FieldType.STRING),
        ("inet", FieldType.STRING),
        ("interval", FieldType.STRING),
        ("time", FieldType.STRING),
        ("time without time zone", FieldType.STRING),
        ("oid", FieldType.INTEGER),
    ],
)
def test_cockroachdb_to_field_type_known(crdb_type: str, expected: FieldType) -> None:
    assert type_mapping.cockroachdb_to_field_type(crdb_type) is expected


def test_cockroachdb_to_field_type_unknown_type_raises_clear_error() -> None:
    """Unknown types must raise — never a silent STRING fallback."""
    with pytest.raises(ValueError, match=r"unknown CockroachDB type 'geometry'"):
        type_mapping.cockroachdb_to_field_type("geometry")


def test_cockroachdb_to_field_type_non_string_raises() -> None:
    with pytest.raises(ValueError, match="expects a string"):
        type_mapping.cockroachdb_to_field_type(42)  # type: ignore[arg-type]


def test_introspect_schema_maps_crdb_shapes() -> None:
    """``introspect_schema`` handles a REGIONAL BY ROW table's real column shapes."""
    fake_cursor = MagicMock()
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_cursor.fetchall.return_value = [
        ("crdb_region", "USER-DEFINED", "NO"),
        ("id", "character varying", "NO"),
        ("media_urls", "ARRAY", "NO"),
        ("config", "jsonb", "YES"),
        ("time_updated", "timestamp with time zone", "YES"),
    ]
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    schema = type_mapping.introspect_schema(fake_conn, "public", "social_media_post")

    assert schema.fields == (
        Field(name="crdb_region", type=FieldType.STRING, mode=FieldMode.REQUIRED),
        Field(name="id", type=FieldType.STRING, mode=FieldMode.REQUIRED),
        Field(name="media_urls", type=FieldType.JSON, mode=FieldMode.REQUIRED),
        Field(name="config", type=FieldType.JSON, mode=FieldMode.NULLABLE),
        Field(name="time_updated", type=FieldType.TIMESTAMP, mode=FieldMode.NULLABLE),
    )
    sql_executed, params = fake_cursor.execute.call_args.args
    assert "WHERE table_schema = %s AND table_name = %s" in sql_executed
    assert params == ("public", "social_media_post")


def test_introspect_schema_empty_table_raises() -> None:
    fake_cursor = MagicMock()
    fake_cursor.__enter__ = MagicMock(return_value=fake_cursor)
    fake_cursor.__exit__ = MagicMock(return_value=False)
    fake_cursor.fetchall.return_value = []
    fake_conn = MagicMock()
    fake_conn.cursor.return_value = fake_cursor

    with pytest.raises(ValueError, match="no columns found"):
        type_mapping.introspect_schema(fake_conn, "public", "ghost_table")


# ===========================================================================
# connect() — sslrootcert special values
# ===========================================================================


def test_connect_resolves_certifi_sslrootcert(monkeypatch: pytest.MonkeyPatch) -> None:
    """``sslrootcert: certifi`` resolves to the certifi CA bundle path."""
    captured: dict[str, Any] = {}

    def fake_connect(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "fake-conn"

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    client.connect(_config(sslrootcert="certifi"))
    assert captured["sslrootcert"].endswith("cacert.pem")


def test_connect_omits_empty_sslrootcert_and_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_connect(**kwargs: Any) -> str:
        captured.update(kwargs)
        return "fake-conn"

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    client.connect(_config(sslrootcert="", options=""))
    assert "sslrootcert" not in captured
    assert "options" not in captured
    assert captured["autocommit"] is True


# ===========================================================================
# Identifier quoting — the "; DROP TABLE x" guard
# ===========================================================================


def test_quote_identifier_neutralises_classic_injection() -> None:
    name = '"; DROP TABLE x; --'
    rendered = client.quote_identifier(name).as_string(None)
    assert rendered.startswith('"') and rendered.endswith('"')
    assert '""' in rendered
    assert rendered.count('"') >= 3


def test_qualified_table_is_dot_joined_and_quoted() -> None:
    assert client.qualified_table("public", "users").as_string(None) == '"public"."users"'


# ===========================================================================
# AS OF SYSTEM TIME composition
# ===========================================================================


def _normalise_sql(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def test_aost_clause_empty_is_disabled() -> None:
    assert client.aost_clause("") is None
    assert client.aost_clause("   ") is None
    assert client.set_transaction_aost_sql("") is None


def test_aost_clause_follower_read_function_form_is_verbatim() -> None:
    clause = client.aost_clause("follower_read_timestamp()")
    assert clause is not None
    assert clause.as_string(None) == "AS OF SYSTEM TIME follower_read_timestamp()"


def test_aost_clause_interval_form_is_literal_quoted() -> None:
    clause = client.aost_clause("-10s")
    assert clause is not None
    assert clause.as_string(None) == "AS OF SYSTEM TIME '-10s'"


def test_aost_clause_injection_attempt_is_literal_quoted() -> None:
    """A hostile config value is quoted into a (bogus) literal, never spliced as SQL."""
    clause = client.aost_clause("now(); DROP TABLE x; --")
    assert clause is not None
    rendered = clause.as_string(None)
    assert rendered == "AS OF SYSTEM TIME 'now(); DROP TABLE x; --'"


def test_set_transaction_aost_forms() -> None:
    fn = client.set_transaction_aost_sql("follower_read_timestamp()")
    assert fn is not None
    assert fn.as_string(None) == (
        "SET TRANSACTION AS OF SYSTEM TIME follower_read_timestamp()"
    )
    lit = client.set_transaction_aost_sql("-5s")
    assert lit is not None
    assert lit.as_string(None) == "SET TRANSACTION AS OF SYSTEM TIME '-5s'"


# ===========================================================================
# SQL-construction helpers — pk-keyset / keyset / query / full-scan
# ===========================================================================


def test_pk_keyset_select_sql_first_page_has_no_where() -> None:
    composed = client.pk_keyset_select_sql(
        "public", "social_media_post", ("id",), first_page=True
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT "id", * FROM "public"."social_media_post" ORDER BY "id" LIMIT %s'
    )


def test_pk_keyset_select_sql_resume_page_uses_row_value_comparison() -> None:
    composed = client.pk_keyset_select_sql(
        "public", "message", ("crdb_region", "id"), first_page=False
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT "crdb_region", "id", * FROM "public"."message" '
        'WHERE ("crdb_region", "id") > (%s, %s) '
        'ORDER BY "crdb_region", "id" LIMIT %s'
    )


def test_pk_keyset_select_sql_with_aost() -> None:
    composed = client.pk_keyset_select_sql(
        "public", "users", ("id",), first_page=True,
        as_of_system_time="follower_read_timestamp()",
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT "id", * FROM "public"."users" '
        "AS OF SYSTEM TIME follower_read_timestamp() "
        'ORDER BY "id" LIMIT %s'
    )


def test_pk_keyset_select_sql_rejects_empty_pk() -> None:
    with pytest.raises(ValueError, match="primary_key must be non-empty"):
        client.pk_keyset_select_sql("public", "t", (), first_page=True)


def test_keyset_select_sql_shape_includes_cursor_pk_limit() -> None:
    composed = client.keyset_select_sql(
        schema_name="public",
        table_name="users",
        cursor_field="time_updated",
        primary_key=("id",),
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT "id", * FROM "public"."users" '
        'WHERE "time_updated" > %s '
        'ORDER BY "time_updated", "id" '
        'LIMIT %s'
    )


def test_keyset_select_sql_with_aost_places_clause_before_where() -> None:
    composed = client.keyset_select_sql(
        schema_name="public",
        table_name="users",
        cursor_field="time_updated",
        primary_key=("id",),
        as_of_system_time="-10s",
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text == (
        'SELECT "id", * FROM "public"."users" AS OF SYSTEM TIME \'-10s\' '
        'WHERE "time_updated" > %s '
        'ORDER BY "time_updated", "id" '
        'LIMIT %s'
    )


def test_query_select_sql_wraps_user_query_as_subquery() -> None:
    composed = client.query_select_sql(
        user_query="SELECT id, occurred_at FROM events WHERE kind = 'click'",
        cursor_field="occurred_at",
    )
    sql_text = _normalise_sql(composed.as_string(None))
    assert sql_text.startswith(
        'SELECT * FROM (SELECT id, occurred_at FROM events WHERE kind = \'click\') '
        'AS _dtex_sub WHERE "occurred_at" > %s ORDER BY "occurred_at" LIMIT %s'
    )


def test_full_scan_select_sql_is_unconditional_select() -> None:
    composed = client.full_scan_select_sql("public", "products")
    assert _normalise_sql(composed.as_string(None)) == 'SELECT * FROM "public"."products"'


def test_fetch_forward_sql_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="positive int"):
        client.fetch_forward_sql("det_x", 0)


# ===========================================================================
# State-scalar round-tripping
# ===========================================================================


def test_to_state_scalar_roundtrips_timestamps() -> None:
    ts = datetime(2026, 7, 17, 12, 0, 0, tzinfo=UTC)
    stored = extract._to_state_scalar(ts)
    assert stored == "2026-07-17T12:00:00+00:00"
    assert extract._cursor_from_state(stored, CursorType.TIMESTAMP) == ts


def test_to_state_scalar_roundtrips_dates_and_ints() -> None:
    d = date(2026, 7, 17)
    assert extract._cursor_from_state(extract._to_state_scalar(d), CursorType.DATE) == d
    assert extract._cursor_from_state(42, CursorType.INT) == 42
    assert extract._cursor_from_state("42", CursorType.INT) == 42
    assert extract._cursor_from_state(None, CursorType.TIMESTAMP) is None


def test_to_state_scalar_passes_json_natives_and_stringifies_rest() -> None:
    assert extract._to_state_scalar("x") == "x"
    assert extract._to_state_scalar(7) == 7
    assert extract._to_state_scalar(None) is None
    from decimal import Decimal

    assert extract._to_state_scalar(Decimal("1.5")) == "1.5"


# ===========================================================================
# Fake-connection plumbing
# ===========================================================================


class _FakeColumn:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeCursor:
    """A psycopg-shaped cursor that yields pre-staged result batches."""

    def __init__(
        self,
        batches: list[list[tuple[Any, ...]]],
        column_names: list[str],
    ) -> None:
        self._batches = list(batches)
        self.description = [_FakeColumn(n) for n in column_names]
        self.executes: list[tuple[Any, ...]] = []

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: Any, params: Any = None) -> None:
        self.executes.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self._batches:
            return self._batches.pop(0)
        return []


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def silent_log() -> logging.Logger:
    log = logging.getLogger("test.cockroachdb")
    log.addHandler(logging.NullHandler())
    return log


# ===========================================================================
# Bootstrap path — pk sweep, page cap + resume, cursor hand-off, full refresh
# ===========================================================================


def test_bootstrap_sweeps_by_pk_and_hands_over_global_cursor_max(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """First sync sweeps in PK order; the cursor hand-off is the max seen anywhere.

    The staged pages put the *highest* time_updated in the FIRST page — pk
    order is uncorrelated with cursor order, so the hand-off must not be
    "last page's max".
    """
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 50), (2, "b", 20)],
        [(3, "c", 30)],  # short page — sweep complete
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn)

    state = State(None)
    cursor = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=0)

    batches = list(
        extract.extract_stream(
            stream_name="users",
            config=_config(), state=state, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )

    assert len(batches) == 2
    # Bootstrap complete: flag set, resume point cleared, global max handed over.
    assert state.get("bootstrapped") is True
    assert state.get("bootstrap_last_pk") is None
    assert cursor.observed_max == 50

    # First execute = first page (batch_size only); second binds the last PK.
    _, params0 = fake_cursor.executes[0]
    _, params1 = fake_cursor.executes[1]
    assert params0 == (2,)
    assert params1 == (2, 2)  # (last id, batch_size)
    sql0 = _normalise_sql(fake_cursor.executes[0][0].as_string(None))
    assert "WHERE" not in sql0
    sql1 = _normalise_sql(fake_cursor.executes[1][0].as_string(None))
    assert 'WHERE ("id") > (%s)' in sql1
    assert fake_conn.closed is True


def test_bootstrap_page_cap_pauses_and_resumes_from_state(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """A page-capped bootstrap stops without the flag, then resumes from the stored PK."""
    run1_pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 50), (2, "b", 20)],
        [(3, "c", 30), (4, "d", 40)],  # would be more, but the cap hits first
    ]
    fake_cursor1 = _FakeCursor(run1_pages, column_names=["id", "label", "updated_at"])
    fake_conn1 = _FakeConn(fake_cursor1)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn1)

    state = State(None)
    cursor1 = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=0)

    batches1 = list(
        extract.extract_stream(
            stream_name="users",
            config=_config(bootstrap_max_pages=2), state=state, cursor=cursor1,
            log=silent_log, schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    assert len(batches1) == 2
    assert not state.get("bootstrapped")
    assert state.get("bootstrap_last_pk") == [4]
    assert state.get("bootstrap_cursor_max") == 50

    # Run 2 — same state, fresh connection; picks up at pk > 4 and completes.
    run2_pages: list[list[tuple[Any, ...]]] = [
        [(5, "e", 10)],  # short page — sweep complete
    ]
    fake_cursor2 = _FakeCursor(run2_pages, column_names=["id", "label", "updated_at"])
    fake_conn2 = _FakeConn(fake_cursor2)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn2)
    cursor2 = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=0)

    batches2 = list(
        extract.extract_stream(
            stream_name="users",
            config=_config(bootstrap_max_pages=2), state=state, cursor=cursor2,
            log=silent_log, schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    assert len(batches2) == 1
    _, params0 = fake_cursor2.executes[0]
    assert params0 == (4, 2)  # resumed from the stored PK
    assert state.get("bootstrapped") is True
    # Global max came from run 1's stored state, not run 2's rows (max 10).
    assert cursor2.observed_max == 50


def test_bootstrap_full_refresh_resets_state_and_restarts_sweep(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 5)],  # short page — sweep completes immediately
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn)

    state = State(
        {"bootstrapped": True, "bootstrap_last_pk": [99], "bootstrap_cursor_max": 77}
    )
    cursor = Cursor(
        cursor_field="updated_at", cursor_type=CursorType.INT,
        start_value=None, is_full_refresh=True,
    )

    list(
        extract.extract_stream(
            stream_name="users",
            config=_config(), state=state, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    # The stale resume point was discarded — the sweep restarted from page 1.
    sql0 = _normalise_sql(fake_cursor.executes[0][0].as_string(None))
    assert "WHERE" not in sql0
    assert state.get("bootstrapped") is True
    assert cursor.observed_max == 5


def test_bootstrapped_stream_takes_cursor_keyset_path(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """Once bootstrapped, extraction is the plain cursor keyset (floor-bound)."""
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "a", 60)],  # short page
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn)

    state = State({"bootstrapped": True})
    cursor = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=50)

    batches = list(
        extract.extract_stream(
            stream_name="users",
            config=_config(), state=state, cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    assert batches == [[{"id": 1, "label": "a", "updated_at": 60}]]
    sql0 = _normalise_sql(fake_cursor.executes[0][0].as_string(None))
    assert 'WHERE "updated_at" > %s' in sql0
    _, params0 = fake_cursor.executes[0]
    assert params0 == (50, 2)
    assert cursor.observed_max == 60


def test_incremental_requires_state_and_pk(silent_log: logging.Logger) -> None:
    cursor = Cursor(cursor_field="u", cursor_type=CursorType.INT, start_value=0)
    with pytest.raises(ValueError, match="requires state"):
        list(
            extract.extract_stream(
                stream_name="bad",
                config=_config(), state=None, cursor=cursor, log=silent_log,
                table_name="t", cursor_field="u", primary_key=("id",),
            )
        )
    with pytest.raises(ValueError, match="non-empty primary_key"):
        list(
            extract.extract_stream(
                stream_name="bad",
                config=_config(), state=State(None), cursor=cursor, log=silent_log,
                table_name="t", cursor_field="u", primary_key=(),
            )
        )


def test_extract_stream_rejects_both_table_and_query(silent_log: logging.Logger) -> None:
    cursor = Cursor(cursor_field="x", cursor_type=CursorType.INT, start_value=0)
    with pytest.raises(ValueError, match="mutually exclusive"):
        list(
            extract.extract_stream(
                stream_name="bad",
                config=_config(), cursor=cursor, log=silent_log,
                table_name="t", query="SELECT 1",
            )
        )


def test_full_scan_pins_transaction_aost(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    """The no-cursor path pins the transaction with SET TRANSACTION AOST when configured."""
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "alpha")],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "name"])

    class _FakeTxn:
        def __enter__(self) -> _FakeTxn:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class _FakeConnWithTxn(_FakeConn):
        def transaction(self) -> _FakeTxn:
            return _FakeTxn()

    fake_conn = _FakeConnWithTxn(fake_cursor)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn)

    list(
        extract.extract_stream(
            stream_name="things",
            config=_config(as_of_system_time="follower_read_timestamp()"),
            cursor=None, log=silent_log,
            schema_name="public", table_name="things",
        )
    )
    rendered = [
        e[0].as_string(None) if hasattr(e[0], "as_string") else str(e[0])
        for e in fake_cursor.executes
    ]
    assert rendered[0].startswith("SET TRANSACTION AS OF SYSTEM TIME")
    assert rendered[1].startswith("DECLARE ")
    assert rendered[-1].startswith("CLOSE ")


def test_password_does_not_appear_in_rendered_sql(
    monkeypatch: pytest.MonkeyPatch, silent_log: logging.Logger
) -> None:
    pages: list[list[tuple[Any, ...]]] = [
        [(1, "alpha", 10)],
    ]
    fake_cursor = _FakeCursor(pages, column_names=["id", "label", "updated_at"])
    fake_conn = _FakeConn(fake_cursor)
    monkeypatch.setattr(extract, "connect", lambda config: fake_conn)

    secret = "S3cr3t_p@ssw0rd_DO_NOT_LEAK"
    config = Config(
        params={
            "host": "h", "port": 26257, "database": "x", "user": "u",
            "sslmode": "verify-full", "sslrootcert": "system", "options": "",
            "as_of_system_time": "", "application_name": "dtex",
            "connect_timeout_seconds": 30, "batch_size": 100,
            "bootstrap_max_pages": 0,
        },
        secrets={"password": secret},
    )
    cursor = Cursor(cursor_field="updated_at", cursor_type=CursorType.INT, start_value=0)

    list(
        extract.extract_stream(
            stream_name="users",
            config=config, state=State(None), cursor=cursor, log=silent_log,
            schema_name="public", table_name="users",
            cursor_field="updated_at", primary_key=("id",),
        )
    )
    rendered = " ".join(
        e[0].as_string(None) if hasattr(e[0], "as_string") else str(e[0])
        for e in fake_cursor.executes
    )
    assert secret not in rendered


# ===========================================================================
# Connector discovery — manifest parses + @stream decorators register
# ===========================================================================


def test_cockroachdb_connector_discovers_and_validates(tmp_path: Path) -> None:
    """The full discovery flow finds the source folder and validates it."""
    from dtex.engine.discovery import resolve_source

    (tmp_path / "dtex_project.yml").write_text(
        "name: test_project\nversion: '1.0.0'\nsource_paths: [sources]\n"
    )
    (tmp_path / "sources").mkdir()  # empty — forces fallback to baked

    loaded = resolve_source("cockroachdb", tmp_path)
    assert loaded.manifest.name == "cockroachdb"
    assert loaded.manifest.kind.value == "source"
    assert {s.name for s in loaded.manifest.streams} == {"users", "events"}
    assert {n for n in loaded.registry.stream_names} == {"users", "events"}
    # Every stream has the (config, state, cursor, log) inject list — state
    # carries the bootstrap progress.
    for name in ("users", "events"):
        reg = loaded.registry.stream(name)
        assert reg is not None
        assert set(reg.inject) == {"config", "state", "cursor", "log"}


# ===========================================================================
# Integration tests — gated by COCKROACHDB_TEST_URL
# ===========================================================================


pytestmark_integration = pytest.mark.skipif(
    not os.getenv("COCKROACHDB_TEST_URL"),
    reason="needs a live CockroachDB at COCKROACHDB_TEST_URL",
)


@pytest.fixture
def live_crdb_conn() -> Iterator[psycopg.Connection[Any]]:
    """A live CockroachDB connection from ``COCKROACHDB_TEST_URL``."""
    url = os.environ["COCKROACHDB_TEST_URL"]
    conn = psycopg.connect(url)
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.integration
@pytestmark_integration
def test_integration_bootstrap_then_resume(
    live_crdb_conn: psycopg.Connection[Any], tmp_path: Path
) -> None:
    """End-to-end: bootstrap lands 100 rows on run 1, keyset finds 0 on run 2.

    Needs a URL whose user can CREATE SCHEMA in the target database.
    """
    import dtex

    schema = f"det_crdb_it_{os.getpid()}"
    with live_crdb_conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        cur.execute(f'CREATE SCHEMA "{schema}"')
        cur.execute(
            f'CREATE TABLE "{schema}".users ('
            "id STRING PRIMARY KEY, "
            "email STRING, "
            "full_name STRING, "
            "created_at TIMESTAMPTZ, "
            "updated_at TIMESTAMPTZ NOT NULL)"
        )
        cur.executemany(
            f'INSERT INTO "{schema}".users VALUES (%s, %s, %s, now(), now())',
            [(f"u{i:04d}", f"u{i}@x.com", f"u{i}") for i in range(100)],
        )
    live_crdb_conn.commit()

    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / "dtex_project.yml").write_text(
        "name: it\nversion: '1.0.0'\nsource_paths: [sources]\n"
        "destination_paths: [destinations]\nconfig_paths: [configs]\n"
    )
    (project_dir / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n      path: "
        f"'{tmp_path}/wh.duckdb'\n"
    )
    (project_dir / "configs").mkdir()
    (project_dir / "configs" / "crdb_dev.yml").write_text(
        "name: crdb_dev\nsource: cockroachdb\ndestination: duckdb\ntarget: dev\n"
    )
    fork = project_dir / "sources" / "cockroachdb"
    fork.mkdir(parents=True)
    (fork / "register.yaml").write_text(
        "name: cockroachdb\nkind: source\nversion: '1.0.0'\n"
        "params:\n"
        "  host: {type: string, required: true}\n"
        "  port: {type: int, default: 26257}\n"
        "  database: {type: string, required: true}\n"
        "  user: {type: string, required: true}\n"
        "  sslmode: {type: string, default: prefer}\n"
        "  sslrootcert: {type: string, default: ''}\n"
        "  options: {type: string, default: ''}\n"
        "  as_of_system_time: {type: string, default: ''}\n"
        "  application_name: {type: string, default: dtex}\n"
        "  connect_timeout_seconds: {type: int, default: 30}\n"
        "  batch_size: {type: int, default: 30}\n"
        "  bootstrap_max_pages: {type: int, default: 0}\n"
        "secrets:\n  - {name: password, ref: '${env.CRDB_IT_PASSWORD}'}\n"
        "streams:\n"
        "  - name: users\n"
        "    table: crdb_users\n"
        "    primary_key: id\n"
        "    write_disposition: merge\n"
        "    incremental:\n"
        "      cursor_field: updated_at\n"
        "      cursor_type: timestamp\n"
        "      initial_value: '1970-01-01T00:00:00'\n"
    )
    (fork / "__init__.py").write_text("")
    (fork / "source.py").write_text(
        "from dtex import stream\n"
        "from dtex.sources.cockroachdb.extract import extract_stream\n\n"
        "@stream(name='users')\n"
        "def users(config, state, cursor, log):\n"
        "    yield from extract_stream(\n"
        "        stream_name='users', config=config, state=state, cursor=cursor,\n"
        f"        log=log, schema_name='{schema}', table_name='users',\n"
        "        cursor_field='updated_at', primary_key=('id',),\n"
        "    )\n"
    )

    info = live_crdb_conn.info
    overrides = {
        "host": info.host,
        "port": info.port,
        "database": info.dbname,
        "user": info.user,
    }
    os.environ.setdefault("CRDB_IT_PASSWORD", info.password or "")

    r1 = dtex.run(
        config="crdb_dev", project_dir=str(project_dir),
        params_override=overrides,
    )
    assert r1.status.value == "succeeded", r1.error
    assert (r1.stream("users").rows_loaded if r1.stream("users") else 0) == 100  # type: ignore[union-attr]

    r2 = dtex.run(
        config="crdb_dev", project_dir=str(project_dir),
        params_override=overrides,
    )
    assert r2.status.value == "succeeded", r2.error
    assert (r2.stream("users").rows_loaded if r2.stream("users") else 0) == 0  # type: ignore[union-attr]

    with live_crdb_conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    live_crdb_conn.commit()
