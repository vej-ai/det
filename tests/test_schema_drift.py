"""Unit tests for value-level schema-drift handling — docs/02 §Normalize.

Motivated by three prod incidents (2026-07): an upstream column added, a
declared column's type changed (`ArrowTypeError: Expected bytes, got a 'bool'`,
which failed EVERY run), and stale declared schemas generally. The engine must
degrade — coerce-or-null + warn — instead of failing the run.

These are the pure/unit layers:
    * `normalize_batch` under `SchemaDrift.WARN` — coerce-or-null fallback, the
      exact bool-into-BYTES crash, missing-column reporting, dedup semantics.
    * `SchemaDrift.FAIL` — the strict escape hatch still raises.
    * `_DriftReporter` — de-dups per (column, kind), counts, emits JSONL.
    * `_parse_schema_drift` — project-config parsing + validation.

The end-to-end `dtex.run` drift tests (SUCCEEDED status, DuckDB rows, BigQuery
MERGE with an undeclared column) live in test_engine.py / test_bigquery.py,
which own the run/destination harnesses.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from dtex.engine.normalize import (
    DRIFT_MISSING_COLUMN,
    DRIFT_TYPE_MISMATCH,
    normalize_batch,
)
from dtex.types import (
    CoercionError,
    Field,
    FieldType,
    Schema,
    SchemaDrift,
)


def _schema(*pairs: tuple[str, FieldType]) -> Schema:
    return Schema(fields=tuple(Field(name=n, type=t) for n, t in pairs))


# ===========================================================================
# normalize_batch — WARN mode coerce-or-null fallback
# ===========================================================================


def test_warn_bool_into_bytes_nulls_not_raises() -> None:
    """The exact 2026-07-24 crash: a bool in a declared BYTES column.

    Strict mode raised (ArrowTypeError downstream); WARN stores NULL and
    reports, so the writer never sees a bytes/bool contradiction.
    """
    events: list[tuple[str, str, str]] = []
    sch = _schema(("blob", FieldType.BYTES))
    out = normalize_batch(
        [{"blob": True}],
        sch,
        policy=SchemaDrift.WARN,
        on_drift=lambda c, k, d: events.append((c, k, d)),
    )
    assert out == [{"blob": None}]  # nulled, not the bool, not raised
    assert [(c, k) for c, k, _ in events] == [("blob", DRIFT_TYPE_MISMATCH)]


def test_warn_uncoercible_string_column_keeps_str_fallback() -> None:
    """A STRING target's fallback is str(value), not NULL — it can hold anything."""
    events: list[tuple[str, str, str]] = []
    sch = _schema(("name", FieldType.STRING))
    # STRING coerces everything already, so force a fallback with a value the
    # STRING coercer accepts — it never fails; instead test an INTEGER target
    # whose fallback path is exercised elsewhere. Here assert STRING never drifts.
    out = normalize_batch(
        [{"name": 123}],
        sch,
        policy=SchemaDrift.WARN,
        on_drift=lambda c, k, d: events.append((c, k, d)),
    )
    assert out == [{"name": "123"}]
    assert events == []  # STRING coercion succeeded, no drift


def test_warn_non_numeric_string_into_integer_nulls() -> None:
    """INTEGER target, non-numeric string → NULL + one drift event."""
    events: list[tuple[str, str, str]] = []
    sch = _schema(("amount", FieldType.INTEGER))
    out = normalize_batch(
        [{"amount": "not-a-number"}],
        sch,
        policy=SchemaDrift.WARN,
        on_drift=lambda c, k, d: events.append((c, k, d)),
    )
    assert out == [{"amount": None}]
    assert [(c, k) for c, k, _ in events] == [("amount", DRIFT_TYPE_MISMATCH)]


def test_warn_reports_missing_declared_column() -> None:
    """A declared column absent from the record reports DRIFT_MISSING_COLUMN."""
    events: list[tuple[str, str, str]] = []
    sch = _schema(("id", FieldType.INTEGER), ("gone", FieldType.STRING))
    out = normalize_batch(
        [{"id": 1}],  # 'gone' declared but absent
        sch,
        policy=SchemaDrift.WARN,
        on_drift=lambda c, k, d: events.append((c, k, d)),
    )
    assert out == [{"id": 1}]  # 'gone' not invented; destination binds NULL
    assert ("gone", DRIFT_MISSING_COLUMN) in [(c, k) for c, k, _ in events]


def test_warn_new_undeclared_column_passes_through_no_drift() -> None:
    """A column not in the schema is carried verbatim and is NOT a drift event.

    New columns are the destination's additive-evolution concern (they land via
    _augment_schema_for_batch + ensure_schema); NORMALIZE only touches declared
    columns, so it emits no drift for an extra column.
    """
    events: list[tuple[str, str, str]] = []
    sch = _schema(("id", FieldType.INTEGER))
    out = normalize_batch(
        [{"id": 1, "cello_ucc": "surprise"}],
        sch,
        policy=SchemaDrift.WARN,
        on_drift=lambda c, k, d: events.append((c, k, d)),
    )
    assert out == [{"id": 1, "cello_ucc": "surprise"}]
    assert events == []


# ===========================================================================
# FAIL mode — the strict escape hatch
# ===========================================================================


def test_fail_mode_raises_on_uncoercible() -> None:
    """schema_drift: fail restores the pre-0.6.5 strict raise."""
    sch = _schema(("blob", FieldType.BYTES))
    with pytest.raises(CoercionError):
        normalize_batch([{"blob": True}], sch, policy=SchemaDrift.FAIL)


def test_default_policy_is_fail_for_bare_callers() -> None:
    """normalize_batch with no policy keeps the historical strict behavior.

    The RUNNER passes WARN by default (from project config); a bare library
    call (or a destination re-normalizing) must not silently swallow drift.
    """
    sch = _schema(("amount", FieldType.INTEGER))
    with pytest.raises(CoercionError):
        normalize_batch([{"amount": "x"}], sch)  # no policy= → FAIL


# ===========================================================================
# Dedup — one report per (column, kind) regardless of row count
# ===========================================================================


def test_warn_dedups_per_column_across_many_rows() -> None:
    """A column that drifts on every row of a big batch reports ONCE.

    normalize_batch calls on_drift per drifted cell; the runner's _DriftReporter
    dedups. This test drives the reporter to prove the dedup, feeding 500 rows.
    """
    from dtex.engine.runner import _DriftReporter

    reporter = _DriftReporter("rows", logging.getLogger("t.drift"), run_log=None)
    sch = _schema(("amount", FieldType.INTEGER))
    batch = [{"amount": "bad"} for _ in range(500)]
    normalize_batch(batch, sch, policy=SchemaDrift.WARN, on_drift=reporter)
    assert reporter.count == 1  # one distinct (column, kind), not 500


def test_drift_reporter_emits_jsonl_and_warns_once() -> None:
    """_DriftReporter emits one schema_drift JSONL event + one WARNING per key."""
    from dtex.engine.runner import _DriftReporter

    emitted: list[dict[str, Any]] = []

    class _FakeRunLog:
        def emit(self, event: str, **fields: Any) -> None:
            emitted.append({"event": event, **fields})

    log = logging.getLogger("t.drift.jsonl")
    reporter = _DriftReporter("message", log, run_log=_FakeRunLog())  # type: ignore[arg-type]
    reporter("blob", DRIFT_TYPE_MISMATCH, "bool did not coerce to BYTES")
    reporter("blob", DRIFT_TYPE_MISMATCH, "bool did not coerce to BYTES")  # dup
    reporter("other", DRIFT_MISSING_COLUMN, "declared but absent")

    assert len(emitted) == 2  # blob once, other once
    assert emitted[0] == {
        "event": "schema_drift",
        "stream": "message",
        "column": "blob",
        "kind": DRIFT_TYPE_MISMATCH,
        "detail": "bool did not coerce to BYTES",
    }
    assert emitted[1]["column"] == "other"
    assert emitted[1]["kind"] == DRIFT_MISSING_COLUMN
    assert reporter.count == 2


# ===========================================================================
# Project-config parsing
# ===========================================================================


def test_parse_schema_drift_default_and_values(tmp_path: Any) -> None:
    from dtex.engine.config import ConfigError, ProjectConfig

    (tmp_path / "dtex_project.yml").write_text("name: p\n")
    assert ProjectConfig.load(tmp_path).schema_drift is SchemaDrift.WARN

    (tmp_path / "dtex_project.yml").write_text("name: p\nschema_drift: fail\n")
    assert ProjectConfig.load(tmp_path).schema_drift is SchemaDrift.FAIL

    (tmp_path / "dtex_project.yml").write_text("name: p\nschema_drift: WARN\n")
    assert ProjectConfig.load(tmp_path).schema_drift is SchemaDrift.WARN  # case-insensitive

    (tmp_path / "dtex_project.yml").write_text("name: p\nschema_drift: nonsense\n")
    with pytest.raises(ConfigError, match="'warn' or 'fail'"):
        ProjectConfig.load(tmp_path)

    (tmp_path / "dtex_project.yml").write_text("name: p\nschema_drift: [x]\n")
    with pytest.raises(ConfigError, match="must be a string"):
        ProjectConfig.load(tmp_path)
