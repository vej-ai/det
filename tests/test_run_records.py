"""End-to-end tests for the run-record + JSONL log layer — docs/09, stage 8a.

Two surfaces cooperate to give every run a queryable history:

* the per-run JSON-lines file at ``.detx/logs/<run_id>/run.jsonl`` (the
  *narrative*); and
* the destination-side ``_detx_runs`` audit table (the *receipt*) written via
  ``@destination.write_run_record`` when the destination declares
  ``Capability.RUN_RECORDS``.

This module exercises both, plus the CLI commands that read them back. The
secret-redaction tests are the load-bearing security check — neither sink
may ever leak a resolved secret value (docs/08, docs/09 §5).
"""

from __future__ import annotations

import json
import shutil
import textwrap
from pathlib import Path
from typing import Any

import duckdb
import pytest
from click.testing import CliRunner

import detx
from detx.cli import cli
from detx.engine.runner import EngineError

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Copy the fixture project into a temp dir so .detx/ writes are isolated."""
    dst = tmp_path / "project"
    shutil.copytree(FIXTURES_DIR, dst)
    return dst


@pytest.fixture
def warehouse(tmp_path: Path) -> str:
    """A fresh ``.duckdb`` file under the temp dir."""
    return str(tmp_path / "warehouse.duckdb")


def _query(db_path: str, sql: str) -> list[tuple[Any, ...]]:
    """Read-only query helper on a separate DuckDB connection."""
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Parse a JSON-lines file into a list of dicts."""
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


# ---------------------------------------------------------------------------
# Successful run: _detx_runs row + JSONL log
# ---------------------------------------------------------------------------


def test_successful_run_writes_detx_runs_row(project: Path, warehouse: str) -> None:
    """A successful run lands one fully-populated row in _detx_runs."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "succeeded"

    rows = _query(
        warehouse,
        "SELECT run_id, config, source, destination, target, status, "
        "rows_loaded, full_refresh, duration_s, error_type, error_message "
        "FROM _detx_runs",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == result.run_id
    assert row[1] == "echo_dev"
    assert row[2] == "echo"
    assert row[3] == "duckdb"
    assert row[4] == "dev"
    assert row[5] == "succeeded"
    assert row[6] == 9  # 4 events + 5 items
    assert row[7] is False
    assert row[8] >= 0.0
    assert row[9] is None
    assert row[10] is None


def test_successful_run_streams_json_carries_per_stream_detail(
    project: Path, warehouse: str
) -> None:
    """streams_json holds the same per-stream shape as StreamResult.to_dict."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "succeeded"

    raw = _query(warehouse, "SELECT streams_json FROM _detx_runs")[0][0]
    streams = json.loads(raw) if isinstance(raw, str) else raw
    by_name = {s["name"]: s for s in streams}
    assert set(by_name) == {"events", "items"}
    assert by_name["events"]["rows_loaded"] == 4
    assert by_name["items"]["rows_loaded"] == 5
    assert by_name["items"]["cursor_after"] == 5
    assert by_name["events"]["status"] == "succeeded"


def test_successful_run_writes_jsonl_log_with_expected_events(
    project: Path, warehouse: str
) -> None:
    """The JSONL log carries the docs/09 §2 event sequence in order."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    log_path = project / ".detx" / "logs" / result.run_id / "run.jsonl"
    assert log_path.exists()

    events = _read_jsonl(log_path)
    types = [e["event"] for e in events]
    assert types[0] == "run_start"
    assert types[-1] == "run_end"
    # Each stream emits stream_start → batch_loaded(+) → stream_committed.
    assert "stream_start" in types
    assert "batch_loaded" in types
    assert "stream_committed" in types
    # No failure on a clean run.
    assert "stream_failed" not in types

    # run_start carries the bound config/source/destination/target.
    rs = next(e for e in events if e["event"] == "run_start")
    assert rs["config"] == "echo_dev"
    assert rs["source"] == "echo"
    assert rs["destination"] == "duckdb"
    assert rs["target"] == "dev"
    assert rs["full_refresh"] is False
    # ISO-8601 UTC timestamps with offset (docs/09 §2).
    assert rs["ts"].endswith("+00:00")

    # run_end matches the RunResult summary.
    re_evt = next(e for e in events if e["event"] == "run_end")
    assert re_evt["status"] == "succeeded"
    assert re_evt["rows_loaded"] == 9


def test_two_consecutive_runs_produce_two_detx_runs_rows(
    project: Path, warehouse: str
) -> None:
    """Each run is one distinct PK row; idempotent upsert does not collapse them."""
    a = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    b = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert a.run_id != b.run_id
    rows = _query(warehouse, "SELECT count(*) FROM _detx_runs")
    assert rows[0][0] == 2


# ---------------------------------------------------------------------------
# Failed run: still gets a row + a JSONL log
# ---------------------------------------------------------------------------


def _write_breaking_stream_source(project: Path) -> None:
    """Replace the echo source with one whose ``items`` stream raises mid-run.

    Used to assert a failed run still produces both surfaces.
    """
    src_dir = project / "sources" / "broken"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: broken
            kind: source
            streams:
              - name: items
                table: broken_items
                schema:
                  - { name: id, type: INTEGER }
            """
        ).strip()
        + "\n"
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from detx import stream

            @stream(name="items")
            def items():
                yield [{"id": 1}]
                raise RuntimeError("boom mid-stream")
            """
        ).strip()
        + "\n"
    )
    (project / "configs" / "broken.yml").write_text(
        textwrap.dedent(
            """
            configs:
              - name: broken_dev
                source: broken
                destination: duckdb
                target: dev
            """
        ).strip()
        + "\n"
    )


def test_failed_run_writes_detx_runs_row_with_error(
    project: Path, warehouse: str
) -> None:
    """A failed run still lands one _detx_runs row, with FAILED + error_type."""
    _write_breaking_stream_source(project)
    result = detx.run(
        config="broken_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "failed"

    rows = _query(
        warehouse,
        "SELECT run_id, status, error_type, error_message FROM _detx_runs",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == result.run_id
    assert row[1] == "failed"
    assert row[2] == "RuntimeError"
    assert "boom" in row[3]


def test_failed_run_writes_jsonl_with_stream_failed_event(
    project: Path, warehouse: str
) -> None:
    """A failed run's JSONL ends with stream_failed → run_end(status=failed)."""
    _write_breaking_stream_source(project)
    result = detx.run(
        config="broken_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    log_path = project / ".detx" / "logs" / result.run_id / "run.jsonl"
    assert log_path.exists()
    events = _read_jsonl(log_path)
    types = [e["event"] for e in events]
    assert "stream_failed" in types
    sf = next(e for e in events if e["event"] == "stream_failed")
    assert sf["error_type"] == "RuntimeError"
    assert "boom" in sf["error_message"]
    # The traceback lands in the JSONL (NOT in _detx_runs — docs/09 §4 NOTE).
    assert "Traceback" in sf["traceback"]

    re_evt = next(e for e in events if e["event"] == "run_end")
    assert re_evt["status"] == "failed"


# ---------------------------------------------------------------------------
# Destination without Capability.RUN_RECORDS still produces the JSONL
# ---------------------------------------------------------------------------


def _install_minimal_destination(project: Path) -> None:
    """Drop a tiny destination that declares STATE only (no RUN_RECORDS) in-project."""
    dest_dir = project / "destinations" / "tiny"
    dest_dir.mkdir(parents=True, exist_ok=True)
    (dest_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: tiny
            kind: destination
            """
        ).strip()
        + "\n"
    )
    (dest_dir / "destination.py").write_text(
        textwrap.dedent(
            """
            from detx import destination, Capability

            _STORE = {"state": [], "tables": {}}

            @destination.capabilities
            def capabilities():
                return {Capability.STATE}

            @destination.open
            def open(config):
                return {"store": _STORE}

            @destination.close
            def close(conn):
                pass

            @destination.ensure_schema
            def ensure_schema(conn, stream):
                conn["store"]["tables"].setdefault(stream.table, [])

            @destination.write_batch
            def write_batch(conn, batch, stream):
                conn["store"]["tables"][stream.table].extend(batch)
                return len(batch)

            @destination.read_state
            def read_state(conn, connector):
                return [r for r in conn["store"]["state"] if r.connector == connector]

            @destination.commit_state
            def commit_state(conn, run_id, records):
                conn["store"]["state"].extend(records)
            """
        ).strip()
        + "\n"
    )


def test_destination_without_run_records_capability_still_writes_jsonl(
    project: Path,
) -> None:
    """A run against a destination missing RUN_RECORDS still produces a JSONL log."""
    _install_minimal_destination(project)
    # Bind echo source to the new tiny destination.
    (project / "configs" / "echo_tiny.yml").write_text(
        textwrap.dedent(
            """
            configs:
              - name: echo_tiny
                source: echo
                destination: tiny
            """
        ).strip()
        + "\n"
    )
    # Reference the tiny dest from profiles.yml so resolve_target_name does
    # not invent a "default" target.
    profiles_path = project / "profiles.yml"
    profiles_text = profiles_path.read_text()
    profiles_path.write_text(
        profiles_text
        + textwrap.dedent(
            """

            tiny:
              default_target: dev
              targets:
                dev: {}
            """
        )
    )

    result = detx.run(config="echo_tiny", project_dir=str(project))
    assert result.status.value == "succeeded"

    log_path = project / ".detx" / "logs" / result.run_id / "run.jsonl"
    assert log_path.exists()
    events = _read_jsonl(log_path)
    types = [e["event"] for e in events]
    assert types[0] == "run_start"
    assert types[-1] == "run_end"


# ---------------------------------------------------------------------------
# A destination that DECLARES Capability.RUN_RECORDS but lacks the hook
# ---------------------------------------------------------------------------


def _install_broken_run_records_destination(project: Path) -> None:
    """A destination that declares RUN_RECORDS without implementing the hook.

    The engine must raise EngineError at hook resolution — the conditional
    rule must fire (docs/09 §4 / docs/05 §1 transactional precedent).
    """
    dest_dir = project / "destinations" / "claims_rr"
    dest_dir.mkdir(parents=True, exist_ok=True)
    (dest_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: claims_rr
            kind: destination
            """
        ).strip()
        + "\n"
    )
    (dest_dir / "destination.py").write_text(
        textwrap.dedent(
            """
            from detx import destination, Capability

            @destination.capabilities
            def capabilities():
                # Declares the capability but does NOT implement write_run_record.
                return {Capability.STATE, Capability.RUN_RECORDS}

            @destination.open
            def open(config):
                return {}

            @destination.close
            def close(conn):
                pass

            @destination.ensure_schema
            def ensure_schema(conn, stream):
                pass

            @destination.write_batch
            def write_batch(conn, batch, stream):
                return len(batch)

            @destination.read_state
            def read_state(conn, connector):
                return []

            @destination.commit_state
            def commit_state(conn, run_id, records):
                pass
            """
        ).strip()
        + "\n"
    )


def test_declaring_run_records_without_hook_fails_engine_error(project: Path) -> None:
    """RUN_RECORDS declared without the hook ⇒ EngineError surfaces in RunResult."""
    _install_broken_run_records_destination(project)
    (project / "configs" / "echo_claims.yml").write_text(
        textwrap.dedent(
            """
            configs:
              - name: echo_claims
                source: echo
                destination: claims_rr
            """
        ).strip()
        + "\n"
    )
    profiles_path = project / "profiles.yml"
    profiles_path.write_text(
        profiles_path.read_text()
        + textwrap.dedent(
            """

            claims_rr:
              default_target: dev
              targets:
                dev: {}
            """
        )
    )

    result = detx.run(config="echo_claims", project_dir=str(project))
    assert result.status.value == "failed"
    assert isinstance(result.error, EngineError)
    assert "Capability.RUN_RECORDS" in str(result.error)
    assert "write_run_record" in str(result.error)


# ---------------------------------------------------------------------------
# Secret redaction (docs/08, docs/09 §5)
# ---------------------------------------------------------------------------


def _install_secret_source(project: Path, secret_value: str) -> None:
    """Install an echo-style source that declares a secret and logs it.

    The connector body deliberately logs the resolved secret to BOTH the
    stdlib logger and through a chain that ends up serialized to the JSONL.
    Redaction must mask the value in both sinks (docs/08 / docs/09 §5).
    """
    src_dir = project / "sources" / "echo_secret"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: echo_secret
            kind: source
            secrets:
              - { name: api_token, ref: "${env.DET_TEST_SECRET}" }
            streams:
              - name: items
                table: echo_secret_items
                schema:
                  - { name: id, type: INTEGER }
            """
        ).strip()
        + "\n"
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from detx import stream

            @stream(name="items")
            def items(config, log):
                token = config.secrets["api_token"]
                # Defence in depth — connector author's "accidental" log call.
                log.info("calling API with token=%s", token)
                yield [{"id": 1}]
            """
        ).strip()
        + "\n"
    )
    (project / "configs" / "echo_secret.yml").write_text(
        textwrap.dedent(
            """
            configs:
              - name: echo_secret_dev
                source: echo_secret
                destination: duckdb
                target: dev
            """
        ).strip()
        + "\n"
    )


def test_secret_redacted_in_jsonl_and_stdlib_logger(
    project: Path,
    warehouse: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A resolved secret value appears nowhere — not stdlib log, not JSONL."""
    secret = "wholly-implausible-credential-9f4c8a"
    monkeypatch.setenv("DET_TEST_SECRET", secret)
    _install_secret_source(project, secret)

    result = detx.run(
        config="echo_secret_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "succeeded"

    captured = capsys.readouterr()
    assert secret not in captured.err
    assert secret not in captured.out

    log_path = project / ".detx" / "logs" / result.run_id / "run.jsonl"
    body = log_path.read_text()
    assert secret not in body
    assert "***" in body  # the masked form did make it into the log


# ---------------------------------------------------------------------------
# CLI surface: detx runs list / detx runs show
# ---------------------------------------------------------------------------


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def test_cli_runs_list_shows_run_rows(
    runner: CliRunner, project: Path, warehouse: str
) -> None:
    """detx runs list -p echo_dev shows the run rows from _detx_runs."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "succeeded"

    out = runner.invoke(
        cli,
        [
            "runs",
            "list",
            "-p",
            "echo_dev",
            "--project-dir",
            str(project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert out.exit_code == 0, out.output
    # Short id is the trailing 12 hex of run_id.
    short = result.run_id[len("run-"):]
    assert short in out.output
    assert "echo_dev" in out.output
    assert "succeeded" in out.output


def test_cli_runs_list_empty_message(
    runner: CliRunner, project: Path, warehouse: str
) -> None:
    """detx runs list against an empty destination prints a 'no records' message."""
    # Open the destination once to create an empty database, but never run.
    # _detx_runs does not yet exist; the query helper treats that as 'no rows'.
    duckdb.connect(warehouse).close()
    out = runner.invoke(
        cli,
        [
            "runs",
            "list",
            "-p",
            "echo_dev",
            "--project-dir",
            str(project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert out.exit_code == 0, out.output
    assert "no run records" in out.output


def test_cli_runs_show_prints_record_and_jsonl(
    runner: CliRunner, project: Path, warehouse: str
) -> None:
    """detx runs show <run_id> prints the audit row + every JSONL event."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.status.value == "succeeded"

    out = runner.invoke(
        cli,
        [
            "runs",
            "show",
            result.run_id,
            "-p",
            "echo_dev",
            "--project-dir",
            str(project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert out.exit_code == 0, out.output
    # The record header.
    assert result.run_id in out.output
    assert "echo_dev" in out.output
    assert "succeeded" in out.output
    # The JSONL section appears with each engine event.
    assert "log:" in out.output
    assert "run_start" in out.output
    assert "stream_start" in out.output
    assert "run_end" in out.output


def test_successful_run_populates_log_path_on_runresult(
    project: Path, warehouse: str
) -> None:
    """RunResult.log_path points at the JSONL file — docs/09 §6 orchestrator hook."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    assert result.log_path
    path = Path(result.log_path)
    assert path.exists()
    assert path.name == "run.jsonl"
    assert result.run_id in path.parts


# ---------------------------------------------------------------------------
# write_run_record failure must not mask the run's real outcome
# ---------------------------------------------------------------------------


def _install_breaking_run_records_destination(project: Path) -> None:
    """A destination whose write_run_record always raises.

    Task brief: 'A failure to write the run record should be logged (not
    raise) — losing the audit row must not mask the run's real outcome.'
    """
    dest_dir = project / "destinations" / "breaks_rr"
    dest_dir.mkdir(parents=True, exist_ok=True)
    (dest_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: breaks_rr
            kind: destination
            """
        ).strip()
        + "\n"
    )
    (dest_dir / "destination.py").write_text(
        textwrap.dedent(
            """
            from detx import destination, Capability

            _STORE = {"state": [], "tables": {}}

            @destination.capabilities
            def capabilities():
                return {Capability.STATE, Capability.RUN_RECORDS}

            @destination.open
            def open(config):
                return {"store": _STORE}

            @destination.close
            def close(conn):
                pass

            @destination.ensure_schema
            def ensure_schema(conn, stream):
                conn["store"]["tables"].setdefault(stream.table, [])

            @destination.write_batch
            def write_batch(conn, batch, stream):
                conn["store"]["tables"][stream.table].extend(batch)
                return len(batch)

            @destination.read_state
            def read_state(conn, connector):
                return [r for r in conn["store"]["state"] if r.connector == connector]

            @destination.commit_state
            def commit_state(conn, run_id, records):
                conn["store"]["state"].extend(records)

            @destination.write_run_record
            def write_run_record(conn, record):
                raise RuntimeError("simulated audit-store outage")
            """
        ).strip()
        + "\n"
    )


def test_write_run_record_failure_is_logged_not_raised(project: Path) -> None:
    """A raising write_run_record hook must NOT change the run's status."""
    _install_breaking_run_records_destination(project)
    (project / "configs" / "echo_breaks.yml").write_text(
        textwrap.dedent(
            """
            configs:
              - name: echo_breaks
                source: echo
                destination: breaks_rr
            """
        ).strip()
        + "\n"
    )
    profiles_path = project / "profiles.yml"
    profiles_path.write_text(
        profiles_path.read_text()
        + textwrap.dedent(
            """

            breaks_rr:
              default_target: dev
              targets:
                dev: {}
            """
        )
    )

    result = detx.run(config="echo_breaks", project_dir=str(project))
    # The streams loaded successfully; a downstream audit-write failure
    # must not flip the run's real outcome.
    assert result.status.value == "succeeded"
    assert result.error is None
    # And the JSONL run_end still landed, with the real status.
    log_path = project / ".detx" / "logs" / result.run_id / "run.jsonl"
    assert log_path.exists()
    events = _read_jsonl(log_path)
    re_evt = next(e for e in events if e["event"] == "run_end")
    assert re_evt["status"] == "succeeded"


def test_cli_runs_show_accepts_short_id(
    runner: CliRunner, project: Path, warehouse: str
) -> None:
    """detx runs show accepts the trimmed (no ``run-`` prefix) id form."""
    result = detx.run(
        config="echo_dev",
        project_dir=str(project),
        destination_params_override={"path": warehouse},
    )
    short = result.run_id[len("run-"):]
    out = runner.invoke(
        cli,
        [
            "runs",
            "show",
            short,
            "-p",
            "echo_dev",
            "--project-dir",
            str(project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert out.exit_code == 0, out.output
    assert result.run_id in out.output
