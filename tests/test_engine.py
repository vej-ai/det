"""Engine tests — discovery, config resolution, the run lifecycle (stage 5).

The smoke test (``test_smoke.py``) is the end-to-end executable spec. This file
tests the engine's *parts* directly:

* discovery resolves project-local connectors over baked ones (docs/03 §5);
* config precedence layers merge in the documented order (docs/03 §6);
* ``--tag`` selection filters discovered connectors (docs/02);
* discovery-time validation rejects a malformed connector (docs/03 §7);
* ``--full-refresh`` re-extracts past a committed cursor (docs/03 §3.2);
* a failing stream does not lose an earlier stream's committed state
  (docs/02 §Commit granularity);
* secret resolution reads ``${env.X}`` / ``${profile.X.Y}`` (docs/03 §2.5).

The real ``echo`` fixture + DuckDB destination drive the lifecycle tests;
discovery/validation tests build throwaway projects in ``tmp_path``.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

import det
from det.engine import config as cfg
from det.engine import discovery as disc
from det.engine.config import ConfigError
from det.engine.discovery import DiscoveryError
from det.engine.logger import RedactingFilter, build_logger
from det.types import (
    Incremental,
    ParamSpec,
    ParamType,
    SecretRef,
    StreamDef,
    WriteDisposition,
)

# The committed test project — tests/fixtures/ holds det_project.yml,
# profiles.yml and connectors/echo/.
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ==========================================================================
# Helpers — throwaway projects in tmp_path
# ==========================================================================


def _write_project(
    root: Path,
    *,
    connector_paths: str = "[connectors]",
    default_destination: str = "duckdb",
    vars_block: str = "",
) -> None:
    """Write a minimal ``det_project.yml`` into ``root``."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "det_project.yml").write_text(
        textwrap.dedent(
            f"""\
            name: tmp_project
            version: "1.0.0"
            connector_paths: {connector_paths}
            default_destination: {default_destination}
            default_target: dev
            {vars_block}
            """
        )
    )


def _write_echo_clone(folder: Path, *, summary: str, tags: str = "[fixture]") -> None:
    """Write a tiny one-stream source connector folder at ``folder``.

    The ``summary`` differs per clone so a resolution test can prove *which*
    copy was picked.
    """
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            f"""\
            name: clone
            kind: source
            version: "1.0.0"
            summary: {summary}
            tags: {tags}
            streams:
              - name: rows
                table: clone_rows
                write_disposition: append
                schema:
                  - {{name: id, type: INTEGER}}
            destination:
              connector: duckdb
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}, {"id": 2}]
            """
        )
    )


# ==========================================================================
# Discovery — project root + connector resolution (docs/03 §5)
# ==========================================================================


def test_find_project_root_walks_up(tmp_path: Path) -> None:
    """find_project_root walks up from a nested dir to the marker file."""
    _write_project(tmp_path)
    nested = tmp_path / "a" / "b" / "c"
    nested.mkdir(parents=True)
    assert disc.find_project_root(nested) == tmp_path.resolve()


def test_find_project_root_missing_raises(tmp_path: Path) -> None:
    """A directory tree with no det_project.yml raises DiscoveryError."""
    with pytest.raises(DiscoveryError, match="no det_project.yml"):
        disc.find_project_root(tmp_path)


def test_resolve_connector_project_local(tmp_path: Path) -> None:
    """A project-local connector folder resolves and imports cleanly."""
    _write_project(tmp_path)
    _write_echo_clone(tmp_path / "connectors" / "clone", summary="local-copy")
    loaded = disc.resolve_connector("clone", tmp_path, ["connectors"])
    assert loaded.manifest.name == "clone"
    assert loaded.manifest.summary == "local-copy"
    assert "rows" in loaded.registry.stream_names


def test_resolve_connector_baked_destination(tmp_path: Path) -> None:
    """The baked DuckDB destination resolves from det/destinations/."""
    _write_project(tmp_path)
    loaded = disc.resolve_connector("duckdb", tmp_path, ["connectors"])
    assert loaded.manifest.name == "duckdb"
    assert loaded.manifest.kind.value == "destination"
    # It resolved from the baked package path, not the (empty) project.
    assert "det" in str(loaded.folder)
    assert "destinations" in str(loaded.folder)


def test_project_local_shadows_baked(tmp_path: Path) -> None:
    """A project-local connector named like a baked one wins (docs/03 §5).

    A project-local folder named ``duckdb`` is found before the baked DuckDB
    destination — proving project-local beats baked on a name collision.
    """
    _write_project(tmp_path)
    shadow = tmp_path / "connectors" / "duckdb"
    _write_echo_clone(shadow, summary="shadowing-copy")
    folder = disc.find_connector_folder("duckdb", tmp_path, ["connectors"])
    # The project-local folder, NOT the baked det/destinations/duckdb.
    assert folder == shadow.resolve()
    assert "det/destinations" not in str(folder)


def test_resolve_connector_unknown_name_raises(tmp_path: Path) -> None:
    """An unresolvable connector name raises DiscoveryError listing the search."""
    _write_project(tmp_path)
    with pytest.raises(DiscoveryError, match="not found"):
        disc.resolve_connector("does_not_exist", tmp_path, ["connectors"])


# ==========================================================================
# Tag selection (docs/02 §Tag-based selection)
# ==========================================================================


def test_connectors_with_tag_selects_matching(tmp_path: Path) -> None:
    """connectors_with_tag returns exactly the connectors declaring the tag."""
    _write_project(tmp_path)
    _write_echo_clone(
        tmp_path / "connectors" / "alpha", summary="a", tags="[hourly, fixture]"
    )
    # alpha's manifest name is hard-coded "clone"; give beta its own folder but
    # the same manifest name is fine — tag selection returns manifest NAMEs.
    beta = tmp_path / "connectors" / "beta"
    _write_echo_clone(beta, summary="b", tags="[daily]")
    (beta / "register.yaml").write_text(
        (beta / "register.yaml").read_text().replace("name: clone", "name: beta")
    )
    matched = disc.connectors_with_tag("hourly", tmp_path, ["connectors"])
    assert "clone" in matched
    assert "beta" not in matched


def test_connectors_with_tag_no_match(tmp_path: Path) -> None:
    """A tag no connector declares yields an empty selection."""
    _write_project(tmp_path)
    _write_echo_clone(tmp_path / "connectors" / "alpha", summary="a", tags="[fixture]")
    assert disc.connectors_with_tag("nonexistent", tmp_path, ["connectors"]) == []


# ==========================================================================
# Discovery-time validation (docs/03 §7)
# ==========================================================================


def test_validation_rejects_orphan_manifest_stream(tmp_path: Path) -> None:
    """A streams[] entry with no matching @stream fails validation (rule 7)."""
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "broken"
    _write_echo_clone(folder, summary="broken")
    # Declare a second stream in the manifest with no @stream implementing it.
    reg = folder / "register.yaml"
    reg.write_text(
        reg.read_text()
        + textwrap.dedent(
            """\
              - name: ghost
                table: ghost_rows
                schema:
                  - {name: id, type: INTEGER}
            """
        )
    )
    with pytest.raises(DiscoveryError, match="ghost"):
        disc.resolve_connector("broken", tmp_path, ["connectors"])


def test_validation_rejects_orphan_stream_function(tmp_path: Path) -> None:
    """A @stream with no matching streams[] entry fails validation (rule 7)."""
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "extra"
    _write_echo_clone(folder, summary="extra")
    # Add a @stream the manifest never declares.
    src = folder / "source.py"
    src.write_text(
        src.read_text()
        + textwrap.dedent(
            """\


            @stream(name="undeclared")
            def undeclared() -> Iterator[Batch]:
                yield [{"id": 9}]
            """
        )
    )
    with pytest.raises(DiscoveryError, match="undeclared"):
        disc.resolve_connector("extra", tmp_path, ["connectors"])


def test_validation_rejects_bad_stream_signature(tmp_path: Path) -> None:
    """A @stream declaring a non-injectable parameter fails discovery (rule 8)."""
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "badsig"
    _write_echo_clone(folder, summary="badsig")
    # `cusror` is a typo for `cursor` — not an injectable name.
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows(cusror) -> Iterator[Batch]:
                yield [{"id": 1}]
            """
        )
    )
    with pytest.raises(DiscoveryError, match="import cleanly|cusror"):
        disc.resolve_connector("badsig", tmp_path, ["connectors"])


def test_validation_rejects_unknown_manifest_key(tmp_path: Path) -> None:
    """An unknown register.yaml key is a hard error (docs/03 §7 step 2)."""
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "typo"
    _write_echo_clone(folder, summary="typo")
    reg = folder / "register.yaml"
    reg.write_text(reg.read_text() + "write_dispostion: append\n")
    with pytest.raises(DiscoveryError, match="unknown register.yaml key"):
        disc.resolve_connector("typo", tmp_path, ["connectors"])


# ==========================================================================
# Config resolution — the layered precedence of docs/03 §6
# ==========================================================================


def test_config_precedence_register_default_only() -> None:
    """With nothing overriding it, a param resolves to its register.yaml default."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={},
        target_block={},
        overrides={},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 50


def test_config_precedence_project_vars_over_default() -> None:
    """det_project.yml vars override the register.yaml default."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        target_block={},
        overrides={},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 100


def test_config_precedence_overrides_win() -> None:
    """run()/CLI overrides beat every lower layer; values are type-coerced."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        target_block={"page_size": 200},
        overrides={"page_size": "999"},  # string → coerced to int
        connector_name="cfg",
    )
    assert resolved["page_size"] == 999
    assert isinstance(resolved["page_size"], int)


def test_config_precedence_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIMPLE_E_PARAM_<NAME> sits above profiles, below run()/CLI overrides."""
    monkeypatch.setenv("SIMPLE_E_PARAM_PAGE_SIZE", "777")
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        target_block={"page_size": 200},
        overrides={},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 777


def test_config_required_param_missing_raises() -> None:
    """A required param with no value on any layer fails config resolution."""
    with pytest.raises(ConfigError, match="required param"):
        cfg.resolve_params(
            {"token": ParamSpec(type=ParamType.STRING, required=True)},
            project_vars={},
            target_block={},
            overrides={},
            connector_name="cfg",
        )


def test_config_bad_type_raises() -> None:
    """A param value that will not coerce to its declared type fails."""
    with pytest.raises(ConfigError, match="not a valid int"):
        cfg.resolve_params(
            {"page_size": ParamSpec(type=ParamType.INT, default="not-a-number")},
            project_vars={},
            target_block={},
            overrides={},
            connector_name="cfg",
        )


# ==========================================================================
# Secret resolution — ${env.X} / ${profile.X.Y} (docs/03 §2.5)
# ==========================================================================


def test_secret_resolves_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ${env.X} secret ref resolves to the environment variable's value."""
    monkeypatch.setenv("MY_API_TOKEN", "s3cr3t-value")
    ref = SecretRef(name="api_token", ref="${env.MY_API_TOKEN}")
    assert cfg.resolve_secret_ref(ref, target_block={}) == "s3cr3t-value"


def test_secret_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ${env.X} ref to an unset variable fails — without leaking a value."""
    monkeypatch.delenv("DEFINITELY_UNSET", raising=False)
    ref = SecretRef(name="api_token", ref="${env.DEFINITELY_UNSET}")
    with pytest.raises(ConfigError, match="DEFINITELY_UNSET"):
        cfg.resolve_secret_ref(ref, target_block={})


def test_secret_resolves_from_profile() -> None:
    """A ${profile.X.Y} ref reads key Y of the active target's profiles.X block."""
    ref = SecretRef(name="refresh", ref="${profile.shiphero.refresh_token}")
    target_block = {"profiles": {"shiphero": {"refresh_token": "profile-token"}}}
    assert cfg.resolve_secret_ref(ref, target_block) == "profile-token"


def test_secret_profile_value_nested_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A profile value that is itself ${env.VAR} resolves one level deeper."""
    monkeypatch.setenv("NESTED_TOKEN", "deep-value")
    ref = SecretRef(name="refresh", ref="${profile.acme.token}")
    target_block = {"profiles": {"acme": {"token": "${env.NESTED_TOKEN}"}}}
    assert cfg.resolve_secret_ref(ref, target_block) == "deep-value"


# ==========================================================================
# Project + profiles parsing (docs/06)
# ==========================================================================


def test_project_config_loads_fixture() -> None:
    """The committed fixture project parses with its documented keys."""
    project = cfg.ProjectConfig.load(FIXTURES_DIR)
    assert project.name == "det_test_project"
    assert project.connector_paths == ("connectors",)
    assert project.default_destination == "duckdb"
    assert project.default_target == "dev"
    assert project.vars["page_size"] == 100


def test_profiles_unknown_target_raises() -> None:
    """Selecting a target profiles.yml does not define fails clearly."""
    profiles = cfg.Profiles.load(FIXTURES_DIR)
    with pytest.raises(ConfigError, match="not defined"):
        profiles.target("staging")


def test_resolve_target_name_defaults_to_project_default() -> None:
    """With no explicit target, the project's default_target is used."""
    project = cfg.ProjectConfig.load(FIXTURES_DIR)
    profiles = cfg.Profiles.load(FIXTURES_DIR)
    assert cfg.resolve_target_name(None, project, profiles) == "dev"
    assert cfg.resolve_target_name("prod", project, profiles) == "prod"


# ==========================================================================
# The logger — secret redaction (docs/08)
# ==========================================================================


def test_redacting_filter_masks_secret() -> None:
    """The redacting filter replaces a secret value with the mask."""
    import logging

    f = RedactingFilter(["super-secret-token"])
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "calling API with super-secret-token", None, None
    )
    f.filter(record)
    assert "super-secret-token" not in record.getMessage()
    assert "***" in record.getMessage()


def test_build_logger_redacts(capsys: pytest.CaptureFixture[str]) -> None:
    """A logger built with a secret value never emits that value."""
    log = build_logger("test-run", ["leaked-credential-xyz"])
    log.info("token is leaked-credential-xyz here")
    captured = capsys.readouterr()
    assert "leaked-credential-xyz" not in captured.err
    assert "***" in captured.err


# ==========================================================================
# The run lifecycle — end to end through det.run (docs/02)
# ==========================================================================


def _query(db_path: str, sql: str) -> list[tuple]:
    """Run a read-only query against a .duckdb file on a fresh connection."""
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def test_run_succeeds_and_returns_runresult(duckdb_path: str) -> None:
    """det.run drives the echo connector and returns a SUCCEEDED RunResult."""
    result = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.connector == "echo"
    assert result.destination == "duckdb"
    assert result.target == "dev"
    assert result.rows_loaded == 9
    assert result.error is None
    assert result.duration_s >= 0


def test_run_target_defaults_to_project_default(duckdb_path: str) -> None:
    """run() with no target uses the project's default_target (docs/06)."""
    result = det.run(
        connector="echo",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.target == "dev"


def test_run_full_refresh_re_extracts(duckdb_path: str) -> None:
    """--full-refresh ignores a committed cursor and re-extracts (docs/03 §3.2).

    Run 1 commits the ``items`` cursor at 5. A plain run 2 would resume and
    yield 0 items; a ``full_refresh`` run 2 ignores the cursor and re-extracts
    all 5.
    """
    first = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    items_first = first.stream("items")
    assert items_first is not None and items_first.rows_loaded == 5

    # A plain re-run resumes — items yields 0.
    plain = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    items_plain = plain.stream("items")
    assert items_plain is not None and items_plain.rows_loaded == 0

    # A full-refresh re-run ignores the committed cursor — items yields all 5.
    refreshed = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
        full_refresh=True,
    )
    items_refreshed = refreshed.stream("items")
    assert items_refreshed is not None and items_refreshed.rows_loaded == 5
    assert refreshed.full_refresh is True


def test_run_select_skips_unselected_streams(duckdb_path: str) -> None:
    """run(select=...) runs only the named streams; the rest are SKIPPED."""
    result = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
        select=("events",),
    )
    events = result.stream("events")
    items = result.stream("items")
    assert events is not None and events.status.value == "succeeded"
    assert items is not None and items.status.value == "skipped"
    assert items.rows_loaded == 0


def test_run_failure_returns_failed_runresult(duckdb_path: str) -> None:
    """run() never raises — an unknown connector becomes a FAILED RunResult."""
    result = det.run(
        connector="no_such_connector",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert isinstance(result.error, DiscoveryError)
    with pytest.raises(DiscoveryError):
        result.raise_for_status()


def test_run_failing_stream_keeps_prior_stream_state(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A stream failure does not lose an earlier stream's committed state.

    docs/02 §Commit granularity: state commits per stream. This builds a source
    whose first stream succeeds and whose second stream raises. The run fails,
    but the first stream's row already committed to ``_det_state`` — proof
    that per-stream commit survives a later failure and a re-run would resume.
    """
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "partial"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: partial
            kind: source
            version: "1.0.0"
            summary: first stream ok, second stream raises.
            streams:
              - name: good
                table: partial_good
                write_disposition: append
                schema:
                  - {name: id, type: INTEGER}
              - name: bad
                table: partial_bad
                write_disposition: append
                schema:
                  - {name: id, type: INTEGER}
            destination:
              connector: duckdb
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="good")
            def good() -> Iterator[Batch]:
                yield [{"id": 1}, {"id": 2}]


            @stream(name="bad")
            def bad() -> Iterator[Batch]:
                yield [{"id": 3}]
                raise RuntimeError("boom — the bad stream fails mid-run")
            """
        )
    )

    result = det.run(
        connector="partial",
        target="dev",
        project_dir=str(tmp_path),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert isinstance(result.error, RuntimeError)
    good_result = result.stream("good")
    bad_result = result.stream("bad")
    assert good_result is not None and good_result.status.value == "succeeded"
    assert bad_result is not None and bad_result.status.value == "failed"

    # The crash-safety guarantee: the `good` stream's state committed before
    # `bad` failed, so its row survives in _det_state.
    state = _query(
        duckdb_path,
        "SELECT stream, rows_total FROM _det_state WHERE connector = 'partial'",
    )
    streams_committed = {row[0]: row[1] for row in state}
    assert streams_committed.get("good") == 2
    assert "bad" not in streams_committed  # bad failed before its commit_state

    # The TRANSACTIONAL_LOAD guarantee (docs/05 §5.3): the `bad` stream yielded
    # one batch — {"id": 3} — which write_batch persisted, then raised. Because
    # DuckDB declares TRANSACTIONAL_LOAD, that write happened inside the
    # per-stream transaction; the failure rolled it back. The table exists
    # (ensure_schema runs outside the transaction) but holds ZERO rows — no
    # half-written append duplicates for the re-run to trip over.
    bad_rows = _query(duckdb_path, "SELECT COUNT(*) FROM partial_bad")
    assert bad_rows[0][0] == 0


def test_run_append_stream_rollback_leaves_no_partial_rows(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A crash mid-append rolls back every batch already written this run.

    docs/05 §5.3: this is the guarantee TRANSACTIONAL_LOAD exists for. An
    ``append`` stream that yields several batches and then fails must leave the
    table empty — otherwise every crash would duplicate rows on the next run.
    The connector here yields three batches (6 rows) before raising; all six
    must be rolled back.
    """
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "crasher"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: crasher
            kind: source
            version: "1.0.0"
            summary: an append stream that crashes after several batches.
            streams:
              - name: rows
                table: crasher_rows
                write_disposition: append
                schema:
                  - {name: id, type: INTEGER}
            destination:
              connector: duckdb
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}, {"id": 2}]
                yield [{"id": 3}, {"id": 4}]
                yield [{"id": 5}, {"id": 6}]
                raise RuntimeError("boom — after 6 rows written this run")
            """
        )
    )

    result = det.run(
        connector="crasher",
        target="dev",
        project_dir=str(tmp_path),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert isinstance(result.error, RuntimeError)

    # Every one of the 6 written rows was rolled back — the table is empty.
    landed = _query(duckdb_path, "SELECT COUNT(*) FROM crasher_rows")
    assert landed[0][0] == 0
    # ...and no cursor/state row was committed for the stream either.
    state = _query(
        duckdb_path,
        "SELECT COUNT(*) FROM _det_state WHERE connector = 'crasher'",
    )
    assert state[0][0] == 0


def test_run_inferred_schema_for_undeclared_stream(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A stream with no declared schema has one inferred from the first batch.

    docs/02 §Normalize: omitting ``schema`` opts into inference. The engine
    infers columns + types from the first batch and the destination creates the
    table — the rows still land.
    """
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "noschema"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: noschema
            kind: source
            version: "1.0.0"
            summary: a stream that declares no schema.
            streams:
              - name: things
                table: noschema_things
                write_disposition: append
            destination:
              connector: duckdb
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="things")
            def things() -> Iterator[Batch]:
                yield [{"id": 1, "label": "a", "ratio": 1.5, "ok": True}]
            """
        )
    )
    result = det.run(
        connector="noschema",
        target="dev",
        project_dir=str(tmp_path),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    rows = _query(duckdb_path, "SELECT id, label, ratio, ok FROM noschema_things")
    assert rows == [(1, "a", 1.5, True)]


def test_run_strict_schema_rejects_divergence(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A schema_contract: strict stream fails when its batch carries an extra column.

    Locked decision: ``strict`` fails the run on any schema divergence, before
    ``ensure_schema``. The stream below declares only ``id`` but yields a record
    also carrying ``surprise`` — the run must fail.
    """
    _write_project(tmp_path)
    folder = tmp_path / "connectors" / "strict"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: strict
            kind: source
            version: "1.0.0"
            summary: a strict-contract stream.
            streams:
              - name: rows
                table: strict_rows
                write_disposition: append
                schema_contract: strict
                schema:
                  - {name: id, type: INTEGER}
            destination:
              connector: duckdb
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1, "surprise": "undeclared!"}]
            """
        )
    )
    result = det.run(
        connector="strict",
        target="dev",
        project_dir=str(tmp_path),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert "strict" in str(result.error).lower()


def test_run_default_destination_when_no_binding(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A source with no destination binding uses project default_destination."""
    _write_project(tmp_path, default_destination="duckdb")
    folder = tmp_path / "connectors" / "nobinding"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: nobinding
            kind: source
            version: "1.0.0"
            summary: declares no destination binding.
            streams:
              - name: rows
                table: nobinding_rows
                write_disposition: append
                schema:
                  - {name: id, type: INTEGER}
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            """\
            from det import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 7}]
            """
        )
    )
    result = det.run(
        connector="nobinding",
        target="dev",
        project_dir=str(tmp_path),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.destination == "duckdb"


def test_run_rejects_running_a_destination(duckdb_path: str) -> None:
    """Asking run() to run a destination connector fails cleanly."""
    result = det.run(
        connector="duckdb",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert "not a source" in str(result.error)


def test_run_incremental_initial_value_seeds_cursor(duckdb_path: str) -> None:
    """The engine types initial_value per cursor_type when seeding (docs/03 §3.2).

    echo's ``items`` declares ``initial_value: "0"`` with ``cursor_type: int``.
    On the first run the engine parses ``"0"`` → ``0``, so the stream yields all
    5 records and the committed cursor is the int 5.
    """
    result = det.run(
        connector="echo",
        target="dev",
        project_dir=str(FIXTURES_DIR),
        destination_params={"path": duckdb_path},
    )
    items = result.stream("items")
    assert items is not None
    assert items.cursor_before == 0  # initial_value "0" typed to int 0
    assert items.cursor_after == 5  # observed max
    state = _query(
        duckdb_path,
        "SELECT cursor_type FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert state[0][0] == "int"


# A couple of contract-type sanity checks the engine depends on, so a future
# types.py change that would break the engine fails here loudly.


def test_streamdef_is_incremental_flag() -> None:
    """StreamDef.is_incremental reflects the presence of an incremental block."""
    plain = StreamDef(name="s", table="s")
    inc = StreamDef(
        name="s",
        table="s",
        write_disposition=WriteDisposition.APPEND,
        incremental=Incremental(cursor_field="updated_at"),
    )
    assert plain.is_incremental is False
    assert inc.is_incremental is True
