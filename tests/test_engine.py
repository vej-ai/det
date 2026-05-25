"""Engine tests — discovery, config resolution, the run lifecycle (stage 5 + 8.B).

The smoke test (``test_smoke.py``) is the end-to-end executable spec. This
file tests the engine's *parts* directly:

* discovery resolves project-local sources/destinations over baked ones
  (docs/03 §5);
* config precedence layers merge in the documented order (docs/03 §6);
* discovery-time validation rejects a malformed source (docs/03 §7);
* ``--full-refresh`` re-extracts past a committed cursor (docs/03 §3.2);
* a failing stream does not lose an earlier stream's committed state
  (docs/02 §Commit granularity);
* secret resolution reads ``${env.X}`` / ``${profile.X.Y}`` (docs/03 §2.5)
  against the post-8.B destination-keyed profiles.yml;
* the run is driven by a *config* (``echo_dev``) — the stage-8.B runtime unit.

The real ``echo`` fixture + DuckDB destination drive the lifecycle tests;
discovery/validation tests build throwaway projects in ``tmp_path`` via the
:func:`_write_project` helper.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import duckdb
import pytest

import det
from det.engine import config as cfg
from det.engine import configs as cfgs
from det.engine import discovery as disc
from det.engine.config import ConfigError, Profiles
from det.engine.discovery import DiscoveryError
from det.engine.logger import RedactingFilter, build_logger
from det.types import (
    Incremental,
    ParamSpec,
    ParamType,
    PipelineConfig,
    SecretRef,
    StreamDef,
    WriteDisposition,
)

# The committed test project — tests/fixtures/ holds det_project.yml,
# profiles.yml, sources/echo/, configs/echo.yml.
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ==========================================================================
# Helpers — throwaway projects in tmp_path
# ==========================================================================


def _write_project(
    root: Path,
    *,
    vars_block: str = "",
    profiles_override: str | None = None,
) -> None:
    """Write a minimal post-8.B ``det_project.yml`` and a profiles file.

    The default profiles.yml carries one DuckDB target (``dev``) with no
    ``path`` set — engine tests pass ``destination_params_override={"path":
    ...}`` per call. ``profiles_override`` (raw YAML text) replaces the
    default profiles.yml entirely when the test needs a different shape.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "det_project.yml").write_text(
        textwrap.dedent(
            f"""\
            name: tmp_project
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            {vars_block}
            """
        )
    )
    profiles_text = profiles_override if profiles_override is not None else (
        textwrap.dedent(
            """\
            duckdb:
              default_target: dev
              targets:
                dev: {}
            """
        )
    )
    (root / "profiles.yml").write_text(profiles_text)


def _write_config(
    root: Path,
    *,
    name: str,
    source: str,
    destination: str = "duckdb",
    target: str | None = "dev",
    extra_lines: str = "",
) -> None:
    """Write a one-config-per-file under ``root/configs/<name>.yml``.

    Used by every test that needs to drive ``det.run(config=<name>)`` against
    a tmp_path source the test just authored.
    """
    configs_dir = root / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    target_line = f"target: {target}\n" if target is not None else ""
    (configs_dir / f"{name}.yml").write_text(
        textwrap.dedent(
            f"""\
            name: {name}
            source: {source}
            destination: {destination}
            {target_line}{extra_lines}
            """
        )
    )


def _write_source_clone(
    folder: Path, *, summary: str, tags: str = "[fixture]"
) -> None:
    """Write a tiny one-stream source connector folder at ``folder``."""
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
# Discovery — project root + source/destination resolution (docs/03 §5)
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


def test_resolve_source_project_local(tmp_path: Path) -> None:
    """A project-local source folder resolves and imports cleanly."""
    _write_project(tmp_path)
    _write_source_clone(tmp_path / "sources" / "clone", summary="local-copy")
    loaded = disc.resolve_source("clone", tmp_path, ["sources"])
    assert loaded.manifest.name == "clone"
    assert loaded.manifest.summary == "local-copy"
    assert "rows" in loaded.registry.stream_names


def test_resolve_destination_baked(tmp_path: Path) -> None:
    """The baked DuckDB destination resolves from det/destinations/."""
    _write_project(tmp_path)
    loaded = disc.resolve_destination("duckdb", tmp_path, ["destinations"])
    assert loaded.manifest.name == "duckdb"
    assert loaded.manifest.kind.value == "destination"
    assert "det" in str(loaded.folder)
    assert "destinations" in str(loaded.folder)


def test_project_local_source_shadows_baked(tmp_path: Path) -> None:
    """A project-local source named like a baked one wins (docs/03 §5)."""
    _write_project(tmp_path)
    # Author a project-local source with the same name as a baked source.
    shadow = tmp_path / "sources" / "filesystem"
    _write_source_clone(shadow, summary="shadowing-copy")
    folder = disc.find_source_folder("filesystem", tmp_path, ["sources"])
    assert folder == shadow.resolve()


def test_resolve_source_unknown_name_raises(tmp_path: Path) -> None:
    """An unresolvable source name raises DiscoveryError listing the search."""
    _write_project(tmp_path)
    with pytest.raises(DiscoveryError, match="not found"):
        disc.resolve_source("does_not_exist", tmp_path, ["sources"])


def test_resolve_destination_unknown_name_raises(tmp_path: Path) -> None:
    """An unresolvable destination name raises DiscoveryError listing the search."""
    _write_project(tmp_path)
    with pytest.raises(DiscoveryError, match="not found"):
        disc.resolve_destination("nope", tmp_path, ["destinations"])


def test_resolve_source_rejects_destination(tmp_path: Path) -> None:
    """resolve_source on a destination's name raises (kind enforcement)."""
    _write_project(tmp_path)
    # Plant a `kind: destination` folder under the source path.
    folder = tmp_path / "sources" / "fake_dest"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: fake_dest
            kind: destination
            version: "1.0.0"
            """
        )
    )
    (folder / "destination.py").write_text("# no hooks\n")
    with pytest.raises(DiscoveryError, match="not a source"):
        disc.resolve_source("fake_dest", tmp_path, ["sources"])


# ==========================================================================
# Discovery-time validation (docs/03 §7)
# ==========================================================================


def test_validation_rejects_orphan_manifest_stream(tmp_path: Path) -> None:
    """A streams[] entry with no matching @stream fails validation (rule 7)."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "broken"
    _write_source_clone(folder, summary="broken")
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
        disc.resolve_source("broken", tmp_path, ["sources"])


def test_validation_rejects_orphan_stream_function(tmp_path: Path) -> None:
    """A @stream with no matching streams[] entry fails validation (rule 7)."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "extra"
    _write_source_clone(folder, summary="extra")
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
        disc.resolve_source("extra", tmp_path, ["sources"])


def test_validation_rejects_bad_stream_signature(tmp_path: Path) -> None:
    """A @stream declaring a non-injectable parameter fails discovery (rule 8)."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "badsig"
    _write_source_clone(folder, summary="badsig")
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
        disc.resolve_source("badsig", tmp_path, ["sources"])


def test_validation_rejects_unknown_manifest_key(tmp_path: Path) -> None:
    """An unknown register.yaml key is a hard error (docs/03 §7 step 2)."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "typo"
    _write_source_clone(folder, summary="typo")
    reg = folder / "register.yaml"
    reg.write_text(reg.read_text() + "write_dispostion: append\n")
    with pytest.raises(DiscoveryError, match="unknown register.yaml key"):
        disc.resolve_source("typo", tmp_path, ["sources"])


# ==========================================================================
# Config resolution — the layered precedence of docs/03 §6
# ==========================================================================


def test_config_precedence_register_default_only() -> None:
    """With nothing overriding it, a param resolves to its register.yaml default."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={},
        config_params={},
        overrides={},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 50


def test_config_precedence_project_vars_over_default() -> None:
    """det_project.yml vars override the register.yaml default."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        config_params={},
        overrides={},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 100


def test_config_precedence_overrides_win() -> None:
    """run()/CLI overrides beat every lower layer; values are type-coerced."""
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        config_params={"page_size": 200},
        overrides={"page_size": "999"},
        connector_name="cfg",
    )
    assert resolved["page_size"] == 999
    assert isinstance(resolved["page_size"], int)


def test_config_precedence_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """SIMPLE_E_PARAM_<NAME> sits above the config's params, below run()/CLI."""
    monkeypatch.setenv("SIMPLE_E_PARAM_PAGE_SIZE", "777")
    resolved = cfg.resolve_params(
        {"page_size": ParamSpec(type=ParamType.INT, default=50)},
        project_vars={"page_size": 100},
        config_params={"page_size": 200},
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
            config_params={},
            overrides={},
            connector_name="cfg",
        )


def test_config_bad_type_raises() -> None:
    """A param value that will not coerce to its declared type fails."""
    with pytest.raises(ConfigError, match="not a valid int"):
        cfg.resolve_params(
            {"page_size": ParamSpec(type=ParamType.INT, default="not-a-number")},
            project_vars={},
            config_params={},
            overrides={},
            connector_name="cfg",
        )


# ==========================================================================
# Secret resolution — ${env.X} / ${profile.X.Y} (docs/03 §2.5, docs/06)
# ==========================================================================


def test_secret_resolves_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ${env.X} secret ref resolves to the environment variable's value."""
    monkeypatch.setenv("MY_API_TOKEN", "s3cr3t-value")
    ref = SecretRef(name="api_token", ref="${env.MY_API_TOKEN}")
    profiles = Profiles(destinations={}, secret_profiles={})
    assert cfg.resolve_secret_ref(ref, "dev", profiles) == "s3cr3t-value"


def test_secret_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ${env.X} ref to an unset variable fails — without leaking a value."""
    monkeypatch.delenv("DEFINITELY_UNSET", raising=False)
    ref = SecretRef(name="api_token", ref="${env.DEFINITELY_UNSET}")
    profiles = Profiles(destinations={}, secret_profiles={})
    with pytest.raises(ConfigError, match="DEFINITELY_UNSET"):
        cfg.resolve_secret_ref(ref, "dev", profiles)


def test_secret_resolves_from_profile() -> None:
    """A ${profile.X.Y} ref reads key Y of the active target's profiles.<target>.X block."""
    ref = SecretRef(name="refresh", ref="${profile.shiphero.refresh_token}")
    profiles = Profiles(
        destinations={},
        secret_profiles={"dev": {"shiphero": {"refresh_token": "profile-token"}}},
    )
    assert cfg.resolve_secret_ref(ref, "dev", profiles) == "profile-token"


def test_secret_profile_value_nested_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """A profile value that is itself ${env.VAR} resolves one level deeper."""
    monkeypatch.setenv("NESTED_TOKEN", "deep-value")
    ref = SecretRef(name="refresh", ref="${profile.acme.token}")
    profiles = Profiles(
        destinations={},
        secret_profiles={"dev": {"acme": {"token": "${env.NESTED_TOKEN}"}}},
    )
    assert cfg.resolve_secret_ref(ref, "dev", profiles) == "deep-value"


# ==========================================================================
# Project + profiles parsing (docs/06 post-8.B)
# ==========================================================================


def test_project_config_loads_fixture() -> None:
    """The committed fixture project parses with its post-8.B keys."""
    project = cfg.ProjectConfig.load(FIXTURES_DIR)
    assert project.name == "det_test_project"
    assert project.source_paths == ("sources",)
    assert project.destination_paths == ("destinations",)
    assert project.config_paths == ("configs",)
    assert project.vars["page_size"] == 100


def test_profiles_loads_destinations() -> None:
    """profiles.yml's destination-keyed blocks parse into DestinationTargets."""
    profiles = Profiles.load(FIXTURES_DIR)
    assert "duckdb" in profiles.destinations
    block = profiles.destination("duckdb")
    assert block.default_target == "dev"
    assert "dev" in block.targets
    assert "prod" in block.targets


def test_profiles_unknown_destination_raises() -> None:
    """Looking up a destination block not in the file fails clearly."""
    profiles = Profiles.load(FIXTURES_DIR)
    with pytest.raises(ConfigError, match="not found|no block"):
        profiles.destination("snowflake")


def test_profiles_unknown_target_raises() -> None:
    """A target undefined for a destination raises a clear error."""
    profiles = Profiles.load(FIXTURES_DIR)
    with pytest.raises(ConfigError, match="not defined"):
        profiles.target_params("duckdb", "staging")


def test_resolve_target_name_uses_destination_default() -> None:
    """With no explicit target, the destination's profiles.yml default applies."""
    profiles = Profiles.load(FIXTURES_DIR)
    assert cfg.resolve_target_name(None, "duckdb", profiles) == "dev"
    assert cfg.resolve_target_name("prod", "duckdb", profiles) == "prod"


def test_resolve_target_name_falls_back_to_default_when_destination_absent() -> None:
    """A destination with no profiles block falls back to the synthetic 'default'."""
    profiles = Profiles(destinations={}, secret_profiles={})
    assert cfg.resolve_target_name(None, "nonexistent", profiles) == "default"


# ==========================================================================
# The logger — secret redaction (docs/08)
# ==========================================================================


def test_redacting_filter_masks_secret() -> None:
    """The redacting filter replaces a secret value with the mask."""
    import logging

    from det.engine.logger import Redactor

    f = RedactingFilter(Redactor(["super-secret-token"]))
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "calling API with super-secret-token", None, None
    )
    f.filter(record)
    assert "super-secret-token" not in record.getMessage()
    assert "***" in record.getMessage()


def test_build_logger_redacts(capsys: pytest.CaptureFixture[str]) -> None:
    """A logger built with a secret value never emits that value."""
    from det.engine.logger import Redactor

    log = build_logger("test-run", Redactor(["leaked-credential-xyz"]))
    log.info("token is leaked-credential-xyz here")
    captured = capsys.readouterr()
    assert "leaked-credential-xyz" not in captured.err
    assert "***" in captured.err


# ==========================================================================
# The run lifecycle — end to end through det.run (docs/02, docs/12)
# ==========================================================================


def _query(db_path: str, sql: str) -> list[tuple]:
    """Run a read-only query against a .duckdb file on a fresh connection."""
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def test_run_succeeds_and_returns_runresult(duckdb_path: str) -> None:
    """det.run drives the echo_dev config and returns a SUCCEEDED RunResult."""
    result = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.config == "echo_dev"
    assert result.connector == "echo"  # source name
    assert result.destination == "duckdb"
    assert result.target == "dev"
    assert result.rows_loaded == 9
    assert result.error is None
    assert result.duration_s >= 0


def test_run_target_override(duckdb_path: str) -> None:
    """run(target_override=...) overrides the config's target field."""
    result = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        target_override="prod",
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.target == "prod"


def test_run_full_refresh_re_extracts(duckdb_path: str) -> None:
    """--full-refresh ignores a committed cursor and re-extracts (docs/03 §3.2)."""
    first = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    items_first = first.stream("items")
    assert items_first is not None and items_first.rows_loaded == 5

    plain = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    items_plain = plain.stream("items")
    assert items_plain is not None and items_plain.rows_loaded == 0

    refreshed = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
        full_refresh=True,
    )
    items_refreshed = refreshed.stream("items")
    assert items_refreshed is not None and items_refreshed.rows_loaded == 5
    assert refreshed.full_refresh is True


def test_run_select_replaces_config_select(duckdb_path: str) -> None:
    """run(select=...) replaces the config's `select:`; unnamed streams SKIP."""
    result = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
        select=("events",),
    )
    events = result.stream("events")
    items = result.stream("items")
    assert events is not None and events.status.value == "succeeded"
    assert items is not None and items.status.value == "skipped"
    assert items.rows_loaded == 0


def test_run_failure_returns_failed_runresult(duckdb_path: str) -> None:
    """run() never raises — an unknown config becomes a FAILED RunResult."""
    result = det.run(
        config="no_such_config",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert isinstance(result.error, ConfigError)
    with pytest.raises(ConfigError):
        result.raise_for_status()


def test_run_failing_stream_keeps_prior_stream_state(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A stream failure does not lose an earlier stream's committed state."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "partial"
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
    _write_config(tmp_path, name="partial_dev", source="partial")

    result = det.run(
        config="partial_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert isinstance(result.error, RuntimeError)
    good_result = result.stream("good")
    bad_result = result.stream("bad")
    assert good_result is not None and good_result.status.value == "succeeded"
    assert bad_result is not None and bad_result.status.value == "failed"

    state = _query(
        duckdb_path,
        "SELECT stream, rows_total FROM _det_state WHERE connector = 'partial'",
    )
    streams_committed = {row[0]: row[1] for row in state}
    assert streams_committed.get("good") == 2
    assert "bad" not in streams_committed

    bad_rows = _query(duckdb_path, "SELECT COUNT(*) FROM partial_bad")
    assert bad_rows[0][0] == 0


def test_run_append_stream_rollback_leaves_no_partial_rows(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A crash mid-append rolls back every batch already written this run."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "crasher"
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
    _write_config(tmp_path, name="crasher_dev", source="crasher")

    result = det.run(
        config="crasher_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert isinstance(result.error, RuntimeError)

    landed = _query(duckdb_path, "SELECT COUNT(*) FROM crasher_rows")
    assert landed[0][0] == 0
    state = _query(
        duckdb_path,
        "SELECT COUNT(*) FROM _det_state WHERE connector = 'crasher'",
    )
    assert state[0][0] == 0


def test_run_inferred_schema_for_undeclared_stream(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A stream with no declared schema has one inferred from the first batch."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "noschema"
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
    _write_config(tmp_path, name="noschema_dev", source="noschema")
    result = det.run(
        config="noschema_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    rows = _query(duckdb_path, "SELECT id, label, ratio, ok FROM noschema_things")
    assert rows == [(1, "a", 1.5, True)]


def test_run_strict_schema_rejects_divergence(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A schema_contract: strict stream fails when its batch carries an extra column."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "strict"
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
    _write_config(tmp_path, name="strict_dev", source="strict")
    result = det.run(
        config="strict_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert "strict" in str(result.error).lower()


def test_run_rejects_config_pointing_at_destination_as_source(
    duckdb_path: str, tmp_path: Path
) -> None:
    """A config whose `source:` resolves to a destination fails cleanly."""
    _write_project(tmp_path)
    # No source under tmp_path; the config's `source: duckdb` will resolve via
    # the baked destinations root and be rejected.
    _write_config(tmp_path, name="bad_dev", source="duckdb")
    # tmp_path has no destinations/duckdb folder; baked duckdb wins.
    # Build a destinations dir so resolve_destination succeeds, then the
    # source side is the one that fails.
    result = det.run(
        config="bad_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert "not a source" in str(result.error) or "not found" in str(result.error)


def test_run_incremental_initial_value_seeds_cursor(duckdb_path: str) -> None:
    """The engine types initial_value per cursor_type when seeding (docs/03 §3.2)."""
    result = det.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    items = result.stream("items")
    assert items is not None
    assert items.cursor_before == 0
    assert items.cursor_after == 5
    state = _query(
        duckdb_path,
        "SELECT cursor_type FROM _det_state "
        "WHERE connector = 'echo' AND stream = 'items'",
    )
    assert state[0][0] == "int"


def test_run_legacy_destination_block_tolerated(
    tmp_path: Path, duckdb_path: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A source still carrying a legacy `destination:` block runs (warn + ignore)."""
    _write_project(tmp_path)
    folder = tmp_path / "sources" / "legacy"
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: legacy
            kind: source
            version: "1.0.0"
            summary: still carries an old-style destination binding.
            streams:
              - name: rows
                table: legacy_rows
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
                yield [{"id": 1}]
            """
        )
    )
    _write_config(tmp_path, name="legacy_dev", source="legacy")
    import logging

    with caplog.at_level(logging.WARNING, logger="det.engine"):
        result = det.run(
            config="legacy_dev",
            project_dir=str(tmp_path),
            destination_params_override={"path": duckdb_path},
        )
    assert result.status.value == "succeeded"
    # The warning was emitted but the run still succeeded.
    assert any("legacy" in rec.message for rec in caplog.records)


# A couple of contract-type sanity checks the engine depends on.


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


def test_pipeline_config_from_dict_minimal() -> None:
    """PipelineConfig.from_dict accepts the three required keys."""
    pc = PipelineConfig.from_dict(
        {"name": "p", "source": "s", "destination": "d"}
    )
    assert pc.name == "p"
    assert pc.source == "s"
    assert pc.destination == "d"
    assert pc.target is None
    assert pc.params == {}
    assert pc.select == ()


def test_pipeline_config_from_dict_rejects_unknown_key() -> None:
    """An unknown top-level key in a config is a hard error (typo guard)."""
    with pytest.raises(ValueError, match="unknown config key"):
        PipelineConfig.from_dict(
            {"name": "p", "source": "s", "destination": "d", "tgt": "dev"}
        )


def test_pipeline_config_from_dict_requires_source() -> None:
    """A config missing `source:` is rejected."""
    with pytest.raises(ValueError, match="source"):
        PipelineConfig.from_dict({"name": "p", "destination": "d"})


def test_configs_discover_fixtures() -> None:
    """The committed fixture configs file parses both echo_dev and echo_prod."""
    configs = cfgs.discover_configs(FIXTURES_DIR, ["configs"])
    assert set(configs) == {"echo_dev", "echo_prod"}
    assert configs["echo_dev"].source == "echo"
    assert configs["echo_dev"].destination == "duckdb"
    assert configs["echo_dev"].target == "dev"
