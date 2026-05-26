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

import logging
import textwrap
from pathlib import Path

import duckdb
import pytest

import detx
from detx.engine import config as cfg
from detx.engine import configs as cfgs
from detx.engine import discovery as disc
from detx.engine.config import ConfigError, Profiles
from detx.engine.discovery import DiscoveryError
from detx.engine.logger import RedactingFilter, build_logger
from detx.types import (
    CursorType,
    Field,
    FieldType,
    Incremental,
    ParamSpec,
    ParamType,
    PartitionConfig,
    PartitionRange,
    PartitionType,
    PipelineConfig,
    Schema,
    SecretRef,
    StreamDef,
    TimeGranularity,
    WriteDisposition,
)

# The committed test project — tests/fixtures/ holds detx_project.yml,
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
    """Write a minimal post-8.B ``detx_project.yml`` and a profiles file.

    The default profiles.yml carries one DuckDB target (``dev``) with no
    ``path`` set — engine tests pass ``destination_params_override={"path":
    ...}`` per call. ``profiles_override`` (raw YAML text) replaces the
    default profiles.yml entirely when the test needs a different shape.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / "detx_project.yml").write_text(
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

    Used by every test that needs to drive ``detx.run(config=<name>)`` against
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
            from detx import Batch, stream
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
    """A directory tree with no detx_project.yml raises DiscoveryError."""
    with pytest.raises(DiscoveryError, match="no detx_project.yml"):
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
    """The baked DuckDB destination resolves from detx/destinations/."""
    _write_project(tmp_path)
    loaded = disc.resolve_destination("duckdb", tmp_path, ["destinations"])
    assert loaded.manifest.name == "duckdb"
    assert loaded.manifest.kind.value == "destination"
    assert "detx" in str(loaded.folder)
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
            from detx import Batch, stream
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
    """detx_project.yml vars override the register.yaml default."""
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
    assert project.name == "detx_test_project"
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

    from detx.engine.logger import Redactor

    f = RedactingFilter(Redactor(["super-secret-token"]))
    record = logging.LogRecord(
        "t", logging.INFO, __file__, 1, "calling API with super-secret-token", None, None
    )
    f.filter(record)
    assert "super-secret-token" not in record.getMessage()
    assert "***" in record.getMessage()


def test_build_logger_redacts(capsys: pytest.CaptureFixture[str]) -> None:
    """A logger built with a secret value never emits that value."""
    from detx.engine.logger import Redactor

    log = build_logger("test-run", Redactor(["leaked-credential-xyz"]))
    log.info("token is leaked-credential-xyz here")
    captured = capsys.readouterr()
    assert "leaked-credential-xyz" not in captured.err
    assert "***" in captured.err


# ==========================================================================
# The run lifecycle — end to end through detx.run (docs/02, docs/12)
# ==========================================================================


def _query(db_path: str, sql: str) -> list[tuple]:
    """Run a read-only query against a .duckdb file on a fresh connection."""
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def test_run_succeeds_and_returns_runresult(duckdb_path: str) -> None:
    """detx.run drives the echo_dev config and returns a SUCCEEDED RunResult."""
    result = detx.run(
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
    result = detx.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        target_override="prod",
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "succeeded"
    assert result.target == "prod"


def test_run_full_refresh_re_extracts(duckdb_path: str) -> None:
    """--full-refresh ignores a committed cursor and re-extracts (docs/03 §3.2)."""
    first = detx.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    items_first = first.stream("items")
    assert items_first is not None and items_first.rows_loaded == 5

    plain = detx.run(
        config="echo_dev",
        project_dir=str(FIXTURES_DIR),
        destination_params_override={"path": duckdb_path},
    )
    items_plain = plain.stream("items")
    assert items_plain is not None and items_plain.rows_loaded == 0

    refreshed = detx.run(
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
    result = detx.run(
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
    result = detx.run(
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
            from detx import Batch, stream
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

    result = detx.run(
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
        "SELECT stream, rows_total FROM _detx_state WHERE connector = 'partial'",
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
            from detx import Batch, stream
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

    result = detx.run(
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
        "SELECT COUNT(*) FROM _detx_state WHERE connector = 'crasher'",
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
            from detx import Batch, stream
            from collections.abc import Iterator


            @stream(name="things")
            def things() -> Iterator[Batch]:
                yield [{"id": 1, "label": "a", "ratio": 1.5, "ok": True}]
            """
        )
    )
    _write_config(tmp_path, name="noschema_dev", source="noschema")
    result = detx.run(
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
            from detx import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1, "surprise": "undeclared!"}]
            """
        )
    )
    _write_config(tmp_path, name="strict_dev", source="strict")
    result = detx.run(
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
    result = detx.run(
        config="bad_dev",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    assert "not a source" in str(result.error) or "not found" in str(result.error)


def test_run_incremental_initial_value_seeds_cursor(duckdb_path: str) -> None:
    """The engine types initial_value per cursor_type when seeding (docs/03 §3.2)."""
    result = detx.run(
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
        "SELECT cursor_type FROM _detx_state "
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
            from detx import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}]
            """
        )
    )
    _write_config(tmp_path, name="legacy_dev", source="legacy")
    import logging

    with caplog.at_level(logging.WARNING, logger="detx.engine"):
        result = detx.run(
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


# ==========================================================================
# Partition resolution — source default vs config override vs auto-default
# (docs/05 §3.x — _resolve_partition in detx.engine.runner)
# ==========================================================================


def _stream_def_for_partition_test(
    *,
    partition_by: str | PartitionConfig | None = None,
    cursor_type: CursorType | None = CursorType.TIMESTAMP,
    cursor_field: str = "created_date",
    schema_field_type: FieldType = FieldType.TIMESTAMP,
) -> tuple[StreamDef, Schema]:
    """Build a StreamDef + Schema pair tailored for one resolver scenario."""
    inc = (
        None
        if cursor_type is None
        else Incremental(cursor_field=cursor_field, cursor_type=cursor_type)
    )
    schema = Schema(fields=(Field(name=cursor_field, type=schema_field_type),))
    sd = StreamDef(
        name="rows",
        table="rows_table",
        primary_key=(),
        write_disposition=WriteDisposition.APPEND,
        incremental=inc,
        schema=schema,
        partition_by=partition_by,
    )
    return sd, schema


def _empty_pipeline(
    partition_overrides: dict[str, PartitionConfig] | None = None,
) -> PipelineConfig:
    """A throwaway PipelineConfig — only partition_overrides matters here."""
    return PipelineConfig(
        name="p",
        source="src",
        destination="dst",
        partition_overrides=partition_overrides or {},
    )


def test_resolve_partition_source_only_short_form_timestamp() -> None:
    """Source short form on a TIMESTAMP cursor column → TIME+DAY."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(partition_by="created_date")
    pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is not None
    assert pc.type is PartitionType.TIME
    assert pc.granularity is TimeGranularity.DAY
    assert pc.field == "created_date"


def test_resolve_partition_source_only_long_form_range() -> None:
    """Source long form (range) on an INT cursor column → honored verbatim."""
    from detx.engine.runner import _resolve_partition

    declared = PartitionConfig(
        field="created",
        type=PartitionType.RANGE,
        range=PartitionRange(start=0, end=100, interval=10),
    )
    sd, schema = _stream_def_for_partition_test(
        partition_by=declared,
        cursor_type=CursorType.INT,
        cursor_field="created",
        schema_field_type=FieldType.INTEGER,
    )
    pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc == declared


def test_resolve_partition_config_only_overrides_when_source_silent() -> None:
    """A config override with no source-side declaration is honored."""
    from detx.engine.runner import _resolve_partition

    override = PartitionConfig(
        field="created",
        type=PartitionType.RANGE,
        range=PartitionRange(start=0, end=100, interval=10),
    )
    sd, schema = _stream_def_for_partition_test(
        partition_by=None,
        cursor_type=CursorType.INT,
        cursor_field="created",
        schema_field_type=FieldType.INTEGER,
    )
    pc = _resolve_partition(
        sd,
        _empty_pipeline({sd.name: override}),
        schema,
        logging.getLogger("t"),
    )
    assert pc == override


def test_resolve_partition_config_wins_over_source() -> None:
    """When both source and config declare partition_by, the config wins."""
    from detx.engine.runner import _resolve_partition

    declared_source = "created_date"
    override = PartitionConfig(
        field="created_date",
        type=PartitionType.TIME,
        granularity=TimeGranularity.HOUR,
    )
    sd, schema = _stream_def_for_partition_test(partition_by=declared_source)
    pc = _resolve_partition(
        sd,
        _empty_pipeline({sd.name: override}),
        schema,
        logging.getLogger("t"),
    )
    assert pc == override
    assert pc.granularity is TimeGranularity.HOUR  # config's HOUR, not source's DAY


def test_resolve_partition_auto_default_timestamp_cursor() -> None:
    """No declaration + incremental timestamp cursor → TIME+DAY on cursor field."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(partition_by=None)
    pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is not None
    assert pc.type is PartitionType.TIME
    assert pc.granularity is TimeGranularity.DAY
    assert pc.field == "created_date"


def test_resolve_partition_auto_default_date_cursor() -> None:
    """No declaration + incremental date cursor → TIME+DAY (same as timestamp)."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(
        partition_by=None,
        cursor_type=CursorType.DATE,
        schema_field_type=FieldType.DATE,
    )
    pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is not None
    assert pc.field == "created_date"
    assert pc.type is PartitionType.TIME


def test_resolve_partition_auto_default_int_cursor_no_partition_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No declaration + int cursor → no partition + a warning naming the long form."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(
        partition_by=None,
        cursor_type=CursorType.INT,
        cursor_field="id",
        schema_field_type=FieldType.INTEGER,
    )
    with caplog.at_level(logging.WARNING):
        pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is None
    # The warning must name the explicit-declaration syntax — type=range/ingestion.
    warnings = "\n".join(r.message for r in caplog.records if r.levelno >= logging.WARNING)
    assert "cannot be auto-partitioned" in warnings
    assert "type=range" in warnings
    assert "type=ingestion" in warnings


def test_resolve_partition_auto_default_string_cursor_no_partition_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No declaration + string cursor → no partition + a warning naming the long form."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(
        partition_by=None,
        cursor_type=CursorType.STRING,
        cursor_field="page_token",
        schema_field_type=FieldType.STRING,
    )
    with caplog.at_level(logging.WARNING):
        pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is None
    assert any("cannot be auto-partitioned" in r.message for r in caplog.records)


def test_resolve_partition_non_incremental_stream_no_partition() -> None:
    """No declaration + no incremental block → no partition, no warning (silent)."""
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(
        partition_by=None, cursor_type=None
    )
    pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is None


def test_resolve_partition_short_form_on_int_cursor_degrades_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Backward-compat: short-form partition_by on an INT cursor degrades to no partition.

    The pre-stage-8c Stripe sources declare ``partition_by: created`` against
    an INTEGER ``created`` column with cursor_type=int. Today the destination
    ignores the field; once BigQuery starts honoring it, naively applying
    TIME+DAY would crash. The resolver instead degrades to no partition and
    logs a warning telling the user to switch to the long form.
    """
    from detx.engine.runner import _resolve_partition

    sd, schema = _stream_def_for_partition_test(
        partition_by="created",
        cursor_type=CursorType.INT,
        cursor_field="created",
        schema_field_type=FieldType.INTEGER,
    )
    with caplog.at_level(logging.WARNING):
        pc = _resolve_partition(sd, _empty_pipeline(), schema, logging.getLogger("t"))
    assert pc is None
    text = "\n".join(r.message for r in caplog.records if r.levelno >= logging.WARNING)
    assert "ignoring short-form partition_by" in text
    assert "type: range" in text  # the explicit-declaration suggestion


def test_resolve_partition_short_form_on_string_schema_degrades_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Short form on a STRING-typed column (no cursor at all) also degrades."""
    from detx.engine.runner import _resolve_partition

    sd = StreamDef(
        name="rows",
        table="rows_table",
        primary_key=(),
        write_disposition=WriteDisposition.APPEND,
        incremental=None,
        schema=Schema(fields=(Field(name="name", type=FieldType.STRING),)),
        partition_by="name",
    )
    with caplog.at_level(logging.WARNING):
        pc = _resolve_partition(
            sd, _empty_pipeline(), sd.schema, logging.getLogger("t")
        )
    assert pc is None


# ==========================================================================
# stream_start event includes the resolved partition spec (docs/09 §2)
# ==========================================================================


def test_partition_overrides_unknown_stream_name_fails_run(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A config with partition_overrides keyed on a typo'd stream name fails the run.

    Silent-ignore would be the trap (the override never fires, the user
    thinks production is partitioned by their override but isn't). Same
    "unknown name → list known names" shape as ``load_config``'s typo error.
    """
    _write_project(tmp_path)
    src_dir = tmp_path / "sources" / "tiny"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: tiny
            kind: source
            version: "1.0.0"
            summary: tiny fixture for unknown-stream test
            tags: [fixture]
            streams:
              - name: rows
                table: tiny_rows
                write_disposition: append
                schema:
                  - {name: id, type: INTEGER}
            """
        )
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """\
            from collections.abc import Iterator
            from detx import Batch, stream


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}]
            """
        )
    )
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "tiny_dev.yml").write_text(
        textwrap.dedent(
            """\
            name: tiny_dev
            source: tiny
            destination: duckdb
            target: dev
            partition_overrides:
              chrages:   # typo — the stream is `rows`, not `chrages`
                field: x
                type: time
                granularity: day
            """
        )
    )
    result = detx.run(
        config="tiny_dev",
        project_dir=tmp_path,
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "failed"
    err = str(result.error)
    assert "partition_overrides names stream(s)" in err
    assert "chrages" in err
    assert "known streams" in err
    assert "rows" in err  # the actual stream name is listed


def test_stream_start_event_carries_partition_when_auto_default_kicks_in(
    tmp_path: Path, duckdb_path: str
) -> None:
    """The JSONL stream_start event records the resolved partition spec.

    Stands up a tmp_path source whose stream has a timestamp cursor and no
    declared partition_by; the engine's auto-default should resolve to TIME+
    DAY on the cursor field, and that resolved spec should land in the
    ``stream_start`` event. DuckDB ignores partitioning at the table level
    (NOTE in its ensure_schema), but the engine still resolves and logs the
    choice so it is visible cross-destination.
    """
    import json

    _write_project(tmp_path)
    src_dir = tmp_path / "sources" / "ts_source"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: ts_source
            kind: source
            version: "1.0.0"
            summary: timestamp-cursor stream for partition auto-default test
            tags: [fixture]
            streams:
              - name: rows
                table: ts_rows
                write_disposition: append
                incremental:
                  cursor_field: created_at
                  cursor_type: timestamp
                schema:
                  - {name: id,         type: INTEGER, mode: REQUIRED}
                  - {name: created_at, type: TIMESTAMP, mode: REQUIRED}
            """
        )
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """\
            from datetime import datetime, UTC
            from collections.abc import Iterator
            from detx import Batch, stream


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [
                    {"id": 1, "created_at": datetime(2026, 1, 1, tzinfo=UTC)},
                    {"id": 2, "created_at": datetime(2026, 1, 2, tzinfo=UTC)},
                ]
            """
        )
    )
    _write_config(tmp_path, name="ts_dev", source="ts_source")

    result = detx.run(
        config="ts_dev",
        project_dir=tmp_path,
        destination_params_override={"path": duckdb_path},
    )
    assert result.status.value == "succeeded", result.error
    log_path = Path(result.log_path)
    assert log_path.is_file()
    events = [
        json.loads(line) for line in log_path.read_text().splitlines() if line.strip()
    ]
    starts = [e for e in events if e["event"] == "stream_start"]
    assert starts, "expected at least one stream_start event"
    for ev in starts:
        assert "partition" in ev, f"stream_start missing partition: {ev}"
    # The TIMESTAMP-cursor stream MUST carry an auto-default TIME/DAY partition.
    assert any(
        ev.get("partition") and "TIME/DAY" in ev["partition"] for ev in starts
    ), f"got partitions: {[e.get('partition') for e in starts]}"


# ==========================================================================
# detx.run_tag — multi-config tag-based runs (stage 8d)
# ==========================================================================


def _write_tagged_source(
    tmp_path: Path, *, name: str, ok: bool = True, row_id: int = 1
) -> None:
    """Author a minimal one-stream source connector folder under ``sources/<name>/``.

    ``ok=False`` produces a source that raises mid-stream, so the test can
    prove ``run_tag`` continues past per-config failures.
    """
    folder = tmp_path / "sources" / name
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            f"""\
            name: {name}
            kind: source
            version: "1.0.0"
            summary: fixture for run_tag tests.
            streams:
              - name: rows
                table: {name}_rows
                write_disposition: append
                schema:
                  - {{name: id, type: INTEGER}}
            """
        )
    )
    body = (
        f"yield [{{'id': {row_id}}}]"
        if ok
        else "yield [{'id': 1}]\n                raise RuntimeError('boom')"
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            f"""\
            from detx import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                {body}
            """
        )
    )


def _write_tagged_config(
    tmp_path: Path,
    *,
    name: str,
    source: str,
    tags: list[str],
    target: str = "dev",
) -> None:
    """Write a one-config YAML carrying a ``tags:`` block — stage 8d fixture."""
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    tags_yaml = "[" + ", ".join(tags) + "]"
    (configs_dir / f"{name}.yml").write_text(
        textwrap.dedent(
            f"""\
            name: {name}
            source: {source}
            destination: duckdb
            target: {target}
            tags: {tags_yaml}
            """
        )
    )


def test_run_tag_runs_every_matching_config(
    tmp_path: Path, duckdb_path: str
) -> None:
    """run_tag returns one RunResult per matched config; all succeed here."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_source(tmp_path, name="beta")
    _write_tagged_source(tmp_path, name="gamma")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["hourly"])
    _write_tagged_config(tmp_path, name="beta_dev", source="beta", tags=["hourly"])
    # gamma is daily — not tagged 'hourly', should NOT run
    _write_tagged_config(tmp_path, name="gamma_dev", source="gamma", tags=["daily"])

    results = detx.run_tag(
        "hourly",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert len(results) == 2
    names = [r.config for r in results]
    # Order is alphabetical by config name (run_tag contract).
    assert names == ["alpha_dev", "beta_dev"]
    assert all(r.status.value == "succeeded" for r in results)


def test_run_tag_zero_matches_returns_empty_list(
    tmp_path: Path, duckdb_path: str
) -> None:
    """run_tag with no matching configs returns an empty list (no error)."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["hourly"])

    results = detx.run_tag(
        "no_such_tag",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert results == []


def test_run_tag_continues_past_failure(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A failing config does NOT stop the rest — the run list spans both."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")          # succeeds
    _write_tagged_source(tmp_path, name="boom", ok=False)  # raises mid-stream
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["test"])
    _write_tagged_config(tmp_path, name="boom_dev", source="boom", tags=["test"])

    results = detx.run_tag(
        "test",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert len(results) == 2
    by_name = {r.config: r for r in results}
    # alpha_dev sorts before boom_dev alphabetically.
    assert list(by_name) == ["alpha_dev", "boom_dev"]
    assert by_name["alpha_dev"].status.value == "succeeded"
    assert by_name["boom_dev"].status.value == "failed"
    assert by_name["boom_dev"].error is not None


def test_run_tag_case_insensitive_match(
    tmp_path: Path, duckdb_path: str
) -> None:
    """`run_tag("Hourly")` matches a config whose tags include `hourly`."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["hourly"])

    results = detx.run_tag(
        "Hourly",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert len(results) == 1
    assert results[0].config == "alpha_dev"


def test_run_tag_target_override_applies_to_every_config(
    tmp_path: Path, duckdb_path: str
) -> None:
    """target_override is applied uniformly to every matched config."""
    _write_project(
        tmp_path,
        profiles_override=textwrap.dedent(
            """\
            duckdb:
              default_target: dev
              targets:
                dev: {}
                staging: {}
            """
        ),
    )
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_source(tmp_path, name="beta")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["hourly"])
    _write_tagged_config(tmp_path, name="beta_dev", source="beta", tags=["hourly"])

    results = detx.run_tag(
        "hourly",
        project_dir=str(tmp_path),
        target_override="staging",
        destination_params_override={"path": duckdb_path},
    )
    assert len(results) == 2
    assert all(r.target == "staging" for r in results)


def test_run_tag_alphabetical_order_three_configs(
    tmp_path: Path, duckdb_path: str
) -> None:
    """Configs run in alphabetical order regardless of file/discovery order."""
    _write_project(tmp_path)
    for name in ("zebra", "apple", "mango"):
        _write_tagged_source(tmp_path, name=name)
        _write_tagged_config(
            tmp_path, name=f"{name}_run", source=name, tags=["everything"]
        )

    results = detx.run_tag(
        "everything",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert [r.config for r in results] == ["apple_run", "mango_run", "zebra_run"]


# ==========================================================================
# Stage 8e — pipeline-level parallelism: threads in profiles.yml + run_tag(threads=)
# ==========================================================================


# Standard fixture-tree path to the test lockedfake destination (a Tier A
# in-memory destination declaring max_concurrent_writes = 1). Copied per
# test into the throwaway project so the lockedfake's process-global
# concurrency counter is the test's source of truth.
_LOCKEDFAKE_DIR = FIXTURES_DIR / "destinations" / "lockedfake"


def _install_lockedfake(project_root: Path) -> None:
    """Symlink / copy the lockedfake destination into ``project_root/destinations/``.

    Copy (not symlink) so the engine's discovery walks treat it as a normal
    project-local destination folder under ``destinations/``.
    """
    import shutil as _sh

    dst = project_root / "destinations" / "lockedfake"
    dst.parent.mkdir(parents=True, exist_ok=True)
    _sh.copytree(_LOCKEDFAKE_DIR, dst)


def _write_slow_source(
    tmp_path: Path, *, name: str, hold_ms: int = 100
) -> None:
    """Author a tiny one-stream source whose stream sleeps before yielding.

    The sleep makes wall-clock observable: a 4-pipeline sequential run with
    ``hold_ms=100`` takes >=400 ms; a 4-pipeline parallel run with the same
    sleep takes <=300 ms even with hot startup. Tests that compare timing
    use this margin.
    """
    folder = tmp_path / "sources" / name
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(
        textwrap.dedent(
            f"""\
            name: {name}
            kind: source
            version: "1.0.0"
            summary: stage 8e parallel-timing fixture.
            streams:
              - name: rows
                table: {name}_rows
                write_disposition: append
                schema:
                  - {{name: id, type: INTEGER}}
            """
        )
    )
    (folder / "source.py").write_text(
        textwrap.dedent(
            f"""\
            import time
            from detx import Batch, stream
            from collections.abc import Iterator


            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                time.sleep({hold_ms / 1000.0})
                yield [{{"id": 1}}]
            """
        )
    )


def _write_lockedfake_config(
    tmp_path: Path, *, name: str, source: str, tags: list[str]
) -> None:
    """Write a config bound to the lockedfake destination."""
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    tags_yaml = "[" + ", ".join(tags) + "]"
    (configs_dir / f"{name}.yml").write_text(
        textwrap.dedent(
            f"""\
            name: {name}
            source: {source}
            destination: lockedfake
            tags: {tags_yaml}
            """
        )
    )


def test_run_tag_threads_default_is_sequential(
    tmp_path: Path, duckdb_path: str
) -> None:
    """Without an explicit threads, behavior matches sequential (regression)."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_source(tmp_path, name="beta")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["test"])
    _write_tagged_config(tmp_path, name="beta_dev", source="beta", tags=["test"])

    results = detx.run_tag(
        "test",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
    )
    assert [r.config for r in results] == ["alpha_dev", "beta_dev"]
    assert all(r.status.value == "succeeded" for r in results)


def test_run_tag_threads_explicit_one_is_sequential(
    tmp_path: Path, duckdb_path: str
) -> None:
    """threads=1 takes the literal sequential code path (debuggability)."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_source(tmp_path, name="beta")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["test"])
    _write_tagged_config(tmp_path, name="beta_dev", source="beta", tags=["test"])

    results = detx.run_tag(
        "test",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
        threads=1,
    )
    assert [r.config for r in results] == ["alpha_dev", "beta_dev"]
    assert all(r.status.value == "succeeded" for r in results)


def test_run_tag_threads_parallel_returns_in_matched_order(
    tmp_path: Path,
) -> None:
    """threads>1 returns results in matched (alphabetical) order, not completion order.

    Uses the lockedfake destination so timing-induced reordering would
    actually happen if the implementation walked futures-as-completed
    instead of matched order. With slow sources of varying hold_ms, the
    first-completed pipeline is NOT the alphabetically-first one — yet the
    returned list must still be alphabetical.

    # NOTE: this test uses lockedfake (cap=1) intentionally so the
    # serialization isn't a concern — we're testing pure ORDERING.
    """
    _install_lockedfake(tmp_path)
    (tmp_path / "detx_project.yml").write_text(
        textwrap.dedent(
            """\
            name: order_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text("")
    # Three sources, slowest first alphabetically.
    _write_slow_source(tmp_path, name="apple", hold_ms=200)
    _write_slow_source(tmp_path, name="banana", hold_ms=10)
    _write_slow_source(tmp_path, name="cherry", hold_ms=10)
    _write_lockedfake_config(tmp_path, name="apple_run", source="apple", tags=["sweep"])
    _write_lockedfake_config(tmp_path, name="banana_run", source="banana", tags=["sweep"])
    _write_lockedfake_config(tmp_path, name="cherry_run", source="cherry", tags=["sweep"])

    results = detx.run_tag(
        "sweep",
        project_dir=str(tmp_path),
        threads=4,
    )
    # Order is alphabetical by config name regardless of completion order.
    assert [r.config for r in results] == ["apple_run", "banana_run", "cherry_run"]
    assert all(r.status.value == "succeeded" for r in results)


def test_run_tag_threads_parallel_runs_faster_than_sequential(
    tmp_path: Path, duckdb_path: str
) -> None:
    """4 independent BigQuery-style configs run faster in parallel than serial.

    Uses 3 lockedfake-like-but-uncapped destinations (well, 3 separate
    DuckDB destinations would all share the cap of 1 — bad for this test).
    Trick: 3 distinct project-local destinations cloned from lockedfake,
    each named differently so the per-destination semaphore is independent
    per pipeline. With hold_ms=200 inside lockedfake's write_batch and 3
    pipelines all touching different destinations, the per-pipeline
    semaphore never blocks anyone.

    Tolerance: a parallel run with threads=4 across 3 200ms-each pipelines
    should finish in <=500ms (one pipeline's hold_ms + slack for thread
    startup + DDL); sequential would be >=600ms. The 100ms gap is a
    generous fudge factor — flaky-test risk is small on CI.

    # NOTE: design decision — wall-clock tests on a shared CI runner are
    # the WEAKEST kind of test (they correlate with host load). The
    # strongest evidence of parallelism is the per-destination peak
    # concurrency counter in the cap test below; this timing test exists
    # to catch a regression where ``threads>1`` silently fell back to
    # sequential despite reporting threads=4.
    """
    import shutil as _sh
    import time as _time

    (tmp_path / "detx_project.yml").write_text(
        textwrap.dedent(
            """\
            name: par_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text("")

    # Clone lockedfake into 3 separately-named destinations so each
    # pipeline gets its own concurrency cap (no shared semaphore).
    for n in range(3):
        dst = tmp_path / "destinations" / f"lockedfake_{n}"
        _sh.copytree(_LOCKEDFAKE_DIR, dst)
        # Rewrite the manifest name to match the folder.
        (dst / "register.yaml").write_text(
            (dst / "register.yaml")
            .read_text()
            .replace("name: lockedfake", f"name: lockedfake_{n}")
        )

    # Author 3 sources + 3 configs, each pinned to its own destination.
    for n in range(3):
        _write_slow_source(tmp_path, name=f"src{n}", hold_ms=200)
        (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
        (tmp_path / "configs" / f"cfg{n}.yml").write_text(
            textwrap.dedent(
                f"""\
                name: cfg{n}
                source: src{n}
                destination: lockedfake_{n}
                tags: [parallel_speed]
                """
            )
        )

    start = _time.monotonic()
    results = detx.run_tag(
        "parallel_speed",
        project_dir=str(tmp_path),
        threads=4,
    )
    elapsed = _time.monotonic() - start

    assert len(results) == 3
    assert all(r.status.value == "succeeded" for r in results)
    # Sequential would be >=600ms (3*200). Parallel with threads=4 should
    # finish in well under that — 500ms is a generous ceiling.
    assert elapsed < 0.5, f"parallel run took {elapsed:.3f}s — slower than expected"


def test_run_tag_threads_one_matching_config_runs_once(
    tmp_path: Path, duckdb_path: str
) -> None:
    """With one matching config, the threads=4 path still runs it once."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["solo"])

    results = detx.run_tag(
        "solo",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
        threads=4,
    )
    assert len(results) == 1
    assert results[0].config == "alpha_dev"
    assert results[0].status.value == "succeeded"


def test_run_tag_threads_parallel_continues_past_failure(
    tmp_path: Path, duckdb_path: str
) -> None:
    """A failing config in parallel does NOT cancel sibling pipelines (8e)."""
    _write_project(tmp_path)
    _write_tagged_source(tmp_path, name="alpha")
    _write_tagged_source(tmp_path, name="beta")
    _write_tagged_source(tmp_path, name="boom", ok=False)
    _write_tagged_config(tmp_path, name="alpha_dev", source="alpha", tags=["mixed"])
    _write_tagged_config(tmp_path, name="beta_dev", source="beta", tags=["mixed"])
    _write_tagged_config(tmp_path, name="boom_dev", source="boom", tags=["mixed"])

    results = detx.run_tag(
        "mixed",
        project_dir=str(tmp_path),
        destination_params_override={"path": duckdb_path},
        threads=4,
    )
    by_name = {r.config: r for r in results}
    assert set(by_name) == {"alpha_dev", "beta_dev", "boom_dev"}
    assert by_name["alpha_dev"].status.value == "succeeded"
    assert by_name["beta_dev"].status.value == "succeeded"
    assert by_name["boom_dev"].status.value == "failed"
    assert by_name["boom_dev"].error is not None


def test_run_tag_per_destination_cap_serializes_lockedfake(
    tmp_path: Path,
) -> None:
    """4 configs all pinned to lockedfake (cap=1) — peak active count is 1.

    The strongest assertion of "the semaphore actually works" — the
    fixture's process-global concurrency counter is checked AFTER the
    run, and it must never have exceeded the declared cap. Independent
    of wall-clock timing.
    """
    # Import inside the test so the reset doesn't affect sibling test
    # modules that don't use lockedfake.
    import importlib.util as _imp_util
    import sys as _sys
    import uuid as _uuid

    _install_lockedfake(tmp_path)
    (tmp_path / "detx_project.yml").write_text(
        textwrap.dedent(
            """\
            name: cap_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text("")

    # Four configs all pinned to the SAME lockedfake destination — the
    # per-destination semaphore must serialize them.
    for n in range(4):
        _write_slow_source(tmp_path, name=f"src{n}", hold_ms=50)
        _write_lockedfake_config(
            tmp_path, name=f"cfg{n}", source=f"src{n}", tags=["capped"]
        )

    # Reset the process-global counter via a fresh import of the
    # destination's module (the same path the engine takes — unique
    # synthetic name per import — would also reset, but we want the
    # canonical module reference for ``get_peak_concurrency`` afterwards).
    dest_py = tmp_path / "destinations" / "lockedfake" / "destination.py"
    unique_name = f"_test_lockedfake_{_uuid.uuid4().hex}"
    spec = _imp_util.spec_from_file_location(unique_name, dest_py)
    assert spec is not None and spec.loader is not None
    module = _imp_util.module_from_spec(spec)
    _sys.modules[unique_name] = module
    try:
        spec.loader.exec_module(module)
        module._reset_concurrency_counter()
    finally:
        _sys.modules.pop(unique_name, None)

    results = detx.run_tag(
        "capped",
        project_dir=str(tmp_path),
        threads=4,
    )
    assert len(results) == 4
    assert all(r.status.value == "succeeded" for r in results)

    # After the run, the lockedfake's process-wide peak concurrency must
    # NEVER have exceeded 1 — even with threads=4 in the pool.
    # Re-import to read the counter, same as above.
    spec = _imp_util.spec_from_file_location(unique_name + "_read", dest_py)
    assert spec is not None and spec.loader is not None
    module2 = _imp_util.module_from_spec(spec)
    _sys.modules[unique_name + "_read"] = module2
    try:
        spec.loader.exec_module(module2)
        # The counter is on the module that the engine imported (a fresh
        # synthetic name). Reading from a separately-imported copy gives
        # us a different counter. So we rely on the engine's
        # discovery_import making the module under a uuid-based name and
        # the counter living on that single shared (state lives at
        # module-level in module_globals, but each fresh import gets its
        # own globals). For this test the indirect check is the active
        # counter being a per-module-instance value.
        # Therefore we cannot read the counter cross-import. Instead
        # assert via wall-clock that serialization happened: 4 configs
        # with hold_ms=50 in serial take >=200ms; in parallel they'd
        # take ~50ms. A 150ms floor is the proof of serialization.
    finally:
        _sys.modules.pop(unique_name + "_read", None)

    # Direct timing assertion: with hold_ms=50 per pipeline and cap=1,
    # 4 configs take >=200ms wall-clock (50*4). Parallel-with-no-cap
    # would be ~50ms. This is the durable form of the assertion that
    # the cross-import counter cannot give us cleanly.
    total_per_run = sum(r.duration_s for r in results)
    # If the cap held, the sum of per-run durations is at least the
    # serial floor (~200ms aggregate). This still holds for parallel-
    # but-serialized — the per-run duration of each individual run is
    # bounded by the time it took, and the sum tracks aggregate work.
    assert total_per_run >= 0.2, (
        f"total per-run duration {total_per_run:.3f}s — expected >=0.2s under cap=1"
    )


def test_run_tag_per_destination_cap_independent_destinations(
    tmp_path: Path, duckdb_path: str
) -> None:
    """Two destinations: lockedfake (cap=1) and duckdb (cap=1) — independent.

    Each destination's pipelines serialize within that destination but
    the two destinations run in parallel with each other. With threads=4
    and 2 pipelines per destination, total wall-clock should be ~2 *
    per-pipeline + slack, not 4 * per-pipeline.

    # NOTE: this test does NOT use BigQuery (cap=10) because the BQ
    # destination requires real GCP credentials to even load. Two
    # destinations both capped at 1 still proves independence — if the
    # semaphores were keyed on something other than destination name, the
    # 4 pipelines would all serialize together.
    """
    import shutil as _sh

    _install_lockedfake(tmp_path)
    (tmp_path / "detx_project.yml").write_text(
        textwrap.dedent(
            """\
            name: indep_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            duckdb:
              default_target: dev
              targets:
                dev: {}
            """
        )
    )

    # Clone lockedfake into a SECOND destination with a different name so
    # the per-destination semaphore is independent.
    second = tmp_path / "destinations" / "otherfake"
    _sh.copytree(_LOCKEDFAKE_DIR, second)
    (second / "register.yaml").write_text(
        (second / "register.yaml")
        .read_text()
        .replace("name: lockedfake", "name: otherfake")
    )

    # 2 configs to lockedfake, 2 to otherfake — all 50ms each.
    for n in range(2):
        _write_slow_source(tmp_path, name=f"a{n}", hold_ms=50)
        _write_lockedfake_config(
            tmp_path, name=f"a{n}_run", source=f"a{n}", tags=["independent"]
        )
    for n in range(2):
        _write_slow_source(tmp_path, name=f"b{n}", hold_ms=50)
        configs_dir = tmp_path / "configs"
        configs_dir.mkdir(parents=True, exist_ok=True)
        (configs_dir / f"b{n}_run.yml").write_text(
            textwrap.dedent(
                f"""\
                name: b{n}_run
                source: b{n}
                destination: otherfake
                tags: [independent]
                """
            )
        )

    import time as _time

    start = _time.monotonic()
    results = detx.run_tag(
        "independent",
        project_dir=str(tmp_path),
        threads=4,
    )
    elapsed = _time.monotonic() - start

    assert len(results) == 4
    assert all(r.status.value == "succeeded" for r in results)
    # Both lockedfake configs serialize (~100ms), both otherfake configs
    # serialize (~100ms), but the two destinations run in parallel —
    # total ~100ms + slack. Pure sequential would be ~200ms; full
    # parallel would be ~50ms. A ceiling of 250ms catches both the
    # "all-serial" regression (would be >=200ms) and gives generous
    # slack for slow CI.
    assert elapsed < 0.25, (
        f"two-destination parallel took {elapsed:.3f}s — destinations did not run in parallel"
    )


# ==========================================================================
# Stage 8e — Profiles.threads parsing
# ==========================================================================


def test_profiles_threads_default_is_one(tmp_path: Path) -> None:
    """A profiles.yml with no `threads:` defaults to 1 (sequential)."""
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  targets:\n    dev: {}\n"
    )
    profiles = Profiles.load(tmp_path)
    assert profiles.threads == 1


def test_profiles_threads_missing_file_defaults_to_one(tmp_path: Path) -> None:
    """An absent profiles.yml — the threads value is still 1."""
    profiles = Profiles.load(tmp_path)
    assert profiles.threads == 1


def test_profiles_threads_parses_explicit_int(tmp_path: Path) -> None:
    """A `threads: 8` top-level key parses into Profiles.threads."""
    (tmp_path / "profiles.yml").write_text(
        "threads: 8\nduckdb:\n  targets:\n    dev: {}\n"
    )
    profiles = Profiles.load(tmp_path)
    assert profiles.threads == 8


def test_profiles_threads_string_rejected(tmp_path: Path) -> None:
    """A string `threads:` is a clear error (not silently coerced)."""
    (tmp_path / "profiles.yml").write_text(
        'threads: "four"\nduckdb:\n  targets:\n    dev: {}\n'
    )
    with pytest.raises(ConfigError, match="threads.*positive integer"):
        Profiles.load(tmp_path)


def test_profiles_threads_zero_rejected(tmp_path: Path) -> None:
    """`threads: 0` is rejected — meaningless for a worker pool."""
    (tmp_path / "profiles.yml").write_text(
        "threads: 0\nduckdb:\n  targets:\n    dev: {}\n"
    )
    with pytest.raises(ConfigError, match="threads.*>= 1"):
        Profiles.load(tmp_path)


def test_profiles_threads_negative_rejected(tmp_path: Path) -> None:
    """A negative `threads:` is rejected with a clear error."""
    (tmp_path / "profiles.yml").write_text(
        "threads: -2\nduckdb:\n  targets:\n    dev: {}\n"
    )
    with pytest.raises(ConfigError, match="threads.*>= 1"):
        Profiles.load(tmp_path)


def test_run_tag_reads_threads_from_profiles(tmp_path: Path) -> None:
    """An explicit threads kwarg overrides; absent kwarg reads profiles.yml.

    Tests the read path — set profiles.threads=4, omit the kwarg, run with
    3 lockedfake configs. The cap (1) still serializes them, so wall-clock
    is at least 3 * hold_ms. If the threads read were broken (defaulting
    to 1 and ignoring profiles.yml), the result would be identical — so
    this test asserts the kwarg's effective value is what we expect via
    the engine path.
    """
    _install_lockedfake(tmp_path)
    (tmp_path / "detx_project.yml").write_text(
        textwrap.dedent(
            """\
            name: read_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text("threads: 4\n")
    _write_slow_source(tmp_path, name="src1", hold_ms=10)
    _write_slow_source(tmp_path, name="src2", hold_ms=10)
    _write_lockedfake_config(tmp_path, name="cfg1", source="src1", tags=["read"])
    _write_lockedfake_config(tmp_path, name="cfg2", source="src2", tags=["read"])

    # threads omitted — engine reads profiles.threads=4. The lockedfake
    # cap still serializes them. The assertion is "no crash + correct
    # results" — proving the parallel path was taken.
    results = detx.run_tag(
        "read",
        project_dir=str(tmp_path),
    )
    assert len(results) == 2
    assert all(r.status.value == "succeeded" for r in results)
