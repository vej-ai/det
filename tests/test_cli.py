"""CLI tests — real ``dtex`` invocations via click's :class:`CliRunner`.

The CLI is a thin shell over the engine, so these tests invoke the command
group exactly as a shell would and assert on exit codes and printed output.

Stage 8.B made *configs* the runtime unit (docs/12). The CLI's primary
selection arg is now ``-p / --conf``; ``dtex list`` and ``dtex validate`` cover
sources, destinations, and configs; ``dtex new`` has source/destination/
config subcommands.

Tests that need a real project copy ``tests/fixtures/`` into ``tmp_path``
(via the ``cli_project`` fixture) so a run never writes into the repo; the
DuckDB file is redirected with ``--destination-param path=...``.
"""

from __future__ import annotations

import shutil
import textwrap
import traceback
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

import dtex
from dtex.cli import cli

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ==========================================================================
# Fixtures
# ==========================================================================


@pytest.fixture
def runner() -> CliRunner:
    """A click test runner that invokes the ``dtex`` command group."""
    return CliRunner()


@pytest.fixture
def cli_project(tmp_path: Path) -> Path:
    """A throwaway copy of ``tests/fixtures/`` — a real, runnable dtex project.

    Copied per test so a ``run`` / ``state reset`` mutates a temp tree, never
    the committed fixture. Returns the project root.
    """
    dst = tmp_path / "project"
    shutil.copytree(FIXTURES_DIR, dst)
    return dst


@pytest.fixture
def warehouse(tmp_path: Path) -> str:
    """A path to a fresh ``.duckdb`` file the CLI's run/state commands target."""
    return str(tmp_path / "warehouse.duckdb")


def _show(result: Result) -> str:
    """Render a CLI result for an assertion message — output + any exception."""
    text = result.output
    if result.exception is not None and not isinstance(result.exception, SystemExit):
        text += "\n" + "".join(
            traceback.format_exception(
                type(result.exception),
                result.exception,
                result.exception.__traceback__,
            )
        )
    return text


# ==========================================================================
# --version / --help
# ==========================================================================


def test_version(runner: CliRunner) -> None:
    """``dtex --version`` prints the package version and exits 0."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, _show(result)
    assert dtex.__version__ in result.output


def test_help_lists_every_command(runner: CliRunner) -> None:
    """``dtex --help`` lists each top-level command group."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0, _show(result)
    for command in ("run", "list", "validate", "init", "new", "state"):
        assert command in result.output


# ==========================================================================
# run
# ==========================================================================


def test_run_echo_succeeds(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run -p echo_dev`` loads the fixture data and exits 0."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "succeeded" in result.output
    assert "9 row(s)" in result.output
    assert "events" in result.output and "items" in result.output


def test_run_incremental_resume(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """A second ``run`` resumes past the committed cursor — items yields 0 rows."""
    args = [
        "run",
        "-p",
        "echo_dev",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    first = runner.invoke(cli, args)
    assert first.exit_code == 0, _show(first)
    second = runner.invoke(cli, args)
    assert second.exit_code == 0, _show(second)
    assert "4 row(s)" in second.output


def test_run_full_refresh(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--full-refresh`` re-extracts the incremental stream past its cursor."""
    args = [
        "run",
        "-p",
        "echo_dev",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    runner.invoke(cli, args)
    refreshed = runner.invoke(cli, [*args, "--full-refresh"])
    assert refreshed.exit_code == 0, _show(refreshed)
    assert "9 row(s)" in refreshed.output


def test_run_select_single_stream(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--select`` restricts the run to the named stream(s)."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-p",
            "echo_dev",
            "--select",
            "events",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "4 row(s)" in result.output
    assert "skip" in result.output


def test_run_unknown_config_exits_1_no_traceback(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run`` of an unknown config exits 1 with a clean message, no traceback."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-p",
            "does_not_exist",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 1, _show(result)
    assert "FAILED" in result.output
    assert "does_not_exist" in result.output
    assert "Traceback" not in result.output


def test_run_requires_config(runner: CliRunner, cli_project: Path) -> None:
    """``run`` with no -p/--conf is a click usage error."""
    result = runner.invoke(cli, ["run", "--project-dir", str(cli_project)])
    assert result.exit_code == 2, _show(result)


# ==========================================================================
# run --tag (stage 8d)
# ==========================================================================


def test_run_tag_succeeds_shows_summary(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run --tag test`` runs both fixture configs + prints the multi-run summary."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    out = result.output
    # Both configs ran (their per-run output + the rollup mentions them).
    assert "echo_dev" in out
    assert "echo_prod" in out
    # The multi-run summary header carries the tag and the totals.
    assert "TAG test:" in out
    assert "2 config(s)" in out
    assert "2 succeeded" in out
    # Per-config status row in the summary table.
    assert "succeeded" in out


def test_run_tag_zero_matches_exits_2(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run --tag <unmatched>`` exits 2 with a clear "no configs match" message."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "no_such_tag_anywhere",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 2, _show(result)
    assert "no configs match tag" in result.output


def test_run_tag_failure_exits_1_with_summary(
    runner: CliRunner, tmp_path: Path, warehouse: str
) -> None:
    """A failure among successes exits 1 and the summary names the failed config."""
    # Build a tmp project with two tagged configs — one ok, one that raises.
    (tmp_path / "dtex_project.yml").write_text(
        textwrap.dedent(
            """\
            name: tag_fail_proj
            version: "1.0.0"
            source_paths: [sources]
            destination_paths: [destinations]
            config_paths: [configs]
            """
        )
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev: {}\n"
    )
    # Good source.
    good = tmp_path / "sources" / "good"
    good.mkdir(parents=True)
    (good / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: good
            kind: source
            version: "1.0.0"
            streams:
              - name: rows
                table: good_rows
                schema: [{name: id, type: INTEGER}]
            """
        )
    )
    (good / "source.py").write_text(
        textwrap.dedent(
            """\
            from dtex import Batch, stream
            from collections.abc import Iterator

            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}]
            """
        )
    )
    # Bad source.
    bad = tmp_path / "sources" / "bad"
    bad.mkdir(parents=True)
    (bad / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: bad
            kind: source
            version: "1.0.0"
            streams:
              - name: rows
                table: bad_rows
                schema: [{name: id, type: INTEGER}]
            """
        )
    )
    (bad / "source.py").write_text(
        textwrap.dedent(
            """\
            from dtex import Batch, stream
            from collections.abc import Iterator

            @stream(name="rows")
            def rows() -> Iterator[Batch]:
                yield [{"id": 1}]
                raise RuntimeError("boom")
            """
        )
    )
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir()
    (configs_dir / "configs.yml").write_text(
        textwrap.dedent(
            """\
            configs:
              - name: good_run
                source: good
                destination: duckdb
                target: dev
                streams: all
                tags: [mixed]
              - name: bad_run
                source: bad
                destination: duckdb
                target: dev
                streams: all
                tags: [mixed]
            """
        )
    )

    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "mixed",
            "--project-dir",
            str(tmp_path),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 1, _show(result)
    out = result.output
    assert "TAG mixed:" in out
    assert "1 succeeded" in out
    assert "1 failed" in out
    assert "bad_run" in out
    assert "good_run" in out


def test_run_tag_and_conf_mutually_exclusive(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """Both -p and --tag → usage error (exit 2)."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-p",
            "echo_dev",
            "--tag",
            "test",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 2, _show(result)
    assert "mutually exclusive" in result.output


def test_run_tag_param_combo_rejected(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """--param with --tag is a usage error (silent-apply footgun)."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--param",
            "page_size=100",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 2, _show(result)
    assert "--param is not supported with --tag" in result.output


# ==========================================================================
# list
# ==========================================================================


def test_list_shows_all_kinds(runner: CliRunner, cli_project: Path) -> None:
    """``list`` shows sources, destinations, and configs in three sections."""
    result = runner.invoke(cli, ["list", "--project-dir", str(cli_project)])
    assert result.exit_code == 0, _show(result)
    assert "SOURCES" in result.output
    assert "DESTINATIONS" in result.output
    assert "CONFIGS" in result.output
    assert "echo" in result.output
    assert "duckdb" in result.output
    assert "echo_dev" in result.output


def test_list_kind_source(runner: CliRunner, cli_project: Path) -> None:
    """``list --kind source`` restricts to sources."""
    result = runner.invoke(
        cli, ["list", "--kind", "source", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "SOURCES" in result.output
    assert "DESTINATIONS" not in result.output
    assert "CONFIGS" not in result.output


def test_list_kind_destination(runner: CliRunner, cli_project: Path) -> None:
    """``list --kind destination`` restricts to destinations."""
    result = runner.invoke(
        cli, ["list", "--kind", "destination", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "DESTINATIONS" in result.output
    assert "SOURCES" not in result.output


def test_list_kind_config(runner: CliRunner, cli_project: Path) -> None:
    """``list --kind config`` restricts to pipeline configs."""
    result = runner.invoke(
        cli, ["list", "--kind", "config", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "CONFIGS" in result.output
    assert "echo_dev" in result.output


def test_list_tag_filters_configs(
    runner: CliRunner, cli_project: Path
) -> None:
    """``list --tag hourly`` shows only the configs tagged hourly (echo_dev only)."""
    result = runner.invoke(
        cli,
        ["list", "--tag", "hourly", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 0, _show(result)
    out = result.output
    # echo_dev is tagged [test, hourly] in the fixture; echo_prod is [test, daily].
    assert "echo_dev" in out
    assert "echo_prod" not in out


def test_list_tag_no_match_per_section_placeholders(
    runner: CliRunner, cli_project: Path
) -> None:
    """``list --tag <unmatched>`` shows the per-section "no match" placeholder."""
    result = runner.invoke(
        cli,
        ["list", "--tag", "no_such_tag_anywhere", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 0, _show(result)
    out = result.output
    # Each section still appears, but with the per-section placeholder.
    assert "SOURCES" in out
    assert "DESTINATIONS" in out
    assert "CONFIGS" in out
    assert "no sources match tag" in out
    assert "no destinations match tag" in out
    assert "no configs match tag" in out


def test_list_shows_tags_column_for_configs(
    runner: CliRunner, cli_project: Path
) -> None:
    """The CONFIGS table now carries a TAGS column with each config's tags."""
    result = runner.invoke(
        cli, ["list", "--kind", "config", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    out = result.output
    assert "TAGS" in out
    assert "hourly" in out and "daily" in out


# ==========================================================================
# validate
# ==========================================================================


def test_validate_all_ok(runner: CliRunner, cli_project: Path) -> None:
    """``validate`` passes a clean project's sources, destinations, configs."""
    result = runner.invoke(cli, ["validate", "--project-dir", str(cli_project)])
    assert result.exit_code == 0, _show(result)
    assert "echo" in result.output
    assert "duckdb" in result.output
    assert "echo_dev" in result.output
    assert "valid" in result.output


def test_validate_bad_source_fails(runner: CliRunner, cli_project: Path) -> None:
    """``validate`` exits non-zero on a source that fails discovery."""
    bad = cli_project / "sources" / "broken"
    bad.mkdir()
    (bad / "register.yaml").write_text(
        textwrap.dedent(
            """\
            name: broken
            kind: source
            streams:
              - name: ghost
                table: ghost_table
            """
        )
    )
    (bad / "source.py").write_text(
        "# no @stream function — 'ghost' is an orphan declaration.\n"
    )
    result = runner.invoke(cli, ["validate", "--project-dir", str(cli_project)])
    assert result.exit_code == 1, _show(result)
    assert "FAIL" in result.output
    assert "broken" in result.output


def test_validate_config_missing_source_fails(
    runner: CliRunner, cli_project: Path
) -> None:
    """``validate`` flags a config that names a non-existent source."""
    (cli_project / "configs" / "ghost.yml").write_text(
        textwrap.dedent(
            """\
            name: ghost_pipe
            source: ghost_source
            destination: duckdb
            target: dev
            streams: all
            """
        )
    )
    result = runner.invoke(cli, ["validate", "--project-dir", str(cli_project)])
    assert result.exit_code == 1, _show(result)
    assert "ghost_source" in result.output


# ==========================================================================
# init
# ==========================================================================


def test_init_scaffolds_project(runner: CliRunner, tmp_path: Path) -> None:
    """``init`` writes a complete project tree into the target directory."""
    target = tmp_path / "fresh_project"
    result = runner.invoke(cli, ["init", str(target)])
    assert result.exit_code == 0, _show(result)
    assert (target / "dtex_project.yml").is_file()
    assert (target / "profiles.yml").is_file()
    assert (target / ".gitignore").is_file()
    assert (target / "README.md").is_file()
    assert (target / "sources").is_dir()
    assert (target / "destinations").is_dir()
    assert (target / "configs").is_dir()
    assert (target / "configs" / "example.yml").is_file()


def test_init_refuses_to_clobber(runner: CliRunner, tmp_path: Path) -> None:
    """``init`` refuses to overwrite an existing project unless --force."""
    target = tmp_path / "existing"
    first = runner.invoke(cli, ["init", str(target)])
    assert first.exit_code == 0, _show(first)
    second = runner.invoke(cli, ["init", str(target)])
    assert second.exit_code == 2, _show(second)
    assert "already exists" in second.output


def test_init_force_overwrites(runner: CliRunner, tmp_path: Path) -> None:
    """``init --force`` overwrites an existing project."""
    target = tmp_path / "existing"
    runner.invoke(cli, ["init", str(target)])
    forced = runner.invoke(cli, ["init", str(target), "--force"])
    assert forced.exit_code == 0, _show(forced)


def test_init_scaffolds_runnable_project(runner: CliRunner, tmp_path: Path) -> None:
    """A scaffolded project's dtex_project.yml is a valid, parseable project."""
    target = tmp_path / "p"
    runner.invoke(cli, ["init", str(target)])
    result = runner.invoke(cli, ["list", "--project-dir", str(target)])
    assert result.exit_code == 0, _show(result)


def test_scaffold_chain_validates_clean(runner: CliRunner, tmp_path: Path) -> None:
    """init → new source → new config → validate exits 0 without any edits.

    This is the streams-redesign-plan §4.3.4 acceptance test. If a template
    drifts from the schema (e.g. a config scaffold that forgets to declare
    `streams:`, or a source scaffold whose stream name conflicts with the
    seeded example config), this chain breaks and the test catches it.
    """
    project = tmp_path / "p"
    init_result = runner.invoke(cli, ["init", str(project)])
    assert init_result.exit_code == 0, _show(init_result)
    new_src = runner.invoke(
        cli, ["new", "source", "my_source", "--project-dir", str(project)]
    )
    assert new_src.exit_code == 0, _show(new_src)
    new_cfg = runner.invoke(
        cli, ["new", "config", "my_pipeline", "--project-dir", str(project)]
    )
    assert new_cfg.exit_code == 0, _show(new_cfg)
    validate = runner.invoke(cli, ["validate", "--project-dir", str(project)])
    assert validate.exit_code == 0, _show(validate)


def test_init_default_profiles_has_only_duckdb(
    runner: CliRunner, tmp_path: Path
) -> None:
    """Default ``dtex init`` scaffolds only the DuckDB block in profiles.yml."""
    target = tmp_path / "p"
    runner.invoke(cli, ["init", str(target)])
    text = (target / "profiles.yml").read_text()
    assert "\nduckdb:\n" in text
    assert "\nbigquery:\n" not in text


def test_init_with_bigquery_scaffolds_block(
    runner: CliRunner, tmp_path: Path
) -> None:
    """``dtex init --with bigquery`` adds a BigQuery block alongside DuckDB.

    The scaffolded block must NOT include a `credentials_path:` field — the
    default `auth_type: oauth` uses ADC and needs no path.
    """
    target = tmp_path / "p"
    result = runner.invoke(cli, ["init", str(target), "--with", "bigquery"])
    assert result.exit_code == 0, _show(result)
    text = (target / "profiles.yml").read_text()
    assert "\nduckdb:\n" in text
    assert "\nbigquery:\n" in text
    # Parse the YAML and assert on the structure — substring checks would
    # false-positive on comment text mentioning the SA path.
    import yaml

    parsed = yaml.safe_load(text)
    bq_target = parsed["bigquery"]["targets"]["dev"]
    assert bq_target["auth_type"] == "oauth"
    assert "credentials_path" not in bq_target


def test_init_with_unknown_destination_errors(
    runner: CliRunner, tmp_path: Path
) -> None:
    """An unknown ``--with`` name fails with a clean error listing valid names."""
    target = tmp_path / "p"
    result = runner.invoke(cli, ["init", str(target), "--with", "frobnicate"])
    assert result.exit_code != 0
    assert "frobnicate" in result.output
    assert "bigquery" in result.output  # valid options listed


def test_init_with_repeated_dedupes(runner: CliRunner, tmp_path: Path) -> None:
    """``--with bigquery --with bigquery`` writes the bigquery block once."""
    target = tmp_path / "p"
    runner.invoke(
        cli, ["init", str(target), "--with", "bigquery", "--with", "bigquery"]
    )
    text = (target / "profiles.yml").read_text()
    assert text.count("\nbigquery:\n") == 1


def test_init_with_duckdb_explicit_is_noop(
    runner: CliRunner, tmp_path: Path
) -> None:
    """Passing ``--with duckdb`` is allowed but doesn't double the block."""
    target = tmp_path / "p"
    runner.invoke(cli, ["init", str(target), "--with", "duckdb"])
    text = (target / "profiles.yml").read_text()
    assert text.count("\nduckdb:\n") == 1


# ==========================================================================
# new <source|destination|config>
# ==========================================================================


def test_new_source_scaffolds(runner: CliRunner, tmp_path: Path) -> None:
    """``new source <name>`` scaffolds ``sources/<name>/``.

    Stage 11: also emits an ``__init__.py`` marker so the folder is an
    explicit Python package and ``source.py`` can use relative imports for
    sibling helpers (``from .client import X``).
    """
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    result = runner.invoke(
        cli, ["new", "source", "my_src", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, _show(result)
    folder = project / "sources" / "my_src"
    assert (folder / "register.yaml").is_file()
    assert (folder / "source.py").is_file()
    assert (folder / "__init__.py").is_file()


def test_new_destination_scaffolds(runner: CliRunner, tmp_path: Path) -> None:
    """``new destination <name>`` scaffolds ``destinations/<name>/``.

    Stage 11: also emits an ``__init__.py`` marker so the folder is an
    explicit Python package (a sibling ``ddl.py`` / ``client.py`` can be
    imported with ``from .ddl import X``).
    """
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    result = runner.invoke(
        cli, ["new", "destination", "my_dst", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, _show(result)
    folder = project / "destinations" / "my_dst"
    assert (folder / "register.yaml").is_file()
    assert (folder / "destination.py").is_file()
    assert (folder / "__init__.py").is_file()


def test_new_config_scaffolds(runner: CliRunner, tmp_path: Path) -> None:
    """``new config <name>`` scaffolds ``configs/<name>.yml``."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    result = runner.invoke(
        cli, ["new", "config", "my_pipeline", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, _show(result)
    assert (project / "configs" / "my_pipeline.yml").is_file()


def test_new_source_is_validatable(runner: CliRunner, tmp_path: Path) -> None:
    """A scaffolded source passes ``dtex validate`` out of the box."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "source", "fresh", "--project-dir", str(project)])
    # The scaffolded example.yml config still references the placeholder
    # 'my_source' — delete it so validate doesn't fail on the dangling config.
    (project / "configs" / "example.yml").unlink()
    result = runner.invoke(cli, ["validate", "--project-dir", str(project)])
    assert result.exit_code == 0, _show(result)


def test_new_source_refuses_existing(runner: CliRunner, tmp_path: Path) -> None:
    """``new source`` refuses to overwrite an existing source folder."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "source", "dup", "--project-dir", str(project)])
    again = runner.invoke(
        cli, ["new", "source", "dup", "--project-dir", str(project)]
    )
    assert again.exit_code == 2, _show(again)
    assert "already exists" in again.output


def test_new_config_refuses_existing(runner: CliRunner, tmp_path: Path) -> None:
    """``new config`` refuses to overwrite an existing config file."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "config", "dup", "--project-dir", str(project)])
    again = runner.invoke(
        cli, ["new", "config", "dup", "--project-dir", str(project)]
    )
    assert again.exit_code == 2, _show(again)
    assert "already exists" in again.output


# ==========================================================================
# state
# ==========================================================================


def test_state_list_after_run(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state list`` shows committed cursor rows after a run."""
    runner.invoke(
        cli,
        [
            "run",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "state",
            "list",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "items" in result.output
    assert "events" in result.output


def test_state_list_no_state(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state list`` on a never-run config reports no committed state."""
    result = runner.invoke(
        cli,
        [
            "state",
            "list",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "no committed state" in result.output


def test_state_reset_clears_cursor(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state reset`` clears state so the next run re-extracts everything."""
    run_args = [
        "run",
        "-p",
        "echo_dev",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    runner.invoke(cli, run_args)
    second = runner.invoke(cli, run_args)
    assert "4 row(s)" in second.output

    reset = runner.invoke(
        cli,
        [
            "state",
            "reset",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert reset.exit_code == 0, _show(reset)
    assert "cleared" in reset.output

    after = runner.invoke(cli, run_args)
    assert after.exit_code == 0, _show(after)
    assert "9 row(s)" in after.output


def test_state_reset_single_stream(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state reset --stream`` clears just one stream's cursor."""
    run_args = [
        "run",
        "-p",
        "echo_dev",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    runner.invoke(cli, run_args)
    reset = runner.invoke(
        cli,
        [
            "state",
            "reset",
            "-p",
            "echo_dev",
            "--stream",
            "items",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert reset.exit_code == 0, _show(reset)
    assert "items" in reset.output
    after = runner.invoke(cli, run_args)
    assert "9 row(s)" in after.output


def test_state_reset_never_run_is_clean(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state reset`` on a config that never ran is a clean no-op."""
    result = runner.invoke(
        cli,
        [
            "state",
            "reset",
            "-p",
            "echo_dev",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "0 cursor row(s) cleared" in result.output


# ==========================================================================
# Stage 8e — `dtex run --threads N`
# ==========================================================================


def test_run_tag_threads_flag_passes_through(
    runner: CliRunner, cli_project: Path, warehouse: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``run --tag test --threads 4`` reaches ``run_tag(threads=4)``."""
    captured: dict[str, object] = {}

    real_run_tag = dtex.run_tag

    def _spy_run_tag(*args, **kwargs):  # type: ignore[no-untyped-def]
        captured["args"] = args
        captured["kwargs"] = kwargs
        return real_run_tag(*args, **kwargs)

    monkeypatch.setattr(dtex, "run_tag", _spy_run_tag)
    # The CLI module references ``dtex.run_tag`` via the ``dtex`` package, so
    # monkeypatching on the package surface is sufficient (no second
    # patch on dtex.cli needed).

    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--threads",
            "4",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert captured["kwargs"].get("threads") == 4  # type: ignore[union-attr]


def test_run_single_config_threads_ignored(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run -p X --threads 4`` is accepted (debug-logged ignore), still succeeds."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-p",
            "echo_dev",
            "--threads",
            "4",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    # The run itself still succeeds — --threads is silently ignored with -p.
    assert "succeeded" in result.output


def test_run_threads_zero_rejected(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--threads 0`` is a click range error → exit 2."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--threads",
            "0",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 2, _show(result)


def test_run_threads_negative_rejected(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--threads -2`` is a click range error → exit 2."""
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--threads",
            "-2",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 2, _show(result)


def test_run_tag_parallel_output_has_progress_lines(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run --tag … --threads 4`` against 2 DuckDB configs prints parallel banners.

    DuckDB's max_concurrent_writes=1 serializes them, but the live "▸
    starting" / "✓ done" banners still print (they're a function of the
    parallel branch, not the cap).
    """
    result = runner.invoke(
        cli,
        [
            "run",
            "--tag",
            "test",
            "--threads",
            "4",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    out = result.output
    # Both per-pipeline progress banners landed.
    assert "starting echo_dev" in out
    assert "starting echo_prod" in out
    assert "done echo_dev" in out
    assert "done echo_prod" in out
    # And the rollup table still printed last.
    assert "TAG test:" in out


# ==========================================================================
# dtex secrets test — stage 9a
# ==========================================================================


@pytest.fixture
def _isolate_resolvers() -> Iterator[None]:
    """Wipe the secrets module registry around CLI secrets tests."""
    from dtex.secrets import _reset_resolvers_for_testing

    _reset_resolvers_for_testing()
    try:
        yield
    finally:
        _reset_resolvers_for_testing()


def test_secrets_help_shows_test(runner: CliRunner) -> None:
    """``dtex secrets --help`` lists the ``test`` subcommand."""
    result = runner.invoke(cli, ["secrets", "--help"])
    assert result.exit_code == 0, _show(result)
    assert "test" in result.output


def test_secrets_test_no_references_exits_zero(
    runner: CliRunner, cli_project: Path, _isolate_resolvers: None
) -> None:
    """The echo fixture declares no secrets — output reports that, exit 0."""
    result = runner.invoke(
        cli,
        ["secrets", "test", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 0, _show(result)
    assert "no secret references" in result.output


def test_secrets_test_env_var_resolves(
    runner: CliRunner,
    cli_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    _isolate_resolvers: None,
) -> None:
    """A source with a ``${env.X}`` secret resolves cleanly when the env var
    is set; the printed line includes ✓ and the reference URL, never the
    resolved value."""
    monkeypatch.setenv("MY_SECRET_VALUE", "super-secret-credential-xyz")
    # Author a source with a single env-var secret. The source's @stream
    # implementation can be a no-op — secrets test only walks register.yaml.
    src_dir = cli_project / "sources" / "with_secret"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: with_secret
            kind: source
            version: "1.0.0"
            secrets:
              - name: api_token
                ref: ${env.MY_SECRET_VALUE}
            streams:
              - name: things
                table: things
                schema:
                  - {name: id, type: INTEGER}
            """
        ).strip()
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from dtex import stream

            @stream(name="things")
            def things():
                yield []
            """
        ).strip()
    )
    # And a config pointing at it.
    (cli_project / "configs" / "with_secret.yml").write_text(
        textwrap.dedent(
            """
            name: with_secret_cfg
            source: with_secret
            destination: duckdb
            target: dev
            streams: all
            """
        ).strip()
    )

    result = runner.invoke(
        cli,
        ["secrets", "test", "-p", "with_secret_cfg", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 0, _show(result)
    assert "✓" in result.output
    assert "${env.MY_SECRET_VALUE}" in result.output
    # The resolved value MUST NOT appear anywhere in the output.
    assert "super-secret-credential-xyz" not in result.output


def test_secrets_test_missing_env_var_fails(
    runner: CliRunner,
    cli_project: Path,
    monkeypatch: pytest.MonkeyPatch,
    _isolate_resolvers: None,
) -> None:
    """A missing env var surfaces as ✗ + a message; exit 1."""
    monkeypatch.delenv("MY_MISSING_VAR", raising=False)
    src_dir = cli_project / "sources" / "with_missing"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: with_missing
            kind: source
            version: "1.0.0"
            secrets:
              - name: api_token
                ref: ${env.MY_MISSING_VAR}
            streams:
              - name: things
                table: things
                schema:
                  - {name: id, type: INTEGER}
            """
        ).strip()
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from dtex import stream

            @stream(name="things")
            def things():
                yield []
            """
        ).strip()
    )
    (cli_project / "configs" / "with_missing.yml").write_text(
        textwrap.dedent(
            """
            name: with_missing_cfg
            source: with_missing
            destination: duckdb
            target: dev
            streams: all
            """
        ).strip()
    )

    result = runner.invoke(
        cli,
        ["secrets", "test", "-p", "with_missing_cfg", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 1, _show(result)
    assert "✗" in result.output
    assert "MY_MISSING_VAR" in result.output


def test_secrets_test_unknown_scheme_fails(
    runner: CliRunner, cli_project: Path, _isolate_resolvers: None
) -> None:
    """A ``secret://unknown/...`` ref fails with ✗ + exit 1; value never printed."""
    src_dir = cli_project / "sources" / "with_url"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: with_url
            kind: source
            version: "1.0.0"
            secrets:
              - name: api_token
                ref: secret://unknown-scheme/projects/x/secrets/y
            streams:
              - name: things
                table: things
                schema:
                  - {name: id, type: INTEGER}
            """
        ).strip()
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from dtex import stream

            @stream(name="things")
            def things():
                yield []
            """
        ).strip()
    )
    (cli_project / "configs" / "with_url.yml").write_text(
        textwrap.dedent(
            """
            name: with_url_cfg
            source: with_url
            destination: duckdb
            target: dev
            streams: all
            """
        ).strip()
    )

    result = runner.invoke(
        cli,
        ["secrets", "test", "-p", "with_url_cfg", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 1, _show(result)
    assert "✗" in result.output
    assert "unknown-scheme" in result.output
    assert "no resolver" in result.output


def test_secrets_test_secret_url_with_project_plugin(
    runner: CliRunner, cli_project: Path, _isolate_resolvers: None
) -> None:
    """A ``dtex_plugins.py`` registering a ``secret://`` scheme makes that
    scheme resolvable from ``dtex secrets test``."""
    (cli_project / "dtex_plugins.py").write_text(
        textwrap.dedent(
            """
            from typing import ClassVar
            import dtex

            class P:
                scheme: ClassVar[str] = 'plugin'
                def resolve(self, path, field):
                    return f'val-for-{path}'

            dtex.register_secret_resolver('plugin', P)
            """
        ).strip()
    )
    src_dir = cli_project / "sources" / "plugin_user"
    src_dir.mkdir(parents=True)
    (src_dir / "register.yaml").write_text(
        textwrap.dedent(
            """
            name: plugin_user
            kind: source
            version: "1.0.0"
            secrets:
              - name: api_token
                ref: secret://plugin/some/path
            streams:
              - name: things
                table: things
                schema:
                  - {name: id, type: INTEGER}
            """
        ).strip()
    )
    (src_dir / "source.py").write_text(
        textwrap.dedent(
            """
            from dtex import stream

            @stream(name="things")
            def things():
                yield []
            """
        ).strip()
    )
    (cli_project / "configs" / "plugin_user.yml").write_text(
        textwrap.dedent(
            """
            name: plugin_user_cfg
            source: plugin_user
            destination: duckdb
            target: dev
            streams: all
            """
        ).strip()
    )
    result = runner.invoke(
        cli,
        ["secrets", "test", "-p", "plugin_user_cfg", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 0, _show(result)
    assert "✓" in result.output
    # The reference URL is printed; the resolved value is not.
    assert "secret://plugin/some/path" in result.output
    assert "val-for-some/path" not in result.output
