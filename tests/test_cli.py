"""CLI tests — real ``det`` invocations via click's :class:`CliRunner`.

The CLI is a thin shell over the engine, so these tests invoke the command
group exactly as a shell would and assert on exit codes and printed output:

* ``run`` against the ``echo`` fixture succeeds (exit 0) and loads rows;
* ``run`` of an unknown connector exits 1 with a clean message, no traceback;
* ``list`` shows the ``echo`` connector;
* ``validate`` passes a good connector and fails a malformed one;
* ``init`` scaffolds a project and refuses to clobber one;
* ``new connector`` scaffolds a folder;
* ``state list`` shows committed cursors after a run, and ``state reset``
  clears them so the next run re-extracts;
* ``--version`` prints the version.

Tests that need a real project copy ``tests/fixtures/`` into ``tmp_path`` (via
the ``cli_project`` fixture) so a run never writes into the repo; the DuckDB
file is redirected with ``--destination-param path=...``.
"""

from __future__ import annotations

import shutil
import textwrap
import traceback
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

import det
from det.cli import cli

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


# ==========================================================================
# Fixtures
# ==========================================================================


@pytest.fixture
def runner() -> CliRunner:
    """A click test runner that invokes the ``det`` command group."""
    return CliRunner()


@pytest.fixture
def cli_project(tmp_path: Path) -> Path:
    """A throwaway copy of ``tests/fixtures/`` — a real, runnable det project.

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
    """``det --version`` prints the package version and exits 0."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0, _show(result)
    assert det.__version__ in result.output


def test_help_lists_every_command(runner: CliRunner) -> None:
    """``det --help`` lists all seven command groups."""
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
    """``run -c echo`` loads the fixture data and exits 0."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-c",
            "echo",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "succeeded" in result.output
    # echo: 4 events + 5 items on a first run.
    assert "9 row(s)" in result.output
    assert "events" in result.output and "items" in result.output


def test_run_incremental_resume(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """A second ``run`` resumes past the committed cursor — items yields 0 rows."""
    args = [
        "run",
        "-c",
        "echo",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    first = runner.invoke(cli, args)
    assert first.exit_code == 0, _show(first)
    second = runner.invoke(cli, args)
    assert second.exit_code == 0, _show(second)
    # Re-run: events re-appends (4), items resumes from cursor 5 and yields 0.
    assert "4 row(s)" in second.output


def test_run_full_refresh(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--full-refresh`` re-extracts the incremental stream past its cursor."""
    args = [
        "run",
        "-c",
        "echo",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    runner.invoke(cli, args)
    refreshed = runner.invoke(cli, [*args, "--full-refresh"])
    assert refreshed.exit_code == 0, _show(refreshed)
    # full-refresh ignores the cursor: items yields all 5 again → 9 total.
    assert "9 row(s)" in refreshed.output


def test_run_select_single_stream(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``--select`` restricts the run to the named stream(s)."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-c",
            "echo",
            "--select",
            "events",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    # Only events ran (4 rows); items was skipped.
    assert "4 row(s)" in result.output
    assert "skip" in result.output


def test_run_bad_connector_exits_1_no_traceback(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``run`` of an unknown connector exits 1 with a clean message, no traceback."""
    result = runner.invoke(
        cli,
        [
            "run",
            "-c",
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
    # No Python traceback leaked to the user.
    assert "Traceback" not in result.output


def test_run_requires_connector_or_tag(runner: CliRunner, cli_project: Path) -> None:
    """``run`` with neither --connector nor --tag is a usage error (exit 2)."""
    result = runner.invoke(cli, ["run", "--project-dir", str(cli_project)])
    assert result.exit_code == 2, _show(result)
    assert "exactly one" in result.output


def test_run_connector_and_tag_mutually_exclusive(
    runner: CliRunner, cli_project: Path
) -> None:
    """``run`` rejects passing both --connector and --tag."""
    result = runner.invoke(
        cli,
        ["run", "-c", "echo", "--tag", "test", "--project-dir", str(cli_project)],
    )
    assert result.exit_code == 2, _show(result)


def test_run_by_tag(runner: CliRunner, cli_project: Path, warehouse: str) -> None:
    """``run --tag`` runs every connector carrying the tag (echo has tag 'test')."""
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
    assert "echo" in result.output


# ==========================================================================
# list
# ==========================================================================


def test_list_shows_echo(runner: CliRunner, cli_project: Path) -> None:
    """``list`` shows the echo connector with its kind and streams."""
    result = runner.invoke(cli, ["list", "--project-dir", str(cli_project)])
    assert result.exit_code == 0, _show(result)
    assert "echo" in result.output
    assert "source" in result.output
    assert "events" in result.output and "items" in result.output
    # The baked duckdb destination is discoverable too.
    assert "duckdb" in result.output


def test_list_tag_filter(runner: CliRunner, cli_project: Path) -> None:
    """``list --tag`` filters to connectors carrying the tag."""
    result = runner.invoke(
        cli, ["list", "--tag", "fixture", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "echo" in result.output
    # duckdb does not carry the 'fixture' tag.
    assert "duckdb" not in result.output


def test_list_tag_no_match(runner: CliRunner, cli_project: Path) -> None:
    """``list --tag`` with no match prints a clear "nothing found" line."""
    result = runner.invoke(
        cli, ["list", "--tag", "nonsuch", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "no connectors found" in result.output


# ==========================================================================
# validate
# ==========================================================================


def test_validate_good_connector(runner: CliRunner, cli_project: Path) -> None:
    """``validate -c echo`` passes the well-formed fixture connector."""
    result = runner.invoke(
        cli, ["validate", "-c", "echo", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 0, _show(result)
    assert "echo" in result.output
    assert "valid" in result.output


def test_validate_all(runner: CliRunner, cli_project: Path) -> None:
    """``validate`` with no -c validates every discoverable connector."""
    result = runner.invoke(cli, ["validate", "--project-dir", str(cli_project)])
    assert result.exit_code == 0, _show(result)
    assert "echo" in result.output
    assert "duckdb" in result.output


def test_validate_bad_connector_fails(runner: CliRunner, cli_project: Path) -> None:
    """``validate`` exits non-zero on a connector that fails discovery."""
    # A malformed connector: declares a stream in register.yaml with no
    # matching @stream function — a docs/03 §7 coverage failure.
    bad = cli_project / "connectors" / "broken"
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
    result = runner.invoke(
        cli, ["validate", "-c", "broken", "--project-dir", str(cli_project)]
    )
    assert result.exit_code == 1, _show(result)
    assert "FAIL" in result.output
    assert "broken" in result.output


# ==========================================================================
# init
# ==========================================================================


def test_init_scaffolds_project(runner: CliRunner, tmp_path: Path) -> None:
    """``init`` writes a complete project tree into the target directory."""
    target = tmp_path / "fresh_project"
    result = runner.invoke(cli, ["init", str(target)])
    assert result.exit_code == 0, _show(result)
    assert (target / "det_project.yml").is_file()
    assert (target / "profiles.yml").is_file()
    assert (target / ".gitignore").is_file()
    assert (target / "README.md").is_file()
    assert (target / "connectors").is_dir()
    assert (target / "destinations").is_dir()


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
    """A scaffolded project's det_project.yml is a valid, parseable project."""
    target = tmp_path / "p"
    runner.invoke(cli, ["init", str(target)])
    # `list` walks up for det_project.yml — it succeeds on the scaffold.
    result = runner.invoke(cli, ["list", "--project-dir", str(target)])
    assert result.exit_code == 0, _show(result)


# ==========================================================================
# new connector
# ==========================================================================


def test_new_connector_scaffolds_source(runner: CliRunner, tmp_path: Path) -> None:
    """``new connector`` scaffolds a source folder under connectors/."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    result = runner.invoke(
        cli, ["new", "connector", "my_src", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, _show(result)
    folder = project / "connectors" / "my_src"
    assert (folder / "register.yaml").is_file()
    assert (folder / "source.py").is_file()


def test_new_connector_destination_kind(runner: CliRunner, tmp_path: Path) -> None:
    """``new connector --kind destination`` scaffolds under destinations/."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    result = runner.invoke(
        cli,
        [
            "new",
            "connector",
            "my_dest",
            "--kind",
            "destination",
            "--project-dir",
            str(project),
        ],
    )
    assert result.exit_code == 0, _show(result)
    folder = project / "destinations" / "my_dest"
    assert (folder / "register.yaml").is_file()
    assert (folder / "destination.py").is_file()


def test_new_connector_is_validatable(runner: CliRunner, tmp_path: Path) -> None:
    """A scaffolded source connector passes ``det validate`` out of the box."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "connector", "fresh", "--project-dir", str(project)])
    result = runner.invoke(
        cli, ["validate", "-c", "fresh", "--project-dir", str(project)]
    )
    assert result.exit_code == 0, _show(result)


def test_new_connector_refuses_existing(runner: CliRunner, tmp_path: Path) -> None:
    """``new connector`` refuses to overwrite an existing connector folder."""
    project = tmp_path / "proj"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "connector", "dup", "--project-dir", str(project)])
    again = runner.invoke(
        cli, ["new", "connector", "dup", "--project-dir", str(project)]
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
            "-c",
            "echo",
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
            "-c",
            "echo",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    # echo's items stream commits an int cursor (max updated_at == 5).
    assert "items" in result.output
    assert "events" in result.output


def test_state_list_no_state(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state list`` on a never-run connector reports no committed state."""
    result = runner.invoke(
        cli,
        [
            "state",
            "list",
            "-c",
            "echo",
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
        "-c",
        "echo",
        "--project-dir",
        str(cli_project),
        "--destination-param",
        f"path={warehouse}",
    ]
    # Run once → cursor commits at 5 → a re-run would yield 0 items.
    runner.invoke(cli, run_args)
    second = runner.invoke(cli, run_args)
    assert "4 row(s)" in second.output  # items resumed → 0, only events.

    reset = runner.invoke(
        cli,
        [
            "state",
            "reset",
            "-c",
            "echo",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert reset.exit_code == 0, _show(reset)
    assert "cleared" in reset.output

    # After reset the next run re-extracts items from initial_value → 9 total.
    after = runner.invoke(cli, run_args)
    assert after.exit_code == 0, _show(after)
    assert "9 row(s)" in after.output


def test_state_reset_single_stream(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state reset --stream`` clears just one stream's cursor."""
    run_args = [
        "run",
        "-c",
        "echo",
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
            "-c",
            "echo",
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
    # items state is gone; a re-run re-extracts it.
    after = runner.invoke(cli, run_args)
    assert "9 row(s)" in after.output


def test_state_reset_never_run_is_clean(
    runner: CliRunner, cli_project: Path, warehouse: str
) -> None:
    """``state reset`` on a connector that never ran is a clean no-op."""
    result = runner.invoke(
        cli,
        [
            "state",
            "reset",
            "-c",
            "echo",
            "--project-dir",
            str(cli_project),
            "--destination-param",
            f"path={warehouse}",
        ],
    )
    assert result.exit_code == 0, _show(result)
    assert "0 cursor row(s) cleared" in result.output
