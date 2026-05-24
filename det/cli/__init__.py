"""The ``det`` command-line interface — a thin shell over the engine.

docs/07 §The CLI is a thin shell over a real Python library: every command here
parses arguments, calls the engine's *public* surface — :func:`det.run`,
the discovery functions in :mod:`det.engine.discovery` — and formats the
result. There is **no** run logic in this module; the engine owns all of it.

Command surface (the seven commands the build stage scopes):

* ``det run``     — extract + load one connector (or every connector
  carrying a ``--tag``); blocks until the run finishes; exit 0 on success,
  1 on any failure.
* ``det list``    — list discoverable connectors (name, kind, streams, tags).
* ``det validate``— run discovery-time validation on one connector or all.
* ``det init``    — scaffold a new project tree.
* ``det new connector`` — scaffold a connector folder.
* ``det state``   — inspect (``list``) / clear (``reset``) incremental state.
* ``det --version`` — print the version.

# NOTE: docs/07 §2 also documents ``det test`` and a ``--log-level`` flag,
# and §3 a five-value exit-code table (0/1/2/3/130). The build stage scopes the
# CLI to exactly the seven commands above, so ``test`` and ``--log-level`` are
# deliberately not built here. The exit code is collapsed to 0/1 — see the
# ``run`` command's own ``# NOTE:`` for why. These are tracked doc-vs-code
# divergences, resolved toward the task's explicit scope.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import click

import det
from det.cli._discovery import discover_all
from det.cli._format import print_run_result, render_table
from det.cli._scaffold import ScaffoldError, scaffold_connector, scaffold_project
from det.cli._state import StateError, list_state, reset_state
from det.engine import ConfigError, DiscoveryError, EngineError
from det.engine import config as cfg
from det.engine import discovery as disc
from det.types import ConnectorKind, RunStatus

# Exceptions the engine raises for "cannot start / cannot discover" problems.
# The CLI catches these at the command boundary and prints a clean one-line
# message instead of a Python traceback (the task's friendly-error bar).
_FRIENDLY_ERRORS = (DiscoveryError, ConfigError, EngineError, ScaffoldError, StateError)


def _fail(message: str, code: int = 1) -> None:
    """Print ``message`` to stderr and exit with ``code`` — no traceback."""
    click.echo(click.style(f"error: {message}", fg="red"), err=True)
    raise SystemExit(code)


def _split_select(values: Sequence[str]) -> tuple[str, ...]:
    """Flatten a repeatable, comma-separated ``--select`` option into stream names.

    # NOTE: docs/07 §2 documents ``--select`` as comma-separated
    # (``--select a,b``); the build task says repeatable (``--select a
    # --select b``). Both are supported — each occurrence is split on commas —
    # so the doc form and the task form are equivalent. Empty / whitespace
    # entries are dropped.
    """
    out: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                out.append(part)
    return tuple(out)


def _parse_destination_params(values: Sequence[str]) -> dict[str, Any]:
    """Parse repeatable ``--destination-param key=value`` options into a dict.

    Lets the operator override a destination's own config for this invocation —
    e.g. ``--destination-param path=/tmp/wh.duckdb`` to point DuckDB at a
    scratch file. Passed straight through to ``det.run(destination_params=)``.
    A value with no ``=`` is a usage error.
    """
    out: dict[str, Any] = {}
    for value in values:
        if "=" not in value:
            _fail(
                f"--destination-param expects key=value, got {value!r}", code=2
            )
        key, _, val = value.partition("=")
        out[key.strip()] = val
    return out


# ---------------------------------------------------------------------------
# The command group
# ---------------------------------------------------------------------------


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=det.__version__, prog_name="det")
def cli() -> None:
    """det — a simple, open-source extract-load tool.

    Move data from a source into a destination, and nothing more. Run
    ``det <command> --help`` for command-specific options.
    """


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@cli.command()
@click.option("-c", "--connector", "connector", help="Run a single connector by name.")
@click.option(
    "--tag",
    "tag",
    help="Run every connector carrying this tag, in sequence. "
    "Mutually exclusive with --connector.",
)
@click.option(
    "--target",
    "target",
    help="profiles.yml target to use. Defaults to the project's default_target.",
)
@click.option(
    "--select",
    "select",
    multiple=True,
    metavar="STREAM",
    help="Limit the run to these streams. Repeatable and/or comma-separated.",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    help="Ignore prior incremental state; re-extract every stream from the start.",
)
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root (or a directory under it). Defaults to the current directory.",
)
@click.option(
    "--destination-param",
    "destination_params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a destination config value for this run. Repeatable.",
)
def run(
    connector: str | None,
    tag: str | None,
    target: str | None,
    select: tuple[str, ...],
    full_refresh: bool,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Extract and load — the core command.

    Runs synchronously: it blocks until the run finishes, then exits. This is
    the "wait until it succeeds" contract orchestrators depend on.

    Exit codes:

    \b
      0  every run succeeded.
      1  any run failed (or a config/discovery error stopped a run).
    """
    # NOTE: docs/07 §3 specifies a finer exit-code table — 0/1/2/3/130, splitting
    # config errors (2) and planning errors (3) from runtime failures (1). The
    # engine's ``det.run()`` returns a uniform FAILED ``RunResult`` for
    # every failure class and never raises (runner.py docstring), so the CLI
    # has no signal to tell a config error from a load error apart. Per the
    # task ("0 = all succeeded, 1 = any failed"; code is source of truth), the
    # CLI collapses to 0/1. SIGINT still surfaces naturally via KeyboardInterrupt.
    if bool(connector) == bool(tag):
        _fail("pass exactly one of --connector / --tag", code=2)

    dest_params = _parse_destination_params(destination_params)
    selected = _split_select(select)

    # Resolve which connectors to run. --tag fans out to a sequence.
    try:
        if tag:
            project_root = disc.find_project_root(project_dir)
            project = cfg.ProjectConfig.load(project_root)
            names = disc.connectors_with_tag(
                tag, project_root, list(project.connector_paths)
            )
            if not names:
                _fail(f"no connectors carry the tag {tag!r}", code=2)
        else:
            assert connector is not None  # guaranteed by the XOR check above.
            names = [connector]
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable — _fail raises — but keeps mypy's flow analysis happy.

    # Run each connector. det.run() never raises — it returns a FAILED
    # RunResult — so the only thing that can throw here is a programming bug.
    any_failed = False
    for index, name in enumerate(names):
        if index:
            click.echo()  # blank line between connectors in a --tag run.
        result = det.run(
            name,
            target,
            project_dir=project_dir,
            full_refresh=full_refresh,
            select=selected,
            destination_params=dest_params or None,
        )
        print_run_result(result)
        if result.status is not RunStatus.SUCCEEDED:
            any_failed = True

    if len(names) > 1:
        click.echo()
        if any_failed:
            verdict = click.style("one or more FAILED", fg="red")
        else:
            verdict = click.style("all succeeded", fg="green")
        click.echo(f"{len(names)} connector(s): {verdict}")

    raise SystemExit(1 if any_failed else 0)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@cli.command(name="list")
@click.option("--tag", "tag", help="Show only connectors carrying this tag.")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def list_connectors(tag: str | None, project_dir: Path | None) -> None:
    """List discoverable connectors — name, kind, streams, tags.

    Reads each connector's ``register.yaml`` via discovery; runs nothing.
    Project-local connectors shadow same-named baked ones (docs/03 §5).
    """
    try:
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
        connectors = discover_all(project_root, list(project.connector_paths))
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if tag:
        connectors = [c for c in connectors if tag in c.manifest.tags]

    if not connectors:
        msg = "no connectors found"
        if tag:
            msg += f" with tag {tag!r}"
        click.echo(msg)
        return

    rows: list[list[str]] = []
    for c in connectors:
        m = c.manifest
        if m.kind is ConnectorKind.SOURCE:
            stream_count = str(len(m.streams))
            streams = ", ".join(s.name for s in m.streams) or "-"
        else:
            stream_count = "-"
            streams = "-"
        rows.append(
            [
                c.name,
                m.kind.value,
                c.origin,
                stream_count,
                streams,
                ", ".join(m.tags) or "-",
            ]
        )
    click.echo(
        render_table(
            ["CONNECTOR", "KIND", "ORIGIN", "#STREAMS", "STREAMS", "TAGS"], rows
        )
    )


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "-c", "--connector", "connector", help="Validate a single connector by name."
)
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def validate(connector: str | None, project_dir: Path | None) -> None:
    """Run discovery-time validation (docs/03 §7) on connectors.

    Validates one connector (``-c``) or every discoverable connector. Reports
    each problem found; exits non-zero if any connector is invalid — a useful
    CI / pre-commit gate.
    """
    try:
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
        paths = list(project.connector_paths)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if connector:
        names = [connector]
    else:
        names = [c.name for c in discover_all(project_root, paths)]
        if not names:
            click.echo("no connectors found to validate")
            return

    invalid = 0
    for name in names:
        try:
            # resolve_connector runs validate_connector internally (validate=True
            # is the default) — discovery + validation in one call.
            disc.resolve_connector(name, project_root, paths)
        except (DiscoveryError, ConfigError) as exc:
            invalid += 1
            click.echo(click.style(f"FAIL  {name}", fg="red"))
            for line in str(exc).splitlines():
                click.echo(f"      {line}")
        else:
            click.echo(click.style(f"ok    {name}", fg="green"))

    total = len(names)
    if invalid:
        click.echo()
        click.echo(
            click.style(
                f"{invalid} of {total} connector(s) failed validation", fg="red"
            ),
            err=True,
        )
        raise SystemExit(1)
    click.echo()
    click.echo(click.style(f"all {total} connector(s) valid", fg="green"))


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


@cli.command()
@click.argument(
    "directory",
    required=False,
    default=".",
    type=click.Path(file_okay=False, path_type=Path),
)
@click.option(
    "--force", is_flag=True, help="Overwrite an existing det_project.yml."
)
def init(directory: Path, force: bool) -> None:
    """Scaffold a new det project in DIRECTORY (default: current directory).

    Writes ``det_project.yml``, ``profiles.yml``, ``connectors/``,
    ``destinations/``, ``.gitignore`` and a short ``README.md``. Refuses to
    clobber an existing project unless ``--force`` is passed.
    """
    try:
        root = scaffold_project(directory, force=force)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created det project at {root}", fg="green"))
    click.echo("next:")
    click.echo("  det new connector my_source")
    click.echo("  det validate")
    click.echo("  det run -c my_source")


# ---------------------------------------------------------------------------
# new connector
# ---------------------------------------------------------------------------


@cli.group()
def new() -> None:
    """Scaffold a new component (a connector)."""


@new.command(name="connector")
@click.argument("name")
@click.option(
    "--kind",
    type=click.Choice(["source", "destination"]),
    default="source",
    show_default=True,
    help="Connector kind to scaffold.",
)
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def new_connector(name: str, kind: str, project_dir: Path | None) -> None:
    """Scaffold a connector folder ``connectors/NAME/``.

    A source gets a ``register.yaml`` with an example stream plus a ``source.py``
    ``@stream`` stub; a destination gets a ``register.yaml`` plus a
    ``destination.py`` ``@destination`` hook stub. Modeled on the ``echo``
    fixture connector.
    """
    # NOTE: ``--kind`` is documented in docs/07 §2 but the build task's prose
    # described only the source form. It is included here because omitting it
    # would make ``det new connector x --kind destination`` (a documented
    # invocation) fail. Code/docs are reconciled toward supporting both.
    try:
        project_root = disc.find_project_root(project_dir)
    except DiscoveryError as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    # New connectors go under connectors/ (sources) or destinations/ — the
    # readability convention of docs/06; both are on connector_paths anyway.
    subdir = "connectors" if kind == "source" else "destinations"
    target_dir = project_root / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        folder = scaffold_connector(target_dir, name, kind=kind)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created {kind} connector at {folder}", fg="green"))
    click.echo(f"edit {folder}/register.yaml and its connector body, then:")
    click.echo(f"  det validate -c {name}")


# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------


@cli.group()
def state() -> None:
    """Inspect and reset incremental state (the destination's _det_state)."""


@state.command(name="list")
@click.option(
    "-c", "--connector", "connector", required=True, help="Source connector name."
)
@click.option("--target", "target", help="profiles.yml target to use.")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
@click.option(
    "--destination-param",
    "destination_params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a destination config value. Repeatable.",
)
def state_list(
    connector: str,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Show the ``_det_state`` rows for a connector.

    Opens the bound destination and calls its ``read_state`` hook — the same
    call the engine makes at run start. One row per stream that has committed
    incremental state.
    """
    dest_params = _parse_destination_params(destination_params)
    try:
        records = list_state(
            connector,
            project_dir=project_dir,
            target=target,
            destination_params=dest_params or None,
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if not records:
        click.echo(f"connector {connector!r} has no committed state")
        return

    rows: list[list[str]] = []
    for r in sorted(records, key=lambda x: x.stream):
        rows.append(
            [
                r.connector,
                r.stream,
                "-" if r.cursor_value is None else str(r.cursor_value),
                str(r.rows_total),
                "-" if r.updated_at is None else r.updated_at.isoformat(sep=" "),
            ]
        )
    click.echo(
        render_table(
            ["CONNECTOR", "STREAM", "CURSOR VALUE", "ROWS TOTAL", "UPDATED AT"], rows
        )
    )


@state.command(name="reset")
@click.option(
    "-c", "--connector", "connector", required=True, help="Source connector name."
)
@click.option(
    "--stream",
    "stream_name",
    help="Reset only this stream's cursor. Omit to reset every stream.",
)
@click.option("--target", "target", help="profiles.yml target to use.")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
@click.option(
    "--destination-param",
    "destination_params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a destination config value. Repeatable.",
)
def state_reset(
    connector: str,
    stream_name: str | None,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Clear incremental state so the next run is a full re-extract.

    Deletes the connector's ``_det_state`` rows (or one stream's, with
    ``--stream``). The next run finds no prior cursor and seeds from each
    stream's ``initial_value`` — the surgical alternative to ``--full-refresh``.
    Loaded data is untouched.
    """
    dest_params = _parse_destination_params(destination_params)
    try:
        cleared = reset_state(
            connector,
            stream=stream_name,
            project_dir=project_dir,
            target=target,
            destination_params=dest_params or None,
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    scope = f"stream {stream_name!r}" if stream_name else "all streams"
    click.echo(
        click.style(
            f"reset state for connector {connector!r} ({scope}): "
            f"{cleared} cursor row(s) cleared",
            fg="green",
        )
    )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the ``det`` console script (pyproject scripts).

    Delegates to the click command group. click handles its own
    ``SystemExit``; a Ctrl-C surfaces as :class:`KeyboardInterrupt`, which is
    caught here and turned into a clean non-zero exit (no traceback) — the
    "interrupted" path docs/07 §3 describes.
    """
    try:
        cli()
    except KeyboardInterrupt:  # pragma: no cover — interactive only.
        click.echo(click.style("interrupted", fg="yellow"), err=True)
        sys.exit(130)


if __name__ == "__main__":
    main()
