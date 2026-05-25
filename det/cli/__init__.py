"""The ``det`` command-line interface — a thin shell over the engine.

docs/07 §The CLI is a thin shell over a real Python library: every command here
parses arguments, calls the engine's *public* surface — :func:`det.run`,
the discovery functions in :mod:`det.engine.discovery`, and the config loader
in :mod:`det.engine.configs` — and formats the result. There is **no** run
logic in this module; the engine owns all of it.

Stage 8.B made *configs* the runtime unit (docs/12). The CLI's primary
selection arg is now ``-p / --conf <config_name>``; a connector alone is no
longer runnable because it doesn't say where to write. ``det list`` /
``det validate`` cover sources, destinations, AND configs; ``det init``
scaffolds the new layout (``sources/`` + ``destinations/`` + ``configs/``);
``det new`` has three subcommands (``source``, ``destination``, ``config``);
``det state`` is keyed by config (which resolves to a source for the actual
state lookup).

Command surface:

* ``det run -p <config> [--target T] [--select S] [--full-refresh]
  [--param k=v] [--destination-param k=v]`` — extract + load one config.
* ``det list [--kind {{source,destination,config}}]`` — list discoverable
  components.
* ``det validate`` — discovery-time validation of every source, destination,
  and config.
* ``det init [<dir>]`` — scaffold a new project tree.
* ``det new {{source,destination,config}} <name>`` — scaffold one folder/file.
* ``det state list -p <config>`` / ``det state reset -p <config>
  [--stream S]`` — inspect / clear incremental state.
* ``det --version`` — print the version.
"""

from __future__ import annotations

import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import click

import det
from det.cli._discovery import (
    discover_all_configs,
    discover_all_destinations,
    discover_all_sources,
)
from det.cli._format import print_run_result, render_table
from det.cli._scaffold import (
    ScaffoldError,
    scaffold_config,
    scaffold_destination,
    scaffold_project,
    scaffold_source,
)
from det.cli._state import StateError, list_state, reset_state
from det.engine import ConfigError, DiscoveryError, EngineError
from det.engine import config as cfg
from det.engine import discovery as disc
from det.types import RunStatus

# Exceptions the engine raises for "cannot start / cannot discover" problems.
# The CLI catches these at the command boundary and prints a clean one-line
# message instead of a Python traceback.
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
    # entries are dropped. A non-empty ``--select`` REPLACES the config's own
    # ``select:`` (it does not union — docs/07 / docs/12).
    """
    out: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                out.append(part)
    return tuple(out)


def _parse_kv(label: str, values: Sequence[str]) -> dict[str, Any]:
    """Parse repeatable ``--<label> key=value`` options into a dict.

    Shared by ``--param`` and ``--destination-param``. A value with no ``=``
    is a usage error — surfaced as a clean message + exit 2.
    """
    out: dict[str, Any] = {}
    for value in values:
        if "=" not in value:
            _fail(f"--{label} expects key=value, got {value!r}", code=2)
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
@click.option(
    "-p",
    "--conf",
    "config",
    required=True,
    metavar="CONFIG",
    help="Run the pipeline config of this name (under configs/).",
)
@click.option(
    "--target",
    "target",
    help="Override the config's `target:`. Falls back to the destination's "
    "default_target in profiles.yml.",
)
@click.option(
    "--select",
    "select",
    multiple=True,
    metavar="STREAM",
    help="Replace the config's `select:` with these streams (repeatable / "
    "comma-separated).",
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
    "--param",
    "params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a source param for this run. Repeatable.",
)
@click.option(
    "--destination-param",
    "destination_params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a destination config value for this run. Repeatable.",
)
def run(
    config: str,
    target: str | None,
    select: tuple[str, ...],
    full_refresh: bool,
    project_dir: Path | None,
    params: tuple[str, ...],
    destination_params: tuple[str, ...],
) -> None:
    """Extract and load — the core command, driven by a pipeline config.

    Runs synchronously: it blocks until the run finishes, then exits. This is
    the "wait until it succeeds" contract orchestrators depend on.

    Exit codes:

    \b
      0  the run succeeded.
      1  the run failed (config/discovery error or load error).
    """
    src_overrides = _parse_kv("param", params)
    dest_overrides = _parse_kv("destination-param", destination_params)
    selected = _split_select(select)

    # det.run() never raises — it returns a FAILED RunResult.
    result = det.run(
        config,
        project_dir=project_dir,
        target_override=target,
        params_override=src_overrides or None,
        destination_params_override=dest_overrides or None,
        full_refresh=full_refresh,
        select=selected,
    )
    print_run_result(result)
    raise SystemExit(0 if result.status is RunStatus.SUCCEEDED else 1)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@cli.command(name="list")
@click.option(
    "--kind",
    "kind",
    type=click.Choice(["source", "destination", "config"]),
    help="Restrict the listing to one kind (default: list all three).",
)
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def list_components(kind: str | None, project_dir: Path | None) -> None:
    """List discoverable sources, destinations, and configs — docs/03 §5, docs/12.

    Reads each ``register.yaml`` / config file via discovery; runs nothing.
    Project-local connectors shadow same-named baked ones (docs/03 §5).
    """
    try:
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    show_src = kind is None or kind == "source"
    show_dst = kind is None or kind == "destination"
    show_cfg = kind is None or kind == "config"

    sections: list[tuple[str, str]] = []

    if show_src:
        try:
            sources = discover_all_sources(project_root, list(project.source_paths))
        except _FRIENDLY_ERRORS as exc:
            _fail(str(exc), code=2)
            return
        src_rows: list[list[str]] = []
        for s in sources:
            stream_names = ", ".join(x.name for x in s.manifest.streams) or "-"
            src_rows.append(
                [
                    s.name,
                    s.origin,
                    str(len(s.manifest.streams)),
                    stream_names,
                    ", ".join(s.manifest.tags) or "-",
                ]
            )
        if src_rows:
            sections.append(
                ("SOURCES", render_table(
                    ["NAME", "ORIGIN", "#STREAMS", "STREAMS", "TAGS"], src_rows
                ))
            )
        else:
            sections.append(("SOURCES", "(no sources found)"))

    if show_dst:
        try:
            destinations = discover_all_destinations(
                project_root, list(project.destination_paths)
            )
        except _FRIENDLY_ERRORS as exc:
            _fail(str(exc), code=2)
            return
        dst_rows: list[list[str]] = []
        for d in destinations:
            dst_rows.append(
                [
                    d.name,
                    d.origin,
                    ", ".join(d.manifest.tags) or "-",
                ]
            )
        if dst_rows:
            sections.append(
                ("DESTINATIONS", render_table(["NAME", "ORIGIN", "TAGS"], dst_rows))
            )
        else:
            sections.append(("DESTINATIONS", "(no destinations found)"))

    if show_cfg:
        try:
            configs = discover_all_configs(project_root, list(project.config_paths))
        except _FRIENDLY_ERRORS as exc:
            _fail(str(exc), code=2)
            return
        cfg_rows: list[list[str]] = []
        for c in configs:
            cfg_rows.append(
                [
                    c.name,
                    c.source,
                    c.destination,
                    c.target or "-",
                    ", ".join(c.select) or "(all)",
                ]
            )
        if cfg_rows:
            sections.append(
                ("CONFIGS", render_table(
                    ["NAME", "SOURCE", "DESTINATION", "TARGET", "SELECT"], cfg_rows
                ))
            )
        else:
            sections.append(("CONFIGS", "(no configs found)"))

    for index, (title, body) in enumerate(sections):
        if index:
            click.echo()
        click.echo(click.style(title, bold=True))
        click.echo(body)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@cli.command()
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def validate(project_dir: Path | None) -> None:
    """Run discovery-time validation on every source, destination, and config.

    Sources + destinations: docs/03 §7 — schema parse, kind consistency,
    stream integrity, decorator coverage, signature injectability. Configs:
    parse + cross-check ``source`` exists, ``destination`` exists, and
    ``target`` (if given) is defined for the destination in
    ``profiles.yml``. Reports each problem found; exits non-zero if any
    component is invalid — a useful CI / pre-commit gate.
    """
    try:
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
        profiles = cfg.Profiles.load(project_root)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    invalid = 0
    total = 0

    # Sources
    try:
        sources = discover_all_sources(project_root, list(project.source_paths))
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return
    for s in sources:
        total += 1
        try:
            disc.resolve_source(s.name, project_root, list(project.source_paths))
        except (DiscoveryError, ConfigError) as exc:
            invalid += 1
            click.echo(click.style(f"FAIL  source {s.name}", fg="red"))
            for line in str(exc).splitlines():
                click.echo(f"      {line}")
        else:
            click.echo(click.style(f"ok    source {s.name}", fg="green"))

    # Destinations
    try:
        destinations = discover_all_destinations(
            project_root, list(project.destination_paths)
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return
    for d in destinations:
        total += 1
        try:
            disc.resolve_destination(
                d.name, project_root, list(project.destination_paths)
            )
        except (DiscoveryError, ConfigError) as exc:
            invalid += 1
            click.echo(click.style(f"FAIL  destination {d.name}", fg="red"))
            for line in str(exc).splitlines():
                click.echo(f"      {line}")
        else:
            click.echo(click.style(f"ok    destination {d.name}", fg="green"))

    # Configs
    try:
        configs = discover_all_configs(project_root, list(project.config_paths))
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return
    src_names = {s.name for s in sources}
    dst_names = {d.name for d in destinations}
    for c in configs:
        total += 1
        problems: list[str] = []
        if c.source not in src_names:
            problems.append(
                f"source {c.source!r} is not discoverable under "
                f"{', '.join(project.source_paths)}/ or baked sources"
            )
        if c.destination not in dst_names:
            problems.append(
                f"destination {c.destination!r} is not discoverable under "
                f"{', '.join(project.destination_paths)}/ or baked destinations"
            )
        if c.target is not None and c.destination in profiles.destinations:
            block = profiles.destinations[c.destination]
            if c.target not in block.targets:
                known = ", ".join(sorted(block.targets)) or "(none defined)"
                problems.append(
                    f"target {c.target!r} is not defined under "
                    f"destination {c.destination!r} in profiles.yml; "
                    f"known targets: {known}"
                )
        if problems:
            invalid += 1
            click.echo(click.style(f"FAIL  config {c.name}", fg="red"))
            for line in problems:
                click.echo(f"      {line}")
        else:
            click.echo(click.style(f"ok    config {c.name}", fg="green"))

    if invalid:
        click.echo()
        click.echo(
            click.style(
                f"{invalid} of {total} component(s) failed validation", fg="red"
            ),
            err=True,
        )
        raise SystemExit(1)
    if total == 0:
        click.echo("no components found to validate")
        return
    click.echo()
    click.echo(click.style(f"all {total} component(s) valid", fg="green"))


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

    Writes ``det_project.yml``, ``profiles.yml`` (destination-keyed),
    empty ``sources/`` and ``destinations/`` folders, a ``configs/`` folder
    seeded with one ``example.yml`` stub, ``.gitignore`` and a short
    ``README.md``. Refuses to clobber an existing project unless ``--force``.
    """
    try:
        root = scaffold_project(directory, force=force)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created det project at {root}", fg="green"))
    click.echo("next:")
    click.echo("  det new source my_source")
    click.echo("  det new config my_pipeline")
    click.echo("  det validate")
    click.echo("  det run -p my_pipeline")


# ---------------------------------------------------------------------------
# new <source|destination|config>
# ---------------------------------------------------------------------------


@cli.group()
def new() -> None:
    """Scaffold a new component (source, destination, or config)."""


@new.command(name="source")
@click.argument("name")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def new_source(name: str, project_dir: Path | None) -> None:
    """Scaffold a source folder ``sources/NAME/``."""
    try:
        project_root = disc.find_project_root(project_dir)
    except DiscoveryError as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    target_dir = project_root / "sources"
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        folder = scaffold_source(target_dir, name)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created source at {folder}", fg="green"))
    click.echo(f"edit {folder}/register.yaml and {folder}/source.py, then:")
    click.echo("  det new config <name>   # bind this source to a destination")
    click.echo("  det validate")


@new.command(name="destination")
@click.argument("name")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def new_destination(name: str, project_dir: Path | None) -> None:
    """Scaffold a destination folder ``destinations/NAME/``."""
    try:
        project_root = disc.find_project_root(project_dir)
    except DiscoveryError as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    target_dir = project_root / "destinations"
    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        folder = scaffold_destination(target_dir, name)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created destination at {folder}", fg="green"))
    click.echo(f"edit {folder}/register.yaml and {folder}/destination.py, then:")
    click.echo("  det validate")


@new.command(name="config")
@click.argument("name")
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def new_config(name: str, project_dir: Path | None) -> None:
    """Scaffold a pipeline config file ``configs/NAME.yml`` — docs/12."""
    try:
        project_root = disc.find_project_root(project_dir)
    except DiscoveryError as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    target_dir = project_root / "configs"
    try:
        path = scaffold_config(target_dir, name)
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.
    click.echo(click.style(f"created config at {path}", fg="green"))
    click.echo(f"edit {path}, then:")
    click.echo("  det validate")
    click.echo(f"  det run -p {name}")


# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------


@cli.group()
def state() -> None:
    """Inspect and reset incremental state (the destination's _det_state)."""


@state.command(name="list")
@click.option(
    "-p",
    "--conf",
    "config",
    required=True,
    metavar="CONFIG",
    help="Pipeline config name.",
)
@click.option(
    "--target", "target", help="Override the config's target for this lookup."
)
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
    config: str,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Show the ``_det_state`` rows for one config's source.

    Opens the config's bound destination and calls its ``read_state`` hook
    — the same call the engine makes at run start. One row per stream that
    has committed incremental state. State rows are keyed by source name
    (not config name) — a re-run under a different config that shares this
    source resumes off the same rows.
    """
    dest_params = _parse_kv("destination-param", destination_params)
    try:
        records = list_state(
            config,
            project_dir=project_dir,
            target=target,
            destination_params=dest_params or None,
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if not records:
        click.echo(f"config {config!r} has no committed state")
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
            ["SOURCE", "STREAM", "CURSOR VALUE", "ROWS TOTAL", "UPDATED AT"], rows
        )
    )


@state.command(name="reset")
@click.option(
    "-p",
    "--conf",
    "config",
    required=True,
    metavar="CONFIG",
    help="Pipeline config name.",
)
@click.option(
    "--stream",
    "stream_name",
    help="Reset only this stream's cursor. Omit to reset every stream.",
)
@click.option(
    "--target", "target", help="Override the config's target for this reset."
)
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
    config: str,
    stream_name: str | None,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Clear incremental state so the next run is a full re-extract.

    Deletes the rows in ``_det_state`` for the config's source (or one
    stream's, with ``--stream``). The next run finds no prior cursor and
    seeds from each stream's ``initial_value`` — the surgical alternative to
    ``--full-refresh``. Loaded data is untouched.
    """
    dest_params = _parse_kv("destination-param", destination_params)
    try:
        cleared = reset_state(
            config,
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
            f"reset state for config {config!r} ({scope}): "
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
