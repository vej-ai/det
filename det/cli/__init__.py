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

import json
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
from det.cli._format import (
    print_multi_run_summary,
    print_run_result,
    render_table,
)
from det.cli._runs import get_run as runs_get
from det.cli._runs import list_runs as runs_list
from det.cli._runs import read_log_lines
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
    default=None,
    metavar="CONFIG",
    help="Run the pipeline config of this name (under configs/). "
    "Mutually exclusive with --tag.",
)
@click.option(
    "--tag",
    "tag",
    default=None,
    metavar="TAG",
    help="Run every pipeline config whose tags: list includes this tag. "
    "Sequential, continue-on-failure. Mutually exclusive with -p/--conf.",
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
    help="Override a source param for this run. Repeatable. "
    "Not supported with --tag (use -p for per-config overrides).",
)
@click.option(
    "--destination-param",
    "destination_params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override a destination config value for this run. Repeatable.",
)
@click.option(
    "--threads",
    "threads",
    type=click.IntRange(min=1),
    default=None,
    metavar="N",
    help="Pipeline-level concurrency for --tag (stage 8e). Overrides "
    "profiles.yml `threads:`. Each destination's "
    "@destination.max_concurrent_writes caps further. Meaningless with "
    "-p (single config) — ignored with a debug log there.",
)
def run(
    config: str | None,
    tag: str | None,
    target: str | None,
    select: tuple[str, ...],
    full_refresh: bool,
    project_dir: Path | None,
    params: tuple[str, ...],
    destination_params: tuple[str, ...],
    threads: int | None,
) -> None:
    """Extract and load — the core command, driven by a pipeline config.

    Runs synchronously: it blocks until the run finishes, then exits. This is
    the "wait until it succeeds" contract orchestrators depend on.

    Exactly one of ``-p/--conf`` or ``--tag`` must be given. ``--tag``
    runs every config whose ``tags:`` list contains the tag, sequentially
    in alphabetical name order, continuing past per-config failures.

    Exit codes:

    \b
      0  every run succeeded.
      1  at least one run failed (config/discovery error or load error).
      2  CLI usage error (no selector, both selectors, no matching configs,
         or --param combined with --tag).
    """
    # Mutual exclusion + required-one — hand-rolled per docs/02 §Pipeline
    # selection (stage 8d). Hand-rolling keeps the click option set vanilla
    # and the error message close to the call site.
    if config is not None and tag is not None:
        _fail("-p/--conf and --tag are mutually exclusive", code=2)
        return  # unreachable
    if config is None and tag is None:
        _fail("exactly one of -p/--conf or --tag is required", code=2)
        return  # unreachable

    selected = _split_select(select)
    dest_overrides = _parse_kv("destination-param", destination_params)

    if tag is not None:
        # --tag path. --param is not supported (see run_tag NOTE) — flag
        # the misuse with a clean exit-2 instead of silently dropping the
        # overrides.
        if params:
            _fail(
                "--param is not supported with --tag (a source param would "
                "apply to every matched config silently); use `det run -p "
                "<config> --param k=v` per config instead",
                code=2,
            )
            return  # unreachable

        results = det.run_tag(
            tag,
            project_dir=project_dir,
            target_override=target,
            destination_params_override=dest_overrides or None,
            full_refresh=full_refresh,
            select=selected,
            threads=threads,
        )
        if not results:
            _fail(f"no configs match tag {tag!r}", code=2)
            return  # unreachable

        # Per-config rendering first (same shape as single-run output), then
        # the multi-run rollup so the at-a-glance summary is the last thing.
        for index, result in enumerate(results):
            if index:
                click.echo()
            print_run_result(result)
        print_multi_run_summary(tag, results)

        any_failed = any(r.status is not RunStatus.SUCCEEDED for r in results)
        raise SystemExit(1 if any_failed else 0)

    # -p path — the single-config invocation that's existed since stage 8.B.
    assert config is not None  # validated above
    # --threads has no meaning with -p (one config = no parallelism). Note
    # in the debug log per the task spec, then silently ignore — surfacing
    # it as a user-visible warning would clutter CI output for orchestrators
    # that always pass --threads. The debug log is enough for an operator
    # actively investigating.
    if threads is not None:
        import logging as _logging

        _logging.getLogger("det.cli").debug(
            "--threads %d ignored with -p (single-config runs are not parallelizable)",
            threads,
        )
    src_overrides = _parse_kv("param", params)

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
    "--tag",
    "tag",
    default=None,
    metavar="TAG",
    help="Filter the listing to components carrying this tag. One tag "
    "namespace across sources, destinations, and configs (a config's "
    "tags: drives `det run --tag`; source/destination tags are catalog "
    "metadata only).",
)
@click.option(
    "--project-dir",
    "project_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Project root. Defaults to the current directory.",
)
def list_components(
    kind: str | None, tag: str | None, project_dir: Path | None
) -> None:
    """List discoverable sources, destinations, and configs — docs/03 §5, docs/12.

    Reads each ``register.yaml`` / config file via discovery; runs nothing.
    Project-local connectors shadow same-named baked ones (docs/03 §5).

    ``--tag <tag>`` filters each section to components whose ``tags:`` list
    contains the tag. Sources and destinations match against their
    ``register.yaml`` ``tags:`` (catalog metadata — "what this connector
    IS"); configs match against their config ``tags:`` (the same field
    ``det run --tag`` selects on). A section with no matches still shows
    its header with a "(no … match tag 'X')" placeholder so the user can
    see what was searched.
    """
    # Normalize the filter tag once. The connector parser keeps source/
    # destination tags verbatim (no normalization at parse time — that's
    # pre-stage-8d behavior), so case-fold both sides at compare time.
    # Config tags ARE normalized at parse time
    # (PipelineConfig.from_dict) — comparing the lowercased filter to
    # those is exact-match.
    tag_filter = tag.strip().lower() if tag is not None else None
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
        if tag_filter is not None:
            sources = [
                s for s in sources
                if any(t.lower() == tag_filter for t in s.manifest.tags)
            ]
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
        elif tag_filter is not None:
            sections.append(
                ("SOURCES", f"(no sources match tag {tag_filter!r})")
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
        if tag_filter is not None:
            destinations = [
                d for d in destinations
                if any(t.lower() == tag_filter for t in d.manifest.tags)
            ]
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
        elif tag_filter is not None:
            sections.append(
                ("DESTINATIONS", f"(no destinations match tag {tag_filter!r})")
            )
        else:
            sections.append(("DESTINATIONS", "(no destinations found)"))

    if show_cfg:
        try:
            configs = discover_all_configs(project_root, list(project.config_paths))
        except _FRIENDLY_ERRORS as exc:
            _fail(str(exc), code=2)
            return
        if tag_filter is not None:
            # Config tags are already lowercased at parse time
            # (PipelineConfig.from_dict) — exact-match against the
            # lowercased filter.
            configs = [c for c in configs if tag_filter in c.tags]
        cfg_rows: list[list[str]] = []
        for c in configs:
            cfg_rows.append(
                [
                    c.name,
                    c.source,
                    c.destination,
                    c.target or "-",
                    ", ".join(c.select) or "(all)",
                    ", ".join(c.tags) or "-",
                ]
            )
        if cfg_rows:
            sections.append(
                ("CONFIGS", render_table(
                    ["NAME", "SOURCE", "DESTINATION", "TARGET", "SELECT", "TAGS"],
                    cfg_rows,
                ))
            )
        elif tag_filter is not None:
            sections.append(
                ("CONFIGS", f"(no configs match tag {tag_filter!r})")
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
# runs
# ---------------------------------------------------------------------------


_RUN_STATUS_COLOR: dict[str, str] = {
    "succeeded": "green",
    "failed": "red",
}


def _fmt_dt(value: Any) -> str:
    """Render a datetime cell for the runs table — '-' if absent."""
    if value is None:
        return "-"
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ", timespec="seconds")
    return str(value)


def _fmt_short(run_id: str) -> str:
    """Trim a ``run-<12hex>`` id to ``<12hex>`` for compact tables."""
    if run_id.startswith("run-"):
        return run_id[len("run-"):]
    return run_id


@cli.group()
def runs() -> None:
    """Inspect run history (the destination's _det_runs + per-run JSONL log)."""


@runs.command(name="list")
@click.option(
    "-p",
    "--conf",
    "config",
    required=True,
    metavar="CONFIG",
    help="Pipeline config name. Required — run records are per-destination, "
    "and the config disambiguates which destination to query.",
)
@click.option(
    "--limit", "limit", type=int, default=20, show_default=True,
    help="Max number of recent runs to show.",
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
def runs_list_cmd(
    config: str,
    limit: int,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """List recent runs from the destination's ``_det_runs`` table.

    Run records are written by the destination's
    ``@destination.write_run_record`` hook (docs/09 §4). Tier-A destinations
    declaring ``Capability.RUN_RECORDS`` host the table; this command opens
    the same destination the config binds to and queries it.
    """
    dest_params = _parse_kv("destination-param", destination_params)
    try:
        rows = runs_list(
            config,
            limit=limit,
            project_dir=project_dir,
            target=target,
            destination_params=dest_params or None,
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if not rows:
        click.echo(f"config {config!r} has no run records")
        return

    table_rows: list[list[str]] = []
    for r in rows:
        color = _RUN_STATUS_COLOR.get(r.status.value, "white")
        table_rows.append(
            [
                _fmt_short(r.run_id),
                r.config,
                click.style(r.status.value, fg=color),
                _fmt_dt(r.started_at),
                f"{r.duration_s:.2f}s",
                str(r.rows_loaded),
                r.error_type or "-",
            ]
        )
    click.echo(
        render_table(
            ["RUN", "CONFIG", "STATUS", "STARTED", "DURATION", "ROWS", "ERROR"],
            table_rows,
        )
    )


@runs.command(name="show")
@click.argument("run_id")
@click.option(
    "-p",
    "--conf",
    "config",
    required=True,
    metavar="CONFIG",
    help="Pipeline config name. Required — run records are per-destination.",
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
def runs_show_cmd(
    run_id: str,
    config: str,
    target: str | None,
    project_dir: Path | None,
    destination_params: tuple[str, ...],
) -> None:
    """Show one run's full record + its JSONL log file — docs/09 §3.2, §4.

    Looks up ``<run_id>`` in ``_det_runs`` (the queryable summary) and
    reads ``.det/logs/<run_id>/run.jsonl`` (the narrative). Accepts the
    short form (``abc123def``) or the long form (``run-abc123def``).
    """
    dest_params = _parse_kv("destination-param", destination_params)
    canonical = run_id if run_id.startswith("run-") else f"run-{run_id}"
    try:
        record, log_path = runs_get(
            config,
            canonical,
            project_dir=project_dir,
            target=target,
            destination_params=dest_params or None,
        )
    except _FRIENDLY_ERRORS as exc:
        _fail(str(exc), code=2)
        return  # unreachable.

    if record is None and log_path is None:
        _fail(f"no run record or log for {run_id!r}", code=1)
        return

    if record is not None:
        click.echo(click.style(f"run {record.run_id}", bold=True))
        click.echo(f"  config       : {record.config}")
        click.echo(f"  source       : {record.source}")
        click.echo(f"  destination  : {record.destination}")
        click.echo(f"  target       : {record.target}")
        color = _RUN_STATUS_COLOR.get(record.status.value, "white")
        click.echo(f"  status       : {click.style(record.status.value, fg=color)}")
        click.echo(f"  started_at   : {_fmt_dt(record.started_at)}")
        click.echo(f"  ended_at     : {_fmt_dt(record.ended_at)}")
        click.echo(f"  duration_s   : {record.duration_s:.2f}")
        click.echo(f"  rows_loaded  : {record.rows_loaded}")
        click.echo(f"  full_refresh : {record.full_refresh}")
        if record.error_type or record.error_message:
            click.echo(
                click.style(
                    f"  error        : {record.error_type}: {record.error_message}",
                    fg="red",
                )
            )
        if record.streams:
            click.echo("  streams      :")
            for s in record.streams:
                click.echo(
                    f"    - {s.get('name')}: status={s.get('status')} "
                    f"rows_loaded={s.get('rows_loaded')} "
                    f"cursor_after={s.get('cursor_after')}"
                )
    else:
        click.echo(click.style(f"no _det_runs row for {run_id!r}", fg="yellow"))

    click.echo()
    if log_path is None:
        click.echo(click.style("no JSONL log file found", fg="yellow"))
        return

    click.echo(click.style(f"log: {log_path}", bold=True))
    events = read_log_lines(log_path)
    if not events:
        click.echo("(log file is empty)")
        return
    use_color = click.get_text_stream("stdout").isatty()
    for event in events:
        event_name = str(event.get("event", "?"))
        rendered = json.dumps(event, default=str)
        if use_color:
            event_color = {
                "run_start": "cyan",
                "stream_start": "cyan",
                "batch_loaded": "white",
                "stream_committed": "green",
                "stream_failed": "red",
                "run_end": "cyan",
                "user": "yellow",
            }.get(event_name)
            if event_color:
                rendered = click.style(rendered, fg=event_color)
        click.echo(rendered)


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
