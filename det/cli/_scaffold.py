"""Project + connector scaffolding for ``det init`` / ``det new``.

These functions write the starter files docs/06 describes — a project tree
(``det_project.yml``, ``profiles.yml``, ``connectors/``, ``destinations/``,
``.gitignore``, ``README.md``) and a connector folder (``register.yaml`` +
a ``source.py`` / ``destination.py`` stub). The templates are modeled on the
real working ``tests/fixtures/`` project and the ``echo`` fixture connector.

Pure file I/O — no engine logic.
"""

from __future__ import annotations

from pathlib import Path

from det.engine.discovery import PROJECT_FILE

# --- project scaffold templates -------------------------------------------

_PROJECT_YML = """\
# {name} - a det project (docs/06).
#
# Committed to version control. Pure declaration: no logic, no credentials.
name: {name}
version: "0.1.0"

# Directories scanned for custom connector folders, relative to this file.
# 'connectors' and 'destinations' are a readability convention, not a typed
# boundary - a connector's own register.yaml 'kind:' decides source vs dest.
connector_paths:
  - connectors
  - destinations

# The destination a source binds to when its register.yaml omits a
# 'destination:' block. 'duckdb' is the pre-baked zero-config dev default.
default_destination: duckdb

# Which profiles.yml target to use when --target is omitted.
default_target: dev

# Project-wide param overrides applied to every connector (docs/03 §6).
vars: {{}}
"""

_PROFILES_YML = """\
# profiles.yml - credentials per target (docs/06).
#
# NOT committed to version control - it is listed in .gitignore. CI and
# production supply it out of band. Each target is a named environment
# selected by `det run --target <name>`.
targets:

  dev:
    # Destination credential/routing blocks, keyed by destination name.
    destinations:
      # The pre-baked DuckDB destination. 'path' is the .duckdb file; it
      # defaults to .det/warehouse.duckdb when this block is omitted.
      duckdb:
        path: ".det/warehouse.duckdb"

    # Named credential blocks resolved by ${{profile.<block>.<key>}} secret
    # refs in any register.yaml (docs/03 §2.5). Add one per connector that
    # declares secrets, e.g.:
    #
    #   profiles:
    #     my_source:
    #       api_token: ${{env.MY_SOURCE_API_TOKEN}}
"""

_GITIGNORE = """\
# credentials - never commit
profiles.yml

# disposable working directory (docs/06)
.det/
"""

_README = """\
# {name}

A [det](https://github.com/albinasplesnys/det) extract-load project.

## Layout

- `det_project.yml` - project config (committed).
- `profiles.yml` - credentials per target (**not** committed - gitignored).
- `connectors/` - custom source connectors (`kind: source`).
- `destinations/` - custom destination connectors (`kind: destination`).
- `.det/` - disposable working dir (gitignored).

## Getting started

```bash
det new connector my_source   # scaffold a source connector
det validate                  # discovery-time validation
det run -c my_source          # extract + load
```
"""

# --- connector scaffold templates -----------------------------------------

_SOURCE_REGISTER_YML = """\
# {name} - a det SOURCE connector (docs/03).
name: {name}
kind: source
version: "0.1.0"
summary: Describe what {name} extracts.
tags: []

streams:
  # One output table. The matching @stream function lives in source.py.
  - name: example
    table: {name}_example
    write_disposition: append
    schema:
      - {{name: id,   type: INTEGER, mode: REQUIRED}}
      - {{name: name, type: STRING}}

# Where this source's streams land. Omit to use the project's
# default_destination (docs/03 §2.3).
destination:
  connector: duckdb
"""

_SOURCE_PY = '''\
"""The {name} source connector body - its @stream functions.

A @stream generator yields *batches* (list[dict]), not single records
(docs/03 §3.1). The engine declares which arguments it can inject - `config`,
`state`, `cursor`, `log` - and supplies only the ones a function names.
"""

from __future__ import annotations

from collections.abc import Iterator

from det import Batch, stream


@stream(name="example")
def example() -> Iterator[Batch]:
    """Yield the `example` stream's records, one batch at a time.

    Replace this stub with a real extract. A connector that needs config or
    secrets declares them in register.yaml and names `config` here::

        @stream(name="example")
        def example(config) -> Iterator[Batch]:
            page = fetch(config.api_token, config.page_size)
            yield page
    """
    yield [
        {{"id": 1, "name": "hello"}},
        {{"id": 2, "name": "world"}},
    ]
'''

_DEST_REGISTER_YML = """\
# {name} - a det DESTINATION connector (docs/05).
name: {name}
kind: destination
version: "0.1.0"
summary: Describe what {name} writes to.
tags: []

# Routing/credential knobs the destination needs, declared as typed params.
params:
  example_path:
    type: string
    default: "./out"
    description: Where this destination writes.
"""

_DEST_PY = '''\
"""The {name} destination connector body - its @destination hooks.

A destination implements the docs/03 §3.4 hook contract. The engine drives
them in order: open -> read_state -> [ensure_schema -> write_batch ...]* ->
commit_state -> close. See det/destinations/duckdb/ for a complete,
production-quality reference implementation.
"""

from __future__ import annotations

from det import (
    Batch,
    Capability,
    Config,
    StateRecord,
    StreamMeta,
    destination,
)


@destination.capabilities
def capabilities() -> set[Capability]:
    """Declare what this destination can do (docs/05 §1)."""
    return {{Capability.STATE}}


@destination.open
def open(config: Config) -> object:
    """Open and return the connection handle passed to every later hook."""
    raise NotImplementedError("implement {name}.open")


@destination.ensure_schema
def ensure_schema(conn: object, stream: StreamMeta) -> None:
    """Create the target table if absent; additively evolve it (docs/05 §3)."""
    raise NotImplementedError("implement {name}.ensure_schema")


@destination.write_batch
def write_batch(conn: object, batch: Batch, stream: StreamMeta) -> int:
    """Persist one batch per its write disposition; return rows written."""
    raise NotImplementedError("implement {name}.write_batch")


@destination.read_state
def read_state(conn: object, connector: str) -> list[StateRecord]:
    """Load every prior StateRecord for a connector (docs/05 §5)."""
    raise NotImplementedError("implement {name}.read_state")


@destination.commit_state
def commit_state(conn: object, run_id: str, records: list[StateRecord]) -> None:
    """Persist the run's StateRecord set after all batches land (docs/05 §5)."""
    raise NotImplementedError("implement {name}.commit_state")


@destination.close
def close(conn: object) -> None:
    """Close the connection. Always called, even on failure (docs/05 §1)."""
    raise NotImplementedError("implement {name}.close")
'''


class ScaffoldError(Exception):
    """A scaffold target already exists, or could not be written.

    The CLI catches this and prints a clean message + non-zero exit, never a
    traceback — the same friendly-error contract the engine errors get.
    """


def scaffold_project(directory: Path, *, force: bool = False) -> Path:
    """Write a new det project tree into ``directory`` — docs/06.

    Creates ``det_project.yml``, ``profiles.yml``, empty ``connectors/``
    and ``destinations/`` folders, ``.gitignore`` and a short ``README.md``.
    Refuses to clobber an existing project (a ``det_project.yml`` already
    present) unless ``force`` is set. Returns the project root.
    """
    project_file = directory / PROJECT_FILE
    if project_file.exists() and not force:
        raise ScaffoldError(
            f"{project_file} already exists; pass --force to overwrite an "
            f"existing project"
        )
    name = directory.resolve().name or "det_project"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "connectors").mkdir(exist_ok=True)
    (directory / "destinations").mkdir(exist_ok=True)

    project_file.write_text(_PROJECT_YML.format(name=name))
    (directory / "profiles.yml").write_text(_PROFILES_YML)
    (directory / ".gitignore").write_text(_GITIGNORE)
    (directory / "README.md").write_text(_README.format(name=name))
    # An empty folder needs a marker to survive `git add`; a comment file is
    # friendlier than .gitkeep for a human reading the tree.
    return directory


def scaffold_connector(
    connectors_dir: Path, name: str, *, kind: str = "source"
) -> Path:
    """Write a new connector folder ``connectors_dir/<name>/`` — docs/03, docs/06.

    For ``kind="source"``: a ``register.yaml`` with one example stream plus a
    ``source.py`` carrying a ``@stream`` stub. For ``kind="destination"``: a
    ``register.yaml`` plus a ``destination.py`` carrying the full
    ``@destination`` hook stub set. Refuses to overwrite an existing folder.
    Returns the connector folder path.
    """
    if kind not in ("source", "destination"):
        raise ScaffoldError(f"unknown connector kind {kind!r}; expected source|destination")
    folder = connectors_dir / name
    if folder.exists():
        raise ScaffoldError(
            f"{folder} already exists; choose a different connector name"
        )
    folder.mkdir(parents=True)
    if kind == "source":
        (folder / "register.yaml").write_text(_SOURCE_REGISTER_YML.format(name=name))
        (folder / "source.py").write_text(_SOURCE_PY.format(name=name))
    else:
        (folder / "register.yaml").write_text(_DEST_REGISTER_YML.format(name=name))
        (folder / "destination.py").write_text(_DEST_PY.format(name=name))
    return folder
