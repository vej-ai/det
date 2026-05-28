# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Project + connector + config scaffolding for ``dtex init`` / ``dtex new``.

These functions write the starter files docs/06 describes (post-8.B):

* :func:`scaffold_project` — a project tree with ``dtex_project.yml``,
  ``profiles.yml`` (destination-keyed), ``sources/``, ``destinations/``,
  ``configs/`` (with one example config), ``.gitignore`` and a short
  ``README.md``.
* :func:`scaffold_source` — a source folder ``sources/<name>/`` with a
  ``register.yaml`` + ``source.py`` ``@stream`` stub.
* :func:`scaffold_destination` — a destination folder
  ``destinations/<name>/`` with a ``register.yaml`` + ``destination.py``
  full hook stub set.
* :func:`scaffold_config` — a config file ``configs/<name>.yml`` binding a
  source + destination + target.

Pure file I/O — no engine logic. The templates are modeled on the real
working ``tests/fixtures/`` project, the ``echo`` fixture source, and the
baked ``duckdb`` destination.
"""

from __future__ import annotations

from pathlib import Path

from dtex.engine.discovery import PROJECT_FILE

# --- project scaffold templates -------------------------------------------

_PROJECT_YML = """\
# {name} - a dtex project (docs/06 post-8.B).
#
# Committed to version control. Pure declaration: no logic, no credentials.
name: {name}
version: "0.1.0"

# Directories scanned for project-local sources / destinations / configs,
# relative to this file. Project-local connectors shadow baked same-named
# ones (docs/03 §5).
source_paths:
  - sources
destination_paths:
  - destinations
config_paths:
  - configs

# Project-wide param overrides applied to every connector (docs/03 §6).
# Lower precedence than the active config's `params:` block, higher than
# register.yaml param defaults.
vars: {{}}
"""

_PROFILES_YML = """\
# profiles.yml - per-destination connection params (docs/06 post-8.B).
#
# NOT committed to version control - it is listed in .gitignore. CI and
# production supply it out of band. Top-level keys are destination connector
# names (dbt outputs-style); each carries its own `default_target` plus a
# `targets:` map of named-environment connection params.

# The pre-baked DuckDB destination. `path` is the .duckdb file location.
duckdb:
  default_target: dev
  targets:
    dev:
      path: ".dtex/warehouse.duckdb"
    prod:
      path: "/var/data/dtex/warehouse.duckdb"

# Per-target source-secret blocks. Resolved by ${{profile.<block>.<key>}}
# secret refs in any source's register.yaml (docs/03 §2.5).
#
#   profiles:
#     dev:
#       my_source:
#         api_token: ${{env.MY_SOURCE_API_TOKEN_DEV}}
"""

_GITIGNORE = """\
# credentials - never commit
profiles.yml

# disposable working directory (docs/06)
.dtex/
"""

_README = """\
# {name}

A [dtex](https://github.com/albinasplesnys/dtex) extract-load project.

## Layout

- `dtex_project.yml` - project config (committed).
- `profiles.yml` - per-destination connection params (**not** committed - gitignored).
- `sources/` - custom source connectors (`kind: source`).
- `destinations/` - custom destination connectors (`kind: destination`).
- `configs/` - pipeline configs (one source + one destination + one target each).
- `.dtex/` - disposable working dir (gitignored).

## Getting started

```bash
dtex new source my_source        # scaffold a source connector
dtex new config my_pipeline      # scaffold a configs/my_pipeline.yml
dtex validate                    # discovery-time validation
dtex run -p my_pipeline          # extract + load
```
"""

# An example config so a fresh project has at least one runnable thing.
# This template is NOT passed through .format() so single braces are fine.
_EXAMPLE_CONFIG_YML = """\
# An example pipeline config (docs/12). One config = one source + one
# destination + one target + the params customizing both ends. Run with
# `dtex run -p example`.
name: example
source: my_source            # rename me to a source under sources/
destination: duckdb
target: dev
params: {}
destination_params: {}
"""

# --- shared marker file for connector folders -----------------------------
#
# An empty ``__init__.py`` makes the connector folder an *explicit* Python
# package, so ``source.py`` / ``destination.py`` can use relative imports for
# sibling helpers (``from .client import SigmaClient``). The engine's
# load-as-package mechanism (stage 11, ``dtex/engine/discovery.py``) treats a
# folder *without* one as a PEP 420 namespace package, so this file is purely
# explicit-is-better-than-implicit — it makes the package shape obvious to
# IDEs and to readers.

_INIT_PY = (
    "# Marker file — makes this folder a Python package so source.py / "
    "destination.py can use relative imports for sibling helpers "
    "(e.g. `from .client import X`).\n"
)


# --- source connector templates -------------------------------------------

_SOURCE_REGISTER_YML = """\
# {name} - a dtex SOURCE connector (docs/03).
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

# As of stage 8.B, a source's register.yaml carries NO `destination:` block.
# A config (configs/<name>.yml) binds this source to a destination + target.
"""

_SOURCE_PY = '''\
"""The {name} source connector body - its @stream functions.

A @stream generator yields *batches* (list[dict]), not single records
(docs/03 §3.1). The engine declares which arguments it can inject - `config`,
`state`, `cursor`, `log` - and supplies only the ones a function names.
"""

from __future__ import annotations

from collections.abc import Iterator

from dtex import Batch, stream


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

# --- destination connector templates --------------------------------------

_DEST_REGISTER_YML = """\
# {name} - a dtex DESTINATION connector (docs/05).
name: {name}
kind: destination
version: "0.1.0"
summary: Describe what {name} writes to.
tags: []

# Routing/credential knobs the destination needs, declared as typed params.
# At run time these are supplied by profiles.yml's per-target connection row
# (docs/06 post-8.B); a config's `destination_params:` block can override.
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
commit_state -> close. See dtex/destinations/duckdb/ for a complete,
production-quality reference implementation.
"""

from __future__ import annotations

from dtex import (
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

# --- config scaffold template ---------------------------------------------

_CONFIG_YML = """\
# {name} - a dtex pipeline config (docs/12).
#
# One config = one source + one destination + one target + the params that
# customize both ends. Run with `dtex run -p {name}`.
name: {name}
source: my_source            # rename me to a source under sources/
destination: duckdb
target: dev

# Per-pipeline source param overrides (docs/03 §6 layer 3).
params: {{}}

# Per-pipeline destination param overrides (docs/12). Layered on top of the
# destination's profiles.yml row.
destination_params: {{}}

# Optional: limit to a subset of the source's streams. Empty = all.
# select: [items, events]

# Optional: cron expression surfaced to an external scheduler. The engine
# itself never acts on this (docs/03 §2.6).
# schedule: "0 */6 * * *"
"""


class ScaffoldError(Exception):
    """A scaffold target already exists, or could not be written.

    The CLI catches this and prints a clean message + non-zero exit, never a
    traceback — the same friendly-error contract the engine errors get.
    """


def scaffold_project(directory: Path, *, force: bool = False) -> Path:
    """Write a new dtex project tree into ``directory`` — docs/06 post-8.B.

    Creates ``dtex_project.yml``, ``profiles.yml`` (destination-keyed),
    empty ``sources/``, ``destinations/``, and ``configs/`` folders (the
    last seeded with one ``example.yml`` stub), ``.gitignore`` and a short
    ``README.md``. Refuses to clobber an existing project (a
    ``dtex_project.yml`` already present) unless ``force`` is set. Returns the
    project root.
    """
    project_file = directory / PROJECT_FILE
    if project_file.exists() and not force:
        raise ScaffoldError(
            f"{project_file} already exists; pass --force to overwrite an "
            f"existing project"
        )
    name = directory.resolve().name or "dtex_project"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "sources").mkdir(exist_ok=True)
    (directory / "destinations").mkdir(exist_ok=True)
    (directory / "configs").mkdir(exist_ok=True)

    project_file.write_text(_PROJECT_YML.format(name=name))
    (directory / "profiles.yml").write_text(_PROFILES_YML)
    (directory / ".gitignore").write_text(_GITIGNORE)
    (directory / "README.md").write_text(_README.format(name=name))
    (directory / "configs" / "example.yml").write_text(_EXAMPLE_CONFIG_YML)
    return directory


def scaffold_source(sources_dir: Path, name: str) -> Path:
    """Write a new source folder ``sources_dir/<name>/`` — docs/03, docs/06.

    Writes a ``register.yaml`` with one example stream, a ``source.py``
    carrying a ``@stream`` stub, and an empty ``__init__.py`` marker so the
    folder is an explicit Python package (a sibling ``client.py`` /
    ``helpers.py`` can then be imported with ``from .client import X``).
    Refuses to overwrite an existing folder. Returns the source folder path.
    """
    folder = sources_dir / name
    if folder.exists():
        raise ScaffoldError(
            f"{folder} already exists; choose a different source name"
        )
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(_SOURCE_REGISTER_YML.format(name=name))
    (folder / "source.py").write_text(_SOURCE_PY.format(name=name))
    (folder / "__init__.py").write_text(_INIT_PY)
    return folder


def scaffold_destination(destinations_dir: Path, name: str) -> Path:
    """Write a new destination folder ``destinations_dir/<name>/`` — docs/05, docs/06.

    Writes a ``register.yaml``, a ``destination.py`` carrying the full
    ``@destination`` hook stub set, and an empty ``__init__.py`` marker so
    the folder is an explicit Python package (a sibling ``ddl.py`` /
    ``client.py`` can then be imported with ``from .ddl import X``).
    Refuses to overwrite an existing folder. Returns the destination folder
    path.
    """
    folder = destinations_dir / name
    if folder.exists():
        raise ScaffoldError(
            f"{folder} already exists; choose a different destination name"
        )
    folder.mkdir(parents=True)
    (folder / "register.yaml").write_text(_DEST_REGISTER_YML.format(name=name))
    (folder / "destination.py").write_text(_DEST_PY.format(name=name))
    (folder / "__init__.py").write_text(_INIT_PY)
    return folder


def scaffold_config(configs_dir: Path, name: str) -> Path:
    """Write a new config file ``configs_dir/<name>.yml`` — docs/12.

    Writes a one-config-per-file stub binding a placeholder source to the
    baked ``duckdb`` destination at the ``dev`` target. Refuses to overwrite
    an existing file. Returns the config file path.
    """
    configs_dir.mkdir(parents=True, exist_ok=True)
    path = configs_dir / f"{name}.yml"
    if path.exists():
        raise ScaffoldError(
            f"{path} already exists; choose a different config name"
        )
    path.write_text(_CONFIG_YML.format(name=name))
    return path
