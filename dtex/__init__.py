# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""dtex — a simple, open-source Python extract-load (EL) tool.

dtex moves data from a **source** into a **destination** and nothing more.
Pipelines are configs, connectors are folders of plain Python; projects are
folders of plain files run from the ``dtex`` CLI or the importable ``dtex``
library. Architectural inspiration is dbt.

This module is the **public API** a connector author imports. After build
stage 3 it exposes the decorator surface a connector body binds to —
``stream``, ``resource``, ``destination``, ``Connector``, ``stream_method`` —
plus the contract types an author references (``Capability``, ``Schema``,
``Field``, ``Config``, ``State``, ``Cursor``, ``Batch``, ``StateRecord`` and
the enums). A connector body should need to import only from ``dtex``::

    from dtex import stream, destination, Capability, Schema

As of build stage 5 it also exposes the engine entry point :func:`run` — the
library front door onto the run lifecycle (docs/02 §The triad). The CLI is a
thin shell over this same function. Stage 8.B made *configs* the runtime
unit — a config (``configs/<name>.yml``) names one source-to-destination
pipeline; ``run`` takes the config's name::

    import dtex
    result = dtex.run(config="shiphero_prod")

``run`` returns a :class:`~dtex.types.RunResult` and never raises on a
connector/destination failure (docs/07 §4.1).
"""

from __future__ import annotations

# Imported last: dtex.engine pulls in dtex.registry / dtex.types,
# which are already bound above — no import cycle. The engine is the library's
# run entry point (docs/02).
from dtex.engine import run, run_tag
from dtex.registry import (
    Connector,
    destination,
    resource,
    stream,
    stream_method,
)
from dtex.secrets import (
    SecretResolutionError,
    SecretResolver,
    register_secret_resolver,
)
from dtex.types import (
    Batch,
    Capability,
    CoercionError,
    Config,
    ConnectorKind,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    GaqlConfig,
    LeaseRecord,
    LeaseStatus,
    PartitionConfig,
    PartitionRange,
    PartitionType,
    PipelineConfig,
    Record,
    RunConfig,
    RunRecord,
    RunResult,
    RunStatus,
    Schema,
    SchemaContract,
    SigmaConfig,
    State,
    StateBackend,
    StateRecord,
    StreamDef,
    StreamMeta,
    StreamMode,
    StreamResult,
    StreamRunConfig,
    StreamStatus,
    TimeGranularity,
    WriteDisposition,
)

# Source of truth: pyproject.toml's [project] version. Read from the
# installed-package metadata so a hardcoded mirror cannot drift (which it did
# in 0.1.0/0.1.1 — `pyproject.toml` was bumped but this string was not, so
# `dtex --version` lied). importlib.metadata is stdlib in Python 3.11+.
try:
    from importlib.metadata import version as _pkg_version

    __version__ = _pkg_version("dtex")
except Exception:  # noqa: BLE001 — also covers PackageNotFoundError
    # Running from an unbuilt source tree where the package isn't installed.
    # The version is then unknown; fall back to a placeholder so importing
    # `dtex` doesn't blow up. The hardcoded fallback is acceptable here
    # because every published wheel/sdist HAS metadata — this branch only
    # fires in a uniquely broken local dev setup.
    __version__ = "0.0.0+unknown"

__all__ = [
    # Version
    "__version__",
    # Engine entry point (dtex.engine) — the library front door
    "run",
    "run_tag",
    # Decorator API surface (dtex.registry)
    "stream",
    "resource",
    "destination",
    "Connector",
    "stream_method",
    # Secret-resolver plugin surface (dtex.secrets — stage 9a, docs/08 §3)
    "SecretResolver",
    "SecretResolutionError",
    "register_secret_resolver",
    # Contract types a connector author references (dtex.types)
    "Batch",
    "Capability",
    "CoercionError",
    "Config",
    "ConnectorKind",
    "Cursor",
    "CursorType",
    "Field",
    "FieldMode",
    "FieldType",
    "GaqlConfig",
    "PartitionConfig",
    "PartitionRange",
    "PartitionType",
    "PipelineConfig",
    "Record",
    "RunConfig",
    "RunRecord",
    "RunResult",
    "RunStatus",
    "Schema",
    "SchemaContract",
    "SigmaConfig",
    "State",
    "LeaseRecord",
    "LeaseStatus",
    "StateBackend",
    "StateRecord",
    "StreamDef",
    "StreamMode",
    "StreamRunConfig",
    "StreamMeta",
    "StreamResult",
    "StreamStatus",
    "TimeGranularity",
    "WriteDisposition",
]
