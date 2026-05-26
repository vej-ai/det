# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""detx — a simple, open-source Python extract-load (EL) tool.

detx moves data from a **source** into a **destination** and nothing more.
Pipelines are configs, connectors are folders of plain Python; projects are
folders of plain files run from the ``detx`` CLI or the importable ``detx``
library. Architectural inspiration is dbt.

This module is the **public API** a connector author imports. After build
stage 3 it exposes the decorator surface a connector body binds to —
``stream``, ``resource``, ``destination``, ``Connector``, ``stream_method`` —
plus the contract types an author references (``Capability``, ``Schema``,
``Field``, ``Config``, ``State``, ``Cursor``, ``Batch``, ``StateRecord`` and
the enums). A connector body should need to import only from ``detx``::

    from detx import stream, destination, Capability, Schema

As of build stage 5 it also exposes the engine entry point :func:`run` — the
library front door onto the run lifecycle (docs/02 §The triad). The CLI is a
thin shell over this same function. Stage 8.B made *configs* the runtime
unit — a config (``configs/<name>.yml``) names one source-to-destination
pipeline; ``run`` takes the config's name::

    import detx
    result = detx.run(config="shiphero_prod")

``run`` returns a :class:`~detx.types.RunResult` and never raises on a
connector/destination failure (docs/07 §4.1).
"""

from __future__ import annotations

# Imported last: detx.engine pulls in detx.registry / detx.types,
# which are already bound above — no import cycle. The engine is the library's
# run entry point (docs/02).
from detx.engine import run, run_tag
from detx.registry import (
    Connector,
    destination,
    resource,
    stream,
    stream_method,
)
from detx.secrets import (
    SecretResolutionError,
    SecretResolver,
    register_secret_resolver,
)
from detx.types import (
    Batch,
    Capability,
    Config,
    ConnectorKind,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
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
    State,
    StateBackend,
    StateRecord,
    StreamMeta,
    StreamResult,
    StreamStatus,
    TimeGranularity,
    WriteDisposition,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    # Engine entry point (detx.engine) — the library front door
    "run",
    "run_tag",
    # Decorator API surface (detx.registry)
    "stream",
    "resource",
    "destination",
    "Connector",
    "stream_method",
    # Secret-resolver plugin surface (detx.secrets — stage 9a, docs/08 §3)
    "SecretResolver",
    "SecretResolutionError",
    "register_secret_resolver",
    # Contract types a connector author references (detx.types)
    "Batch",
    "Capability",
    "Config",
    "ConnectorKind",
    "Cursor",
    "CursorType",
    "Field",
    "FieldMode",
    "FieldType",
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
    "State",
    "StateBackend",
    "StateRecord",
    "StreamMeta",
    "StreamResult",
    "StreamStatus",
    "TimeGranularity",
    "WriteDisposition",
]
