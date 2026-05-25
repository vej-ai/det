"""det — a simple, open-source Python extract-load (EL) tool.

det moves data from a **source** into a **destination** and nothing more —
"dlt meets dbt". Connectors are folders of plain Python; projects are folders
of plain files run from the ``det`` CLI or the importable ``det``
library.

This module is the **public API** a connector author imports. After build
stage 3 it exposes the decorator surface a connector body binds to —
``stream``, ``resource``, ``destination``, ``Connector``, ``stream_method`` —
plus the contract types an author references (``Capability``, ``Schema``,
``Field``, ``Config``, ``State``, ``Cursor``, ``Batch``, ``StateRecord`` and
the enums). A connector body should need to import only from ``det``::

    from det import stream, destination, Capability, Schema

As of build stage 5 it also exposes the engine entry point :func:`run` — the
library front door onto the run lifecycle (docs/02 §The triad). The CLI is a
thin shell over this same function. Stage 8.B made *configs* the runtime
unit — a config (``configs/<name>.yml``) names one source-to-destination
pipeline; ``run`` takes the config's name::

    import det
    result = det.run(config="shiphero_prod")

``run`` returns a :class:`~det.types.RunResult` and never raises on a
connector/destination failure (docs/07 §4.1).
"""

from __future__ import annotations

# Imported last: det.engine pulls in det.registry / det.types,
# which are already bound above — no import cycle. The engine is the library's
# run entry point (docs/02).
from det.engine import run
from det.registry import (
    Connector,
    destination,
    resource,
    stream,
    stream_method,
)
from det.types import (
    Batch,
    Capability,
    Config,
    ConnectorKind,
    Cursor,
    CursorType,
    Field,
    FieldMode,
    FieldType,
    PipelineConfig,
    Record,
    RunConfig,
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
    WriteDisposition,
)

__version__ = "0.1.0"

__all__ = [
    # Version
    "__version__",
    # Engine entry point (det.engine) — the library front door
    "run",
    # Decorator API surface (det.registry)
    "stream",
    "resource",
    "destination",
    "Connector",
    "stream_method",
    # Contract types a connector author references (det.types)
    "Batch",
    "Capability",
    "Config",
    "ConnectorKind",
    "Cursor",
    "CursorType",
    "Field",
    "FieldMode",
    "FieldType",
    "PipelineConfig",
    "Record",
    "RunConfig",
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
    "WriteDisposition",
]
