"""det contract types — the core type definitions every module imports.

This module (``det/types.py``) is the authoritative, in-code expression
of the det connector contract. It contains *only* type definitions and their small intrinsic
behavior (enum parsing, schema lookup, cursor bookkeeping, validation). It
holds **no** engine logic: no YAML reading, no connector discovery, no run
loop. Those belong to later build stages.

Doc references in docstrings point at the design handbook under ``docs/``:
``docs/02-architecture.md``, ``docs/03-connector-contract.md``,
``docs/04-connector-body.md``, ``docs/05-destinations-and-state.md``,
``docs/07-cli-and-library-api.md``, ``docs/09-logging-and-observability.md``.

Design choices made where the handbook left a micro-detail ambiguous are
tagged with ``# NOTE:`` comments.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, StrEnum
from typing import Any, ClassVar, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Record = dict[str, Any]
"""One record: a flat dict — column names to JSON-serializable values.

Mirrors docs/04 "A record is a flat ``dict``". Nested data lives under a key
whose declared column ``type`` is :attr:`FieldType.JSON`.
"""

Batch = list[Record]
"""A batch: a ``list[dict]`` — the unit of transfer source → destination.

Mirrors docs/04 "A batch is a ``list[dict]``". A ``@stream`` generator yields
batches; ``@destination.write_batch`` receives one batch per call. There is
deliberately no heavier envelope object — per-batch metadata (table, schema,
write disposition) arrives as the hook's own arguments.
"""


# ---------------------------------------------------------------------------
# Enum base — string-valued enums that parse leniently from YAML scalars
# ---------------------------------------------------------------------------


class _StrEnum(StrEnum):
    """Base for string-valued enums parsed from ``register.yaml`` scalars.

    ``parse()`` accepts the enum value, the member name, and is
    case-insensitive — so a YAML author may write ``merge`` / ``MERGE`` /
    ``Merge`` interchangeably. This is the single coercion point for every
    contract enum loaded from YAML.

    # NOTE: subclasses ``enum.StrEnum`` (Python 3.11+, the project's floor) so
    # a member *is* a ``str`` — ``WriteDisposition.MERGE == "merge"`` and YAML
    # serialization is trivial. ``StrEnum`` preserves *explicit* string values
    # verbatim (it only lowercases ``auto()``), so ``FieldType.STRING`` keeps
    # its uppercase ``"STRING"`` value as the handbook requires.
    """

    @classmethod
    def parse(cls, value: Any) -> Any:
        """Coerce a YAML scalar (or an existing member) into a member.

        Raises :class:`ValueError` listing the valid options on a bad value,
        which is the message ``det validate`` surfaces to the author.
        """
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ValueError(
                f"{cls.__name__} expects a string, got {type(value).__name__}: {value!r}"
            )
        key = value.strip()
        for member in cls:
            if key == member.value or key.lower() == member.value.lower():
                return member
            if key.upper() == member.name:
                return member
        valid = ", ".join(repr(m.value) for m in cls)
        raise ValueError(f"{key!r} is not a valid {cls.__name__}; expected one of: {valid}")


# ---------------------------------------------------------------------------
# Enums / constants
# ---------------------------------------------------------------------------


class Capability(Enum):
    """A capability a destination connector declares — docs/05 §1, docs/09 §4.

    The set returned by ``@destination.capabilities`` fixes the destination's
    capability tier at init time and drives engine behavior (whether ``merge``
    is allowed, whether the destination hosts its own state table, etc.).

    # NOTE: docs/05 §1 originally enumerated four members. Stage 8a added
    # ``RUN_RECORDS`` (docs/09 §4) to gate the ``_det_runs`` audit table
    # without a contract break — destinations without it remain valid and
    # still produce the per-run JSONL log. The enum is now the source of
    # truth; docs/05 §1 follows it.
    """

    STATE = "state"
    """Destination can host the ``_det_state`` table itself (Tier A)."""
    MERGE = "merge"
    """Destination supports the ``merge`` (upsert) write disposition."""
    SCHEMA_EVOLUTION = "schema_evolution"
    """Destination can ``ALTER TABLE ADD COLUMN`` for additive evolution."""
    TRANSACTIONAL_LOAD = "transactional_load"
    """Destination can make a batch load + state commit atomic-ish."""
    RUN_RECORDS = "run_records"
    """Destination can host the ``_det_runs`` audit table itself (docs/09 §4).

    A destination that declares this capability MUST implement
    ``@destination.write_run_record`` — the engine calls it once per run, after
    streams finish and before ``close``, with a fully-built :class:`RunRecord`.
    Without this capability the engine still writes the per-run JSONL log file
    but skips the destination-side audit row.
    """


class WriteDisposition(_StrEnum):
    """How a stream's records land in the destination — docs/03 §2.2, docs/05 §4.

    ``merge`` additionally requires a ``primary_key`` and a destination with
    :attr:`Capability.MERGE`; both are validated by the engine at planning time.
    """

    APPEND = "append"
    """Insert all rows. Duplicates are the source's concern."""
    MERGE = "merge"
    """Upsert on ``primary_key``: insert new rows, overwrite matched rows."""
    REPLACE = "replace"
    """Truncate the table, then load — full-snapshot semantics."""


class FieldType(_StrEnum):
    """Logical column type — docs/03 §2.2.1.

    A small portable type system; the destination maps each member to a native
    warehouse type (docs/05 §3.1). Stored uppercase to match the handbook's
    schema-entry examples (``type: STRING``).

    # NOTE: ``BYTES`` is included beyond docs/03 §2.2.1's original 7-member
    # list — every target warehouse (DuckDB BLOB, BigQuery BYTES, Snowflake
    # BINARY, Postgres BYTEA) has a binary type, and binary-handling sources
    # (raw signatures, file bytes) need it. This module is the source of
    # truth; docs/03 §2.2.1 and docs/05 §3.1 follow it.
    """

    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    JSON = "JSON"
    BYTES = "BYTES"


class FieldMode(_StrEnum):
    """Column nullability / repetition — docs/03 §2.2.1.

    ``REPEATED`` targets destinations with a native array type; on destinations
    that lack one it degrades to a ``JSON`` column.
    """

    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"


class CursorType(_StrEnum):
    """How an incremental cursor value is compared and stored — docs/03 §2.2.

    Stored lowercase to match the handbook's ``incremental.cursor_type``
    examples and the ``_det_state.cursor_type`` column.
    """

    TIMESTAMP = "timestamp"
    DATE = "date"
    INT = "int"
    STRING = "string"


class ConnectorKind(_StrEnum):
    """Whether a connector reads from or writes to the outside world — docs/03 §2.1."""

    SOURCE = "source"
    """Exposes ``@stream`` functions that *yield* records."""
    DESTINATION = "destination"
    """Exposes ``@destination`` hooks that *accept* records."""


class SchemaContract(_StrEnum):
    """Per-stream schema-evolution policy — docs/05 §3.2.

    Locked design decision: the default is ``evolve`` (auto-additive); a stream
    may opt into ``strict`` in ``register.yaml`` via ``schema_contract: strict``.
    """

    EVOLVE = "evolve"
    """New source fields are added automatically with ``ALTER TABLE ADD COLUMN``."""
    STRICT = "strict"
    """Any schema difference from the existing table fails the run."""


class ParamType(_StrEnum):
    """The value type of a declared connector param — docs/03 §2.4.

    # NOTE: distinct from :class:`FieldType`. ``ParamType`` types *config knobs*
    # (``page_size``, ``start_date``) and has exactly four lowercase members per
    # docs/03 §2.4; ``FieldType`` types *table columns* and is uppercase per
    # docs/03 §2.2.1. They are deliberately not the same enum.
    """

    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"


class RunStatus(_StrEnum):
    """Terminal status of a run — docs/07 §4.1, docs/09 §4."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"


class StreamStatus(_StrEnum):
    """Terminal status of one stream within a run — docs/07 §4.1, docs/09 §2."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


# ---------------------------------------------------------------------------
# Schema types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Field:
    """One column definition — mirrors a ``register.yaml`` ``schema[]`` entry.

    docs/03 §2.2.1. Frozen because a declared schema is immutable contract data;
    schema *evolution* produces a new :class:`Schema`, it never mutates a field.
    """

    name: str
    type: FieldType
    mode: FieldMode = FieldMode.NULLABLE
    description: str = ""

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Field:
        """Build a :class:`Field` from a parsed YAML mapping — docs/03 §2.2.1.

        ``type`` is required; ``mode`` defaults to ``NULLABLE``; ``description``
        defaults to ``""``. Unknown keys raise, mirroring the handbook's
        "unknown keys are a hard error" discovery rule (docs/03 §7).
        """
        known = {"name", "type", "mode", "description"}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown schema field key(s): {', '.join(sorted(unknown))}")
        if "name" not in data:
            raise ValueError("schema field requires a 'name'")
        if "type" not in data:
            raise ValueError(f"schema field {data['name']!r} requires a 'type'")
        return cls(
            name=str(data["name"]),
            type=FieldType.parse(data["type"]),
            mode=FieldMode.parse(data.get("mode", FieldMode.NULLABLE)),
            description=str(data.get("description", "")),
        )


@dataclass(frozen=True)
class Schema:
    """An ordered list of :class:`Field` — a stream's column definitions.

    docs/03 §2.2.1 / docs/05 §3.1. ``Schema`` is the typed carrier of a
    stream's declared columns; a destination's ``ensure_schema`` translates it
    into native DDL. Frozen — evolution returns a new ``Schema``.
    """

    fields: tuple[Field, ...] = ()

    SYNCED_AT_COLUMN: ClassVar[str] = "_det_synced_at"
    """Name of the load-timestamp column the engine appends to every record.

    # NOTE: ``ClassVar`` — a true class constant, deliberately excluded from
    # the dataclass field set so it is not an ``__init__`` arg, not compared
    # by ``__eq__``, and not rebindable per instance.
    """

    @classmethod
    def from_list(cls, items: list[Mapping[str, Any]] | None) -> Schema | None:
        """Build a :class:`Schema` from a parsed YAML ``schema`` list.

        Returns ``None`` when ``items`` is ``None`` — the handbook's signal for
        "no declared schema, infer from the first batch" (docs/03 §2.2.1).
        """
        if items is None:
            return None
        return cls(fields=tuple(Field.from_dict(item) for item in items))

    def field(self, name: str) -> Field | None:
        """Look up a field by column name; ``None`` if absent."""
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def has(self, name: str) -> bool:
        """Whether a column of this name is declared."""
        return self.field(name) is not None

    @property
    def names(self) -> tuple[str, ...]:
        """The declared column names, in order."""
        return tuple(f.name for f in self.fields)

    @staticmethod
    def synced_at_field() -> Field:
        """The engine-appended ``_det_synced_at`` column — docs/03 §2.2.1.

        # NOTE: this column is *not* baked into a declared ``Schema`` at parse
        # time — that would conflate author-declared columns with the engine's
        # contribution. The engine calls :meth:`with_synced_at` just before
        # handing a schema to ``ensure_schema``, keeping the two concerns
        # separate. The column is ``NULLABLE`` so an existing table can have it
        # added by additive evolution.
        """
        return Field(
            name=Schema.SYNCED_AT_COLUMN,
            type=FieldType.TIMESTAMP,
            mode=FieldMode.NULLABLE,
            description="Engine-set load timestamp.",
        )

    def with_synced_at(self) -> Schema:
        """Return a new :class:`Schema` with ``_det_synced_at`` appended.

        Idempotent: if the column is already present the schema is returned
        unchanged, so re-appending on a resumed run is safe.
        """
        if self.has(Schema.SYNCED_AT_COLUMN):
            return self
        return Schema(fields=(*self.fields, self.synced_at_field()))

    def __iter__(self) -> Iterator[Field]:
        """Iterate the declared fields in order."""
        return iter(self.fields)

    def __len__(self) -> int:
        """Number of declared fields."""
        return len(self.fields)


# ---------------------------------------------------------------------------
# Manifest types — the parsed register.yaml (docs/03 §2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParamSpec:
    """A declared, typed configuration knob — mirrors ``register.yaml`` ``params``.

    docs/03 §2.4. ``required: true`` with no ``default`` and no supplied value
    fails discovery-time validation.
    """

    type: ParamType
    default: Any = None
    required: bool = False
    description: str = ""

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ParamSpec:
        """Build a :class:`ParamSpec` from a parsed YAML mapping — docs/03 §2.4."""
        known = {"type", "default", "required", "description"}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown param key(s): {', '.join(sorted(unknown))}")
        if "type" not in data:
            raise ValueError("param spec requires a 'type'")
        return cls(
            type=ParamType.parse(data["type"]),
            default=data.get("default"),
            required=bool(data.get("required", False)),
            description=str(data.get("description", "")),
        )


@dataclass(frozen=True)
class SecretRef:
    """A declared secret reference — mirrors a ``register.yaml`` ``secrets[]`` entry.

    docs/03 §2.5. The value is *referenced*, never inlined: ``ref`` is a
    resolution expression resolved lazily at run time.
    """

    name: str
    ref: str

    # NOTE: Locked design decision — exactly two resolver forms exist:
    # ``${env.X}`` and ``${profile.X.Y}``. No ``${secret.X}`` / vault form.
    # docs/03 §2.5 left this as an open question; the owner closed it.
    # ``ClassVar`` — true class constants, excluded from the dataclass fields.
    ENV_PREFIX: ClassVar[str] = "${env."
    PROFILE_PREFIX: ClassVar[str] = "${profile."

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SecretRef:
        """Build a :class:`SecretRef` from a parsed YAML mapping — docs/03 §2.5."""
        known = {"name", "ref"}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown secret key(s): {', '.join(sorted(unknown))}")
        if "name" not in data:
            raise ValueError("secret requires a 'name'")
        if "ref" not in data:
            raise ValueError(f"secret {data['name']!r} requires a 'ref'")
        ref = str(data["ref"])
        if not cls.is_valid_ref(ref):
            raise ValueError(
                f"secret {data['name']!r} ref {ref!r} must use a known resolver: "
                f"${{env.X}} or ${{profile.X.Y}}"
            )
        return cls(name=str(data["name"]), ref=ref)

    @classmethod
    def is_valid_ref(cls, ref: str) -> bool:
        """Whether ``ref`` uses one of the two supported resolver forms.

        This is the syntax check the engine applies at discovery (docs/03 §7
        step 5: "every ``secrets[].ref`` uses a known resolver form").
        """
        ref = ref.strip()
        if not ref.endswith("}"):
            return False
        return ref.startswith(cls.ENV_PREFIX) or ref.startswith(cls.PROFILE_PREFIX)


@dataclass(frozen=True)
class Incremental:
    """A stream's cursor-based incremental config — mirrors ``register.yaml``.

    docs/03 §2.2 "The ``incremental`` block". Absence of this block on a stream
    means a full table is fetched every run.
    """

    cursor_field: str
    cursor_type: CursorType = CursorType.TIMESTAMP
    lookback: str | None = None
    initial_value: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Incremental:
        """Build an :class:`Incremental` from a parsed YAML mapping — docs/03 §2.2."""
        known = {"cursor_field", "cursor_type", "lookback", "initial_value"}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown incremental key(s): {', '.join(sorted(unknown))}")
        if "cursor_field" not in data:
            raise ValueError("incremental block requires a 'cursor_field'")
        lookback = data.get("lookback")
        initial = data.get("initial_value")
        return cls(
            cursor_field=str(data["cursor_field"]),
            cursor_type=CursorType.parse(data.get("cursor_type", CursorType.TIMESTAMP)),
            lookback=None if lookback is None else str(lookback),
            initial_value=None if initial is None else str(initial),
        )


@dataclass(frozen=True)
class StreamDef:
    """One declared output table — mirrors a ``register.yaml`` ``streams[]`` entry.

    docs/03 §2.2. This is the static *declaration* of a stream; the matching
    ``@stream`` function in the connector body is its *implementation*.
    """

    name: str
    table: str
    primary_key: tuple[str, ...] = ()
    write_disposition: WriteDisposition = WriteDisposition.APPEND
    incremental: Incremental | None = None
    schema: Schema | None = None
    partition_by: str | None = None
    params: Mapping[str, ParamSpec] = field(default_factory=dict)
    schema_contract: SchemaContract = SchemaContract.EVOLVE

    def __post_init__(self) -> None:
        """Enforce stream-level integrity rules — docs/03 §7 step 4.

        ``merge`` requires a ``primary_key``; if both a ``schema`` and an
        ``incremental`` block are present, ``cursor_field`` must appear in the
        schema.
        """
        if self.write_disposition is WriteDisposition.MERGE and not self.primary_key:
            raise ValueError(
                f"stream {self.name!r}: write_disposition 'merge' requires a primary_key"
            )
        if self.incremental is not None and self.schema is not None:
            cf = self.incremental.cursor_field
            if not self.schema.has(cf):
                raise ValueError(
                    f"stream {self.name!r}: incremental cursor_field {cf!r} "
                    f"is not in the declared schema"
                )

    @property
    def is_incremental(self) -> bool:
        """Whether this stream declares an ``incremental`` block — docs/03 §3.2.

        Drives whether the engine injects a :class:`Cursor` into the ``@stream``
        function.
        """
        return self.incremental is not None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> StreamDef:
        """Build a :class:`StreamDef` from a parsed YAML mapping — docs/03 §2.2."""
        known = {
            "name",
            "table",
            "primary_key",
            "write_disposition",
            "incremental",
            "schema",
            "partition_by",
            "params",
            "schema_contract",
        }
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown stream key(s): {', '.join(sorted(unknown))}")
        if "name" not in data:
            raise ValueError("stream requires a 'name'")
        name = str(data["name"])

        pk_raw = data.get("primary_key")
        if pk_raw is None:
            primary_key: tuple[str, ...] = ()
        elif isinstance(pk_raw, str):
            primary_key = (pk_raw,)
        elif isinstance(pk_raw, (list, tuple)):
            primary_key = tuple(str(k) for k in pk_raw)
        else:
            raise ValueError(f"stream {name!r}: primary_key must be a string or list")

        incremental_raw = data.get("incremental")
        incremental = (
            Incremental.from_dict(incremental_raw) if incremental_raw is not None else None
        )

        params_raw = data.get("params") or {}
        params = {k: ParamSpec.from_dict(v) for k, v in params_raw.items()}

        return cls(
            name=name,
            table=str(data.get("table", name)),
            primary_key=primary_key,
            write_disposition=WriteDisposition.parse(
                data.get("write_disposition", WriteDisposition.APPEND)
            ),
            incremental=incremental,
            schema=Schema.from_list(data.get("schema")),
            partition_by=(
                None if data.get("partition_by") is None else str(data["partition_by"])
            ),
            params=params,
            schema_contract=SchemaContract.parse(
                data.get("schema_contract", SchemaContract.EVOLVE)
            ),
        )


@dataclass(frozen=True)
class StreamMeta:
    """The per-stream metadata a destination hook needs to write one stream.

    docs/05 §1 / docs/03 §3.4. The engine builds one ``StreamMeta`` per stream
    from the resolved :class:`StreamDef` and passes it to ``write_batch`` and
    ``ensure_schema``. It is the *single* metadata object those hooks receive —
    new per-stream concerns are added as fields here, never as new hook
    keyword arguments, so the destination contract stays stable as the engine
    grows. ``schema`` is the resolved, ready-to-write schema (engine-inferred
    when the stream declares none), distinct from :class:`StreamDef`'s optional
    declared schema.
    """

    table: str
    write_disposition: WriteDisposition
    schema: Schema
    primary_key: tuple[str, ...] = ()
    partition_by: str | None = None
    schema_contract: SchemaContract = SchemaContract.EVOLVE

    @classmethod
    def from_stream_def(cls, stream: StreamDef, schema: Schema) -> StreamMeta:
        """Build a :class:`StreamMeta` from a :class:`StreamDef` plus a resolved schema.

        ``schema`` is the engine's resolved schema for the stream — the declared
        one when present, otherwise the inferred one. Every other field is
        copied straight from the declaration.
        """
        return cls(
            table=stream.table,
            write_disposition=stream.write_disposition,
            schema=schema,
            primary_key=stream.primary_key,
            partition_by=stream.partition_by,
            schema_contract=stream.schema_contract,
        )


@dataclass(frozen=True)
class DestinationBinding:
    """Where a source's streams land — mirrors ``register.yaml`` ``destination``.

    docs/03 §2.3. ``connector`` names a ``kind: destination`` connector; any
    other keys are free-form routing params (e.g. ``dataset``) whose meaning is
    defined by that destination's own ``register.yaml``. The binding never
    names a project, host, or credential — those are environment concerns.

    # NOTE: as of build stage 8.B, ``DestinationBinding`` is *no longer* part of
    # the source contract — the source's ``register.yaml`` may not declare a
    # ``destination:`` block, and the project picks the destination via a
    # ``configs/<name>.yml`` file instead (docs/06, docs/12). The dataclass is
    # kept so older fixtures and external authors who still carry a
    # ``destination:`` block parse without error; the engine logs a warning when
    # it encounters one and otherwise ignores it. The field is expected to be
    # removed in a later stage.
    """

    connector: str
    routing: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DestinationBinding:
        """Build a :class:`DestinationBinding` from a parsed YAML mapping — docs/03 §2.3."""
        if "connector" not in data:
            raise ValueError("destination binding requires a 'connector'")
        routing = {k: v for k, v in data.items() if k != "connector"}
        return cls(connector=str(data["connector"]), routing=routing)


@dataclass(frozen=True)
class ConnectorManifest:
    """The parsed ``register.yaml`` — the connector discovery manifest.

    docs/03 §2.1. Carries exactly the eleven top-level keys the handbook
    fixes: ``name, kind, version, summary, tags, streams, destination,
    params, secrets, schedule, requires``. A ``kind: destination`` manifest
    uses the same eleven minus ``streams``/``destination``.
    """

    name: str
    kind: ConnectorKind
    version: str = "0.1.0"
    summary: str = ""
    tags: tuple[str, ...] = ()
    streams: tuple[StreamDef, ...] = ()
    destination: DestinationBinding | None = None
    params: Mapping[str, ParamSpec] = field(default_factory=dict)
    secrets: tuple[SecretRef, ...] = ()
    schedule: str | None = None
    requires: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Enforce ``kind``-consistency and stream-name uniqueness — docs/03 §7.

        Step 3 (kind consistency): ``source`` ⇒ ``streams`` non-empty;
        ``destination`` ⇒ no ``streams`` and no ``destination`` binding.
        Step 4 (stream integrity): stream ``name``s must be unique.
        """
        if self.kind is ConnectorKind.SOURCE:
            if not self.streams:
                raise ValueError(
                    f"connector {self.name!r}: kind 'source' requires a non-empty 'streams'"
                )
        else:  # ConnectorKind.DESTINATION
            if self.streams:
                raise ValueError(
                    f"connector {self.name!r}: kind 'destination' must not declare 'streams'"
                )
            if self.destination is not None:
                raise ValueError(
                    f"connector {self.name!r}: kind 'destination' must not declare a "
                    f"'destination' binding"
                )
        seen: set[str] = set()
        for s in self.streams:
            if s.name in seen:
                raise ValueError(
                    f"connector {self.name!r}: duplicate stream name {s.name!r}"
                )
            seen.add(s.name)

    def stream(self, name: str) -> StreamDef | None:
        """Look up a declared stream by name; ``None`` if absent."""
        for s in self.streams:
            if s.name == name:
                return s
        return None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ConnectorManifest:
        """Build a :class:`ConnectorManifest` from a parsed ``register.yaml``.

        Enforces docs/03 §7 step 2: every top-level key is known (unknown keys
        are a hard error, catching typos like ``write_dispostion``) and every
        required key is present.
        """
        known = {
            "name",
            "kind",
            "version",
            "summary",
            "tags",
            "streams",
            "destination",
            "params",
            "secrets",
            "schedule",
            "requires",
        }
        unknown = set(data) - known
        if unknown:
            raise ValueError(
                f"unknown register.yaml key(s): {', '.join(sorted(unknown))}"
            )
        if "name" not in data:
            raise ValueError("register.yaml requires a 'name'")
        if "kind" not in data:
            raise ValueError(f"connector {data['name']!r}: register.yaml requires a 'kind'")

        tags_raw = data.get("tags") or []
        requires_raw = data.get("requires") or []
        streams_raw = data.get("streams") or []
        params_raw = data.get("params") or {}
        secrets_raw = data.get("secrets") or []
        dest_raw = data.get("destination")
        schedule = data.get("schedule")

        return cls(
            name=str(data["name"]),
            kind=ConnectorKind.parse(data["kind"]),
            version=str(data.get("version", "0.1.0")),
            summary=str(data.get("summary", "")),
            tags=tuple(str(t) for t in tags_raw),
            streams=tuple(StreamDef.from_dict(s) for s in streams_raw),
            destination=(
                DestinationBinding.from_dict(dest_raw) if dest_raw is not None else None
            ),
            params={k: ParamSpec.from_dict(v) for k, v in params_raw.items()},
            secrets=tuple(SecretRef.from_dict(s) for s in secrets_raw),
            schedule=None if schedule is None else str(schedule),
            requires=tuple(str(r) for r in requires_raw),
        )


# ---------------------------------------------------------------------------
# Runtime types — passed to connector code at run time
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    """Resolved, immutable params + secrets handed to connector code — docs/03 §3, §6.

    The engine resolves every config layer (register.yaml defaults → profiles →
    project → CLI/run kwargs), type-checks each value, resolves secret refs, and
    hands the connector body this single immutable object. The connector never
    parses YAML, reads env vars, or touches files itself.

    Access patterns (docs/03 §3 "Injected arguments"):

    * ``config.page_size`` — attribute access reads a resolved *param*. This is
      sugar for ``config.params["page_size"]``.
    * ``config.secrets["api_token"]`` — secrets are read by explicit subscript,
      never as attributes, so secret access always reads as such in connector
      code (and is harder to leak by an accidental ``repr``).
    """

    params: Mapping[str, Any] = field(default_factory=dict)
    secrets: Mapping[str, str] = field(default_factory=dict)

    def __getattr__(self, name: str) -> Any:
        """Resolve an unknown attribute as a param — ``config.page_size``.

        # NOTE: ``__getattr__`` runs only when normal lookup fails, so the
        # real ``params``/``secrets`` fields are never shadowed. A missing
        # param raises :class:`AttributeError` (not ``KeyError``) so it behaves
        # like any other attribute miss and so ``hasattr`` works as expected.
        """
        try:
            params: Mapping[str, Any] = object.__getattribute__(self, "params")
        except AttributeError:  # during unpickling / partial init
            raise AttributeError(name) from None
        if name in params:
            return params[name]
        raise AttributeError(f"Config has no param {name!r}")

    def get(self, name: str, default: Any = None) -> Any:
        """Return a resolved param value, or ``default`` if it is not set."""
        return self.params.get(name, default)

    def has_secret(self, name: str) -> bool:
        """Whether a secret of this logical name was resolved."""
        return name in self.secrets


class State:
    """Per-stream persisted key/value scratch space — docs/03 §3, docs/04.

    Free-form, *untyped* memory that survives between runs. The connector reads
    and writes it freely; the engine persists its contents as the
    ``state_blob`` JSON column of ``_det_state`` after batches durably
    land. Scoped per ``(connector, stream)``.

    Unlike :class:`Cursor` (the *typed* incremental slice), ``State`` holds
    anything else a stream must remember — a vendor pagination token, a
    high-water id the API exposes instead of a timestamp.

    # NOTE: ``State`` is intentionally *not* frozen — connector code mutates it.
    # It is a thin dict wrapper rather than a bare dict so the engine can
    # snapshot it (``to_dict``) and detect changes without exposing internals.
    """

    def __init__(self, initial: Mapping[str, Any] | None = None) -> None:
        """Create state, optionally seeded from a prior run's ``state_blob``."""
        self._data: dict[str, Any] = dict(initial or {})

    def __getitem__(self, key: str) -> Any:
        """Read a state value; raises :class:`KeyError` if absent."""
        return self._data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Write a state value (must be JSON-serializable for persistence)."""
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        """Remove a state value."""
        del self._data[key]

    def __contains__(self, key: str) -> bool:
        """Whether a key is present in state."""
        return key in self._data

    def __len__(self) -> int:
        """Number of stored keys."""
        return len(self._data)

    def __iter__(self) -> Iterator[str]:
        """Iterate stored keys."""
        return iter(self._data)

    def __eq__(self, other: object) -> bool:
        """Two :class:`State` objects are equal when their contents match."""
        if isinstance(other, State):
            return self._data == other._data
        return NotImplemented

    def __repr__(self) -> str:
        """Developer-readable representation listing the stored keys."""
        return f"State({self._data!r})"

    def get(self, key: str, default: Any = None) -> Any:
        """Read a state value, or ``default`` if the key is absent."""
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Write a state value — the imperative form of ``state[key] = value``."""
        self._data[key] = value

    def to_dict(self) -> dict[str, Any]:
        """Return a shallow copy of the state contents for persistence."""
        return dict(self._data)


class Cursor:
    """The incremental cursor helper injected into ``@stream`` — docs/03 §3.2.

    A ``Cursor`` is present only for streams that declare an ``incremental``
    block. It turns incremental loading into a three-line affair: read the
    resume point with :meth:`start_value`, report each record's cursor value
    with :meth:`observe`, and the engine persists the observed max after the
    batches durably land. The connector never writes cursor state itself.

    # NOTE: ``Cursor`` is deliberately *dumb*. ``start_value()`` returns
    # whatever the engine handed in at construction — the engine has already
    # applied the ``lookback`` subtraction and ``initial_value`` fallback
    # (parsing ``"2d"`` etc. is engine work, not a contract-type concern, per
    # docs/03 §3.2). The Cursor only tracks the observed max for the engine to
    # read back after the generator is exhausted.
    """

    def __init__(
        self,
        cursor_field: str,
        cursor_type: CursorType,
        start_value: Any = None,
        is_full_refresh: bool = False,
    ) -> None:
        """Create a cursor for one stream.

        ``start_value`` is the engine-computed resume point (last committed
        cursor minus lookback, or ``initial_value`` on the first run, or
        ``None`` under ``--full-refresh``).
        """
        self._cursor_field = cursor_field
        self._cursor_type = cursor_type
        self._start_value = start_value
        self._is_full_refresh = is_full_refresh
        self._observed_max: Any = None
        self._observed_any = False

    @property
    def cursor_field(self) -> str:
        """The record field this cursor tracks (e.g. ``created_date``)."""
        return self._cursor_field

    @property
    def cursor_type(self) -> CursorType:
        """How this cursor's values are compared and stored."""
        return self._cursor_type

    @property
    def is_full_refresh(self) -> bool:
        """``True`` when the run was invoked with ``--full-refresh`` — docs/03 §3.2."""
        return self._is_full_refresh

    def start_value(self) -> Any:
        """Where to resume — docs/03 §3.2.

        The last persisted cursor value minus ``lookback``, or
        ``incremental.initial_value`` on the first run. ``None`` under
        ``--full-refresh`` (start from the beginning).
        """
        if self._is_full_refresh:
            return None
        return self._start_value

    def observe(self, value: Any) -> None:
        """Report a record's cursor-field value — docs/03 §3.2.

        The engine tracks the maximum across every observed value; that maximum
        becomes the next run's resume point once the batches durably land.
        ``None`` values are ignored (a record missing its cursor field must not
        drag the cursor backward).
        """
        if value is None:
            return
        if not self._observed_any or value > self._observed_max:
            self._observed_max = value
            self._observed_any = True

    @property
    def observed_max(self) -> Any:
        """The maximum value seen via :meth:`observe`; ``None`` if none observed.

        Read by the engine after the ``@stream`` generator is exhausted, to
        persist as the new cursor value.
        """
        return self._observed_max


@runtime_checkable
class StateBackend(Protocol):
    """The state-I/O contract for Tier-B destinations — docs/05 §5.4.

    Object-storage destinations cannot host the ``_det_state`` table, so
    the engine routes state through a companion backend implementing this
    Protocol. A destination's ``@destination.state_backend`` hook returns one
    (or ``None`` when the destination is Tier A and is its own backend).
    """

    def read_state(self, connector: str) -> list[StateRecord]:
        """Load every prior :class:`StateRecord` for a connector at run start."""
        ...

    def commit_state(self, run_id: str, records: list[StateRecord]) -> None:
        """Persist the run's :class:`StateRecord` set after all batches land."""
        ...


@dataclass
class StateRecord:
    """One ``_det_state`` row — the per-stream resume point.

    Passed between the engine and a destination's ``read_state`` /
    ``commit_state`` hooks. Primary key is ``(connector, stream)``.

    # NOTE: docs/03 §3.5 and docs/05 §5.1 originally gave *different* column
    # sets. The canonical schema is their merged union — eight columns:
    # connector, stream, cursor_value, cursor_type, state_blob, last_run_id,
    # rows_total, updated_at. ``cursor_field`` (docs/05) is excluded — it is
    # recoverable from the stream's manifest. ``last_run_at`` (docs/03) is
    # excluded — it is recoverable by joining ``_det_runs`` on
    # ``last_run_id``. Every retained column is load-bearing: ``cursor_type``
    # is required to deserialize ``cursor_value`` correctly, ``last_run_id``
    # links state to the run record's audit chain, ``state_blob`` persists the
    # per-stream ``State`` scratch space. This module is now the source of
    # truth; docs/03 §3.5 and docs/05 §5.1-5.2 follow it.

    # NOTE: not frozen because the engine advances ``rows_total`` /
    # ``updated_at`` in place across a run; a frozen variant would force a
    # rebuild per stream. Round-trips via ``to_row`` / ``from_row`` are the
    # persistence boundary.
    """

    connector: str
    stream: str
    cursor_value: Any = None
    cursor_type: CursorType | None = None
    state_blob: Mapping[str, Any] = field(default_factory=dict)
    last_run_id: str | None = None
    rows_total: int = 0
    updated_at: datetime | None = None

    def to_row(self) -> dict[str, Any]:
        """Serialize to a plain dict matching the ``_det_state`` columns.

        ``cursor_type`` becomes its string value; timestamps become ISO-8601
        strings; this is the JSON-friendly shape a destination writes.
        """
        return {
            "connector": self.connector,
            "stream": self.stream,
            "cursor_value": self.cursor_value,
            "cursor_type": None if self.cursor_type is None else self.cursor_type.value,
            "state_blob": dict(self.state_blob),
            "last_run_id": self.last_run_id,
            "rows_total": self.rows_total,
            "updated_at": None if self.updated_at is None else self.updated_at.isoformat(),
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> StateRecord:
        """Deserialize a ``_det_state`` row read back from a destination.

        Inverse of :meth:`to_row`. ``cursor_type`` and timestamps are parsed
        back into their typed forms; absent optional columns become ``None``.
        """
        ct = row.get("cursor_type")
        return cls(
            connector=str(row["connector"]),
            stream=str(row["stream"]),
            cursor_value=row.get("cursor_value"),
            cursor_type=None if ct is None else CursorType.parse(ct),
            state_blob=dict(row.get("state_blob") or {}),
            last_run_id=None if row.get("last_run_id") is None else str(row["last_run_id"]),
            rows_total=int(row.get("rows_total") or 0),
            updated_at=_parse_dt(row.get("updated_at")),
        )


def _parse_dt(value: Any) -> datetime | None:
    """Parse a timestamp column back into a :class:`datetime` (or ``None``)."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


@dataclass(frozen=True)
class RunConfig:
    """The frozen, fully resolved config for one run — docs/02 stage 2 RESOLVE.

    The engine merges every config layer (register.yaml defaults → project →
    profiles → env → CLI/run kwargs) into this single immutable object; after
    it is built, nothing reads ambient config. It names the run's *intent*
    (which pipeline, which source, which target, which streams, full-refresh or
    not) and carries the resolved :class:`Config` handed to connector code.

    # NOTE: ``connector`` keeps meaning "source connector name" — the
    # ``_det_state.connector`` column is keyed by source so a re-run of a
    # different config against the same source reuses state correctly. The
    # ``pipeline`` field names the :class:`PipelineConfig` the run was driven
    # from (the CLI's ``-p/--conf`` arg).
    """

    run_id: str
    pipeline: str
    connector: str
    target: str
    config: Config
    select: tuple[str, ...] = ()
    full_refresh: bool = False
    dry_run: bool = False

    @property
    def is_select_all(self) -> bool:
        """``True`` when no ``--select`` subset was given — run every stream."""
        return not self.select

    def selects(self, stream_name: str) -> bool:
        """Whether ``stream_name`` is in scope for this run — docs/07 §2 ``--select``."""
        return self.is_select_all or stream_name in self.select


@dataclass
class StreamResult:
    """Per-stream outcome inside a :class:`RunResult` — docs/07 §4.1, docs/09 §2.

    The structured record of one stream's run: how many rows moved and where
    its cursor stood before and after.
    """

    name: str
    rows_extracted: int = 0
    rows_loaded: int = 0
    cursor_before: Any = None
    cursor_after: Any = None
    status: StreamStatus = StreamStatus.SUCCEEDED

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the per-stream JSON shape of the run record — docs/09 §4."""
        return {
            "name": self.name,
            "rows_extracted": self.rows_extracted,
            "rows_loaded": self.rows_loaded,
            "cursor_before": self.cursor_before,
            "cursor_after": self.cursor_after,
            "status": self.status.value,
        }


@dataclass
class RunResult:
    """The structured outcome of one run — docs/07 §4.1, docs/09 §4.

    The same object ``project.run()`` returns and the engine persists as the
    run record (``_det_runs`` row / ``.det/runs/<run_id>.json``).
    ``project.run()`` does **not** raise on failure — it returns a ``RunResult``
    with ``status=FAILED`` and a populated ``error``.

    # NOTE: field set follows docs/07 §4.1 (the canonical ``RunResult``
    # definition the handbook gives in code), which includes ``target`` and
    # ``log_path``. The task description's prose bullet omitted those two; the
    # explicit handbook dataclass wins. ``destination`` is added because the
    # docs/09 §4 run-record JSON carries it and it is not otherwise
    # recoverable from this object alone.
    """

    run_id: str
    config: str
    connector: str
    target: str
    destination: str
    status: RunStatus
    started_at: datetime
    ended_at: datetime
    streams: list[StreamResult] = field(default_factory=list)
    rows_loaded: int = 0
    full_refresh: bool = False
    error: BaseException | None = None
    log_path: str = ""

    @property
    def duration_s(self) -> float:
        """Wall-clock run duration in seconds — docs/09 §4 ``duration_s``."""
        return (self.ended_at - self.started_at).total_seconds()

    def stream(self, name: str) -> StreamResult | None:
        """Look up a stream's result by name; ``None`` if absent."""
        for s in self.streams:
            if s.name == name:
                return s
        return None

    def raise_for_status(self) -> RunResult:
        """Raise :attr:`error` if the run failed; otherwise return ``self``.

        The opt-in exception path described in docs/07 §4.1 — callers wanting
        exceptions use ``project.run(...).raise_for_status()``.
        """
        if self.status is RunStatus.FAILED and self.error is not None:
            raise self.error
        return self

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the run-record JSON shape — docs/09 §4.

        ``error`` is rendered as ``"<ExcType>: <message>"`` (or ``None``); the
        logging layer is responsible for secret redaction within it (docs/09 §5).
        """
        return {
            "run_id": self.run_id,
            "config": self.config,
            "connector": self.connector,
            "target": self.target,
            "destination": self.destination,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "duration_s": self.duration_s,
            "rows_loaded": self.rows_loaded,
            "full_refresh": self.full_refresh,
            "streams": [s.to_dict() for s in self.streams],
            "error": (
                None
                if self.error is None
                else f"{type(self.error).__name__}: {self.error}"
            ),
        }


# ---------------------------------------------------------------------------
# RunRecord — the persistence-layer twin of RunResult (docs/09 §4)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunRecord:
    """One row in ``_det_runs`` — the queryable audit record of one run.

    docs/09 §4: where :class:`RunResult` is the in-memory shape the library
    returns and the JSONL file is the *narrative*, ``RunRecord`` is the
    *receipt* — the destination-persisted summary one row of ``_det_runs``
    carries. The engine builds one from the completed :class:`RunResult`
    and hands it to ``@destination.write_run_record``.

    # NOTE: ``traceback`` is intentionally **not** a field. A full traceback is
    # often huge, frequently embeds filesystem paths and identifiers, and would
    # bloat every ``_det_runs`` row in a way that confounds SQL drill-down.
    # The traceback IS written (once, in full) to the per-run JSONL log file
    # (docs/09 §3.2) — the JSONL is for forensics, the table is for
    # queryability. ``error_type`` + ``error_message`` give a SQL caller the
    # filterable shape ("which runs failed with ``HTTPError``?") without
    # forcing a join through unstructured text.

    # NOTE: frozen because once a run finishes the record is the canonical
    # immutable artifact of that run. A retry of the destination write
    # rebinds the same record; an in-place mutation would muddy the audit
    # promise.
    """

    run_id: str
    config: str
    source: str
    destination: str
    target: str
    status: RunStatus
    started_at: datetime
    ended_at: datetime
    rows_loaded: int
    streams: tuple[StreamResult, ...] = ()
    full_refresh: bool = False
    error_type: str | None = None
    error_message: str | None = None

    @property
    def duration_s(self) -> float:
        """Wall-clock run duration in seconds — docs/09 §4 ``duration_s``."""
        return (self.ended_at - self.started_at).total_seconds()

    def streams_json(self) -> list[dict[str, Any]]:
        """The per-stream breakdown for the ``streams_json`` table column.

        docs/09 §4: a JSON array under one column so an admin/UI can drill
        down without joining. Uses :meth:`StreamResult.to_dict` so the per-
        stream shape matches the JSONL log's ``stream_end`` event exactly.
        """
        return [s.to_dict() for s in self.streams]


# ---------------------------------------------------------------------------
# PipelineConfig — the parsed configs/<name>.yml file (docs/06, docs/12)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineConfig:
    """One parsed pipeline config — a named binding of source + destination + target.

    docs/12 §The config concept. A *config* is the runtime unit: one config =
    one pipeline. It names which source feeds which destination at which
    target, plus the params that customize both ends.

    Discovery scans ``configs/*.yml`` and ``configs/*.yaml`` under the project
    root; a file may carry one config (top-level keys ``name`` / ``source`` /
    …) or many (under a ``configs:`` list). Duplicate names across files are a
    hard error at discovery time. The engine then resolves a config NAME to
    one of these and runs its lifecycle.

    # NOTE: ``PipelineConfig`` is the new first-class concept of stage 8.B.
    # New per-pipeline concerns are added as fields here, never as new CLI
    # flags or engine ``run()`` args — the same stability rule
    # :class:`StreamMeta` follows for the destination contract.

    Fields:

    * ``name`` — config name; the CLI's ``-p/--conf`` matches this.
    * ``source`` — source connector name (resolved project-local-first,
      then baked — docs/03 §5).
    * ``destination`` — destination connector name (same resolution rule).
    * ``target`` — which ``profiles.yml[<destination>].targets[<target>]``
      block supplies the destination's connection params. ``None`` falls
      back to ``profiles.yml[<destination>].default_target`` (docs/06).
    * ``params`` — source param overrides (a higher precedence layer than
      ``register.yaml`` defaults and ``det_project.yml`` ``vars``; lower
      than CLI ``--param`` / ``run(params_override=)``).
    * ``destination_params`` — per-config destination param overrides
      (higher than ``profiles.yml`` rows, lower than CLI
      ``--destination-param``).
    * ``select`` — streams to run; empty means "all". A CLI ``--select``
      *replaces* (not unions) this list (docs/07).
    * ``schedule`` — advisory cron expression for an external scheduler;
      the engine itself never acts on it (docs/03 §2.6).
    """

    name: str
    source: str
    destination: str
    target: str | None = None
    params: Mapping[str, Any] = field(default_factory=dict)
    destination_params: Mapping[str, Any] = field(default_factory=dict)
    select: tuple[str, ...] = ()
    schedule: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PipelineConfig:
        """Build a :class:`PipelineConfig` from a parsed YAML mapping — docs/12.

        Enforces the required keys (``name``, ``source``, ``destination``) and
        rejects unknown top-level keys (catches typos like ``destintion``).
        Coerces ``select`` from a string-or-list scalar into a tuple.
        """
        known = {
            "name",
            "source",
            "destination",
            "target",
            "params",
            "destination_params",
            "select",
            "schedule",
        }
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"unknown config key(s): {', '.join(sorted(unknown))}")
        for required in ("name", "source", "destination"):
            if required not in data:
                raise ValueError(f"config requires a {required!r} key")

        select_raw = data.get("select") or ()
        if isinstance(select_raw, str):
            select: tuple[str, ...] = (select_raw,)
        elif isinstance(select_raw, (list, tuple)):
            select = tuple(str(s) for s in select_raw)
        else:
            raise ValueError(
                f"config {data['name']!r}: 'select' must be a string or list of stream names"
            )

        target_raw = data.get("target")
        schedule_raw = data.get("schedule")
        return cls(
            name=str(data["name"]),
            source=str(data["source"]),
            destination=str(data["destination"]),
            target=None if target_raw is None else str(target_raw),
            params=dict(data.get("params") or {}),
            destination_params=dict(data.get("destination_params") or {}),
            select=select,
            schedule=None if schedule_raw is None else str(schedule_raw),
        )
