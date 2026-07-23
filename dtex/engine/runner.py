# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The run loop — the 6-stage run lifecycle (docs/02 §Run lifecycle).

This module is the engine's keystone: :func:`run` executes one synchronous pass
of the lifecycle docs/02 fixes — DISCOVER → RESOLVE → INIT DEST → LOAD STATE →
RUN STREAMS → RUN RECORD — and returns a :class:`~dtex.types.RunResult`.

Stage 8.B made *configs* the runtime unit: :func:`run` takes a config NAME (the
``-p/--conf`` arg of the CLI), looks it up under ``configs/``, and drives the
source → destination binding the config defines (docs/12). The lifecycle
itself is unchanged.

The destination hooks are driven in the exact order docs/03 §3.4 / docs/05 §1
fix::

    open → read_state → [ensure_schema → write_batch ...]* → commit_state → write_run_record → close

(``write_run_record`` is added at stage 8a — docs/09 §4. It is conditional
on ``Capability.RUN_RECORDS``; without that capability the engine still
writes the per-run JSONL log file but skips the destination-side audit row.)

Locked decisions honored here:

* **Per-stream state commit** — a stream's cursor is committed via
  ``commit_state`` *immediately* after that stream's batches durably land. A
  later stream failing does not lose an earlier stream's progress, and a re-run
  resumes correctly (docs/02 §Commit granularity).
* **Sequential streams** — streams run one at a time in declared order
  (docs/02 §Concurrency model, v1).
* **Schema evolution default ``evolve``** — a stream with no declared schema has
  one inferred from its first batch; a ``strict`` stream whose first batch
  diverges from its declared schema fails the run *before* ``ensure_schema``.
* **``close`` always runs** — in a ``finally``, even on failure, but only when
  ``open`` succeeded (docs/05 §1).

``run`` never raises on a connector or destination failure: it returns a
``RunResult`` with ``status=FAILED`` and a populated ``error`` (docs/07 §4.1).
"""

from __future__ import annotations

import logging
import sys
import threading
import traceback as _tb
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import (
    CancelledError,
    Future,
    ThreadPoolExecutor,
    as_completed,
    wait,
)
from contextlib import nullcontext
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from typing import Any, TextIO

from dtex.engine import config as cfg
from dtex.engine import configs as cfgs
from dtex.engine import discovery as disc
from dtex.engine.logger import Redactor, RunLog, build_logger
from dtex.engine.normalize import normalize_batch
from dtex.registry import compute_injection
from dtex.secrets import load_project_plugins
from dtex.types import (
    LEASE_HEARTBEAT_INTERVAL_SECONDS,
    STATE_COMMIT_INTERVAL_SECONDS,
    Batch,
    Capability,
    Config,
    Cursor,
    CursorType,
    Field,
    FieldType,
    LeaseRecord,
    LeaseStatus,
    PartitionConfig,
    PartitionType,
    PipelineConfig,
    RunConfig,
    RunRecord,
    RunResult,
    RunStatus,
    Schema,
    SchemaContract,
    State,
    StateRecord,
    StreamDef,
    StreamMeta,
    StreamMode,
    StreamResult,
    StreamRunConfig,
    StreamStatus,
    TimeGranularity,
)

# The destination hooks the engine drives in a non-state-aware run. Tier A
# destinations (Capability.STATE) additionally need read_state / commit_state;
# that conditional rule is applied in _resolve_destination_hooks (docs/03 §3.4
# leaves the capability-dependent check to the engine).
_CORE_HOOKS = ("capabilities", "open", "ensure_schema", "write_batch", "close")
_STATE_HOOKS = ("read_state", "commit_state")
_LEASE_HOOKS = (
    "read_leases",
    "acquire_leases",
    "heartbeat_leases",
    "release_leases",
)

# How often the parallel dispatcher's main thread wakes to pump the batched
# lease heartbeat. Deliberately much shorter than
# LEASE_HEARTBEAT_INTERVAL_SECONDS: this is only the *polling* cadence, and
# the beat itself is self-throttled to the real interval, so a short poll
# costs one cheap comparison and keeps a long-running stream's lease fresh
# even when no stream completes for minutes.
_LEASE_PUMP_INTERVAL_SECONDS = 5.0


class EngineError(Exception):
    """A run could not start, or a destination is unusable for this run.

    Raised inside :func:`run` for problems the engine itself detects (a missing
    mandatory hook, a destination that cannot host state). It is caught within
    :func:`run` and folded into the ``FAILED`` :class:`RunResult` — it never
    escapes to the caller.
    """


# ---------------------------------------------------------------------------
# Cursor seeding — the engine's resume-point logic (docs/03 §3.2)
# ---------------------------------------------------------------------------


def _seed_value(
    prior: StateRecord | None, cursor_type: CursorType, initial_value: str | None
) -> Any:
    """Compute an incremental stream's resume point — docs/03 §3.2.

    The engine owns this (the smoke test's ``_resume_value`` was the manual
    stand-in): the last committed cursor on a resumed run, else the manifest's
    ``initial_value`` typed per ``cursor_type`` on the first run, else ``None``.
    """
    if prior is not None and prior.cursor_value is not None:
        return prior.cursor_value
    if initial_value is None:
        return None
    if cursor_type is CursorType.INT:
        return int(initial_value)
    if cursor_type is CursorType.DATE:
        return date.fromisoformat(initial_value)
    if cursor_type is CursorType.TIMESTAMP:
        return datetime.fromisoformat(initial_value)
    return initial_value  # CursorType.STRING — verbatim.


# ---------------------------------------------------------------------------
# Schema resolution — declared vs inferred, strict vs evolve (docs/02, docs/05 §3.2)
# ---------------------------------------------------------------------------

_PY_TO_FIELD_TYPE: tuple[tuple[type, FieldType], ...] = (
    (bool, FieldType.BOOLEAN),
    (int, FieldType.INTEGER),
    (float, FieldType.FLOAT),
    (datetime, FieldType.TIMESTAMP),
    (date, FieldType.DATE),
    (str, FieldType.STRING),
    (dict, FieldType.JSON),
    (list, FieldType.JSON),
)


def _infer_field_type(value: Any) -> FieldType:
    """Infer a column's :class:`FieldType` from one sample value — docs/02 §Normalize."""
    if value is None:
        return FieldType.STRING
    for py_type, field_type in _PY_TO_FIELD_TYPE:
        if isinstance(value, py_type):
            return field_type
    return FieldType.STRING


def _infer_schema(first_batch: Batch) -> Schema:
    """Infer a :class:`Schema` from a stream's first batch — docs/02 §Normalize."""
    columns: list[str] = []
    types: dict[str, FieldType] = {}
    for record in first_batch:
        for key, value in record.items():
            if key not in types:
                columns.append(key)
                types[key] = _infer_field_type(value)
            elif types[key] is FieldType.STRING and value is not None:
                types[key] = _infer_field_type(value)
    return Schema(fields=tuple(Field(name=c, type=types[c]) for c in columns))


def _check_strict_schema(stream: StreamDef, declared: Schema, first_batch: Batch) -> None:
    """Fail a ``strict`` stream whose first batch diverges from its schema — docs/05 §3.2."""
    declared_names = set(declared.names)
    unexpected: set[str] = set()
    for record in first_batch:
        unexpected.update(k for k in record if k not in declared_names)
    if unexpected:
        raise EngineError(
            f"stream {stream.name!r} is schema_contract 'strict' but its records "
            f"carry undeclared column(s) {', '.join(sorted(unexpected))}; "
            f"a strict stream's schema may not diverge (docs/05 §3.2)"
        )


# ---------------------------------------------------------------------------
# Partition resolution — source default vs config override vs auto-default
# (docs/05 §3.x)
# ---------------------------------------------------------------------------

# Cursor types that map to a sensible TIME+DAY auto-default. Other cursor
# types (INT, STRING) cannot be physically time-partitioned, so the auto-
# default leaves the stream unpartitioned and logs a warning that names the
# explicit long-form declaration the user should add.
_TIME_PARTITION_CURSOR_TYPES = frozenset({CursorType.TIMESTAMP, CursorType.DATE})


def _resolve_stream_mode(
    stream_def: StreamDef,
    pipeline: PipelineConfig,
    run_full_refresh: bool,
) -> StreamMode:
    """Resolve a stream's effective mode for this run — docs/12 §3.

    Precedence chain (highest → lowest):

    1. ``run_full_refresh`` (the CLI ``--full-refresh`` flag) — when set,
       every stream this run is treated as full_refresh.
    2. ``pipeline.streams[stream.name].mode`` — per-stream config override.
    3. The stream's natural mode — incremental if the source declares an
       ``incremental:`` block in register.yaml, full_refresh otherwise.

    The §3.1 state rule applies whenever the resolved mode is
    ``FULL_REFRESH``: don't read the cursor, don't advance it, don't
    reset it. Implemented in :func:`_run_one_stream`.
    """
    if run_full_refresh:
        return StreamMode.FULL_REFRESH
    stream_run = pipeline.streams.get(stream_def.name)
    if stream_run is not None and stream_run.mode is not None:
        return stream_run.mode
    return StreamMode.INCREMENTAL if stream_def.is_incremental else StreamMode.FULL_REFRESH


def _resolve_partition(
    stream_def: StreamDef,
    pipeline: PipelineConfig,
    schema: Schema,
    log: Any,
) -> PartitionConfig | None:
    """Resolve a stream's partition spec — docs/05 §3.x.

    Precedence chain (highest → lowest):

    1. ``pipeline.streams[name].partition`` — per-stream config override
       (the redesigned home of what used to be ``partition_overrides``).
       Always wins; always honored verbatim. A user adding this block
       has made an explicit decision.
    2. ``stream_def.partition_by`` — the source's ``register.yaml``
       declaration. Either a long-form :class:`PartitionConfig` (honored
       verbatim) or a short-form string column name (promoted to TIME+DAY,
       with a backward-compat degradation — see NOTE below).
    3. Cursor-based auto-default — only for incremental streams whose cursor
       type is ``timestamp`` or ``date``. Emits an INFO log naming the
       chosen column. INT / STRING cursors leave the stream unpartitioned
       and emit a one-line WARNING with the explicit-declaration syntax.

    Returns ``None`` when the stream should not be physically partitioned
    (full-refresh streams without a declared partition, or an unsupported
    auto-default case). The destination receives the resolved value via
    :attr:`StreamMeta.partition`; ``None`` means "no partitioning".

    # NOTE: backward-compat degradation for the short form. The short form
    # ``partition_by: <column>`` defaults to TIME+DAY at the *type* layer
    # (``PartitionConfig.from_short``). But several pre-existing sources
    # (Stripe, ShipHero) declare a short form against a column that is not
    # a TIMESTAMP/DATE (Stripe's ``created`` is an INTEGER Unix epoch with
    # cursor_type=int). Today the destination ignores ``partition_by``
    # entirely, so the short form has been a no-op for those sources. Once
    # the BigQuery destination starts honoring this field, naively applying
    # TIME+DAY to an INT column would crash every existing Stripe run.
    #
    # The resolver therefore degrades a short-form declaration to "no
    # partition + WARNING" when the cursor type is INT or STRING (or when
    # the named column's schema type isn't a TIMESTAMP/DATE). The warning
    # text is the same as the unsupported-cursor warning — it tells the user
    # to switch to the long form. Long-form declarations and per-config
    # overrides are *never* degraded; those are explicit decisions.
    """
    # 1. Per-stream config override wins (always honored verbatim — long form only).
    stream_run = pipeline.streams.get(stream_def.name)
    if stream_run is not None and stream_run.partition is not None:
        chosen = stream_run.partition
        log.info(
            "stream %r: partition = %s (from pipeline.streams[%r].partition)",
            stream_def.name,
            chosen.describe(),
            stream_def.name,
        )
        # An explicit NONE means "unpartitioned, and do NOT auto-default".
        return None if chosen.type is PartitionType.NONE else chosen

    # 2. Source-declared partition_by from register.yaml.
    declared = stream_def.partition_by
    if isinstance(declared, PartitionConfig):
        log.info(
            "stream %r: partition = %s (from source register.yaml)",
            stream_def.name,
            declared.describe(),
        )
        return None if declared.type is PartitionType.NONE else declared
    if isinstance(declared, str):
        # Short form. Check whether TIME+DAY can actually apply to this
        # column on this destination — see the backward-compat NOTE above.
        if _short_form_compatible(declared, stream_def, schema):
            chosen = PartitionConfig.from_short(declared)
            log.info(
                "stream %r: partition = %s (from source register.yaml, short form)",
                stream_def.name,
                chosen.describe(),
            )
            return chosen
        # Degrade to unpartitioned + a one-line warning naming the long form.
        log.warning(
            "stream %r: ignoring short-form partition_by: %r — column is not "
            "a TIMESTAMP/DATE (cursor type / schema type is incompatible with "
            "TIME+DAY). To partition this stream, declare the long form: "
            "partition_by: { field: %s, type: range, range: { start: ..., "
            "end: ..., interval: ... } }",
            stream_def.name,
            declared,
            declared,
        )
        return None

    # 3. No declaration — try the cursor-based auto-default.
    if not stream_def.is_incremental:
        # Full-refresh stream with no declaration: no partition, no warning.
        return None
    inc = stream_def.incremental
    assert inc is not None  # guarded by is_incremental
    if inc.cursor_type in _TIME_PARTITION_CURSOR_TYPES:
        chosen = PartitionConfig(
            field=inc.cursor_field,
            type=PartitionType.TIME,
            granularity=TimeGranularity.DAY,
        )
        log.info(
            "stream %r: partition = %s (auto-default from %s cursor)",
            stream_def.name,
            chosen.describe(),
            inc.cursor_type.value,
        )
        return chosen
    # INT / STRING cursor — no partition + warning naming the long form.
    log.warning(
        "stream %r: %s cursor %r cannot be auto-partitioned; declare "
        "partition_by: explicitly with type=range or type=ingestion to "
        "partition this stream",
        stream_def.name,
        inc.cursor_type.value,
        inc.cursor_field,
    )
    return None


def _validate_streams_block(
    pipeline: PipelineConfig, source: disc.LoadedConnector
) -> None:
    """Reject a ``streams:`` block that names a stream the source does not declare.

    Raised here (not in the parser) because the parser does not know which
    source a config will bind to. Same shape as the codebase's other
    "unknown name → list known names" errors so the typo is debuggable from
    the message alone.

    Additionally checks the §3.2 rule: ``mode: incremental`` on a stream
    that has no ``incremental:`` block in its register.yaml is a hard error
    — the stream has no cursor field to advance.

    # NOTE: design decision — the strongest long-run answer to "what happens
    # when streams names a stream that doesn't exist?" is a hard error.
    # Silently ignoring would let a typo (``chrages:`` for ``charges:``)
    # ship to production with the original partition spec / mode quietly
    # winning, which is exactly the failure mode this block exists to
    # prevent. Hard error here matches:
    #   * StreamDef.__post_init__ rejecting unknown stream keys;
    #   * PipelineConfig.from_dict rejecting unknown top-level keys;
    #   * configs.load_config listing known configs on a typo.
    """
    if pipeline.all_streams:
        # `streams: all` is the catch-all opt-in — no per-name validation
        # needed (the runner expands against source.manifest.streams).
        return
    if not pipeline.streams:
        # Cannot happen — the parser enforces non-empty. Defensive guard.
        return
    streams_by_name = {s.name: s for s in source.manifest.streams}
    unknown = sorted(set(pipeline.streams) - set(streams_by_name))
    if unknown:
        raise EngineError(
            f"config {pipeline.name!r}: streams names stream(s) that do not "
            f"exist on source {pipeline.source!r}: "
            f"{', '.join(repr(s) for s in unknown)}; valid streams: "
            f"{', '.join(repr(s) for s in sorted(streams_by_name)) or '(none)'}"
        )
    # Per-stream mode coherence — mode=incremental requires the source to
    # declare a cursor (an `incremental:` block in register.yaml). The
    # opposite (mode=full_refresh on an incremental-capable stream) is
    # always allowed — that's the §3.1 escape hatch.
    for stream_name, stream_run in pipeline.streams.items():
        if stream_run.mode is StreamMode.INCREMENTAL:
            stream_def = streams_by_name[stream_name]
            if not stream_def.is_incremental:
                raise EngineError(
                    f"config {pipeline.name!r}: stream {stream_name!r} has no "
                    f"incremental cursor in source {pipeline.source!r}'s "
                    f"register.yaml; cannot set mode=incremental"
                )


def _short_form_compatible(
    column: str, stream_def: StreamDef, schema: Schema
) -> bool:
    """Whether a short-form ``partition_by: <column>`` can be TIME+DAY-mapped.

    Used by :func:`_resolve_partition` to apply the backward-compat
    degradation rule. Two signals are checked:

    * the schema's declared field type for ``column`` — must be TIMESTAMP/DATE;
    * the stream's incremental cursor_type — if the column IS the cursor
      field, the cursor type must be TIMESTAMP/DATE too.

    When the schema has no entry for the column (inferred schema, no
    declaration), we fall back to the cursor signal alone; an inferred
    timestamp/date works fine.
    """
    # Check schema type when declared.
    field = schema.field(column)
    if field is not None:
        if field.type not in (FieldType.TIMESTAMP, FieldType.DATE):
            return False
    # Check cursor type if the column IS the cursor field.
    inc = stream_def.incremental
    if inc is not None and inc.cursor_field == column:
        if inc.cursor_type not in _TIME_PARTITION_CURSOR_TYPES:
            return False
    return True


# ---------------------------------------------------------------------------
# Destination hook resolution
# ---------------------------------------------------------------------------


def _resolve_destination_hooks(
    dest: disc.LoadedConnector,
) -> tuple[dict[str, Callable[..., Any]], set[Capability]]:
    """Bind a destination's ``@destination`` hooks and read its capability tier."""
    registry = dest.registry
    hooks: dict[str, Callable[..., Any]] = {}
    for name in _CORE_HOOKS:
        hook = registry.hook(name)
        if hook is None:
            raise EngineError(
                f"destination {dest.manifest.name!r} is missing the mandatory "
                f"@destination.{name} hook (docs/03 §3.4)"
            )
        hooks[name] = hook.func

    capabilities: set[Capability] = set(hooks["capabilities"]())

    if Capability.STATE in capabilities:
        for name in _STATE_HOOKS:
            hook = registry.hook(name)
            if hook is None:
                raise EngineError(
                    f"destination {dest.manifest.name!r} declares Capability.STATE "
                    f"(Tier A) but is missing the @destination.{name} hook "
                    f"required to host state (docs/05 §5)"
                )
            hooks[name] = hook.func
    else:
        raise EngineError(
            f"destination {dest.manifest.name!r} does not declare Capability.STATE; "
            f"Tier B (companion state backend) destinations are not supported in v1"
        )

    if Capability.TRANSACTIONAL_LOAD in capabilities:
        hook = registry.hook("transaction")
        if hook is None:
            raise EngineError(
                f"destination {dest.manifest.name!r} declares "
                f"Capability.TRANSACTIONAL_LOAD but is missing the "
                f"@destination.transaction hook required to honor it (docs/05 §5.3)"
            )
        hooks["transaction"] = hook.func

    if Capability.RUN_RECORDS in capabilities:
        hook = registry.hook("write_run_record")
        if hook is None:
            raise EngineError(
                f"destination {dest.manifest.name!r} declares "
                f"Capability.RUN_RECORDS but is missing the "
                f"@destination.write_run_record hook required to honor it "
                f"(docs/09 §4)"
            )
        hooks["write_run_record"] = hook.func

    if Capability.LEASE in capabilities:
        for name in _LEASE_HOOKS:
            hook = registry.hook(name)
            if hook is None:
                raise EngineError(
                    f"destination {dest.manifest.name!r} declares "
                    f"Capability.LEASE but is missing the @destination.{name} "
                    f"hook required to host stream leases (docs/05 §5.5)"
                )
            hooks[name] = hook.func

    # Optional hook (stage 8e): bound when present so stream-level parallelism
    # (`dtex run -p … --threads N`) can clamp concurrency to it. Absent ⇒ the
    # engine treats the destination as unbounded and only ``threads`` limits.
    mcw = registry.hook("max_concurrent_writes")
    if mcw is not None:
        hooks["max_concurrent_writes"] = mcw.func
    return hooks, capabilities


# ---------------------------------------------------------------------------
# Stream leasing — cross-build coordination (docs/05 §5.5)
# ---------------------------------------------------------------------------


class _LeaseCoordinator:
    """Owns the stream-lease lifecycle for one run — docs/05 §5.5.

    Constructed only when the destination declares :attr:`Capability.LEASE`;
    otherwise the run never builds one and behaves exactly as before (the
    ``None`` coordinator is the backward-compatible no-op path). It:

    * reads every existing lease for the source once at run start,
    * :meth:`acquire_all`s the whole selected stream set in ONE batched call
      before any stream runs (skipping streams a *live* lease from another
      build holds, and honoring the per-source ``max_parallel`` cap),
    * :meth:`beat`s every held lease with ONE batched call on a throttle, and
    * :meth:`release_all`s every held lease in ONE batched call at the end.

    A build only ever *releases* leases it acquired this run — it never
    terminates another build's lease, so two builds cooperate rather than
    fight. A crashed build's lease is reclaimed by staleness, not by force.

    # NOTE: every mutating lease call is BATCHED (whole stream set per call)
    # and issued from the MAIN thread only — never from a worker thread. This
    # is a correctness requirement discovered in production: with
    # ``--threads N`` the old per-stream design had N worker threads each
    # issuing their own lease DML against the single ``_dtex_leases`` table,
    # and BigQuery — which serializes DML per table — failed the losers with
    # "Could not serialize access to table … due to concurrent update",
    # killing the run. One statement per phase cannot self-conflict, so
    # leasing and stream parallelism compose. This also removes the need for
    # the in-process lock the per-stream design required: the coordinator's
    # bookkeeping is now only ever touched by the main thread.
    """

    def __init__(
        self,
        hooks: Mapping[str, Callable[..., Any]],
        conn: Any,
        connector: str,
        run_id: str,
        max_parallel: int | None,
        log: Any,
    ) -> None:
        self._hooks = hooks
        self._conn = conn
        self._connector = connector
        self._run_id = run_id
        self._max_parallel = max_parallel
        self._log = log
        # Live leases held by *other* runs, keyed by stream — the skip set.
        now = datetime.now(UTC)
        existing = hooks["read_leases"](conn, connector)
        self._live_by_other: dict[str, LeaseRecord] = {
            lease.stream: lease
            for lease in existing
            if lease.run_id != run_id and lease.is_live(now=now)
        }
        # Streams this run acquired (so it only ever releases its own).
        self._held: set[str] = set()
        # Wall-clock of the last batched heartbeat, for throttling. One value
        # for the whole run (not per stream) — the heartbeat is now a single
        # statement covering every held lease.
        self._last_beat: datetime | None = None

    def acquire_all(self, streams: Sequence[str]) -> set[str]:
        """Lease as many of ``streams`` as this build may run. Returns the won set.

        One batched destination call for the whole candidate set. A stream is
        excluded up front when a *live* lease from another build holds it, or
        when the per-source ``max_parallel`` cap is already met by the
        preceding candidates (declared order decides who gets the slots). The
        destination's batched compare-and-set is still authoritative: it may
        refuse a candidate that a concurrent build won between our read and
        the write, and those are dropped from the returned set too.

        Called once, from the main thread, BEFORE any stream starts.
        """
        candidates: list[str] = []
        for stream in streams:
            if stream in self._live_by_other:
                self._log.info(
                    "stream %r is leased by run %r (live) — skipping",
                    stream,
                    self._live_by_other[stream].run_id,
                )
                continue
            if self._max_parallel is not None and len(candidates) >= self._max_parallel:
                self._log.info(
                    "stream %r not leased — max_parallel=%d reached this build",
                    stream,
                    self._max_parallel,
                )
                continue
            candidates.append(stream)

        if not candidates:
            return set()

        now = datetime.now(UTC)
        records = [
            LeaseRecord(
                connector=self._connector,
                stream=stream,
                run_id=self._run_id,
                status=LeaseStatus.RUNNING,
                acquired_at=now,
                heartbeat_at=now,
            )
            for stream in candidates
        ]
        won = set(self._hooks["acquire_leases"](self._conn, records))
        for stream in candidates:
            if stream not in won:
                self._log.info("stream %r lost lease race — skipping", stream)
        self._held = {s for s in candidates if s in won}
        self._last_beat = now
        return set(self._held)

    def beat(self) -> None:
        """Refresh every held lease in ONE batched call, throttled.

        Driven from the main thread while worker threads run streams — a
        worker never touches ``_dtex_leases``. A no-op when nothing is held or
        the throttle interval has not elapsed.
        """
        if not self._held:
            return
        now = datetime.now(UTC)
        if (
            self._last_beat is not None
            and (now - self._last_beat).total_seconds() < LEASE_HEARTBEAT_INTERVAL_SECONDS
        ):
            return
        self._last_beat = now
        records = [
            LeaseRecord(
                connector=self._connector,
                stream=stream,
                run_id=self._run_id,
                status=LeaseStatus.RUNNING,
                heartbeat_at=now,
            )
            for stream in sorted(self._held)
        ]
        self._hooks["heartbeat_leases"](self._conn, records)

    def release_all(self, statuses: Mapping[str, LeaseStatus]) -> None:
        """Release every held lease in ONE batched call with per-stream statuses.

        ``statuses`` maps stream → terminal status; a held stream missing from
        it is released ``FAILED`` (it neither completed nor was recorded, so
        the conservative status is the honest one). Streams this run never
        acquired are untouched — a skipped-leased stream never has the other
        build's lease disturbed.
        """
        if not self._held:
            return
        now = datetime.now(UTC)
        records = [
            LeaseRecord(
                connector=self._connector,
                stream=stream,
                run_id=self._run_id,
                status=statuses.get(stream, LeaseStatus.FAILED),
                heartbeat_at=now,
            )
            for stream in sorted(self._held)
        ]
        self._hooks["release_leases"](self._conn, records)
        self._held = set()


def _safe_release_all(
    leases: _LeaseCoordinator, statuses: Mapping[str, LeaseStatus], log: Any
) -> None:
    """Release every held lease, swallowing errors so they can't mask the run.

    A lease release is best-effort bookkeeping: if the destination write fails
    (e.g. the connection is already broken because a stream just crashed), the
    leases will simply be reclaimed by staleness later. Never let a release
    failure replace the real stream error the caller is about to propagate.
    """
    try:
        leases.release_all(statuses)
    except Exception as exc:  # noqa: BLE001 — bookkeeping must not mask the run.
        log.warning("failed to release leases: %s", exc)


def _safe_beat(leases: _LeaseCoordinator, log: Any) -> None:
    """Refresh held leases, swallowing errors — a missed beat is not fatal.

    A failed heartbeat only risks the leases going stale (and so becoming
    reclaimable by another build); it must never take down a run that is
    otherwise loading data fine.
    """
    try:
        leases.beat()
    except Exception as exc:  # noqa: BLE001 — bookkeeping must not mask the run.
        log.warning("failed to refresh leases: %s", exc)


# ---------------------------------------------------------------------------
# Stream execution — one stream's EXTRACT → NORMALIZE → LOAD → COMMIT
# ---------------------------------------------------------------------------


def _stream_transaction(
    hooks: Mapping[str, Callable[..., Any]],
    conn: Any,
    stream_meta: StreamMeta,
) -> Any:
    """Return the context wrapping a stream's load + state commit — docs/05 §5.3."""
    tx = hooks.get("transaction")
    if tx is None:
        return nullcontext()
    return tx(conn, stream_meta)


def _run_one_stream(
    stream_def: StreamDef,
    source: disc.LoadedConnector,
    hooks: Mapping[str, Callable[..., Any]],
    conn: Any,
    run_config: RunConfig,
    pipeline: PipelineConfig,
    prior: StateRecord | None,
    log: Any,
    run_log: RunLog | None = None,
    stream_config_override: Any = None,
    heartbeat: Callable[[], None] | None = None,
) -> StreamResult:
    """Run one stream end to end — docs/02 §Run lifecycle step 5 (a–d).

    Emits ``stream_start`` / ``batch_loaded`` / ``stream_committed`` events to
    ``run_log`` (docs/09 §2) when one is supplied. ``stream_failed`` is
    emitted by the caller when this raises — the exception carries the data
    needed (error_type / message / traceback) and is fully recorded there.

    ``heartbeat``, when supplied (stream leasing is active — docs/05 §5.5), is
    invoked once per written batch. It refreshes this stream's lease heartbeat,
    self-throttling to at most one write per
    :data:`~dtex.types.LEASE_HEARTBEAT_INTERVAL_SECONDS`, so a long
    many-batch bootstrap keeps its lease alive without a background thread —
    a running stream yields batches steadily, which is exactly the liveness
    signal a heartbeat should track (between-batch beats also avoid any
    thread-safety concern with the single-threaded destination client).
    """
    registration = source.registry.stream(stream_def.name)
    if registration is None:  # pragma: no cover — validate_connector caught it.
        raise EngineError(f"stream {stream_def.name!r} has no registered @stream function")

    # -- 5a: cursor (incremental streams only) ------------------------------
    # The effective mode is resolved per stream — CLI --full-refresh forces
    # FULL_REFRESH; otherwise pipeline.streams[name].mode wins; else the
    # stream's natural mode (incremental if the source declares a cursor).
    effective_mode = _resolve_stream_mode(
        stream_def, pipeline, run_full_refresh=run_config.full_refresh
    )
    is_full_refresh_stream = effective_mode is StreamMode.FULL_REFRESH

    cursor: Cursor | None = None
    cursor_before: Any = None
    if stream_def.is_incremental:
        inc = stream_def.incremental
        assert inc is not None
        # §3.1: when this stream runs as FULL_REFRESH, the engine does NOT
        # read the prior cursor row. The seed comes from the source's
        # `initial_value` (or a config-supplied `since:` override).
        # When INCREMENTAL, seed from prior state as usual.
        stream_run = pipeline.streams.get(stream_def.name) or StreamRunConfig()
        if is_full_refresh_stream:
            seed = (
                stream_run.since
                if stream_run.since is not None
                else inc.initial_value
            )
        elif stream_run.since is not None:
            # §3.3: explicit `since:` replaces the seed for this run only
            # (no max with prior). Lets an operator say "re-pull from here
            # just this once" without mutating _dtex_state.
            seed = stream_run.since
        else:
            seed = _seed_value(prior, inc.cursor_type, inc.initial_value)
        cursor_before = None if is_full_refresh_stream else seed
        cursor = Cursor(
            cursor_field=inc.cursor_field,
            cursor_type=inc.cursor_type,
            start_value=seed,
            is_full_refresh=is_full_refresh_stream,
        )

    state = State(prior.state_blob if prior is not None else None)

    # Per-stream `streams[name].params` overlay (docs/12 §3.4): when the
    # caller pre-built a stream-specific Config (the runner does this for
    # any stream with a non-empty per-stream params block), use it instead
    # of the run-wide source_config. The base Config still wins for every
    # other stream — no overhead for the common case.
    stream_config = stream_config_override or run_config.config
    available: dict[str, Any] = {
        "config": stream_config,
        "state": state,
        "log": log,
        "stream_def": stream_def,
    }
    if cursor is not None:
        available["cursor"] = cursor
    kwargs = compute_injection(registration.func, available)

    # -- 5b: NORMALIZE — pull the first batch and resolve the schema --------
    rows_loaded = 0
    rows_extracted = 0
    batches = iter(registration.func(**kwargs))
    first_batch = next(batches, None)

    if first_batch is not None and stream_def.schema is not None:
        if stream_def.schema_contract is SchemaContract.STRICT:
            _check_strict_schema(stream_def, stream_def.schema, first_batch)
        resolved_schema = stream_def.schema
    elif first_batch is not None:
        resolved_schema = _infer_schema(first_batch)
    else:
        resolved_schema = stream_def.schema if stream_def.schema is not None else Schema()

    # Partition resolution needs the resolved schema (the short-form backward-
    # compat check inspects the field type), so it lives after schema
    # resolution and before ensure_schema. The destination's ensure_schema is
    # the first hook that needs to know the partition spec.
    partition = _resolve_partition(stream_def, pipeline, resolved_schema, log)

    if run_log is not None:
        run_log.emit(
            "stream_start",
            stream=stream_def.name,
            disposition=stream_def.write_disposition.value,
            cursor_before=cursor_before,
            partition=None if partition is None else partition.describe(),
        )

    stream_meta = StreamMeta.from_stream_def(
        stream_def, resolved_schema, partition=partition
    )
    hooks["ensure_schema"](conn, stream_meta)

    # §3.1 state rule: when an INCREMENTAL-capable stream runs as FULL_REFRESH
    # this invocation, the engine does NOT write _dtex_state. The prior cursor
    # row (if any) stays intact, so a sibling incremental config sharing this
    # source keeps its cursor. Streams that are naturally non-incremental (no
    # cursor at all) still write state — _dtex_state also tracks rows_total /
    # last_run_id for those, which is operator-visible audit info. This gates
    # BOTH the mid-stream flushes and the final commit.
    skip_state = is_full_refresh_stream and stream_def.is_incremental
    # A destination that cannot host state (no ``commit_state`` hook) writes no
    # state at all — the v1 engine mandates Capability.STATE, but a unit test
    # or a future Tier-B path may drive a stream without it, and that must
    # behave exactly as before this change: no commit, no error.
    can_commit_state = "commit_state" in hooks

    def _build_state_record(cursor_value: Any) -> StateRecord:
        """Build the ``_dtex_state`` row for this stream at the current point.

        Used identically by the throttled mid-stream flush and the final
        end-of-stream commit so the two never diverge. ``rows_total`` is the
        prior persisted total plus rows loaded so far this run; a mid-stream
        flush therefore records a monotonically growing partial total, and the
        final commit records the complete one.
        """
        return StateRecord(
            connector=run_config.connector,
            stream=stream_def.name,
            cursor_value=cursor_value,
            cursor_type=(
                stream_def.incremental.cursor_type
                if stream_def.incremental is not None
                else None
            ),
            state_blob=state.to_dict(),
            last_run_id=run_config.run_id,
            rows_total=(prior.rows_total if prior is not None else 0) + rows_loaded,
        )

    # Throttled mid-stream state flush — docs/05 §5.2. Persists the connector's
    # in-progress state (e.g. a bootstrap's resume pointer) so an interruption
    # does not lose it and force a restart-from-far-behind that re-appends
    # duplicates. MUST be invoked only AFTER a batch's rows are durable
    # (commit-after-write ordering): the flush records "rows up to here are
    # safely landed", so recording it before the write would, on a crash
    # between flush and write, point a resume past rows that never landed.
    # Wall-clock of the last mid-stream flush; ``None`` until the first flush.
    # Held in a one-element list so the closure can rebind it (a plain
    # ``nonlocal`` would work too; the list keeps the closure free of extra
    # declarations and reads the same as ``_LeaseCoordinator._last_beat``).
    last_state_flush: list[datetime | None] = [None]

    def _maybe_flush_state() -> None:
        if skip_state or not can_commit_state:
            return
        now = datetime.now(UTC)
        prev = last_state_flush[0]
        if prev is not None and (now - prev).total_seconds() < STATE_COMMIT_INTERVAL_SECONDS:
            return
        last_state_flush[0] = now
        # Persist the cursor observed so far (falling back to the run's seed
        # when nothing has been observed yet) alongside the connector's
        # in-progress state_blob — the resume pointer that makes an
        # interrupted stream restart correctly.
        cursor_now = cursor.observed_max if cursor is not None else None
        if cursor_now is None:
            cursor_now = cursor_before
        hooks["commit_state"](conn, run_config.run_id, [_build_state_record(cursor_now)])

    # -- 5c/5d: LOAD + COMMIT — inside the per-stream transaction -----------
    # The NORMALIZE step (docs/02 §Normalize) coerces each batch's values
    # to the resolved schema's declared FieldType right before write_batch.
    # A CoercionError raised mid-batch propagates out of the ``with`` and
    # the destination's transaction rolls back any partial load — same
    # crash-safety guarantee as a write_batch failure. See
    # :mod:`dtex.engine.normalize` for the per-FieldType rules.
    with _stream_transaction(hooks, conn, stream_meta):
        if first_batch is not None:
            rows_extracted += len(first_batch)
            normalized = normalize_batch(first_batch, resolved_schema)
            written = hooks["write_batch"](conn, normalized, stream_meta)
            rows_loaded += written
            # Order matters: heartbeat + state flush happen strictly AFTER the
            # batch's rows are durable (write_batch returned). See
            # STATE_COMMIT_INTERVAL_SECONDS for why the ordering is the crux.
            if heartbeat is not None:
                heartbeat()
            _maybe_flush_state()
            if run_log is not None:
                run_log.emit(
                    "batch_loaded",
                    stream=stream_def.name,
                    rows=written,
                    cumulative_rows=rows_loaded,
                )
            for batch in batches:
                rows_extracted += len(batch)
                normalized = normalize_batch(batch, resolved_schema)
                written = hooks["write_batch"](conn, normalized, stream_meta)
                rows_loaded += written
                if heartbeat is not None:
                    heartbeat()
                _maybe_flush_state()
                if run_log is not None:
                    run_log.emit(
                        "batch_loaded",
                        stream=stream_def.name,
                        rows=written,
                        cumulative_rows=rows_loaded,
                    )

        cursor_after = cursor_before
        if cursor is not None and cursor.observed_max is not None:
            cursor_after = cursor.observed_max

        # Final commit carries the terminal cursor + complete rows_total. It
        # always runs (subject to the same skip_state / capability guards as
        # the mid-stream flushes), so a stream that never crossed a flush
        # interval still persists its state exactly once, as before.
        if not skip_state and can_commit_state:
            hooks["commit_state"](
                conn, run_config.run_id, [_build_state_record(cursor_after)]
            )

    if run_log is not None:
        run_log.emit(
            "stream_committed",
            stream=stream_def.name,
            rows_loaded=rows_loaded,
            cursor_after=cursor_after,
        )

    return StreamResult(
        name=stream_def.name,
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        cursor_before=cursor_before,
        cursor_after=cursor_after,
        status=StreamStatus.SUCCEEDED,
    )


# ---------------------------------------------------------------------------
# Run-record construction — the engine builds RunRecord from RunResult
# ---------------------------------------------------------------------------


def _format_traceback(exc: BaseException) -> str:
    """Render an exception's traceback as a plain string for the JSONL log.

    The full traceback lands in ``.dtex/logs/<run_id>/run.jsonl`` (the
    forensics surface — docs/09 §3.2); deliberately NOT in the ``_dtex_runs``
    audit row (the queryability surface — docs/09 §4 NOTE on RunRecord).
    """
    return "".join(_tb.format_exception(type(exc), exc, exc.__traceback__))


def _build_run_record(
    *,
    run_id: str,
    config_name: str,
    connector_name: str,
    destination_name: str,
    target_name: str,
    started_at: datetime,
    streams: list[StreamResult],
    full_refresh: bool,
    final_result: RunResult | None,
) -> RunRecord:
    """Build the :class:`RunRecord` from the engine's run state — docs/09 §4.

    Single source of truth: the engine builds the record from the
    :class:`RunResult` it already built (or from the loose locals if a
    BaseException prevented even that). The record is what
    ``@destination.write_run_record`` receives; the table column set is
    derived from it.
    """
    if final_result is not None:
        status = final_result.status
        ended_at = final_result.ended_at
        rows_loaded = final_result.rows_loaded
        error_type, error_message = _split_error(final_result.error)
    else:  # pragma: no cover — only on BaseException between except/finally.
        status = RunStatus.FAILED
        ended_at = datetime.now(UTC)
        rows_loaded = sum(s.rows_loaded for s in streams)
        error_type, error_message = ("BaseException", "run aborted before record")

    return RunRecord(
        run_id=run_id,
        config=config_name,
        source=connector_name,
        destination=destination_name,
        target=target_name,
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        rows_loaded=rows_loaded,
        streams=tuple(streams),
        full_refresh=full_refresh,
        error_type=error_type,
        error_message=error_message,
    )


def _split_error(error: BaseException | None) -> tuple[str | None, str | None]:
    """Split a run's terminal error into (error_type, error_message) — docs/09 §4."""
    if error is None:
        return None, None
    return type(error).__name__, str(error)


def _stream_write_cap(
    hooks: Mapping[str, Callable[..., Any]],
    dest_config: Config,
) -> int:
    """Resolve the destination's ``max_concurrent_writes`` for stream parallelism.

    Stage-5 stream concurrency is clamped to this so a destination that can't
    take concurrent writers serializes regardless of ``--threads`` (DuckDB
    returns 1). A destination that doesn't declare the hook is unbounded
    (``sys.maxsize``) — only the ``threads`` budget then limits it. A hook that
    raises is treated as unbounded and the failure surfaces later on a real
    write, not here (mirrors ``_destination_concurrency_cap``'s planning-stage
    fallback for the ``--tag`` path).
    """
    hook = hooks.get("max_concurrent_writes")
    if hook is None:
        return _UNLIMITED_CONCURRENCY
    try:
        return max(1, int(hook(Config(params=dict(dest_config.params)))))
    except Exception:  # noqa: BLE001 — planning-stage fallback, see docstring.
        return _UNLIMITED_CONCURRENCY


# ---------------------------------------------------------------------------
# The run loop — the public engine entry point
# ---------------------------------------------------------------------------


def run_tag(
    tag: str,
    *,
    project_dir: str | Path | None = None,
    target_override: str | None = None,
    destination_params_override: Mapping[str, Any] | None = None,
    full_refresh: bool = False,
    select: tuple[str, ...] = (),
    threads: int | None = None,
) -> list[RunResult]:
    """Run every config whose ``tags:`` list contains ``tag`` — docs/12 §Tags.

    Multi-run sibling of :func:`run`. The runtime unit is still one config
    per ``run()``; ``run_tag`` is a thin wrapper that discovers every
    matching config and drives them through :func:`run` either sequentially
    (the default; ``threads=1`` or omitted ⇒ today's stage 8d behavior) or
    in parallel via a :class:`~concurrent.futures.ThreadPoolExecutor`
    (stage 8e, ``threads>1`` or ``profiles.threads>1``).

    Semantics:

    * **Selection**: exact string match on the lowercased tag against each
      config's lowercased :attr:`PipelineConfig.tags`. No glob, no regex —
      a user wanting "match anything starting with ``hourly_``" writes the
      tag explicitly. Matching is case-insensitive because the parser
      normalizes both sides (see :meth:`PipelineConfig.from_dict`).
    * **Order**: alphabetical by config name. Predictable, stable across
      runs, independent of filesystem walk order. Reuses
      :func:`dtex.cli._discovery.discover_all_configs` which already returns
      sorted output, so the order matches ``dtex list --kind config``. The
      returned list also preserves this order in parallel mode — futures
      complete in any order, but the returned list is rebuilt by
      ``matching`` so output ordering is independent of completion order.
    * **Continue-on-failure**: each config runs through the same
      :func:`run` the CLI's ``-p`` path uses (which never raises — it
      returns a FAILED :class:`RunResult`). A failure in one config does
      NOT stop the rest. The caller inspects the returned list to decide
      overall outcome (the CLI exits 1 if any result is FAILED, 0 if all
      succeeded; 2 if zero configs matched the tag — that's a usage error).
    * **Uniform args**: ``target_override`` / ``destination_params_override``
      / ``full_refresh`` / ``select`` apply to EVERY matched config. That
      is the right semantic for the common case ("run hourly with prod
      target" should apply prod to every hourly pipeline).
    * **Parallelism (stage 8e)**: ``threads`` is the project-wide
      concurrency budget. ``None`` (default) reads
      :attr:`Profiles.threads` from ``profiles.yml``; explicit
      ``threads=N`` (CLI ``--threads N`` or kwarg) overrides. Effective
      value is clamped to ``max(1, ...)``. The engine ALSO consults each
      destination's ``@destination.max_concurrent_writes`` hook (one call
      per unique destination, cached) and narrows per-destination
      concurrency via a :class:`threading.Semaphore` keyed by destination
      name. DuckDB returns 1, so a tag-sweep that targets DuckDB
      serializes even at ``threads=8`` — the destination's
      file-lock honesty wins.

    Returns ``[]`` when no config matches — the caller (usually the CLI)
    treats that as a usage error. Never raises on a connector or
    destination failure: each per-config call goes through :func:`run`,
    which folds exceptions into a FAILED :class:`RunResult`.

    # NOTE: ``params_override`` is intentionally NOT exposed on this
    # function. A source param override that names ``page_size`` would
    # silently apply to every config whether or not its source's
    # ``register.yaml`` declares ``page_size`` — a usability footgun on a
    # multi-source tag selection. Users that need per-config param
    # overrides should call ``dtex run -p <config> --param k=v`` per
    # invocation; ``--tag`` is for "run them all" sweeps, not for
    # surgical knob-tweaking.

    # NOTE: ``destination_params_override`` IS exposed even though it has
    # the same uniform-apply caveat in principle, because the common
    # tag-sweep destination override is ``path=`` / ``dataset=`` — a knob
    # the destination connector defines, not the source. When configs
    # tagged ``hourly`` bind to different destinations, an override that
    # doesn't apply at one of them is silently dropped by the destination's
    # own param resolution (unknown destination params raise inside the
    # destination's ``open`` hook — that failure mode is already covered).
    # The verification step ``dtex run --tag test
    # --destination-param path=/tmp/det_8d_demo.duckdb`` depends on this
    # threading.

    # NOTE (stage 8e parallel-path design): the sequential branch is kept
    # as a literal ``for cfg in matching: run(...)`` even though it could
    # be expressed as ``ThreadPoolExecutor(max_workers=1)``. The reason is
    # debuggability — a stack trace from a sequential run is the user's
    # own thread, not a worker thread, and ``pdb`` / ``breakpoint()`` in
    # the engine path Just Works. The parallel branch is opt-in
    # (``threads>1``), so it should not regress the default debugging
    # experience.
    """
    normalized = tag.strip().lower()
    # Call the engine-layer discoverer directly. Sorting by name lives
    # here rather than reaching into ``dtex.cli._discovery.discover_all_configs``
    # so the engine doesn't depend on a CLI internal — the engine layer is
    # below the CLI layer, the inverse direction would invert the
    # dependency graph and force a deferred import to break the cycle.
    project_root = disc.find_project_root(project_dir)
    # Load project-local secret-resolver plugins ONCE before walking the
    # configs — see the corresponding call in :func:`run` for rationale.
    load_project_plugins(project_root)
    project = cfg.ProjectConfig.load(project_root)
    profiles = cfg.Profiles.load(project_root)
    discovered = cfgs.discover_configs(project_root, list(project.config_paths))
    matching = sorted(
        (pc for pc in discovered.values() if normalized in pc.tags),
        key=lambda pc: pc.name,
    )

    # Resolve effective thread count: explicit arg wins, else profiles.yml,
    # else 1. Clamp to at least 1 — a zero or negative value is degenerate.
    effective_threads = threads if threads is not None else profiles.threads
    effective_threads = max(1, int(effective_threads))

    if not matching:
        return []

    # The sequential (threads=1) path is a clean literal loop — see the
    # design NOTE above. Empty match list also short-circuits here.
    if effective_threads == 1 or len(matching) == 1:
        results: list[RunResult] = []
        for pipeline in matching:
            result = run(
                pipeline.name,
                project_dir=project_root,
                target_override=target_override,
                destination_params_override=destination_params_override,
                full_refresh=full_refresh,
                select=select,
            )
            results.append(result)
        return results

    return _run_tag_parallel(
        matching=matching,
        project_root=project_root,
        project=project,
        profiles=profiles,
        target_override=target_override,
        destination_params_override=destination_params_override,
        full_refresh=full_refresh,
        select=select,
        threads=effective_threads,
    )


# ---------------------------------------------------------------------------
# run_tag parallel branch — stage 8e
# ---------------------------------------------------------------------------


# Fallback per-destination cap when the destination either does not declare
# ``@destination.max_concurrent_writes`` or the hook itself fails at the
# planning stage. ``sys.maxsize`` is effectively unlimited — the semaphore
# becomes a no-op for that destination, and the project ``threads:`` budget
# is the only ceiling.
_UNLIMITED_CONCURRENCY = sys.maxsize


# Stash for the most-recent ``run_tag`` invocation's parallelism summary.
# The CLI reads + clears this so the "parallelism: clamped to K for X"
# line lands at the END of the multi-run summary (the natural place a user
# expects status info). Reading is single-process, single-flight: each
# ``dtex run --tag`` invocation is one process from the user's shell, so
# the global is safe.
#
# # NOTE: design decision — the strongest alternative is making
# ``run_tag`` return a richer object (``RunTagResult``) with both
# ``results`` and ``clamps``. That's a breaking API change at the library
# layer, and library callers DON'T care about display niceties — the
# data they need (per-run status, errors, rows) already lives on
# RunResult. A library-side caller wanting clamp info can call
# ``last_run_tag_clamps()`` itself. The CLI does, in print_multi_run_summary.
_LAST_RUN_TAG_PARALLELISM: dict[str, int] = {}
_LAST_RUN_TAG_THREADS: int = 1


def last_run_tag_parallelism() -> tuple[int, dict[str, int]]:
    """Return ``(threads, clamps)`` for the most recent ``run_tag`` call.

    ``threads`` is the effective project-wide pool size; ``clamps`` maps
    destination name → its per-destination cap, but ONLY for destinations
    whose cap was strictly less than ``threads`` (i.e. the cap narrowed
    the budget). Empty dict ⇒ nothing got clamped.

    Sequential ``run_tag`` calls (``threads=1``) leave the stash at its
    default: threads=1, clamps={}. Reading clears the stash so a second
    read in the same process gets the empty state — preventing a stale
    notice from a prior tag-sweep leaking into a later single-run output.
    """
    global _LAST_RUN_TAG_THREADS, _LAST_RUN_TAG_PARALLELISM
    threads = _LAST_RUN_TAG_THREADS
    clamps = dict(_LAST_RUN_TAG_PARALLELISM)
    _LAST_RUN_TAG_THREADS = 1
    _LAST_RUN_TAG_PARALLELISM = {}
    return threads, clamps


def _destination_concurrency_cap(
    pipeline: PipelineConfig,
    project_root: Path,
    project: cfg.ProjectConfig,
    profiles: cfg.Profiles,
    target_override: str | None,
    destination_params_override: Mapping[str, Any] | None,
) -> int:
    """Resolve one destination's ``max_concurrent_writes`` cap — stage 8e.

    Called once per unique destination across the matched configs (the
    result is cached by destination name in :func:`_run_tag_parallel`). The
    destination's :class:`Config` is built using ``pipeline`` as the
    representative — the first matching pipeline for that destination —
    because the hook is destination-wide but the ``Config`` it receives
    needs to be a real, fully-resolved one (BigQuery reads
    ``max_concurrent_writes`` from its params, which themselves layer
    through profiles + the config's ``destination_params``).

    # NOTE: design decision — if destination resolution or hook execution
    # fails at PLANNING time (e.g. the destination's open hook is fine but
    # validating its register.yaml during ``resolve_destination`` raises),
    # we fall back to unlimited cap and let the per-pipeline ``run()``
    # surface the real error in its own ``RunResult``. The strongest
    # long-run answer to "what happens if a planning-stage hook fails for
    # one destination" is "the whole tag sweep does NOT fail" — that would
    # cancel other pipelines whose destinations are perfectly healthy and
    # invert the continue-on-failure contract. The user sees the failure
    # per-pipeline in the rollup table, not as a meta-error.
    """
    try:
        target_name = cfg.resolve_target_name(
            target_override if target_override is not None else pipeline.target,
            pipeline.destination,
            profiles,
        )
        dest = disc.resolve_destination(
            pipeline.destination, project_root, list(project.destination_paths)
        )
        hook = dest.registry.hook("max_concurrent_writes")
        if hook is None:
            return _UNLIMITED_CONCURRENCY
        dest_config = cfg.build_destination_config(
            dest.manifest,
            project,
            pipeline,
            target_name=target_name,
            profiles=profiles,
            overrides=dict(destination_params_override or {}),
        )
        cap = int(hook.func(Config(params=dict(dest_config.params))))
        return max(1, cap)
    except Exception:  # noqa: BLE001 — planning-stage fallback per docstring NOTE
        return _UNLIMITED_CONCURRENCY


def _run_tag_parallel(
    *,
    matching: list[PipelineConfig],
    project_root: Path,
    project: cfg.ProjectConfig,
    profiles: cfg.Profiles,
    target_override: str | None,
    destination_params_override: Mapping[str, Any] | None,
    full_refresh: bool,
    select: tuple[str, ...],
    threads: int,
) -> list[RunResult]:
    """The parallel branch of :func:`run_tag` — stage 8e.

    Submits each matched pipeline to a
    :class:`~concurrent.futures.ThreadPoolExecutor` sized at ``threads``,
    with a per-destination :class:`threading.Semaphore` enforcing each
    destination's ``@destination.max_concurrent_writes`` cap. Per-pipeline
    stdout is buffered to a :class:`io.StringIO` and flushed to stderr
    under a global print-lock after the pipeline completes, so engine logs
    from different pipelines never interleave on the user's screen. The
    per-run JSONL log writes live (separate file per pipeline) — that's
    unchanged from sequential mode and is the forensics surface.

    Returns the results in the SAME order as ``matching`` (alphabetical by
    name), regardless of completion order — the public ordering contract.
    """
    # Plan per-destination caps. One hook call per unique destination, not
    # per pipeline; multiple configs targeting the same destination share
    # the same semaphore. Effective per-destination cap is
    # ``min(threads, cap)`` — the semaphore is bounded above by the worker
    # pool size anyway, so we use the cap directly and let the pool be the
    # outer ceiling.
    seen_dests: dict[str, PipelineConfig] = {}
    for pipeline in matching:
        seen_dests.setdefault(pipeline.destination, pipeline)

    caps: dict[str, int] = {
        dest_name: _destination_concurrency_cap(
            representative,
            project_root,
            project,
            profiles,
            target_override,
            destination_params_override,
        )
        for dest_name, representative in seen_dests.items()
    }
    # Track which destinations got clamped to less than ``threads`` so the
    # summary can surface it ("ran with N threads, capped at K for X").
    clamped: dict[str, int] = {
        dest: cap for dest, cap in caps.items() if cap < threads
    }
    semaphores: dict[str, threading.Semaphore] = {
        dest_name: threading.Semaphore(cap) for dest_name, cap in caps.items()
    }

    print_lock = threading.Lock()
    # Buffered output is flushed to stderr (matching the stdlib StreamHandler
    # default) so live progress lines interleave correctly with stderr-bound
    # output the host process may also write. ``sys.stderr`` is captured by
    # click's CliRunner in tests, so this is also test-observable.
    sink = sys.stderr

    def _emit(line: str) -> None:
        """Write ``line`` to the print sink under the global print-lock."""
        with print_lock:
            sink.write(line)
            if not line.endswith("\n"):
                sink.write("\n")
            sink.flush()

    def _execute(pipeline: PipelineConfig) -> RunResult:
        """Run one pipeline with semaphore + per-pipeline log buffer."""
        buf = StringIO()
        sema = semaphores.get(pipeline.destination)
        # An emit-on-start banner is printed under the lock the moment we
        # acquire the semaphore — so the user sees the pipeline "starting"
        # only when it actually runs (not when the future is queued waiting
        # behind a saturated semaphore). Strongest UX signal for "this is
        # what's happening NOW".
        if sema is not None:
            sema.acquire()
        try:
            _emit(f"▸ starting {pipeline.name}")
            try:
                result = run(
                    pipeline.name,
                    project_dir=project_root,
                    target_override=target_override,
                    destination_params_override=destination_params_override,
                    full_refresh=full_refresh,
                    select=select,
                    _log_stream=buf,
                )
            except Exception as exc:  # noqa: BLE001 — run() should never raise; belt-and-braces.
                # Defensive: ``run()`` already folds every exception class
                # into a FAILED RunResult. The wrapper here exists so a
                # future change that lets one through still produces a
                # synthetic RunResult instead of a Future-with-exception
                # that ``as_completed`` would surface as an uncaught error.
                result = RunResult(
                    run_id="run-unknown",
                    config=pipeline.name,
                    connector=pipeline.source,
                    target=target_override or pipeline.target or "default",
                    destination=pipeline.destination,
                    status=RunStatus.FAILED,
                    started_at=datetime.now(UTC),
                    ended_at=datetime.now(UTC),
                    streams=[],
                    rows_loaded=0,
                    full_refresh=full_refresh,
                    error=exc,
                    log_path="",
                )
            # Flush the buffered stdlib-logger output + the completion banner
            # under one lock acquisition so the two never interleave with
            # another pipeline's output.
            buffered = buf.getvalue()
            if result.status is RunStatus.SUCCEEDED:
                banner = (
                    f"✓ done {pipeline.name} "
                    f"({result.duration_s:.1f}s, {result.rows_loaded} rows)"
                )
            else:
                err = result.error
                err_msg = (
                    f"{type(err).__name__}: {err}" if err is not None else "(no error)"
                )
                banner = (
                    f"✗ failed {pipeline.name} "
                    f"({result.duration_s:.1f}s, {err_msg})"
                )
            with print_lock:
                if buffered:
                    sink.write(buffered)
                    if not buffered.endswith("\n"):
                        sink.write("\n")
                sink.write(banner + "\n")
                sink.flush()
            return result
        finally:
            if sema is not None:
                sema.release()

    results_by_name: dict[str, RunResult] = {}
    with ThreadPoolExecutor(max_workers=threads) as pool:
        future_to_name: dict[Future[RunResult], str] = {
            pool.submit(_execute, pipeline): pipeline.name for pipeline in matching
        }
        for fut in as_completed(future_to_name):
            name = future_to_name[fut]
            try:
                results_by_name[name] = fut.result()
            except Exception as exc:  # noqa: BLE001 — paranoid wrapper; see _execute
                # Should never trigger: _execute itself catches and folds
                # every exception into a synthetic RunResult. This is the
                # last-ditch safety net so a Future never propagates an
                # uncaught error and breaks the iteration.
                results_by_name[name] = RunResult(
                    run_id="run-unknown",
                    config=name,
                    connector="unknown",
                    target="unknown",
                    destination="unknown",
                    status=RunStatus.FAILED,
                    started_at=datetime.now(UTC),
                    ended_at=datetime.now(UTC),
                    streams=[],
                    rows_loaded=0,
                    full_refresh=full_refresh,
                    error=exc,
                    log_path="",
                )

    # Stash the parallelism summary so the CLI can render it at the end of
    # the multi-run summary block (after print_multi_run_summary). See the
    # NOTE on _LAST_RUN_TAG_PARALLELISM for the design rationale — the
    # alternative is a breaking API change to ``run_tag``.
    global _LAST_RUN_TAG_THREADS, _LAST_RUN_TAG_PARALLELISM
    _LAST_RUN_TAG_THREADS = threads
    _LAST_RUN_TAG_PARALLELISM = dict(clamped)

    # Return in matched-name order, NOT completion order — the public
    # contract (and the regression test the sequential path satisfies).
    return [results_by_name[p.name] for p in matching]


def run(
    config: str,
    *,
    project_dir: str | Path | None = None,
    target_override: str | None = None,
    params_override: Mapping[str, Any] | None = None,
    destination_params_override: Mapping[str, Any] | None = None,
    full_refresh: bool = False,
    select: tuple[str, ...] = (),
    threads: int | None = None,
    _log_stream: TextIO | None = None,
) -> RunResult:
    """Run one config end to end — the 6-stage lifecycle (docs/02), config-driven.

    This is the engine. The CLI and the library both call it (it is re-exported
    as :func:`dtex.run`). It executes one synchronous pass:

    1. **DISCOVER** — find the project root (``project_dir`` or walk up for
       ``dtex_project.yml``); load ``configs/`` and look up ``config``; resolve
       the source and destination it names (project-local-first per docs/03 §5).
    2. **RESOLVE** — merge every config layer into a frozen :class:`RunConfig`
       and immutable per-connector :class:`Config` objects (docs/03 §6, docs/12).
    3. **INIT DEST** — bind the destination hooks, fix the capability tier via
       ``capabilities()``, ``open`` the connection.
    4. **LOAD STATE** — ``read_state`` the prior :class:`StateRecord` set,
       indexed by stream name (keyed by *source* name — state is a property of
       the source, not the config).
    5. **RUN STREAMS** — for each selected stream in declared order: build its
       context, resolve its schema, ``ensure_schema``, drive the generator and
       ``write_batch`` each batch, then ``commit_state`` *that stream's* record
       immediately (per-stream commit — docs/02 §Commit granularity).
    6. **RUN RECORD** — build and return the :class:`RunResult`; ``close`` the
       destination in a ``finally``.

    Parameters:

    * ``config`` — the config NAME (the CLI's ``-p/--conf`` arg, the key under
      ``configs/``).
    * ``project_dir`` — the project root, or a directory under it to walk up
      from; defaults to the current working directory.
    * ``target_override`` — overrides the config's ``target:``; falls back to
      the named target, then ``profiles.yml[<dest>].default_target`` (docs/06).
    * ``params_override`` — per-invocation source param overrides; merged
      *on top of* the config's ``params:`` block (highest precedence layer for
      a source param — docs/03 §6).
    * ``destination_params_override`` — per-invocation destination param
      overrides; merged on top of ``PipelineConfig.destination_params`` and
      the ``profiles.yml`` row.
    * ``full_refresh`` — when ``True``, incremental cursors ignore prior state
      and re-extract from the beginning (docs/03 §3.2).
    * ``select`` — when non-empty, *replaces* the config's ``select:`` (the CLI
      ``--select`` semantics, docs/07).
    * ``threads`` — stream-level concurrency within this one config. ``None``
      or ``1`` (the default) runs streams sequentially in declared order —
      byte-identical to the pre-parallel behavior. ``>1`` runs up to that many
      streams concurrently via a :class:`~concurrent.futures.ThreadPoolExecutor`,
      clamped down by the destination's ``@destination.max_concurrent_writes``
      (so DuckDB, which returns 1, always serializes). Failure and result
      ordering are unchanged: one stream failing fails the run, and the
      returned ``streams`` list is always in declared order regardless of
      completion order.

    Never raises on a connector/destination failure: returns a ``RunResult``
    with ``status=FAILED`` and a populated ``error`` (docs/07 §4.1). Callers
    wanting an exception use ``run(...).raise_for_status()``.
    """
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    src_overrides: dict[str, Any] = dict(params_override or {})
    dest_overrides: dict[str, Any] = dict(destination_params_override or {})

    # Defaults so an early-failure RunResult is still well-formed.
    config_name = config
    connector_name = "unknown"
    destination_name = "unknown"
    target_name = target_override or "default"
    streams: list[StreamResult] = []
    conn: Any = None
    hooks: dict[str, Callable[..., Any]] | None = None
    capabilities: set[Capability] = set()
    run_log: RunLog | None = None
    log_path: str = ""
    # The RunResult assembled in the try/except. Captured into this slot so
    # the finally block can build the RunRecord from the same authoritative
    # data (the in-memory shape is the source; the record is its
    # persistence-layer twin — docs/09 §4).
    final_result: RunResult | None = None
    # The shared redactor is created here so the JSONL writer (opened before
    # stage 2 RESOLVE) and the stdlib logger (rebuilt after secrets resolve)
    # both mask through the same mutable bag of secret values (docs/09 §5).
    redactor = Redactor()
    # ``_log_stream`` is threaded through both build_logger calls so a
    # parallel run_tag can capture each pipeline's stdlib-logger output
    # into a per-pipeline StringIO (stage 8e). ``None`` ⇒ stderr (the
    # default; today's behavior).
    log = build_logger(run_id, redactor, stream=_log_stream)

    try:
        # -- Stage 1: DISCOVER ----------------------------------------------
        project_root = disc.find_project_root(project_dir)
        # Project-local plugin file: ``dtex_plugins.py`` next to
        # ``dtex_project.yml`` (stage 9a — docs/08 §3). The file (if present)
        # calls ``dtex.register_secret_resolver(...)`` to register custom
        # ``secret://`` schemes. Idempotent per project per process. A
        # plugin-file import error surfaces as :class:`SecretResolutionError`,
        # which is caught by the run loop's outer ``except`` and folded into
        # a FAILED :class:`RunResult`. See :func:`dtex.secrets.load_project_plugins`.
        load_project_plugins(project_root)
        project = cfg.ProjectConfig.load(project_root)
        profiles = cfg.Profiles.load(project_root)

        # Open the JSONL log file as soon as the project root is known — a
        # destination-open failure (or any later stage) is still captured.
        # ``.dtex/logs/`` is per-project; the dir is created lazily.
        run_log = RunLog(run_id, project_root / ".dtex" / "logs", redactor)
        log_path = str(run_log.path)
        log = build_logger(run_id, redactor, run_log=run_log, stream=_log_stream)

        pipeline: PipelineConfig = cfgs.load_config(
            config, project_root, list(project.config_paths)
        )
        connector_name = pipeline.source
        destination_name = pipeline.destination

        target_name = cfg.resolve_target_name(
            target_override if target_override is not None else pipeline.target,
            destination_name,
            profiles,
        )

        source = disc.resolve_source(
            pipeline.source, project_root, list(project.source_paths)
        )
        # Tolerate but warn on a legacy source-side `destination:` block
        # (docs/03 §2.3 historical / types.py::DestinationBinding NOTE).
        if source.manifest.destination is not None:
            logging.getLogger("dtex.engine").warning(
                "source %r still carries a legacy register.yaml 'destination:' "
                "block (%r); ignoring — configs/%s.yml binds the destination "
                "now (docs/12)",
                pipeline.source,
                source.manifest.destination.connector,
                config,
            )
        dest = disc.resolve_destination(
            pipeline.destination, project_root, list(project.destination_paths)
        )

        # Validate that every `streams:` key names an actual stream on the
        # resolved source, and that mode=incremental only appears on streams
        # whose register.yaml declares a cursor. The parser layer
        # (PipelineConfig.from_dict) cannot do either — it doesn't know
        # which source the config will bind to — so the engine does it
        # here, as soon as both sides are known. A typo'd stream name is
        # silently ignored otherwise (the per-stream lookup just never
        # hits), and that is the "silent
        # drop" pattern the rest of the codebase rejects (unknown YAML keys
        # are hard errors, unknown configs list known names).
        _validate_streams_block(pipeline, source)

        # -- Stage 2: RESOLVE -----------------------------------------------
        source_config = cfg.build_source_config(
            source.manifest,
            project,
            pipeline,
            target_name=target_name,
            profiles=profiles,
            overrides=src_overrides,
        )
        dest_config = cfg.build_destination_config(
            dest.manifest,
            project,
            pipeline,
            target_name=target_name,
            profiles=profiles,
            overrides=dest_overrides,
        )

        # Resolved-secret values now exist; load them into the shared redactor
        # so every subsequent emission (stdlib + JSONL) masks them. Both
        # source and destination secrets are registered — stage 9a fixed an
        # asymmetry where destination-side secrets (a future destination
        # carrying a credential ref) were not redacted. The Redactor dedupes
        # short / repeated values, so calling ``add`` twice is harmless.
        redactor.add(source_config.secrets.values())
        redactor.add(dest_config.secrets.values())

        # The config's `streams:` block defines the in-scope stream set
        # for this pipeline. CLI --select NARROWS further (intersection):
        # only streams that are both in `streams:` AND in --select run.
        # A --select name that isn't in `streams:` is a hard error — you
        # can't materialize a stream the pipeline blueprint doesn't list.
        # `streams: all` (pipeline.all_streams=True) expands to every
        # stream the source declares.
        if pipeline.all_streams:
            in_scope = tuple(s.name for s in source.manifest.streams)
        else:
            in_scope = tuple(pipeline.streams)
        if select:
            requested = set(select)
            in_scope_set = set(in_scope)
            unknown = sorted(requested - in_scope_set)
            if unknown:
                raise EngineError(
                    f"config {config_name!r}: --select names stream(s) not in "
                    f"the config's 'streams:' block: "
                    f"{', '.join(repr(s) for s in unknown)}; in-scope: "
                    f"{', '.join(repr(s) for s in in_scope) or '(none)'}"
                )
            effective_select = tuple(s for s in in_scope if s in requested)
        else:
            effective_select = in_scope

        run_config = RunConfig(
            run_id=run_id,
            pipeline=config_name,
            connector=connector_name,
            target=target_name,
            config=source_config,
            select=effective_select,
            full_refresh=full_refresh,
        )

        # The single ``run_start`` event — emitted *after* discovery + resolve
        # succeed so every config/source/destination/target field is known
        # (docs/09 §2 event table). A pre-discovery failure has no run_start,
        # which itself signals "the run never began."
        run_log.emit(
            "run_start",
            config=config_name,
            source=connector_name,
            destination=destination_name,
            target=target_name,
            full_refresh=full_refresh,
        )

        # -- Stage 3: INIT DEST ---------------------------------------------
        hooks, capabilities = _resolve_destination_hooks(dest)
        conn = hooks["open"](Config(params=dict(dest_config.params)))

        # -- Stage 4: LOAD STATE --------------------------------------------
        # State is keyed by *source* name, not config name: rerunning under a
        # different config that shares this source resumes off the same cursor.
        prior_records = hooks["read_state"](conn, source.manifest.name)
        state_by_stream: dict[str, StateRecord] = {
            r.stream: r for r in prior_records
        }

        # -- Stage 4b: LEASES (docs/05 §5.5) --------------------------------
        # Only when the destination hosts leases. Otherwise ``leases`` stays
        # None and the whole path below is inert — the pre-leasing behavior,
        # so a destination without Capability.LEASE runs exactly as before.
        leases: _LeaseCoordinator | None = None
        if Capability.LEASE in capabilities:
            leases = _LeaseCoordinator(
                hooks,
                conn,
                connector=source.manifest.name,
                run_id=run_id,
                max_parallel=project.max_parallel_for(source.manifest.name),
                log=log,
            )

        # Pre-build a per-stream Config for any stream with a non-empty
        # `streams[name].params` overlay (docs/12 §3.4 — precedence layer 4).
        # Streams without an overlay share the base `source_config`; this
        # keeps the overhead surgical (one extra Config build per stream
        # that actually overrides something).
        per_stream_config: dict[str, Any] = {}
        for stream_name in (pipeline.streams or {}):
            sr = pipeline.streams[stream_name]
            if sr.params:
                per_stream_config[stream_name] = cfg.build_source_config(
                    source.manifest,
                    project,
                    pipeline,
                    target_name=target_name,
                    profiles=profiles,
                    overrides=src_overrides,
                    stream_name=stream_name,
                )

        # -- Stage 5: RUN STREAMS -------------------------------------------
        # Effective stream concurrency: the caller's ``threads`` (or 1),
        # clamped DOWN by the destination's ``@destination.max_concurrent_writes``
        # so a destination that can't take concurrent writers (DuckDB → 1)
        # serializes regardless of what the operator asked for. A destination
        # without the hook is treated as unbounded (only ``threads`` limits it).
        selected_streams = [
            sd for sd in source.manifest.streams if run_config.selects(sd.name)
        ]
        for sd in source.manifest.streams:
            if not run_config.selects(sd.name):
                streams.append(
                    StreamResult(name=sd.name, status=StreamStatus.SKIPPED)
                )
        requested_threads = max(1, int(threads)) if threads is not None else 1
        dest_cap = _stream_write_cap(hooks, dest_config)
        effective_threads = max(1, min(requested_threads, dest_cap))

        # -- Leasing: acquire the WHOLE selected set up front, in one batched
        # call from this (the main) thread — docs/05 §5.5. Streams a live
        # foreign lease holds, or that the max_parallel cap excludes, come back
        # unleased and are recorded SKIPPED_LEASED without ever running. No
        # lease DML is issued from a worker thread (see _LeaseCoordinator's
        # NOTE: per-stream lease writes from N threads collide on BigQuery,
        # which serializes DML per table).
        leased: set[str] | None = None
        if leases is not None:
            leased = leases.acquire_all([sd.name for sd in selected_streams])
            for sd in selected_streams:
                if sd.name not in leased:
                    run_log.emit("stream_skipped_leased", stream=sd.name)
                    streams.append(
                        StreamResult(
                            name=sd.name, status=StreamStatus.SKIPPED_LEASED
                        )
                    )
            selected_streams = [sd for sd in selected_streams if sd.name in leased]

        # The per-stream unit of work, shared by the sequential and parallel
        # dispatchers. Returns the StreamResult to record, or raises on a
        # genuine stream failure — the raise is caught by the dispatcher, which
        # fails the run (unchanged semantics). Leases are NOT touched here.
        stream_error: Exception | None = None
        error_lock = threading.Lock()
        # Terminal lease status per stream, filled as streams finish and
        # applied in one batched release after the dispatcher drains.
        lease_statuses: dict[str, LeaseStatus] = {}

        def _execute_stream(stream_def: StreamDef) -> StreamResult:
            log.info("running stream %r", stream_def.name)
            # active_stream is a single-valued field that can't represent
            # multiple concurrent streams; set it only in the sequential path
            # (effective_threads == 1). Every JSONL event already carries an
            # explicit ``stream=`` field, so per-event attribution is intact in
            # parallel mode — active_stream is just the fallback tag for stdlib
            # log lines, left None (untagged) when streams run concurrently.
            if effective_threads == 1:
                run_log.active_stream = stream_def.name
            try:
                result = _run_one_stream(
                    stream_def,
                    source,
                    hooks,
                    conn,
                    run_config,
                    pipeline,
                    state_by_stream.get(stream_def.name),
                    log,
                    run_log=run_log,
                    stream_config_override=per_stream_config.get(stream_def.name),
                    # The between-batch beat is wired only in the SEQUENTIAL
                    # path, where this *is* the main thread — so the batched
                    # heartbeat statement is still single-threaded. In parallel
                    # mode the dispatcher below beats from the main thread
                    # instead; a worker must never issue lease DML.
                    heartbeat=(
                        (lambda: _safe_beat(leases, log))
                        if leases is not None and effective_threads == 1
                        else None
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — recorded, then re-raised.
                run_log.emit(
                    "stream_failed",
                    stream=stream_def.name,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    traceback=_format_traceback(exc),
                )
                # Record the terminal lease status; the single batched release
                # after the dispatcher applies it. (A crash still frees the
                # lease promptly — the release runs in the finally below.)
                with error_lock:
                    lease_statuses[stream_def.name] = LeaseStatus.FAILED
                raise
            else:
                with error_lock:
                    lease_statuses[stream_def.name] = LeaseStatus.DONE
                log.info(
                    "stream %r loaded %d row(s)", stream_def.name, result.rows_loaded
                )
                return result
            finally:
                if effective_threads == 1:
                    run_log.active_stream = None

        # Results keyed by stream name so the final list can be rebuilt in
        # declared order regardless of completion order (the public contract).
        results_by_name: dict[str, StreamResult] = {}

        if effective_threads == 1 or len(selected_streams) <= 1:
            # Sequential path — a literal loop, kept distinct from the
            # single-worker executor for debuggability (a stack trace is the
            # caller's own thread; breakpoint() Just Works). Stops at the first
            # failure, exactly as before parallelism existed.
            for stream_def in selected_streams:
                try:
                    results_by_name[stream_def.name] = _execute_stream(stream_def)
                except Exception as exc:  # noqa: BLE001 — recorded, then re-raised below.
                    results_by_name[stream_def.name] = StreamResult(
                        name=stream_def.name, status=StreamStatus.FAILED
                    )
                    stream_error = exc
                    break
        else:
            # Parallel path — up to ``effective_threads`` streams at once. A
            # failing stream fails the run (first exception wins), but in-flight
            # streams are allowed to finish rather than being force-cancelled:
            # each holds its own destination transaction / lease and cancelling
            # mid-write could strand a half-committed load or an unreleased
            # lease. Not-yet-started futures ARE cancelled so a failure stops
            # new work promptly.
            with ThreadPoolExecutor(
                max_workers=effective_threads,
                thread_name_prefix=f"dtex-{config_name}",
            ) as pool:
                future_to_stream = {
                    pool.submit(_execute_stream, sd): sd for sd in selected_streams
                }
                # Collect completions while pumping the batched lease heartbeat
                # from THIS (the main) thread. ``wait`` with a timeout is what
                # gives the main thread a periodic wake-up: ``as_completed``
                # would block until a stream finishes, and a stream that runs
                # longer than LEASE_STALE_SECONDS would let its own lease go
                # stale mid-run. The beat is self-throttled, so waking often is
                # cheap — the vast majority of these calls return immediately.
                pending_futures = set(future_to_stream)
                while pending_futures:
                    done, pending_futures = wait(
                        pending_futures, timeout=_LEASE_PUMP_INTERVAL_SECONDS
                    )
                    if leases is not None:
                        _safe_beat(leases, log)
                    for future in done:
                        stream_def = future_to_stream[future]
                        try:
                            results_by_name[stream_def.name] = future.result()
                        except CancelledError:
                            # Cancelled after a sibling failed — never started,
                            # so there is nothing to record beyond the FAILED
                            # run the sibling already caused.
                            continue
                        except Exception as exc:  # noqa: BLE001 — first failure fails the run.
                            results_by_name[stream_def.name] = StreamResult(
                                name=stream_def.name, status=StreamStatus.FAILED
                            )
                            with error_lock:
                                if stream_error is None:
                                    stream_error = exc
                            # Cancel queued-but-unstarted work; running futures
                            # run on (cancelling mid-write could strand a
                            # half-committed load).
                            for p in pending_futures:
                                p.cancel()

        # Every stream has finished (or was cancelled): release all leases this
        # run holds in ONE batched call, from the main thread. A stream that
        # never recorded a status (cancelled before it started) is released
        # FAILED by release_all's default — conservative and honest.
        if leases is not None:
            _safe_release_all(leases, lease_statuses, log)

        # ``streams`` currently holds only the SKIPPED entries; merge the
        # executed results back in manifest order for a stable, declared-order
        # final list.
        skipped_by_name = {s.name: s for s in streams}
        streams = []
        for sd in source.manifest.streams:
            if sd.name in results_by_name:
                streams.append(results_by_name[sd.name])
            elif sd.name in skipped_by_name:
                streams.append(skipped_by_name[sd.name])

        if stream_error is not None:
            raise stream_error

        ended_at = datetime.now(UTC)
        total_rows = sum(s.rows_loaded for s in streams)
        final_result = RunResult(
            run_id=run_id,
            config=config_name,
            connector=connector_name,
            target=target_name,
            destination=destination_name,
            status=RunStatus.SUCCEEDED,
            started_at=started_at,
            ended_at=ended_at,
            streams=streams,
            rows_loaded=total_rows,
            full_refresh=full_refresh,
            log_path=log_path,
        )
        return final_result

    except Exception as exc:  # noqa: BLE001 — run() never raises; see docstring.
        log.error("run failed: %s: %s", type(exc).__name__, exc)
        final_result = RunResult(
            run_id=run_id,
            config=config_name,
            connector=connector_name,
            target=target_name,
            destination=destination_name,
            status=RunStatus.FAILED,
            started_at=started_at,
            ended_at=datetime.now(UTC),
            streams=streams,
            rows_loaded=sum(s.rows_loaded for s in streams),
            full_refresh=full_refresh,
            error=exc,
            log_path=log_path,
        )
        return final_result
    finally:
        # The RunResult is built; rebind it into a RunRecord (the
        # persistence-layer twin — docs/09 §4) and persist it before
        # close. ``final_result`` is None ONLY if a BaseException escaped
        # both branches (e.g. KeyboardInterrupt during except-block
        # assignment) — in that case we still want a record on disk if
        # possible, so build one from the loose locals.
        record = _build_run_record(
            run_id=run_id,
            config_name=config_name,
            connector_name=connector_name,
            destination_name=destination_name,
            target_name=target_name,
            started_at=started_at,
            streams=streams,
            full_refresh=full_refresh,
            final_result=final_result,
        )
        # Run-record write goes BEFORE close, INSIDE the finally so it
        # runs on success and failure paths (docs/09 §4: "A run record is
        # written even when a run fails"). Failure to write the audit row
        # must not mask the run's real error, so any exception here is
        # logged and dropped (the JSONL file is the durable fallback).
        if (
            conn is not None
            and hooks is not None
            and Capability.RUN_RECORDS in capabilities
            and "write_run_record" in hooks
        ):
            try:
                hooks["write_run_record"](conn, record)
            except Exception as wr_exc:  # noqa: BLE001 — must not mask the real error.
                log.error(
                    "write_run_record failed for run %s: %s: %s",
                    run_id,
                    type(wr_exc).__name__,
                    wr_exc,
                )

        if run_log is not None:
            run_log.emit(
                "run_end",
                status=record.status.value,
                rows_loaded=record.rows_loaded,
                duration_s=record.duration_s,
                error_type=record.error_type,
                error_message=record.error_message,
            )
            run_log.close()

        if conn is not None and hooks is not None:
            hooks["close"](conn)
