"""The run loop — the 6-stage run lifecycle (docs/02 §Run lifecycle).

This module is the engine's keystone: :func:`run` executes one synchronous pass
of the lifecycle docs/02 fixes — DISCOVER → RESOLVE → INIT DEST → LOAD STATE →
RUN STREAMS → RUN RECORD — and returns a :class:`~det.types.RunResult`.

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
import traceback as _tb
import uuid
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from det.engine import config as cfg
from det.engine import configs as cfgs
from det.engine import discovery as disc
from det.engine.logger import Redactor, RunLog, build_logger
from det.registry import compute_injection
from det.types import (
    Batch,
    Capability,
    Config,
    Cursor,
    CursorType,
    Field,
    FieldType,
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
    StreamResult,
    StreamStatus,
)

# The destination hooks the engine drives in a non-state-aware run. Tier A
# destinations (Capability.STATE) additionally need read_state / commit_state;
# that conditional rule is applied in _resolve_destination_hooks (docs/03 §3.4
# leaves the capability-dependent check to the engine).
_CORE_HOOKS = ("capabilities", "open", "ensure_schema", "write_batch", "close")
_STATE_HOOKS = ("read_state", "commit_state")


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
    return hooks, capabilities


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
    prior: StateRecord | None,
    log: Any,
    run_log: RunLog | None = None,
) -> StreamResult:
    """Run one stream end to end — docs/02 §Run lifecycle step 5 (a–d).

    Emits ``stream_start`` / ``batch_loaded`` / ``stream_committed`` events to
    ``run_log`` (docs/09 §2) when one is supplied. ``stream_failed`` is
    emitted by the caller when this raises — the exception carries the data
    needed (error_type / message / traceback) and is fully recorded there.
    """
    registration = source.registry.stream(stream_def.name)
    if registration is None:  # pragma: no cover — validate_connector caught it.
        raise EngineError(f"stream {stream_def.name!r} has no registered @stream function")

    # -- 5a: cursor (incremental streams only) ------------------------------
    cursor: Cursor | None = None
    cursor_before: Any = None
    if stream_def.is_incremental:
        inc = stream_def.incremental
        assert inc is not None
        seed = _seed_value(prior, inc.cursor_type, inc.initial_value)
        cursor_before = None if run_config.full_refresh else seed
        cursor = Cursor(
            cursor_field=inc.cursor_field,
            cursor_type=inc.cursor_type,
            start_value=seed,
            is_full_refresh=run_config.full_refresh,
        )

    state = State(prior.state_blob if prior is not None else None)

    available: dict[str, Any] = {
        "config": run_config.config,
        "state": state,
        "log": log,
    }
    if cursor is not None:
        available["cursor"] = cursor
    kwargs = compute_injection(registration.func, available)

    if run_log is not None:
        run_log.emit(
            "stream_start",
            stream=stream_def.name,
            disposition=stream_def.write_disposition.value,
            cursor_before=cursor_before,
        )

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

    stream_meta = StreamMeta.from_stream_def(stream_def, resolved_schema)
    hooks["ensure_schema"](conn, stream_meta)

    # -- 5c/5d: LOAD + COMMIT — inside the per-stream transaction -----------
    with _stream_transaction(hooks, conn, stream_meta):
        if first_batch is not None:
            rows_extracted += len(first_batch)
            written = hooks["write_batch"](conn, first_batch, stream_meta)
            rows_loaded += written
            if run_log is not None:
                run_log.emit(
                    "batch_loaded",
                    stream=stream_def.name,
                    rows=written,
                    cumulative_rows=rows_loaded,
                )
            for batch in batches:
                rows_extracted += len(batch)
                written = hooks["write_batch"](conn, batch, stream_meta)
                rows_loaded += written
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

        record = StateRecord(
            connector=run_config.connector,
            stream=stream_def.name,
            cursor_value=cursor_after,
            cursor_type=(
                stream_def.incremental.cursor_type
                if stream_def.incremental is not None
                else None
            ),
            state_blob=state.to_dict(),
            last_run_id=run_config.run_id,
            rows_total=(prior.rows_total if prior is not None else 0) + rows_loaded,
        )
        hooks["commit_state"](conn, run_config.run_id, [record])

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

    The full traceback lands in ``.det/logs/<run_id>/run.jsonl`` (the
    forensics surface — docs/09 §3.2); deliberately NOT in the ``_det_runs``
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


# ---------------------------------------------------------------------------
# The run loop — the public engine entry point
# ---------------------------------------------------------------------------


def run(
    config: str,
    *,
    project_dir: str | Path | None = None,
    target_override: str | None = None,
    params_override: Mapping[str, Any] | None = None,
    destination_params_override: Mapping[str, Any] | None = None,
    full_refresh: bool = False,
    select: tuple[str, ...] = (),
) -> RunResult:
    """Run one config end to end — the 6-stage lifecycle (docs/02), config-driven.

    This is the engine. The CLI and the library both call it (it is re-exported
    as :func:`det.run`). It executes one synchronous pass:

    1. **DISCOVER** — find the project root (``project_dir`` or walk up for
       ``det_project.yml``); load ``configs/`` and look up ``config``; resolve
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
    log = build_logger(run_id, redactor)

    try:
        # -- Stage 1: DISCOVER ----------------------------------------------
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
        profiles = cfg.Profiles.load(project_root)

        # Open the JSONL log file as soon as the project root is known — a
        # destination-open failure (or any later stage) is still captured.
        # ``.det/logs/`` is per-project; the dir is created lazily.
        run_log = RunLog(run_id, project_root / ".det" / "logs", redactor)
        log_path = str(run_log.path)
        log = build_logger(run_id, redactor, run_log=run_log)

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
            logging.getLogger("det.engine").warning(
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
        # so every subsequent emission (stdlib + JSONL) masks them.
        redactor.add(source_config.secrets.values())

        # CLI --select REPLACES the config's select (not unions). docs/07.
        effective_select = tuple(select) if select else pipeline.select

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

        # -- Stage 5: RUN STREAMS (sequential, declared order) --------------
        stream_error: Exception | None = None
        for stream_def in source.manifest.streams:
            if not run_config.selects(stream_def.name):
                streams.append(
                    StreamResult(name=stream_def.name, status=StreamStatus.SKIPPED)
                )
                continue
            log.info("running stream %r", stream_def.name)
            run_log.active_stream = stream_def.name
            try:
                result = _run_one_stream(
                    stream_def,
                    source,
                    hooks,
                    conn,
                    run_config,
                    state_by_stream.get(stream_def.name),
                    log,
                    run_log=run_log,
                )
            except Exception as exc:  # noqa: BLE001 — recorded, then re-raised.
                streams.append(
                    StreamResult(name=stream_def.name, status=StreamStatus.FAILED)
                )
                # Capture the traceback in the JSONL (the table-side record
                # deliberately omits tracebacks — docs/09 §4 NOTE).
                run_log.emit(
                    "stream_failed",
                    stream=stream_def.name,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                    traceback=_format_traceback(exc),
                )
                stream_error = exc
                break
            else:
                streams.append(result)
                log.info(
                    "stream %r loaded %d row(s)", stream_def.name, result.rows_loaded
                )
            finally:
                run_log.active_stream = None

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
