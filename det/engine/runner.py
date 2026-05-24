"""The run loop — the 6-stage run lifecycle (docs/02 §Run lifecycle).

This module is the engine's keystone: :func:`run` executes one synchronous pass
of the lifecycle docs/02 fixes — DISCOVER → RESOLVE → INIT DEST → LOAD STATE →
RUN STREAMS → RUN RECORD — and returns a :class:`~det.types.RunResult`.

The destination hooks are driven in the exact order docs/03 §3.4 / docs/05 §1
fix::

    open → read_state → [ensure_schema → write_batch ...]* → commit_state → close

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

import uuid
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from det.engine import config as cfg
from det.engine import discovery as disc
from det.engine.logger import build_logger
from det.registry import compute_injection
from det.types import (
    Batch,
    Capability,
    Config,
    Cursor,
    CursorType,
    Field,
    FieldType,
    RunConfig,
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

    docs/03 §3.2: parsing ``initial_value`` (a YAML string) into its typed form
    is engine work — an ``int`` cursor's ``"0"`` becomes ``0``, a ``date``
    cursor's ``"2024-01-01"`` becomes a :class:`datetime.date`, so the value the
    :class:`Cursor` hands the connector compares correctly. ``Cursor`` itself is
    deliberately dumb and stores whatever it is handed.
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

# Python type → det FieldType, for inferring a schema from the first batch
# of a stream that declares none (docs/02 §Normalize: "infer from the first
# batch"). bool is checked before int because bool is an int subclass.
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
    """Infer a column's :class:`FieldType` from one sample value — docs/02 §Normalize.

    Used only for a stream that declares no ``schema``. A ``None`` sample gives
    no type signal, so it defaults to ``STRING`` (the widest portable type); an
    unrecognized Python type likewise falls back to ``STRING``.
    """
    if value is None:
        return FieldType.STRING
    for py_type, field_type in _PY_TO_FIELD_TYPE:
        if isinstance(value, py_type):
            return field_type
    return FieldType.STRING


def _infer_schema(first_batch: Batch) -> Schema:
    """Infer a :class:`Schema` from a stream's first batch — docs/02 §Normalize.

    docs/02: when a stream omits ``schema`` the engine "infers it from the first
    batch". Columns are collected in first-seen order across the batch's records
    (records may be ragged); each column's type comes from the first non-``None``
    sample seen for it. An empty first batch yields an empty schema — the
    destination then creates a table the engine evolves as later batches arrive.
    """
    columns: list[str] = []
    types: dict[str, FieldType] = {}
    for record in first_batch:
        for key, value in record.items():
            if key not in types:
                columns.append(key)
                types[key] = _infer_field_type(value)
            elif types[key] is FieldType.STRING and value is not None:
                # A later record gives a stronger type signal than an earlier
                # all-None column — upgrade off the STRING fallback.
                types[key] = _infer_field_type(value)
    return Schema(fields=tuple(Field(name=c, type=types[c]) for c in columns))


def _check_strict_schema(stream: StreamDef, declared: Schema, first_batch: Batch) -> None:
    """Fail a ``strict`` stream whose first batch diverges from its schema — docs/05 §3.2.

    Locked decision: ``schema_contract: strict`` means "any schema difference
    from the declared schema fails the run". The engine enforces it *before*
    ``ensure_schema`` (the destination's ``ensure_schema`` is always additive
    and never sees the contract — destination.py docstring). A record carrying a
    column the schema does not declare is the divergence ``strict`` forbids; a
    declared column merely *absent* from a batch is fine (it is ``NULL``).

    Raises :class:`EngineError` naming the offending columns.
    """
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
    """Bind a destination's ``@destination`` hooks and read its capability tier.

    docs/03 §3.4: ``capabilities`` / ``open`` / ``ensure_schema`` /
    ``write_batch`` / ``close`` are unconditionally mandatory;
    ``read_state`` / ``commit_state`` are mandatory only when the destination
    declares :attr:`Capability.STATE` (Tier A). This function applies that
    capability-dependent rule — the registry deliberately leaves it to the
    engine because it needs the parsed ``capabilities()`` result.

    Returns the hook-name → callable map and the capability set. A missing
    mandatory hook raises :class:`EngineError`.
    """
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
        # Tier A — the destination hosts its own _det_state table, so it
        # must implement read_state + commit_state (docs/05 §5).
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
        # Tier B (object storage) is documented but not exercised in v1: it
        # would route state through a @destination.state_backend companion
        # (docs/02 §tiers, docs/05 §5.4). The engine fails clearly rather than
        # silently dropping state for a destination it cannot persist state for.
        raise EngineError(
            f"destination {dest.manifest.name!r} does not declare Capability.STATE; "
            f"Tier B (companion state backend) destinations are not supported in v1"
        )

    if Capability.TRANSACTIONAL_LOAD in capabilities:
        # The destination promises atomic per-stream loads — it must provide the
        # @destination.transaction context the engine wraps each stream's
        # write_batch + commit_state block in (docs/05 §5.3).
        hook = registry.hook("transaction")
        if hook is None:
            raise EngineError(
                f"destination {dest.manifest.name!r} declares "
                f"Capability.TRANSACTIONAL_LOAD but is missing the "
                f"@destination.transaction hook required to honor it (docs/05 §5.3)"
            )
        hooks["transaction"] = hook.func
    return hooks, capabilities


# ---------------------------------------------------------------------------
# Stream execution — one stream's EXTRACT → NORMALIZE → LOAD → COMMIT
# ---------------------------------------------------------------------------


def _stream_transaction(
    hooks: Mapping[str, Callable[..., Any]],
    conn: Any,
    stream_meta: StreamMeta,
) -> Any:
    """Return the context wrapping a stream's load + state commit — docs/05 §5.3.

    When the destination declares :attr:`Capability.TRANSACTIONAL_LOAD` it
    provides a ``@destination.transaction`` hook; the engine enters it around
    each stream's ``[write_batch… → commit_state]`` block so data and cursor
    flip atomically and a crash mid-stream rolls back (no half-written
    ``append`` duplicates). A destination without the capability gets a
    :func:`~contextlib.nullcontext` — the load runs in the destination's own
    statement-level semantics, unchanged.
    """
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
) -> StreamResult:
    """Run one stream end to end — docs/02 §Run lifecycle step 5 (a–d).

    The per-stream pipeline (docs/02 §extract → normalize → load):

    * **5a/5b — build context** — the stream's :class:`State` (seeded from the
      prior ``state_blob``) and, if incremental, its :class:`Cursor` (seeded
      from the prior committed value, or ``initial_value``, or ``None`` under
      ``--full-refresh``).
    * **5b — resolve schema** — the declared schema, else one inferred from the
      first batch (``evolve`` mode); a ``strict`` stream's first batch is
      checked against its declared schema and a divergence fails the run.
    * **5c — ensure + load** — ``ensure_schema`` once (outside the transaction —
      DDL implicitly commits on some destinations), then, inside the
      per-stream transaction (docs/05 §5.3), drive the ``@stream`` generator,
      ``write_batch`` each yielded batch, and ``commit_state`` the advanced
      cursor. The transaction makes the data + cursor flip atomic.

    Returns the stream's :class:`StreamResult`. Raises whatever the connector
    raises — the per-stream transaction rolls the partial load back, then the
    caller marks the stream FAILED and stops the run, keeping earlier streams'
    committed state.
    """
    registration = source.registry.stream(stream_def.name)
    if registration is None:  # pragma: no cover — validate_connector caught it.
        raise EngineError(f"stream {stream_def.name!r} has no registered @stream function")

    # -- 5a: cursor (incremental streams only) ------------------------------
    cursor: Cursor | None = None
    cursor_before: Any = None
    if stream_def.is_incremental:
        inc = stream_def.incremental
        assert inc is not None  # is_incremental guarantees this.
        seed = _seed_value(prior, inc.cursor_type, inc.initial_value)
        cursor_before = None if run_config.full_refresh else seed
        cursor = Cursor(
            cursor_field=inc.cursor_field,
            cursor_type=inc.cursor_type,
            start_value=seed,
            is_full_refresh=run_config.full_refresh,
        )

    # -- 5a: per-stream State scratch space, seeded from prior state_blob ----
    state = State(prior.state_blob if prior is not None else None)

    # The injectables the engine has on hand; compute_injection picks the subset
    # the @stream function actually declared (docs/03 §3.1).
    available: dict[str, Any] = {
        "config": run_config.config,
        "state": state,
        "log": log,
    }
    if cursor is not None:
        available["cursor"] = cursor
    kwargs = compute_injection(registration.func, available)

    # -- 5b: NORMALIZE — pull the first batch and resolve the schema --------
    # The generator is iterated manually so ensure_schema can run *before* the
    # per-stream transaction opens: DDL implicitly commits on some destinations
    # (e.g. DuckDB), so a CREATE/ALTER inside the transaction would break its
    # atomicity. ensure_schema therefore stays outside; write_batch +
    # commit_state run inside (docs/05 §5.3).
    rows_loaded = 0
    rows_extracted = 0
    batches = iter(registration.func(**kwargs))
    first_batch = next(batches, None)

    if first_batch is not None and stream_def.schema is not None:
        if stream_def.schema_contract is SchemaContract.STRICT:
            _check_strict_schema(stream_def, stream_def.schema, first_batch)
        resolved_schema = stream_def.schema
    elif first_batch is not None:
        # evolve mode — infer the schema from the first batch (docs/02 §Normalize).
        resolved_schema = _infer_schema(first_batch)
    else:
        # The generator yielded nothing — use the declared schema, or an empty
        # one, so ensure_schema still creates an (empty) table for the stream.
        resolved_schema = stream_def.schema if stream_def.schema is not None else Schema()

    stream_meta = StreamMeta.from_stream_def(stream_def, resolved_schema)
    hooks["ensure_schema"](conn, stream_meta)

    # -- 5c/5d: LOAD + COMMIT — inside the per-stream transaction -----------
    # write_batch each batch (starting with the one already pulled), then
    # commit the advanced cursor. On a connector exception the transaction
    # rolls back the partial load (docs/05 §5.3) and the error propagates.
    with _stream_transaction(hooks, conn, stream_meta):
        if first_batch is not None:
            rows_extracted += len(first_batch)
            rows_loaded += hooks["write_batch"](conn, first_batch, stream_meta)
            for batch in batches:
                rows_extracted += len(batch)
                rows_loaded += hooks["write_batch"](conn, batch, stream_meta)

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
        # 5d — per-stream commit, inside the transaction: the data written
        # above and this cursor advance flip atomically (docs/02 §Commit
        # granularity, docs/05 §5.3).
        hooks["commit_state"](conn, run_config.run_id, [record])

    return StreamResult(
        name=stream_def.name,
        rows_extracted=rows_extracted,
        rows_loaded=rows_loaded,
        cursor_before=cursor_before,
        cursor_after=cursor_after,
        status=StreamStatus.SUCCEEDED,
    )


# ---------------------------------------------------------------------------
# The run loop — the public engine entry point
# ---------------------------------------------------------------------------


def run(
    connector: str,
    target: str | None = None,
    *,
    project_dir: str | Path | None = None,
    full_refresh: bool = False,
    select: tuple[str, ...] = (),
    params: Mapping[str, Any] | None = None,
    destination_params: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> RunResult:
    """Run one source connector end to end — the 6-stage lifecycle (docs/02).

    This is the engine. The CLI and the library both call it (it is re-exported
    as :func:`det.run`). It executes one synchronous pass:

    1. **DISCOVER** — find the project root (``project_dir`` or walk up for
       ``det_project.yml``); resolve the source connector and its bound
       destination, project-local beating baked (docs/03 §5).
    2. **RESOLVE** — merge every config layer into a frozen :class:`RunConfig`
       and immutable per-connector :class:`Config` objects (docs/03 §6).
    3. **INIT DEST** — bind the destination hooks, fix the capability tier via
       ``capabilities()``, ``open`` the connection.
    4. **LOAD STATE** — ``read_state`` the prior :class:`StateRecord` set,
       indexed by stream name.
    5. **RUN STREAMS** — for each selected stream in declared order: build its
       context, resolve its schema, ``ensure_schema``, drive the generator and
       ``write_batch`` each batch, then ``commit_state`` *that stream's* record
       immediately (per-stream commit — docs/02 §Commit granularity).
    6. **RUN RECORD** — build and return the :class:`RunResult`; ``close`` the
       destination in a ``finally``.

    Parameters:

    * ``connector`` — the source connector NAME to run.
    * ``target`` — the ``profiles.yml`` target; falls back to the project's
      ``default_target`` (docs/06).
    * ``project_dir`` — the project root, or a directory under it to walk up
      from; defaults to the current working directory.
    * ``full_refresh`` — when ``True``, incremental cursors ignore prior state
      and re-extract from the beginning (docs/03 §3.2).
    * ``select`` — run only this subset of streams (empty ⇒ all).
    * ``params`` / ``**kwargs`` — per-invocation source param overrides, the
      highest precedence layer (docs/03 §6); ``kwargs`` is the keyword-argument
      convenience form, merged under ``params``.
    * ``destination_params`` — per-invocation overrides for the *destination*
      connector's config (e.g. DuckDB's ``path``), highest precedence on the
      destination side.

    Never raises on a connector/destination failure: returns a ``RunResult``
    with ``status=FAILED`` and a populated ``error`` (docs/07 §4.1). Callers
    wanting an exception use ``run(...).raise_for_status()``.
    """
    run_id = f"run-{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(UTC)
    overrides: dict[str, Any] = {**kwargs, **(params or {})}
    dest_overrides: dict[str, Any] = dict(destination_params or {})

    # Defaults for the RunResult fields populated as the lifecycle advances —
    # so an early failure still yields a complete, well-formed FAILED result.
    connector_name = connector
    destination_name = "unknown"
    target_name = target or "default"
    streams: list[StreamResult] = []
    conn: Any = None
    hooks: dict[str, Callable[..., Any]] | None = None
    log = build_logger(run_id)

    try:
        # -- Stage 1: DISCOVER ----------------------------------------------
        project_root = disc.find_project_root(project_dir)
        project = cfg.ProjectConfig.load(project_root)
        profiles = cfg.Profiles.load(project_root)
        target_name = cfg.resolve_target_name(target, project, profiles)
        target_block = (
            profiles.target(target_name) if profiles.targets else {}
        )

        source = disc.resolve_connector(
            connector, project_root, list(project.connector_paths)
        )
        if source.manifest.kind.value != "source":
            raise EngineError(
                f"connector {connector!r} is a {source.manifest.kind.value}, not a "
                f"source — only sources can be run (docs/03 §2.1)"
            )
        destination_name = cfg.resolve_destination_name(source.manifest, project)
        dest = disc.resolve_connector(
            destination_name, project_root, list(project.connector_paths)
        )

        # -- Stage 2: RESOLVE -----------------------------------------------
        source_config = cfg.build_config(
            source.manifest,
            project,
            target_block,
            section="profiles",
            overrides=overrides,
        )
        # The destination's config also carries the source's `destination:`
        # binding routing params (docs/03 §2.3), under the per-invocation
        # destination_params and above profiles.yml.
        routing = dict(source.manifest.destination.routing) if source.manifest.destination else {}
        dest_config = cfg.build_config(
            dest.manifest,
            project,
            target_block,
            section="destinations",
            overrides={**routing, **dest_overrides},
        )

        run_config = RunConfig(
            run_id=run_id,
            connector=connector_name,
            target=target_name,
            config=source_config,
            select=tuple(select),
            full_refresh=full_refresh,
        )
        # Rebuild the logger now that secrets are resolved, so any value a
        # connector logs is redacted (docs/08).
        log = build_logger(run_id, source_config.secrets.values())

        # -- Stage 3: INIT DEST ---------------------------------------------
        hooks, _capabilities = _resolve_destination_hooks(dest)
        conn = hooks["open"](Config(params=dict(dest_config.params)))

        # -- Stage 4: LOAD STATE --------------------------------------------
        prior_records = hooks["read_state"](conn, source.manifest.name)
        state_by_stream: dict[str, StateRecord] = {
            r.stream: r for r in prior_records
        }

        # -- Stage 5: RUN STREAMS (sequential, declared order) --------------
        # A stream failure stops the run, but every stream that already
        # committed keeps its cursor (per-stream commit). The failing stream is
        # recorded FAILED so the run record localizes the failure; the original
        # exception is re-raised to the handler below, which builds the FAILED
        # RunResult carrying these partial per-stream results.
        stream_error: Exception | None = None
        for stream_def in source.manifest.streams:
            if not run_config.selects(stream_def.name):
                streams.append(
                    StreamResult(name=stream_def.name, status=StreamStatus.SKIPPED)
                )
                continue
            log.info("running stream %r", stream_def.name)
            try:
                result = _run_one_stream(
                    stream_def,
                    source,
                    hooks,
                    conn,
                    run_config,
                    state_by_stream.get(stream_def.name),
                    log,
                )
            except Exception as exc:  # noqa: BLE001 — recorded, then re-raised.
                # _run_one_stream commits this stream's state inside its own
                # per-stream transaction (rolled back on this failure). Earlier
                # streams already committed keep their progress (docs/02
                # §Commit granularity).
                streams.append(
                    StreamResult(name=stream_def.name, status=StreamStatus.FAILED)
                )
                stream_error = exc
                break
            streams.append(result)
            log.info(
                "stream %r loaded %d row(s)", stream_def.name, result.rows_loaded
            )

        if stream_error is not None:
            raise stream_error

        ended_at = datetime.now(UTC)
        total_rows = sum(s.rows_loaded for s in streams)
        return RunResult(
            run_id=run_id,
            connector=connector_name,
            target=target_name,
            destination=destination_name,
            status=RunStatus.SUCCEEDED,
            started_at=started_at,
            ended_at=ended_at,
            streams=streams,
            rows_loaded=total_rows,
            full_refresh=full_refresh,
        )

    except Exception as exc:  # noqa: BLE001 — run() never raises; see docstring.
        # The stream that failed (if any) is recorded FAILED so the run record
        # localizes the failure; streams that already committed keep their
        # progress (per-stream commit) and a re-run resumes from there.
        log.error("run failed: %s: %s", type(exc).__name__, exc)
        return RunResult(
            run_id=run_id,
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
        )
    finally:
        # close — always runs, even on failure, but only if open() succeeded
        # (docs/05 §1). A None conn means open never returned a handle.
        if conn is not None and hooks is not None:
            hooks["close"](conn)
