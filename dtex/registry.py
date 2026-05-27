# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""dtex connector registry — the decorator + registration layer.

This module (``dtex/registry.py``) is the **author-facing decorator API**
and the **engine-facing read interface** for a connector's entry points. It is
the layer between ``register.yaml`` (declaration, parsed into ``types.py``
objects) and the run loop (the engine, a later build stage).

Doc references: ``docs/03-connector-contract.md`` §3 (the decorator API) and §4
(the class escape hatch); ``docs/05-destinations-and-state.md`` §1 (the
``@destination`` hooks). Where the handbook left a micro-detail ambiguous the
choice is tagged with a ``# NOTE:`` comment, per ``CONTRIBUTING.md``.


Registry design — the key decision
-----------------------------------

A connector is a *folder* of plain Python (docs/03 §1): ``source.py`` plus
optional ``streams.py`` / ``schema.py`` / ``client.py``. The engine imports
those files *as a unit* and must then answer, for that one connector:

* "give me the ``@stream`` function for stream X",
* "give me the ``@destination.write_batch`` hook",
* "is this a source or a destination module".

Two files in one folder are two separate Python modules, so a registry keyed by
``__module__`` would split one connector across several keys. And two different
connectors can each define ``@stream(name="orders")`` — a process-global
flat registry would collide them.

The design here is a **scoped active-connector context** combined with
**metadata attached directly to each decorated function**:

* The engine calls :func:`registration_scope` (a context manager) around the
  import of one connector folder. While that scope is open, *every*
  ``@stream`` / ``@destination.*`` / ``@stream_method`` registration — from
  every ``.py`` file in the folder — lands in the *same* :class:`ConnectorRegistry`.
  Entering the scope returns a fresh registry; leaving it deactivates it. This
  gives natural cross-file collection (source.py + streams.py both contribute)
  and natural isolation (two connectors never share a registry, so two
  identically named streams in *different* connectors never collide).
* Each decorator *also* stamps its metadata onto the wrapped function object
  (``__dtex_*__`` attributes). The function therefore *self-describes*: a
  connector author's own unit test can ``import source; source.shipments(...)``
  with **no scope open at all** — the decorator returns the function unchanged
  (``functools.wraps`` metadata intact, directly callable) and registration is
  simply skipped. "Decorators must preserve callability" (the task's quality
  bar) falls out for free.

Why this combo over the alternatives:

* A process-global dict keyed by module name cannot express "these N modules
  are one connector" and offers no collision boundary between connectors.
* Harvesting ``__dtex_*__`` attributes by walking every module after import
  works, but forces the engine to re-discover *which* modules belong together
  and to re-scan namespaces; the scope already knows the unit boundary at
  import time, which is exactly when collisions (duplicate stream name, bad
  hook) should be reported.

So: the **scope** owns "*which connector* a registration belongs to" (a
contextual fact); the **function attributes** own "*what* this function is" (an
intrinsic fact). The two concerns are deliberately not entangled.

The scope is held in a :class:`contextvars.ContextVar`, so it is correct under
``asyncio`` and isolated per thread/task. Outside any scope the
"active registry" is ``None`` and registration is a clean no-op.
"""

from __future__ import annotations

import contextvars
import functools
import inspect
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, ClassVar

from dtex.types import ConnectorKind

__all__ = [
    "Connector",
    "ConnectorRegistry",
    "DESTINATION_HOOKS",
    "DestinationHook",
    "MANDATORY_DESTINATION_HOOKS",
    "STREAM_INJECTABLES",
    "StreamRegistration",
    "active_registry",
    "compute_injection",
    "destination",
    "registration_scope",
    "resource",
    "stream",
    "stream_method",
]


# ---------------------------------------------------------------------------
# Injection — the parameter names the engine may inject (docs/03 §3.1)
# ---------------------------------------------------------------------------

STREAM_INJECTABLES: frozenset[str] = frozenset({"config", "state", "cursor", "log"})
"""The parameter names the engine injects into a ``@stream`` function — docs/03 §3.1.

The engine inspects the function signature and injects, *by name*, only the
objects the function declares. A ``@stream`` function may declare any subset of
these four; declaring anything else is a discovery-time error (docs/03 §7 rule 8).
"""

# NOTE: docs/03 §4 says a ``@stream_method`` follows "the same injection rules
# as ``@stream``, minus ``config`` (use ``self.config``)". So a bound stream
# method draws from {state, cursor, log}; ``self`` is the bound instance, never
# an injectable name.
STREAM_METHOD_INJECTABLES: frozenset[str] = STREAM_INJECTABLES - {"config"}
"""Injectable parameter names for a ``@stream_method`` — ``@stream`` minus ``config``."""


# Function-attribute names used to stamp metadata onto decorated functions.
# Dunder-style so they never collide with an author's own attributes and are
# hidden from ordinary ``dir()`` browsing.
_ATTR_KIND = "__dtex_kind__"
_ATTR_STREAM_NAME = "__dtex_stream_name__"
_ATTR_INJECT = "__dtex_inject__"
_ATTR_HOOK = "__dtex_hook__"
_ATTR_IS_METHOD = "__dtex_is_method__"


# ---------------------------------------------------------------------------
# The active-connector scope
# ---------------------------------------------------------------------------

# The registry that decorators populate while a connector folder is being
# imported. ``None`` outside any scope — registration is then a no-op so the
# decorated function is still importable/callable for unit testing.
_active: contextvars.ContextVar[ConnectorRegistry | None] = contextvars.ContextVar(
    "det_active_registry", default=None
)


def active_registry() -> ConnectorRegistry | None:
    """Return the :class:`ConnectorRegistry` for the connector being imported.

    ``None`` when no :func:`registration_scope` is open — the state a connector
    author's standalone unit test runs in. The engine reads this only
    indirectly, via :func:`registration_scope`.
    """
    return _active.get()


@contextmanager
def registration_scope(connector_name: str) -> Iterator[ConnectorRegistry]:
    """Open a registration scope for one connector folder — docs/03 §1.

    The engine wraps the import of a connector's ``.py`` files in this context
    manager. Every ``@stream`` / ``@resource`` / ``@destination.*`` /
    ``@stream_method`` decorator that runs while the scope is open registers
    into the single :class:`ConnectorRegistry` yielded here — so the multiple
    files of one connector folder (``source.py``, ``streams.py``, …) all
    contribute to one registry, and two *different* connectors can never share
    one.

    # NOTE: each call yields a *fresh* registry. The handbook's discovery model
    # (docs/03 §5) is "two filesystem lookups", connector-at-a-time; there is no
    # documented re-import of one connector within a process. A fresh registry
    # per scope therefore keeps the duplicate-detection semantics simple — a
    # duplicate is always a real authoring bug, never a stale-import artifact.
    """
    registry = ConnectorRegistry(connector=connector_name)
    token = _active.set(registry)
    try:
        yield registry
    finally:
        _active.reset(token)


def _register_into_active(register: Callable[[ConnectorRegistry], None]) -> None:
    """Apply ``register`` to the active registry, or no-op outside a scope.

    The single point where a decorator hands its registration to the registry.
    Outside a :func:`registration_scope` (a standalone unit test) the call is
    skipped — the decorated function is returned and remains callable, it is
    just not recorded anywhere.
    """
    registry = _active.get()
    if registry is not None:
        register(registry)


# ---------------------------------------------------------------------------
# Signature introspection
# ---------------------------------------------------------------------------


def _injectable_params(func: Callable[..., Any], *, skip_self: bool) -> tuple[str, ...]:
    """Return the *injectable* parameter names a function declares, in order.

    Used at decoration time to record what the engine must inject. ``skip_self``
    drops the leading ``self`` of a ``@stream_method``. Only positional-or-keyword
    and keyword-only parameters are considered injectables; ``*args`` / ``**kwargs``
    are not names the engine can inject by-name and are reported as an error by
    :func:`_validate_injectables`.
    """
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    if skip_self and params and params[0].name == "self":
        params = params[1:]
    names: list[str] = []
    for p in params:
        # *args / **kwargs are recorded with their sigil so _validate_injectables
        # can reject them with a clear message — they are not by-name injectables.
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            names.append(f"*{p.name}")
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            names.append(f"**{p.name}")
        else:
            names.append(p.name)
    return tuple(names)


def _validate_injectables(
    func: Callable[..., Any],
    declared: tuple[str, ...],
    allowed: frozenset[str],
    *,
    what: str,
) -> None:
    """Reject any declared parameter that is not an engine injectable — docs/03 §7 rule 8.

    Raised at decoration time (import time for the connector module) so a typo
    like ``def shipments(config, cusror): ...`` fails fast with a message that
    names the offending parameter — never silently mis-injected at run time.
    """
    unknown = [p for p in declared if p not in allowed]
    if unknown:
        valid = ", ".join(sorted(allowed))
        raise TypeError(
            f"{what} {getattr(func, '__name__', func)!r} declares "
            f"parameter(s) {', '.join(repr(p) for p in unknown)} that the engine "
            f"cannot inject; valid injectable parameters are: {valid}"
        )


def compute_injection(
    func: Callable[..., Any], available: Mapping[str, Any]
) -> dict[str, Any]:
    """Compute the kwargs to pass a decorated function — docs/03 §3.1.

    The engine injects ``config`` / ``state`` / ``cursor`` / ``log`` into a
    ``@stream`` function **by name**, supplying only the parameters the function
    actually declares (docs/03 §3.1: "injects, by name, only the objects the
    function asks for"). This helper is that selection step, and the engine
    (a later stage) calls it for every stream invocation.

    ``func`` must be a function decorated by :func:`stream`, :func:`resource` or
    :func:`stream_method` — the decorator stamped the declared injectable list
    onto it. ``available`` is the dict of injectables the engine has on hand for
    this call (e.g. ``{"config": ..., "state": ..., "cursor": ..., "log": ...}``;
    ``cursor`` is absent for a non-incremental stream).

    Returns the subset of ``available`` keyed by exactly the parameters ``func``
    declares. An injectable the function declares but ``available`` does not
    provide raises :class:`KeyError` with a clear message — the engine must
    supply everything a declared parameter needs (e.g. a ``cursor`` parameter on
    a non-incremental stream is itself a discovery-time error caught elsewhere).

    Parameter names were already validated against the injectable set at
    decoration time (docs/03 §7 rule 8), so this helper never silently injects
    something unrecognized.
    """
    declared: tuple[str, ...] | None = getattr(func, _ATTR_INJECT, None)
    if declared is None:
        raise TypeError(
            f"{getattr(func, '__name__', func)!r} is not a dtex stream function; "
            f"decorate it with @stream / @resource / @stream_method first"
        )
    kwargs: dict[str, Any] = {}
    for name in declared:
        if name not in available:
            offered = ", ".join(sorted(available)) or "(none)"
            raise KeyError(
                f"stream function {getattr(func, '__name__', func)!r} declares "
                f"parameter {name!r} but the engine offered no such injectable; "
                f"available: {offered}"
            )
        kwargs[name] = available[name]
    return kwargs


# ---------------------------------------------------------------------------
# Registration records — the engine's read interface
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamRegistration:
    """One registered source stream — a ``@stream`` / ``@resource`` / ``@stream_method``.

    The engine reads this to bind a ``streams[].name`` from ``register.yaml``
    (a :class:`~dtex.types.StreamDef`) to its implementing function
    (docs/03 §7 rule 7, decorator coverage).
    """

    name: str
    """The stream name — matches a ``streams[].name`` in ``register.yaml``."""
    func: Callable[..., Any]
    """The decorated generator. Directly callable; ``functools.wraps`` metadata intact."""
    inject: tuple[str, ...]
    """The injectable parameter names the function declares, in signature order."""
    is_method: bool = False
    """``True`` for a ``@stream_method`` — the engine binds it to a :class:`Connector` instance."""


@dataclass(frozen=True)
class DestinationHook:
    """One registered ``@destination.*`` hook — docs/03 §3.4, docs/05 §1.

    The engine reads this to drive the destination lifecycle
    (``open → read_state → [ensure_schema → write_batch ...]* → commit_state → close``).
    """

    hook: str
    """The hook name — one of :data:`DESTINATION_HOOKS`."""
    func: Callable[..., Any]
    """The decorated hook function. Directly callable; ``functools.wraps`` metadata intact."""


# The complete, fixed set of @destination hook names — docs/03 §3.4 table +
# docs/05 §1. Any attribute access on ``destination`` outside this set is a
# typo and raises at import time.
DESTINATION_HOOKS: frozenset[str] = frozenset(
    {
        "capabilities",
        "open",
        "ensure_schema",
        "write_batch",
        "commit_state",
        "read_state",
        "state_backend",
        "transaction",
        "write_run_record",
        "max_concurrent_writes",
        "close",
    }
)
"""Every valid ``@destination.*`` hook name — docs/03 §3.4, docs/05 §1.

Eleven hooks. ``@destination.<anything-else>`` (e.g. a ``write_batchs`` typo)
raises :class:`AttributeError` at import time.

``transaction`` is a *conditionally* mandatory hook: a destination that
declares ``Capability.TRANSACTIONAL_LOAD`` must define it (a context-manager
hook the engine wraps around each stream's ``[ensure_schema → write_batch… →
commit_state]`` block, so data and cursor flip atomically per stream).

``write_run_record`` is a *conditionally* mandatory hook (docs/09 §4, stage
8a): a destination that declares ``Capability.RUN_RECORDS`` must define it.
The engine calls it once per run, after streams finish and before ``close``,
with a fully-built :class:`~dtex.types.RunRecord`. It is the destination's
half of the run-record audit table (``_dtex_runs``); the per-run JSONL log
file is the engine's half and is written regardless of capability.

``max_concurrent_writes`` is an *optional* hook (stage 8e): when present,
the engine reads it (with the resolved destination :class:`~dtex.types.Config`)
and clamps the number of pipelines that may concurrently target this
destination under ``dtex run --tag … --threads N``. Returning ``1`` (DuckDB)
forces serial execution against this destination, however high the project
``threads:`` is set; returning a larger number (BigQuery: 10) sets the
per-destination ceiling. Absent ⇒ unlimited (``sys.maxsize`` — no clamp).
The hook is NOT tied to a :class:`~dtex.types.Capability` flag — every
destination is free to declare it, and the absence is its own opt-in for
"I don't care about per-destination concurrency". Same precedent as
``transaction`` / ``write_run_record`` (conditional hooks the engine reads
only when relevant).
"""

# NOTE: docs/03 §3.4 / docs/05 §1 mark capabilities/open/ensure_schema/
# write_batch/close as always-mandatory; commit_state/read_state are mandatory
# only with Capability.STATE and state_backend only without it. That
# conditional check needs the parsed capabilities() result, which is engine
# work (a later stage). The registry records the unconditionally-mandatory set
# here so the engine can build the full rule on top of it.
MANDATORY_DESTINATION_HOOKS: frozenset[str] = frozenset(
    {"capabilities", "open", "ensure_schema", "write_batch", "close"}
)
"""The ``@destination`` hooks every destination must define — docs/03 §3.4.

``commit_state`` / ``read_state`` / ``state_backend`` are *conditionally*
mandatory (they depend on ``Capability.STATE``); that rule is the engine's to
apply since it needs the parsed ``capabilities()`` result.
"""


@dataclass
class ConnectorRegistry:
    """The per-connector registry the engine queries — docs/03 §3, §7.

    Populated by the decorators while a :func:`registration_scope` is open, then
    read by the engine. One registry == one connector folder; this is the unit
    boundary that keeps two connectors' registrations from colliding.

    A registry holds *either* ``@stream`` registrations (a source) *or*
    ``@destination`` hooks (a destination) — never both. :attr:`kind` reports
    which, or ``None`` while still empty.
    """

    connector: str
    """The connector ``name`` this registry belongs to."""
    streams: dict[str, StreamRegistration] = field(default_factory=dict)
    """Registered source streams, keyed by stream name."""
    hooks: dict[str, DestinationHook] = field(default_factory=dict)
    """Registered destination hooks, keyed by hook name."""
    connector_classes: list[type[Connector]] = field(default_factory=list)
    """:class:`Connector` subclasses defined in this connector (the §4 escape hatch)."""

    @property
    def kind(self) -> ConnectorKind | None:
        """Whether this connector is a source or a destination — docs/03 §2.1.

        ``SOURCE`` once any ``@stream`` is registered, ``DESTINATION`` once any
        ``@destination`` hook is. ``None`` while the registry is still empty.
        A connector that registered both is rejected at registration time, so
        this never has to arbitrate a conflict.
        """
        if self.streams:
            return ConnectorKind.SOURCE
        if self.hooks:
            return ConnectorKind.DESTINATION
        return None

    def _guard_kind(self, registering: ConnectorKind) -> None:
        """Reject mixing ``@stream`` and ``@destination`` in one connector — docs/03 §2.1.

        docs/03 §2.1: a connector's ``kind`` is fixed — it either reads from the
        world (sources, ``@stream``) or writes to it (destinations,
        ``@destination``), never both.
        """
        current = self.kind
        if current is not None and current is not registering:
            raise TypeError(
                f"connector {self.connector!r} registers both @stream functions "
                f"and @destination hooks; a connector is a source or a "
                f"destination, not both (docs/03 §2.1)"
            )

    def add_stream(self, reg: StreamRegistration) -> None:
        """Record a source stream; reject a duplicate stream name — docs/03 §7 rule 7.

        Two ``@stream`` decorators sharing one ``name`` within a connector is an
        authoring bug; it raises here at import time, not silently last-wins.
        """
        self._guard_kind(ConnectorKind.SOURCE)
        if reg.name in self.streams:
            raise ValueError(
                f"connector {self.connector!r}: stream {reg.name!r} is registered "
                f"twice; each stream name must have exactly one @stream/@resource/"
                f"@stream_method (docs/03 §7)"
            )
        self.streams[reg.name] = reg

    def add_hook(self, hook: DestinationHook) -> None:
        """Record a destination hook; reject a duplicate hook — docs/03 §3.4.

        Defining e.g. ``@destination.write_batch`` twice in one connector raises
        here — the engine must have exactly one function per lifecycle step.
        """
        self._guard_kind(ConnectorKind.DESTINATION)
        if hook.hook in self.hooks:
            raise ValueError(
                f"connector {self.connector!r}: @destination.{hook.hook} is "
                f"registered twice; each destination hook may be defined only once"
            )
        self.hooks[hook.hook] = hook

    def add_connector_class(self, cls: type[Connector]) -> None:
        """Record a :class:`Connector` subclass — the docs/03 §4 escape hatch.

        ``@stream_method``-decorated methods on ``cls`` are added to
        :attr:`streams` as they are discovered; this list keeps the class
        itself so the engine can instantiate it and run ``setup``/``teardown``.
        """
        self.connector_classes.append(cls)

    # -- engine read interface ------------------------------------------

    def stream(self, name: str) -> StreamRegistration | None:
        """Look up the registration for a stream by name; ``None`` if absent."""
        return self.streams.get(name)

    def hook(self, name: str) -> DestinationHook | None:
        """Look up a destination hook by name; ``None`` if absent."""
        return self.hooks.get(name)

    @property
    def stream_names(self) -> tuple[str, ...]:
        """The registered stream names — for docs/03 §7 rule 7 coverage checks."""
        return tuple(self.streams)

    @property
    def hook_names(self) -> tuple[str, ...]:
        """The registered destination hook names."""
        return tuple(self.hooks)

    def missing_mandatory_hooks(self) -> tuple[str, ...]:
        """The unconditionally-mandatory ``@destination`` hooks not yet defined.

        docs/03 §3.4: ``capabilities`` / ``open`` / ``ensure_schema`` /
        ``write_batch`` / ``close`` are always required. The conditional hooks
        (``commit_state`` etc.) are the engine's to check since they depend on
        the parsed ``Capability`` set.
        """
        return tuple(sorted(MANDATORY_DESTINATION_HOOKS - set(self.hooks)))


# ---------------------------------------------------------------------------
# @stream / @resource — the default source decorator (docs/03 §3.1, §3.3)
# ---------------------------------------------------------------------------


def stream(name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a generator function as a source stream — docs/03 §3.1.

    ``@stream(name="shipments")`` records the decorated generator as the
    implementation of the ``streams[].name`` ``"shipments"`` declared in
    ``register.yaml`` — ``name`` is the manifest↔code link (docs/03 §3.1) and
    is **required**.

    The decorator:

    * introspects the signature and records the *injectable* parameters the
      function declares (``config`` / ``state`` / ``cursor`` / ``log`` — see
      :data:`STREAM_INJECTABLES`), rejecting any other parameter name at import
      time (docs/03 §7 rule 8);
    * registers the function into the active :class:`ConnectorRegistry` if a
      :func:`registration_scope` is open (the engine's import path), and is a
      clean no-op otherwise (a standalone unit test);
    * returns the function **unchanged and directly callable**, with
      ``functools.wraps`` metadata preserved, so an author can unit-test the
      stream body in isolation.

    Raises :class:`TypeError` if ``name`` is not a non-empty string — which also
    catches a bare ``@stream`` (no parentheses): there the function itself is
    passed where ``name`` is expected, and the type check rejects it with a
    clear message.
    """
    if not isinstance(name, str) or not name:
        raise TypeError(
            "@stream requires a non-empty string 'name' matching a streams[].name "
            "in register.yaml — write @stream(name=\"...\"), not a bare @stream"
        )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not callable(func):
            raise TypeError(f"@stream(name={name!r}) must decorate a callable")

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        inject = _injectable_params(func, skip_self=False)
        _validate_injectables(func, inject, STREAM_INJECTABLES, what="@stream function")

        # Stamp intrinsic metadata onto the function so it self-describes even
        # with no registration scope open (docs/03 §3.1 unit-testing promise).
        wrapper.__dtex_kind__ = ConnectorKind.SOURCE  # type: ignore[attr-defined]
        wrapper.__dtex_stream_name__ = name  # type: ignore[attr-defined]
        wrapper.__dtex_inject__ = inject  # type: ignore[attr-defined]
        wrapper.__dtex_is_method__ = False  # type: ignore[attr-defined]

        _register_into_active(
            lambda reg: reg.add_stream(
                StreamRegistration(name=name, func=wrapper, inject=inject, is_method=False)
            )
        )
        return wrapper

    return decorator


# NOTE: docs/03 §3.3 — ``@resource`` is a *registered alias* of ``@stream``,
# "identical in every respect", provided for dlt familiarity. The strongest
# implementation of "identical" is literal identity: one function, two names.
# Anything else risks the two drifting apart.
resource = stream
"""``@resource`` — a registered alias of :func:`stream` — docs/03 §3.3.

Identical to ``@stream`` in every respect; provided so authors arriving from
dlt feel at home. One implementation, exposed under both names — they cannot
drift apart. The handbook, examples and scaffolding all use ``@stream``.
"""


# ---------------------------------------------------------------------------
# @destination — the destination hook namespace (docs/03 §3.4, docs/05 §1)
# ---------------------------------------------------------------------------


class _DestinationNamespace:
    """The ``destination`` object — a namespace of ``@destination.*`` hook decorators.

    docs/03 §3.4: a destination has several jobs (open a connection, manage
    tables, persist batches, hold state, close down), so it implements a small
    *namespace* of hooks rather than one function. ``destination`` is the single
    instance of this class exported as :data:`destination`; each attribute
    (``destination.open``, ``destination.write_batch``, …) is the decorator that
    registers a function under that hook name.

    An attribute that is not a known hook (a typo like ``destination.write_batchs``)
    raises :class:`AttributeError` *at import time* — the moment the connector
    module is loaded — instead of silently doing nothing. The valid set is
    :data:`DESTINATION_HOOKS`.
    """

    # NOTE: the hook set is checked explicitly against DESTINATION_HOOKS rather
    # than left to attribute-presence magic, so a typo fails loudly and the
    # error message can list every valid hook.
    _hooks: ClassVar[frozenset[str]] = DESTINATION_HOOKS

    def __getattr__(self, hook: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Return the registering decorator for a ``@destination.<hook>`` hook.

        ``__getattr__`` runs only for attributes not found normally, so the real
        methods/dunders of this class are never shadowed. An unknown ``hook``
        raises :class:`AttributeError` listing the valid hooks — this is the
        import-time guard against ``@destination.<typo>``.
        """
        # Dunder lookups (pickling, copy, ...) must raise normally, not get
        # mistaken for a hook name.
        if hook.startswith("__") and hook.endswith("__"):
            raise AttributeError(hook)
        if hook not in self._hooks:
            valid = ", ".join(sorted(self._hooks))
            raise AttributeError(
                f"@destination.{hook} is not a valid destination hook; "
                f"valid hooks are: {valid}"
            )
        return _make_hook_decorator(hook)

    def __repr__(self) -> str:
        """Developer-readable representation of the hook namespace."""
        return f"<dtex.destination namespace: {', '.join(sorted(self._hooks))}>"


def _make_hook_decorator(
    hook: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Build the decorator that registers a function under destination hook ``hook``.

    ``hook`` is already validated to be in :data:`DESTINATION_HOOKS` by the
    caller (:meth:`_DestinationNamespace.__getattr__`).
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not callable(func):
            raise TypeError(f"@destination.{hook} must decorate a callable")

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        # NOTE: destination hooks are not parameter-injected by name the way
        # @stream functions are — docs/03 §3.4 fixes each hook's exact
        # positional signature (``open(config)``, ``write_batch(conn, table,
        # batch, disposition)``, …). The engine calls them positionally, so the
        # registry records the function without an injectable-name list. We do
        # not over-validate hook signatures here: a destination author may add
        # a typed return annotation or default and that must not be rejected.
        wrapper.__dtex_kind__ = ConnectorKind.DESTINATION  # type: ignore[attr-defined]
        wrapper.__dtex_hook__ = hook  # type: ignore[attr-defined]

        _register_into_active(
            lambda reg: reg.add_hook(DestinationHook(hook=hook, func=wrapper))
        )
        return wrapper

    return decorator


destination = _DestinationNamespace()
"""``@destination`` — the destination hook namespace — docs/03 §3.4, docs/05 §1.

Not a single decorator: authors write ``@destination.open``,
``@destination.write_batch``, ``@destination.commit_state``, etc. Each attribute
is the decorator registering its function under that hook name. An unknown hook
name raises :class:`AttributeError` at import time. See :data:`DESTINATION_HOOKS`.
"""


# ---------------------------------------------------------------------------
# Connector + @stream_method — the class-based escape hatch (docs/03 §4)
# ---------------------------------------------------------------------------


def stream_method(
    name: str,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Mark a :class:`Connector` instance method as a source stream — docs/03 §4.

    The class-escape-hatch counterpart of :func:`stream`. Same ``name``
    semantics (must match a ``streams[].name`` in ``register.yaml``) and the
    same injection rules **minus** ``config`` — a method reads ``self.config``
    instead, so the injectable set is ``{state, cursor, log}`` (see
    :data:`STREAM_METHOD_INJECTABLES`). The leading ``self`` is the bound
    instance and is never an injectable name.

    Registration of the method happens when its enclosing :class:`Connector`
    subclass is *defined* (``Connector.__init_subclass__`` collects every
    ``@stream_method`` on the class), so a method registers into the same
    :func:`registration_scope` as the class. The decorated method stays a normal
    callable.
    """
    if not isinstance(name, str) or not name:
        raise TypeError(
            "@stream_method requires a non-empty string 'name' matching a "
            "streams[].name in register.yaml — write @stream_method(name=\"...\")"
        )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        if not callable(func):
            raise TypeError(f"@stream_method(name={name!r}) must decorate a callable")

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        inject = _injectable_params(func, skip_self=True)
        _validate_injectables(
            func, inject, STREAM_METHOD_INJECTABLES, what="@stream_method"
        )

        wrapper.__dtex_kind__ = ConnectorKind.SOURCE  # type: ignore[attr-defined]
        wrapper.__dtex_stream_name__ = name  # type: ignore[attr-defined]
        wrapper.__dtex_inject__ = inject  # type: ignore[attr-defined]
        wrapper.__dtex_is_method__ = True  # type: ignore[attr-defined]

        # NOTE: a @stream_method is *not* registered here. At decoration time
        # the enclosing class does not exist yet, so there is no instance to
        # bind to. Registration is deferred to Connector.__init_subclass__,
        # which scans the finished class body for these marked methods. This
        # also makes "@stream_method used outside a Connector subclass" a
        # detectable no-op rather than a half-registration.
        return wrapper

    return decorator


class Connector:
    """The class-based escape hatch for complex stateful sources — docs/03 §4.

    The decorator style (:func:`stream`) covers the overwhelming majority of
    connectors and is the documented default. Subclass ``Connector`` **only**
    when a genuinely shared lifecycle — a pooled auth/session, an SDK that must
    be opened and closed, cross-stream ordering — cannot be expressed cleanly
    per function (docs/03 §4: "If you are not writing ``setup()``/``teardown()``
    you do not need the class").

    Members:

    * :attr:`config` — the resolved :class:`~dtex.types.Config`, the same
      object the decorators inject. Set by the engine before :meth:`setup`.
    * :meth:`setup` — optional; runs once before any stream. Open shared
      resources here.
    * :meth:`teardown` — optional; runs once after all streams, *including on
      failure*. Release shared resources here.
    * ``@stream_method(name=...)`` — marks an instance method as a stream.

    When a subclass is *defined*, :meth:`__init_subclass__` scans it for
    ``@stream_method``-decorated methods and registers each — plus the class
    itself — into the active :class:`ConnectorRegistry`.
    """

    config: Any
    """The resolved :class:`~dtex.types.Config`. Assigned by the engine before :meth:`setup`."""

    def setup(self) -> None:
        """Run once before any stream — docs/03 §4. Override to open shared resources.

        The base implementation does nothing; a subclass without shared state
        need not override it.
        """

    def teardown(self) -> None:
        """Run once after all streams, *including on failure* — docs/03 §4.

        Override to release shared resources. The base implementation does
        nothing.
        """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Register a ``Connector`` subclass and its ``@stream_method`` streams — docs/03 §4.

        Runs when the subclass is *defined* — i.e. while the connector module is
        being imported, inside the engine's :func:`registration_scope`. It walks
        the class's own attributes for methods carrying the ``@stream_method``
        marker and adds a :class:`StreamRegistration` for each, then records the
        class itself so the engine can instantiate it and call
        ``setup``/``teardown``.

        Outside a scope (a standalone unit test that defines a ``Connector``
        subclass) this is a clean no-op — the class and its methods stay fully
        usable.
        """
        super().__init_subclass__(**kwargs)

        def register(reg: ConnectorRegistry) -> None:
            reg.add_connector_class(cls)
            # Walk the full MRO so a @stream_method on a Connector base class is
            # picked up, but record each stream name once (a subclass override
            # of a marked method wins — vars(cls) is consulted first).
            seen: set[str] = set()
            for klass in cls.__mro__:
                if klass is Connector or klass is object:
                    continue
                for attr_name, attr in vars(klass).items():
                    if attr_name in seen:
                        continue
                    stream_name = getattr(attr, _ATTR_STREAM_NAME, None)
                    is_method = getattr(attr, _ATTR_IS_METHOD, False)
                    if stream_name is None or not is_method:
                        continue
                    seen.add(attr_name)
                    inject: tuple[str, ...] = getattr(attr, _ATTR_INJECT, ())
                    reg.add_stream(
                        StreamRegistration(
                            name=stream_name,
                            func=attr,
                            inject=inject,
                            is_method=True,
                        )
                    )

        _register_into_active(register)
