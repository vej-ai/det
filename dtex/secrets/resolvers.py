# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""``SecretResolver`` Protocol + ``secret://`` URL parser + plugin registry.

The concrete implementation behind :mod:`dtex.secrets`. Three things live here:

1. The :class:`SecretResolver` Protocol every plugin implements.
2. The module-level resolver registry (factories registered lazily, instances
   constructed on first use, cached for the rest of the process).
3. The URL parser (:func:`resolve_secret_url`) the config layer calls when it
   sees a value beginning with ``secret://``.

URL grammar (docs/08 §3 — implemented exactly as documented)::

    secret://<scheme>/<path>[#<field>]

* ``scheme`` — the resolver's registered name, lowercase, ``[a-z0-9-]+`` only.
  Case-normalized to lowercase at parse time so ``secret://GCP/...`` and
  ``secret://gcp/...`` route to the same resolver.
* ``path`` — opaque to dtex, handed to the resolver verbatim. Required to be
  non-empty (a ``secret://gcp/`` with no path is a parse error). Slashes inside
  the path are preserved — the resolver's contract is "you parse what's after
  ``<scheme>/``".
* ``field`` — optional ``#<field>`` suffix for resolvers that return structured
  payloads (Vault returns a JSON blob; ``#token`` picks one key). Dots are
  preserved inside the field so ``#deep.nested.field`` round-trips.

Plugin discovery — TWO surfaces, both honored:

* **Entry-points** — third-party packages register a resolver factory under the
  ``dtex.secret_resolvers`` group. Loaded once on first
  :func:`resolve_secret_url` call (lazy — a project that uses zero
  ``secret://`` refs never pays the import cost).
* **Project-local ``dtex_plugins.py``** — a Python file at the project root
  (sibling of ``dtex_project.yml``). Imported once per project, at startup,
  via :func:`load_project_plugins`. The file calls
  :func:`register_secret_resolver` to add per-project resolvers.

Resolution precedence (locked decision — project-local always wins):

1. Project-local ``dtex_plugins.py`` registrations.
2. Entry-points discovery (only consulted for schemes the project did not
   already register).

# NOTE: project-local wins because explicit beats implicit — a developer
# writing ``register_secret_resolver("gcp", ...)`` in ``dtex_plugins.py`` has
# made a deliberate decision, while an entry-point can be activated by an
# unrelated ``pip install`` in the environment. Same precedence as docs/03 §5
# ("project-local connectors shadow same-named baked ones").

Resolver lifecycle (lazy, fresh-every-run):

* The registry holds factories (callables returning a fresh
  :class:`SecretResolver`).
* The first :func:`resolve_secret_url` for a given scheme calls its factory
  and caches the instance for the rest of the run; subsequent references on
  the same scheme reuse the instance.
* "Fresh-every-run" (docs/08 Q11) is the per-process commitment: no on-disk
  cache. A new ``dtex run`` invocation is a new process; it pays the resolver
  init cost once per scheme that ever runs.

# NOTE: caching at the *instance* layer (not the *value* layer) is the
# minimum that keeps GCP SDK init from re-running for every reference in a
# run; values are not cached, so the resolver still talks to the manager
# per resolve() call. Per-run *value* caching is a v2 question — see
# docs/11-open-questions.md Q11.

Error model: every failure path raises :class:`SecretResolutionError`, the
single new exception type stage 9a introduces. The original cause (an
``ImportError`` from a misregistered entry-point, a network error from a
resolver's ``.resolve``) is chained via ``raise ... from exc`` so the
traceback survives without the engine having to special-case per-source
exception classes.

# NOTE: error messages NEVER include resolved values or path components that
# could embed credentials. The reference URL is safe to print (it's what was
# in profiles.yml). The resolved value is not — a leaked error like
# "resolver gcp returned 'sk_live_xyz'" would defeat the entire point.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import re
import sys
import threading
from pathlib import Path
from typing import Any, ClassVar, Protocol, runtime_checkable
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# The Protocol — what every resolver implements (docs/08 §3 verbatim)
# ---------------------------------------------------------------------------


@runtime_checkable
class SecretResolver(Protocol):
    """A pluggable secret-manager adapter — docs/08 §3.

    A resolver knows how to turn a ``(<path>, <field>)`` pair into a string
    value (the credential). The engine's plugin layer dispatches a
    ``secret://<scheme>/<path>[#<field>]`` URL to the resolver whose
    :attr:`scheme` matches.

    Implementations:

    * Set :attr:`scheme` to a lowercase ``[a-z0-9-]+`` string — the URL token
      the engine matches against. The class attribute is the same string the
      factory passes to :func:`register_secret_resolver`; both are kept in
      sync by convention, not by the engine (the engine routes by the
      registration name, never re-reading :attr:`scheme`).
    * Implement :meth:`resolve` synchronously. The ``path`` argument is the
      URL's path component verbatim (with leading slashes stripped); the
      ``field`` argument is the URL's ``#fragment`` (without the ``#``), or
      ``None`` if absent.
    * On failure, raise any exception you like; the engine wraps it in
      :class:`SecretResolutionError`. Never embed the resolved value in your
      own exception text — the engine's redactor catches values that DO
      surface, but staying secret-free at the source is the strongest
      contract.

    # NOTE: ``runtime_checkable`` so a test can ``isinstance(x, SecretResolver)``
    # — useful for fake resolvers; production code never type-checks Protocol
    # membership (it routes by scheme name).

    # NOTE: ``scheme`` is typed :class:`typing.ClassVar` because it's a true
    # class constant — the same scheme value across every instance of a
    # resolver class. mypy treats ``ClassVar`` correctly here; non-ClassVar
    # would require the implementer to declare ``scheme`` as an instance
    # attribute and would be more awkward to satisfy with a plain class.
    """

    scheme: ClassVar[str]

    def resolve(self, path: str, field: str | None) -> str:
        """Resolve one reference. ``path`` is the URL path; ``field`` is the
        optional ``#`` fragment. Returns the resolved value as a string.

        Implementations raise any exception class on failure; the engine
        wraps the failure in :class:`SecretResolutionError` with the
        original cause chained.
        """
        ...


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class SecretResolutionError(Exception):
    """A ``secret://`` reference could not be parsed, dispatched, or resolved.

    Single exception class for every stage-9a failure path:

    * URL parse failure (malformed ``secret://`` URL, empty path, invalid
      scheme characters).
    * Unknown scheme (no resolver registered for the URL's scheme, neither
      via :func:`register_secret_resolver` nor through entry-points).
    * Resolver factory raised (instantiation failed — the factory's
      exception is chained as ``__cause__``).
    * Resolver's ``.resolve(...)`` raised (the underlying network / SDK
      error is chained as ``__cause__``).

    # NOTE: a single exception class keeps the call sites simple — the
    # engine's :func:`resolve_secret_ref` catches ``SecretResolutionError``
    # and re-raises as :class:`ConfigError` (the engine's existing
    # secret-resolution failure surface). Callers needing the underlying
    # cause read ``.__cause__``.

    # NOTE: error messages NEVER include the resolved value (it doesn't
    # exist at parse-time / dispatch-time anyway, and at resolve-time the
    # underlying exception is the resolver's — staying secret-free is a
    # contract on resolver implementations, not something this class can
    # enforce). The reference URL IS embedded in the message because it's
    # what the operator wrote in profiles.yml; surfacing it makes the
    # failure debuggable.
    """


# ---------------------------------------------------------------------------
# Registry — module-level, lock-protected, lazy
# ---------------------------------------------------------------------------


# Schemes are matched against this strict pattern — lowercase alphanumerics
# plus a dash separator. No underscores (URL schemes traditionally don't carry
# them), no dots (the dot is reserved for the ``#field`` syntax). Same
# permissiveness as RFC 3986 §3.1 minus uppercase + plus disallowed punctuation.
_VALID_SCHEME = re.compile(r"^[a-z0-9][a-z0-9-]*$")

# The registry lock protects every read/write to the four module-level
# mutables below. ``Lock``, not ``RLock``: no function holds the lock across
# a re-entrant call (the factory invocation in :func:`_get_resolver` runs
# OUTSIDE the lock — see its NOTE).
_REGISTRY_LOCK = threading.Lock()

# factory map: scheme -> callable returning a SecretResolver instance.
_RESOLVERS: dict[str, _ResolverFactory] = {}

# per-process instance cache: scheme -> resolved SecretResolver. Populated on
# first ``secret://<scheme>/...`` reference within a run; the same instance
# is reused for every subsequent reference on that scheme.
_RESOLVER_INSTANCES: dict[str, SecretResolver] = {}

# Set to True the first time :func:`resolve_secret_url` runs; entry-points
# discovery happens once per process at that point. A re-import would reset it
# (per-import-call mutables), but in normal use dtex runs as one process per
# CLI invocation.
_ENTRY_POINTS_LOADED = False

# Type alias for the factory callable shape. Kept here so the registry's
# typing is uniform across :func:`register_secret_resolver` and the
# entry-point loading branch.
_ResolverFactory = Any  # Callable[[], SecretResolver] — typed loosely
# to keep the entry-point branch's ``.load()`` result (typed Any by importlib)
# compatible without a cast at every call site. The runtime check on the
# returned object happens in :func:`_get_resolver`.


def register_secret_resolver(
    scheme: str,
    factory: Any,  # Callable[[], SecretResolver] — see _ResolverFactory NOTE
) -> None:
    """Register a resolver factory under ``scheme`` — the project-local
    plugin call.

    Called from a project's ``dtex_plugins.py``, or from third-party packages
    discovered via entry-points (internally; entry-point callers do not call
    this directly). The ``factory`` is a zero-arg callable that returns a
    fresh :class:`SecretResolver` instance.

    The instance is constructed lazily: this call only stores the factory.
    The resolver itself is instantiated the first time a
    ``secret://<scheme>/...`` reference is dispatched in the process — so
    registering an "expensive" resolver (one whose ``__init__`` initializes
    a cloud SDK client) does not pay that cost unless something actually
    uses it.

    Args:
        scheme: the URL token to register, lowercase ``[a-z0-9-]+``. A
            mixed-case or empty / invalid scheme is rejected with a
            :class:`ValueError` — keep this strict so an obvious typo
            (``register_secret_resolver("GCP", ...)`` shadowing
            ``"gcp"``) fails noisily.
        factory: zero-arg callable returning a :class:`SecretResolver`.
            Validated structurally only at first use — passing a non-callable
            here raises now (callable check), but a callable that returns
            something not implementing the Protocol surfaces as a
            :class:`SecretResolutionError` at first use.

    Raises:
        :class:`ValueError` for a bad scheme or non-callable factory.
        :class:`SecretResolutionError` for a duplicate scheme (first
        registration wins — second call is a hard error). The locked-state
        decision is "first registration wins" because the alternative (last
        wins) silently changes resolver behavior depending on
        ``dtex_plugins.py`` execution order, which is the kind of bug whose
        repro takes hours to find. Hard error makes the conflict visible at
        the second call site.

    # NOTE: ``Any`` typing on ``factory`` keeps mypy clean for both
    # ``Callable[[], MyResolver]`` (project-local register) and
    # ``importlib.metadata.EntryPoint.load()`` results (typed ``Any`` by
    # importlib). The structural check at call time is the runtime gate.
    """
    if not isinstance(scheme, str) or not _VALID_SCHEME.match(scheme):
        raise ValueError(
            f"secret resolver scheme {scheme!r} is invalid; expected "
            f"lowercase alphanumeric + dash (e.g. 'gcp', 'aws-secrets-manager')"
        )
    if not callable(factory):
        raise ValueError(
            f"secret resolver factory for scheme {scheme!r} must be callable; "
            f"got {type(factory).__name__}"
        )
    with _REGISTRY_LOCK:
        if scheme in _RESOLVERS:
            raise SecretResolutionError(
                f"secret resolver scheme {scheme!r} is already registered "
                f"(first registration wins; the duplicate registration was "
                f"rejected). To override, restart the process — registry "
                f"state is per-process."
            )
        _RESOLVERS[scheme] = factory


def is_secret_url(value: Any) -> bool:
    """Whether ``value`` looks like a ``secret://...`` URL — the dispatch
    predicate.

    Used by the config layer (:func:`dtex.engine.config.resolve_secret_ref`)
    to decide whether to hand a ref to :func:`resolve_secret_url`. Returns
    ``False`` for non-strings, empty strings, and any string that does not
    begin with the literal ``secret://`` prefix. Case-sensitive on the
    prefix — ``Secret://`` is NOT recognized; the URL form is fixed
    lowercase per docs/08 §3.

    # NOTE: the prefix check intentionally short-circuits before parsing.
    # Every value the config layer encounters runs through this predicate;
    # most values are env-var interpolation results (already-resolved
    # strings), so the cheap prefix test keeps the hot path fast.
    """
    return isinstance(value, str) and value.startswith("secret://")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _parse_secret_url(url: str) -> tuple[str, str, str | None]:
    """Parse ``secret://<scheme>/<path>[#<field>]`` into ``(scheme, path, field)``.

    Returns the three components. Raises :class:`SecretResolutionError`
    on a malformed URL (no scheme, empty path, invalid scheme characters).

    Implementation uses :func:`urllib.parse.urlsplit` — handles the
    ``#fragment`` split correctly, keeps dots in the fragment so
    ``#deep.nested.field`` round-trips. The scheme is lowercase per RFC
    3986; the path is whatever follows ``://<netloc>/`` (urlsplit treats
    ``<scheme>`` as the URL's netloc here because the URL has the
    ``scheme://authority/path`` shape — we treat the netloc AS the scheme
    and the path verbatim).

    # NOTE: design decision — we route on the URL's *netloc*, not its
    # ``urlsplit().scheme``. ``urlsplit("secret://gcp/foo/bar")`` returns
    # ``scheme='secret'``, ``netloc='gcp'``, ``path='/foo/bar'``. The
    # "scheme" the user writes is what urlsplit calls the netloc; the
    # outer ``secret://`` is a fixed prefix that distinguishes plugin URLs
    # from regular config strings. Using netloc-as-resolver-name keeps
    # urlsplit's grammar intact (it correctly handles authority, path,
    # fragment) without us re-implementing URL parsing by hand.
    """
    parts = urlsplit(url)
    if parts.scheme != "secret":
        raise SecretResolutionError(
            f"secret reference {url!r} must start with 'secret://'"
        )
    scheme = parts.netloc.lower()
    if not scheme:
        raise SecretResolutionError(
            f"secret reference {url!r} is missing a resolver scheme; "
            f"expected secret://<scheme>/<path>"
        )
    if not _VALID_SCHEME.match(scheme):
        raise SecretResolutionError(
            f"secret reference {url!r} has an invalid resolver scheme "
            f"{scheme!r}; expected lowercase alphanumeric + dash"
        )
    # Strip the leading slash urlsplit puts on the path (it splits at
    # ``netloc/path`` so the path always starts with '/'). An empty path
    # after stripping is a parse error — every resolver needs SOMETHING to
    # look up.
    path = parts.path.lstrip("/")
    if not path:
        raise SecretResolutionError(
            f"secret reference {url!r} is missing a path; expected "
            f"secret://<scheme>/<path>"
        )
    field = parts.fragment if parts.fragment else None
    return scheme, path, field


# ---------------------------------------------------------------------------
# Entry-points discovery (lazy — first use only)
# ---------------------------------------------------------------------------


_ENTRY_POINT_GROUP = "dtex.secret_resolvers"


def _load_entry_points() -> None:
    """Discover entry-points under ``dtex.secret_resolvers``; register each
    factory under its name if no project-local registration already claimed
    the scheme.

    Called from :func:`_get_resolver` the first time ANY scheme is looked
    up — never at module import. A project with zero ``secret://``
    references therefore never inspects entry-points, never pays the
    discovery cost.

    # NOTE: a single bad entry-point (a package that registered a resolver
    # but the import is broken) MUST NOT poison discovery for the rest. We
    # catch per-entry-point exceptions, store the error keyed by the
    # entry-point's name in a module-level diagnostic dict, and continue.
    # If the user later references that scheme, the resolution failure
    # surfaces the chained import error then — at the call site that
    # actually needed it. This is the strongest long-run answer: a broken
    # plugin should fail loud only when used, never as a meta-error that
    # blocks unrelated resolvers.

    # NOTE: project-local registrations always win — we only add an
    # entry-point factory if its scheme isn't already in ``_RESOLVERS``.
    # Project-local ``dtex_plugins.py`` runs at startup (BEFORE the first
    # ``secret://`` ref because :func:`load_project_plugins` is invoked by
    # the engine's discovery step), so by the time this function fires the
    # project-local schemes are already claimed.
    """
    global _ENTRY_POINTS_LOADED
    with _REGISTRY_LOCK:
        if _ENTRY_POINTS_LOADED:
            return
        _ENTRY_POINTS_LOADED = True
    try:
        eps = importlib.metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception as exc:  # noqa: BLE001 — see NOTE below
        # importlib.metadata can raise on broken installs (a corrupt
        # dist-info on the path). The strongest long-run answer is "don't
        # break resolution that doesn't need entry-points at all" — log
        # nothing here (the entry-points layer is below the logging stack)
        # but stash the error so a later debugging session can fish it
        # out. We don't raise: a project relying entirely on
        # ``dtex_plugins.py`` should still work.
        _ENTRY_POINTS_ERROR.append(exc)
        return
    for ep in eps:
        try:
            factory = ep.load()
        except Exception as exc:  # noqa: BLE001 — see _load_entry_points NOTE
            # A broken entry-point: record it, keep walking. The error
            # surfaces if a user actually references this entry-point's
            # scheme — but the entry-point's NAME might not match its
            # *intended* scheme (the EntryPoint name is what's in
            # pyproject.toml; the scheme is what the factory exposes). We
            # key on the entry-point name here as the best we can do
            # without instantiating it.
            _ENTRY_POINT_ERRORS[ep.name] = exc
            continue
        scheme = ep.name.lower()
        if not _VALID_SCHEME.match(scheme):
            # An entry-point whose name isn't a valid scheme can't be
            # routed to — log it as an error, skip.
            _ENTRY_POINT_ERRORS[ep.name] = ValueError(
                f"entry-point name {ep.name!r} is not a valid resolver "
                f"scheme (lowercase alphanumeric + dash)"
            )
            continue
        with _REGISTRY_LOCK:
            # Project-local registration wins — see this function's NOTE.
            if scheme in _RESOLVERS:
                continue
            _RESOLVERS[scheme] = factory


# Module-level diagnostic stash for entry-point load failures. Keyed by the
# entry-point name; populated by :func:`_load_entry_points`. Read by
# :func:`_resolve_one` when an unknown-scheme error needs context (e.g. "the
# 'gcp' entry-point is installed but failed to load: <original error>").
_ENTRY_POINT_ERRORS: dict[str, Exception] = {}
# Catastrophic entry-points discovery failure (rare; corrupt dist-info on the
# path). Stored separately because it has no per-name key.
_ENTRY_POINTS_ERROR: list[Exception] = []


# ---------------------------------------------------------------------------
# Project-local plugin import (called once per project from discovery)
# ---------------------------------------------------------------------------


# Track project roots whose ``dtex_plugins.py`` has already been imported, so
# a re-call (multiple ``dtex.run(...)`` invocations against the same project
# in one process) doesn't re-execute the file.
_LOADED_PROJECTS: set[Path] = set()
_PROJECT_PLUGINS_LOCK = threading.Lock()


def load_project_plugins(project_root: Path) -> None:
    """Import ``<project_root>/dtex_plugins.py`` if present — runs its
    registration calls.

    Called once per project from the engine's discovery step. The file is
    arbitrary user Python; it runs in-process with the engine's privileges.
    The trust model is identical to the rest of the project's
    user-supplied code (``sources/<name>/source.py``,
    ``destinations/<name>/destination.py``) — dtex imports project Python
    by design.

    Args:
        project_root: the directory holding ``dtex_project.yml`` (the value
            :func:`dtex.engine.discovery.find_project_root` returns).

    Idempotent per ``project_root`` per process: the second call for the
    same root is a no-op. A re-call for a *different* root imports the
    new file; both files' registrations coexist in the module-level
    registry until the process ends.

    # NOTE: import failure (the file raises at import time) is wrapped in
    # :class:`SecretResolutionError` and chained to the original exception.
    # The strongest long-run answer to "what happens when the plugin file
    # is broken" is "fail at startup with a clear error" — silently
    # swallowing would leave an unresolved scheme later with no breadcrumb.
    # We propagate so the engine's normal error path (a FAILED RunResult
    # with the exception attached) surfaces it.

    # NOTE: the file runs in a synthetic module name so a re-import of a
    # changed file would re-run its body — mirrors the connector-folder
    # import harness in :mod:`dtex.engine.discovery`. In practice ``dtex
    # run`` is one process per invocation; the harness exists for the
    # programmatic ``dtex.run(...)`` looping case.
    """
    plugin_path = project_root / "dtex_plugins.py"
    with _PROJECT_PLUGINS_LOCK:
        if project_root in _LOADED_PROJECTS:
            return
        if not plugin_path.is_file():
            # Mark as loaded so subsequent calls don't re-stat the file.
            _LOADED_PROJECTS.add(project_root)
            return
        # Synthetic module name; same shape as the connector-folder import
        # harness (``_dtex_connector_<stem>_<uuid>``). Uses a stable name
        # keyed by the project path so two calls for the same project
        # don't pollute sys.modules with N copies.
        module_name = f"_dtex_plugins_{abs(hash(str(project_root)))}"
        spec = importlib.util.spec_from_file_location(module_name, plugin_path)
        if spec is None or spec.loader is None:  # pragma: no cover — defensive
            raise SecretResolutionError(
                f"cannot load project plugins from {plugin_path}"
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            sys.modules.pop(module_name, None)
            raise SecretResolutionError(
                f"project plugins file {plugin_path} raised during import: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        _LOADED_PROJECTS.add(project_root)


# ---------------------------------------------------------------------------
# Dispatch — first-use instantiation + caching
# ---------------------------------------------------------------------------


def _get_resolver(scheme: str) -> SecretResolver:
    """Return the cached resolver for ``scheme``, instantiating its factory
    on first use.

    The lazy-instantiation hot path: check the instance cache (cheap, under
    lock), and only call the factory if absent. The factory call runs
    OUTSIDE the lock — a resolver whose ``__init__`` does network I/O (GCP
    SDK init can take a second) must not block every other scheme's
    lookup.

    # NOTE: the cost of "call the factory outside the lock" is that two
    # concurrent first-references on the same scheme might both run the
    # factory; the second to finish loses its instance to the first's
    # cached one. That's fine — the factory must be idempotent (returning
    # a fresh, equivalent instance) per the protocol contract. A run-once
    # behavior would need an in-flight lock per scheme, which is
    # complexity stage 9a doesn't need.
    """
    # Trigger entry-points discovery on first lookup of ANY scheme.
    _load_entry_points()

    with _REGISTRY_LOCK:
        existing = _RESOLVER_INSTANCES.get(scheme)
        if existing is not None:
            return existing
        factory = _RESOLVERS.get(scheme)
    if factory is None:
        # Augment the message if this scheme MATCHES a broken entry-point
        # — the most useful debugging hint in the unknown-scheme branch.
        hint = ""
        if scheme in _ENTRY_POINT_ERRORS:
            hint = (
                f" (an entry-point named {scheme!r} exists but failed to "
                f"load: {type(_ENTRY_POINT_ERRORS[scheme]).__name__}: "
                f"{_ENTRY_POINT_ERRORS[scheme]})"
            )
        known = sorted(_RESOLVERS)
        known_str = ", ".join(known) if known else "(none registered)"
        raise SecretResolutionError(
            f"no resolver registered for scheme {scheme!r}{hint}; "
            f"known schemes: {known_str}"
        )

    try:
        instance = factory()
    except Exception as exc:
        raise SecretResolutionError(
            f"failed to instantiate secret resolver for scheme {scheme!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    # Validate the instance shape — a factory that returns the wrong type
    # is a useful early-fail, even though Protocol checking is structural.
    resolve_attr = getattr(instance, "resolve", None)
    if resolve_attr is None or not callable(resolve_attr):
        raise SecretResolutionError(
            f"secret resolver factory for scheme {scheme!r} returned "
            f"{type(instance).__name__}, which has no callable .resolve() "
            f"method"
        )

    with _REGISTRY_LOCK:
        # A racing call might have cached one already — first writer wins;
        # we accept whichever is in the cache.
        existing = _RESOLVER_INSTANCES.get(scheme)
        if existing is not None:
            return existing
        _RESOLVER_INSTANCES[scheme] = instance
    return instance


def resolve_secret_url(url: str) -> str:
    """Resolve a ``secret://<scheme>/<path>[#<field>]`` URL to its value.

    The single public dispatch entry point. Used by
    :func:`dtex.engine.config.resolve_secret_ref` when a ``register.yaml``
    ``secrets[].ref`` value (or a profile value passed through it) begins
    with ``secret://``. Steps:

    1. Parse the URL (:func:`_parse_secret_url`).
    2. Look up — or lazily instantiate — the resolver for the URL's scheme
       (:func:`_get_resolver`).
    3. Call ``resolver.resolve(path, field)`` and return its string result.

    Every failure path raises :class:`SecretResolutionError` with the
    original cause chained.

    # NOTE: this function is the redaction boundary for the value layer.
    # The caller (config.resolve_secret_ref) is responsible for adding the
    # returned string to the run's :class:`~dtex.engine.logger.Redactor` so
    # subsequent log emissions mask it — see the runner's
    # ``redactor.add(source_config.secrets.values())`` call. This function
    # cannot do that itself because it has no Redactor handle; making
    # redaction a side effect here would couple the secrets layer to the
    # logger layer in the wrong direction.
    """
    scheme, path, field = _parse_secret_url(url)
    resolver = _get_resolver(scheme)
    try:
        value = resolver.resolve(path, field)
    except Exception as exc:
        # The resolver's exception text MIGHT embed the value (the contract
        # says it shouldn't, but the engine cannot enforce it). We re-wrap
        # WITHOUT inlining the original exception's message in our own —
        # the chained ``__cause__`` carries the original for tracebacks,
        # but our outer message stays minimal. The Redactor is the
        # backstop on the log side.
        raise SecretResolutionError(
            f"resolver {scheme!r} failed to resolve {url!r}: "
            f"{type(exc).__name__}"
        ) from exc
    if not isinstance(value, str):
        raise SecretResolutionError(
            f"resolver {scheme!r} returned a non-string value of type "
            f"{type(value).__name__} for {url!r}; resolvers must return str"
        )
    return value


# ---------------------------------------------------------------------------
# Test-only helper
# ---------------------------------------------------------------------------


def _reset_resolvers_for_testing() -> None:
    """Wipe all module-level state — used by the test suite as an autouse
    fixture to keep test isolation.

    NOT public API. Tests that register a fake resolver MUST call this
    in setup AND teardown so a leaked registration from one test cannot
    affect another. The function clears the factory map, the instance
    cache, the entry-points-loaded flag, the project-plugins-loaded set,
    and the diagnostic stashes.

    # NOTE: also used in real test fixtures for the project-local plugin
    # test (the test writes a dtex_plugins.py to a tmp_path project; without
    # the reset, a previous test's project-plugin registrations would
    # still occupy the registry).
    """
    global _ENTRY_POINTS_LOADED
    with _REGISTRY_LOCK:
        _RESOLVERS.clear()
        _RESOLVER_INSTANCES.clear()
        _ENTRY_POINTS_LOADED = False
        _ENTRY_POINT_ERRORS.clear()
        _ENTRY_POINTS_ERROR.clear()
    with _PROJECT_PLUGINS_LOCK:
        _LOADED_PROJECTS.clear()
