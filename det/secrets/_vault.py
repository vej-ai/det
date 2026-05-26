"""HashiCorp Vault resolver — the third ``secret://`` plugin (stage 9c).

Provides :class:`VaultResolver`, the concrete adapter for
``secret://vault/<mount-path>/<kv-path>#<field>`` references. The scheme
spelling is locked at docs/08 §3.

URL path shape::

    secret://vault/<mount-path>/<kv-path>#<field>          (KV v1)
    secret://vault/<mount-path>/data/<kv-path>#<field>     (KV v2)

* ``<mount-path>/<kv-path>`` — the full Vault read path, opaque to det.
  The resolver hands it to :func:`hvac.Client.read` verbatim. For KV v1
  this is typically ``secret/<name>``; for KV v2 it is
  ``secret/data/<name>`` (KV v2 mounts an extra ``/data/`` segment under
  every read path — operators familiar with the Vault CLI write
  ``vault kv get secret/<name>`` against either, but the raw API path
  reflects the v2 layout).
* ``<field>`` — REQUIRED. Vault's KV engines return a JSON object;
  ``#field`` picks one key out of it. A reference without ``#field``
  raises a clear :class:`~det.secrets.SecretResolutionError`.

Auto-registered via ``[project.entry-points."det.secret_resolvers"]``::

    pip install 'det[vault]'

The resolver is built on first use by stage 9a's lazy instantiation
(:func:`det.secrets.resolvers._get_resolver`); the :class:`hvac.Client`
itself is built on the first ``.resolve()`` call inside the instance and
cached on ``self._client``.

Operator setup (one-time per Vault deployment):

1. Create the secret in Vault::

       # KV v2 (the default for modern Vault)
       vault kv put secret/warehouse username=u password=p

       # KV v1 (legacy)
       vault kv put -mount=secret/legacy/ warehouse username=u password=p

2. Grant the token det runs as a policy with
   ``read`` capability on the secret's path.

3. Install the extra: ``pip install 'det[vault]'``.

4. Export ``VAULT_ADDR`` (the Vault URL) and ``VAULT_TOKEN`` (the auth
   token) in the environment det runs in. These are the same env vars
   the official Vault CLI consults.

5. Reference it in ``profiles.yml``::

       prod:
         # KV v2 (the URL includes the explicit /data/ segment AWS-style)
         user:     secret://vault/secret/data/warehouse#username
         password: secret://vault/secret/data/warehouse#password

         # KV v1
         legacy:   secret://vault/secret/legacy/warehouse#password

# NOTE: v1 ONLY supports token auth via the ``VAULT_TOKEN`` env var.
# AppRole, Kubernetes auth, and other auth methods are deferred — they
# need credentials beyond a single bearer token and add multi-step login
# flows (``client.auth.approle.login(...)``) the resolver protocol does
# not currently model. The strongest long-run path is to either layer
# them under additional URL forms (``secret://vault-approle/...``) or
# adopt a Vault-native helper sidecar; both are v2 questions.

# NOTE: ``#field`` is REQUIRED for Vault — unlike GCP (where ``#field``
# is silently dropped because GCP returns one opaque blob) or AWS
# (where ``#field`` is optional and selects from a JSON payload),
# Vault's KV engines ALWAYS return a JSON object. The resolver cannot
# know which key the operator wanted without ``#field``, so we fail
# loudly at parse time rather than silently returning the first key or
# the whole object.

# NOTE: KV v1 vs KV v2 detection is a heuristic on the response shape.
# KV v1 returns ``{"data": {<field>: <value>, ...}}``; KV v2 returns
# ``{"data": {"data": {<field>: <value>, ...}, "metadata": {...}}}``.
# We branch on whether ``result["data"]`` is a dict containing a
# nested ``"data"`` key that is itself a dict. A KV v1 secret with a
# field literally named ``data`` whose value is a dict would be
# misread as KV v2 — operators avoid that by not nesting under
# ``data``, and there is no API surface to distinguish v1 from v2
# above the read response.

# NOTE: this resolver does NOT register its resolved value with the
# run's :class:`~det.engine.logger.Redactor`. The engine does that
# post-hoc in :mod:`det.engine.runner` (see
# ``redactor.add(source_config.secrets.values())`` at runner.py:1290),
# AFTER ``resolve_config_for_target`` collects every resolved secret
# value into a dict. Registering here would couple secrets→logger in
# the wrong direction — the same call-site decision documented in
# :func:`det.secrets.resolvers.resolve_secret_url`'s NOTE.

# NOTE: hvac exception messages can echo policy denial text including
# secret names. The catch-all surfaces only ``type(exc).__name__``;
# the chained ``__cause__`` preserves the original for tracebacks.
"""

from __future__ import annotations

import os
from typing import Any, ClassVar

from det.secrets.resolvers import SecretResolutionError

_INSTALL_HINT = (
    "the Vault resolver needs `hvac`; "
    "install with `pip install det[vault]`"
)

# Env var names hvac itself documents: VAULT_ADDR for the URL,
# VAULT_TOKEN for the bearer token. The official Vault CLI reads the
# same names; honoring them keeps the resolver no-config for operators
# who already export them.
_ENV_VAULT_ADDR = "VAULT_ADDR"
_ENV_VAULT_TOKEN = "VAULT_TOKEN"  # noqa: S105 — env var name, not a secret


def _lazy_import_hvac() -> Any:
    """Import :mod:`hvac` on first use, or raise a clear :class:`ImportError`.

    Mirrors the lazy-import precedent in
    :func:`det.secrets._gcp._lazy_import_gcp_secrets` — the SDK is NOT
    imported at ``det`` package import time, so a base install keeps
    importing cleanly. A missing SDK surfaces with the install command
    in the error message.
    """
    try:
        # NOTE: ``type: ignore[import-untyped]`` — hvac ships no
        # py.typed marker. Stage 9c's verify checklist installs the
        # ``vault`` extra which brings hvac in, so mypy emits
        # ``import-untyped`` rather than ``import-not-found``.
        import hvac  # type: ignore[import-untyped]  # noqa: PLC0415 — lazy
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return hvac


def _lazy_import_hvac_exceptions() -> Any:
    """Import :mod:`hvac.exceptions` on first use.

    Returned separately because ``client.read`` raises
    :class:`hvac.exceptions.InvalidRequest`,
    :class:`~hvac.exceptions.Forbidden`,
    :class:`~hvac.exceptions.VaultError`, etc.; we need their classes
    to branch the error surface. Same install hint applies (the same
    extra brings both in).
    """
    try:
        # NOTE: no ``type: ignore`` needed here — once the parent
        # ``hvac`` module is silenced at the prior shim, mypy treats
        # the sub-import as ``Any``. Adding an ignore would trip
        # ``warn_unused_ignores``.
        from hvac import exceptions as hvac_exceptions  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return hvac_exceptions


class VaultResolver:
    """Resolver for ``secret://vault/<mount-path>/<kv-path>#<field>`` references.

    Implements the :class:`~det.secrets.SecretResolver` Protocol. The
    ``path`` argument to :meth:`resolve` is the URL path with the
    scheme stripped — handed to :func:`hvac.Client.read` verbatim. The
    ``field`` argument is REQUIRED (a missing ``#field`` raises).

    Lazy instantiation contract (stage 9a):

    * ``__init__`` does NOT import the SDK or build any client. The
      resolver instance is built by stage 9a's
      :func:`~det.secrets.resolvers._get_resolver` on first URL referencing
      this scheme; the SDK client is built inside :meth:`resolve` on
      first ``.resolve()`` call and cached on ``self._client``.

    * Effective lifetime: one ``hvac.Client`` per ``det`` process,
      however many ``secret://vault/...`` references the run consumes.

    Errors:

    * Missing ``#field`` → :class:`~det.secrets.SecretResolutionError`
      with the contract message — fired BEFORE SDK import.
    * Empty path → :class:`~det.secrets.SecretResolutionError` — fired
      BEFORE SDK import.
    * Missing ``VAULT_ADDR`` or ``VAULT_TOKEN`` env var → clear error
      naming the missing variable — fired on first client build.
    * ``client.read(path)`` returns ``None`` (the secret does not exist
      OR the token lacks read capability) →
      ``"vault: no secret at path X (path missing or token lacks read)"``.
    * Field not present in the secret data → ``"vault: field X not in
      secret at path Y"``.
    * Non-string field value (e.g. nested object) → ``"vault: field X
      is not a string (got type Y)"``.
    * Any other ``hvac.exceptions.*`` → wrapped; only the class name
      is surfaced (same caution as GCP/AWS — Vault errors can echo
      policy text).
    """

    scheme: ClassVar[str] = "vault"

    def __init__(self) -> None:
        """Build the resolver shell — NO SDK import, NO client construction.

        The SDK client is built lazily inside :meth:`resolve` on first
        call; until then, the resolver is a free object.
        """
        # The hvac client, cached on first ``.resolve()`` call. Typed
        # :class:`typing.Any` because the SDK type is only available
        # when the optional extra is installed.
        self._client: Any | None = None

    def _build_client(self) -> Any:
        """Construct + cache the hvac client on first use.

        Reads ``VAULT_ADDR`` and ``VAULT_TOKEN`` from the environment —
        both must be set. A missing env var raises
        :class:`SecretResolutionError` naming which variable is
        missing.
        """
        if self._client is not None:
            return self._client
        addr = os.environ.get(_ENV_VAULT_ADDR)
        if not addr:
            raise SecretResolutionError(
                f"vault: ${_ENV_VAULT_ADDR} is not set; the Vault resolver "
                f"needs the Vault server URL in this env var (same convention "
                f"as the `vault` CLI)"
            )
        token = os.environ.get(_ENV_VAULT_TOKEN)
        if not token:
            raise SecretResolutionError(
                f"vault: ${_ENV_VAULT_TOKEN} is not set; the Vault resolver "
                f"needs a Vault auth token in this env var (same convention "
                f"as the `vault` CLI)"
            )
        hvac = _lazy_import_hvac()
        # NOTE: ``hvac.Client(...)`` does not perform a network
        # round-trip on construction — the underlying ``requests``
        # session is built lazily. So this stays cheap to call under
        # the engine's discovery + dry-run paths.
        self._client = hvac.Client(url=addr, token=token)
        return self._client

    def resolve(self, path: str, field: str | None) -> str:
        """Resolve one ``secret://vault/<path>#<field>`` reference.

        Args:
            path: the URL path with the scheme stripped — the full
                Vault read path (e.g. ``secret/data/warehouse`` for
                KV v2, ``secret/legacy/warehouse`` for KV v1).
            field: the URL's ``#<field>`` suffix. REQUIRED — a ``None``
                value raises :class:`SecretResolutionError`.

        Returns:
            The string value at ``data[field]`` (KV v1) or
            ``data.data[field]`` (KV v2) in the Vault response.

        Raises:
            :class:`~det.secrets.SecretResolutionError` — see the class
            docstring's error table.
        """
        # ``#field`` is REQUIRED — fail BEFORE SDK import so a typo in
        # profiles.yml fails fast without paying the hvac import cost.
        if field is None:
            raise SecretResolutionError(
                f"vault secrets require a #field; got path={path!r} with no "
                f"#field. The KV engine returns a JSON object; you must name "
                f"which key to extract (e.g. "
                f"secret://vault/secret/data/warehouse#password)"
            )
        if not path:
            raise SecretResolutionError(
                "vault: path is empty; expected "
                "secret://vault/<mount-path>/<kv-path>#<field>"
            )

        client = self._build_client()
        hvac_exceptions = _lazy_import_hvac_exceptions()

        try:
            result = client.read(path)
        except hvac_exceptions.VaultError as exc:
            # Catch-all for every hvac.exceptions surface (Forbidden,
            # InvalidPath, InvalidRequest, etc.). The class name
            # surfaces; the hvac message body is NOT inlined because
            # Vault errors echo policy text that may include secret
            # names or path templates. The chained ``__cause__``
            # carries full detail for tracebacks.
            raise SecretResolutionError(
                f"vault: error reading {path!r}: {type(exc).__name__}"
            ) from exc

        if result is None:
            # hvac returns ``None`` for a missing path OR for a path
            # the token lacks read capability on (Vault deliberately
            # returns 404 in both cases to avoid leaking existence).
            # The error message reflects both possibilities.
            raise SecretResolutionError(
                f"vault: no secret at path {path!r} (path missing or "
                f"token lacks read)"
            )

        # KV v1 response shape: ``{"data": {<field>: <value>, ...}}``
        # KV v2 response shape: ``{"data": {"data": {<field>: <value>, ...},
        #                          "metadata": {...}}}``
        # Detect v2 by the nested ``data`` key being a dict — see the
        # module-level KV v1/v2 NOTE for the degenerate edge.
        outer_data = result.get("data") if isinstance(result, dict) else None
        if not isinstance(outer_data, dict):
            raise SecretResolutionError(
                f"vault: unexpected response shape for {path!r}; expected a "
                f"data dict at the top level"
            )
        inner = outer_data.get("data")
        if isinstance(inner, dict):
            # KV v2.
            payload: dict[str, Any] = inner
        else:
            # KV v1.
            payload = outer_data

        if field not in payload:
            raise SecretResolutionError(
                f"vault: field {field!r} not in secret at path {path!r}"
            )
        value = payload[field]
        if not isinstance(value, str):
            raise SecretResolutionError(
                f"vault: field {field!r} at path {path!r} is not a string "
                f"(got type {type(value).__name__})"
            )
        return value
