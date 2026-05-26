"""GCP Secret Manager resolver — the first ``secret://`` plugin (stage 9b).

Provides :class:`GcpSecretManagerResolver`, the concrete adapter for
``secret://gcp-secret-manager/<resource-name>`` references. The scheme spelling
is locked at docs/08 §3:

    secret://gcp-secret-manager/projects/<project>/secrets/<name>/versions/<v>

Auto-registered via ``[project.entry-points."detx.secret_resolvers"]`` —
operators install the optional extra and reference the URL in
``profiles.yml`` without any other wiring::

    pip install 'detx[gcp-secrets]'

The resolver is built on first use by stage 9a's lazy instantiation
(:func:`detx.secrets.resolvers._get_resolver`); the SDK client itself is
built on the first ``.resolve()`` call inside the instance and then
cached on ``self._client``. Authentication uses Application Default
Credentials — the operator's environment provides
``GOOGLE_APPLICATION_CREDENTIALS`` or ``gcloud auth
application-default login``, same as the BigQuery destination
(see :mod:`detx.destinations.bigquery.client`).

Operator setup (one-time per project):

1. Create the secret in GCP::

       gcloud secrets create my-stripe-key --replication-policy=automatic
       gcloud secrets versions add my-stripe-key --data-file=-  <<< 'sk_live_xxx'

2. Grant the service account (or your user ADC) the
   ``roles/secretmanager.secretAccessor`` role on the secret.

3. Install the extra: ``pip install 'detx[gcp-secrets]'``.

4. Reference it in ``profiles.yml``::

       prod:
         api_key: secret://gcp-secret-manager/projects/my-proj/secrets/my-stripe-key/versions/latest

# NOTE: docs/08 §3's table row originally listed the scheme as
# ``secret://gcp/...`` (and status ``v2``); the example a few lines above
# uses the locked spelling ``secret://gcp-secret-manager/...``. Stage 9b
# updates the table row to match the example (the example is the canonical
# decision per the orchestrator). The scheme spelling is intentionally
# verbose so the URL is unambiguous about WHICH GCP secret service it
# routes to (Cloud KMS, Secret Manager, IAM Workload Identity are all
# "GCP secrets" in some loose sense).

# NOTE: ``field`` (the ``#<field>`` URL suffix) is IGNORED by this
# resolver. GCP Secret Manager returns a single opaque blob per version —
# there is no structured payload to pick a key from. We log a one-time
# warning per (path, field) pair so an operator who wrote
# ``secret://gcp-secret-manager/.../versions/latest#token`` (probably by
# mistake — the Vault syntax) gets a hint without spamming logs. The
# resolution itself still succeeds with the full blob. Vault's resolver
# (stage 9c) is the resolver that DOES honor ``#field``.

# NOTE: this resolver does NOT register its resolved value with the
# run's :class:`~detx.engine.logger.Redactor`. The engine does that
# post-hoc in :mod:`detx.engine.runner` (see
# ``redactor.add(source_config.secrets.values())`` at runner.py:1290),
# AFTER ``resolve_config_for_target`` collects every resolved secret
# value into a dict. Registering here would couple secrets→logger in
# the wrong direction — the same call-site decision documented in
# :func:`detx.secrets.resolvers.resolve_secret_url`'s NOTE.

# NOTE: retries are left to the SDK. ``google.api_core``'s default retry
# policy on ``access_secret_version`` already handles transient 5xx /
# 429 with exponential backoff; layering detx-side retries would double
# the effective attempt count without changing the failure mode. A
# clear ``SecretResolutionError`` after the SDK exhausts its own
# retries is the correct surface.
"""

from __future__ import annotations

import logging
import re
from typing import Any, ClassVar

from detx.secrets.resolvers import SecretResolutionError

logger = logging.getLogger("detx.secrets")


_INSTALL_HINT = (
    "the GCP Secret Manager resolver needs `google-cloud-secret-manager`; "
    "install with `pip install detx[gcp-secrets]`"
)

# The canonical resource-name shape for a Secret Manager version, per GCP
# docs: https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/access
# ``latest`` is a valid version token (the alias for "newest enabled version").
_RESOURCE_NAME = re.compile(
    r"^projects/[^/]+/secrets/[^/]+/versions/[^/]+$"
)


def _lazy_import_gcp_secrets() -> Any:
    """Import :mod:`google.cloud.secretmanager` on first use, or raise a clear
    :class:`ImportError`.

    Mirrors the lazy-import precedent in
    :func:`detx.sources.filesystem.backends._lazy_import_gcs` and
    :func:`detx.destinations.bigquery.client._bigquery_module` — the SDK is
    NOT imported at ``detx`` package import time, so a base install (or one
    that uses ``[bigquery]`` / ``[gcs]`` but not ``[gcp-secrets]``) keeps
    importing cleanly. A missing SDK surfaces with the install command in
    the error message.

    # NOTE: unlike ``google-cloud-storage``, the ``google-cloud-secret-manager``
    # package ships a ``py.typed`` marker (since v2.x), so mypy resolves
    # the ``from google.cloud import secretmanager`` attribute access
    # without a ``# type: ignore[attr-defined]``. The GCS backend's
    # lazy-import shim DOES need that ignore because google-cloud-storage
    # ships no typed marker. If a future SDK release drops py.typed, this
    # branch should regain the ignore comment.
    """
    try:
        from google.cloud import secretmanager  # noqa: PLC0415 — lazy by design
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return secretmanager


def _lazy_import_google_exceptions() -> Any:
    """Import :mod:`google.api_core.exceptions` on first use.

    Returned separately because ``access_secret_version`` raises
    :class:`google.api_core.exceptions.PermissionDenied` /
    :class:`~.NotFound` etc.; we need their classes to branch the error
    surface, and they live in a different sub-package than the client.
    A missing import re-uses :data:`_INSTALL_HINT` (the same extra brings
    both in).
    """
    try:
        from google.api_core import exceptions as gapi_exceptions  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return gapi_exceptions


class GcpSecretManagerResolver:
    """Resolver for ``secret://gcp-secret-manager/<resource-name>`` references.

    Implements the :class:`~detx.secrets.SecretResolver` Protocol. The
    ``path`` argument to :meth:`resolve` is the full GCP resource name
    (everything after ``secret://gcp-secret-manager/``) — see the module
    docstring for the shape. The optional ``field`` argument is IGNORED
    with a one-time warning per (path, field) pair; GCP Secret Manager
    payloads are opaque blobs.

    Lazy instantiation contract (stage 9a):

    * ``__init__`` does NOT import the SDK or build any client. The
      resolver instance is built by stage 9a's
      :func:`~detx.secrets.resolvers._get_resolver` on first URL referencing
      this scheme; the SDK client is built inside :meth:`resolve` on first
      ``.resolve()`` call and cached on ``self._client``.

    * The cache is per-instance, and the instance is itself cached
      per-process per-scheme by stage 9a. Effective lifetime: one
      SDK client per ``detx`` process, however many ``secret://gcp-secret-manager/...``
      references the run consumes.

    Errors:

    * Malformed path (does not match :data:`_RESOURCE_NAME`) →
      :class:`~detx.secrets.SecretResolutionError` with the validation
      message.
    * ``google.api_core.exceptions.PermissionDenied`` →
      :class:`~detx.secrets.SecretResolutionError` with "permission denied
      accessing X". The resource name X is safe to surface (it's what the
      operator wrote in ``profiles.yml``); the value is NEVER touched.
    * ``google.api_core.exceptions.NotFound`` →
      :class:`~detx.secrets.SecretResolutionError` with "secret X version
      Y not found".
    * Any other SDK error → :class:`~detx.secrets.SecretResolutionError`
      wrapping it; the original exception is chained via ``__cause__``.
    """

    scheme: ClassVar[str] = "gcp-secret-manager"

    def __init__(self) -> None:
        """Build the resolver shell — NO SDK import, NO client construction.

        The SDK client is built lazily inside :meth:`resolve` on first
        call; until then, the resolver is a free object.
        """
        # The SDK client, cached on first ``.resolve()`` call. Typed
        # :class:`typing.Any` because the SDK type is only available when
        # the optional extra is installed (mypy sees ``Any`` either way).
        self._client: Any | None = None
        # Per-instance dedupe set for the ``#field`` ignored-warning.
        # Module-level would cross instances; per-instance is correct
        # because stage 9a caches the instance per-process anyway.
        self._field_warned: set[tuple[str, str | None]] = set()

    def _build_client(self) -> Any:
        """Construct + cache the SDK client on first use.

        Uses Application Default Credentials — no credential argument is
        passed. The operator wires ADC via
        ``GOOGLE_APPLICATION_CREDENTIALS`` or ``gcloud auth
        application-default login``, identical to the BigQuery
        destination's auth model.
        """
        if self._client is not None:
            return self._client
        secretmanager = _lazy_import_gcp_secrets()
        # NOTE: ``SecretManagerServiceClient()`` does NOT perform a
        # network round-trip on construction — the underlying gRPC
        # channel is opened on first RPC. So this stays cheap to call
        # under the engine's discovery + dry-run paths.
        self._client = secretmanager.SecretManagerServiceClient()
        return self._client

    def resolve(self, path: str, field: str | None) -> str:
        """Resolve one ``secret://gcp-secret-manager/<path>[#<field>]`` reference.

        Args:
            path: the URL path with the scheme stripped — the full GCP
                resource name ``projects/<p>/secrets/<n>/versions/<v>``.
                ``<v>`` may be ``latest`` (GCP alias for the newest
                enabled version) or a numeric version id.
            field: the URL's ``#<field>`` suffix, or ``None`` if absent.
                IGNORED by this resolver (GCP payloads are opaque blobs);
                a one-time warning per (path, field) tuple is emitted on
                first sighting.

        Returns:
            The secret's payload decoded as UTF-8. GCP stores the value
            as raw bytes; ASCII / UTF-8 is the universal convention for
            credentials, and the engine's downstream consumers all expect
            ``str``. A payload that is not valid UTF-8 surfaces as a
            :class:`~detx.secrets.SecretResolutionError` (the underlying
            :class:`UnicodeDecodeError` is chained).

        Raises:
            :class:`~detx.secrets.SecretResolutionError` — see the class
            docstring's error table.
        """
        # Validate the path shape FIRST — a bad path should fail before
        # paying the SDK import cost, so a typo in profiles.yml fails fast.
        if not _RESOURCE_NAME.match(path):
            raise SecretResolutionError(
                f"GCP Secret Manager path {path!r} is malformed; expected "
                f"projects/<project>/secrets/<name>/versions/<version> "
                f"(use 'latest' or a numeric version id)"
            )

        if field is not None:
            key = (path, field)
            if key not in self._field_warned:
                self._field_warned.add(key)
                logger.warning(
                    "secret://gcp-secret-manager/%s#%s: GCP Secret Manager "
                    "returns a single opaque blob per version; the '#%s' "
                    "field suffix is ignored. The full payload is returned. "
                    "(Vault is the resolver that honors '#field'.)",
                    path,
                    field,
                    field,
                )

        client = self._build_client()
        gapi_exceptions = _lazy_import_google_exceptions()

        try:
            response = client.access_secret_version(name=path)
        except gapi_exceptions.PermissionDenied as exc:
            # Path is safe to surface (it's what's in profiles.yml). The
            # SDK's own exception text may include the resource name plus
            # a service URL; we strip it down to a minimal message because
            # the chained ``__cause__`` preserves the full traceback.
            raise SecretResolutionError(
                f"permission denied accessing GCP secret {path!r}; verify "
                f"the credentials have roles/secretmanager.secretAccessor "
                f"on the secret"
            ) from exc
        except gapi_exceptions.NotFound as exc:
            raise SecretResolutionError(
                f"GCP secret {path!r} not found; verify the resource name "
                f"and that the version exists"
            ) from exc
        except gapi_exceptions.GoogleAPIError as exc:
            # Catch-all for every other google-api-core surface (DeadlineExceeded,
            # ServiceUnavailable after SDK retries exhausted, etc.). The class
            # name surfaces; the SDK's message body is NOT inlined here because
            # an exotic server-side error MIGHT echo metadata that brushes the
            # secret. The chained ``__cause__`` carries full detail for tracebacks.
            raise SecretResolutionError(
                f"GCP Secret Manager error resolving {path!r}: "
                f"{type(exc).__name__}"
            ) from exc

        # GCP returns ``response.payload.data`` as bytes. Decode as UTF-8 —
        # the universal credential encoding. Surface a decode failure as
        # :class:`SecretResolutionError` with the underlying UnicodeDecodeError
        # chained.
        payload_bytes: bytes = response.payload.data
        try:
            return payload_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SecretResolutionError(
                f"GCP secret {path!r} payload is not valid UTF-8; "
                f"detx requires resolved secret values to be strings"
            ) from exc
