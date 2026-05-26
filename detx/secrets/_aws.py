"""AWS Secrets Manager resolver — the second ``secret://`` plugin (stage 9c).

Provides :class:`AwsSecretsManagerResolver`, the concrete adapter for
``secret://aws-secrets-manager/<region>/<secret-id>[:<version-stage>][#<field>]``
references. The scheme spelling is locked at docs/08 §3.

URL path shape::

    secret://aws-secrets-manager/<region>/<secret-id>
    secret://aws-secrets-manager/<region>/<secret-id>:<version-stage>
    secret://aws-secrets-manager/<region>/<secret-id>#<json-field>

* ``<region>`` — an AWS region name (lowercase alphanumerics + dashes,
  ``[a-z0-9-]+``). Validated BEFORE the SDK is imported so a typo in
  ``profiles.yml`` fails fast.
* ``<secret-id>`` — the secret name (or full ARN). Opaque to detx.
* ``<version-stage>`` — optional staging label appended via ``:``.
  Default is ``AWSCURRENT`` (the live version). Common alternates are
  ``AWSPENDING`` (the staged rotation candidate) and ``AWSPREVIOUS``.
* ``<json-field>`` — optional ``#field`` suffix. AWS Secrets Manager
  returns a single string per version; when operators store a JSON
  blob like ``{"username":"u","password":"p"}``, the ``#field`` suffix
  picks one key out of the parsed JSON (a common idiom — for example,
  the ``secretsmanager`` Postgres credentials format).

Auto-registered via ``[project.entry-points."detx.secret_resolvers"]`` —
operators install the optional extra and reference the URL without any
other wiring::

    pip install 'detx[aws-secrets]'

The resolver is built on first use by stage 9a's lazy instantiation
(:func:`detx.secrets.resolvers._get_resolver`); the SDK client itself is
built on the first ``.resolve()`` call inside the instance and cached
per-region on ``self._clients[region]``. Authentication uses boto3's
standard credential chain (env vars → ``~/.aws/credentials`` → IAM role)
— no detx-side credential argument is passed.

Operator setup (one-time per AWS region):

1. Create the secret in AWS::

       aws secretsmanager create-secret \\
           --name my-stripe-key \\
           --secret-string 'sk_live_xxx' \\
           --region us-east-1

   Or for JSON payloads::

       aws secretsmanager create-secret \\
           --name my-db-creds \\
           --secret-string '{"username":"u","password":"p"}' \\
           --region us-east-1

2. Grant the IAM principal detx runs as the
   ``secretsmanager:GetSecretValue`` permission on the secret's ARN.

3. Install the extra: ``pip install 'detx[aws-secrets]'``.

4. Reference it in ``profiles.yml``::

       prod:
         api_key:  secret://aws-secrets-manager/us-east-1/my-stripe-key
         db_pass:  secret://aws-secrets-manager/us-east-1/my-db-creds#password

# NOTE: ``SecretBinary`` is NOT supported in v1. AWS Secrets Manager can
# store either ``SecretString`` (UTF-8 text) or ``SecretBinary`` (raw
# bytes). Credentials are text-shaped; the binary surface is the rare
# path. Surfacing a base64-decoded binary as a detx secret value would
# also conflict with downstream consumers that expect ``str``. A binary
# secret raises :class:`~detx.secrets.SecretResolutionError` with a clear
# "binary secrets not supported in v1" message.

# NOTE: this resolver does NOT register its resolved value with the
# run's :class:`~detx.engine.logger.Redactor`. The engine does that
# post-hoc in :mod:`detx.engine.runner` (see
# ``redactor.add(source_config.secrets.values())`` at runner.py:1290),
# AFTER ``resolve_config_for_target`` collects every resolved secret
# value into a dict. Registering here would couple secrets→logger in
# the wrong direction — the same call-site decision documented in
# :func:`detx.secrets.resolvers.resolve_secret_url`'s NOTE.

# NOTE: retries are left to the SDK. boto3's default
# ``standard`` retry mode handles transient 5xx / throttling with
# exponential backoff; layering detx-side retries would double the
# effective attempt count without changing the failure mode.

# NOTE: ``boto3>=1.28`` is ALSO declared under the ``s3`` extra (the
# filesystem connector's S3 backend). Both extras can be installed
# side-by-side — pip dedupes the shared dependency. Removing it from
# either extra would break that extra's standalone install path.
"""

from __future__ import annotations

import json
import re
from typing import Any, ClassVar

from detx.secrets.resolvers import SecretResolutionError

_INSTALL_HINT = (
    "the AWS Secrets Manager resolver needs `boto3`; "
    "install with `pip install detx[aws-secrets]`"
)

# Lowercase alphanumerics + dashes; matches every published AWS region name
# (``us-east-1``, ``eu-west-2``, ``ap-southeast-3``, etc.). The validator
# stays permissive ("a region-shaped token") rather than enumerating the
# full live region list — AWS adds regions, and a strict allow-list would
# go stale.
_VALID_REGION = re.compile(r"^[a-z0-9-]+$")

# Default version stage if the URL omits ``:<stage>``. AWS treats
# ``AWSCURRENT`` as "the live version" — the most common operator
# intent. ``AWSPENDING`` and ``AWSPREVIOUS`` are the other built-ins.
_DEFAULT_VERSION_STAGE = "AWSCURRENT"


def _lazy_import_boto3() -> Any:
    """Import :mod:`boto3` on first use, or raise a clear :class:`ImportError`.

    Mirrors the lazy-import precedent in
    :func:`detx.secrets._gcp._lazy_import_gcp_secrets` — the SDK is NOT
    imported at ``detx`` package import time, so a base install (or one
    that uses ``[gcp-secrets]`` but not ``[aws-secrets]``) keeps importing
    cleanly. A missing SDK surfaces with the install command in the error
    message.
    """
    try:
        # NOTE: ``type: ignore[import-untyped]`` — boto3 ships no
        # py.typed marker. The dev install for stage 9c brings
        # boto3 in (via ``[aws-secrets]`` or ``[s3]``), so mypy
        # emits ``import-untyped`` rather than ``import-not-found``.
        # A contributor running ``pip install -e .[dev]`` with no
        # extras would see ``import-not-found`` here instead — the
        # filesystem connector's matching shim handles that path
        # under ``import-not-found``; stage 9c's CI assumes the
        # extras land per the verify checklist.
        import boto3  # type: ignore[import-untyped]  # noqa: PLC0415 — lazy
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return boto3


def _lazy_import_botocore_exceptions() -> Any:
    """Import :mod:`botocore.exceptions` on first use.

    Returned separately because ``client.get_secret_value`` raises
    :class:`botocore.exceptions.ClientError`; we need that class to
    branch the error surface, and it lives in a different sub-package
    than the ``boto3`` client factory. Same install hint applies (the
    same extra brings both in — ``botocore`` is a transitive of
    ``boto3``).
    """
    try:
        # NOTE: same ``import-untyped`` ignore as the boto3 shim —
        # botocore has no py.typed marker either. Same caveat
        # about a base ``[dev]`` install — the verify checklist
        # installs the ``aws-secrets`` extra, which brings botocore
        # in transitively.
        from botocore import (  # type: ignore[import-untyped]  # noqa: PLC0415
            exceptions as botocore_exceptions,
        )
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(_INSTALL_HINT) from exc
    return botocore_exceptions


def _parse_path(path: str) -> tuple[str, str, str]:
    """Split ``<region>/<secret-id>[:<version-stage>]`` into the three components.

    Returns ``(region, secret_id, version_stage)``. Raises
    :class:`SecretResolutionError` for any malformed shape — empty
    region, empty secret-id, missing slash, or a region that does not
    match :data:`_VALID_REGION`. Runs BEFORE any SDK import so a typo
    in ``profiles.yml`` fails fast (mirrors the GCP resolver's
    ``_RESOURCE_NAME`` pre-check).

    # NOTE: only the FIRST ``/`` is treated as the region/secret
    # separator. Secret names in AWS may contain slashes (the console
    # uses ``/`` to render hierarchical paths) — splitting on every
    # slash would mangle them. ``us-east-1/team/db-creds`` parses to
    # region=``us-east-1``, secret-id=``team/db-creds``.

    # NOTE: the ``:`` that splits ``<secret-id>:<version-stage>`` is
    # only honored at the END of the secret-id, by splitting from the
    # right. AWS secret ARNs contain ``:`` (e.g.
    # ``arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf``)
    # so an unconditional split-on-first-``:`` would break ARN inputs.
    # The version-stage suffix is optional and tested separately below.
    """
    if "/" not in path:
        raise SecretResolutionError(
            f"AWS Secrets Manager path {path!r} is malformed; expected "
            f"<region>/<secret-id>[:<version-stage>] "
            f"(e.g. us-east-1/my-secret or us-east-1/my-secret:AWSPENDING)"
        )
    region, secret_part = path.split("/", 1)
    if not region:
        raise SecretResolutionError(
            f"AWS Secrets Manager path {path!r} has an empty region; expected "
            f"<region>/<secret-id>[:<version-stage>]"
        )
    if not _VALID_REGION.match(region):
        raise SecretResolutionError(
            f"AWS Secrets Manager path {path!r} has an invalid region "
            f"{region!r}; expected lowercase alphanumeric + dash "
            f"(e.g. us-east-1, eu-west-2)"
        )
    if not secret_part:
        raise SecretResolutionError(
            f"AWS Secrets Manager path {path!r} has an empty secret id; "
            f"expected <region>/<secret-id>[:<version-stage>]"
        )

    # Honor a ``:<version-stage>`` suffix. We discriminate on whether
    # the secret-id is an ARN: ARNs start with ``arn:`` and embed
    # multiple ``:`` separators that must stay intact. Non-ARN
    # secret-ids never contain ``:`` (AWS secret name charset is
    # ``[A-Za-z0-9/_+=.@-]``), so a trailing ``:<stage>`` on a non-ARN
    # is unambiguously a version stage suffix in ANY case (AWSCURRENT,
    # AWSPENDING, or a custom label the operator created).
    #
    # NOTE: a defensive earlier draft restricted version-stage detection
    # to UPPERCASE-only labels — that broke operators using
    # custom-cased staging labels (AWS permits the full label
    # charset). The arn-prefix discriminator below restores full
    # custom-label support without re-introducing the ARN-misparse
    # bug, since ARNs ALWAYS start with the literal ``arn:`` prefix.
    if secret_part.startswith("arn:"):
        # ARN — never strip a trailing colon-token; pass the whole
        # string to AWS verbatim.
        return region, secret_part, _DEFAULT_VERSION_STAGE
    if ":" in secret_part:
        candidate_id, _, candidate_stage = secret_part.rpartition(":")
        if candidate_id and candidate_stage:
            return region, candidate_id, candidate_stage
    return region, secret_part, _DEFAULT_VERSION_STAGE


class AwsSecretsManagerResolver:
    """Resolver for ``secret://aws-secrets-manager/<region>/<secret-id>[:<stage>][#<field>]``.

    Implements the :class:`~detx.secrets.SecretResolver` Protocol. The
    ``path`` argument to :meth:`resolve` is the URL path with the scheme
    stripped — see the module docstring for the shape. The optional
    ``field`` argument is honored only when the resolved
    ``SecretString`` is valid JSON (the common idiom for storing
    multiple credentials under one secret name).

    Lazy instantiation contract (stage 9a):

    * ``__init__`` does NOT import the SDK or build any client. The
      resolver instance is built by stage 9a's
      :func:`~detx.secrets.resolvers._get_resolver` on first URL referencing
      this scheme; the SDK client is built inside :meth:`resolve` on
      first ``.resolve()`` call and cached on ``self._clients[region]``.

    * The cache is per-region: different regions in the same run get
      separate clients (a boto3 client is region-pinned). Same region
      reuses.

    Errors:

    * Malformed path (no slash, empty region, invalid region characters,
      empty secret-id) → :class:`~detx.secrets.SecretResolutionError`
      with the validation message — fired BEFORE SDK import.
    * ``ClientError`` with ``Code=ResourceNotFoundException`` →
      ``"secret X not found"``.
    * ``ClientError`` with ``Code=AccessDeniedException`` →
      ``"permission denied accessing secret X"``.
    * Any other ``ClientError`` → wrapped; only the class name is
      surfaced (the SDK's message body MAY echo metadata that brushes
      the secret name or policy text, mirror GCP's caution).
    * Binary secret (``SecretBinary`` present, ``SecretString`` absent)
      → ``"binary secrets not supported in v1"``.
    * ``#field`` requested but ``SecretString`` is not JSON → clear
      error naming both the secret id and the missing-field condition.
    * ``#field`` requested but the parsed JSON does not contain that
      key → ``"field X not in JSON payload of secret Y"``.
    """

    scheme: ClassVar[str] = "aws-secrets-manager"

    def __init__(self) -> None:
        """Build the resolver shell — NO SDK import, NO client construction.

        The SDK client is built lazily inside :meth:`resolve` on first
        call; until then, the resolver is a free object. Clients are
        cached per-region on ``self._clients``.
        """
        # Per-region client cache, populated by :meth:`_build_client`.
        # Typed :class:`typing.Any` because the SDK client type is only
        # available when the optional extra is installed.
        self._clients: dict[str, Any] = {}

    def _build_client(self, region: str) -> Any:
        """Construct + cache a boto3 secretsmanager client for ``region``.

        Re-uses an existing client if one was built for this region in
        the current process. boto3 clients themselves are not thread-
        safe by guarantee; the engine resolves secrets sequentially per
        run, so single-thread reuse is the actual use pattern.
        """
        existing = self._clients.get(region)
        if existing is not None:
            return existing
        boto3 = _lazy_import_boto3()
        # NOTE: ``boto3.client("secretsmanager", region_name=region)``
        # opens no network connection on construction — the underlying
        # botocore session is initialized but the HTTPS pool is lazy.
        # Cheap to call under the engine's discovery + dry-run paths.
        client = boto3.client("secretsmanager", region_name=region)
        self._clients[region] = client
        return client

    def resolve(self, path: str, field: str | None) -> str:
        """Resolve one ``secret://aws-secrets-manager/<path>[#<field>]`` reference.

        Args:
            path: the URL path with the scheme stripped — shape
                ``<region>/<secret-id>[:<version-stage>]``. The
                version stage defaults to ``AWSCURRENT`` when absent.
            field: the URL's ``#<field>`` suffix, or ``None`` if absent.
                When present, the ``SecretString`` is parsed as JSON
                and ``data[field]`` is returned — the idiomatic AWS
                pattern for storing structured credentials.

        Returns:
            The secret's string payload (the full ``SecretString``
            when ``field`` is None, otherwise ``data[field]`` from the
            parsed JSON).

        Raises:
            :class:`~detx.secrets.SecretResolutionError` — see the class
            docstring's error table.
        """
        # Validate the path shape FIRST — a bad path should fail before
        # paying the SDK import cost, so a typo in profiles.yml fails fast.
        region, secret_id, version_stage = _parse_path(path)

        client = self._build_client(region)
        botocore_exceptions = _lazy_import_botocore_exceptions()

        try:
            response = client.get_secret_value(
                SecretId=secret_id, VersionStage=version_stage
            )
        except botocore_exceptions.ClientError as exc:
            # ClientError carries a structured ``response`` dict with the
            # AWS error code under ``Error.Code``. We branch on a small
            # set of well-known codes; everything else surfaces with
            # just the class name (mirroring the GCP resolver's caution
            # about not inlining exotic server error messages that
            # could echo metadata).
            err_obj = getattr(exc, "response", None) or {}
            err_section = err_obj.get("Error", {}) if isinstance(err_obj, dict) else {}
            code = err_section.get("Code", "") if isinstance(err_section, dict) else ""
            if code == "ResourceNotFoundException":
                raise SecretResolutionError(
                    f"AWS secret {secret_id!r} not found in region {region!r} "
                    f"(version stage {version_stage!r}); verify the secret "
                    f"exists and the version is current"
                ) from exc
            if code == "AccessDeniedException":
                raise SecretResolutionError(
                    f"permission denied accessing AWS secret {secret_id!r} "
                    f"in region {region!r}; verify the IAM principal has "
                    f"secretsmanager:GetSecretValue on this secret's ARN"
                ) from exc
            # Catch-all for every other ClientError — class name only,
            # no SDK message body inlined. ``__cause__`` carries full
            # detail for tracebacks.
            raise SecretResolutionError(
                f"AWS Secrets Manager error resolving secret {secret_id!r} "
                f"in region {region!r}: {type(exc).__name__}"
            ) from exc

        # The response has either ``SecretString`` (UTF-8 text) or
        # ``SecretBinary`` (raw bytes). v1 only supports the text path.
        secret_string = response.get("SecretString")
        if secret_string is None:
            if "SecretBinary" in response:
                raise SecretResolutionError(
                    f"AWS secret {secret_id!r} in region {region!r} contains "
                    f"binary data; binary secrets not supported in v1 (use "
                    f"a string secret or a v2 release that supports binary)"
                )
            # No SecretString and no SecretBinary — shape violation.
            raise SecretResolutionError(
                f"AWS secret {secret_id!r} in region {region!r} returned "
                f"no SecretString or SecretBinary payload"
            )

        if field is None:
            # The whole opaque blob is the value.
            return str(secret_string)

        # ``#field`` was requested — parse as JSON and extract the key.
        try:
            parsed = json.loads(secret_string)
        except json.JSONDecodeError as exc:
            # Don't include the parser's character offset; it may
            # implicitly reveal payload structure. The class name and a
            # generic "not JSON" line are enough — the chained
            # ``__cause__`` carries the parser detail for tracebacks.
            raise SecretResolutionError(
                f"AWS secret {secret_id!r} in region {region!r} is not JSON; "
                f"cannot extract field {field!r}"
            ) from exc
        if not isinstance(parsed, dict):
            raise SecretResolutionError(
                f"AWS secret {secret_id!r} in region {region!r} is not a JSON "
                f"object; cannot extract field {field!r} (top-level type was "
                f"{type(parsed).__name__})"
            )
        if field not in parsed:
            raise SecretResolutionError(
                f"AWS secret {secret_id!r} in region {region!r}: field "
                f"{field!r} not in JSON payload"
            )
        value = parsed[field]
        if not isinstance(value, str):
            raise SecretResolutionError(
                f"AWS secret {secret_id!r} in region {region!r}: field "
                f"{field!r} is not a string (got type {type(value).__name__})"
            )
        return value
