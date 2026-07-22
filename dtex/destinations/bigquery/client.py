# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""BigQuery + GCS client wrapper for the BigQuery destination — docs/05 §2.

Lifts every SDK import into *lazy* accessors so the rest of dtex
(``import dtex``, ``dtex list``, the DuckDB destination, every test that does
not touch BigQuery) does not pay the cost of importing
``google-cloud-bigquery`` / ``google-cloud-storage`` / ``pyarrow``. A base
install without ``[bigquery]`` stays importable; the SDK imports happen only
inside the hooks that actually need them.

The lazy accessors are also the unit-test injection seam: a test substitutes
fakes for :func:`_bigquery_module` / :func:`_storage_module` /
:func:`_pyarrow_modules` via ``monkeypatch.setattr`` on this module, and
every hook then sees the fake. One swap point, total coverage.

# NOTE: with the ``[bigquery]`` extra not installed, mypy does not have stubs
# for ``google.cloud.bigquery`` / ``google.cloud.storage`` and would reject
# the type annotations. We therefore type the SDK-facing seams as ``Any``;
# the function signatures are still fully annotated (``disallow_untyped_defs``
# stays satisfied). The runtime behavior is unchanged either way.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# google-cloud-bigquery + google-cloud-storage are in dtex's base dependencies
# (the BigQuery destination is a baked connector). Reaching this error means
# the SDK was removed from the active environment after install — reinstall
# with `pip install dtex` (or `pip install --force-reinstall google-cloud-bigquery
# google-cloud-storage`).
_INSTALL_HINT = (
    "BigQuery destination requires google-cloud-bigquery + google-cloud-storage "
    "(both ship with `pip install dtex`). If you see this, the SDK was removed "
    "from your environment — reinstall with `pip install dtex`."
)


# --------------------------------------------------------------------------
# Lazy SDK accessors — the single injection seam for unit tests
# --------------------------------------------------------------------------


def _bigquery_module() -> Any:
    """Return the ``google.cloud.bigquery`` module (lazy import).

    Imported the first time a BigQuery hook needs the SDK — so an ``import
    dtex`` without ``[bigquery]`` installed does not crash. A missing SDK
    raises a clear :class:`ImportError` pointing at the install command,
    rather than the bare ``ModuleNotFoundError`` the unguarded import would
    surface.
    """
    try:
        from google.cloud import bigquery  # noqa: PLC0415 — lazy by design
    except ImportError as exc:  # pragma: no cover — happy path imports cleanly
        raise ImportError(_INSTALL_HINT) from exc
    return bigquery


def _storage_module() -> Any:
    """Return the ``google.cloud.storage`` module (lazy import)."""
    try:
        # NOTE: mypy can't resolve ``google.cloud.storage`` as an attribute
        # of the namespace package even with the ``[bigquery]`` extra
        # installed — the package ships no ``py.typed`` for the namespace.
        # The actual import works at runtime; ``Any`` is the honest type.
        from google.cloud import storage  # type: ignore[attr-defined]  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover — happy path imports cleanly
        raise ImportError(_INSTALL_HINT) from exc
    return storage


def _pyarrow_modules() -> tuple[Any, Any]:
    """Return the ``(pyarrow, pyarrow.parquet)`` modules (lazy import).

    ``pyarrow`` powers the Parquet serialization used to stage one batch
    before the LOAD job; it is in the ``[bigquery]`` extra. Returned as a
    tuple so call-sites get both at once.
    """
    try:
        import pyarrow as pa  # noqa: PLC0415 — lazy by design
        import pyarrow.parquet as pq  # noqa: PLC0415 — lazy by design
    except ImportError as exc:  # pragma: no cover — happy path imports cleanly
        raise ImportError(_INSTALL_HINT) from exc
    return pa, pq


def _service_account_credentials(path: str) -> Any:
    """Build service-account credentials from a JSON key file (lazy import).

    Credentials are *referenced* by path on disk — dtex never logs the path's
    contents or the credentials object. The path itself may be useful in an
    error message ("file not found"), so it is allowed to surface from the
    SDK's own exception.
    """
    try:
        from google.oauth2 import service_account  # noqa: PLC0415 — lazy by design
    except ImportError as exc:  # pragma: no cover — happy path imports cleanly
        raise ImportError(_INSTALL_HINT) from exc
    return service_account.Credentials.from_service_account_file(path)


# --------------------------------------------------------------------------
# Retry policy — surfaces transient BigQuery failures uniformly
# --------------------------------------------------------------------------


# HTTP status codes the BigQuery job APIs return on transient problems.
# 429 = rate-limit, 500/502/503/504 = server-side blips. A 4xx that is not 429
# is a real authoring error (bad SQL, missing perms) and is NOT retried — a
# failure surfaces immediately so the user gets the real error message.
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def _status_code(exc: BaseException) -> int | None:
    """Pull an HTTP status code off a Google API exception, if it carries one.

    google-api-core exceptions expose ``.code`` (an int); some lower-level
    wrappers expose ``.response.status_code``. Anything without a numeric
    status is treated as non-retryable (return ``None``).
    """
    for attr in ("code", "status_code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    if isinstance(code, int):
        return code
    return None


def _is_retryable_network_error(exc: BaseException) -> bool:
    """Whether ``exc`` is a transient network-layer failure worth retrying.

    Long-running runs (a backfill against a high-row-count source like
    RevenueCat that involves hundreds of sequential LOAD jobs) routinely
    hit stale-TCP-socket failures: a keep-alive connection held open by
    the google-cloud-bigquery client gets killed by an intermediary,
    and the next reuse raises a ``requests.exceptions.ConnectionError``
    (typically wrapping ``http.client.RemoteDisconnected``). These have
    NO HTTP status code (the connection died before any response
    arrived), so the status-code path in :func:`run_with_retries` would
    re-raise them immediately. This helper recognises them so the
    retry loop covers them.

    Genuine programming errors (``KeyError``, ``TypeError``, etc.) are
    NOT matched — they should surface immediately, not get masked under
    a few backoff cycles. The match is by concrete network-exception
    type, not "any statusless exception."
    """
    # requests-level connection/timeout failures (ConnectionError /
    # Timeout / ConnectTimeout / ReadTimeout / ChunkedEncodingError /
    # ProxyError / SSLError — all RequestException subclasses).
    import requests  # noqa: PLC0415 — narrow runtime import

    if isinstance(
        exc,
        (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
    ):
        return True

    # google-api-core wraps some of the above under its own
    # ServiceUnavailable / RetryError. These usually DO carry a 503
    # status code so the existing path catches them, but RetryError
    # (raised when google's own internal retry exhausts) sometimes
    # surfaces without one.
    try:
        from google.api_core import exceptions as gapic_exc  # noqa: PLC0415

        if isinstance(exc, gapic_exc.RetryError):
            return True
    except ImportError:  # pragma: no cover — google-api-core is installed
        pass

    return False


def run_with_retries(
    operation: Any,
    *,
    max_attempts: int,
    backoff_seconds: float,
    sleep: Any = time.sleep,
) -> Any:
    """Run ``operation()`` with exponential backoff on transient failures.

    ``operation`` is a zero-arg callable — typically a ``lambda: job.result(
    timeout=...)`` or a ``lambda: client.load_table_from_uri(...).result(
    timeout=...)``. A retryable failure (status in :data:`_RETRYABLE_STATUS`
    OR a connection-class network error matched by
    :func:`_is_retryable_network_error`) sleeps ``backoff_seconds * 2**attempt``
    and retries; a non-retryable failure re-raises immediately. The
    ``sleep`` parameter is injectable so unit tests can avoid real
    wall-clock delays.
    """
    if max_attempts < 1:
        max_attempts = 1
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 — re-raises non-retryable below
            status = _status_code(exc)
            retryable_by_status = status is not None and status in _RETRYABLE_STATUS
            retryable_by_network = _is_retryable_network_error(exc)
            if not (retryable_by_status or retryable_by_network):
                raise
            last_exc = exc
            if attempt == max_attempts - 1:
                raise
            delay = backoff_seconds * (2 ** attempt)
            reason = (
                f"status={status}"
                if retryable_by_status
                else f"network={type(exc).__name__}"
            )
            logger.warning(
                "BigQuery transient error (%s, attempt=%d/%d): %s — "
                "retrying in %.1fs",
                reason,
                attempt + 1,
                max_attempts,
                exc,
                delay,
            )
            sleep(delay)
    # Unreachable — the loop either returns or raises.
    assert last_exc is not None  # pragma: no cover
    raise last_exc  # pragma: no cover


# --------------------------------------------------------------------------
# BigQueryClient — the live BQ + GCS handle pair
# --------------------------------------------------------------------------


@dataclass
class BigQueryClient:
    """The pair of live clients the BigQuery destination drives — docs/05 §2.

    Carries the configured BQ + GCS clients alongside the resolved routing
    knobs (project / dataset / location / staging bucket / staging prefix).
    A per-run unique ``run_suffix`` keeps two concurrent runs from colliding
    on a GCS object path or a MERGE staging table.

    The SDK-typed fields are annotated as :class:`Any` — see the module NOTE.
    The two clients are built by :func:`build_client` from a :class:`~dtex.types.Config`.
    """

    bq: Any
    gcs: Any
    project: str
    dataset: str
    location: str
    staging_bucket: str
    staging_prefix: str
    job_timeout_seconds: int
    retry_max_attempts: int
    retry_backoff_seconds: float
    run_suffix: str

    def staging_uri(self, table: str, batch_uuid: str) -> str:
        """Return the ``gs://...`` URI for one batch's Parquet object.

        The path is ``<prefix>/<run_suffix>/<table>/batch-<uuid>.parquet`` —
        the run suffix isolates concurrent runs; the per-batch uuid isolates
        sibling batches; the table folder makes the object easy to track in
        the GCS console without leaking record content into the URI.
        """
        return (
            f"gs://{self.staging_bucket}/"
            f"{self.staging_prefix.strip('/')}/{self.run_suffix}/{table}/"
            f"batch-{batch_uuid}.parquet"
        )

    def staging_blob_name(self, table: str, batch_uuid: str) -> str:
        """The GCS object name (path within the bucket) for one batch's Parquet."""
        return (
            f"{self.staging_prefix.strip('/')}/{self.run_suffix}/{table}/"
            f"batch-{batch_uuid}.parquet"
        )


@dataclass
class BQConn:
    """The handle passed between ``@destination`` hooks for one run — see DuckConn.

    docs/05 §1 fixes ``write_batch(conn, batch, stream)`` — the signature
    carries no per-run scratch space. But ``replace`` needs exactly that:
    "truncate the table, then load" means *truncate once per run on the
    first batch*, then plain-append the rest. Returning the raw BigQuery
    client from ``open`` would leave nowhere to record "this table was
    already truncated this run".

    So ``open`` returns this wrapper instead. It carries:

    * :attr:`client` — the live :class:`BigQueryClient` pair;
    * :attr:`replace_truncated` — the set of tables already truncated this
      run, so a ``replace`` stream truncates exactly once however many
      batches it yields;
    * :attr:`state_table_ready` — whether ``_dtex_state`` has been created
      this run, so the creation runs at most once;
    * :attr:`runs_table_ready` — same flag for ``_dtex_runs`` (docs/09 §4).
    """

    client: BigQueryClient
    replace_truncated: set[str] = field(default_factory=set)
    state_table_ready: bool = False
    runs_table_ready: bool = False
    lease_table_ready: bool = False
    # Guards the mutable per-run scratch above (the ``*_ready`` create-once
    # flags and the ``replace_truncated`` set) when the engine runs streams
    # concurrently (`dtex run -p … --threads N`). BigQuery's own client is
    # thread-safe and every load/query is an independent job, so the ONLY
    # shared mutable state is this bookkeeping; guarding it here keeps the
    # "create the _dtex_* table at most once" and "truncate a replace target
    # at most once per run" invariants under concurrent first-callers.
    lock: threading.Lock = field(default_factory=threading.Lock)


# --------------------------------------------------------------------------
# Client construction — from a dtex Config to a live BigQueryClient
# --------------------------------------------------------------------------


def build_client(
    *,
    project: str,
    dataset: str,
    location: str,
    staging_bucket: str,
    staging_prefix: str,
    credentials_path: str,
    job_timeout_seconds: int,
    retry_max_attempts: int,
    retry_backoff_seconds: float,
) -> BigQueryClient:
    """Build the live BQ + GCS client pair from resolved destination params.

    ``credentials_path`` empty (the default) ⇒ use Application Default
    Credentials (the ``GOOGLE_APPLICATION_CREDENTIALS`` env var, or a
    ``gcloud auth application-default login`` cache). A non-empty value
    loads service-account JSON from disk via
    :func:`_service_account_credentials`. Credentials never appear in log
    output; the path may, since "file not found" is a legitimate operator
    error.

    The clients are constructed lazily (BigQuery's ``Client.__init__`` does
    not perform a network round-trip), so this is safe to call even when
    the destination is being introspected without intent to load.
    """
    bq_mod = _bigquery_module()
    storage_mod = _storage_module()

    credentials: Any | None
    if credentials_path:
        credentials = _service_account_credentials(credentials_path)
    else:
        credentials = None  # ADC

    bq_client = bq_mod.Client(
        project=project, credentials=credentials, location=location
    )
    gcs_client = storage_mod.Client(project=project, credentials=credentials)

    return BigQueryClient(
        bq=bq_client,
        gcs=gcs_client,
        project=project,
        dataset=dataset,
        location=location,
        staging_bucket=staging_bucket,
        staging_prefix=staging_prefix,
        job_timeout_seconds=job_timeout_seconds,
        retry_max_attempts=retry_max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        run_suffix=uuid.uuid4().hex[:12],
    )
