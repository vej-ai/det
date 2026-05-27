# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""URI-scheme dispatched read backends — local / GCS / S3.

Each backend exposes two operations:

* :meth:`Backend.list_files` — enumerate :class:`FileRef` objects under a
  prefix + glob. Each ``FileRef`` carries the URI **and** its precomputed
  ``cursor_key`` (the per-file value the cursor will advance over), so the
  source layer never branches on scheme to compute ``mtime`` vs S3
  ``LastModified``.
* :meth:`Backend.open_binary` — return a streaming binary handle for one file.
  Readers (CSV / JSONL / Parquet) consume that handle directly, so the source
  never loads a whole file into memory.

:func:`pick_backend` dispatches on the URI scheme:

* no scheme / ``file://`` → :class:`LocalBackend` (always available);
* ``gs://`` → :class:`GcsBackend` (lazy ``google-cloud-storage`` import);
* ``s3://`` → :class:`S3Backend` (lazy ``boto3`` import).

GCS / S3 are lazy-imported so the local-only path has zero new runtime deps.
A missing optional package raises an :class:`ImportError` naming the install
extra (``dtex[gcs]`` / ``dtex[s3]``) so the fix is one line.

# NOTE: GCS / S3 authentication uses ADC (application default credentials)
# via the underlying SDK — no per-call credential plumbing. The decision to
# defer explicit secret declarations to a v2 is recorded in
# ``register.yaml``'s top-comment.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any, Protocol
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# Public data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FileRef:
    """One enumerated file — URI plus its precomputed cursor data.

    Three cursor-related fields:

    * :attr:`cursor_key` — a human-readable string the source attaches to
      every record as the synthetic ``_dtex_file_cursor`` field. For
      ``mtime`` strategy a UTC ISO 8601 timestamp; for ``name`` the
      basename. This is what shows up in the warehouse.
    * :attr:`cursor_observe` — the typed value the source hands to
      :meth:`Cursor.observe`. For ``mtime`` a tz-aware UTC :class:`datetime`
      (DuckDB's JSON state column binds it cleanly); for ``name`` the
      basename string (matches the declared ``cursor_type: timestamp``
      contract only for ``mtime`` — see ``register.yaml`` NOTE).
    * :attr:`sort_key` — used only to sort enumerated files into a stable
      run order. Same type as ``cursor_observe``.

    ``size`` is informational only — surfaced in log lines, never used to
    drive the read.
    """

    uri: str
    cursor_key: str
    cursor_observe: Any
    sort_key: Any
    size: int = 0


class Backend(Protocol):
    """The two-operation contract every URI-scheme backend implements."""

    def list_files(
        self, prefix: str, pattern: str, *, cursor_strategy: str
    ) -> list[FileRef]:
        """Enumerate files under ``prefix`` matching ``pattern``.

        ``cursor_strategy`` selects how each :attr:`FileRef.cursor_key` is
        computed (``"mtime"`` or ``"name"``).
        """
        ...

    def open_binary(self, file_ref: FileRef) -> IO[bytes]:
        """Open one file for streaming binary read."""
        ...


# ---------------------------------------------------------------------------
# Cursor-key encoding
# ---------------------------------------------------------------------------


def _mtime_to_datetime(mtime: float) -> datetime:
    """Encode a POSIX mtime as a tz-aware UTC :class:`datetime`.

    Used as both the sort key and the value handed to ``Cursor.observe`` for
    the ``mtime`` strategy. A datetime (not its ISO string) is what gets
    observed so the DuckDB destination's JSON state column binds it
    cleanly — see ``register.yaml`` and the source module's NOTE on the
    `string` cursor-type bug. ``microsecond=0`` keeps the value terse;
    sub-second mtimes are not load-bearing for the cursor.
    """
    return (
        datetime.fromtimestamp(mtime, tz=UTC)
        .replace(microsecond=0)
    )


def _datetime_to_iso(dt: datetime) -> str:
    """ISO 8601 string form of a :class:`datetime` — the synthetic record field."""
    return dt.isoformat()


def _name_to_key(name: str) -> str:
    """Encode a file name as its lex-comparable cursor key — identity."""
    return name


# ---------------------------------------------------------------------------
# URI parsing + dispatch
# ---------------------------------------------------------------------------


def pick_backend(uri: str) -> Backend:
    """Resolve a URI to its backend — docs/03 §2 file URI dispatch.

    No scheme (or ``file://``) → :class:`LocalBackend`. ``gs://`` →
    :class:`GcsBackend`. ``s3://`` → :class:`S3Backend`. Anything else raises
    :class:`ValueError` listing the supported schemes — a typo like
    ``gcs://`` fails loudly here, never silently in the wrong backend.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()
    if scheme in ("", "file"):
        return LocalBackend()
    if scheme == "gs":
        return GcsBackend(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))
    if scheme == "s3":
        return S3Backend(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))
    raise ValueError(
        f"unsupported URI scheme {scheme!r} in {uri!r}; "
        f"supported schemes: file (or no scheme), gs, s3"
    )


# ---------------------------------------------------------------------------
# Local backend — always available, stdlib only
# ---------------------------------------------------------------------------


class LocalBackend:
    """Local-filesystem backend — uses :mod:`pathlib` and :func:`open`.

    No optional deps, always selected for a bare path or ``file://`` URI.
    """

    def list_files(
        self, prefix: str, pattern: str, *, cursor_strategy: str
    ) -> list[FileRef]:
        """Enumerate files under ``prefix`` matching ``pattern`` — deterministic.

        Uses :meth:`pathlib.Path.glob` (recursive ``**`` supported in
        3.11+). Symlinks pointing at files are followed; directories are
        skipped. Results are sorted by ``cursor_key`` so the source runs in
        a reproducible order and the cursor advances monotonically.
        """
        # `file://` URIs that reach here have already been parsed; the
        # caller passes the bare filesystem path as `prefix`.
        root = Path(_strip_file_scheme(prefix)).expanduser()
        refs: list[FileRef] = []
        if not root.is_dir():
            return refs
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            stat = path.stat()
            if cursor_strategy == "name":
                key = _name_to_key(path.name)
                observe: Any = key
                sort_key: Any = key
            else:  # "mtime" — the default
                dt = _mtime_to_datetime(stat.st_mtime)
                key = _datetime_to_iso(dt)
                observe = dt
                sort_key = dt
            refs.append(
                FileRef(
                    uri=str(path),
                    cursor_key=key,
                    cursor_observe=observe,
                    sort_key=sort_key,
                    size=stat.st_size,
                )
            )
        refs.sort(key=lambda r: (r.sort_key, r.uri))
        return refs

    def open_binary(self, file_ref: FileRef) -> IO[bytes]:
        """Open the file for streaming binary read."""
        return Path(file_ref.uri).open("rb")


def _strip_file_scheme(uri: str) -> str:
    """Convert a ``file://`` URI back to a bare filesystem path; identity otherwise."""
    if uri.startswith("file://"):
        return uri[len("file://") :]
    return uri


# ---------------------------------------------------------------------------
# GCS backend — lazy `google-cloud-storage`
# ---------------------------------------------------------------------------


def _lazy_import_gcs() -> Any:
    """Import :mod:`google.cloud.storage` on first use, or raise a clear ImportError.

    The error message names the install extra (``dtex[gcs]``) so a user
    hitting a ``gs://`` path without the extra installed has a one-line fix.
    """
    try:
        # NOTE: google-cloud-storage is a namespace package without py.typed,
        # so mypy reports `attr-defined` on `google.cloud.storage` even when
        # the package is installed. The ImportError branch handles the
        # genuinely-missing case at runtime.
        from google.cloud import storage as gcs_storage  # type: ignore[attr-defined]
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(
            "the filesystem connector needs `google-cloud-storage` to read gs:// URIs; "
            "install with `pip install dtex[gcs]`"
        ) from exc
    return gcs_storage


class GcsBackend:
    """Google Cloud Storage backend — uses :mod:`google.cloud.storage` (lazy)."""

    def __init__(self, bucket: str, prefix: str) -> None:
        self.bucket = bucket
        self.prefix = prefix

    def list_files(
        self, prefix: str, pattern: str, *, cursor_strategy: str
    ) -> list[FileRef]:
        """List objects under the bucket+prefix that match ``pattern``.

        ``prefix`` is the full ``gs://bucket/path`` URI; the GCS SDK's
        ``list_blobs(prefix=...)`` paginates by itself, so we hand it the
        object prefix and filter object names client-side via
        :func:`fnmatch.fnmatch` against ``pattern`` (the glob).
        """
        gcs_storage = _lazy_import_gcs()
        client = gcs_storage.Client()
        bucket = client.bucket(self.bucket)
        refs: list[FileRef] = []
        for blob in client.list_blobs(bucket, prefix=self.prefix):
            name = blob.name
            # Match the glob against the object path *relative to the
            # configured prefix* — the same shape the local backend matches
            # (a relative path under the directory root).
            rel = _strip_prefix(name, self.prefix)
            if not _glob_matches(rel, pattern):
                continue
            if cursor_strategy == "name":
                key = _name_to_key(name)
                observe: Any = key
                sort_key: Any = key
            else:
                mtime = blob.updated.timestamp() if blob.updated is not None else 0.0
                dt = _mtime_to_datetime(mtime)
                key = _datetime_to_iso(dt)
                observe = dt
                sort_key = dt
            uri = f"gs://{self.bucket}/{name}"
            refs.append(
                FileRef(
                    uri=uri,
                    cursor_key=key,
                    cursor_observe=observe,
                    sort_key=sort_key,
                    size=int(blob.size or 0),
                )
            )
        refs.sort(key=lambda r: (r.sort_key, r.uri))
        return refs

    def open_binary(self, file_ref: FileRef) -> IO[bytes]:
        """Open the object for streaming binary read.

        Returns the SDK's ``BlobReader`` — a streaming binary file-like
        handle (no whole-object download). The caller closes it.
        """
        gcs_storage = _lazy_import_gcs()
        client = gcs_storage.Client()
        bucket = client.bucket(self.bucket)
        # gs://<bucket>/<object>  →  object name is everything after the bucket.
        prefix = f"gs://{self.bucket}/"
        object_name = file_ref.uri[len(prefix) :]
        blob = bucket.blob(object_name)
        return blob.open("rb")


def _strip_prefix(name: str, prefix: str) -> str:
    """Return ``name`` relative to ``prefix`` — the path-suffix the glob matches.

    Mirrors :meth:`pathlib.Path.glob`'s semantics (which matches against paths
    relative to the directory it is called on). Leading ``"/"`` characters in
    the stripped suffix are removed so a trailing-slash and a no-trailing-slash
    prefix both behave the same.
    """
    if prefix and name.startswith(prefix):
        return name[len(prefix) :].lstrip("/")
    return name


def _glob_matches(name: str, pattern: str) -> bool:
    """Recursive-aware glob match — ``**`` matches any number of path segments.

    :mod:`fnmatch` is shell-flat (``*`` matches anything including ``/``); to
    make ``"**/foo"`` work like :meth:`pathlib.Path.glob`'s recursive form
    (zero-or-more segments + ``/foo``), we accept either the bare pattern
    *or* the pattern with the leading ``**/`` stripped. That covers the two
    cases authors actually write: ``"**/*.csv"`` matching a top-level
    ``a.csv`` and a nested ``sub/a.csv`` alike.
    """
    if fnmatch.fnmatch(name, pattern):
        return True
    if pattern.startswith("**/"):
        return fnmatch.fnmatch(name, pattern[len("**/") :])
    return False


# ---------------------------------------------------------------------------
# S3 backend — lazy `boto3`
# ---------------------------------------------------------------------------


def _lazy_import_boto3() -> Any:
    """Import :mod:`boto3` on first use, or raise a clear ImportError."""
    try:
        # NOTE: stage 9c added the ``aws-secrets`` extra and its
        # verify checklist installs ``[s3,aws-secrets]`` together, so
        # mypy sees boto3 as installed-but-unstubbed
        # (``import-untyped``) rather than missing
        # (``import-not-found``). Updated to match the current install
        # state.
        import boto3  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover — tested via monkeypatch.
        raise ImportError(
            "the filesystem connector needs `boto3` to read s3:// URIs; "
            "install with `pip install dtex[s3]`"
        ) from exc
    return boto3


class S3Backend:
    """Amazon S3 backend — uses :mod:`boto3` (lazy)."""

    def __init__(self, bucket: str, prefix: str) -> None:
        self.bucket = bucket
        self.prefix = prefix

    def list_files(
        self, prefix: str, pattern: str, *, cursor_strategy: str
    ) -> list[FileRef]:
        """List objects under the bucket+prefix that match ``pattern``."""
        boto3 = _lazy_import_boto3()
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        refs: list[FileRef] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents") or []:
                name = obj["Key"]
                rel = _strip_prefix(name, self.prefix)
                if not _glob_matches(rel, pattern):
                    continue
                if cursor_strategy == "name":
                    key = _name_to_key(name)
                    observe: Any = key
                    sort_key: Any = key
                else:
                    last_mod = obj.get("LastModified")
                    mtime = last_mod.timestamp() if last_mod is not None else 0.0
                    dt = _mtime_to_datetime(mtime)
                    key = _datetime_to_iso(dt)
                    observe = dt
                    sort_key = dt
                uri = f"s3://{self.bucket}/{name}"
                refs.append(
                    FileRef(
                        uri=uri,
                        cursor_key=key,
                        cursor_observe=observe,
                        sort_key=sort_key,
                        size=int(obj.get("Size") or 0),
                    )
                )
        refs.sort(key=lambda r: (r.sort_key, r.uri))
        return refs

    def open_binary(self, file_ref: FileRef) -> IO[bytes]:
        """Open the object for streaming binary read.

        Returns the body stream from ``get_object`` — a ``StreamingBody`` that
        the readers consume incrementally. The caller closes it.
        """
        boto3 = _lazy_import_boto3()
        s3 = boto3.client("s3")
        prefix = f"s3://{self.bucket}/"
        object_name = file_ref.uri[len(prefix) :]
        response = s3.get_object(Bucket=self.bucket, Key=object_name)
        return response["Body"]


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------


__all__ = [
    "Backend",
    "FileRef",
    "GcsBackend",
    "LocalBackend",
    "S3Backend",
    "pick_backend",
]


def __dir__() -> list[str]:
    return list(__all__)
