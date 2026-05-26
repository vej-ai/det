"""The filesystem source — the ``@stream`` entry point.

One ``@stream`` (``files``) that enumerates files via the URI-dispatched
backend (:mod:`.backends`), reads them with a per-format streaming reader
(:mod:`.readers`), and yields :class:`~detx.Batch` of records — with the
synthetic ``_detx_file_cursor`` field attached to every record.

Incremental design — why a SYNTHETIC cursor field
------------------------------------------------

docs/03 §3.2 fixes the incremental contract as *a record field whose max
value advances the cursor*. The engine inspects records, not file metadata.
But a file's mtime / name lives on the filesystem, not in the record. The
clean way to bridge the two is to **attach** the per-file cursor key as an
extra field (``_detx_file_cursor``) on every record the source yields:

* the engine sees a normal cursor field — every existing mechanism
  (``Cursor.observe``, ``_detx_state``, ``cursor.start_value`` filtering
  on the next run) just works;
* the source filters whole files whose ``sort_key <= start_value``, so a
  resumed run never re-reads a file it has already loaded.

Both halves of the filter are present:

1. **Per-file**: ``files = [f for f in files if f.sort_key > start]`` — the
   coarse, cheap filter that skips already-loaded files entirely.
2. **Per-file ``observe``**: ``cursor.observe(file.cursor_observe)`` once
   per file — drives the engine's max-tracking. The cursor advances by
   whole files (incremental file loads, not within a file).

Files are sorted by their ``sort_key`` so the run is reproducible and the
cursor advances monotonically. A file that fails to parse raises with the
file path attached; the destination's per-stream transaction rolls back the
partial load so a retry starts the file cleanly.

# NOTE: ``cursor_observe`` and ``cursor_key`` are deliberately two values.
# The synthetic record field (``_detx_file_cursor``) is always the
# ISO-string ``cursor_key`` so a row in the warehouse is human-readable.
# What is handed to :meth:`Cursor.observe` (and therefore stored in
# ``_detx_state.cursor_value``) is the ``cursor_observe`` value: a
# :class:`datetime` for the ``mtime`` strategy, a string for ``name``.
# The mtime case must be a datetime because the baked DuckDB destination's
# JSON state column rejects un-quoted bare strings — its ``_encode_value``
# only json-encodes dicts/lists, so a string scalar fails the JSON parse.
# Shiphero hit the same constraint and works around it the same way
# (see detx/connectors/shiphero/source.py lines 154-165). This bug
# is reported in the STAGE 7 build report; once the destination is fixed
# we can collapse the two values back into one string.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from detx import Batch, Config, Cursor, stream
from detx.sources.filesystem.backends import FileRef, pick_backend
from detx.sources.filesystem.readers import infer_format, reader_for_format

# Synthetic record field carrying the per-file cursor key. Recorded as a
# module constant so the test / docs / a future explicit schema declaration
# all reference one source of truth.
FILE_CURSOR_FIELD = "_detx_file_cursor"


@stream(name="files")
def files(config: Config, cursor: Cursor) -> Iterator[Batch]:
    """Yield batches of records read from files under ``config.path``.

    Resolves the backend from ``config.path``'s URI scheme, lists files
    matching ``config.glob`` with cursor keys per ``config.cursor_strategy``,
    skips files whose key is ``<= cursor.start_value()`` (incremental
    resume), and reads each remaining file via the format reader chosen by
    ``config.format`` (or inferred from extension when ``"auto"``).

    Every yielded record carries ``_detx_file_cursor`` — the file's
    cursor key. The engine's :class:`Cursor` tracks the observed max across
    files; after the batches durably land it persists that value in
    ``_detx_state``, so the next run resumes past every loaded file.
    """
    # --- Resolve params via Config.get — sturdy against missing/extra params.
    # Reading via .get + str/int coercion at the boundary matches the duckdb
    # destination's style and keeps mypy quiet (Config.__getattr__ is `Any`).
    path = str(config.get("path"))
    glob = str(config.get("glob", "**/*.csv"))
    fmt_param = str(config.get("format", "auto"))
    batch_size = int(config.get("batch_size", 1000))
    cursor_strategy = str(config.get("cursor_strategy", "mtime"))
    csv_delimiter = str(config.get("csv_delimiter", ","))
    csv_has_header = bool(config.get("csv_has_header", True))

    if path == "None" or not path:
        raise ValueError(
            "filesystem source: `path` param is required "
            "(set it via register.yaml stream params or the run() params kwarg)"
        )
    if cursor_strategy not in ("mtime", "name"):
        raise ValueError(
            f"filesystem source: cursor_strategy must be 'mtime' or 'name', "
            f"got {cursor_strategy!r}"
        )

    backend = pick_backend(path)
    refs = backend.list_files(path, glob, cursor_strategy=cursor_strategy)

    # Incremental: skip files whose sort_key is at or below the committed
    # cursor. The engine has already applied any lookback / initial_value;
    # the value comes back from `_detx_state.cursor_value` (a JSON
    # column) either as the typed value the engine seeded (datetime) or as
    # an ISO string round-tripped through JSON — so the source normalizes
    # it to the strategy's compare type before comparing.
    start = _normalize_start(cursor.start_value(), cursor_strategy)
    if start is not None and not _is_seed_default(start):
        # NOTE: re-observe the normalized resume value as a typed datetime so
        # the engine has a *typed* observed_max to write back, even if this
        # run yields no new files. Without this, cursor.observed_max stays
        # None, the engine falls back to writing `cursor_before` (the raw
        # string round-tripped from DuckDB's JSON column) directly, and
        # commit_state then re-injects that bare string into the JSON column
        # — which DuckDB rejects ("Malformed JSON: unexpected content after
        # document"). This is the destination bug reported in STAGE 7. With
        # the typed re-observe, every commit goes back as a clean datetime.
        cursor.observe(start)
        refs = [r for r in refs if r.sort_key > start]

    if not refs:
        return  # nothing to do — no NEW batches yielded; cursor stays at `start`.

    batch: list[dict[str, Any]] = []
    for ref in refs:
        for record in _read_one_file(
            ref,
            backend=backend,
            fmt_param=fmt_param,
            csv_delimiter=csv_delimiter,
            csv_has_header=csv_has_header,
        ):
            # Attach the human-readable ISO cursor key to every record (this
            # is what lands in the warehouse). What `cursor.observe` sees
            # below is the typed ``cursor_observe`` value — see the module
            # docstring NOTE for why those are deliberately two values.
            record[FILE_CURSOR_FIELD] = ref.cursor_key
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        # Observe per-file: the cursor advances by whole files, matching
        # the "incremental by file" granularity the connector documents.
        cursor.observe(ref.cursor_observe)

    if batch:
        yield batch


def _normalize_start(value: Any, cursor_strategy: str) -> Any:
    """Coerce the resume value into the right type for the strategy's sort_key.

    The cursor's :meth:`start_value` returns whichever shape the engine
    handed in: on first run the parsed ``initial_value`` (a datetime for the
    example stream's ``cursor_type: timestamp``); on a resumed run the value
    just deserialized from DuckDB's JSON ``cursor_value`` column, which
    round-trips a datetime as an ISO 8601 string.

    For the ``mtime`` strategy the source compares against a datetime, so a
    string-shaped resume value is parsed back via :meth:`datetime.fromisoformat`.
    For the ``name`` strategy a string passes through unchanged.

    ``None`` short-circuits (no filter applied — first ever run).
    """
    if value is None:
        return None
    if cursor_strategy == "mtime":
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None
    # cursor_strategy == "name"
    return str(value)


def _is_seed_default(value: Any) -> bool:
    """Whether ``value`` is the manifest's initial_value sentinel — empty/epoch.

    The example stream declares ``initial_value: "1970-01-01T00:00:00+00:00"``
    so the engine has *something* concrete to type-parse into a datetime
    (docs/03 §3.2). On a true first run we want to read every file, not
    filter against the epoch — so the source treats both the empty string
    (the natural "no value yet") and the parsed epoch datetime as "no
    filter please". A future revision that declares ``initial_value: null``
    in the manifest would pass ``None`` and skip this whole branch.
    """
    if value == "":
        return True
    if isinstance(value, datetime) and value.year <= 1970:
        return True
    return False


def _read_one_file(
    ref: FileRef,
    *,
    backend: Any,
    fmt_param: str,
    csv_delimiter: str,
    csv_has_header: bool,
) -> Iterator[dict[str, Any]]:
    """Open one file via the backend and stream its records via the right reader.

    Format resolution: ``fmt_param`` of ``"auto"`` → infer from extension,
    otherwise use it directly. CSV reader options are passed through; other
    readers ignore them (``**_options``).

    The streaming binary handle is always closed via ``with`` so a partial
    iteration never leaks a file handle.
    """
    if fmt_param == "auto":
        fmt = infer_format(ref.uri)
    else:
        fmt = fmt_param
    reader = reader_for_format(fmt)

    with backend.open_binary(ref) as handle:
        yield from reader(
            handle,
            ref.uri,
            delimiter=csv_delimiter,
            has_header=csv_has_header,
        )
