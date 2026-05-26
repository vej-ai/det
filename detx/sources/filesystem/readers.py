"""Per-format streaming readers — CSV / JSONL / Parquet.

Each reader takes a streaming binary handle (the kind a :class:`Backend`
returns) and yields plain ``dict`` records. Records are yielded one at a
time — none of these loads a whole file into memory — so the source layer
can batch and checkpoint freely (docs/03 §3.1 batching is the connector's
call).

Format dispatch lives in :func:`reader_for_format`:

* ``csv`` → :func:`read_csv` (stdlib ``csv.DictReader``)
* ``jsonl`` → :func:`read_jsonl` (one JSON object per line)
* ``parquet`` → :func:`read_parquet` (lazy ``pyarrow`` import; raises with the
  install-extra hint if pyarrow is missing)

A file that fails to parse raises with the file path attached, so the engine's
per-stream rollback (``TRANSACTIONAL_LOAD``) surfaces a useful error.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Callable, Iterator
from typing import IO, Any

# A reader takes (binary_handle, file_path_for_errors, **format_options) and
# yields one dict per record.
ReaderFunc = Callable[..., Iterator[dict[str, Any]]]


# ---------------------------------------------------------------------------
# Format dispatch
# ---------------------------------------------------------------------------


# Extension → canonical format name. The connector's `format: auto` param
# defaults to this map; the author can force a format by setting `format:` to
# any of the values.
EXTENSION_FORMATS: dict[str, str] = {
    ".csv": "csv",
    ".tsv": "csv",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".json": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
}


def infer_format(file_path: str) -> str:
    """Pick a format from a file path's extension; raise on an unknown extension.

    Called when the connector's ``format`` param is ``"auto"``. A misnamed
    file (``.dat``) fails here with a message listing the recognized
    extensions — better than silently routing it through the wrong reader.
    """
    lower = file_path.lower()
    for ext, fmt in EXTENSION_FORMATS.items():
        if lower.endswith(ext):
            return fmt
    valid = ", ".join(sorted(EXTENSION_FORMATS))
    raise ValueError(
        f"cannot infer format from extension of {file_path!r}; "
        f"recognized extensions: {valid}. "
        f"Set the `format` connector param to one of: csv, jsonl, parquet."
    )


def reader_for_format(fmt: str) -> ReaderFunc:
    """Return the reader function for a format name; raise on unknown.

    ``fmt`` is one of ``csv`` / ``jsonl`` / ``parquet``. Anything else is a
    typo or an unsupported format and raises ``ValueError`` listing the valid
    options.
    """
    if fmt == "csv":
        return read_csv
    if fmt == "jsonl":
        return read_jsonl
    if fmt == "parquet":
        return read_parquet
    raise ValueError(
        f"unknown format {fmt!r}; expected one of: csv, jsonl, parquet (or 'auto')"
    )


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def read_csv(
    handle: IO[bytes],
    file_path: str,
    *,
    delimiter: str = ",",
    has_header: bool = True,
) -> Iterator[dict[str, Any]]:
    """Stream CSV records from ``handle`` as dicts.

    Uses :class:`csv.DictReader` (a streaming reader — no whole-file load).
    With ``has_header=True`` the first row supplies the column names; without
    a header, columns are named ``col_0``, ``col_1``, ... so the record shape
    is well-defined either way.

    A parse error (malformed quoting, etc.) is re-raised as a ``ValueError``
    naming ``file_path`` — the engine's per-stream rollback can then surface
    *which* file broke the load.
    """
    # csv.reader / DictReader wants a text iterable. Wrap the binary handle in
    # a TextIOWrapper with utf-8 + replace so a stray byte does not abort the
    # whole file; the line-level parser still raises on structural errors.
    text = io.TextIOWrapper(handle, encoding="utf-8", errors="replace", newline="")
    try:
        if has_header:
            # strict=True makes the underlying _csv.reader raise on structural
            # errors (e.g. characters after a closing quote, lone quote in an
            # unquoted field) rather than silently coercing them. That is what
            # makes "malformed CSV raises a clear error naming the file" hold.
            reader = csv.DictReader(text, delimiter=delimiter, strict=True)
            try:
                for dict_row in reader:
                    yield dict(dict_row)
            except csv.Error as exc:
                raise ValueError(
                    f"failed to parse CSV file {file_path!r} at line "
                    f"{reader.line_num}: {exc}"
                ) from exc
        else:
            raw_reader = csv.reader(text, delimiter=delimiter, strict=True)
            line_num = 0
            try:
                for list_row in raw_reader:
                    line_num += 1
                    yield {f"col_{i}": value for i, value in enumerate(list_row)}
            except csv.Error as exc:
                raise ValueError(
                    f"failed to parse CSV file {file_path!r} at line "
                    f"{line_num}: {exc}"
                ) from exc
    finally:
        # Detach so closing the TextIOWrapper does not also close the binary
        # handle the caller opened — the caller owns the underlying handle.
        text.detach()


# ---------------------------------------------------------------------------
# JSONL
# ---------------------------------------------------------------------------


def read_jsonl(
    handle: IO[bytes],
    file_path: str,
    **_options: Any,
) -> Iterator[dict[str, Any]]:
    """Stream JSONL records — one JSON object per line.

    Blank lines are skipped (a common artifact at the end of a file).
    A non-object line (``[1, 2]``, a bare number) raises ``ValueError``
    naming the file and line number — the loader cannot turn a non-dict into
    a record.
    """
    text = io.TextIOWrapper(handle, encoding="utf-8", errors="replace")
    try:
        for line_num, raw in enumerate(text, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"failed to parse JSONL file {file_path!r} at line "
                    f"{line_num}: {exc}"
                ) from exc
            if not isinstance(value, dict):
                raise ValueError(
                    f"JSONL file {file_path!r} line {line_num} is a "
                    f"{type(value).__name__}, expected a JSON object"
                )
            yield value
    finally:
        text.detach()


# ---------------------------------------------------------------------------
# Parquet — lazy pyarrow import
# ---------------------------------------------------------------------------


def _lazy_import_pyarrow() -> Any:
    """Import :mod:`pyarrow.parquet` on first use, or raise a clear ImportError.

    pyarrow ships with detx's base dependencies (the BigQuery destination
    needs it). Reaching this error means the package was removed from the
    active environment — reinstall with ``pip install detx``.
    """
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ImportError(
            "the filesystem connector needs `pyarrow` to read .parquet files. "
            "pyarrow ships with `pip install detx`; if you see this, the package "
            "was removed from your environment — reinstall with `pip install detx`."
        ) from exc
    return pq


def read_parquet(
    handle: IO[bytes],
    file_path: str,
    **_options: Any,
) -> Iterator[dict[str, Any]]:
    """Stream Parquet records via :mod:`pyarrow` — row-group at a time.

    Iterates row groups (``ParquetFile.iter_batches``) so a large file is
    never loaded into memory whole. Each Arrow record batch is converted to
    a list of dicts via ``pylist`` and yielded one record at a time.

    Parquet parse errors (truncated file, wrong magic) raise with the file
    path attached, mirroring the CSV / JSONL behavior.
    """
    pq = _lazy_import_pyarrow()
    try:
        parquet_file = pq.ParquetFile(handle)
        for batch in parquet_file.iter_batches():
            for record in batch.to_pylist():
                # Arrow returns column-name → value dicts already.
                yield dict(record)
    except Exception as exc:
        # Catch pyarrow's own exceptions (ArrowInvalid, OSError on a bad
        # stream) and re-raise with the file path — the caller cannot
        # otherwise tell which file broke when batching many.
        if isinstance(exc, ImportError):
            raise
        raise ValueError(
            f"failed to read Parquet file {file_path!r}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Module exports
# ---------------------------------------------------------------------------


__all__ = [
    "EXTENSION_FORMATS",
    "ReaderFunc",
    "infer_format",
    "read_csv",
    "read_jsonl",
    "read_parquet",
    "reader_for_format",
]
