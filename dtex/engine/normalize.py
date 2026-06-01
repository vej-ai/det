# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The NORMALIZE step — per-batch, per-column value coercion to the declared schema.

This module is the value-coercion half of the engine's NORMALIZE stage
(docs/02 §The extract → normalize → load pipeline). The schema half (declared
vs inferred, strict vs evolve) lives in :mod:`dtex.engine.runner`
(``_check_strict_schema``, ``_infer_schema``); the runner calls
:func:`normalize_batch` after schema resolution and before each ``write_batch``
hook call.

Why a dedicated engine step:

* Connectors that pull from string-typed sources (CSV-backed REST endpoints,
  Stripe Sigma, ad-platform "always-string" CSV exports) need to yield strings
  and trust the engine to map them to the declared logical type. The BigQuery
  destination previously crashed with
  ``ArrowInvalid: Could not convert '1599' with type str: tried to convert to
  int64`` because the dicts arriving at ``write_batch`` were string-typed
  cells targeting INTEGER columns. The fix belongs in the engine — every
  connector benefits, the contract stays "yield dicts, declare types".
* Destinations no longer need per-destination coercion: by the time they see
  a batch the values are guaranteed to be the canonical Python type for each
  :class:`~dtex.types.FieldType`.

Coercion rules per :class:`~dtex.types.FieldType` are the authoritative
in-code expression of the docs/02 §Normalize table. Each rule:

* Passes through ``None`` unchanged — ``NULL`` means ``NULL``, never the
  empty string and never the type's zero value (an explicit ``0`` and a
  missing reading are different states; the engine has no business
  conflating them).
* Coerces the empty string ``""`` for non-STRING types to ``None`` — the
  CSV-style "no value" idiom. An explicit empty string is not a meaningful
  INTEGER / FLOAT / BOOLEAN / TIMESTAMP / DATE / BYTES; demoting it to
  ``None`` is what every spreadsheet and CSV loader does and what an
  operator expects.
* Accepts the canonical Python type for the FieldType verbatim (the
  zero-work fast path: any connector already yielding correctly-typed
  values pays at most one ``isinstance`` check per cell).
* Accepts a small fixed set of "obvious" alternate input shapes (str of
  digits → INTEGER, "true"/"false" → BOOLEAN, ISO-8601 → TIMESTAMP, …).
* Raises :class:`~dtex.types.CoercionError` on anything else, with a
  message naming column / value / source-type / target-FieldType.

# NOTE: split out of ``dtex/engine/runner.py`` once the per-FieldType
# coercion rules and the empty-string rule pushed the helper set past
# ~80 lines. The runner imports :func:`normalize_batch` (and re-exports
# :class:`~dtex.types.CoercionError` via the engine's ``__init__``) so
# callers see one engine surface; the split here is purely organizational.
"""

from __future__ import annotations

import base64
import binascii
from datetime import UTC, date, datetime
from typing import Any

from dtex.types import Batch, CoercionError, FieldType, Schema

__all__ = ["coerce_value", "normalize_batch"]


# ---------------------------------------------------------------------------
# Per-FieldType coercion rules
# ---------------------------------------------------------------------------


def coerce_value(value: Any, field_type: FieldType, *, column: str) -> Any:
    """Coerce ``value`` to the canonical Python representation of ``field_type``.

    The single dispatch point — every record-cell value travels through here.
    ``column`` is threaded in solely so a :class:`~dtex.types.CoercionError`
    raised below names the offending column without the caller having to
    re-wrap the exception.

    See module docstring for the full coercion table; the rules below mirror
    it 1:1. ``None`` and empty-string-for-non-STRING-types are short-
    circuited at the top so every per-type branch can assume a non-empty,
    non-None input.

    # NOTE: dispatch is a flat ``if`` ladder rather than a dict lookup of
    # per-type callables. The ladder is hot-path code (called once per
    # cell per batch, billions of cells per warehouse-scale run) — Python
    # ``isinstance`` + direct branch is faster than a dict.get + indirect
    # call, and the call graph stays inspectable in a traceback.
    """
    # NULL → NULL: missing-data semantics; never invent a zero value.
    if value is None:
        return None

    # Empty string for non-STRING types → NULL: the CSV-style "no value"
    # idiom. STRING keeps "" verbatim (the only type where the empty string
    # is a legitimate, distinguishable value).
    if field_type is not FieldType.STRING and value == "":
        return None

    if field_type is FieldType.STRING:
        return _to_string(value)
    if field_type is FieldType.INTEGER:
        return _to_integer(value, column=column)
    if field_type is FieldType.FLOAT:
        return _to_float(value, column=column)
    if field_type is FieldType.BOOLEAN:
        return _to_boolean(value, column=column)
    if field_type is FieldType.TIMESTAMP:
        return _to_timestamp(value, column=column)
    if field_type is FieldType.DATE:
        return _to_date(value, column=column)
    if field_type is FieldType.JSON:
        # JSON is intentionally permissive — the destination's
        # ``_encode_json_column`` (DuckDB) / ``_encode_cell`` (BigQuery)
        # handles dict / list / str / scalar uniformly. Coercion would
        # constrain a column whose whole point is to carry arbitrary
        # JSON-serializable shapes.
        return value
    if field_type is FieldType.BYTES:
        return _to_bytes(value, column=column)
    # The enum is closed; an unknown member would be a programmer error,
    # not user data. Defensive fallthrough.
    raise CoercionError(  # pragma: no cover — unreachable while FieldType is closed
        column=column,
        value=value,
        source_type=type(value),
        target_type=field_type,
    )


def _to_string(value: Any) -> str:
    """STRING: any value → ``str``.

    A pre-existing ``str`` short-circuits (no work). Anything else is
    stringified — STRING is the catch-all type and explicitly accepts
    "anything (``str()`` it)" per the docs/02 table.
    """
    if isinstance(value, str):
        return value
    return str(value)


def _to_integer(value: Any, *, column: str) -> int:
    """INTEGER: int / digit-string / .0 float → ``int``.

    The bool-vs-int isinstance trap: ``isinstance(True, int)`` is ``True``
    in Python, so the bool check MUST come first — a ``True`` arriving at
    an INTEGER column is a coercion failure (the source declared the type
    wrong), NOT a silent demotion to ``1``. A connector that genuinely
    means "the boolean True is the value 1 in this column" can yield
    ``int(True)`` upstream.

    Floats are accepted only when the fractional part is exactly zero
    (``1.0`` → ``1``, ``1.5`` → CoercionError). Silently truncating
    non-integer floats would hide a real data-shape mismatch.
    """
    if isinstance(value, bool):
        # See docstring — bool is NOT a valid INTEGER input.
        raise CoercionError(
            column=column,
            value=value,
            source_type=bool,
            target_type=FieldType.INTEGER,
        )
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise CoercionError(
            column=column,
            value=value,
            source_type=float,
            target_type=FieldType.INTEGER,
        )
    if isinstance(value, str):
        stripped = value.strip()
        try:
            return int(stripped)
        except ValueError as exc:
            # Also try the ``"1.0"`` → ``1`` short form, so a CSV cell that
            # the spreadsheet stored as a float ("1.0") still lands as an
            # INTEGER. Failure still raises CoercionError with the original
            # string as the offending value.
            try:
                f = float(stripped)
            except ValueError:
                raise CoercionError(
                    column=column,
                    value=value,
                    source_type=str,
                    target_type=FieldType.INTEGER,
                ) from exc
            if f.is_integer():
                return int(f)
            raise CoercionError(
                column=column,
                value=value,
                source_type=str,
                target_type=FieldType.INTEGER,
            ) from exc
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.INTEGER,
    )


def _to_float(value: Any, *, column: str) -> float:
    """FLOAT: int / float / parseable-string → ``float``.

    bool is rejected for the same isinstance-trap reason as INTEGER.
    Scientific notation strings (``"1.5e3"``) parse via plain ``float()``
    which accepts them natively, so no special branch is needed.
    """
    if isinstance(value, bool):
        raise CoercionError(
            column=column,
            value=value,
            source_type=bool,
            target_type=FieldType.FLOAT,
        )
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError as exc:
            raise CoercionError(
                column=column,
                value=value,
                source_type=str,
                target_type=FieldType.FLOAT,
            ) from exc
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.FLOAT,
    )


# The strings accepted as a BOOLEAN, lowercased + stripped before lookup.
# # NOTE: ``"yes"`` / ``"no"`` are common in form / survey exports; ``"1"`` /
# ``"0"`` are common in CSV exports of stored-as-int booleans. Keeping the set
# small + explicit means a typo (``"truee"``) is a hard error instead of a
# silent demote to ``False``.
_BOOLEAN_TRUE_STRINGS = frozenset({"true", "1", "yes"})
_BOOLEAN_FALSE_STRINGS = frozenset({"false", "0", "no"})


def _to_boolean(value: Any, *, column: str) -> bool:
    """BOOLEAN: bool / 0-or-1-int / true-false-yes-no-1-0-string → ``bool``.

    ``bool`` is checked BEFORE ``int`` (the inverse of the INTEGER branch's
    order) so ``True`` short-circuits as a bool and doesn't fall into the
    int-0/1 check. Ints other than 0 or 1 are rejected — an arbitrary
    integer is not a boolean, and silently truthy-coercing it would mask
    a source-side type error.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 0:
            return False
        if value == 1:
            return True
        raise CoercionError(
            column=column,
            value=value,
            source_type=int,
            target_type=FieldType.BOOLEAN,
        )
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _BOOLEAN_TRUE_STRINGS:
            return True
        if normalized in _BOOLEAN_FALSE_STRINGS:
            return False
        raise CoercionError(
            column=column,
            value=value,
            source_type=str,
            target_type=FieldType.BOOLEAN,
        )
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.BOOLEAN,
    )


def _to_timestamp(value: Any, *, column: str) -> datetime:
    """TIMESTAMP: datetime / ISO-8601 string / Unix epoch number → tz-aware UTC ``datetime``.

    Output invariant: every returned ``datetime`` is tz-aware in UTC.
    Connectors that produce naive datetimes (a common pattern when the
    source's API returns "wall clock UTC" without an explicit tz) get
    them stamped with UTC; aware ones in another tz get converted.
    Destinations rely on this — BigQuery's TIMESTAMP is UTC by definition,
    and DuckDB's TIMESTAMP-with-zone wants tz-aware input.

    Unix-timestamp coercion: ``int`` / ``float`` are interpreted as
    SECONDS since the Unix epoch (the standard reading). Sub-second
    precision is preserved through ``datetime.fromtimestamp``.

    # NOTE: a *digit string* ("1717200000") is genuinely ambiguous —
    # epoch seconds vs an INTEGER cursor accidentally typed as TIMESTAMP.
    # We accept it AS epoch-seconds so a CSV-backed source with a Unix-
    # timestamp column can declare ``type: TIMESTAMP`` and have the
    # engine coerce it. The alternative (reject digit-string at the
    # TIMESTAMP boundary) would force every such source to pre-parse,
    # defeating the whole point of the engine doing the coercion.
    # Operator-side: if a true ISO-8601 string was meant and the source
    # accidentally emitted a digit string, the resulting timestamp lands
    # at 1970 + N seconds and is immediately visible as anomalous data.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    # ``date`` is NOT a TIMESTAMP — promote it to start-of-day UTC. This
    # keeps a source that types a column as DATE in YAML but returns
    # ``date`` from one branch and ``datetime`` from another from blowing
    # up; arguably this should be rejected, but a single canonical mapping
    # (date → midnight-UTC) is the lesser evil and matches Pandas /
    # SQLAlchemy convention.
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, bool):
        raise CoercionError(
            column=column,
            value=value,
            source_type=bool,
            target_type=FieldType.TIMESTAMP,
        )
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise CoercionError(
                column=column,
                value=value,
                source_type=type(value),
                target_type=FieldType.TIMESTAMP,
            ) from exc
    if isinstance(value, str):
        stripped = value.strip()
        # Digit-string → epoch seconds. See NOTE in docstring.
        if stripped and (
            stripped.lstrip("-").isdigit()
            or _is_decimal_string(stripped)
        ):
            try:
                return datetime.fromtimestamp(float(stripped), tz=UTC)
            except (OverflowError, OSError, ValueError) as exc:
                raise CoercionError(
                    column=column,
                    value=value,
                    source_type=str,
                    target_type=FieldType.TIMESTAMP,
                ) from exc
        # ISO-8601: ``datetime.fromisoformat`` (Python 3.11+) accepts
        # ``"YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]"`` AND the ``Z`` suffix
        # AND a space separator — covers the bulk of real-world inputs.
        parsed = _parse_iso8601(stripped)
        if parsed is None:
            raise CoercionError(
                column=column,
                value=value,
                source_type=str,
                target_type=FieldType.TIMESTAMP,
            )
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.TIMESTAMP,
    )


def _to_date(value: Any, *, column: str) -> date:
    """DATE: date / datetime (drop time) / ``YYYY-MM-DD`` string → ``date``.

    Order: check ``datetime`` BEFORE ``date`` — ``datetime`` is a subclass
    of ``date`` in Python, so an isinstance check for ``date`` first would
    misclassify every ``datetime`` as already-a-date and skip the time-
    drop.
    """
    if isinstance(value, datetime):
        # If aware, convert to UTC first so the date matches the UTC wall-
        # clock day (consistent with TIMESTAMP's UTC normalization).
        if value.tzinfo is not None:
            value = value.astimezone(UTC)
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value.strip())
        except ValueError as exc:
            raise CoercionError(
                column=column,
                value=value,
                source_type=str,
                target_type=FieldType.DATE,
            ) from exc
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.DATE,
    )


def _to_bytes(value: Any, *, column: str) -> bytes:
    """BYTES: bytes / base64 string / raw string → ``bytes``.

    String coercion has two paths:

    1. If the string looks like base64 (matches the alphabet, length is
       a multiple of 4, padding is well-formed) → ``base64.b64decode``.
    2. Otherwise → ``.encode("utf-8")`` (treat the string as text).

    # NOTE: order matters and the rule has a corner. Plain ASCII like
    # ``"hello"`` happens to also be valid base64 padding (5 chars, no
    # padding, all in the alphabet — but length 5 fails the
    # length-multiple-of-4 gate). The multiple-of-4 + padding gate is
    # what prevents a UTF-8 text string from getting silently base64-
    # decoded into garbage. Strings whose lengths are accidentally
    # multiples of 4 and that happen to match the base64 alphabet
    # (``"YWJjZA=="``, ``"data"``) ARE base64-decoded; this matches the
    # CSV-export convention for ``bytea`` columns.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if _looks_like_base64(stripped):
            try:
                return base64.b64decode(stripped, validate=True)
            except (binascii.Error, ValueError):
                # Fall through to utf-8 — base64 looked plausible but
                # wasn't actually decodable, so treat as plain text.
                return stripped.encode("utf-8")
        return stripped.encode("utf-8")
    raise CoercionError(
        column=column,
        value=value,
        source_type=type(value),
        target_type=FieldType.BYTES,
    )


# ---------------------------------------------------------------------------
# Helpers — shared parsing primitives
# ---------------------------------------------------------------------------


def _is_decimal_string(text: str) -> bool:
    """Is ``text`` a plain decimal number like ``"123.456"`` (no e-notation)?

    Used by :func:`_to_timestamp` to detect a Unix-epoch-with-fractional-
    seconds digit string. Excludes scientific notation to avoid a 30-char
    string like ``"1.5e308"`` being treated as an epoch — that's
    overflowingly far in the future and indicates a source-side bug, so
    let the ISO-8601 branch reject it.
    """
    if not text:
        return False
    if text.count(".") != 1:
        return False
    head, _, tail = text.partition(".")
    if head.startswith(("-", "+")):
        head = head[1:]
    return head.isdigit() and tail.isdigit()


def _parse_iso8601(text: str) -> datetime | None:
    """Best-effort ISO-8601 parse — returns ``None`` on failure (no raise).

    ``datetime.fromisoformat`` (Python 3.11+) accepts most reasonable shapes
    natively, including a trailing ``Z`` (mapped to UTC) and a space
    separator between date and time. The one corner it misses on some
    minor versions is a ``Z`` paired with fractional seconds; we
    pre-normalize ``Z`` → ``+00:00`` to sidestep that.
    """
    candidate = text
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


_BASE64_ALPHABET = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/="
)


def _looks_like_base64(text: str) -> bool:
    """Heuristic: does ``text`` look like a base64 payload worth decoding?

    Gates the base64 branch of :func:`_to_bytes`. The combined gate
    (alphabet + length-multiple-of-4 + well-formed padding) keeps plain-
    ASCII text like ``"hello world"`` from being treated as base64.
    """
    if not text:
        return False
    if len(text) % 4 != 0:
        return False
    if any(ch not in _BASE64_ALPHABET for ch in text):
        return False
    # Padding (``=``) is only valid as the last 1 or 2 chars.
    pad_count = text.count("=")
    if pad_count > 2:
        return False
    if pad_count and not text.endswith("=" * pad_count):
        return False
    return True


# ---------------------------------------------------------------------------
# Batch driver — runner's call-site
# ---------------------------------------------------------------------------


def normalize_batch(batch: Batch, schema: Schema) -> Batch:
    """Return a new :class:`~dtex.types.Batch` with every cell coerced to its declared type.

    Per-record dict mutation is fine internally — we build fresh dicts to
    avoid side-effecting the connector's yielded records. The returned
    batch is structurally identical to the input but values for any column
    declared in ``schema`` are guaranteed to be the canonical Python type
    for their :class:`~dtex.types.FieldType`.

    Columns NOT in ``schema.fields`` pass through unchanged. This is the
    same "ragged batches survive" promise the destination's
    ``_augment_schema_for_batch`` upholds — the engine has either inferred
    the schema from the first batch (so by construction every column the
    first batch carries is in the schema, and a later batch's extra column
    is the schema-evolution path's responsibility) or accepted a declared
    schema (in which case the author chose what columns matter; extras are
    additive-evolution territory).

    Performance: the per-field-type lookup is hoisted out of the per-
    record loop (precomputed once per batch) so the hot loop is a flat
    dict-walk with one ``isinstance`` ladder per cell. A typical batch
    (100-1000 records × 5-20 cols) costs sub-millisecond.

    # NOTE: returns a new list of new dicts rather than mutating in place.
    # Connectors stash yielded records (rare, but legal — a streaming
    # connector may keep a window of prior records for de-dup), and
    # mutating the engine's normalization back into those references
    # would be a spooky-action-at-a-distance bug. Allocation cost of the
    # fresh dicts is negligible at batch-size scale.
    """
    if not batch:
        return []

    # Precompute (name, type) pairs once. Iterating ``schema.fields`` once
    # builds the column-typing list; the per-record loop reuses it.
    typed_columns: list[tuple[str, FieldType]] = [
        (f.name, f.type) for f in schema.fields
    ]
    if not typed_columns:
        # No declared types → nothing to coerce. Fast-return shallow copies
        # so the caller still gets fresh dicts (uniform contract).
        return [dict(record) for record in batch]

    out: list[dict[str, Any]] = []
    for record in batch:
        new_record = dict(record)  # copy first so extras carry over verbatim
        for name, ftype in typed_columns:
            if name in new_record:
                new_record[name] = coerce_value(
                    new_record[name], ftype, column=name
                )
        out.append(new_record)
    return out
