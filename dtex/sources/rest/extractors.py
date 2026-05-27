# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Field-path extraction — turn a parsed JSON response into a list of records.

A REST API rarely returns a top-level JSON array. The records the connector
cares about live under a path inside the response body — e.g. ``data.items``
for ``{"data": {"items": [...], "meta": ...}}``, or
``data.edges.*.node`` for a GraphQL-shaped envelope. ``record_path`` is the
author-supplied list of keys that walks that nested envelope down to the list
of records.

Two walkers live here — they look similar but serve different shapes and stay
deliberately separate (see ``# NOTE`` in :func:`extract_records`):

* :func:`extract_records` — takes a ``list[str]`` path with ``"*"`` as a
  wildcard step that flattens a list-of-X into X-per-element. The path arrives
  *segmented* because it is author-declared in Python code and lists are the
  natural Python literal — no parsing ambiguity around literal dots in keys.
* :func:`extract_dotted` — takes a ``"a.b.c"`` dotted string, used for
  ``next_cursor_path`` where the response field is a single scalar pointer and
  a string literal reads better than ``["a","b","c"]``.

docs/04 "Record shape": the records this returns are flat-ish ``dict`` s — the
shape the source connector then yields as a batch (``list[dict]``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

# The wildcard step that flattens a list into per-element traversal — borrowed
# from JSONPath's ``[*]`` (without JSONPath's full grammar, which would be a
# disproportionate dependency for a single-character feature).
WILDCARD = "*"


class ExtractionError(Exception):
    """A ``record_path`` did not resolve against the response body.

    Raised when the path leads to a key/index that does not exist, or terminates
    at a value that is not a list of records. The message names the offending
    step so an author can correct ``record_path`` against the API's real shape.
    """


def extract_records(payload: Any, record_path: Sequence[str]) -> list[dict[str, Any]]:
    """Walk ``record_path`` down ``payload`` and return the records list — docs/04.

    The path is a sequence of *string* steps. Each non-wildcard step indexes a
    dict by key (the API responses dtex supports are JSON objects, not
    arrays-of-pairs); the wildcard step :data:`WILDCARD` flattens a list of
    sub-objects so the next step is applied to every element. The final value
    must be a list (each item being one record); an empty list is a valid
    "no records this page" answer and returns ``[]``.

    Worked examples — the two shapes the handbook calls out:

    >>> extract_records({"data": {"items": [{"id": 1}]}}, ["data", "items"])
    [{'id': 1}]
    >>> extract_records(
    ...     {"data": {"edges": [{"node": {"id": 1}}, {"node": {"id": 2}}]}},
    ...     ["data", "edges", "*", "node"],
    ... )
    [{'id': 1}, {'id': 2}]

    An empty path returns ``payload`` itself if it is already a list — the
    degenerate "the API returns a bare array at the root" case.

    Raises :class:`ExtractionError` with the offending step on any walk failure.
    """
    # NOTE: ``record_path`` is segmented (``list[str]``) while ``next_cursor_path``
    # (in :func:`extract_dotted`) is dotted ("a.b.c"). They are deliberately not
    # unified into one walker: ``record_path`` carries a wildcard step that has
    # no place in the single-scalar-pointer use of ``next_cursor_path``, and
    # using a list for records lets a key with a literal ``.`` work — a key
    # cannot be expressed in the dotted form without an escape syntax we
    # consciously avoid (KISS).
    cursor: Any = payload
    walked: list[str] = []
    for idx, step in enumerate(record_path):
        walked.append(step)
        if step == WILDCARD:
            if not isinstance(cursor, list):
                raise ExtractionError(
                    f"record_path step {step!r} expects a list at "
                    f"{'.'.join(walked[:-1]) or '<root>'} but got "
                    f"{type(cursor).__name__}"
                )
            # The wildcard splits the walk: every list element is unwrapped to
            # one record by walking the *remaining* path through it via
            # :func:`_walk_to_scalar` (one value out, not a list). This is the
            # GraphQL-edge shape — ``edges: [{node: {...}}, ...]`` with path
            # ``["edges", "*", "node"]`` produces one record per edge.
            remaining = list(record_path[idx + 1 :])
            results: list[dict[str, Any]] = []
            for i, item in enumerate(cursor):
                unwrapped = _walk_to_scalar(item, remaining, walked[:-1] + [f"[{i}]"])
                if unwrapped is None:
                    continue
                if not isinstance(unwrapped, dict):
                    raise ExtractionError(
                        f"record_path wildcard at "
                        f"{'.'.join(walked[:-1]) or '<root>'}[{i}] resolved to "
                        f"{type(unwrapped).__name__}; expected a JSON object record"
                    )
                results.append(unwrapped)
            return results
        if not isinstance(cursor, dict):
            raise ExtractionError(
                f"record_path step {step!r} expects a dict at "
                f"{'.'.join(walked[:-1]) or '<root>'} but got "
                f"{type(cursor).__name__}"
            )
        if step not in cursor:
            raise ExtractionError(
                f"record_path step {step!r} not found in response at "
                f"{'.'.join(walked[:-1]) or '<root>'}"
            )
        cursor = cursor[step]

    if cursor is None:
        # A path that resolves to ``null`` is an empty page — common when an API
        # uses ``"items": null`` for a no-results response. Treat as no rows.
        return []
    if not isinstance(cursor, list):
        raise ExtractionError(
            f"record_path {'.'.join(record_path) or '<root>'} resolved to "
            f"{type(cursor).__name__}; expected a list of records"
        )
    # Each element must be dict-shaped — the contract requires flat ``Record``
    # dicts (docs/04). A non-dict element here is an API-shape mismatch.
    for i, item in enumerate(cursor):
        if not isinstance(item, dict):
            raise ExtractionError(
                f"record_path {'.'.join(record_path) or '<root>'}[{i}] is "
                f"{type(item).__name__}; expected a JSON object record"
            )
    return list(cursor)


def _walk_to_scalar(
    payload: Any, path: Sequence[str], walked_so_far: Sequence[str]
) -> Any:
    """Walk ``path`` through ``payload`` step-by-step and return the terminal value.

    Used after the wildcard splits the main walk — at that point every step
    produces a single value per element (a record), not a list. An empty path
    returns the input unchanged ("the element itself is the record"). A
    missing key raises :class:`ExtractionError`; ``None`` propagates as-is so
    a record whose nested field is null becomes ``None`` and is skipped by the
    caller.
    """
    cursor: Any = payload
    walked = list(walked_so_far)
    for step in path:
        walked.append(step)
        if cursor is None:
            return None
        if step == WILDCARD:
            # Two wildcards in one path are not supported — the simple "edges →
            # node" pattern needs only one. Catch the misuse explicitly.
            raise ExtractionError(
                "record_path supports only one '*' wildcard step"
            )
        if not isinstance(cursor, dict):
            raise ExtractionError(
                f"record_path step {step!r} expects a dict at "
                f"{'.'.join(walked[:-1]) or '<root>'} but got "
                f"{type(cursor).__name__}"
            )
        if step not in cursor:
            raise ExtractionError(
                f"record_path step {step!r} not found in response at "
                f"{'.'.join(walked[:-1]) or '<root>'}"
            )
        cursor = cursor[step]
    return cursor


def extract_dotted(payload: Any, dotted_path: str) -> Any:
    """Read a single value from ``payload`` via a dotted path — for ``next_cursor_path``.

    Used to read the next-page pointer out of a cursor-paginated response (e.g.
    ``meta.next_cursor`` for ``{"data": [...], "meta": {"next_cursor": "abc"}}``).
    Walks step by step; an empty/absent step returns ``None`` so the pagination
    driver treats it as "no more pages" without raising.

    Returns ``None`` if any step is absent, or the final value otherwise. Unlike
    :func:`extract_records` this is *forgiving* — a missing next-cursor field is
    the normal "you have reached the last page" signal, not a configuration bug.
    """
    if not dotted_path:
        return None
    cursor: Any = payload
    for step in dotted_path.split("."):
        if cursor is None:
            return None
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(step)
    # An empty string is treated the same as None — some APIs return "" to mean
    # "no next page" rather than omitting the field; both should stop pagination.
    if cursor == "":
        return None
    return cursor
