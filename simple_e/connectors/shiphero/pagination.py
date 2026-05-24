"""GraphQL `pageInfo { hasNextPage endCursor }` pagination — main.py 386-446.

ShipHero uses Relay-style cursor pagination: each `data` block carries an
``edges`` list of ``{ "node": ... }`` wrappers and a ``pageInfo`` block with
``hasNextPage`` and ``endCursor``. To exhaust a window, send the first request
with ``after=null``, then keep sending ``after=<previous endCursor>`` while
``hasNextPage`` is True.

The split here matches the docs/04 convention: GraphQL response *shape* lives
next to the queries; the @stream function in ``source.py`` calls this module's
:func:`paginate` and gets a clean stream of records back.

Implemented as a generator — pages are pulled lazily and yielded
record-by-record. A window with thousands of records never materializes more
than one page at a time in memory.
"""

from __future__ import annotations

from collections.abc import Generator, Iterator
from typing import Any


def walk_field_path(response: Any, path: list[str]) -> Any:
    """Walk a `field_path` through a nested dict — the basic descent.

    `*` is **not** handled here — see :func:`extract_records` for the
    list-flattening case. Used by callers that need to navigate to a parent
    container (e.g. the `data` block holding both `edges` and `pageInfo`).
    Returns ``{}`` for a missing key, so a malformed payload never raises a
    ``KeyError`` from this helper.
    """
    cursor = response
    for key in path:
        if isinstance(cursor, dict):
            cursor = cursor.get(key, {})
        else:
            return {}
    return cursor


def extract_records(response: Any, field_path: list[str]) -> list[dict[str, Any]]:
    """Walk ``field_path`` through ``response`` and unwrap GraphQL edge nodes.

    The ``*`` element means "iterate this list and yield each element". A
    canonical ShipHero shipments path is
    ``["data", "shipments", "data", "edges", "*", "node"]`` — descending from
    the root through edges into the per-record `node` wrapper.

    Returns a plain ``list[dict]`` per record. Edge entries without a `node`
    key (defensive: ShipHero has occasionally returned half-shaped edges) are
    skipped silently. A path that does not resolve at all (an empty `edges`,
    a missing intermediate key) returns ``[]``.
    """
    return list(_iter_field_path(response, field_path))


def _iter_field_path(
    value: Any, path: list[str]
) -> Generator[dict[str, Any], None, None]:
    """Walk ``path`` element-by-element, yielding the final ``dict`` records.

    Recursion ends on either an empty ``path`` (yield the value if it is a
    dict) or a leaf descent. The ``*`` element forks the walk over every list
    element. Non-dict leaves are skipped — the caller wants record dicts.
    """
    if not path:
        if isinstance(value, dict):
            yield value
        return
    head, *rest = path
    if head == "*":
        if isinstance(value, list):
            for item in value:
                if item is not None:
                    yield from _iter_field_path(item, rest)
        return
    if isinstance(value, dict):
        yield from _iter_field_path(value.get(head), rest)


def paginate(
    *,
    fetch_page: Any,
    page_size: int,
    field_path_to_records: list[str],
    field_path_to_pageinfo: list[str],
    extra_variables: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield every record across pages — main.py lines 386-446 / 415-438.

    ``fetch_page`` is a callable that takes a single ``variables`` dict
    (``{"first": <page_size>, "after": <cursor_or_null>, ...extra}``) and
    returns the parsed GraphQL response. Decoupling the HTTP from the
    pagination loop is the test seam — a stub server's response shape, not its
    transport, is what matters.

    Stops on the first page where ``pageInfo.hasNextPage`` is false **or**
    where ``endCursor`` is missing (the main.py safety check on line 442). A
    page yielding zero records but claiming ``hasNextPage=True`` is honored —
    ShipHero occasionally returns sparse pages mid-window.
    """
    after: str | None = None
    extra = extra_variables or {}
    while True:
        variables: dict[str, Any] = {"first": page_size, "after": after, **extra}
        response = fetch_page(variables)
        yield from extract_records(response, field_path_to_records)

        page_info = walk_field_path(response, field_path_to_pageinfo)
        if not isinstance(page_info, dict) or not page_info.get("hasNextPage"):
            return
        next_cursor = page_info.get("endCursor")
        if not next_cursor:
            # ``hasNextPage=True`` but no cursor — the main.py "safety check"
            # on line 442. Stop rather than loop forever on a stuck cursor.
            return
        after = str(next_cursor)
