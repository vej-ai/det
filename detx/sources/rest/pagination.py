# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""HTTP pagination strategies for the Generic REST source connector.

A pagination strategy decides three things, and only three things:

1. What query params go on the *first* request to a stream's endpoint.
2. What query params go on the *next* request, given the previous response.
3. Whether to stop (no next request).

Each strategy is a small class with the same two methods, so :mod:`source` can
drive them uniformly::

    params: dict | None = strategy.prepare_first(initial_query)
    while params is not None:
        response = client.get(endpoint, params=params)
        records = extract_records(response.json(), record_path)
        yield records
        params = strategy.update_after(response.json(), response.headers, params)

State (next page token, current offset, current page number, current link URL)
lives on the *instance* — a strategy is constructed fresh per stream invocation,
so two streams in the same connector run never share pagination state.

Four strategies cover the bulk of real REST APIs:

* :class:`CursorPagination` — opaque next-token in the response body.
* :class:`OffsetPagination` — ``offset`` + ``limit`` counters in the query.
* :class:`PagePagination` — ``page`` + ``per_page`` counters in the query.
* :class:`LinkHeaderPagination` — RFC 5988 ``Link: <url>; rel="next"`` header.

A fifth pagination strategy lives in :class:`NoPagination` — the degenerate
single-page case, used when an endpoint just returns its full payload in one
shot. Authors who really want this can opt into it; it is not a default.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlparse

from detx.sources.rest.extractors import extract_dotted

__all__ = [
    "CursorPagination",
    "LinkHeaderPagination",
    "NoPagination",
    "OffsetPagination",
    "PagePagination",
    "PaginationStrategy",
    "build_strategy",
]


class PaginationStrategy:
    """Common interface every pagination strategy implements.

    Subclasses override :meth:`prepare_first` and :meth:`update_after`. The
    contract:

    * :meth:`prepare_first` returns the query params dict for request 1, or
      ``None`` if even the first request should be skipped (no implementation
      currently does so; reserved for future "the API exposes only deltas and
      none are pending" strategies).
    * :meth:`update_after` returns the query params dict for the next request,
      or ``None`` to stop. ``response_json`` is the parsed body; ``headers`` is
      the case-insensitive header mapping; ``last_params`` is what was just sent.

    Both methods MUST return a *new* dict, not the one passed in — the driver
    in :mod:`source` may want to compare consecutive params for cycle detection.
    """

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """Build the query params for the first request — strategy-specific."""
        raise NotImplementedError

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Build the next request's query params, or ``None`` to stop."""
        raise NotImplementedError


# --------------------------------------------------------------------------
# 1. Cursor pagination — opaque next-token in the response body.
# --------------------------------------------------------------------------


@dataclass
class CursorPagination(PaginationStrategy):
    """Cursor pagination — opaque next-token carried in the response body.

    Example shape::

        GET /v1/orders?cursor=abc&limit=100
        ->  {"data": [...], "meta": {"next_cursor": "def"}}

    The strategy reads ``next_cursor_path`` (a dotted path into the response,
    e.g. ``"meta.next_cursor"``) and uses its value as the ``cursor_query_param``
    on the next request. Pagination stops when the cursor field is absent or
    empty (the convention every cursor API uses for "you are at the end").

    On the first request the cursor query param is omitted entirely — most APIs
    treat absence as "give me the first page", whereas an empty value can be a
    400. If the author needs an explicit first-cursor value they can pass it as
    part of ``initial_query`` (the connector's ``extra_query_params``).
    """

    cursor_query_param: str
    """The query parameter name to send the cursor value as (e.g. ``"cursor"``)."""
    next_cursor_path: str
    """Dotted path into the JSON response that yields the next-page cursor."""
    page_size_param: str | None = None
    """Optional query param name to send the page size as (e.g. ``"limit"``)."""
    page_size: int | None = None
    """The page size value paired with :attr:`page_size_param`."""

    _last_cursor_seen: Any = None

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """First-page params: caller's extras + optional page size, no cursor yet."""
        self._last_cursor_seen = None
        params = dict(initial_query)
        if self.page_size_param and self.page_size is not None:
            params.setdefault(self.page_size_param, self.page_size)
        return params

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Next-page params: copy + replace cursor; ``None`` when cursor is absent.

        Also stops on a *non-advancing* cursor (the same value twice in a row),
        which would otherwise be an infinite loop if the API ignores the cursor
        param. A misbehaving API must fail the run, not hang it.
        """
        next_cursor = extract_dotted(response_json, self.next_cursor_path)
        if next_cursor is None:
            return None
        if self._last_cursor_seen is not None and next_cursor == self._last_cursor_seen:
            return None
        self._last_cursor_seen = next_cursor
        params = dict(last_params)
        params[self.cursor_query_param] = next_cursor
        return params


# --------------------------------------------------------------------------
# 2. Offset pagination — ``offset=0,N,2N,...`` with ``limit``.
# --------------------------------------------------------------------------


@dataclass
class OffsetPagination(PaginationStrategy):
    """Offset/limit pagination — classic ``?offset=N&limit=N`` SQL-style.

    The strategy increments :attr:`offset_param` by :attr:`limit` after each
    page. It stops when a page comes back with fewer records than
    :attr:`limit` (the universal end-of-data signal) — that decision needs the
    record count, which the driver in :mod:`source` cannot communicate via
    headers; instead the strategy looks at the response body length itself
    using :attr:`record_path`. This is the only pagination strategy that needs
    to know where the records list is, because every other strategy gets its
    "more?" signal from a header or a pointer field.
    """

    offset_param: str = "offset"
    limit_param: str = "limit"
    limit: int = 100
    record_path: tuple[str, ...] = ()
    """Same ``record_path`` as the stream — needed to count records per page."""

    _current_offset: int = 0

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """First-page params: ``offset=0, limit=page_size``, plus caller's extras."""
        self._current_offset = 0
        params = dict(initial_query)
        params[self.offset_param] = 0
        params[self.limit_param] = self.limit
        return params

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Advance offset; stop when the last page was short or empty."""
        # Late import to avoid a circular at module-load time (extractors imports
        # nothing from us, but pagination.py is imported by source.py which also
        # imports extractors; keeping this import here costs nothing).
        from detx.sources.rest.extractors import extract_records

        try:
            records = extract_records(response_json, list(self.record_path))
        except Exception:
            # If we can't count records, we stop — refusing to loop forever on a
            # malformed response is safer than retrying offsets blindly.
            return None
        count = len(records)
        if count < self.limit:
            return None
        self._current_offset += self.limit
        params = dict(last_params)
        params[self.offset_param] = self._current_offset
        return params


# --------------------------------------------------------------------------
# 3. Page pagination — ``page=1,2,3,...`` with ``per_page``.
# --------------------------------------------------------------------------


@dataclass
class PagePagination(PaginationStrategy):
    """Page-number pagination — ``?page=N&per_page=N``.

    The 1-indexed cousin of :class:`OffsetPagination`. Same end-of-data rule:
    a short page (fewer records than :attr:`per_page`) stops the loop.

    # NOTE: pages are 1-indexed — the universal convention across REST APIs
    # that use page numbers (GitHub, Stripe's older API, most CRMs). A 0-indexed
    # API will surface as "first page is always empty" — easy to spot and
    # easy for the author to work around by setting page_param to a renamed
    # variant or by overriding via initial_query.
    """

    page_param: str = "page"
    per_page_param: str = "per_page"
    per_page: int = 100
    record_path: tuple[str, ...] = ()

    _current_page: int = 1

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """First-page params: ``page=1, per_page=page_size``, plus caller's extras."""
        self._current_page = 1
        params = dict(initial_query)
        params[self.page_param] = 1
        params[self.per_page_param] = self.per_page
        return params

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Advance the page number; stop on a short page."""
        from detx.sources.rest.extractors import extract_records

        try:
            records = extract_records(response_json, list(self.record_path))
        except Exception:
            return None
        count = len(records)
        if count < self.per_page:
            return None
        self._current_page += 1
        params = dict(last_params)
        params[self.page_param] = self._current_page
        return params


# --------------------------------------------------------------------------
# 4. Link-header pagination — RFC 5988 ``Link: <url>; rel="next"``.
# --------------------------------------------------------------------------


# RFC 5988 link-value parser: one entry is ``<URL>; rel="REL"`` (with optional
# extra parameters we ignore). The angle brackets and the quoted rel are the
# only parts the strategy needs to recognize.
_LINK_HEADER_PATTERN = re.compile(
    r'<(?P<url>[^>]+)>\s*;\s*[^,]*?rel\s*=\s*"?(?P<rel>[^",;]+)"?',
    re.IGNORECASE,
)


def _parse_link_header(value: str) -> dict[str, str]:
    """Parse an RFC 5988 ``Link`` header into a ``{rel: url}`` map.

    The relevant rel for pagination is ``"next"``; the helper returns every rel
    so a future strategy could also follow ``"prev"`` / ``"last"``. Unknown
    fragments are skipped — a malformed entry must not crash an otherwise valid
    page traversal.
    """
    out: dict[str, str] = {}
    if not value:
        return out
    for match in _LINK_HEADER_PATTERN.finditer(value):
        rel = match.group("rel").strip().lower()
        url = match.group("url").strip()
        out[rel] = url
    return out


@dataclass
class LinkHeaderPagination(PaginationStrategy):
    """RFC 5988 ``Link``-header pagination — GitHub-style ``rel="next"`` URLs.

    The server returns the *full URL* of the next page in a header like::

        Link: <https://api.example.com/v3/orders?page=2>; rel="next",
              <https://api.example.com/v3/orders?page=10>; rel="last"

    On each iteration the strategy reads the ``next`` rel; if it is absent the
    loop stops. The next URL replaces the previous one wholesale, so the
    strategy returns query params parsed back out of that URL — the HTTP
    client in :mod:`client` re-uses its session against the same base URL, so
    the absolute URL's query string is what carries the pagination state. The
    initial endpoint URL must already match ``base_url + endpoint``; we never
    cross a host boundary (would expose credentials to a different origin).
    """

    base_url: str
    """The connector's base URL — every ``next`` URL must start with this."""
    page_size_param: str | None = None
    page_size: int | None = None

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """First-page params: caller's extras + optional page size."""
        params = dict(initial_query)
        if self.page_size_param and self.page_size is not None:
            params.setdefault(self.page_size_param, self.page_size)
        return params

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Parse the ``Link: rel="next"`` URL; stop if absent or off-origin."""
        link_value = headers.get("Link") or headers.get("link")
        if not link_value:
            return None
        links = _parse_link_header(link_value)
        next_url = links.get("next")
        if not next_url:
            return None
        # Cross-origin "next" links would leak the bearer token to another host —
        # refuse to follow them. A same-origin link returns its query params.
        if not next_url.startswith(self.base_url):
            return None
        parsed = urlparse(next_url)
        params: dict[str, Any] = dict(parse_qsl(parsed.query, keep_blank_values=True))
        return params


# --------------------------------------------------------------------------
# 5. No pagination — single-page endpoints (opt-in).
# --------------------------------------------------------------------------


@dataclass
class NoPagination(PaginationStrategy):
    """Single-page strategy — make exactly one request and stop.

    For an endpoint that returns its full payload in one shot (rare, but common
    enough for config / lookup endpoints). Authors must opt into it; the
    connector defaults to :class:`CursorPagination`.
    """

    def prepare_first(
        self, initial_query: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        """One request — pass through the caller's extras and stop after."""
        return dict(initial_query)

    def update_after(
        self,
        response_json: Any,
        headers: Mapping[str, str],
        last_params: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """Always stop after the first response."""
        return None


# --------------------------------------------------------------------------
# Strategy factory — names → constructors
# --------------------------------------------------------------------------


def build_strategy(
    name: str,
    *,
    base_url: str,
    page_size: int,
    record_path: tuple[str, ...],
    cursor_query_param: str | None = None,
    next_cursor_path: str | None = None,
    page_size_param: str | None = None,
    offset_param: str = "offset",
    limit_param: str = "limit",
    page_param: str = "page",
    per_page_param: str = "per_page",
) -> PaginationStrategy:
    """Build a :class:`PaginationStrategy` from its declarative name + params.

    The connector's ``register.yaml`` carries the strategy *name*
    (``cursor`` / ``offset`` / ``page`` / ``link_header`` / ``none``); a stream
    function maps that name to an instance via this factory and passes any
    per-stream overrides (e.g. ``next_cursor_path``).

    A ``cursor`` strategy missing ``cursor_query_param`` or ``next_cursor_path``
    raises :class:`ValueError` — those are not optional for that mode and the
    failure must surface clearly, not as an "unexpected ``KeyError`` on every
    response".
    """
    if name == "cursor":
        if not cursor_query_param or not next_cursor_path:
            raise ValueError(
                "pagination_strategy='cursor' requires both 'cursor_query_param' "
                "and 'next_cursor_path' on the stream"
            )
        return CursorPagination(
            cursor_query_param=cursor_query_param,
            next_cursor_path=next_cursor_path,
            page_size_param=page_size_param,
            page_size=page_size if page_size_param else None,
        )
    if name == "offset":
        return OffsetPagination(
            offset_param=offset_param,
            limit_param=limit_param,
            limit=page_size,
            record_path=record_path,
        )
    if name == "page":
        return PagePagination(
            page_param=page_param,
            per_page_param=per_page_param,
            per_page=page_size,
            record_path=record_path,
        )
    if name == "link_header":
        return LinkHeaderPagination(
            base_url=base_url,
            page_size_param=page_size_param,
            page_size=page_size if page_size_param else None,
        )
    if name == "none":
        return NoPagination()
    raise ValueError(
        f"unknown pagination_strategy {name!r}; valid: "
        "cursor, offset, page, link_header, none"
    )
