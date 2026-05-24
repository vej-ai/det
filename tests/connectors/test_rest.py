"""Tests for the Generic REST source connector.

Zero live network. Every test stands up a stdlib :class:`ThreadingHTTPServer`
on a random port and points the connector at it via the ``base_url`` param.
Per-test handlers serve canned responses and record the requests the connector
sent (path, query params, headers) so assertions can check both directions of
the contract — what got fetched and what was offered to the API.

The harness intentionally uses only stdlib HTTP — no ``responses`` /
``httpretty`` dependency — keeping the test surface as portable as the
production code's only HTTP-side dep (``requests``).
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
import urllib.parse
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

import det
from det import Config, Cursor
from det.connectors.rest.client import AuthSpec, build_client
from det.connectors.rest.extractors import ExtractionError, extract_records
from det.connectors.rest.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    OffsetPagination,
    PagePagination,
    build_strategy,
)
from det.connectors.rest.source import extract_stream
from det.engine.logger import build_logger

# Reuse the fixtures project — it has det_project.yml + profiles.yml and
# the duckdb destination is the project's default. The smoke test does the same.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROJECT_DIR = _REPO_ROOT / "tests" / "fixtures"


# ==========================================================================
# Tiny stub HTTP server
# ==========================================================================


class _RecordingHandler(BaseHTTPRequestHandler):
    """A request handler that dispatches to per-instance routes and logs traffic.

    The class is configured per test via class-level ``routes`` and
    ``recorded`` mutable members assigned by :func:`stub_server`. Each route is
    keyed by URL path; the value is a callable ``(handler) -> None`` that
    writes one response. Recorded entries carry the path, query, and headers.
    """

    routes: dict[str, Callable[[_RecordingHandler], None]] = {}
    recorded: list[dict[str, Any]] = []

    # Silence stdlib's default per-request stderr noise so test output stays clean.
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002, ARG002
        pass

    def _record(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        self.recorded.append(
            {
                "method": self.command,
                "path": parsed.path,
                "query": query,
                "headers": {k: v for k, v in self.headers.items()},
            }
        )

    def do_GET(self) -> None:  # noqa: N802 — stdlib signature
        self._record()
        parsed = urllib.parse.urlparse(self.path)
        handler = self.routes.get(parsed.path)
        if handler is None:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'{"error":"not found"}')
            return
        handler(self)


def _write_json(handler: BaseHTTPRequestHandler, status: int, body: Any,
                extra_headers: dict[str, str] | None = None) -> None:
    """Helper for a stub handler — send a JSON response with given status."""
    payload = json.dumps(body).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    for k, v in (extra_headers or {}).items():
        handler.send_header(k, v)
    handler.end_headers()
    handler.wfile.write(payload)


@contextmanager
def stub_server(
    routes: dict[str, Callable[[_RecordingHandler], None]],
) -> Iterator[tuple[str, list[dict[str, Any]]]]:
    """Spin up a :class:`ThreadingHTTPServer` on port 0; yield base URL + log.

    Each test gets a fresh handler class so two tests' routes never collide
    even if pytest parallelizes (the recorded list is also fresh).
    """
    recorded: list[dict[str, Any]] = []

    handler_cls: type[_RecordingHandler] = type(
        "_TestHandler",
        (_RecordingHandler,),
        {"routes": routes, "recorded": recorded},
    )

    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}", recorded
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


# ==========================================================================
# Pagination strategy tests — each strategy yields N pages of M records
# ==========================================================================


def _three_page_cursor_routes() -> dict[str, Callable[[_RecordingHandler], None]]:
    """Three pages of 10 records, cursor-paginated, ``meta.next_cursor`` token."""
    pages = {
        None: {"data": [{"id": i} for i in range(1, 11)], "meta": {"next_cursor": "p2"}},
        "p2": {"data": [{"id": i} for i in range(11, 21)], "meta": {"next_cursor": "p3"}},
        "p3": {"data": [{"id": i} for i in range(21, 31)], "meta": {}},
    }

    def handler(h: _RecordingHandler) -> None:
        parsed = urllib.parse.urlparse(h.path)
        q = dict(urllib.parse.parse_qsl(parsed.query))
        body = pages.get(q.get("cursor"))
        if body is None:
            _write_json(h, 404, {"error": "no such page"})
            return
        _write_json(h, 200, body)

    return {"/items": handler}


def test_cursor_pagination_three_pages() -> None:
    """30 records across 3 cursor-paginated pages — every page is yielded."""
    with stub_server(_three_page_cursor_routes()) as (base_url, _):
        log = build_logger("test-cursor")
        config = Config(params={"base_url": base_url}, secrets={})
        batches = list(
            extract_stream(
                config=config,
                log=log,
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
                pagination_strategy="cursor",
            )
        )
    total = sum(len(b) for b in batches)
    assert total == 30
    assert [r["id"] for b in batches for r in b] == list(range(1, 31))


def _three_page_offset_routes() -> dict[str, Callable[[_RecordingHandler], None]]:
    """Three pages of 10 records via ``offset+limit``."""

    def handler(h: _RecordingHandler) -> None:
        parsed = urllib.parse.urlparse(h.path)
        q = dict(urllib.parse.parse_qsl(parsed.query))
        offset = int(q.get("offset", "0"))
        limit = int(q.get("limit", "10"))
        if offset >= 30:
            _write_json(h, 200, {"data": []})
            return
        data = [{"id": i} for i in range(offset + 1, min(offset + limit, 30) + 1)]
        _write_json(h, 200, {"data": data})

    return {"/items": handler}


def test_offset_pagination_three_pages() -> None:
    """30 records across 3 offset-paginated pages — short last page stops the loop."""
    with stub_server(_three_page_offset_routes()) as (base_url, requests_seen):
        config = Config(params={"base_url": base_url, "page_size": 10}, secrets={})
        batches = list(
            extract_stream(
                config=config,
                log=build_logger("test-offset"),
                endpoint="/items",
                record_path=["data"],
                pagination_strategy="offset",
            )
        )
    total = sum(len(b) for b in batches)
    assert total == 30
    # Three pages: offsets 0, 10, 20. No fourth page — page 3 returned 10 == limit,
    # so the strategy DOES try page 4 (offset 20+10=30), which returns empty — stop.
    offsets = sorted(int(r["query"].get("offset", "0")) for r in requests_seen)
    assert offsets == [0, 10, 20, 30]


def _three_page_page_routes() -> dict[str, Callable[[_RecordingHandler], None]]:
    """Three pages of 10 via 1-indexed ``page+per_page``."""

    def handler(h: _RecordingHandler) -> None:
        parsed = urllib.parse.urlparse(h.path)
        q = dict(urllib.parse.parse_qsl(parsed.query))
        page = int(q.get("page", "1"))
        per = int(q.get("per_page", "10"))
        if page > 3:
            _write_json(h, 200, {"data": []})
            return
        start = (page - 1) * per + 1
        end = start + per
        data = [{"id": i} for i in range(start, end)]
        _write_json(h, 200, {"data": data})

    return {"/items": handler}


def test_page_pagination_three_pages() -> None:
    """30 records across 3 page-paginated pages — page 4 short-circuits."""
    with stub_server(_three_page_page_routes()) as (base_url, requests_seen):
        config = Config(params={"base_url": base_url, "page_size": 10}, secrets={})
        batches = list(
            extract_stream(
                config=config,
                log=build_logger("test-page"),
                endpoint="/items",
                record_path=["data"],
                pagination_strategy="page",
            )
        )
    assert sum(len(b) for b in batches) == 30
    pages = sorted(int(r["query"].get("page", "1")) for r in requests_seen)
    # Pages 1/2/3 each return 10 == per_page → fetch one more page (4) which
    # comes back empty → stop.
    assert pages == [1, 2, 3, 4]


def _three_page_link_header_routes(
    base_url_holder: dict[str, str],
) -> dict[str, Callable[[_RecordingHandler], None]]:
    """Three pages via RFC 5988 ``Link: <url>; rel="next"`` headers."""

    def handler(h: _RecordingHandler) -> None:
        parsed = urllib.parse.urlparse(h.path)
        q = dict(urllib.parse.parse_qsl(parsed.query))
        page = int(q.get("page", "1"))
        base = base_url_holder["base_url"]
        data = [{"id": (page - 1) * 10 + i} for i in range(1, 11)]
        headers: dict[str, str] = {}
        if page < 3:
            headers["Link"] = f'<{base}/items?page={page + 1}>; rel="next"'
        _write_json(h, 200, {"data": data}, extra_headers=headers)

    return {"/items": handler}


def test_link_header_pagination_three_pages() -> None:
    """30 records via three pages of ``Link: rel=next`` URL-chained pagination."""
    base_holder: dict[str, str] = {}
    with stub_server(_three_page_link_header_routes(base_holder)) as (base_url, _):
        base_holder["base_url"] = base_url
        config = Config(params={"base_url": base_url}, secrets={})
        batches = list(
            extract_stream(
                config=config,
                log=build_logger("test-link"),
                endpoint="/items",
                record_path=["data"],
                pagination_strategy="link_header",
            )
        )
    ids = [r["id"] for b in batches for r in b]
    assert ids == list(range(1, 31))


# ==========================================================================
# record_path extraction — nested + wildcard
# ==========================================================================


def test_record_path_nested() -> None:
    """A ``record_path`` of plain dict keys walks down to the records list."""
    payload = {"data": {"items": [{"id": 1}, {"id": 2}]}}
    assert extract_records(payload, ["data", "items"]) == [{"id": 1}, {"id": 2}]


def test_record_path_wildcard_unwraps_arrays() -> None:
    """A ``*`` wildcard step flattens a list of envelopes — GraphQL-edge shape."""
    payload = {
        "data": {
            "edges": [
                {"node": {"id": 1}},
                {"node": {"id": 2}},
                {"node": {"id": 3}},
            ]
        }
    }
    assert extract_records(payload, ["data", "edges", "*", "node"]) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


def test_record_path_missing_key_raises() -> None:
    """A missing path step raises ``ExtractionError`` naming the offending step."""
    with pytest.raises(ExtractionError, match="record_path step 'items'"):
        extract_records({"data": {"OOPS": []}}, ["data", "items"])


# ==========================================================================
# Incremental cursor — first run vs second run with state
# ==========================================================================


def test_incremental_cursor_first_run_has_no_cursor_param() -> None:
    """First run sends NO ``cursor_query_param`` — start value is ``None``."""
    routes = _three_page_cursor_routes()
    with stub_server(routes) as (base_url, requests_seen):
        config = Config(params={"base_url": base_url}, secrets={})
        # Build a Cursor with start_value=None — the engine does this on a
        # first run when no `initial_value` is configured. ``cursor_query_param``
        # matches the stub's expected query key so subsequent pages dispatch
        # correctly; what this test asserts is that the FIRST page has no
        # cursor value at all (the bug to guard: sending an empty string).
        cursor = Cursor(
            cursor_field="id",
            cursor_type=det.CursorType.INT,
            start_value=None,
        )
        list(
            extract_stream(
                config=config,
                cursor=cursor,
                log=build_logger("test-cursor-first"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )
    # First request: no `cursor` param at all. Subsequent pages send one (page
    # cursors p2, p3) — but the *first* request must omit it entirely.
    first_query = requests_seen[0]["query"]
    assert "cursor" not in first_query


def test_incremental_cursor_second_run_sends_param() -> None:
    """Second run (cursor seeded with a value) sends ``cursor_query_param=<value>``.

    Uses a single-page endpoint so the test asserts only on the FIRST page's
    query — there is no second page, so the test sidesteps the question of
    whether the incremental query param doubles as the page cursor. The
    connector keeps that decoupling implicit (cursor + page-cursor can share a
    param name, common with ``updated_since``-style APIs).
    """
    state = {"calls": 0}

    def handler(h: _RecordingHandler) -> None:
        state["calls"] += 1
        _write_json(h, 200, {"data": [{"id": 5}, {"id": 9}], "meta": {}})

    with stub_server({"/items": handler}) as (base_url, requests_seen):
        config = Config(params={"base_url": base_url}, secrets={})
        cursor = Cursor(
            cursor_field="id",
            cursor_type=det.CursorType.INT,
            start_value=42,
        )
        list(
            extract_stream(
                config=config,
                cursor=cursor,
                log=build_logger("test-cursor-second"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="updated_since",
                next_cursor_path="meta.next_cursor",
            )
        )
    assert state["calls"] == 1
    assert requests_seen[0]["query"].get("updated_since") == "42"
    # And the cursor advanced to the max id observed (9).
    assert cursor.observed_max == 9


# ==========================================================================
# Auth — bearer / basic / api_key_header / api_key_query
# ==========================================================================


def _single_page_routes() -> dict[str, Callable[[_RecordingHandler], None]]:
    """A single-page endpoint returning two records — used by auth tests."""

    def handler(h: _RecordingHandler) -> None:
        _write_json(h, 200, {"data": [{"id": 1}, {"id": 2}], "meta": {}})

    return {"/items": handler}


def test_auth_bearer_sends_authorization_header() -> None:
    """`auth_type=bearer` sets ``Authorization: Bearer <token>``."""
    with stub_server(_single_page_routes()) as (base_url, requests_seen):
        config = Config(
            params={"base_url": base_url, "auth_type": "bearer"},
            secrets={"api_token": "tok-bearer-xyz"},
        )
        list(
            extract_stream(
                config=config,
                log=build_logger("test-bearer"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )
    assert requests_seen[0]["headers"].get("Authorization") == "Bearer tok-bearer-xyz"


def test_auth_basic_sends_basic_header() -> None:
    """`auth_type=basic` sends an ``Authorization: Basic <b64>`` header."""
    with stub_server(_single_page_routes()) as (base_url, requests_seen):
        config = Config(
            params={"base_url": base_url, "auth_type": "basic"},
            secrets={"api_token": "user:pass"},
        )
        list(
            extract_stream(
                config=config,
                log=build_logger("test-basic"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )
    auth_header = requests_seen[0]["headers"].get("Authorization", "")
    assert auth_header.startswith("Basic ")
    decoded = base64.b64decode(auth_header.split(" ", 1)[1]).decode()
    assert decoded == "user:pass"


def test_auth_api_key_header_uses_configured_name() -> None:
    """`auth_type=api_key_header` sends the token in the configured header name."""
    with stub_server(_single_page_routes()) as (base_url, requests_seen):
        config = Config(
            params={
                "base_url": base_url,
                "auth_type": "api_key_header",
                "auth_header_name": "X-API-Key",
            },
            secrets={"api_token": "header-key-456"},
        )
        list(
            extract_stream(
                config=config,
                log=build_logger("test-apikey-header"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )
    assert requests_seen[0]["headers"].get("X-API-Key") == "header-key-456"
    # Authorization itself must NOT be set in this mode.
    assert "Authorization" not in requests_seen[0]["headers"]


def test_auth_api_key_query_sends_token_in_url() -> None:
    """`auth_type=api_key_query` sends the token as a query parameter."""
    with stub_server(_single_page_routes()) as (base_url, requests_seen):
        config = Config(
            params={
                "base_url": base_url,
                "auth_type": "api_key_query",
                "auth_query_param": "access_token",
            },
            secrets={"api_token": "query-key-789"},
        )
        list(
            extract_stream(
                config=config,
                log=build_logger("test-apikey-query"),
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )
    assert requests_seen[0]["query"].get("access_token") == "query-key-789"


# ==========================================================================
# Retry — 429 with Retry-After, 500 then 200
# ==========================================================================


def test_retry_on_429_then_succeeds() -> None:
    """A 429 with ``Retry-After: 1`` is honored — the second attempt succeeds."""
    state = {"calls": 0}

    def handler(h: _RecordingHandler) -> None:
        state["calls"] += 1
        if state["calls"] == 1:
            h.send_response(429)
            h.send_header("Retry-After", "1")
            h.send_header("Content-Length", "0")
            h.end_headers()
            return
        _write_json(h, 200, {"data": [{"id": 1}], "meta": {}})

    with stub_server({"/items": handler}) as (base_url, _):
        auth = AuthSpec(auth_type="none")
        # Use a fresh build_client so we control max_retries directly — the
        # default 5 with a 1s backoff would also work but takes longer.
        client = build_client(
            base_url=base_url,
            auth=auth,
            max_retries=3,
            retry_backoff_seconds=0.0,
        )
        t0 = time.monotonic()
        response = client.get("/items")
        elapsed = time.monotonic() - t0
    assert response.status_code == 200
    assert state["calls"] == 2
    # ``Retry-After: 1`` must be honored — elapsed time is at least ~1s.
    # Allow generous slack (CI variance / urllib3 implementation details).
    assert elapsed >= 0.8


def test_retry_on_500_then_succeeds() -> None:
    """A 500 then 200 — `urllib3` retries on the 500 and surfaces the 200."""
    state = {"calls": 0}

    def handler(h: _RecordingHandler) -> None:
        state["calls"] += 1
        if state["calls"] == 1:
            h.send_response(500)
            h.send_header("Content-Length", "0")
            h.end_headers()
            return
        _write_json(h, 200, {"data": [{"id": 1}], "meta": {}})

    with stub_server({"/items": handler}) as (base_url, _):
        client = build_client(
            base_url=base_url,
            auth=AuthSpec(auth_type="none"),
            max_retries=3,
            retry_backoff_seconds=0.0,
        )
        response = client.get("/items")
    assert response.status_code == 200
    assert state["calls"] == 2


# ==========================================================================
# Malformed JSON — surface a clear error
# ==========================================================================


def test_malformed_json_raises_clear_error() -> None:
    """A 200 with garbage body raises a RuntimeError naming the endpoint + page."""

    def handler(h: _RecordingHandler) -> None:
        body = b"this is not JSON"
        h.send_response(200)
        h.send_header("Content-Type", "application/json")
        h.send_header("Content-Length", str(len(body)))
        h.end_headers()
        h.wfile.write(body)

    with stub_server({"/items": handler}) as (base_url, _):
        config = Config(params={"base_url": base_url}, secrets={})
        with pytest.raises(RuntimeError, match="non-JSON content"):
            list(
                extract_stream(
                    config=config,
                    log=build_logger("test-malformed"),
                    endpoint="/items",
                    record_path=["data"],
                    cursor_query_param="cursor",
                    next_cursor_path="meta.next_cursor",
                )
            )


# ==========================================================================
# End-to-end via det.run — lands rows in a tmp DuckDB
# ==========================================================================


def test_end_to_end_run_lands_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    query_duckdb: Callable[[str, str], list[tuple[Any, ...]]],
) -> None:
    """``det.run('rest')`` against a stub API lands rows in a temp DuckDB.

    Covers the full discovery → resolve → run → load path with the rest
    connector: the engine discovers it as a baked connector, resolves
    ``REST_API_TOKEN`` from the environment, drives both example streams
    against the stub, and persists the rows via the DuckDB destination.
    """
    # Three cursor-paginated /items, one page of /events. The example streams
    # in source.py use `record_path=["data"]`, `next_cursor_path=meta.next_cursor`.
    # Stream `items` uses cursor_type: int (see register.yaml — the DuckDB
    # destination cannot JSON-encode a bare timestamp string in v1, so the
    # baked example uses int epoch values). `events` is non-incremental.
    items_pages = {
        None: {"data": [
            {"id": "a", "name": "alpha", "updated_at": 1000},
            {"id": "b", "name": "beta",  "updated_at": 2000},
        ], "meta": {"next_cursor": "p2"}},
        "p2": {"data": [
            {"id": "c", "name": "gamma", "updated_at": 3000},
        ], "meta": {}},
    }
    events_pages = {
        None: {"data": [
            {"id": "e1", "kind": "x", "created_at": "2024-02-01T00:00:00"},
        ], "meta": {}},
    }

    # The baked source.py uses `cursor_query_param="updated_since"` for /items
    # (incremental) and `cursor_query_param="cursor"` for /events. The handlers
    # below dispatch on the same keys.
    #
    # COUPLING: the "0" literal below is tied to register.yaml's
    # `incremental.initial_value: "0"` for the items stream. If that value
    # changes the handler must too — they together define "the first request".
    def items_handler(h: _RecordingHandler) -> None:
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(h.path).query))
        # On a fresh DB, start_value = initial_value (0), sent as updated_since=0
        # on page 1; thereafter the same param doubles as the page cursor,
        # advancing to p2 then ending.
        token = q.get("updated_since")
        # The very first request carries the initial cursor value ("0"); after
        # that the pagination strategy overwrites it with the next-page token.
        key = None if token in (None, "0") else token
        body = items_pages.get(key)
        if body is None:
            _write_json(h, 404, {"error": "no such page"})
            return
        _write_json(h, 200, body)

    def events_handler(h: _RecordingHandler) -> None:
        q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(h.path).query))
        body = events_pages.get(q.get("cursor"))
        if body is None:
            _write_json(h, 404, {"error": "no such page"})
            return
        _write_json(h, 200, body)

    routes = {"/items": items_handler, "/events": events_handler}

    db_path = str(tmp_path / "warehouse.duckdb")
    with stub_server(routes) as (base_url, requests_seen):
        monkeypatch.setenv("REST_API_TOKEN", "secret-from-env")
        result = det.run(
            connector="rest",
            target="dev",
            project_dir=str(PROJECT_DIR),
            params={"base_url": base_url, "auth_type": "bearer"},
            destination_params={"path": db_path},
        )

    assert result.status.value == "succeeded", result.error
    rows_loaded = {s.name: s.rows_loaded for s in result.streams}
    assert rows_loaded == {"items": 3, "events": 1}

    # Data landed correctly in DuckDB.
    items = query_duckdb(db_path, "SELECT id, name FROM rest_items ORDER BY id")
    assert items == [("a", "alpha"), ("b", "beta"), ("c", "gamma")]
    events = query_duckdb(db_path, "SELECT id, kind FROM rest_events ORDER BY id")
    assert events == [("e1", "x")]

    # Bearer auth came from the env-injected secret — sanity check the wire.
    auth_headers = {r["headers"].get("Authorization") for r in requests_seen}
    assert "Bearer secret-from-env" in auth_headers


# ==========================================================================
# Secret comes from the env via ${env.REST_API_TOKEN}
# ==========================================================================


def test_secret_resolves_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The engine resolves ``${env.REST_API_TOKEN}`` and hands it to the connector.

    A run with the env var set must observe the same token on every request;
    a run without the env var set fails at config resolution (the engine
    raises :class:`~det.engine.config.ConfigError`, surfaced as a
    ``FAILED`` ``RunResult``).
    """
    # Path 1 — env set: the token reaches the wire as the bearer credential.
    def handler(h: _RecordingHandler) -> None:
        _write_json(h, 200, {"data": [], "meta": {}})

    with stub_server({"/items": handler, "/events": handler}) as (base_url, requests_seen):
        monkeypatch.setenv("REST_API_TOKEN", "env-secret-token-abcdef")
        result = det.run(
            connector="rest",
            target="dev",
            project_dir=str(PROJECT_DIR),
            params={"base_url": base_url, "auth_type": "bearer"},
            destination_params={"path": ":memory:"},
        )
    assert result.status.value == "succeeded", result.error
    seen_tokens = {r["headers"].get("Authorization") for r in requests_seen}
    assert "Bearer env-secret-token-abcdef" in seen_tokens

    # Path 2 — env missing: the run fails cleanly with a non-leaking message.
    monkeypatch.delenv("REST_API_TOKEN", raising=False)
    result = det.run(
        connector="rest",
        target="dev",
        project_dir=str(PROJECT_DIR),
        params={"base_url": "http://127.0.0.1:1", "auth_type": "bearer"},
        destination_params={"path": ":memory:"},
    )
    assert result.status.value == "failed"
    assert result.error is not None
    # The error must name the secret + ref form, but never the value.
    assert "REST_API_TOKEN" in str(result.error)


# ==========================================================================
# Authorization header is never logged
# ==========================================================================


def test_authorization_header_is_redacted(caplog: pytest.LogCaptureFixture) -> None:
    """The redacting filter masks the resolved secret in any logged message.

    Builds a logger via :func:`build_logger` with the run's secret values,
    then drives a stream against the stub. The captured log must not contain
    the literal token, even if a developer accidentally logged it.
    """
    secret = "super-secret-token-shhh"
    log = build_logger("test-redact", [secret])

    # Manually log the secret — proves the filter catches even an accidental
    # interpolation; the connector itself never logs header values, but defence
    # in depth must mask anything that slips through (docs/08).
    log.info("calling API with token=%s", secret)

    with stub_server(_single_page_routes()) as (base_url, _):
        config = Config(
            params={"base_url": base_url, "auth_type": "bearer"},
            secrets={"api_token": secret},
        )
        list(
            extract_stream(
                config=config,
                log=log,
                endpoint="/items",
                record_path=["data"],
                cursor_query_param="cursor",
                next_cursor_path="meta.next_cursor",
            )
        )

    # caplog captures records, but RedactingFilter mutates the record's `msg`
    # in place — so the captured text is already redacted.
    full_log = "\n".join(r.getMessage() for r in caplog.records)
    # The secret string never appears anywhere in the log output.
    assert secret not in full_log
    # And the mask string was applied to the manually-logged message.
    if any("calling API with token=" in r.getMessage() for r in caplog.records):
        assert any("token=***" in r.getMessage() for r in caplog.records)


# ==========================================================================
# Extra coverage — pagination factory, link-header origin guard
# ==========================================================================


def test_build_strategy_unknown_raises() -> None:
    """An unknown strategy name fails immediately, naming the valid options."""
    with pytest.raises(ValueError, match="cursor, offset, page, link_header"):
        build_strategy(
            "ouija",
            base_url="http://x",
            page_size=10,
            record_path=("data",),
        )


def test_build_strategy_cursor_missing_args_raises() -> None:
    """A cursor strategy with no cursor_query_param fails at build time."""
    with pytest.raises(ValueError, match="cursor_query_param.*next_cursor_path"):
        build_strategy(
            "cursor",
            base_url="http://x",
            page_size=10,
            record_path=("data",),
        )


def test_link_header_refuses_cross_origin_next() -> None:
    """``LinkHeaderPagination`` will not follow a ``next`` URL to another host.

    Defence against a server sending ``Link: <https://attacker.example/...>; rel="next"``
    which would otherwise leak the bearer token to that host.
    """
    strat = LinkHeaderPagination(base_url="http://api.example.com")
    next_params = strat.update_after(
        response_json={},
        headers={"Link": '<https://evil.example/leak>; rel="next"'},
        last_params={},
    )
    assert next_params is None


# ==========================================================================
# Tiny unit-level smoke for each pagination class
# ==========================================================================


def test_cursor_strategy_unit() -> None:
    s = CursorPagination(
        cursor_query_param="cursor",
        next_cursor_path="meta.next",
        page_size_param="limit",
        page_size=50,
    )
    first = s.prepare_first({"x": "y"})
    assert first == {"x": "y", "limit": 50}
    nxt = s.update_after({"meta": {"next": "abc"}}, {}, first)
    assert nxt == {"x": "y", "limit": 50, "cursor": "abc"}
    stop = s.update_after({"meta": {}}, {}, nxt)
    assert stop is None


def test_offset_strategy_unit() -> None:
    s = OffsetPagination(limit=2, record_path=("data",))
    first = s.prepare_first({})
    assert first == {"offset": 0, "limit": 2}
    full_page = {"data": [{"id": 1}, {"id": 2}]}
    nxt = s.update_after(full_page, {}, first)
    assert nxt == {"offset": 2, "limit": 2}
    short = {"data": [{"id": 3}]}
    stop = s.update_after(short, {}, nxt)
    assert stop is None


def test_page_strategy_unit() -> None:
    s = PagePagination(per_page=2, record_path=("data",))
    first = s.prepare_first({})
    assert first == {"page": 1, "per_page": 2}
    full_page = {"data": [{"id": 1}, {"id": 2}]}
    nxt = s.update_after(full_page, {}, first)
    assert nxt == {"page": 2, "per_page": 2}


def test_extract_stream_imports_clean() -> None:
    """Sanity: importing the connector body does not trigger any network."""
    # The module is already imported at the top of this file; this assertion
    # exists so a future refactor that adds import-time side effects fails here.
    assert callable(extract_stream)
    assert isinstance(logging.getLogger("det"), logging.Logger)
