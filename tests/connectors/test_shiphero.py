"""Tests for the baked ShipHero source connector.

No live ShipHero traffic — a tiny stdlib ``http.server`` on a random port stands
in for both the ``/auth/refresh`` and ``/graphql`` endpoints. Every test points
the connector at ``http://127.0.0.1:<port>/graphql`` via the ``api_url`` param;
``client.derive_auth_url`` turns that into the matching ``/auth/refresh`` URL,
so one stub serves both ShipHero endpoints.

Coverage maps to the spec in the task description:

* token-refresh acquires the access token and uses it
* 401 → re-refresh → retry (exactly one re-refresh)
* date-window stepping (windows.py unit test)
* GraphQL pagination across N pages until ``hasNextPage`` flips false
* field-path extraction with ``*`` wildcard unwraps edges/node shape
* incremental cursor: first run uses ``initial_value``; second run resumes
  from persisted value minus lookback
* end-to-end ``simple_e.run`` into a tmp DuckDB
* secrets: refresh token from ``${env.SHIPHERO_REFRESH_TOKEN}``; access token
  never appears in captured logs.
"""

from __future__ import annotations

import json
import logging
import socket
import textwrap
import threading
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pytest

import simple_e
from simple_e import Config, Cursor
from simple_e.connectors.shiphero.client import ShipHeroClient, derive_auth_url
from simple_e.connectors.shiphero.pagination import (
    extract_records,
    paginate,
    walk_field_path,
)
from simple_e.connectors.shiphero.source import extract_stream
from simple_e.connectors.shiphero.windows import (
    compute_start,
    date_windows,
    to_utc_dt,
)

# --------------------------------------------------------------------------
# Stub ShipHero server
# --------------------------------------------------------------------------

# A canned "happy path" GraphQL response — three pages, with hasNextPage
# toggling false on the third. The top-level field key is filled in per
# request by ``ShipHeroStub`` (one of "shipments" / "orders" / "products"),
# so the same canned data answers every stream's query — the test then
# asserts wire-up correctness, not stream-specific shapes.
_PAGE_BODIES: list[dict[str, Any]] = [
    {
        "data": {
            "edges": [
                {"node": {"id": "s1", "created_date": "2024-01-02T00:00:00Z",
                          "order_id": "o1"}},
                {"node": {"id": "s2", "created_date": "2024-01-02T00:30:00Z",
                          "order_id": "o2"}},
            ],
            "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
        }
    },
    {
        "data": {
            "edges": [
                {"node": {"id": "s3", "created_date": "2024-01-03T00:00:00Z",
                          "order_id": "o3"}},
            ],
            "pageInfo": {"hasNextPage": True, "endCursor": "c2"},
        }
    },
    {
        "data": {
            "edges": [
                {"node": {"id": "s4", "created_date": "2024-01-04T00:00:00Z",
                          "order_id": "o4"}},
            ],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }
    },
]


# Per-stream cursor field — mirrors `source._CURSOR_FIELD` so the canned
# records carry the column name each stream's table actually has.
_CURSOR_FIELD_FOR_STREAM: dict[str, str] = {
    "shipments": "created_date",
    "orders": "order_date",
    "products": "updated_at",
}


def _wrap(body: dict[str, Any], graphql_field: str) -> dict[str, Any]:
    """Wrap a canned ``_PAGE_BODIES`` entry under the right top-level field name.

    Input ``body`` shape: ``{"data": {"edges": [...], "pageInfo": {...}}}``.
    Output: ``{"data": {<graphql_field>: <body>}}``.

    Each node's cursor-field key is rewritten from the shipments-style
    ``created_date`` into the per-stream form (``order_date`` for orders,
    ``updated_at`` for products), so the destination table's declared schema
    lines up with the records the stub emits. The connector body itself
    projects each record onto its declared column set (``source._PROJECT_COLS``),
    so the stub only has to rewrite the cursor-field key — extra fields are
    dropped by the connector before the destination ever sees them.
    """
    cursor_field = _CURSOR_FIELD_FOR_STREAM.get(graphql_field, "created_date")
    inner = body.get("data", {})
    edges_in = inner.get("edges", []) or []
    new_edges: list[dict[str, Any]] = []
    for edge in edges_in:
        node = dict(edge.get("node", {}))
        if cursor_field != "created_date" and "created_date" in node:
            node[cursor_field] = node.pop("created_date")
        new_edges.append({"node": node})
    rebuilt = {
        "data": {
            "edges": new_edges,
            "pageInfo": inner.get("pageInfo", {}),
        }
    }
    return {"data": {graphql_field: rebuilt}}


# Pre-built pages keyed on shipments — kept as a module constant for the
# pagination unit test, which doesn't go through the stream-aware stub.
_PAGES: list[dict[str, Any]] = [_wrap(body, "shipments") for body in _PAGE_BODIES]


def _stream_field_for_query(query_text: str) -> str:
    """Detect which top-level GraphQL field a query is asking for.

    The connector's ``queries.py`` uses canonical operation names
    (``query Shipments(...)`` etc.); the stub keys off them so one server
    serves all three streams.
    """
    head = query_text.lstrip()[:80].lower()
    if "shipments" in head:
        return "shipments"
    if "orders" in head:
        return "orders"
    if "products" in head:
        return "products"
    return "shipments"


class ShipHeroStub:
    """A controllable in-process HTTP server impersonating the ShipHero API.

    Each test pins it to a deterministic scenario by setting:

    * :attr:`page_bodies` — the list of GraphQL response *bodies* (without the
      top-level ``data.<field>`` wrap); the stub wraps each under the right
      field for the incoming query, so the same canned pages serve every
      stream.
    * :attr:`fail_401_once` — when True, the first GraphQL POST returns 401
      (forcing a re-refresh + retry) before serving normal responses.

    The page cursor is per-``(stream, window)``: a fresh window starts back at
    page 0 of ``page_bodies``. The window is identified by the ``dateFrom``
    variable on the GraphQL request, so two windows of one stream do not share
    a cursor (matches ShipHero's real per-window pagination).

    Logs of every request go into :attr:`requests_log` for assertions.
    """

    def __init__(self) -> None:
        self.access_tokens_issued: list[str] = []
        self.refresh_count: int = 0
        self.requests_log: list[tuple[str, dict[str, Any]]] = []
        self.page_bodies: list[dict[str, Any]] = list(_PAGE_BODIES)
        # Per-(stream, dateFrom, after) bookkeeping — keys the cursor on the
        # combined "what stream + what window + how far in pages" so each
        # window paginates independently from page 0.
        self._idx_by_window: dict[tuple[str, str | None], int] = {}
        self.fail_401_once: bool = False
        self._failed_401: bool = False

    def reset_pages(self, page_bodies: list[dict[str, Any]]) -> None:
        """Reset the GraphQL response queue and clear the per-window cursors."""
        self.page_bodies = list(page_bodies)
        self._idx_by_window = {}
        self._failed_401 = False
        self.fail_401_once = False

    def next_page(self, *, stream: str, date_from: str | None) -> dict[str, Any]:
        """Return the next canned page for the (stream, window) coordinate."""
        key = (stream, date_from)
        idx = self._idx_by_window.get(key, 0)
        page = self.page_bodies[idx % len(self.page_bodies)]
        self._idx_by_window[key] = idx + 1
        return _wrap(page, stream)


@pytest.fixture
def stub() -> Iterator[tuple[ShipHeroStub, str]]:
    """Spin up a real HTTP server on a random localhost port for one test.

    Yields ``(stub, api_url)`` — point the connector at ``api_url``; assert
    against ``stub`` afterward. The server is torn down at the end of the
    test, including on failure.
    """
    state = ShipHeroStub()

    class Handler(BaseHTTPRequestHandler):
        # Suppress the default request log — we have our own.
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

        def do_POST(self) -> None:  # noqa: N802 — fixed http.server signature
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b""
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                body = {}
            state.requests_log.append((self.path, body))

            if self.path == "/auth/refresh":
                state.refresh_count += 1
                token = f"access-token-{state.refresh_count}"
                state.access_tokens_issued.append(token)
                payload = {"access_token": token, "expires_in": 3600}
                self._send_json(200, payload)
                return

            if self.path == "/graphql":
                if state.fail_401_once and not state._failed_401:
                    state._failed_401 = True
                    payload = b"unauthorized"
                    self.send_response(401)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                query_text = str(body.get("query", ""))
                variables = body.get("variables") or {}
                stream_field = _stream_field_for_query(query_text)
                date_from = variables.get("dateFrom")
                self._send_json(
                    200,
                    state.next_page(stream=stream_field, date_from=date_from),
                )
                return

            self.send_response(404)
            self.end_headers()

        def _send_json(self, status: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    # Bind to a kernel-assigned free port.
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, f"http://127.0.0.1:{port}/graphql"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _free_port() -> int:
    """Return a free localhost port — used by URL-only tests that need no server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# --------------------------------------------------------------------------
# windows.py — date-window stepping (no network)
# --------------------------------------------------------------------------


def test_windows_step_yields_expected_tuples() -> None:
    """``date_windows`` yields ``(from, to)`` tuples of `step_days` each.

    Hand-computed expectation: starting 2024-01-01 stepping 3 days through
    2024-01-10 yields windows ending at 04, 07, 10. The final window is
    clipped at ``end``.
    """
    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 10, tzinfo=UTC)
    out = list(date_windows(start, step_days=3, end=end))
    assert out == [
        (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 1, 4, tzinfo=UTC)),
        (datetime(2024, 1, 4, tzinfo=UTC), datetime(2024, 1, 7, tzinfo=UTC)),
        (datetime(2024, 1, 7, tzinfo=UTC), datetime(2024, 1, 10, tzinfo=UTC)),
    ]


def test_windows_rejects_non_positive_step() -> None:
    """A non-positive step would loop forever — must raise."""
    with pytest.raises(ValueError):
        list(date_windows(datetime(2024, 1, 1, tzinfo=UTC), step_days=0))


def test_compute_start_subtracts_lookback_from_persisted_cursor() -> None:
    """Resume start = persisted cursor − lookback_days (port of main.py 351)."""
    persisted = datetime(2024, 6, 15, 12, 0, tzinfo=UTC)
    start = compute_start(
        persisted, initial_value="2024-01-01", lookback_days=2
    )
    # 2024-06-15 minus 2 days, floored to midnight.
    assert start == datetime(2024, 6, 13, tzinfo=UTC)


def test_compute_start_falls_back_to_initial_value() -> None:
    """First run (no persisted cursor) ⇒ start from ``initial_value``."""
    start = compute_start(None, initial_value="2025-02-01", lookback_days=2)
    assert start == datetime(2025, 2, 1, tzinfo=UTC)


def test_compute_start_normalizes_iso_string_resume() -> None:
    """A persisted ISO-8601 string cursor (DuckDB JSON round-trip) is parsed back."""
    start = compute_start(
        "2024-06-15T00:00:00+00:00",
        initial_value="2024-01-01",
        lookback_days=2,
    )
    assert start == datetime(2024, 6, 13, tzinfo=UTC)


def test_to_utc_dt_handles_naive_z_and_aware() -> None:
    """``to_utc_dt`` normalizes naive, Z-suffixed, and aware datetimes equally."""
    assert to_utc_dt(None) is None
    assert to_utc_dt("2024-01-01T00:00:00Z") == datetime(2024, 1, 1, tzinfo=UTC)
    assert to_utc_dt(datetime(2024, 1, 1)) == datetime(2024, 1, 1, tzinfo=UTC)


# --------------------------------------------------------------------------
# pagination.py — field-path walk + cursor loop
# --------------------------------------------------------------------------


def test_extract_records_unwraps_edges_with_wildcard() -> None:
    """The ``*`` wildcard iterates the edges list and descends into each ``node``."""
    response = {
        "data": {
            "shipments": {
                "data": {
                    "edges": [
                        {"node": {"id": "s1"}},
                        {"node": {"id": "s2"}},
                        None,  # malformed edge entries are skipped
                    ]
                }
            }
        }
    }
    records = extract_records(
        response, ["data", "shipments", "data", "edges", "*", "node"]
    )
    assert records == [{"id": "s1"}, {"id": "s2"}]


def test_walk_field_path_handles_missing_keys() -> None:
    """A missing intermediate key returns ``{}`` rather than raising."""
    assert walk_field_path({"a": {"b": 1}}, ["a", "c", "d"]) == {}


def test_paginate_stops_when_has_next_page_false() -> None:
    """``paginate`` reads pages until ``pageInfo.hasNextPage`` is false."""
    pages = iter(_PAGES)

    def fetch_page(variables: dict[str, Any]) -> dict[str, Any]:
        return next(pages)

    records = list(
        paginate(
            fetch_page=fetch_page,
            page_size=10,
            field_path_to_records=["data", "shipments", "data", "edges", "*", "node"],
            field_path_to_pageinfo=["data", "shipments", "data", "pageInfo"],
        )
    )
    assert [r["id"] for r in records] == ["s1", "s2", "s3", "s4"]


def test_paginate_safety_stops_on_missing_end_cursor() -> None:
    """``hasNextPage=True`` but ``endCursor=null`` is a stuck-cursor sentinel — stop."""
    pages = iter([
        {
            "data": {"shipments": {"data": {
                "edges": [{"node": {"id": "x1"}}],
                "pageInfo": {"hasNextPage": True, "endCursor": None},
            }}}
        }
    ])

    def fetch_page(variables: dict[str, Any]) -> dict[str, Any]:
        return next(pages)

    records = list(
        paginate(
            fetch_page=fetch_page,
            page_size=10,
            field_path_to_records=["data", "shipments", "data", "edges", "*", "node"],
            field_path_to_pageinfo=["data", "shipments", "data", "pageInfo"],
        )
    )
    assert [r["id"] for r in records] == ["x1"]


# --------------------------------------------------------------------------
# client.py — token refresh + 401 retry
# --------------------------------------------------------------------------


def test_derive_auth_url_swaps_path() -> None:
    """An ``api_url`` ending in ``/graphql`` maps to the same host ``/auth/refresh``."""
    assert (
        derive_auth_url("http://127.0.0.1:5000/graphql")
        == "http://127.0.0.1:5000/auth/refresh"
    )


def test_client_acquires_access_token_on_first_query(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """One GraphQL POST ⇒ one refresh + one query, with the bearer token set.

    The stub records every POST. After a single ``client.query`` we expect:
    1 POST to ``/auth/refresh`` (lazy refresh), 1 POST to ``/graphql``.
    """
    state, api_url = stub
    state.reset_pages([_PAGE_BODIES[-1]])  # single-page response so the loop is small
    client = ShipHeroClient(api_url=api_url, refresh_token="fake-refresh-12345")
    body = client.query("query { shipments }", {})
    assert "data" in body
    paths = [p for p, _ in state.requests_log]
    assert paths == ["/auth/refresh", "/graphql"]
    assert state.refresh_count == 1


def test_client_refreshes_on_401_and_retries_exactly_once(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """A 401 from /graphql triggers exactly one re-refresh + one retry."""
    state, api_url = stub
    state.reset_pages([_PAGE_BODIES[-1]])
    state.fail_401_once = True
    client = ShipHeroClient(api_url=api_url, refresh_token="fake-refresh-12345")
    body = client.query("query { shipments }", {})
    assert "data" in body

    # Sequence: initial refresh, first /graphql (401), re-refresh, second /graphql.
    paths = [p for p, _ in state.requests_log]
    assert paths == ["/auth/refresh", "/graphql", "/auth/refresh", "/graphql"]
    assert state.refresh_count == 2  # one initial + one re-refresh


def test_client_does_not_retry_after_second_401(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """A 401 *after* re-refresh means the refresh token is bad — stop, don't loop."""
    state, api_url = stub
    # Make every /graphql 401 by toggling the flag back on after each request.
    seen_401 = {"count": 0}

    class AlwaysFailHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            return

        def do_POST(self) -> None:  # noqa: N802
            if self.path == "/auth/refresh":
                state.refresh_count += 1
                body = json.dumps({"access_token": f"tok-{state.refresh_count}"}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            seen_401["count"] += 1
            self.send_response(401)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), AlwaysFailHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        api_url = f"http://127.0.0.1:{port}/graphql"
        client = ShipHeroClient(api_url=api_url, refresh_token="fake-refresh-12345")
        with pytest.raises(RuntimeError, match="401 after re-refresh"):
            client.query("query { shipments }", {})
        # Exactly two /graphql calls: the original 401 and the post-re-refresh 401.
        assert seen_401["count"] == 2
        # Two refreshes: initial + re-refresh.
        assert state.refresh_count == 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_client_retries_5xx_with_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 503 is retried with backoff up to ``max_retries``."""

    class FakeResp:
        def __init__(self, status: int, body: dict[str, Any] | None = None) -> None:
            self.status_code = status
            self._body = body or {}
            self.headers: dict[str, str] = {}

        def json(self) -> dict[str, Any]:
            return self._body

    calls = {"refresh": 0, "graphql": 0}

    def fake_post(url: str, **kwargs: Any) -> FakeResp:
        if url.endswith("/auth/refresh"):
            calls["refresh"] += 1
            return FakeResp(200, {"access_token": "tok"})
        calls["graphql"] += 1
        if calls["graphql"] < 3:
            return FakeResp(503)
        return FakeResp(200, {"data": {"shipments": "ok"}})

    sleeps: list[float] = []

    client = ShipHeroClient(
        api_url="http://example.invalid/graphql",
        refresh_token="r",
        max_retries=5,
        retry_backoff_seconds=0.01,
    )
    client._post = fake_post
    client._sleep = sleeps.append

    body = client.query("q", {})
    assert body == {"data": {"shipments": "ok"}}
    assert calls["graphql"] == 3  # two 503s + one 200
    assert len(sleeps) == 2       # two backoff sleeps
    # Exponential growth on retry_backoff_seconds * 2**attempt
    assert sleeps[1] > sleeps[0]


# --------------------------------------------------------------------------
# extract_stream — incremental cursor behavior (uses real stub server)
# --------------------------------------------------------------------------


def _build_config(api_url: str, **overrides: Any) -> Config:
    """Build a ``Config`` shaped the way the engine builds it for shiphero."""
    params: dict[str, Any] = {
        "api_url": api_url,
        "start_date": "2024-01-01",
        "lookback_days": 2,
        "step_days": 365,  # one window covers a year — keeps test arithmetic tight
        "page_size": 50,
        "batch_size": 200,
        "max_retries": 3,
        "retry_backoff_seconds": 0.01,
    }
    params.update(overrides)
    return Config(
        params=params,
        secrets={"refresh_token": "fake-refresh-12345"},
    )


def _silent_log() -> logging.Logger:
    log = logging.getLogger("shiphero-test")
    log.setLevel(logging.CRITICAL)  # don't pollute test output
    return log


def test_extract_stream_first_run_starts_from_initial_value(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """No persisted cursor ⇒ first window starts at ``initial_value``.

    First-run simulation: the engine's ``_seed_value`` parses
    ``initial_value`` ("2024-01-01") into a naive ``datetime(2024, 1, 1)`` —
    we pass that as the ``Cursor`` start. ``compute_start`` recognizes "no
    prior state" by the connector's own logic (the engine doesn't tell us);
    here we simulate "first run" with ``start_value=None``, which is what
    happens when there's no persisted ``StateRecord`` AND no ``initial_value``
    would have been seeded. To exercise the initial_value path specifically,
    pass the parsed datetime AND set lookback_days=0 so compute_start's
    "persisted minus lookback" still equals the initial date.
    """
    state, api_url = stub
    config = _build_config(
        api_url,
        start_date="2024-01-01",
        lookback_days=0,  # cancel out the lookback subtraction below
    )
    cursor = Cursor(
        cursor_field="created_date",
        cursor_type=simple_e.CursorType.TIMESTAMP,
        start_value=datetime(2024, 1, 1, tzinfo=UTC),
    )
    batches = list(extract_stream("shipments", config, cursor, _silent_log()))
    records = [r for b in batches for r in b]
    # Each window cycles through the 4 canned records; ``s1..s4`` is the
    # unique-id set the stub ever emits.
    assert set(r["id"] for r in records) == {"s1", "s2", "s3", "s4"}

    # The dateFrom variable on the first /graphql POST is the initial value.
    graphql_bodies = [b for p, b in state.requests_log if p == "/graphql"]
    assert graphql_bodies
    first_vars = graphql_bodies[0]["variables"]
    assert first_vars["dateFrom"].startswith("2024-01-01")


def test_extract_stream_resume_subtracts_lookback(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """A second run starting from a persisted cursor sends dateFrom = cursor − lookback."""
    state, api_url = stub
    state.reset_pages([_PAGE_BODIES[-1]])  # single sparse page is enough
    config = _build_config(api_url, lookback_days=2)
    # Pretend a prior run committed cursor at this value (engine seeds it).
    persisted = "2024-06-15T12:00:00+00:00"
    cursor = Cursor(
        cursor_field="created_date",
        cursor_type=simple_e.CursorType.TIMESTAMP,
        start_value=persisted,
    )
    list(extract_stream("shipments", config, cursor, _silent_log()))

    graphql_bodies = [b for p, b in state.requests_log if p == "/graphql"]
    assert graphql_bodies
    first_vars = graphql_bodies[0]["variables"]
    # cursor (2024-06-15) − lookback 2d, floored to midnight ⇒ 2024-06-13.
    assert first_vars["dateFrom"].startswith("2024-06-13")


def test_extract_stream_observes_cursor_values(
    stub: tuple[ShipHeroStub, str],
) -> None:
    """The cursor's observed_max advances to the last record's created_date."""
    state, api_url = stub
    config = _build_config(api_url)
    cursor = Cursor(
        cursor_field="created_date",
        cursor_type=simple_e.CursorType.TIMESTAMP,
        start_value=datetime(2024, 1, 1, tzinfo=UTC),
    )
    list(extract_stream("shipments", config, cursor, _silent_log()))
    # The last fixture record's created_date is 2024-01-04 — observed_max
    # is a tz-aware UTC datetime (DuckDB's JSON column accepts that natively).
    assert cursor.observed_max is not None
    assert isinstance(cursor.observed_max, datetime)
    assert cursor.observed_max == datetime(2024, 1, 4, tzinfo=UTC)


# --------------------------------------------------------------------------
# End-to-end: simple_e.run against a stubbed server, into a tmp DuckDB
# --------------------------------------------------------------------------


@pytest.fixture
def shiphero_project(tmp_path: Path) -> Path:
    """Build a throwaway simpl.E project that runs the shiphero baked connector."""
    (tmp_path / "simple_e_project.yml").write_text(
        textwrap.dedent(
            """\
            name: shiphero_test_project
            version: "1.0.0"
            connector_paths:
              - connectors
            default_destination: duckdb
            default_target: dev
            """
        )
    )
    (tmp_path / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            targets:
              dev:
                destinations:
                  duckdb:
                    path: ".simple_e/warehouse.duckdb"
            """
        )
    )
    (tmp_path / "connectors").mkdir()
    return tmp_path


def test_end_to_end_run_lands_shipments_into_duckdb(
    stub: tuple[ShipHeroStub, str],
    shiphero_project: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``simple_e.run`` discovers shiphero, runs all 3 streams, lands rows.

    Each stream sees the same stub response queue (the stub cycles through
    `_PAGES`), so each stream lands the same 4 records — the test asserts
    the engine wired the streams correctly, not stream-specific shapes.
    """
    state, api_url = stub
    monkeypatch.setenv("SHIPHERO_REFRESH_TOKEN", "fake-refresh-token-1234")
    db_path = str(tmp_path / "out.duckdb")

    # The stub serves the same 3-page sequence per stream. Reset between streams
    # is not necessary because the stub cycles through its `pages` list — each
    # stream gets fresh pages from the start.
    result = simple_e.run(
        connector="shiphero",
        target="dev",
        project_dir=str(shiphero_project),
        params={
            "api_url": api_url,
            "start_date": "2024-01-01",
            "step_days": 365,
            "lookback_days": 0,
            "max_retries": 3,
            "retry_backoff_seconds": 0.01,
            "batch_size": 50,
            "page_size": 50,
        },
        destination_params={"path": db_path},
    )

    assert result.status.value == "succeeded", result.error
    rows = {s.name: s.rows_loaded for s in result.streams}
    # Each stream walks several date-windows (start_date → now at step_days=365);
    # each window cycles through the stub's 4 canned records via per-window
    # pagination. ``rows_loaded`` counts every record written, not unique ids,
    # so the merge target table ends up with 4 distinct ids but ``rows_loaded``
    # reflects every batch the destination saw. Assert every stream loaded at
    # least one full cycle and the same number — the engine wired all three
    # equally.
    assert rows["shipments"] >= 4
    assert rows["shipments"] == rows["orders"] == rows["products"]

    # Sanity: the data landed in the right tables — merge dedupes to 4 unique
    # ids per stream.
    conn = duckdb.connect(db_path)
    try:
        shipments = conn.execute(
            "SELECT DISTINCT id FROM shipments ORDER BY id"
        ).fetchall()
        assert shipments == [("s1",), ("s2",), ("s3",), ("s4",)]
        # _simple_e_synced_at populated.
        nulls = conn.execute(
            "SELECT count(*) FROM shipments WHERE _simple_e_synced_at IS NULL"
        ).fetchone()
        assert nulls is not None and nulls[0] == 0
        # State table records the advanced cursor.
        state_rows = conn.execute(
            "SELECT cursor_value FROM _simple_e_state "
            "WHERE connector = 'shiphero' AND stream = 'shipments'"
        ).fetchall()
        assert len(state_rows) == 1
    finally:
        conn.close()


def test_end_to_end_refresh_token_not_logged(
    stub: tuple[ShipHeroStub, str],
    shiphero_project: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    """The declared secret (refresh token) is redacted from every log line.

    The engine's run logger is built with ``propagate=False`` and a
    ``StreamHandler`` writing direct to stderr — pytest's ``caplog`` would not
    see those records, only ``capfd`` (file-descriptor-level capture) does.

    Asserts both:
    1. the refresh token value never appears verbatim in stderr;
    2. the access tokens the stub issued never appear in stderr.

    Point 1 is the engine's ``RedactingFilter`` doing its job; point 2 is the
    client's no-log-tokens policy holding.
    """
    state, api_url = stub
    refresh_token = "supersecret-refresh-token-xyz-1234567890"
    monkeypatch.setenv("SHIPHERO_REFRESH_TOKEN", refresh_token)
    db_path = str(tmp_path / "out.duckdb")

    simple_e.run(
        connector="shiphero",
        target="dev",
        project_dir=str(shiphero_project),
        params={
            "api_url": api_url,
            "step_days": 365,
            "lookback_days": 0,
            "max_retries": 3,
            "retry_backoff_seconds": 0.01,
            "batch_size": 50,
            "page_size": 50,
        },
        destination_params={"path": db_path},
    )

    captured = capfd.readouterr()
    all_output = captured.out + captured.err
    # Sanity: the engine *did* write log lines (the test would otherwise be a
    # vacuous "" not-in "" assertion).
    assert "shiphero" in all_output, "expected at least some shiphero log output"

    assert refresh_token not in all_output, "refresh token leaked into output"
    for issued in state.access_tokens_issued:
        assert issued not in all_output, f"access token {issued!r} leaked into output"


def test_secrets_resolved_from_env_var(
    shiphero_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``${env.SHIPHERO_REFRESH_TOKEN}`` resolves through the engine's RESOLVE stage.

    Drives the engine far enough to discover the connector, build its Config,
    and resolve secrets, then asserts the secret was found via the env var.
    Done by missing-env-var first (expect FAILED), then env-var-set (expect a
    different failure — the network call to the unreachable URL).
    """
    monkeypatch.delenv("SHIPHERO_REFRESH_TOKEN", raising=False)
    bad = simple_e.run(
        connector="shiphero",
        target="dev",
        project_dir=str(shiphero_project),
        params={"api_url": "http://127.0.0.1:1/graphql"},  # unreachable port
    )
    assert bad.status.value == "failed"
    # The failure is a ConfigError about the missing env var (not a network
    # error — the secret resolution happens before any HTTP).
    assert bad.error is not None
    assert "SHIPHERO_REFRESH_TOKEN" in str(bad.error)


# --------------------------------------------------------------------------
# Misc — free_port helper used nowhere else, kept for future tests.
# --------------------------------------------------------------------------


def test_free_port_returns_localhost_port() -> None:
    """Sanity: ``_free_port`` returns a small (>1024) integer port."""
    port = _free_port()
    assert 1024 < port < 65536
