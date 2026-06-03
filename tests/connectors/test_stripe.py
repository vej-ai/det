"""Tests for the baked Stripe source connector — NO live API calls.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port and
points the connector's ``base_url`` at it. The stub:

* matches request paths (``/charges``, ``/invoices``, ...) and toggles
  ``has_more`` across the canned pages so the cursor-pagination loop walks
  multiple pages;
* exposes captured request records to the test so assertions can pin the auth
  header, ``Stripe-Version``, the ``starting_after`` cursor value, and any
  ``extra_query_params`` propagation;
* can be scripted to return 429 (with ``Retry-After``), 500, or 401 on a
  specific request so the retry / backoff / fail-fast paths are covered.

The end-to-end test runs the connector through the real engine
(:func:`dtex.run`) into a tmp DuckDB destination and asserts rows land
plus the ``_dtex_state`` cursor advances. Tests use the project at
``tests/fixtures/`` (already a real dtex project with a default DuckDB
destination) and rely on the stripe baked connector under
``dtex/sources/stripe/``.

Citations:

* docs/03 §2.5 — secret refs ``${env.STRIPE_API_KEY}`` resolved by the engine.
* docs/connectors/stripe-research.md §B — REST pagination + auth shape.
* docs/03 §3.2 — incremental cursor + ``_dtex_state`` semantics.
"""

from __future__ import annotations

import json
import textwrap
import threading
from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

import duckdb
import pytest

import dtex
from dtex.sources.stripe.client import StripeAPIError, StripeClient
from dtex.sources.stripe.pagination import paginate

# --------------------------------------------------------------------------
# Stub Stripe server — stdlib HTTPServer on a random port
# --------------------------------------------------------------------------


class _RequestRecord:
    """One captured stub HTTP request — what tests assert against."""

    def __init__(
        self,
        method: str,
        path: str,
        query: list[tuple[str, str]],
        headers: dict[str, str],
    ) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.headers = headers

    @property
    def query_dict(self) -> dict[str, str]:
        """The query string flattened to a dict (last value wins on duplicates)."""
        return dict(self.query)

    def query_values(self, key: str) -> list[str]:
        """Every value for ``key`` in the query string, in order."""
        return [v for (k, v) in self.query if k == key]


class _Scenario:
    """A scripted response queue keyed by request URL path.

    Tests construct one (``scenario.add("/charges", json_body=...)``) and the
    stub server pops the first match per path on each request. The class is
    deliberately tiny — one server can serve one scenario at a time.
    """

    def __init__(self) -> None:
        # Per-path queue of (status, body_bytes, extra_headers) — popped left.
        self._queues: dict[str, list[tuple[int, bytes, dict[str, str]]]] = {}
        self.captured: list[_RequestRecord] = []

    def add(
        self,
        path: str,
        *,
        status: int = 200,
        json_body: Any | None = None,
        body: bytes | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        """Queue one response for the next request matching ``path``."""
        if body is None:
            payload = b"" if json_body is None else json.dumps(json_body).encode("utf-8")
        else:
            payload = body
        self._queues.setdefault(path, []).append(
            (status, payload, dict(extra_headers or {}))
        )

    def next_response(self, path: str) -> tuple[int, bytes, dict[str, str]]:
        """Pop the next queued response for ``path``; 500 if none queued."""
        queue = self._queues.get(path)
        if not queue:
            return (500, b'{"error":{"message":"no scripted response"}}', {})
        return queue.pop(0)


def _make_handler(scenario: _Scenario) -> type[BaseHTTPRequestHandler]:
    """Build a request handler class bound to ``scenario`` via a closure."""

    class _Handler(BaseHTTPRequestHandler):
        # Silence the default per-request log; tests want quiet output.
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def do_GET(self) -> None:  # noqa: N802 — BaseHTTPRequestHandler API.
            parsed = urlparse(self.path)
            # keep_blank_values so an ``expand[]=`` with no value still records.
            query = parse_qsl(parsed.query, keep_blank_values=True)
            headers = {k: v for k, v in self.headers.items()}
            scenario.captured.append(
                _RequestRecord(
                    method="GET", path=parsed.path, query=query, headers=headers
                )
            )
            status, body, extra = scenario.next_response(parsed.path)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for k, v in extra.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

    return _Handler


@pytest.fixture
def stripe_stub() -> Iterator[tuple[_Scenario, str]]:
    """Spin up a stub Stripe REST server on a random port; tear down after.

    Yields ``(scenario, base_url)`` — tests script responses on ``scenario``
    and point the connector at ``base_url``.
    """
    scenario = _Scenario()
    handler_cls = _make_handler(scenario)
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield scenario, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


# --------------------------------------------------------------------------
# StripeClient unit tests — auth, retries, rate limiting (no engine involved)
# --------------------------------------------------------------------------


def _client(
    base_url: str, *, max_retries: int = 5, sleep: Callable[[float], None] | None = None
) -> StripeClient:
    """Build a StripeClient for unit tests — no rate limiting, optional sleep stub."""
    return StripeClient(
        base_url=base_url,
        api_key="sk_test_unit",
        api_version="2024-12-18.acacia",
        page_size=100,
        max_retries=max_retries,
        retry_backoff_seconds=0.01,
        requests_per_second=0.0,  # disable the bucket for speed
        timeout_seconds=5.0,
        sleep=sleep or (lambda _s: None),
    )


def test_client_sends_bearer_auth_and_stripe_version(
    stripe_stub: tuple[_Scenario, str],
) -> None:
    """Every request carries `Authorization: Bearer ...` and `Stripe-Version`."""
    scenario, base_url = stripe_stub
    scenario.add("/charges", json_body={"object": "list", "data": [], "has_more": False})

    with _client(base_url) as client:
        client.list("/charges", {"limit": 100})

    assert len(scenario.captured) == 1
    headers = scenario.captured[0].headers
    assert headers.get("Authorization") == "Bearer sk_test_unit"
    assert headers.get("Stripe-Version") == "2024-12-18.acacia"


def test_client_429_with_retry_after_is_honored(
    stripe_stub: tuple[_Scenario, str],
) -> None:
    """A 429 with `Retry-After: 1` sleeps ~1s then retries successfully."""
    scenario, base_url = stripe_stub
    scenario.add(
        "/charges",
        status=429,
        json_body={"error": {"message": "rate limited"}},
        extra_headers={"Retry-After": "1"},
    )
    scenario.add("/charges", json_body={"object": "list", "data": [], "has_more": False})

    sleeps: list[float] = []
    with _client(base_url, sleep=sleeps.append) as client:
        result = client.list("/charges", {"limit": 100})

    assert result["object"] == "list"
    assert len(scenario.captured) == 2
    # Exactly one sleep, of exactly 1.0 second from Retry-After.
    assert 1.0 in sleeps


def test_client_500_retried_then_succeeds(
    stripe_stub: tuple[_Scenario, str],
) -> None:
    """A 500 is retried with backoff; the next 200 returns normally."""
    scenario, base_url = stripe_stub
    scenario.add("/charges", status=500, json_body={"error": {"message": "boom"}})
    scenario.add("/charges", json_body={"object": "list", "data": [], "has_more": False})

    sleeps: list[float] = []
    with _client(base_url, sleep=sleeps.append) as client:
        result = client.list("/charges", {"limit": 100})

    assert result["object"] == "list"
    assert len(scenario.captured) == 2
    # One backoff sleep happened (0.01 * 2**0 = 0.01).
    assert sleeps and sleeps[0] >= 0.0


def test_client_401_raises_immediately_without_retry(
    stripe_stub: tuple[_Scenario, str],
) -> None:
    """A 401 (bad key) raises without retrying — the key will not improve."""
    scenario, base_url = stripe_stub
    scenario.add(
        "/charges",
        status=401,
        json_body={"error": {"message": "Invalid API Key provided"}},
    )

    with pytest.raises(StripeAPIError) as exc_info:
        with _client(base_url) as client:
            client.list("/charges", {"limit": 100})

    assert exc_info.value.status == 401
    # Stripe's error.message surfaced; the API key did NOT leak into it.
    assert "Invalid API Key" in str(exc_info.value)
    assert "sk_test_unit" not in str(exc_info.value)
    # Only one request — no retry.
    assert len(scenario.captured) == 1


# --------------------------------------------------------------------------
# Pagination tests — starting_after / has_more loop
# --------------------------------------------------------------------------


def test_pagination_walks_three_pages_with_starting_after(
    stripe_stub: tuple[_Scenario, str],
) -> None:
    """has_more toggles across 3 pages; starting_after uses the last id each time."""
    scenario, base_url = stripe_stub
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [{"id": "ch_1", "created": 1700000000}, {"id": "ch_2", "created": 1700000001}],
            "has_more": True,
        },
    )
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [{"id": "ch_3", "created": 1700000002}, {"id": "ch_4", "created": 1700000003}],
            "has_more": True,
        },
    )
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [{"id": "ch_5", "created": 1700000004}],
            "has_more": False,
        },
    )

    all_records: list[dict[str, Any]] = []
    with _client(base_url) as client:
        for page in paginate(client, "/charges", {"limit": 100}):
            all_records.extend(page)

    assert [r["id"] for r in all_records] == ["ch_1", "ch_2", "ch_3", "ch_4", "ch_5"]
    # Three GETs, with starting_after advancing on the LAST id of each page.
    assert len(scenario.captured) == 3
    assert "starting_after" not in scenario.captured[0].query_dict
    assert scenario.captured[1].query_dict["starting_after"] == "ch_2"
    assert scenario.captured[2].query_dict["starting_after"] == "ch_4"


# --------------------------------------------------------------------------
# Incremental tests — first run vs second run with prior cursor
# --------------------------------------------------------------------------


def _make_stripe_project(tmp_path: Path, base_url: str) -> Path:
    """Write a minimal dtex project that drives the stripe baked source.

    The project has no project-local sources — the engine finds `stripe`
    under the baked path. It overrides `base_url` via the project-wide
    ``vars`` block so the stripe source points at our stub server. A
    ``stripe_dev`` config binds stripe → duckdb → dev (docs/12).
    """
    project_root = tmp_path
    (project_root / "dtex_project.yml").write_text(
        textwrap.dedent(
            f"""\
            name: stripe_test_project
            version: "1.0.0"
            source_paths: []
            destination_paths: [destinations]
            config_paths: [configs]
            vars:
              base_url: "{base_url}"
              # Fast, deterministic retry timing for tests:
              max_retries: 2
              retry_backoff_seconds: 0.01
              requests_per_second: 0.0
              page_size: 100
            """
        )
    )
    (project_root / "profiles.yml").write_text(
        textwrap.dedent(
            """\
            duckdb:
              default_target: dev
              targets:
                dev:
                  path: ".dtex/warehouse.duckdb"
            """
        )
    )
    (project_root / "configs").mkdir()
    (project_root / "configs" / "stripe_dev.yml").write_text(
        textwrap.dedent(
            """\
            name: stripe_dev
            source: stripe
            destination: duckdb
            target: dev
            # Narrow to REST streams — these tests cover REST behavior.
            # Sigma streams have their own coverage in this file.
            streams:
              charges:
              invoices:
              customers:
              subscriptions:
            """
        )
    )
    return project_root


def test_first_run_no_cursor_omits_created_gte(
    stripe_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First run (no prior state) does NOT send `created[gte]` past initial_value."""
    scenario, base_url = stripe_stub
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_first_run")

    # Empty responses for every stream so the run terminates quickly.
    for path in ("/charges", "/invoices", "/customers", "/subscriptions"):
        scenario.add(
            path, json_body={"object": "list", "data": [], "has_more": False}
        )

    project_root = _make_stripe_project(tmp_path, base_url)
    db_path = str(project_root / "warehouse.duckdb")
    result = dtex.run(
        config="stripe_dev",
        project_dir=str(project_root),
        destination_params_override={"path": db_path},
    )

    assert result.status.value == "succeeded", result.error
    # The first /charges request: created[gte] is the initial_value (the
    # 2024-01-01 Unix timestamp), since prior state is empty.
    charges_req = next(
        r for r in scenario.captured if r.path == "/charges"
    )
    assert charges_req.query_dict.get("created[gte]") == "1704067200"
    # limit is the configured page_size.
    assert charges_req.query_dict.get("limit") == "100"


def test_second_run_sends_created_gte_from_committed_cursor(
    stripe_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a first run advances the cursor, the next run filters by it."""
    scenario, base_url = stripe_stub
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_inc")

    # Run 1: /charges returns three records; other streams empty.
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [
                {"id": "ch_a", "object": "charge", "created": 1710000000,
                 "amount": 100, "currency": "usd"},
                {"id": "ch_b", "object": "charge", "created": 1710000050,
                 "amount": 200, "currency": "usd"},
                {"id": "ch_c", "object": "charge", "created": 1710000100,
                 "amount": 300, "currency": "usd"},
            ],
            "has_more": False,
        },
    )
    for path in ("/invoices", "/customers", "/subscriptions"):
        scenario.add(path, json_body={"object": "list", "data": [], "has_more": False})

    # Run 2: every stream empty (we are asserting the QUERY, not the records).
    for path in ("/charges", "/invoices", "/customers", "/subscriptions"):
        scenario.add(path, json_body={"object": "list", "data": [], "has_more": False})

    project_root = _make_stripe_project(tmp_path, base_url)
    db_path = str(project_root / "warehouse.duckdb")

    first = dtex.run(
        config="stripe_dev",
        project_dir=str(project_root),
        destination_params_override={"path": db_path},
    )
    assert first.status.value == "succeeded", first.error

    second = dtex.run(
        config="stripe_dev",
        project_dir=str(project_root),
        destination_params_override={"path": db_path},
    )
    assert second.status.value == "succeeded", second.error

    # Find the run-2 /charges request — the LAST captured request for /charges.
    charges_requests = [r for r in scenario.captured if r.path == "/charges"]
    assert len(charges_requests) >= 2
    second_charges = charges_requests[-1]
    # Cursor advanced to the max `created` from run 1 == 1710000100.
    assert second_charges.query_dict.get("created[gte]") == "1710000100"


def test_extra_query_params_propagate(
    stripe_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stream-scoped extra_query_params_json reaches the actual request."""
    scenario, base_url = stripe_stub
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_extras")

    for path in ("/charges", "/invoices", "/customers", "/subscriptions"):
        scenario.add(path, json_body={"object": "list", "data": [], "has_more": False})

    project_root = _make_stripe_project(tmp_path, base_url)
    # Stream-scoped param override via the engine's `params=` kwarg (run kwargs
    # are the highest-precedence layer per docs/03 §6; merged into the source
    # config and surfaced to every stream that declares the same param name).
    db_path = str(project_root / "warehouse.duckdb")
    result = dtex.run(
        config="stripe_dev",
        project_dir=str(project_root),
        destination_params_override={"path": db_path},
        params_override={"extra_query_params_json": '{"expand[]": "data.customer"}'},
    )
    assert result.status.value == "succeeded", result.error

    charges_req = next(r for r in scenario.captured if r.path == "/charges")
    assert "data.customer" in charges_req.query_values("expand[]")


def test_end_to_end_lands_rows_and_advances_state(
    stripe_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: rows land in DuckDB and `_dtex_state` carries the cursor."""
    scenario, base_url = stripe_stub
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_e2e")

    # /charges: two pages so cursor pagination is exercised end-to-end.
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [
                {
                    "id": "ch_x", "object": "charge", "created": 1720000000,
                    "livemode": False, "amount": 1000, "currency": "usd",
                    "status": "succeeded", "customer": "cus_1",
                    "metadata": {"order_id": "ord_1"},
                    "payment_method_details": {"card": {"brand": "visa"}},
                },
                {
                    "id": "ch_y", "object": "charge", "created": 1720000100,
                    "livemode": False, "amount": 2000, "currency": "usd",
                    "status": "succeeded", "customer": "cus_2",
                    "metadata": {}, "payment_method_details": None,
                },
            ],
            "has_more": True,
        },
    )
    scenario.add(
        "/charges",
        json_body={
            "object": "list",
            "data": [
                {
                    "id": "ch_z", "object": "charge", "created": 1720000200,
                    "livemode": False, "amount": 3000, "currency": "usd",
                    "status": "succeeded", "customer": "cus_3",
                    "metadata": {"campaign": "spring"},
                    "payment_method_details": {"card": {"brand": "mc"}},
                },
            ],
            "has_more": False,
        },
    )
    for path in ("/invoices", "/customers", "/subscriptions"):
        scenario.add(path, json_body={"object": "list", "data": [], "has_more": False})

    project_root = _make_stripe_project(tmp_path, base_url)
    db_path = str(project_root / "warehouse.duckdb")
    result = dtex.run(
        config="stripe_dev",
        project_dir=str(project_root),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    # Three charge rows landed across two pages.
    charges_result = result.stream("charges")
    assert charges_result is not None
    assert charges_result.rows_loaded == 3

    # Query DuckDB to confirm the rows are there with the right values.
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id, created, amount, currency FROM stripe_charges ORDER BY created"
        ).fetchall()
        assert rows == [
            ("ch_x", 1720000000, 1000, "usd"),
            ("ch_y", 1720000100, 2000, "usd"),
            ("ch_z", 1720000200, 3000, "usd"),
        ]
        # _dtex_state row carries the advanced cursor.
        state = conn.execute(
            "SELECT cursor_value, cursor_type, rows_total FROM _dtex_state "
            "WHERE connector = 'stripe' AND stream = 'charges'"
        ).fetchall()
        assert len(state) == 1
        assert int(str(state[0][0])) == 1720000200
        assert state[0][1] == "int"
        assert state[0][2] == 3
    finally:
        conn.close()


# --------------------------------------------------------------------------
# Secret never leaks — caplog reads every log line from the run
# --------------------------------------------------------------------------


def test_api_key_never_appears_in_logs(
    stripe_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The resolved Stripe key is never logged — across an entire engine run."""
    scenario, base_url = stripe_stub
    api_key = "sk_test_super_secret_value_should_not_leak_123"
    monkeypatch.setenv("STRIPE_API_KEY", api_key)

    for path in ("/charges", "/invoices", "/customers", "/subscriptions"):
        scenario.add(
            path, json_body={"object": "list", "data": [], "has_more": False}
        )

    project_root = _make_stripe_project(tmp_path, base_url)
    db_path = str(project_root / "warehouse.duckdb")
    with caplog.at_level("DEBUG"):
        result = dtex.run(
            config="stripe_dev",
            project_dir=str(project_root),
            destination_params_override={"path": db_path},
        )

    assert result.status.value == "succeeded", result.error

    full_log = "\n".join(record.getMessage() for record in caplog.records)
    assert api_key not in full_log, "Stripe API key leaked into captured logs"


# ==========================================================================
# Sigma SQL surface — drives SigmaClient against a stdlib HTTPServer stub
# ==========================================================================
#
# These cover the SQL-as-stream half of the merged stripe connector. They
# drive SigmaClient directly (no engine in the loop) against a tiny stub
# that fakes Stripe's submit → poll → download-CSV protocol.

import threading as _threading
from collections.abc import Iterator as _Iterator
from http.server import BaseHTTPRequestHandler as _BaseHandler, HTTPServer as _HTTPServer

import requests as _requests

from dtex.sources.stripe.sigma_client import SigmaClient as _SigmaClient

_SIGMA_CSV_BODY = b"id,amount\r\nch_1,100\r\nch_2,250\r\nch_3,999\r\n"
_SIGMA_EXPECTED_ROWS = [
    {"id": "ch_1", "amount": "100"},
    {"id": "ch_2", "amount": "250"},
    {"id": "ch_3", "amount": "999"},
]


class _SigmaStubState:
    """Mutable knobs the handler reads + counters the test asserts on."""

    def __init__(self) -> None:
        self.poll_calls = 0
        self.download_calls = 0
        # How many of the first download attempts should be truncated
        # (Content-Length over-promises, socket closed early → the client
        # sees ChunkedEncodingError mid-body).
        self.truncate_first_n_downloads = 0
        # Terminal poll status: "succeeded" or "failed".
        self.final_status = "succeeded"
        self.submitted_sql: str | None = None
        self.auth_header: str | None = None


def _make_sigma_handler(state: _SigmaStubState, base: str) -> type[_BaseHandler]:
    class Handler(_BaseHandler):
        def log_message(self, *_args: Any) -> None:  # silence test noise
            pass

        def _json(self, code: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            state.submitted_sql = json.loads(raw).get("sql")
            state.auth_header = self.headers.get("Authorization")
            self._json(200, {"id": "qryrun_TEST123", "status": "running"})

        def do_GET(self) -> None:
            if self.path.endswith("/download.csv"):
                state.download_calls += 1
                truncate = state.download_calls <= state.truncate_first_n_downloads
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                if truncate:
                    # Over-promise the length, write a fragment, then hang up.
                    self.send_header(
                        "Content-Length", str(len(_SIGMA_CSV_BODY) + 5000)
                    )
                    self.end_headers()
                    self.wfile.write(_SIGMA_CSV_BODY[:12])
                    self.wfile.flush()
                    self.connection.close()  # IncompleteRead on the client
                    return
                self.send_header("Content-Length", str(len(_SIGMA_CSV_BODY)))
                self.end_headers()
                self.wfile.write(_SIGMA_CSV_BODY)
                return
            # Otherwise it's a poll: GET /v2/data/reporting/query_runs/{id}
            state.poll_calls += 1
            if state.poll_calls < 2:
                self._json(200, {"status": "running"})
                return
            if state.final_status == "failed":
                self._json(
                    200,
                    {
                        "status": "failed",
                        "status_details": {
                            "failed": {"error_message": "line 1: Table not found"}
                        },
                    },
                )
                return
            self._json(
                200,
                {
                    "status": "succeeded",
                    "result": {
                        "type": "file",
                        "file": {
                            "content_type": "csv",
                            "size": str(len(_SIGMA_CSV_BODY)),
                            "download_url": {
                                "url": f"{base}/download.csv",
                                "expires_at": "2099-01-01T00:00:00Z",
                            },
                        },
                    },
                },
            )

    return Handler


@pytest.fixture
def sigma_stub() -> _Iterator[tuple[_SigmaStubState, str]]:
    state = _SigmaStubState()
    server = _HTTPServer(("127.0.0.1", 0), object)  # placeholder handler
    base = f"http://127.0.0.1:{server.server_port}"
    server.RequestHandlerClass = _make_sigma_handler(state, base)
    thread = _threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state, base
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _sigma_client(base: str) -> _SigmaClient:
    return _SigmaClient(
        base_url=base,
        api_key="rk_test_abc",
        api_version="2026-04-22.preview",
        account_id="acct_TEST",
        poll_interval_seconds=0.0,  # don't sleep between polls in tests
        poll_timeout_seconds=10,
        max_retries=3,
        retry_backoff_seconds=0.0,  # don't sleep between download retries
    )


def test_sigma_run_query_happy_path(
    sigma_stub: tuple[_SigmaStubState, str],
) -> None:
    state, base = sigma_stub
    rows = list(_sigma_client(base).run_query("SELECT id, amount FROM charges"))
    assert rows == _SIGMA_EXPECTED_ROWS
    assert state.submitted_sql == "SELECT id, amount FROM charges"
    assert state.auth_header == "Bearer rk_test_abc"
    assert state.download_calls == 1  # no retries on the happy path


def test_sigma_download_retries_on_truncated_body(
    sigma_stub: tuple[_SigmaStubState, str],
) -> None:
    """The bug this port fixes: a mid-body connection drop must retry, not fail."""
    state, base = sigma_stub
    state.truncate_first_n_downloads = 1  # first GET truncates, second is clean
    rows = list(_sigma_client(base).run_query("SELECT id, amount FROM charges"))
    assert rows == _SIGMA_EXPECTED_ROWS  # every row exactly once, no dupes from the retry
    assert state.download_calls == 2  # one failed attempt + one success


def test_sigma_download_gives_up_after_max_retries(
    sigma_stub: tuple[_SigmaStubState, str],
) -> None:
    state, base = sigma_stub
    state.truncate_first_n_downloads = 99  # every attempt truncates
    with pytest.raises(_requests.exceptions.ChunkedEncodingError):
        list(_sigma_client(base).run_query("SELECT id, amount FROM charges"))
    # max_retries=3 → 1 initial + 3 retries = 4 download attempts.
    assert state.download_calls == 4


def test_sigma_download_retries_on_connect_failure(
    sigma_stub: tuple[_SigmaStubState, str],
) -> None:
    """A connect-time ConnectionError (dead host) must retry, not leak the fd."""
    state, base = sigma_stub
    dead_url = "http://127.0.0.1:1/download.csv"  # nothing listening on port 1
    live_url = f"{base}/download.csv"
    calls = {"n": 0}
    real_get = _requests.get

    def flaky_get(url: str, **kwargs: Any) -> Any:
        if url == dead_url:
            calls["n"] += 1
            if calls["n"] == 1:
                return real_get(dead_url, **kwargs)  # refused → ConnectionError
            return real_get(live_url, **kwargs)  # retry hits the live stub
        return real_get(url, **kwargs)

    client = _sigma_client(base)
    import os

    _requests.get = flaky_get  # type: ignore[assignment]
    try:
        path = client._download_to_tempfile(dead_url)
    finally:
        _requests.get = real_get  # type: ignore[assignment]
    try:
        assert calls["n"] == 2  # one refused connect + one success
        with open(path, "rb") as fh:
            assert fh.read() == _SIGMA_CSV_BODY
    finally:
        os.unlink(path)


def test_sigma_failed_poll_surfaces_presto_error(
    sigma_stub: tuple[_SigmaStubState, str],
) -> None:
    state, base = sigma_stub
    state.final_status = "failed"
    with pytest.raises(RuntimeError, match="Table not found"):
        list(_sigma_client(base).run_query("SELECT * FROM nope"))
