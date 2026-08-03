# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Chargebacks911 CBAPIv2 connector.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port
and points :class:`Chargebacks911Client` at it. The stub records every
request and responds based on a scripted scenario — no real network
calls, no flakes from upstream availability.

Test areas:

* Auth — the two-step flow (Basic on GET /auth → Bearer everywhere else),
  the 401-re-mint-once policy, the proactive ~50-minute re-mint, and the
  persistent-401 failure mode.
* Client transport — retry-on-429 (bounded, Retry-After honored),
  retry-on-5xx, immediate raise on other 4xx, envelope-vs-bare-list
  unwrapping.
* Pagination — `limit` + `page` (1-based), stop on a short page, date
  params passed on the first request.
* End-to-end — `dtex.run` drives both streams into DuckDB; the
  incremental cursor advances between runs; credentials never leak into
  logs.
"""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest
import requests

import dtex
from dtex.sources.chargebacks911.client import Chargebacks911Client

# --------------------------------------------------------------------------
# Stub CBAPIv2 server — stdlib HTTPServer on a random port
# --------------------------------------------------------------------------


_AUTH_OK: dict[str, Any] = {
    "success": True,
    "code": 200,
    "message": "ok",
    "data": {"accessToken": "tok_1"},
}


def _auth_response(token: str) -> dict[str, Any]:
    return {
        "success": True,
        "code": 200,
        "message": "ok",
        "data": {"accessToken": token},
    }


def _envelope(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"success": True, "code": 200, "message": "ok", "data": rows}


class _RequestRecord:
    """One captured request — path, query string, headers."""

    def __init__(self, path: str, headers: dict[str, str]) -> None:
        self.path = path
        self.headers = headers


class _Scenario:
    """Scripts responses + captures requests for one test.

    Tests `.add(...)` a sequence of responses; the handler pops them
    in order off `_queue` as requests arrive. The captured request
    list is available as `.captured`.
    """

    def __init__(self) -> None:
        self._queue: list[dict[str, Any]] = []
        self.captured: list[_RequestRecord] = []

    def add(
        self,
        *,
        status: int = 200,
        json_body: Any = None,
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        self._queue.append(
            {
                "status": status,
                "body": json_body,
                "headers": extra_headers or {},
            }
        )

    def pop(self) -> dict[str, Any]:
        if not self._queue:
            return {"status": 500, "body": {"error": "scenario exhausted"}, "headers": {}}
        return self._queue.pop(0)


def _make_handler(scenario: _Scenario) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            # Silence test noise.
            return

        def do_GET(self) -> None:  # noqa: N802 — required by stdlib
            scenario.captured.append(
                _RequestRecord(self.path, dict(self.headers))
            )
            response = scenario.pop()
            body = json.dumps(
                response["body"] if response["body"] is not None else {}
            ).encode()
            self.send_response(response["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for k, v in response["headers"].items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

    return Handler


@pytest.fixture
def cb_stub() -> Iterator[tuple[_Scenario, str]]:
    """Spin up a stub CBAPIv2 server on a random port; tear down after.

    Yields ``(scenario, base_url)`` — tests script responses on
    ``scenario`` and point the connector at ``base_url``.
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


def _client(base_url: str, *, max_retries: int = 3) -> Chargebacks911Client:
    """Build a Chargebacks911Client pointed at the stub."""
    return Chargebacks911Client(
        username="user_test_unit",
        password="pw_test_unit",
        base_url=base_url,
        max_retries=max_retries,
    )


def _expected_basic() -> str:
    return "Basic " + base64.b64encode(b"user_test_unit:pw_test_unit").decode()


def _window_of(path: str) -> tuple[str, str]:
    """Extract (start_date, end_date) from a captured request path."""
    query = parse_qs(urlparse(path).query)
    return query["start_date"][0], query["end_date"][0]


# --------------------------------------------------------------------------
# Auth — the two-step flow
# --------------------------------------------------------------------------


def test_auth_flow_basic_then_bearer(cb_stub: tuple[_Scenario, str]) -> None:
    """The first data request lazily mints via Basic-auth GET /auth, then
    carries the returned token as a Bearer header."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([]))

    list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert len(scenario.captured) == 2
    auth_req, data_req = scenario.captured
    assert auth_req.path == "/auth"
    assert auth_req.headers.get("Authorization") == _expected_basic()
    assert data_req.path.startswith("/clients/my/alerts")
    assert data_req.headers.get("Authorization") == "Bearer tok_1"
    assert data_req.headers.get("Accept") == "application/json"


def test_401_remints_once_then_succeeds(cb_stub: tuple[_Scenario, str]) -> None:
    """A mid-run 401 (expired/stolen token) re-mints ONCE and retries the
    request with the fresh token."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_auth_response("tok_old"))
    scenario.add(status=401, json_body={"success": False, "code": 401})
    scenario.add(json_body=_auth_response("tok_new"))
    scenario.add(json_body=_envelope([{"id": 1}]))

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert rows == [{"id": 1}]
    paths = [r.path.split("?")[0] for r in scenario.captured]
    assert paths == ["/auth", "/clients/my/alerts", "/auth", "/clients/my/alerts"]
    # The retried request carries the NEW token.
    assert scenario.captured[3].headers.get("Authorization") == "Bearer tok_new"


def test_persistent_401_raises_after_single_remint(
    cb_stub: tuple[_Scenario, str],
) -> None:
    """A second consecutive 401 raises — the client re-mints exactly once,
    never loops (two loops of mutual token theft would spin forever)."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_auth_response("tok_1"))
    scenario.add(status=401, json_body={})
    scenario.add(json_body=_auth_response("tok_2"))
    scenario.add(status=401, json_body={})

    with pytest.raises(RuntimeError, match="still unauthorized"):
        list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert len(scenario.captured) == 4  # auth, 401, auth, 401 — no third mint


def test_basic_auth_401_raises_immediately(cb_stub: tuple[_Scenario, str]) -> None:
    """A 401 on /auth itself means bad credentials — no re-mint loop, and
    the raised message names the env vars, not the credential values."""
    scenario, base_url = cb_stub
    scenario.add(status=401, json_body={"success": False, "code": 401})

    with pytest.raises(RuntimeError, match="authentication failed") as excinfo:
        list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert len(scenario.captured) == 1
    assert "pw_test_unit" not in str(excinfo.value)
    assert "user_test_unit" not in str(excinfo.value)


def test_proactive_remint_after_token_age_cap(cb_stub: tuple[_Scenario, str]) -> None:
    """A token older than ~50 minutes is re-minted BEFORE the next request —
    long extractions never sail past the 60-minute TTL."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_auth_response("tok_1"))
    scenario.add(json_body=_envelope([]))
    scenario.add(json_body=_auth_response("tok_2"))
    scenario.add(json_body=_envelope([]))

    client = _client(base_url)
    list(client.paginate("/clients/my/alerts", {"limit": 5}))
    # Age the token past the 50-minute proactive threshold.
    client._token_minted_at -= 51 * 60
    list(client.paginate("/clients/my/alerts", {"limit": 5}))

    paths = [r.path.split("?")[0] for r in scenario.captured]
    assert paths == ["/auth", "/clients/my/alerts", "/auth", "/clients/my/alerts"]
    assert scenario.captured[3].headers.get("Authorization") == "Bearer tok_2"


def test_auth_success_false_is_an_error(cb_stub: tuple[_Scenario, str]) -> None:
    """An HTTP-200 body with success=false is still an error (the envelope
    is the real status channel on this API)."""
    scenario, base_url = cb_stub
    scenario.add(
        json_body={"success": False, "code": 403, "message": "account disabled"}
    )

    with pytest.raises(RuntimeError, match="account disabled"):
        list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))


# --------------------------------------------------------------------------
# Transport — retries and 4xx handling
# --------------------------------------------------------------------------


def test_client_429_is_honored_then_succeeds(
    cb_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 429 with Retry-After sleeps, then the retry succeeds and yields rows."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(
        status=429,
        json_body={"error": "rate limited"},
        extra_headers={"Retry-After": "1"},
    )
    scenario.add(json_body=_envelope([{"id": 7}]))

    sleeps: list[float] = []
    monkeypatch.setattr(
        "dtex.sources.chargebacks911.client.time.sleep",
        lambda s: sleeps.append(s),
    )

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert rows == [{"id": 7}]
    assert sleeps == [1]  # exactly one Retry-After sleep
    assert len(scenario.captured) == 3  # auth, 429, 200


def test_client_429_bounded_by_max_retries(
    cb_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A persistent 429 raises after max_retries — does NOT loop forever,
    and the attempt counter increments even when Retry-After is honored."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    for _ in range(10):
        scenario.add(
            status=429,
            json_body={"error": "rate limited"},
            extra_headers={"Retry-After": "1"},
        )

    monkeypatch.setattr(
        "dtex.sources.chargebacks911.client.time.sleep", lambda s: None
    )

    with pytest.raises(RuntimeError, match="rate-limited after"):
        list(_client(base_url, max_retries=3).paginate("/clients/my/alerts"))

    # 1 auth + (1 initial + 3 retries) = 5 requests total.
    assert len(scenario.captured) == 5


def test_client_500_retries_then_succeeds(
    cb_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient 500 retries with exponential backoff, then succeeds."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(status=500, json_body={"error": "server"})
    scenario.add(json_body=_envelope([{"id": 1}]))

    monkeypatch.setattr(
        "dtex.sources.chargebacks911.client.time.sleep", lambda s: None
    )

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert rows == [{"id": 1}]
    assert len(scenario.captured) == 3


def test_client_403_raises_immediately(cb_stub: tuple[_Scenario, str]) -> None:
    """A non-retryable 4xx (e.g. 403) raises on the first attempt."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(status=403, json_body={"error": "permission denied"})

    with pytest.raises(requests.exceptions.HTTPError):
        list(_client(base_url).paginate("/clients/my/alerts", {"limit": 5}))

    assert len(scenario.captured) == 2  # auth + one 403, no retries


# --------------------------------------------------------------------------
# Pagination + envelope unwrapping
# --------------------------------------------------------------------------


def test_paginate_stops_on_short_page(cb_stub: tuple[_Scenario, str]) -> None:
    """Pages walk `page=1,2,...` with `limit`; the walk ends when a page
    returns fewer than `limit` rows."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([{"id": 1}, {"id": 2}]))  # full page
    scenario.add(json_body=_envelope([{"id": 3}]))  # short page → stop

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 2}))

    assert [r["id"] for r in rows] == [1, 2, 3]
    data_paths = [r.path for r in scenario.captured[1:]]
    assert len(data_paths) == 2
    assert "limit=2" in data_paths[0] and "page=1" in data_paths[0]
    assert "limit=2" in data_paths[1] and "page=2" in data_paths[1]


def test_paginate_stops_on_exactly_empty_page(cb_stub: tuple[_Scenario, str]) -> None:
    """A full page followed by an empty page terminates cleanly (the
    boundary case when the row count is an exact multiple of `limit`)."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([{"id": 1}, {"id": 2}]))
    scenario.add(json_body=_envelope([]))

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 2}))

    assert [r["id"] for r in rows] == [1, 2]
    assert len(scenario.captured) == 3


def test_unwraps_envelope_and_bare_list(cb_stub: tuple[_Scenario, str]) -> None:
    """Both response shapes work: the nominal {success,...,data:[...]}
    envelope AND the bare JSON list that live production sometimes returns."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([{"id": 1}, {"id": 2}]))  # enveloped, full
    scenario.add(json_body=[{"id": 3}])  # bare list, short → stop

    rows = list(_client(base_url).paginate("/clients/my/alerts", {"limit": 2}))

    assert [r["id"] for r in rows] == [1, 2, 3]


def test_unexpected_shape_raises(cb_stub: tuple[_Scenario, str]) -> None:
    """A dict body whose `data` is neither a list nor null is an error, not
    silently swallowed rows."""
    scenario, base_url = cb_stub
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body={"success": True, "data": "oops"})

    with pytest.raises(RuntimeError, match="unexpected response shape"):
        list(_client(base_url).paginate("/clients/my/alerts", {"limit": 2}))


# --------------------------------------------------------------------------
# End-to-end with dtex.run — both streams through the engine into DuckDB
# --------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> None:
    """Scaffold a minimal dtex project with the baked chargebacks911 + a duckdb dev target."""
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )


def _write_config(tmp_path: Path, *, base_url: str, streams: str) -> None:
    """Write a one-config-per-file under configs/cb911_test.yml."""
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "cb911_test.yml").write_text(
        "name: cb911_test\n"
        "source: chargebacks911\n"
        "destination: duckdb\n"
        "target: dev\n"
        f"params:\n  base_url: '{base_url}'\n  page_size: 100\n"
        f"streams:\n{streams}\n"
    )


def _alert_row(alert_id: int, date_updated: str) -> dict[str, Any]:
    """A live-shaped alert row — amounts as strings, camelCase keys."""
    return {
        "id": alert_id,
        "completed": True,
        "clientId": 79320,
        "mid": "ExampleMerchant",
        "type": "RDR",
        "level": 6,
        "caseId": f"case_{alert_id}",
        "amount": "104.97",
        "refundAmount": "0.00",
        "currencyId": 1,
        "currency_name": "USD",
        "outcomeId": 8,
        "outcome_name": "Completed",
        "ccNum": "426684xxxxxx9979",
        "customerEmail": "jane@example.com",
        "dateUpdated": date_updated,
        "an_undeclared_field": "must not land",  # projection drops it
    }


def _chargeback_row(cb_id: str, date_updated: str) -> dict[str, Any]:
    return {
        "id": cb_id,
        "case_no": f"cn_{cb_id}",
        "mid": "ExampleMerchant",
        "reason_code": "10.4",
        "status": "New",
        "chargeback_amount": "55.50",
        "dispute_amount": "55.50",
        "crm_gateway_id": 12,
        "partial_chargeback": False,
        "date_updated": date_updated,
        "status_history": [
            {"status": "New", "date": "2026-01-02"},
            {"status": "Responded", "date": "2026-01-03"},
        ],
    }


def _setenv_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CHARGEBACKS911_USERNAME", "user_e2e")
    monkeypatch.setenv("CHARGEBACKS911_PASSWORD", "pw_e2e_secret")


def test_end_to_end_alerts(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """alerts extracts + lands rows in DuckDB; string amounts coerce to
    FLOAT; the date filter goes out on the first request."""
    scenario, base_url = cb_stub
    _setenv_credentials(monkeypatch)

    scenario.add(json_body=_AUTH_OK)
    scenario.add(
        json_body=_envelope(
            [
                _alert_row(1, "2026-01-05 10:00:00"),
                _alert_row(2, "2026-01-06 11:30:00"),
            ]
        )
    )  # short page (page_size=100) → walk ends

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  alerts:")

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="cb911_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT id, mid, amount, completed, dateUpdated FROM alerts ORDER BY id"
    ).fetchall()
    columns = {
        r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns "
                                   "WHERE table_name = 'alerts'").fetchall()
    }
    conn.close()

    assert rows == [
        (1, "ExampleMerchant", 104.97, True, "2026-01-05 10:00:00"),
        (2, "ExampleMerchant", 104.97, True, "2026-01-06 11:30:00"),
    ]
    assert "an_undeclared_field" not in columns  # projection held

    # A virgin run must go out WITHOUT date filters: CB911's server 503s on
    # wide date_column windows, so the bootstrap is the unfiltered sweep
    # (no initial_value is declared — start_value() is None on run 1).
    data_path = scenario.captured[1].path
    assert "date_column" not in data_path
    assert "start_date" not in data_path


def test_end_to_end_chargebacks(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """chargebacks lands rows; status_history lands as a JSON column."""
    scenario, base_url = cb_stub
    _setenv_credentials(monkeypatch)

    scenario.add(json_body=_AUTH_OK)
    # Bare-list shape end-to-end — production drift covered through the engine.
    scenario.add(
        json_body=[
            _chargeback_row("cb_1", "2026-01-05 09:00:00"),
            _chargeback_row("cb_2", "2026-01-07 09:00:00"),
        ]
    )

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  chargebacks:")

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="cb911_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT id, reason_code, chargeback_amount, partial_chargeback, "
        "CAST(status_history AS VARCHAR) FROM chargebacks ORDER BY id"
    ).fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0][:4] == ("cb_1", "10.4", 55.5, False)
    history = json.loads(rows[0][4])
    assert history == [
        {"status": "New", "date": "2026-01-02"},
        {"status": "Responded", "date": "2026-01-03"},
    ]

    # Chargebacks bootstrap likewise goes out unfiltered (CB911 503s on
    # wide date windows).
    data_path = scenario.captured[1].path
    assert "/clients/my/chargebacks" in data_path
    assert "date_column" not in data_path


def test_alerts_never_sends_date_params_even_with_a_cursor(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """alerts is a FULL SWEEP on every run, including runs that have a
    persisted cursor.

    The endpoint ignores date_column/start_date/end_date (verified live: a
    2019 window returns present-day alerts). Sending them anyway produced
    full account history wearing a date label — in production the stream
    re-extracted all 3341 rows every run while only 9 sat past the cursor,
    and the state table happily recorded that as an "incremental" run. The
    cursor is still observed so freshness state is real; it just must not
    appear in the query string.
    """
    scenario, base_url = cb_stub
    _setenv_credentials(monkeypatch)

    # Run 1: auth + one short page whose max dateUpdated is 2026-01-06.
    scenario.add(json_body=_AUTH_OK)
    scenario.add(
        json_body=_envelope(
            [
                _alert_row(1, "2026-01-06 11:30:00"),
                _alert_row(2, "2026-01-05 10:00:00"),  # out of order on purpose
            ]
        )
    )
    # Run 2: fresh client → new mint, then one empty page.
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([]))

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams="  alerts:\n    params:\n      lookback_days: 0",
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    for _ in range(2):
        result = dtex.run(
            config="cb911_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )
        assert result.status.value == "succeeded", result.error

    data_paths = [
        r.path for r in scenario.captured if r.path.startswith("/clients/")
    ]
    assert len(data_paths) == 2
    # Run 2 HAS a persisted cursor (2026-01-06) — and still sends no dates.
    for path in data_paths:
        assert "date_column" not in path
        assert "start_date" not in path
        assert "end_date" not in path

    # The cursor did advance, so _dtex_state still reports real freshness.
    conn = duckdb.connect(db_path)
    cursor_value = conn.execute(
        "SELECT cursor_value FROM _dtex_state WHERE stream = 'alerts'"
    ).fetchone()
    conn.close()
    assert cursor_value is not None
    assert "2026-01-06 11:30:00" in str(cursor_value[0])


def test_chargebacks_chunks_a_wide_window_into_slices(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A wide catch-up is issued as many bounded windows, never one request.

    Three of five production runs died on a single
    ``start_date=2023-12-25&end_date=2026-08-03`` request: CB911 503s
    computing wide date_column windows. The span is now sliced into
    window_chunk_days-wide requests, each individually paginated and
    retryable. Windows must tile the span with no gap and no overlap.
    """
    scenario, base_url = cb_stub
    _setenv_credentials(monkeypatch)

    # Run 1 bootstraps unfiltered and sets the cursor well in the past.
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([_chargeback_row("cb_1", "2026-01-01")]))
    # Run 2: one mint, then an empty page per chunk. Queue plenty — the
    # scenario returns a 500 once exhausted, which would fail the run.
    scenario.add(json_body=_AUTH_OK)
    for _ in range(200):
        scenario.add(json_body=_envelope([]))

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams=(
            "  chargebacks:\n    params:\n      lookback_days: 0\n"
            "      window_chunk_days: 30"
        ),
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    for _ in range(2):
        result = dtex.run(
            config="cb911_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )
        assert result.status.value == "succeeded", result.error

    data_paths = [
        r.path for r in scenario.captured if r.path.startswith("/clients/")
    ]
    # Run 1 = 1 unfiltered request; run 2 = one request per 30-day chunk
    # from 2026-01-01 to today, which is several months → many windows.
    assert "start_date" not in data_paths[0]
    incremental = data_paths[1:]
    assert len(incremental) > 1, (
        f"expected the wide window to be chunked, got one request: {incremental}"
    )

    windows = [_window_of(p) for p in incremental]
    # Every request is bounded to the configured chunk width.
    for start, end in windows:
        span_days = (date.fromisoformat(end) - date.fromisoformat(start)).days + 1
        assert 1 <= span_days <= 30, f"window {start}→{end} spans {span_days}d"
    # Windows tile the span: each starts the day after the previous ended.
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        assert date.fromisoformat(next_start) == date.fromisoformat(prev_end) + timedelta(
            days=1
        ), f"gap/overlap between {prev_end} and {next_start}"
    # The walk starts at the cursor and ends today.
    assert windows[0][0] == "2026-01-01"
    assert windows[-1][1] == datetime.now(tz=UTC).date().isoformat()


def test_chargebacks_single_short_window_is_one_request(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Chunking must not multiply requests for the everyday case: a cursor
    from today plus a small lookback is one window, not many."""
    scenario, base_url = cb_stub
    _setenv_credentials(monkeypatch)

    today = datetime.now(tz=UTC).date().isoformat()
    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([_chargeback_row("cb_1", today)]))
    scenario.add(json_body=_AUTH_OK)
    for _ in range(10):
        scenario.add(json_body=_envelope([]))

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams=(
            "  chargebacks:\n    params:\n      lookback_days: 0\n"
            "      window_chunk_days: 30"
        ),
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    for _ in range(2):
        result = dtex.run(
            config="cb911_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )
        assert result.status.value == "succeeded", result.error

    incremental = [
        r.path for r in scenario.captured if r.path.startswith("/clients/")
    ][1:]
    assert len(incremental) == 1, incremental
    assert f"start_date={today}" in incremental[0]
    assert f"end_date={today}" in incremental[0]
    assert "date_column=date_updated" in incremental[0]


# --------------------------------------------------------------------------
# _iter_windows — the chunking arithmetic, unit-level
# --------------------------------------------------------------------------


class _FakeCursor:
    """Minimal Cursor stand-in: only start_value() is used by _iter_windows."""

    def __init__(self, value: Any) -> None:
        self._value = value

    def start_value(self) -> Any:
        return self._value


def _windows(value: Any, *, lookback: int = 0, chunk: int = 30) -> list[dict[str, str]]:
    from dtex.sources.chargebacks911.source import _iter_windows

    return _iter_windows(_FakeCursor(value), lookback, chunk)


def test_iter_windows_no_cursor_is_one_unfiltered_request() -> None:
    """Bootstrap / --full-refresh: CB911 503s on wide windows, so the first
    run cannot be expressed as a date range at all."""
    assert _windows(None) == [{}]


def test_iter_windows_tiles_without_gap_or_overlap() -> None:
    today = datetime.now(tz=UTC).date()
    start = today - timedelta(days=95)
    windows = _windows(start.isoformat(), chunk=30)

    assert windows[0]["start_date"] == start.isoformat()
    assert windows[-1]["end_date"] == today.isoformat()
    assert all(w["date_column"] == "date_updated" for w in windows)
    for prev, nxt in zip(windows, windows[1:], strict=False):
        assert date.fromisoformat(nxt["start_date"]) == date.fromisoformat(
            prev["end_date"]
        ) + timedelta(days=1)


def test_iter_windows_applies_lookback_before_chunking() -> None:
    today = datetime.now(tz=UTC).date()
    windows = _windows(today.isoformat(), lookback=7, chunk=30)

    assert len(windows) == 1
    assert windows[0]["start_date"] == (today - timedelta(days=7)).isoformat()
    assert windows[0]["end_date"] == today.isoformat()


def test_iter_windows_accepts_cursor_with_time_component() -> None:
    """Cursor values may carry a time ("2026-01-06 11:30:00") — only the
    ISO date part is meaningful for the filter."""
    today = datetime.now(tz=UTC).date()
    windows = _windows(f"{today.isoformat()} 11:30:00", chunk=30)

    assert windows[0]["start_date"] == today.isoformat()


def test_iter_windows_future_cursor_still_yields_one_window() -> None:
    """A cursor ahead of today (clock skew) must not produce an EMPTY window
    list — that would silently sync nothing instead of erroring or syncing."""
    future = (datetime.now(tz=UTC).date() + timedelta(days=30)).isoformat()
    windows = _windows(future, chunk=30)

    assert len(windows) == 1
    assert windows[0]["end_date"] == datetime.now(tz=UTC).date().isoformat()


def test_iter_windows_chunk_days_floor_is_one_day() -> None:
    """A nonsensical chunk width (0 or negative) must not spin forever —
    it clamps to 1-day windows."""
    today = datetime.now(tz=UTC).date()
    windows = _windows((today - timedelta(days=2)).isoformat(), chunk=0)

    assert len(windows) == 3
    assert [w["start_date"] for w in windows] == [
        (today - timedelta(days=2)).isoformat(),
        (today - timedelta(days=1)).isoformat(),
        today.isoformat(),
    ]


def test_credentials_never_appear_in_logs(
    cb_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Neither the username nor the password is logged across an entire
    engine run — even at DEBUG."""
    scenario, base_url = cb_stub
    username = "user_super_secret_should_not_leak"
    password = "pw_super_secret_should_not_leak_123"
    monkeypatch.setenv("CHARGEBACKS911_USERNAME", username)
    monkeypatch.setenv("CHARGEBACKS911_PASSWORD", password)

    scenario.add(json_body=_AUTH_OK)
    scenario.add(json_body=_envelope([]))

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  alerts:")

    db_path = str(tmp_path / "warehouse.duckdb")
    with caplog.at_level("DEBUG"):
        result = dtex.run(
            config="cb911_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )

    assert result.status.value == "succeeded", result.error

    full_log = "\n".join(record.getMessage() for record in caplog.records)
    assert username not in full_log, "CB911 username leaked into captured logs"
    assert password not in full_log, "CB911 password leaked into captured logs"
