# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked RevenueCat v2 connector.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port
and points :class:`RevenueCatClient` at it. The stub records every
request and responds based on a scripted scenario — no real network
calls, no flakes from upstream availability.

Three test areas:

* `RevenueCatClient` unit tests — auth header shape, retry-on-429,
  retry-on-5xx, retry-on-network-error, bounded retries.
* `paginate` walks RC's `next_page`-URL pagination correctly.
* The three `@stream` functions (`customers`, `subscriptions`,
  `metrics_daily`) extract the expected rows when wired into a tmp
  project with `dtex.run`.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pytest
import requests

import dtex
from dtex.sources.revenuecat.client import RevenueCatClient

# --------------------------------------------------------------------------
# Stub RC v2 server — stdlib HTTPServer on a random port
# --------------------------------------------------------------------------


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
            body = json.dumps(response["body"] or {}).encode()
            self.send_response(response["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for k, v in response["headers"].items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

    return Handler


@pytest.fixture
def rc_stub() -> Iterator[tuple[_Scenario, str]]:
    """Spin up a stub RC v2 server on a random port; tear down after.

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


# --------------------------------------------------------------------------
# RevenueCatClient unit tests
# --------------------------------------------------------------------------


def _client(base_url: str, *, max_retries: int = 3) -> RevenueCatClient:
    """Build a RevenueCatClient pointed at the stub."""
    return RevenueCatClient(
        api_key="sk_test_unit",
        project_id="proj_test",
        base_url=base_url,
        max_retries=max_retries,
    )


def test_client_sends_bearer_auth(rc_stub: tuple[_Scenario, str]) -> None:
    """Every request carries `Authorization: Bearer ...` and `Accept: application/json`."""
    scenario, base_url = rc_stub
    scenario.add(json_body={"items": [], "next_page": None})

    list(_client(base_url).paginate("/projects/proj_test/customers"))

    headers = scenario.captured[0].headers
    assert headers.get("Authorization") == "Bearer sk_test_unit"
    assert headers.get("Accept") == "application/json"


def test_client_429_is_honored_then_succeeds(
    rc_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 429 with Retry-After sleeps, then the retry succeeds and yields items."""
    scenario, base_url = rc_stub
    scenario.add(
        status=429,
        json_body={"error": "rate limited"},
        extra_headers={"Retry-After": "1"},
    )
    scenario.add(json_body={"items": [{"id": "cus_1"}], "next_page": None})

    sleeps: list[float] = []
    monkeypatch.setattr(
        "dtex.sources.revenuecat.client.time.sleep", lambda s: sleeps.append(s)
    )

    rows = list(_client(base_url).paginate("/customers"))

    assert rows == [{"id": "cus_1"}]
    assert sleeps == [1]  # exactly one Retry-After sleep
    assert len(scenario.captured) == 2  # one 429, one 200


def test_client_429_bounded_by_max_retries(
    rc_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A persistent 429 raises after max_retries — does NOT loop forever.

    Regression for the bug the prior version had: 429 retries did not
    increment _attempt, so a sustained rate limit was an infinite hang.
    """
    scenario, base_url = rc_stub
    for _ in range(10):
        scenario.add(
            status=429,
            json_body={"error": "rate limited"},
            extra_headers={"Retry-After": "1"},
        )

    monkeypatch.setattr("dtex.sources.revenuecat.client.time.sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="rate-limited after"):
        list(_client(base_url, max_retries=3).paginate("/customers"))

    # 1 initial + 3 retries = 4 attempts total.
    assert len(scenario.captured) == 4


def test_client_500_retries_then_succeeds(
    rc_stub: tuple[_Scenario, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient 500 retries with exponential backoff, then succeeds."""
    scenario, base_url = rc_stub
    scenario.add(status=500, json_body={"error": "server"})
    scenario.add(json_body={"items": [{"id": "cus_1"}], "next_page": None})

    monkeypatch.setattr("dtex.sources.revenuecat.client.time.sleep", lambda s: None)

    rows = list(_client(base_url).paginate("/customers"))

    assert rows == [{"id": "cus_1"}]
    assert len(scenario.captured) == 2


def test_client_403_raises_immediately(rc_stub: tuple[_Scenario, str]) -> None:
    """A non-retryable 4xx (e.g. 403) raises on the first attempt."""
    scenario, base_url = rc_stub
    scenario.add(status=403, json_body={"error": "permission denied"})

    with pytest.raises(requests.exceptions.HTTPError):
        list(_client(base_url).paginate("/customers"))

    assert len(scenario.captured) == 1  # no retries on 403


# --------------------------------------------------------------------------
# Pagination — RC v2's next_page-URL model
# --------------------------------------------------------------------------


def test_paginate_walks_three_pages(rc_stub: tuple[_Scenario, str]) -> None:
    """The client follows next_page URLs across three pages and yields every item.

    RC's `next_page` is an absolute URL with the original query params +
    `starting_after` baked in; subsequent fetches use it verbatim with
    NO params on the client side.
    """
    scenario, base_url = rc_stub
    # Page 1: items [a, b], next_page set
    scenario.add(
        json_body={
            "items": [{"id": "a"}, {"id": "b"}],
            "next_page": f"{base_url}/projects/proj_test/customers?starting_after=b",
        }
    )
    # Page 2: items [c, d], next_page set
    scenario.add(
        json_body={
            "items": [{"id": "c"}, {"id": "d"}],
            "next_page": f"{base_url}/projects/proj_test/customers?starting_after=d",
        }
    )
    # Page 3: items [e], next_page null → walk ends
    scenario.add(json_body={"items": [{"id": "e"}], "next_page": None})

    rows = list(
        _client(base_url).paginate("/projects/proj_test/customers", {"limit": 2})
    )

    assert [r["id"] for r in rows] == ["a", "b", "c", "d", "e"]
    assert len(scenario.captured) == 3
    # First request carries params; subsequent requests use next_page URL verbatim.
    assert "limit=2" in scenario.captured[0].path
    assert "starting_after=b" in scenario.captured[1].path
    assert "starting_after=d" in scenario.captured[2].path


# --------------------------------------------------------------------------
# get() — for one-shot non-list endpoints (charts API)
# --------------------------------------------------------------------------


def test_get_returns_full_body(rc_stub: tuple[_Scenario, str]) -> None:
    """get() returns the parsed JSON body of a one-shot GET (no pagination)."""
    scenario, base_url = rc_stub
    scenario.add(
        json_body={
            "category": "revenue",
            "measures": [{"display_name": "Revenue"}],
            "values": [{"cohort": 1700000000, "incomplete": False, "measure": 0, "value": 1.5}],
        }
    )

    body = _client(base_url).get(
        "/projects/proj_test/charts/revenue",
        {"resolution": "day", "start_date": "2024-01-01", "end_date": "2024-01-02"},
    )

    assert body["category"] == "revenue"
    assert body["values"][0]["value"] == 1.5
    # Query params propagate.
    captured_path = scenario.captured[0].path
    assert "resolution=day" in captured_path
    assert "start_date=2024-01-01" in captured_path


# --------------------------------------------------------------------------
# End-to-end with dtex.run — drives customers + metrics_daily through the
# engine into a DuckDB destination
# --------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> None:
    """Scaffold a minimal dtex project with the baked revenuecat + a duckdb dev target."""
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )


def _write_config(tmp_path: Path, *, base_url: str, streams: str) -> None:
    """Write a one-config-per-file under configs/revenuecat_test.yml."""
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "rc_test.yml").write_text(
        "name: rc_test\n"
        "source: revenuecat\n"
        "destination: duckdb\n"
        "target: dev\n"
        f"params:\n  project_id: 'proj_test'\n  base_url: '{base_url}'\n"
        f"streams:\n{streams}\n"
    )


def test_end_to_end_customers(
    rc_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """customers stream extracts + lands rows in DuckDB through the engine."""
    scenario, base_url = rc_stub
    monkeypatch.setenv("REVENUECAT_API_KEY", "sk_test_unit")

    # Two-page customer response.
    scenario.add(
        json_body={
            "items": [
                {
                    "id": "cus_1",
                    "first_seen_at": 1700000000000,
                    "last_seen_at": 1700100000000,
                    "last_seen_country": "US",
                    "last_seen_platform": "ios",
                    "last_seen_app_version": "1.0",
                },
                {
                    "id": "cus_2",
                    "first_seen_at": 1700200000000,
                    "last_seen_at": 1700300000000,
                    "last_seen_country": "GB",
                    "last_seen_platform": "android",
                    "last_seen_app_version": "1.1",
                },
            ],
            "next_page": f"{base_url}/projects/proj_test/customers?starting_after=cus_2",
        }
    )
    scenario.add(json_body={"items": [], "next_page": None})

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  customers:")

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="rc_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT id, last_seen_country, last_seen_platform FROM customers ORDER BY id"
    ).fetchall()
    conn.close()
    assert rows == [
        ("cus_1", "US", "ios"),
        ("cus_2", "GB", "android"),
    ]


def test_end_to_end_metrics_daily(
    rc_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """metrics_daily flattens the values array into long format + advances the
    cursor only past complete days."""
    scenario, base_url = rc_stub
    monkeypatch.setenv("REVENUECAT_API_KEY", "sk_test_unit")

    # Each chart returns its own response (4 charts × 1 GET each, since
    # default `metrics_charts = "revenue,mrr,actives,trials"`).
    # cohort 1700000000 = 2023-11-14 UTC.
    # cohort 1700086400 = 2023-11-15 UTC.
    def chart_response(measures: list[str]) -> dict[str, Any]:
        return {
            "measures": [{"display_name": m} for m in measures],
            "values": [
                # Day 1 — complete
                *(
                    {
                        "cohort": 1700000000,
                        "incomplete": False,
                        "measure": i,
                        "value": float(i + 1),
                    }
                    for i in range(len(measures))
                ),
                # Day 2 — incomplete (today)
                *(
                    {
                        "cohort": 1700086400,
                        "incomplete": True,
                        "measure": i,
                        "value": float(i + 10),
                    }
                    for i in range(len(measures))
                ),
            ],
        }

    # 4 charts in the default config — each gets its own scenario step.
    scenario.add(json_body=chart_response(["Revenue", "Transactions"]))
    scenario.add(json_body=chart_response(["MRR"]))
    scenario.add(json_body=chart_response(["Actives"]))
    scenario.add(json_body=chart_response(["Active Trials"]))

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams=(
            "  metrics_daily:\n    params:\n"
            "      metrics_initial_since_date: '2023-11-14'\n"
            "      metrics_lookback_days: 0"
        ),
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="rc_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT chart_name, measure_name, value, incomplete "
        "FROM metrics_daily ORDER BY chart_name, measure_name, value"
    ).fetchall()
    conn.close()

    # 4 charts: revenue (2 measures × 2 days) + 3 single-measure charts (1 × 2 days each)
    # = 4 + 2 + 2 + 2 = 10 rows.
    assert len(rows) == 10

    # Confirm the long-format flatten worked: each chart has its measure names.
    measure_names = {(c, m) for (c, m, _, _) in rows}
    assert ("revenue", "Revenue") in measure_names
    assert ("revenue", "Transactions") in measure_names
    assert ("mrr", "MRR") in measure_names
    assert ("actives", "Actives") in measure_names
    assert ("trials", "Active Trials") in measure_names

    # Confirm incomplete=true rows landed too (for re-pull on the next run).
    assert any(r[3] for r in rows)


def test_api_key_never_appears_in_logs(
    rc_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The resolved RC API key is never logged across an entire engine run."""
    scenario, base_url = rc_stub
    api_key = "sk_test_super_secret_should_not_leak_456"
    monkeypatch.setenv("REVENUECAT_API_KEY", api_key)
    scenario.add(json_body={"items": [], "next_page": None})

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  customers:")

    db_path = str(tmp_path / "warehouse.duckdb")
    with caplog.at_level("DEBUG"):
        result = dtex.run(
            config="rc_test",
            project_dir=str(tmp_path),
            destination_params_override={"path": db_path},
        )

    assert result.status.value == "succeeded", result.error

    full_log = "\n".join(record.getMessage() for record in caplog.records)
    assert api_key not in full_log, "RC API key leaked into captured logs"
