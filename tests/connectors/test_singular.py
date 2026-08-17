# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Singular Reporting API connector.

Every test stands up a tiny ``http.server.HTTPServer`` on a random port
and points :class:`SingularClient` at it. The stub records every request
and responds from a scripted scenario — no real network calls.

Test areas:

* Client transport — the create → poll → download flow, the auth header
  on API calls but NOT on the signed download URL, bounded retry on 429,
  FAILED reports raise with the server's error message.
* Windowing — lookback subtraction, tiling without gap/overlap, the
  yesterday ceiling (today is never fetched), first-run bootstrap from
  ``initial_since_date``.
* Row shaping — string→number coercion, ``''``→NULL, cohort ``revenue``
  dict flattening, dimension NULL→'' (merge-key safety), projection
  (undeclared API fields never land).
* End-to-end — ``dtex.run`` drives both streams into DuckDB; agency rows
  carry the agency tag; the incremental cursor advances.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

import duckdb
import pytest

import dtex
from dtex.sources.singular.client import SingularClient
from dtex.sources.singular.source import (
    _record,
    _report_query,
    _windows,
)

# --------------------------------------------------------------------------
# Stub Singular API server
# --------------------------------------------------------------------------


class _RequestRecord:
    """One captured request — method, path, headers, form body."""

    def __init__(
        self, method: str, path: str, headers: dict[str, str], body: str
    ) -> None:
        self.method = method
        self.path = path
        self.headers = headers
        self.body = body

    @property
    def form(self) -> dict[str, str]:
        return {k: v[0] for k, v in parse_qs(self.body).items()}


class _Scenario:
    """Scripts responses + captures requests for one test."""

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
            {"status": status, "body": json_body, "headers": extra_headers or {}}
        )

    def pop(self) -> dict[str, Any]:
        if not self._queue:
            return {"status": 500, "body": {"error": "scenario exhausted"}, "headers": {}}
        return self._queue.pop(0)


def _make_handler(scenario: _Scenario) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def _respond(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode() if length else ""
            scenario.captured.append(
                _RequestRecord(method, self.path, dict(self.headers), body)
            )
            response = scenario.pop()
            payload = json.dumps(
                response["body"] if response["body"] is not None else {}
            ).encode()
            self.send_response(response["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            for k, v in response["headers"].items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802 — required by stdlib
            self._respond("GET")

        def do_POST(self) -> None:  # noqa: N802 — required by stdlib
            self._respond("POST")

    return Handler


@pytest.fixture
def singular_stub() -> Iterator[tuple[_Scenario, str]]:
    scenario = _Scenario()
    server = HTTPServer(("127.0.0.1", 0), _make_handler(scenario))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield scenario, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


def _client(base_url: str, *, max_retries: int = 3) -> SingularClient:
    return SingularClient(
        api_key="sk_test_unit",
        base_url=base_url,
        poll_timeout_sec=5,
        poll_interval_sec=0.01,
        max_retries=max_retries,
    )


def _created(report_id: str = "r1") -> dict[str, Any]:
    return {"status": 0, "value": {"report_id": report_id}}


def _status_done(download_url: str) -> dict[str, Any]:
    return {"status": 0, "value": {"status": "DONE", "download_url": download_url}}


def _status_running() -> dict[str, Any]:
    return {"status": 0, "value": {"status": "STARTED"}}


def _results(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"results": rows}


def _api_row(
    day: str,
    *,
    source: str = "Peppaads",
    installs: str = "12",
    revenue: float | None = 88.5,
) -> dict[str, Any]:
    """A live-shaped report row — numbers as strings, cohort revenue nested."""
    return {
        "start_date": day,
        "end_date": day,
        "app": "Example App",
        "source": source,
        "os": "Android",
        "platform": "Android",
        "country_field": "USA",
        "adn_campaign_id": "N/A",
        "adn_campaign_name": "N/A",
        "adn_impressions": "",
        "custom_clicks": "40",
        "custom_installs": installs,
        "adn_cost": "",
        "revenue": {"actual": revenue},
        "an_undeclared_field": "must not land",  # projection drops it
    }


# --------------------------------------------------------------------------
# Client transport
# --------------------------------------------------------------------------


def test_create_poll_download_flow(singular_stub: tuple[_Scenario, str]) -> None:
    """create → poll (running once) → poll (done) → download; auth header on
    API calls, NOT on the signed download URL."""
    scenario, base_url = singular_stub
    scenario.add(json_body=_created())
    scenario.add(json_body=_status_running())
    scenario.add(json_body=_status_done(f"{base_url}/signed/r1"))
    scenario.add(json_body=_results([_api_row("2026-08-01")]))

    rows = _client(base_url).run_report(
        {"start_date": "2026-08-01", "end_date": "2026-08-01"}
    )

    assert len(rows) == 1
    create, poll1, poll2, download = scenario.captured
    assert (create.method, create.path) == ("POST", "/v2.0/create_async_report")
    assert create.headers.get("Authorization") == "sk_test_unit"
    assert create.form["start_date"] == "2026-08-01"
    assert poll1.path.startswith("/v2.0/get_report_status")
    assert "report_id=r1" in poll1.path
    assert poll2.headers.get("Authorization") == "sk_test_unit"
    assert download.path == "/signed/r1"
    assert download.headers.get("Authorization") is None  # signed URL: bare GET


def test_failed_report_raises_with_message(
    singular_stub: tuple[_Scenario, str],
) -> None:
    scenario, base_url = singular_stub
    scenario.add(json_body=_created())
    scenario.add(
        json_body={
            "status": 0,
            "value": {"status": "FAILED", "error_message": "bad dimension"},
        }
    )

    with pytest.raises(RuntimeError, match="bad dimension"):
        _client(base_url).run_report({"start_date": "x", "end_date": "x"})


def test_429_bounded_retry(singular_stub: tuple[_Scenario, str]) -> None:
    """A sustained 429 raises after max_retries instead of looping forever."""
    scenario, base_url = singular_stub
    for _ in range(3):
        scenario.add(status=429, extra_headers={"Retry-After": "0"})

    with pytest.raises(RuntimeError, match="rate-limited"):
        _client(base_url, max_retries=2).run_report(
            {"start_date": "x", "end_date": "x"}
        )
    assert len(scenario.captured) == 3  # initial + 2 retries


def test_5xx_then_success(singular_stub: tuple[_Scenario, str]) -> None:
    scenario, base_url = singular_stub
    scenario.add(status=503)
    scenario.add(json_body=_created())
    scenario.add(json_body=_status_done(f"{base_url}/signed/r1"))
    scenario.add(json_body=_results([]))

    assert _client(base_url).run_report({"start_date": "x", "end_date": "x"}) == []


# --------------------------------------------------------------------------
# Windowing
# --------------------------------------------------------------------------

_TODAY = date(2026, 8, 17)


def test_windows_first_run_starts_at_initial() -> None:
    windows = _windows(
        None,
        initial_since_date="2026-08-01",
        lookback_days=30,
        chunk_days=7,
        today=_TODAY,
    )
    assert windows[0][0] == date(2026, 8, 1)
    assert windows[-1][1] == date(2026, 8, 16)  # yesterday, never today


def test_windows_tile_without_gap_or_overlap() -> None:
    windows = _windows(
        None,
        initial_since_date="2026-06-01",
        lookback_days=0,
        chunk_days=10,
        today=_TODAY,
    )
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        assert next_start == prev_end + timedelta(days=1)
    assert windows[0][0] == date(2026, 6, 1)
    assert windows[-1][1] == date(2026, 8, 16)


def test_windows_apply_lookback_behind_cursor() -> None:
    windows = _windows(
        date(2026, 8, 10),
        initial_since_date="2026-01-01",
        lookback_days=7,
        chunk_days=31,
        today=_TODAY,
    )
    assert windows[0][0] == date(2026, 8, 3)  # cursor − 7d


def test_windows_lookback_floored_at_initial() -> None:
    windows = _windows(
        "2026-08-10",
        initial_since_date="2026-08-08",
        lookback_days=30,
        chunk_days=31,
        today=_TODAY,
    )
    assert windows[0][0] == date(2026, 8, 8)


def test_windows_empty_when_nothing_complete() -> None:
    assert (
        _windows(
            None,
            initial_since_date="2026-08-17",
            lookback_days=0,
            chunk_days=31,
            today=_TODAY,
        )
        == []
    )


def test_windows_chunk_floor_is_one_day() -> None:
    windows = _windows(
        None,
        initial_since_date="2026-08-14",
        lookback_days=0,
        chunk_days=0,
        today=_TODAY,
    )
    assert all((end - start).days == 0 for start, end in windows)
    assert len(windows) == 3  # 14th, 15th, 16th


# --------------------------------------------------------------------------
# Row shaping
# --------------------------------------------------------------------------


def test_record_coerces_and_projects() -> None:
    record = _record(_api_row("2026-08-01"))
    assert record["date"] == "2026-08-01"
    assert record["custom_installs"] == 12
    assert record["custom_clicks"] == 40
    assert record["adn_impressions"] is None  # '' → NULL
    assert record["adn_cost"] is None
    assert record["revenue"] == 88.5  # {'actual': …} flattened
    assert "an_undeclared_field" not in record
    assert "agency" not in record


def test_record_null_dimensions_become_empty_strings() -> None:
    row = _api_row("2026-08-02")
    row["adn_campaign_id"] = None
    record = _record(row, agency="agency-a")
    assert record["adn_campaign_id"] == ""  # merge-key safety: never NULL
    assert record["agency"] == "agency-a"


def test_record_revenue_none_period() -> None:
    assert _record(_api_row("2026-08-03", revenue=None))["revenue"] is None


def test_report_query_agency_filter() -> None:
    query = _report_query(date(2026, 8, 1), date(2026, 8, 7), agency="agency-a")
    filters = json.loads(query["filters"])
    assert filters == [
        {"dimension": "agency_name", "operator": "in", "values": ["agency-a"]}
    ]
    assert query["time_breakdown"] == "day"
    assert "filters" not in _report_query(date(2026, 8, 1), date(2026, 8, 7))


# --------------------------------------------------------------------------
# End-to-end with dtex.run — both streams through the engine into DuckDB
# --------------------------------------------------------------------------


def _write_project(tmp_path: Path) -> None:
    (tmp_path / "dtex_project.yml").write_text(
        "name: t\nversion: '0.1'\nsource_paths: []\n"
        "destination_paths: []\nconfig_paths:\n  - configs\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "duckdb:\n  default_target: dev\n  targets:\n    dev:\n"
        "      path: '.dtex/warehouse.duckdb'\n"
    )


def _write_config(
    tmp_path: Path, *, base_url: str, streams: str, agencies: str = ""
) -> None:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    since = (datetime.now(tz=UTC).date() - timedelta(days=2)).isoformat()
    (tmp_path / "configs" / "singular_test.yml").write_text(
        "name: singular_test\n"
        "source: singular\n"
        "destination: duckdb\n"
        "target: dev\n"
        "params:\n"
        f"  base_url: '{base_url}'\n"
        f"  initial_since_date: '{since}'\n"
        "  lookback_days: 1\n"
        "  window_chunk_days: 31\n"
        "  poll_interval_sec: 0.01\n"
        f"  agencies: '{agencies}'\n"
        f"streams:\n{streams}\n"
    )


def test_end_to_end_network_reports(
    singular_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """network_reports lands coerced rows in DuckDB via one report window."""
    scenario, base_url = singular_stub
    monkeypatch.setenv("SINGULAR_API_KEY", "sk_e2e_secret")

    day1 = (datetime.now(tz=UTC).date() - timedelta(days=2)).isoformat()
    day2 = (datetime.now(tz=UTC).date() - timedelta(days=1)).isoformat()
    scenario.add(json_body=_created())
    scenario.add(json_body=_status_done(f"{base_url}/signed/r1"))
    scenario.add(
        json_body=_results(
            [
                _api_row(day1, source="Peppaads", installs="12"),
                _api_row(day2, source="Facebook", installs="", revenue=None),
            ]
        )
    )

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  network_reports:")

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="singular_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date, source, custom_installs, revenue "
        "FROM network_reports ORDER BY date"
    ).fetchall()
    columns = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'network_reports'"
        ).fetchall()
    }
    conn.close()

    assert rows == [
        (date.fromisoformat(day1), "Peppaads", 12, 88.5),
        (date.fromisoformat(day2), "Facebook", None, None),
    ]
    assert "an_undeclared_field" not in columns
    # The report went out asking for exactly the declared grain.
    create = scenario.captured[0]
    assert create.form["dimensions"].startswith("app,source,os,platform")
    assert create.form["cohort_periods"] == "actual"


def test_end_to_end_agency_reports_tags_rows(
    singular_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """agency_reports fetches once per configured agency and tags rows."""
    scenario, base_url = singular_stub
    monkeypatch.setenv("SINGULAR_API_KEY", "sk_e2e_secret")

    day1 = (datetime.now(tz=UTC).date() - timedelta(days=2)).isoformat()
    scenario.add(json_body=_created("ra"))
    scenario.add(json_body=_status_done(f"{base_url}/signed/ra"))
    scenario.add(json_body=_results([_api_row(day1, source="Peppaads")]))

    _write_project(tmp_path)
    _write_config(
        tmp_path, base_url=base_url, streams="  agency_reports:", agencies="agency-a"
    )

    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="singular_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": db_path},
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT agency, source, custom_installs FROM agency_reports"
    ).fetchall()
    conn.close()
    assert rows == [("agency-a", "Peppaads", 12)]

    # The agency went out as a filter on the report request.
    create = scenario.captured[0]
    filters = json.loads(create.form["filters"])
    assert filters[0]["values"] == ["agency-a"]


def test_agency_reports_without_agencies_is_a_noop(
    singular_stub: tuple[_Scenario, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No agencies configured → no HTTP calls, run still succeeds."""
    scenario, base_url = singular_stub
    monkeypatch.setenv("SINGULAR_API_KEY", "sk_e2e_secret")

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  agency_reports:")

    result = dtex.run(
        config="singular_test",
        project_dir=str(tmp_path),
        destination_params_override={"path": str(tmp_path / "warehouse.duckdb")},
    )
    assert result.status.value == "succeeded", result.error
    assert scenario.captured == []
