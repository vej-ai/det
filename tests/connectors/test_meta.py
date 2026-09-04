# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Tests for the baked Meta Marketing API connector.

Every transport / end-to-end test stands up a tiny ``http.server.HTTPServer``
on a random port that plays the Graph API Insights edge and points
:class:`MetaClient` at it. The stub records every request and answers by
route — no real network calls.

Test areas:

* Client transport — the submit → poll → page async report flow, the token
  as a query/form param on every call, ``Job Failed`` resubmits then
  :class:`AsyncJobFailed`, bounded 429 / rate-limit-code retry, a
  deterministic 190 raising at once with the token redacted, the hourly
  ``level`` / ``breakdowns`` submit params and the account timezone GET.
* Pure helpers — account parsing, inclusive window tiling, hour parsing,
  ad-level and hourly record projection, usage-header parsing.
* Walk — lookback behind the cursor, cursor observed only after every
  account of a window, window BISECTION when a job keeps failing.
* End-to-end — ``dtex.run`` drives both streams into DuckDB; numbers are
  coerced, arrays land as JSON, the hour is parsed, the timezone is
  stamped, the per-stream cursor advances.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

import dtex
from dtex.sources.meta import client as client_mod
from dtex.sources.meta.client import (
    AsyncJobFailed,
    MetaClient,
    _is_rate_limit,
    _max_usage_pct,
    _redact,
)
from dtex.sources.meta.records import (
    HOURLY_BREAKDOWN,
    as_date,
    iter_windows,
    parse_accounts,
    parse_hour,
    to_hourly_record,
    to_record,
)
from dtex.sources.meta.source import HOURLY_FIELDS, _walk

# --------------------------------------------------------------------------
# Stub Graph API server
# --------------------------------------------------------------------------


class _Request:
    """One captured request — method, path (no query), query/form params."""

    def __init__(self, method: str, raw_path: str, body: str) -> None:
        parsed = urlparse(raw_path)
        self.method = method
        self.path = parsed.path
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        form = {k: v[0] for k, v in parse_qs(body).items()}
        self.params: dict[str, str] = {**query, **form}


class _Graph:
    """A scripted Graph API: routes by path, plus a one-shot override queue.

    * ``POST /<ver>/act_<id>/insights`` → ``{"report_run_id": "RUN<n>"}``
    * ``GET  /<ver>/RUN<n>`` → next status from ``statuses`` (default
      ``Job Completed`` once the list is exhausted)
    * ``GET  /<ver>/RUN<n>/insights`` → the page for the ``after`` cursor
      from ``pages`` (a list of row lists; page i links to page i+1)
    * ``GET  /<ver>/act_<id>`` → ``accounts[id]``

    ``overrides`` is a FIFO of ``(status, body, headers)`` answered before
    any routing — for injecting a 429 / an error code on the next call.
    """

    def __init__(self) -> None:
        self.captured: list[_Request] = []
        self.statuses: list[str] = []
        self.pages: list[list[dict[str, Any]]] = [[]]
        self.pages_by_run: dict[str, list[list[dict[str, Any]]]] = {}
        self.accounts: dict[str, dict[str, Any]] = {}
        self.overrides: list[tuple[int, Any, dict[str, str]]] = []
        self.submits = 0

    def respond(self, req: _Request) -> tuple[int, Any, dict[str, str]]:
        if self.overrides:
            return self.overrides.pop(0)
        parts = req.path.strip("/").split("/")  # [ver, ...]
        if req.method == "POST" and len(parts) == 3 and parts[2] == "insights":
            self.submits += 1
            run_id = f"RUN{self.submits}"
            self.pages_by_run[run_id] = list(self.pages)
            return 200, {"report_run_id": run_id}, {}
        if req.method == "GET" and len(parts) == 2 and parts[1].startswith("act_"):
            return 200, self.accounts.get(parts[1][4:], {}), {}
        if req.method == "GET" and len(parts) == 2 and parts[1].startswith("RUN"):
            status = self.statuses.pop(0) if self.statuses else "Job Completed"
            return 200, {"async_status": status, "async_percent_completion": 100}, {}
        if req.method == "GET" and len(parts) == 3 and parts[2] == "insights":
            pages = self.pages_by_run.get(parts[1], self.pages)
            index = int(req.params.get("after", "P0")[1:])
            body: dict[str, Any] = {"data": pages[index] if index < len(pages) else []}
            if index + 1 < len(pages):
                body["paging"] = {"cursors": {"after": f"P{index + 1}"}, "next": "http://x"}
            return 200, body, {}
        return 404, {"error": {"code": 803, "message": f"unknown route {req.path}"}}, {}


def _make_handler(graph: _Graph) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: Any) -> None:
            return

        def _respond(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode() if length else ""
            req = _Request(method, self.path, body)
            graph.captured.append(req)
            status, payload_obj, extra = graph.respond(req)
            payload = json.dumps(payload_obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            for k, v in extra.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802 — required by stdlib
            self._respond("GET")

        def do_POST(self) -> None:  # noqa: N802 — required by stdlib
            self._respond("POST")

    return Handler


@pytest.fixture
def graph_stub() -> Iterator[tuple[_Graph, str]]:
    graph = _Graph()
    server = HTTPServer(("127.0.0.1", 0), _make_handler(graph))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield graph, f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoff / pacing / poll sleeps are real ``time.sleep`` calls — skip them."""
    monkeypatch.setattr(client_mod.time, "sleep", lambda _s: None)


def _client(base_url: str, *, max_retries: int = 3) -> MetaClient:
    return MetaClient(
        access_token="EAAtok",
        base_url=base_url,
        page_delay_seconds=0,
        poll_interval_seconds=0,
        job_timeout_seconds=5,
        max_retries=max_retries,
    )


def _ad_row(day: str, ad_id: str = "a1", spend: str = "12.50") -> dict[str, Any]:
    """A live-shaped ad-level Insights object — numbers as strings, arrays."""
    return {
        "account_id": "111",
        "account_name": "Acme",
        "account_currency": "USD",
        "campaign_id": "c1",
        "campaign_name": "Prospecting",
        "adset_id": "s1",
        "adset_name": "Broad",
        "ad_id": ad_id,
        "ad_name": "UGC-03",
        "objective": "OUTCOME_SALES",
        "optimization_goal": "OFFSITE_CONVERSIONS",
        "date_start": day,
        "date_stop": day,
        "impressions": "1503",
        "reach": "1287",
        "frequency": "1.167",
        "clicks": "41",
        "unique_clicks": "39",
        "spend": spend,
        "social_spend": "0",
        "full_view_impressions": "902",
        "actions": [
            {"action_type": "link_click", "value": "38"},
            {"action_type": "purchase", "value": "2"},
        ],
        "action_values": [{"action_type": "purchase", "value": "61.29"}],
        "conversions": [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "2"}],
        "video_p25_watched_actions": [{"action_type": "video_view", "value": "210"}],
    }


def _hourly_row(day: str, hour_range: str, campaign_id: str = "c1") -> dict[str, Any]:
    return {
        "account_id": "111",
        "account_name": "Acme",
        "account_currency": "USD",
        "campaign_id": campaign_id,
        "campaign_name": "Prospecting",
        "date_start": day,
        "date_stop": day,
        "impressions": "150",
        "clicks": "4",
        "spend": "2.31",
        "actions": [{"action_type": "purchase", "value": "1"}],
        "action_values": [{"action_type": "purchase", "value": "8.75"}],
        HOURLY_BREAKDOWN: hour_range,
    }


# --------------------------------------------------------------------------
# Client transport
# --------------------------------------------------------------------------


def test_submit_poll_page_flow(graph_stub: tuple[_Graph, str]) -> None:
    """POST submit → poll (running once) → completed → two result pages."""
    graph, base_url = graph_stub
    graph.statuses = ["Job Running"]
    graph.pages = [[_ad_row("2026-08-20")], [_ad_row("2026-08-21", ad_id="a2")]]

    pages = list(
        _client(base_url).iter_insights(
            "111", date(2026, 8, 20), date(2026, 8, 26), "spend,ad_id", "7d_click,1d_view"
        )
    )

    assert [p for p, _ in pages] == [1, 2]
    assert pages[0][1][0]["ad_id"] == "a1" and pages[1][1][0]["ad_id"] == "a2"

    submit = graph.captured[0]
    assert submit.method == "POST" and submit.path == "/v21.0/act_111/insights"
    assert submit.params["level"] == "ad" and submit.params["time_increment"] == "1"
    assert json.loads(submit.params["time_range"]) == {
        "since": "2026-08-20",
        "until": "2026-08-26",
    }
    assert submit.params["fields"] == "spend,ad_id"
    assert json.loads(submit.params["action_attribution_windows"]) == ["7d_click", "1d_view"]
    assert "breakdowns" not in submit.params
    # every call carries the token; the status poll asks for the status fields
    assert all(r.params["access_token"] == "EAAtok" for r in graph.captured)
    polls = [r for r in graph.captured if r.path == "/v21.0/RUN1"]
    assert len(polls) == 2 and polls[0].params["fields"].startswith("async_status")
    result_gets = [r for r in graph.captured if r.path == "/v21.0/RUN1/insights"]
    assert len(result_gets) == 2
    assert result_gets[0].params["limit"] == "500" and "after" not in result_gets[0].params
    assert result_gets[1].params["after"] == "P1"


def test_job_failed_is_resubmitted_then_raises_async_job_failed(
    graph_stub: tuple[_Graph, str],
) -> None:
    graph, base_url = graph_stub
    # One Job Failed → resubmit → completes.
    graph.statuses = ["Job Failed"]
    graph.pages = [[_ad_row("2026-08-20")]]
    pages = list(_client(base_url).iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))
    assert len(pages) == 1 and graph.submits == 2
    assert [r for r in graph.captured if r.path.endswith("/insights") and r.method == "GET"][
        0
    ].path == "/v21.0/RUN2/insights"

    # Fails on every resubmit → AsyncJobFailed after max_retries + 1 submits,
    # and NO result page was requested.
    graph.captured.clear()
    graph.submits = 0
    graph.statuses = ["Job Skipped"] * 10
    with pytest.raises(AsyncJobFailed, match="3 time"):
        list(
            _client(base_url, max_retries=2).iter_insights(
                "111", date(2026, 8, 20), date(2026, 8, 26), "f"
            )
        )
    assert graph.submits == 3
    assert not [r for r in graph.captured if r.method == "GET" and r.path.endswith("/insights")]


def test_rate_limit_retries_are_bounded(graph_stub: tuple[_Graph, str]) -> None:
    graph, base_url = graph_stub
    graph.pages = [[_ad_row("2026-08-20")]]
    # A plain 429 and a code-17 400 on the first two calls → both retried.
    graph.overrides = [
        (429, {"error": {"code": 4, "message": "too many"}}, {"Retry-After": "1"}),
        (400, {"error": {"code": 17, "message": "User request limit reached"}}, {}),
    ]
    pages = list(_client(base_url).iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))
    assert len(pages) == 1 and graph.submits == 1

    # Bounded: max_retries=1 with three limiter answers → raises with the remedy.
    graph.overrides = [(400, {"error": {"code": 80004, "message": "limit"}}, {})] * 3
    with pytest.raises(RuntimeError, match="page_delay_seconds"):
        list(
            _client(base_url, max_retries=1).iter_insights(
                "111", date(2026, 8, 20), date(2026, 8, 20), "f"
            )
        )


def test_transient_5xx_retried_and_bounded(graph_stub: tuple[_Graph, str]) -> None:
    graph, base_url = graph_stub
    graph.pages = [[_ad_row("2026-08-20")]]
    graph.overrides = [(503, {}, {}), (400, {"error": {"code": 2, "message": "temp"}}, {})]
    pages = list(_client(base_url).iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))
    assert len(pages) == 1

    graph.overrides = [(500, {}, {})] * 3
    with pytest.raises(RuntimeError, match="HTTP 500"):
        list(
            _client(base_url, max_retries=1).iter_insights(
                "111", date(2026, 8, 20), date(2026, 8, 20), "f"
            )
        )


def test_deterministic_errors_raise_at_once_without_the_token(
    graph_stub: tuple[_Graph, str],
) -> None:
    graph, base_url = graph_stub
    graph.overrides = [(400, {"error": {"code": 190, "message": "Invalid OAuth access token"}}, {})]
    with pytest.raises(RuntimeError) as excinfo:
        list(_client(base_url).iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))
    msg = str(excinfo.value)
    assert "Invalid OAuth access token" in msg and "regenerate" in msg
    assert "EAAtok" not in msg
    assert len(graph.captured) == 1  # no retry

    graph.captured.clear()
    graph.overrides = [(400, {"error": {"code": 200, "message": "no perms"}}, {})]
    with pytest.raises(RuntimeError, match="View performance"):
        list(_client(base_url).iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))
    assert len(graph.captured) == 1


def test_hourly_submit_params_and_account_timezone(graph_stub: tuple[_Graph, str]) -> None:
    graph, base_url = graph_stub
    graph.accounts["111"] = {
        "id": "act_111",
        "timezone_name": "America/Los_Angeles",
        "timezone_offset_hours_utc": -7,
    }
    graph.pages = [[_hourly_row("2026-08-20", "13:00:00 - 13:59:59")]]
    client = _client(base_url)

    info = client.get_account("111", "timezone_name,timezone_offset_hours_utc")
    assert info["timezone_name"] == "America/Los_Angeles"
    acct = graph.captured[-1]
    assert acct.path == "/v21.0/act_111" and acct.params["fields"].startswith("timezone_name")

    list(
        client.iter_insights(
            "111",
            date(2026, 8, 20),
            date(2026, 8, 20),
            HOURLY_FIELDS,
            level="campaign",
            breakdowns=HOURLY_BREAKDOWN,
        )
    )
    submit = next(r for r in graph.captured if r.method == "POST")
    assert submit.params["level"] == "campaign"
    assert submit.params["breakdowns"] == HOURLY_BREAKDOWN
    assert submit.params["fields"] == HOURLY_FIELDS


def test_job_timeout_raises(graph_stub: tuple[_Graph, str]) -> None:
    graph, base_url = graph_stub
    graph.statuses = ["Job Running"] * 50
    client = _client(base_url)
    client.job_timeout_seconds = 0.0
    with pytest.raises(RuntimeError, match="job_timeout_seconds"):
        list(client.iter_insights("111", date(2026, 8, 20), date(2026, 8, 20), "f"))


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------


def test_client_helpers() -> None:
    assert _is_rate_limit(4) and _is_rate_limit(17) and _is_rate_limit(613)
    assert _is_rate_limit(80004)
    assert not _is_rate_limit(100) and not _is_rate_limit(190) and not _is_rate_limit(None)
    headers = {
        "x-business-use-case-usage": (
            '{"123":[{"type":"ads_insights","call_count":12,"total_cputime":3,"total_time":87}]}'
        ),
        "x-ad-account-usage": '{"acc_id_util_pct":9.5}',
    }
    assert _max_usage_pct(headers) == 87.0
    assert _max_usage_pct({"x-ad-account-usage": '{"acc_id_util_pct":9.5}'}) == 9.5
    assert _max_usage_pct({}) == 0.0
    assert _max_usage_pct({"x-business-use-case-usage": "not json"}) == 0.0
    assert "EAAG" not in _redact("https://x/y?fields=spend&access_token=EAAGsecret&limit=5")
    assert _redact("no token here") == "no token here"


def test_parse_accounts() -> None:
    assert parse_accounts("act_1, 2 ,act_1,,3") == ["1", "2", "3"]
    with pytest.raises(ValueError, match="account_ids"):
        parse_accounts(" , ")


def test_iter_windows() -> None:
    assert list(iter_windows(date(2026, 8, 1), date(2026, 8, 17), 7)) == [
        (date(2026, 8, 1), date(2026, 8, 7)),
        (date(2026, 8, 8), date(2026, 8, 14)),
        (date(2026, 8, 15), date(2026, 8, 17)),
    ]
    assert list(iter_windows(date(2026, 8, 5), date(2026, 8, 5), 30)) == [
        (date(2026, 8, 5), date(2026, 8, 5))
    ]
    assert list(iter_windows(date(2026, 8, 6), date(2026, 8, 5), 7)) == []
    with pytest.raises(ValueError, match="window_days"):
        list(iter_windows(date(2026, 8, 1), date(2026, 8, 2), 0))


def test_as_date() -> None:
    assert as_date("2026-08-20") == date(2026, 8, 20)
    assert as_date("2026-08-20T10:11:12+00:00") == date(2026, 8, 20)
    assert as_date(datetime(2026, 8, 20, 5, tzinfo=UTC)) == date(2026, 8, 20)
    assert as_date(date(2026, 8, 20)) == date(2026, 8, 20)


def test_to_record() -> None:
    now = datetime.now(tz=UTC)
    sample = _ad_row("2026-08-20")
    rec = to_record(sample, now)
    assert rec is not None
    assert rec["date_start"] == "2026-08-20"
    assert rec["account_id"] == "111" and rec["ad_id"] == "a1"
    assert rec["extracted_at"] == now
    assert rec["actions"][1] == {"action_type": "purchase", "value": "2"}  # untouched
    assert rec["spend"] == "12.50"  # NORMALIZE casts, not us
    rec2 = to_record({**sample, "account_id": "act_42"}, now)
    assert rec2 is not None and rec2["account_id"] == "42"
    assert to_record({**sample, "ad_id": None}, now) is None
    assert to_record({k: v for k, v in sample.items() if k != "date_start"}, now) is None


def test_parse_hour() -> None:
    assert parse_hour("13:00:00 - 13:59:59") == 13
    assert parse_hour("00:00:00 - 00:59:59") == 0
    assert parse_hour("0:00:00 - 0:59:59") == 0
    assert parse_hour("23:00:00 - 23:59:59") == 23
    assert parse_hour(7) == 7
    for bad in (None, "", "unknown", "24:00:00 - 24:59:59", "13:00:00", 24, -1, True):
        assert parse_hour(bad) is None, bad


def test_to_hourly_record() -> None:
    now = datetime.now(tz=UTC)
    sample = _hourly_row("2026-08-20", "13:00:00 - 13:59:59")
    rec = to_hourly_record(sample, now, "America/Los_Angeles", -7.0)
    assert rec is not None
    assert rec["hour"] == 13 and rec["hour_range"] == "13:00:00 - 13:59:59"
    assert HOURLY_BREAKDOWN not in rec
    assert rec["account_id"] == "111" and rec["campaign_id"] == "c1"
    assert rec["timezone_name"] == "America/Los_Angeles"
    assert rec["timezone_offset_hours_utc"] == -7.0
    assert rec["spend"] == "2.31"
    rec2 = to_hourly_record({**sample, "account_id": "act_42"}, now, None, None)
    assert rec2 is not None and rec2["account_id"] == "42" and rec2["timezone_name"] is None
    assert to_hourly_record({**sample, HOURLY_BREAKDOWN: "unknown"}, now, "UTC", 0) is None
    no_breakdown = {k: v for k, v in sample.items() if k != HOURLY_BREAKDOWN}
    assert to_hourly_record(no_breakdown, now, "UTC", 0) is None
    assert to_hourly_record({**sample, "campaign_id": None}, now, "UTC", 0) is None
    assert to_hourly_record({**sample, "date_start": ""}, now, "UTC", 0) is None


# --------------------------------------------------------------------------
# The walk — lookback, cursor ordering, bisection (fake client, no HTTP)
# --------------------------------------------------------------------------


class _FakeConfig:
    def __init__(self, **kw: Any) -> None:
        self.start_date = "2026-08-01"
        self.window_days = 4
        self.lookback_days = 2
        self.action_attribution_windows = ""
        self.__dict__.update(kw)


class _FakeCursor:
    def __init__(self, start: date | None) -> None:
        self._start = start
        self.observed: list[date] = []

    def start_value(self) -> date | None:
        return self._start

    def observe(self, value: date) -> None:
        self.observed.append(value)


class _FakeClient:
    """``iter_insights`` records windows; spans wider than ``fail_over`` days
    raise :class:`AsyncJobFailed` (Meta shedding a too-large report)."""

    last_usage_pct = 0.0

    def __init__(self, fail_over: int | None = None) -> None:
        self.calls: list[tuple[str, date, date]] = []
        self.fail_over = fail_over

    def iter_insights(
        self, account: str, since: date, until: date, *_a: Any, **_k: Any
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        self.calls.append((account, since, until))
        if self.fail_over is not None and (until - since).days + 1 > self.fail_over:
            raise AsyncJobFailed("too big")
        day = since
        rows = []
        while day <= until:
            rows.append(_ad_row(day.isoformat(), ad_id=f"{account}-{day}"))
            day += timedelta(days=1)
        yield 1, rows


def _run_walk(
    client: Any, cursor: _FakeCursor, accounts: list[str], **config: Any
) -> list[list[dict[str, Any]]]:
    now = datetime.now(tz=UTC)
    return list(
        _walk(
            name="t",
            config=_FakeConfig(**config),  # type: ignore[arg-type]
            cursor=cursor,  # type: ignore[arg-type]
            log=logging.getLogger("t"),
            client=client,
            accounts=accounts,
            fields="f",
            level="ad",
            breakdowns="",
            project=lambda row, _a: to_record(row, now),
            key_desc="k",
        )
    )


def test_walk_virgin_run_tiles_from_start_date_and_observes_per_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today = datetime.now(tz=UTC).date()
    start = (today - timedelta(days=5)).isoformat()
    client = _FakeClient()
    cursor = _FakeCursor(None)
    batches = _run_walk(client, cursor, ["1", "2"], start_date=start, window_days=4)

    first = today - timedelta(days=5)
    # 6 days in 4-day windows → 2 windows × 2 accounts, oldest first.
    assert client.calls == [
        ("1", first, first + timedelta(days=3)),
        ("2", first, first + timedelta(days=3)),
        ("1", first + timedelta(days=4), today),
        ("2", first + timedelta(days=4), today),
    ]
    assert cursor.observed == [first + timedelta(days=3), today]
    assert sum(len(b) for b in batches) == 12  # 6 days × 2 accounts


def test_walk_resumes_lookback_behind_cursor_floored_at_start_date() -> None:
    today = datetime.now(tz=UTC).date()
    client = _FakeClient()
    _run_walk(
        client,
        _FakeCursor(today),
        ["1"],
        start_date=(today - timedelta(days=30)).isoformat(),
        window_days=30,
        lookback_days=3,
    )
    assert client.calls == [("1", today - timedelta(days=3), today)]

    # The floor wins over cursor − lookback.
    client = _FakeClient()
    _run_walk(
        client,
        _FakeCursor(today),
        ["1"],
        start_date=(today - timedelta(days=1)).isoformat(),
        window_days=30,
        lookback_days=10,
    )
    assert client.calls == [("1", today - timedelta(days=1), today)]

    # A floor in the future is a no-op — nothing requested, nothing observed.
    client = _FakeClient()
    cursor = _FakeCursor(None)
    future = (today + timedelta(days=1)).isoformat()
    assert _run_walk(client, cursor, ["1"], start_date=future) == []
    assert client.calls == [] and cursor.observed == []


def test_walk_bisects_a_window_whose_job_keeps_failing() -> None:
    today = datetime.now(tz=UTC).date()
    first = today - timedelta(days=7)  # 8 days → one 8-day window
    client = _FakeClient(fail_over=2)  # anything wider than 2 days fails
    cursor = _FakeCursor(None)
    batches = _run_walk(client, cursor, ["1"], start_date=first.isoformat(), window_days=8)

    # 8 → 4+4 → 2+2+2+2: the successful sub-windows tile the window exactly,
    # in order, with no overlap; every day landed once.
    ok = [(s, u) for _, s, u in client.calls if (u - s).days + 1 <= 2]
    assert ok == [
        (first + timedelta(days=i), first + timedelta(days=i + 1)) for i in range(0, 8, 2)
    ]
    landed = sorted(r["date_start"] for b in batches for r in b)
    assert landed == [(first + timedelta(days=i)).isoformat() for i in range(8)]
    # The cursor passed the window ONCE, after every sub-window was yielded.
    assert cursor.observed == [today]

    # A 1-day window that still fails is a real error.
    with pytest.raises(RuntimeError, match="1-day window"):
        _run_walk(_FakeClient(fail_over=0), _FakeCursor(None), ["1"], start_date=first.isoformat())


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


def _write_config(tmp_path: Path, *, base_url: str, streams: str, start_date: str) -> None:
    (tmp_path / "configs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "configs" / "meta_test.yml").write_text(
        "name: meta_test\n"
        "source: meta\n"
        "destination: duckdb\n"
        "target: dev\n"
        "params:\n"
        f"  base_url: '{base_url}'\n"
        "  account_ids: 'act_111'\n"
        f"  start_date: '{start_date}'\n"
        "  window_days: 7\n"
        "  page_delay_seconds: 0\n"
        "  poll_interval_seconds: 0\n"
        "  job_timeout_seconds: 5\n"
        f"streams:\n{streams}\n"
    )


def _cursor(conn: duckdb.DuckDBPyConnection, stream_name: str) -> date:
    """The persisted cursor for one stream (stored JSON-encoded)."""
    row = conn.execute(
        "SELECT cursor_value FROM _dtex_state WHERE connector = 'meta' AND stream = ?",
        [stream_name],
    ).fetchone()
    assert row is not None and row[0] is not None
    raw = row[0]
    return as_date(json.loads(raw) if isinstance(raw, str) else raw)


def test_end_to_end_ads_insights(
    graph_stub: tuple[_Graph, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ads_insights lands coerced rows + JSON arrays; the cursor reaches today."""
    graph, base_url = graph_stub
    monkeypatch.setenv("META_ACCESS_TOKEN", "EAAe2e")

    today = datetime.now(tz=UTC).date()
    day1 = (today - timedelta(days=1)).isoformat()
    day2 = today.isoformat()
    graph.pages = [
        [_ad_row(day1, ad_id="a1", spend="12.50")],
        [_ad_row(day2, ad_id="a2", spend="3.00"), {"date_start": day2, "account_id": "111"}],
    ]

    _write_project(tmp_path)
    _write_config(tmp_path, base_url=base_url, streams="  ads_insights:", start_date=day1)
    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="meta_test", project_dir=str(tmp_path), destination_params_override={"path": db_path}
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date_start, account_id, ad_id, spend, impressions, actions "
        "FROM ads_insights ORDER BY date_start"
    ).fetchall()
    cursor_after = _cursor(conn, "ads_insights")
    conn.close()

    assert [r[:5] for r in rows] == [
        (date.fromisoformat(day1), "111", "a1", 12.5, 1503),
        (date.fromisoformat(day2), "111", "a2", 3.0, 1503),
    ]
    actions = rows[0][5]
    actions = json.loads(actions) if isinstance(actions, str) else actions
    assert {"action_type": "purchase", "value": "2"} in actions
    assert cursor_after == today
    # One async job for the single (window, account); the token never in a path.
    assert graph.submits == 1
    assert all(r.params["access_token"] == "EAAe2e" for r in graph.captured)


def test_end_to_end_insights_hourly(
    graph_stub: tuple[_Graph, str], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """insights_hourly parses the hour, stamps the account timezone, drops
    the unkeyable row, and keeps its own cursor."""
    graph, base_url = graph_stub
    monkeypatch.setenv("META_ACCESS_TOKEN", "EAAe2e")
    graph.accounts["111"] = {
        "id": "act_111",
        "timezone_name": "America/Los_Angeles",
        "timezone_offset_hours_utc": -7,
    }
    today = datetime.now(tz=UTC).date()
    day = today.isoformat()
    graph.pages = [
        [
            _hourly_row(day, "13:00:00 - 13:59:59"),
            _hourly_row(day, "0:00:00 - 0:59:59", campaign_id="c2"),
            _hourly_row(day, "unknown"),  # unkeyable → dropped
        ]
    ]

    _write_project(tmp_path)
    _write_config(
        tmp_path,
        base_url=base_url,
        streams="  insights_hourly:\n    params:\n      window_days: 30\n",
        start_date=day,
    )
    db_path = str(tmp_path / "warehouse.duckdb")
    result = dtex.run(
        config="meta_test", project_dir=str(tmp_path), destination_params_override={"path": db_path}
    )
    assert result.status.value == "succeeded", result.error

    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT date_start, hour, account_id, campaign_id, hour_range, timezone_name, "
        "timezone_offset_hours_utc, spend FROM insights_hourly ORDER BY hour"
    ).fetchall()
    tables = {
        r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
    }
    cursor_after = _cursor(conn, "insights_hourly")
    conn.close()

    assert rows == [
        (today, 0, "111", "c2", "0:00:00 - 0:59:59", "America/Los_Angeles", -7.0, 2.31),
        (today, 13, "111", "c1", "13:00:00 - 13:59:59", "America/Los_Angeles", -7.0, 2.31),
    ]
    assert "ads_insights" not in tables  # only the selected stream ran
    assert cursor_after == today
    # The account timezone was fetched once, the report submitted at campaign level.
    assert len([r for r in graph.captured if r.path == "/v21.0/act_111"]) == 1
    submit = next(r for r in graph.captured if r.method == "POST")
    assert submit.params["level"] == "campaign"
    assert submit.params["breakdowns"] == HOURLY_BREAKDOWN
    assert json.loads(submit.params["time_range"]) == {"since": day, "until": day}
    assert re.fullmatch(r"/v21\.0/act_111/insights", submit.path)
