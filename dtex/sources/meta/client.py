"""Meta Marketing API HTTP client — access token, paced requests, cursor paging.

Transport rules, the same discipline as the other REST connectors here:

* **PACING** — a mandatory ``page_delay_seconds`` sleep before every
  request. Meta's Insights budget is per ad account and generous, but a
  backfill across several accounts is thousands of requests; a fixed pace
  keeps the usage headers flat instead of bursting into a 613.
* **HARD PER-REQUEST TIMEOUT** — ``(10, 120)`` seconds connect/read. A
  hung socket raises, never wedges the run.
* **BOUNDED RETRIES, LOUD FAILURE** — Meta reports rate limiting as HTTP
  400 with a JSON error whose ``code`` is 4 / 17 / 32 / 613 (or 80000-
  80014 for the business-use-case limiter), occasionally as a plain 429.
  All of those back off exponentially with jitter, bounded by
  ``max_retries``; 5xx and network errors likewise; a transient code 1 / 2
  ("unknown error" / "service temporarily unavailable") is retried too.
  Anything else — an invalid field (100), a bad or revoked token (190),
  a missing permission (200/10) — is deterministic and raises at once
  with Meta's own message.
* **USAGE-HEADER AWARENESS** — ``x-business-use-case-usage`` /
  ``x-ad-account-usage`` carry the % of the per-account budget consumed.
  Above ``_USAGE_SOFT_PCT`` the client sleeps proactively so a long walk
  slows down instead of tripping the hard limit.

Auth: the access token (a never-expiring Business Manager SYSTEM USER
token is the recommended kind) rides as the ``access_token`` query param.
It is stripped from every URL that appears in a log or an exception
(``repr=False``, and ``_redact`` on messages).

**ASYNC JOBS, NOT SYNC GETS.** A synchronous ``GET /act_<id>/insights``
at ad level over a week of a large account (~10k ads) fails with Meta's
generic ``code 1 / subcode 99`` ("unknown error" — in practice "too much
data for a sync call"). The documented remedy is the asynchronous
report: ``POST /act_<id>/insights`` returns a ``report_run_id``;
``GET /<report_run_id>`` is polled until ``async_status`` is
``Job Completed``; ``GET /<report_run_id>/insights`` then pages the
result with ``after`` cursors. :meth:`MetaClient.iter_insights` does all
three; a ``Job Failed`` / ``Job Skipped`` job is resubmitted (bounded by
``max_retries``) and then raises :class:`AsyncJobFailed` so the caller
can bisect the window; a job that never completes within
``job_timeout_seconds`` raises.
"""

from __future__ import annotations

import json
import random
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import requests

# (connect_timeout, read_timeout) seconds — a heavy 500-row Insights page
# can take tens of seconds server-side; 120s is generous but finite.
_TIMEOUT: tuple[float, float] = (10.0, 120.0)

# Upper bound on any single retry sleep.
_BACKOFF_CAP: float = 300.0

# Meta error codes that mean "slow down" rather than "you are wrong".
_RATE_LIMIT_CODES: frozenset[int] = frozenset({4, 17, 32, 613})
_BUC_RATE_LIMIT_RANGE: tuple[int, int] = (80000, 80014)
# Transient server-side codes worth a bounded retry.
_TRANSIENT_CODES: frozenset[int] = frozenset({1, 2})

# Proactive throttle threshold on the usage headers (percent).
_USAGE_SOFT_PCT: float = 80.0
# How long to pause once the soft threshold is crossed — the budget is a
# rolling window, so a minute is what lets it drain.
_USAGE_PAUSE_SECONDS: float = 60.0

_TOKEN_RE = re.compile(r"access_token=[^&\s]+")


def _redact(text: str) -> str:
    """Strip the access token from anything that might be logged or raised."""
    return _TOKEN_RE.sub("access_token=<redacted>", text)


def _sleep_backoff(attempt: int, floor: float = 0.0) -> None:
    """Exponential backoff with full jitter, bounded by ``_BACKOFF_CAP``."""
    base = max(min(2.0**attempt, _BACKOFF_CAP), floor, 1.0)
    time.sleep(base * (0.5 + random.random() / 2.0))


def _max_usage_pct(headers: Any) -> float:
    """Highest percentage across Meta's usage headers, 0.0 if absent/unparsable.

    ``x-business-use-case-usage`` is ``{"<business_id>": [{"type": ...,
    "call_count": 12, "total_cputime": 3, "total_time": 4, ...}]}``;
    ``x-ad-account-usage`` is ``{"acc_id_util_pct": 9.5}``. Both are
    percentages of the rolling budget.
    """
    worst = 0.0
    raw = headers.get("x-business-use-case-usage")
    if raw:
        try:
            for entries in json.loads(raw).values():
                for entry in entries or []:
                    for key in ("call_count", "total_cputime", "total_time"):
                        worst = max(worst, float(entry.get(key) or 0))
        except (ValueError, AttributeError, TypeError):
            pass
    raw = headers.get("x-ad-account-usage")
    if raw:
        try:
            worst = max(worst, float(json.loads(raw).get("acc_id_util_pct") or 0))
        except (ValueError, AttributeError, TypeError):
            pass
    return worst


def _error_code(body: dict[str, Any]) -> int | None:
    err = body.get("error") if isinstance(body, dict) else None
    if not isinstance(err, dict):
        return None
    raw = err.get("code")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _is_rate_limit(code: int | None) -> bool:
    if code is None:
        return False
    lo, hi = _BUC_RATE_LIMIT_RANGE
    return code in _RATE_LIMIT_CODES or lo <= code <= hi


class AsyncJobFailed(RuntimeError):
    """The async Insights report ended ``Job Failed`` / ``Job Skipped`` on every
    resubmit. Meta sheds load this way for reports it deems too large or when
    too many jobs are queued for the account — deterministic enough for a
    given window that the caller's remedy is a NARROWER window (bisect), not
    another retry. Raised before any result page is fetched, so the caller
    has yielded nothing for the window yet."""


@dataclass
class MetaClient:
    """The paced, token-bearing HTTP surface. One instance per run."""

    access_token: str = field(repr=False)
    base_url: str = "https://graph.facebook.com"
    api_version: str = "v21.0"
    page_size: int = 500
    max_retries: int = 5
    page_delay_seconds: float = 0.5
    poll_interval_seconds: float = 5.0
    job_timeout_seconds: float = 1800.0
    _session: requests.Session = field(default_factory=requests.Session, init=False, repr=False)
    last_usage_pct: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._session.headers.update({"Accept": "application/json"})

    # -- public surface ------------------------------------------------------

    def get_account(self, account_id: str, fields: str) -> dict[str, Any]:
        """``GET /<ver>/act_<id>?fields=...`` — one ad-account object.

        Used once per run per account by the hourly stream to learn the
        advertiser timezone (``timezone_name``, ``timezone_offset_hours_utc``)
        that the ``hourly_stats_aggregated_by_advertiser_time_zone``
        breakdown is expressed in.
        """
        return self._get_json(f"/{self.api_version}/act_{account_id}", {"fields": fields})

    def iter_insights(
        self,
        account_id: str,
        since: date,
        until: date,
        fields: str,
        action_attribution_windows: str = "",
        level: str = "ad",
        breakdowns: str = "",
    ) -> Iterator[tuple[int, list[dict[str, Any]]]]:
        """Yield ``(page_number, rows)`` for one (account, date window).

        Async report flow: submit ``POST /<ver>/act_<id>/insights``
        (``level`` — ``ad`` by default, ``campaign`` for the hourly stream —
        ``time_increment=1``, inclusive ``time_range``, optional
        ``breakdowns``), poll the returned ``report_run_id`` until
        ``Job Completed``, then page ``GET /<ver>/<report_run_id>/insights``
        via ``after`` cursors until ``paging.next`` disappears.
        """
        submit_path = f"/{self.api_version}/act_{account_id}/insights"
        submit_params: dict[str, Any] = {
            "level": level,
            "time_increment": 1,
            "time_range": json.dumps({"since": since.isoformat(), "until": until.isoformat()}),
            "fields": fields,
        }
        if breakdowns:
            submit_params["breakdowns"] = breakdowns
        if action_attribution_windows:
            submit_params["action_attribution_windows"] = json.dumps(
                [w.strip() for w in action_attribution_windows.split(",") if w.strip()]
            )

        report_run_id = self._run_report(submit_path, submit_params, account_id, since, until)

        page = 1
        page_params: dict[str, Any] = {"limit": int(self.page_size)}
        results_path = f"/{self.api_version}/{report_run_id}/insights"
        while True:
            body = self._get_json(results_path, page_params)
            rows = body.get("data") or []
            yield page, rows
            paging = body.get("paging") or {}
            after = (paging.get("cursors") or {}).get("after")
            if not paging.get("next") or not after:
                return
            page_params["after"] = after
            page += 1

    def _run_report(
        self,
        submit_path: str,
        submit_params: dict[str, Any],
        account_id: str,
        since: date,
        until: date,
    ) -> str:
        """Submit the async report and poll it to completion; returns its id.

        A job that ends ``Job Failed`` / ``Job Skipped`` is resubmitted up
        to ``max_retries`` times (Meta sheds load this way under pressure);
        one that is still running after ``job_timeout_seconds`` raises —
        the run must fail red rather than sit on a stuck job.
        """
        label = f"act_{account_id} {since}..{until} level={submit_params.get('level')}"
        for attempt in range(self.max_retries + 1):
            submitted = self._request_json("POST", submit_path, submit_params)
            report_run_id = str(submitted.get("report_run_id") or "")
            if not report_run_id:
                raise RuntimeError(
                    f"meta: async insights submit for {label} returned no "
                    f"report_run_id: {_redact(json.dumps(submitted)[:300])}"
                )
            status_path = f"/{self.api_version}/{report_run_id}"
            deadline = time.monotonic() + self.job_timeout_seconds
            while True:
                status_body = self._get_json(
                    status_path, {"fields": "async_status,async_percent_completion"}
                )
                status = str(status_body.get("async_status") or "")
                if status == "Job Completed":
                    return report_run_id
                if status in ("Job Failed", "Job Skipped"):
                    break  # resubmit
                if time.monotonic() >= deadline:
                    raise RuntimeError(
                        f"meta: async insights job {report_run_id} for {label} "
                        f"still '{status}' after {self.job_timeout_seconds:.0f}s "
                        f"({status_body.get('async_percent_completion')}% done) "
                        "— raise job_timeout_seconds or narrow window_days"
                    )
                time.sleep(self.poll_interval_seconds)
            if attempt < self.max_retries:
                _sleep_backoff(attempt + 2)
        raise AsyncJobFailed(
            f"meta: async insights job for {label} ended 'Job Failed'/'Job "
            f"Skipped' {self.max_retries + 1} time(s)"
        )

    # -- internals -----------------------------------------------------------

    def _get_json(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._request_json("GET", path, params)

    def _request_json(self, method: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
        """One paced request with bounded retries; the loud-failure core.

        GETs carry params in the query string; POSTs (async report submit)
        carry them as form data. The token rides with the params either way.
        """
        url = f"{self.base_url}{path}"
        attempt = 0
        while True:
            time.sleep(self.page_delay_seconds)
            try:
                payload = {**params, "access_token": self.access_token}
                if method == "POST":
                    resp = self._session.post(url, data=payload, timeout=_TIMEOUT)
                else:
                    resp = self._session.get(url, params=payload, timeout=_TIMEOUT)
            except requests.exceptions.RequestException as exc:
                if attempt < self.max_retries:
                    _sleep_backoff(attempt)
                    attempt += 1
                    continue
                raise RuntimeError(
                    f"meta: network failure after {self.max_retries} retries "
                    f"on {url}: {_redact(str(exc))}"
                ) from exc

            usage = _max_usage_pct(resp.headers)
            self.last_usage_pct = usage
            if usage >= _USAGE_SOFT_PCT:
                time.sleep(_USAGE_PAUSE_SECONDS)

            try:
                body = resp.json() if resp.content else {}
            except ValueError:
                body = {}
            code = _error_code(body)

            if resp.status_code == 429 or _is_rate_limit(code):
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"meta: rate-limited after {self.max_retries} retries "
                        f"on {_redact(url)} (HTTP {resp.status_code}, error "
                        f"code {code}); usage {usage:.0f}%. Raise "
                        "page_delay_seconds before rerunning."
                    )
                _sleep_backoff(
                    attempt + 2,  # start at ~4s: Meta budgets refill slowly
                    floor=float(resp.headers.get("Retry-After", 0) or 0),
                )
                attempt += 1
                continue
            if resp.status_code >= 500 or code in _TRANSIENT_CODES:
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"meta: HTTP {resp.status_code} / error code {code} "
                        f"after {self.max_retries} retries on {_redact(url)}: "
                        f"{_redact(resp.text[:300])}"
                    )
                _sleep_backoff(attempt)
                attempt += 1
                continue
            if resp.status_code >= 400 or code is not None:
                # 190 = invalid/expired token, 200/10 = missing permission,
                # 100 = invalid field/param — deterministic; retrying
                # cannot fix them, and the message must not echo the token.
                msg = (body.get("error") or {}).get("message", resp.text[:300])
                hint = ""
                if code == 190:
                    hint = (
                        " — the access_token secret is invalid, expired or "
                        "revoked; regenerate it (a Business Manager system-user "
                        "token never expires)"
                    )
                elif code in (200, 10):
                    hint = (
                        " — the token's user lacks access to this ad account "
                        "(assign it with View performance in Business Manager)"
                    )
                raise RuntimeError(
                    f"meta: HTTP {resp.status_code} error code {code} on "
                    f"{_redact(url)}: {_redact(str(msg))}{hint}"
                )
            return body
