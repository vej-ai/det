"""Singular Reporting API client — the async create → poll → download flow.

Singular's Reporting API has no synchronous query surface: you POST
``/v2.0/create_async_report`` with the report definition, poll
``/v2.0/get_report_status`` until the report reaches a terminal status,
then GET the (signed, short-lived) ``download_url`` for the result rows.

Transport rules learned from the other REST connectors in this repo:

* Every request carries ``timeout=(connect, read)`` — a hung socket must
  never wedge the run.
* 429 honors ``Retry-After`` but is BOUNDED by ``max_retries`` and
  increments the attempt counter (an uncapped 429 loop wedged an earlier
  RevenueCat connector forever).
* 5xx and network-level failures (timeout, reset, chunked-encoding)
  retry with capped exponential backoff through the same path.
* The API key rides in the ``Authorization`` header on API calls, but the
  ``download_url`` is a pre-signed URL — it is fetched WITHOUT the auth
  header (some object stores reject requests that present both a query
  signature and an Authorization header). The key never appears in log
  output or error messages.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import requests

# (connect_timeout, read_timeout) seconds. The read leg is the dangerous
# one; report downloads can be several MB, so give it headroom.
_TIMEOUT: tuple[float, float] = (10.0, 120.0)

_TERMINAL_FAILED = ("FAILED", "CANCELLED", "CANCELED")


@dataclass
class SingularClient:
    api_key: str = field(repr=False)
    base_url: str = "https://api.singular.net/api"
    poll_timeout_sec: int = 600
    poll_interval_sec: float = 5.0
    max_retries: int = 5
    _session: requests.Session = field(
        default_factory=requests.Session, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._session.headers.update(
            {
                "Authorization": self.api_key,
                "Accept": "application/json",
            }
        )

    # -- public surface ----------------------------------------------------

    def run_report(self, query: dict[str, Any], *, log: Any | None = None) -> list[dict]:
        """Submit one report, wait for it, return its parsed result rows."""
        report_id = self._create(query, log=log)
        download_url = self._poll_until_done(report_id, log=log)
        return self._download(download_url, log=log)

    # -- the three legs ----------------------------------------------------

    def _create(self, query: dict[str, Any], *, log: Any | None = None) -> str:
        body = self._request(
            "POST", f"{self.base_url}/v2.0/create_async_report", data=query
        )
        report_id = (body.get("value") or {}).get("report_id")
        if not report_id:
            raise RuntimeError(
                f"singular: create_async_report returned no report_id: {body!r}"
            )
        if log:
            log.info(
                "singular: report submitted id=%s window=%s→%s",
                report_id,
                query.get("start_date"),
                query.get("end_date"),
            )
        return str(report_id)

    def _poll_until_done(self, report_id: str, *, log: Any | None = None) -> str:
        """Poll get_report_status until DONE; return the download_url.

        Flat sleep of ``poll_interval_sec`` between polls, capped by
        ``poll_timeout_sec`` of total wall-clock wait.
        """
        deadline = time.monotonic() + float(self.poll_timeout_sec)
        status = "UNKNOWN"
        while True:
            body = self._request(
                "GET",
                f"{self.base_url}/v2.0/get_report_status",
                params={"report_id": report_id},
            )
            value = body.get("value") or {}
            status = str(value.get("status") or "UNKNOWN").upper()
            if status == "DONE":
                url = value.get("download_url")
                if not url:
                    raise RuntimeError(
                        f"singular: report {report_id} DONE but no download_url: {body!r}"
                    )
                return str(url)
            if status in _TERMINAL_FAILED:
                raise RuntimeError(
                    f"singular: report {report_id} ended with status={status!r}: "
                    f"{value.get('error_message')!r}"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"singular: report {report_id} still {status!r} after "
                    f"{self.poll_timeout_sec}s — giving up"
                )
            time.sleep(float(self.poll_interval_sec))

    def _download(self, url: str, *, log: Any | None = None) -> list[dict]:
        """GET the signed result URL (NO auth header) and unwrap the rows."""
        body = self._request("GET", url, signed=True)
        rows = body.get("results")
        if rows is None:
            # Some report families nest one level deeper.
            rows = (body.get("value") or {}).get("results")
        if rows is None:
            raise RuntimeError(
                f"singular: report download had no 'results' key: {list(body)!r}"
            )
        if log:
            log.info("singular: report downloaded — %d rows", len(rows))
        return list(rows)

    # -- transport ---------------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        signed: bool = False,
        _attempt: int = 0,
    ) -> dict:
        # Signed download URLs must NOT carry the Authorization header —
        # use a bare request instead of the authed session.
        try:
            if signed:
                resp = requests.get(url, timeout=_TIMEOUT)
            else:
                resp = self._session.request(
                    method, url, params=params, data=data, timeout=_TIMEOUT
                )
        except requests.exceptions.RequestException as exc:
            if _attempt < self.max_retries:
                time.sleep(min(2**_attempt, 60))
                return self._request(
                    method, url, params=params, data=data,
                    signed=signed, _attempt=_attempt + 1,
                )
            raise RuntimeError(
                f"singular: network failure after {self.max_retries} retries "
                f"on {method} {url.split('?')[0]}: {exc}"
            ) from exc

        if resp.status_code == 429:
            if _attempt >= self.max_retries:
                raise RuntimeError(
                    f"singular: rate-limited after {self.max_retries} retries on "
                    f"{method} {url.split('?')[0]}; "
                    f"Retry-After={resp.headers.get('Retry-After')}"
                )
            wait = int(resp.headers.get("Retry-After", 30))
            time.sleep(wait)
            return self._request(
                method, url, params=params, data=data,
                signed=signed, _attempt=_attempt + 1,
            )
        if resp.status_code in (500, 502, 503, 504) and _attempt < self.max_retries:
            time.sleep(min(2**_attempt, 60))
            return self._request(
                method, url, params=params, data=data,
                signed=signed, _attempt=_attempt + 1,
            )
        resp.raise_for_status()
        return resp.json()
