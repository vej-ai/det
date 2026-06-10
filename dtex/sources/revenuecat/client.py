"""RevenueCat v2 HTTP client — thin wrapper over `requests`.

Two surfaces:

* `paginate(path, params)` — for list endpoints (/customers, per-customer
  /subscriptions). RC paginates with `starting_after` cursor tokens; the
  response's `next_page` field is the absolute URL of the next page (with
  the cursor + original query params baked in), so subsequent calls pass
  NO params — using the URL as-is is correct per docs.
* `get(path, params)` — for one-shot endpoints (/charts/{chart_name},
  /metrics/*). Returns parsed JSON.

Both surfaces share retry + rate-limit handling. Three failure classes
get bounded, capped retry — the previous version had two real hangs:

* **No socket timeout.** Default `requests.get` waits forever for both
  connect and read. One dead RC connection mid-walk → infinite sleep
  inside `_session.get`. Fixed with `timeout=(connect, read)`.
* **Network errors weren't caught.** `Timeout`, `ConnectionError`,
  `ChunkedEncodingError`, `ProtocolError` all raise BEFORE a `resp`
  object exists, so neither status-code branch saw them — they
  propagated up and failed the stream on the first blip.
* **429 retry was uncapped + did not increment `_attempt`.** A
  sustained rate-limit (very real: ~480 req/min on the customer domain
  vs. tens of thousands of customer pages per run) wedged the run in a
  permanent sleep-retry loop. Now bounded by `max_retries`, same as
  the other retry paths.

The Bearer token is set on the session header once; it never appears
in log output or error messages.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Iterator

import requests

# (connect_timeout, read_timeout) seconds. The connect leg should be fast
# — RC's edge is near every common region. The read leg is the dangerous
# one: a single hung response will block the whole run. 60s is generous
# enough for slow chart computations without trapping a dead socket.
_TIMEOUT: tuple[float, float] = (10.0, 60.0)


@dataclass
class RevenueCatClient:
    api_key: str
    project_id: str
    base_url: str = "https://api.revenuecat.com/v2"
    max_retries: int = 5
    _session: requests.Session = field(
        default_factory=requests.Session, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
            }
        )

    def paginate(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict]:
        """Yield every item from an RC v2 list endpoint.

        Params apply ONLY to the first request — RC's `next_page` is an
        absolute URL with the original query string + the `starting_after`
        cursor token baked in, so subsequent fetches use it verbatim.
        """
        next_url: str | None = f"{self.base_url}{path}"
        first = True
        while next_url:
            data = self._get(next_url, params=params if first else None)
            first = False
            for item in data.get("items", []):
                yield item
            next_url = data.get("next_page")

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict:
        """One-shot GET on a non-list endpoint. Returns parsed JSON."""
        url = f"{self.base_url}{path}"
        return self._get(url, params=params)

    def _get(
        self,
        url: str,
        params: dict | None = None,
        _attempt: int = 0,
    ) -> dict:
        # Network-level failures (timeout, connection reset, broken
        # chunked encoding) raise BEFORE a `resp` object exists, so we
        # have to catch them around the .get call and route through the
        # same capped-backoff path as a 5xx.
        try:
            resp = self._session.get(url, params=params, timeout=_TIMEOUT)
        except requests.exceptions.RequestException as exc:
            if _attempt < self.max_retries:
                time.sleep(min(2**_attempt, 60))
                return self._get(url, params, _attempt + 1)
            raise RuntimeError(
                f"revenuecat: network failure after {self.max_retries} retries on {url}: {exc}"
            ) from exc

        if resp.status_code == 429:
            # RC enforces a per-domain RPM cap. `Retry-After` is the
            # server's directive — honor it. But ALSO bound the loop:
            # the previous version did not increment `_attempt` and a
            # sustained rate-limit wedged the run forever.
            if _attempt >= self.max_retries:
                raise RuntimeError(
                    f"revenuecat: rate-limited after {self.max_retries} retries on {url}; "
                    f"Retry-After={resp.headers.get('Retry-After')}"
                )
            wait = int(resp.headers.get("Retry-After", 60))
            time.sleep(wait)
            return self._get(url, params, _attempt + 1)
        if resp.status_code in (500, 502, 503, 504) and _attempt < self.max_retries:
            time.sleep(min(2**_attempt, 60))
            return self._get(url, params, _attempt + 1)
        resp.raise_for_status()
        return resp.json()
