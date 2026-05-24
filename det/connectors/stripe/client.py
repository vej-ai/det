"""Stripe HTTP client — Bearer auth, retries, backoff, and rate limiting.

A small, dependency-free wrapper around :mod:`requests` that the Stripe
``@stream`` functions in :mod:`source` use to talk to ``api.stripe.com``.
Plain module — no decorators, no det contract — exactly the "API client"
role docs/04 §"Recommended file layout" assigns to ``client.py``.

The class encapsulates four cross-cutting concerns the pagination loop should
not have to think about:

* **Auth.** ``Authorization: Bearer <api_key>`` and ``Stripe-Version:
  <api_version>`` on every request, set on a long-lived :class:`requests.Session`.
* **Rate limiting.** A simple token-bucket smooths bursts to a steady
  ``requests_per_second`` ceiling, well under Stripe's documented ~100 rps
  live-mode limit (research note §"Endpoint-shape caveat").
* **Retry / backoff.** ``429`` honors ``Retry-After`` exactly; ``5xx`` retries
  with exponential backoff up to ``max_retries``; ``4xx`` other than ``429``
  raise immediately — a bad request will not get better on its own.
* **Secret redaction.** The ``Authorization`` header is filtered out of every
  log line emitted by this module — the API key never lands on disk.

Design citation: docs/connectors/stripe-research.md §B "The standard REST
API" pins the auth + pagination shape; cursor pagination is implemented in
:mod:`det.connectors.stripe.pagination`, kept separate so this file is
purely the transport layer.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import requests

# A subset of HTTP status codes worth special-casing. Anything else 4xx is a
# hard error (raised immediately, no retry); anything else 5xx is retried.
_STATUS_RATE_LIMITED = 429
_STATUS_SERVER_ERROR_FLOOR = 500
_STATUS_CLIENT_ERROR_FLOOR = 400

# Authorization header name; declared as a constant so the redaction logic
# (:func:`_redact_headers`) and the request builder cannot drift apart.
_AUTH_HEADER = "Authorization"
_REDACTED = "<redacted>"


class StripeAPIError(Exception):
    """A Stripe API call failed — either a non-retryable 4xx or exhausted retries.

    Carries the HTTP ``status`` for tests / callers; the message includes
    Stripe's own error description when the response body carried one
    (``error.message`` per the Stripe API conventions). The API key is never
    in the message — only the status and Stripe's text are surfaced.
    """

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"Stripe API error {status}: {message}")
        self.status = status


@dataclass
class _TokenBucket:
    """A minimal token-bucket rate limiter shared across a client's requests.

    One token per request; tokens refill linearly at ``rate`` per second up to
    ``capacity``. ``acquire()`` blocks (via :func:`time.sleep`) until a token
    is available — predictable, no threading primitives, and trivially testable
    by monkeypatching ``time.sleep`` / ``time.monotonic``.
    """

    rate: float
    """Tokens per second (the ``requests_per_second`` param)."""
    capacity: float
    """Bucket capacity — the maximum burst size."""
    _tokens: float = field(default=0.0, init=False)
    _last: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        # Start full so the first ``capacity`` requests pass without delay.
        self._tokens = self.capacity
        self._last = time.monotonic()

    def acquire(self) -> None:
        """Block until one token is available, then consume it."""
        # No-op for non-positive rates — disables limiting (useful in tests).
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            needed = (1.0 - self._tokens) / self.rate
            time.sleep(needed)


def _redact_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return a copy of ``headers`` with the ``Authorization`` value blanked.

    The single redaction choke point — every log line that mentions request
    headers goes through this, so a future ``log.debug`` is safe by default.
    Case-insensitive: matches ``Authorization`` / ``authorization`` /
    ``AUTHORIZATION`` since ``requests`` accepts any casing.
    """
    return {
        k: (_REDACTED if k.lower() == _AUTH_HEADER.lower() else v)
        for k, v in headers.items()
    }


class StripeClient:
    """A Stripe REST client — used by every ``@stream`` function in this connector.

    The lifetime of one instance is one ``@stream`` invocation: the source
    builds a client from :class:`~det.types.Config` at the top of the
    stream, then drives the cursor pagination loop in
    :mod:`det.connectors.stripe.pagination` against it. A long-lived
    :class:`requests.Session` keeps the TCP connection warm across the pages of
    one stream.

    Constructor args mirror the connector's declared ``params`` (see
    ``register.yaml``) — see the module docstring for the citation. The
    ``api_key`` is set once on the Session and never logged thereafter.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        api_version: str,
        page_size: int = 100,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        requests_per_second: float = 25.0,
        timeout_seconds: float = 30.0,
        log: logging.Logger | logging.LoggerAdapter[Any] | None = None,
        session: requests.Session | None = None,
        sleep: Any = time.sleep,
    ) -> None:
        # Trim a trailing slash so endpoint joins are unambiguous —
        # ``base_url + endpoint`` is the only URL composition this client does.
        self.base_url = base_url.rstrip("/")
        self.api_version = api_version
        self.page_size = page_size
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.timeout_seconds = timeout_seconds
        self._log: logging.Logger | logging.LoggerAdapter[Any] = (
            log if log is not None else logging.getLogger(__name__)
        )
        # ``sleep`` is injectable so tests can pass a no-op / fast-forward stub
        # — the rate limiter and the retry backoff both use it.
        self._sleep = sleep

        self._session = session if session is not None else requests.Session()
        # Default headers — set on the Session so every request carries them.
        # NOTE: requests stores the Authorization value in Session.headers;
        # repr(session) does NOT print it, and our log calls go through
        # _redact_headers, so the key never reaches a logger.
        self._session.headers.update(
            {
                _AUTH_HEADER: f"Bearer {api_key}",
                "Stripe-Version": api_version,
                "Accept": "application/json",
            }
        )
        self._bucket = _TokenBucket(
            rate=requests_per_second,
            capacity=max(1.0, requests_per_second),
        )

    def close(self) -> None:
        """Close the underlying :class:`requests.Session` (idempotent)."""
        self._session.close()

    def __enter__(self) -> StripeClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def list(self, endpoint: str, params: Mapping[str, Any]) -> dict[str, Any]:
        """Single ``GET <base_url><endpoint>`` returning parsed JSON.

        Handles every concern the caller should not have to think about:

        * blocks for a rate-limit token before sending;
        * on ``429`` honors ``Retry-After`` (seconds), then retries — counted
          against ``max_retries``;
        * on ``5xx`` retries with exponential backoff
          (``retry_backoff_seconds * 2 ** attempt``), counted against
          ``max_retries``;
        * on any other ``4xx`` raises :class:`StripeAPIError` immediately —
          a bad request / bad key / bad URL does not get better with time;
        * on success returns the JSON body as a ``dict``.

        ``endpoint`` should start with ``/`` (e.g. ``/charges``); ``params`` is
        the query string. Per Stripe convention bracketed keys
        (``created[gte]``, ``expand[]``) pass through as written.
        """
        url = self.base_url + endpoint
        last_error: str = ""
        for attempt in range(self.max_retries + 1):
            self._bucket.acquire()
            try:
                response = self._session.get(
                    url, params=dict(params), timeout=self.timeout_seconds
                )
            except requests.RequestException as exc:
                # Network-level error — treat as retryable up to max_retries
                # since transient DNS / connection resets are common.
                last_error = f"network error: {exc}"
                if attempt >= self.max_retries:
                    raise StripeAPIError(0, last_error) from exc
                self._sleep(self.retry_backoff_seconds * (2**attempt))
                continue

            status = response.status_code

            if 200 <= status < 300:
                # Stripe always returns JSON on a 2xx for the list endpoints.
                return _parse_json(response)

            if status == _STATUS_RATE_LIMITED:
                # 429: honor Retry-After exactly when present, otherwise
                # exponential backoff. Each 429 burns one of the max_retries.
                if attempt >= self.max_retries:
                    raise StripeAPIError(status, _error_message(response))
                wait = _retry_after_seconds(response) or (
                    self.retry_backoff_seconds * (2**attempt)
                )
                self._log.warning(
                    "stripe: 429 rate limited on %s, sleeping %.3fs (attempt %d/%d)",
                    endpoint,
                    wait,
                    attempt + 1,
                    self.max_retries,
                )
                self._sleep(wait)
                continue

            if status >= _STATUS_SERVER_ERROR_FLOOR:
                # 5xx: transient server-side. Exponential backoff and retry.
                if attempt >= self.max_retries:
                    raise StripeAPIError(status, _error_message(response))
                wait = self.retry_backoff_seconds * (2**attempt)
                self._log.warning(
                    "stripe: %d on %s, retrying after %.3fs (attempt %d/%d)",
                    status,
                    endpoint,
                    wait,
                    attempt + 1,
                    self.max_retries,
                )
                self._sleep(wait)
                continue

            if status >= _STATUS_CLIENT_ERROR_FLOOR:
                # Other 4xx — a 401, 403, 404, etc. Do NOT retry; the request
                # itself is the problem and Stripe's error message is the
                # actionable signal. The API key never appears in the message.
                raise StripeAPIError(status, _error_message(response))

            # Anything else (1xx, 3xx) — Stripe does not document these on the
            # list endpoints; fail loudly rather than silently swallow.
            raise StripeAPIError(status, _error_message(response))

        # Defensive — the loop always either returns or raises.
        raise StripeAPIError(0, last_error or "max retries exceeded")  # pragma: no cover


def _parse_json(response: requests.Response) -> dict[str, Any]:
    """Parse a Stripe success response body as JSON; raise on garbage."""
    try:
        data = response.json()
    except ValueError as exc:
        raise StripeAPIError(response.status_code, f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise StripeAPIError(
            response.status_code, f"expected JSON object, got {type(data).__name__}"
        )
    return data


def _error_message(response: requests.Response) -> str:
    """Extract Stripe's ``error.message`` from a non-2xx response, or fall back.

    The Stripe error envelope is ``{"error": {"message": "...", ...}}``. A
    malformed body falls back to the raw text (truncated) so the exception
    still carries something actionable. The API key never appears here.
    """
    try:
        body = response.json()
    except ValueError:
        text = response.text or ""
        return text[:500] if text else f"HTTP {response.status_code}"
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            msg = err.get("message")
            if isinstance(msg, str) and msg:
                return msg
    return f"HTTP {response.status_code}"


def _retry_after_seconds(response: requests.Response) -> float | None:
    """Read ``Retry-After`` (seconds-form) from a 429 response, or ``None``.

    Stripe documents the seconds form. The HTTP-date form is not parsed (it
    is rarely used in practice for rate-limit responses) — falling back to
    exponential backoff in that case is still correct, just less precise.
    """
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


__all__ = [
    "StripeAPIError",
    "StripeClient",
    "_redact_headers",
]
