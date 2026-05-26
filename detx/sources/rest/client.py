"""HTTP client for the Generic REST source connector.

Wraps :mod:`requests` with the four concerns a connector author should not
re-implement per API:

* **Auth** — bearer / basic / api-key-header / api-key-query / none.
  Applied once at session construction; per-request auth would re-allocate the
  auth header on every page and add nothing.
* **Retry on transient errors** — 429 + 5xx via :class:`urllib3.util.Retry`,
  with ``Retry-After`` honored automatically. The retry happens inside
  ``urllib3``, below the requests layer, so a request appears to "succeed"
  with whatever the final retry yielded.
* **Rate limit** — a simple token bucket capped at ``requests_per_second``.
  Skipped when ``requests_per_second == 0`` (the default, "unlimited").
* **Secret redaction** — the ``Authorization`` header is never logged. The
  engine's :class:`~detx.engine.logger.RedactingFilter` masks resolved
  secret *values* anywhere they appear in a log message; this client adds
  defence-in-depth by never logging the header in the first place.

The session is reused across every request for one stream run, so the TCP
connection / TLS handshake / auth header are paid once per stream.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__all__ = [
    "AuthSpec",
    "RestClient",
    "build_client",
]

# Default request timeout — long enough for a slow API call, short enough that
# a permanently-stuck request fails the run instead of hanging it.
_DEFAULT_TIMEOUT_S = 30.0


# --------------------------------------------------------------------------
# Auth — one tiny dataclass describes every supported scheme
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class AuthSpec:
    """How the client authenticates to the API — built from connector config.

    Five auth modes cover the bulk of REST APIs in the wild:

    * ``none`` — no auth. For public/anonymous endpoints.
    * ``bearer`` — ``Authorization: Bearer <token>``.
    * ``basic`` — HTTP Basic with ``token`` as ``user`` and ``""`` as password
      (the GitHub / Stripe convention) unless an explicit colon is present
      (``"user:pass"``).
    * ``api_key_header`` — ``<header_name>: <token>``, where ``header_name``
      defaults to ``Authorization`` but a stream can override (e.g. ``X-API-Key``).
    * ``api_key_query`` — token sent as a query parameter (``?api_key=<token>``).
      Mostly for legacy APIs; less secure than header auth (URLs are logged by
      proxies) but supported because some vendors offer no other option.

    # NOTE: the contract says the connector reads its credential from
    # ``config.secrets["api_token"]`` (per the secrets block in
    # ``register.yaml``). The auth spec carries the resolved value, never the
    # ref — by the time AuthSpec exists, the engine has already resolved it.
    """

    auth_type: str
    token: str = ""
    header_name: str = "Authorization"
    query_param: str = "api_key"


# --------------------------------------------------------------------------
# Rate-limit — minimum interval between requests, enforced before send
# --------------------------------------------------------------------------


@dataclass
class _RateLimiter:
    """Block until at least ``min_interval`` has elapsed since the previous send.

    A simple time-since-last-send gate, not a token bucket — for a single
    stream's sequential pagination loop the two are equivalent and this one is
    one line of state. ``requests_per_second == 0`` produces ``min_interval == 0``
    which is a no-op.
    """

    min_interval: float
    _last_call: float = 0.0

    def wait(self) -> None:
        """Sleep until at least :attr:`min_interval` has passed since the last call."""
        if self.min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_call = time.monotonic()


# --------------------------------------------------------------------------
# RestClient — the per-stream HTTP client
# --------------------------------------------------------------------------


@dataclass
class RestClient:
    """Configured HTTP client used by the connector body for one stream.

    Constructed via :func:`build_client` (which wires up the session, auth,
    retry adapter, and rate limiter). The connector body calls :meth:`get` for
    every page — every cross-cutting concern (auth, retry, rate-limit,
    logging-without-leaks) is already applied.
    """

    session: requests.Session
    base_url: str
    auth: AuthSpec
    rate_limiter: _RateLimiter
    timeout: float = _DEFAULT_TIMEOUT_S
    log: logging.Logger | None = None
    _extra_headers: dict[str, str] = field(default_factory=dict)

    def get(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> requests.Response:
        """GET ``base_url`` + ``path`` with the configured auth + ``params``.

        For ``auth_type == "api_key_query"``, the API key is appended to
        ``params`` here (per-request) so it always travels in the query string
        — the session cannot pre-stuff a query param. Every other auth mode
        sets headers on the session at construction time.

        Logs the request (URL + non-secret param names only — never the values;
        a value could be a token under ``api_key_query``) at INFO. The response
        status alone is logged at INFO; the body is never logged.

        ``raise_for_status`` is called on the response — a 4xx not handled by
        :class:`urllib3.util.Retry` (a real 400 from a bad query, say) becomes
        a :class:`requests.HTTPError` that the engine surfaces as a failed run.
        """
        url = self._build_url(path)
        merged: dict[str, Any] = dict(params or {})

        if self.auth.auth_type == "api_key_query":
            merged[self.auth.query_param] = self.auth.token

        self.rate_limiter.wait()
        # NEVER log header values (Authorization in particular) or query *values*
        # — values can carry tokens under api_key_query. Log only the keys.
        if self.log is not None:
            self.log.info(
                "GET %s params=%s", url, sorted(merged) if merged else "[]"
            )

        response = self.session.get(
            url,
            params=merged,
            timeout=self.timeout,
            headers=self._extra_headers or None,
        )
        if self.log is not None:
            self.log.info("HTTP %d for %s", response.status_code, url)
        response.raise_for_status()
        return response

    def _build_url(self, path: str) -> str:
        """Join ``base_url`` and ``path`` with exactly one slash between them."""
        if path.startswith(("http://", "https://")):
            # An absolute URL is honored verbatim — used by
            # :class:`LinkHeaderPagination` when the API hands back a fully
            # qualified next-page URL.
            return path
        base = self.base_url.rstrip("/")
        rel = path.lstrip("/")
        return f"{base}/{rel}"


# --------------------------------------------------------------------------
# Builder — assemble session + retry + auth + rate limit
# --------------------------------------------------------------------------


def build_client(
    *,
    base_url: str,
    auth: AuthSpec,
    max_retries: int = 5,
    retry_backoff_seconds: float = 1.0,
    requests_per_second: float = 0,
    timeout_seconds: float = _DEFAULT_TIMEOUT_S,
    log: logging.Logger | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> RestClient:
    """Build a :class:`RestClient` configured per the connector's params.

    The single seam :mod:`source` calls to get a client; every other module in
    the connector treats the client as opaque. Retries cover 429 + 5xx with
    ``Retry-After`` respected; ``requests_per_second == 0`` means no
    rate-limiting. ``extra_headers`` adds connector-level constant headers (e.g.
    a vendor-required ``User-Agent``).
    """
    session = requests.Session()

    # Retry policy — applied to both GET and POST so the same client could be
    # repurposed; in v1 only GET is called. ``allowed_methods`` is the urllib3
    # >=1.26 spelling (older ``method_whitelist`` is deprecated).
    retry = Retry(
        total=max_retries,
        backoff_factor=retry_backoff_seconds,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Apply auth to the session up front (where possible) so every request
    # picks it up without per-call work.
    if auth.auth_type == "none":
        pass
    elif auth.auth_type == "bearer":
        session.headers["Authorization"] = f"Bearer {auth.token}"
    elif auth.auth_type == "basic":
        if ":" in auth.token:
            user, _, pwd = auth.token.partition(":")
            session.auth = (user, pwd)
        else:
            # Single-string token used as user — the GitHub / Stripe convention.
            session.auth = (auth.token, "")
    elif auth.auth_type == "api_key_header":
        session.headers[auth.header_name] = auth.token
    elif auth.auth_type == "api_key_query":
        # Applied per-request in RestClient.get — the session cannot pre-stuff
        # a query parameter.
        pass
    else:
        raise ValueError(
            f"unknown auth_type {auth.auth_type!r}; valid: "
            "none, bearer, basic, api_key_header, api_key_query"
        )

    rate_limiter = _RateLimiter(
        min_interval=(1.0 / requests_per_second) if requests_per_second > 0 else 0.0
    )
    return RestClient(
        session=session,
        base_url=base_url,
        auth=auth,
        rate_limiter=rate_limiter,
        timeout=timeout_seconds,
        log=log,
        _extra_headers=dict(extra_headers or {}),
    )
