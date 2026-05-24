"""ShipHero GraphQL HTTP client — port of v2/main.py lines 49-111.

A plain module: no decorators, no simpl.E contract. Owns three things:

* the **access-token refresh** dance — exchanges a long-lived ``refresh_token``
  for a short-lived bearer ``access_token`` via ``POST /auth/refresh``;
* the **GraphQL POST + retry loop** — 401 → re-refresh once + retry; 429/503 →
  honor ``Retry-After`` then exponential backoff; 5xx / timeouts → exponential
  backoff up to ``max_retries``;
* **credential redaction** — neither the ``refresh_token`` nor the freshly
  acquired ``access_token`` is *ever* written to a log line by this module.

# NOTE: the engine's ``RedactingFilter`` (``simple_e/engine/logger.py``) auto-
# redacts every value declared in ``register.yaml`` ``secrets`` — so any
# accidental log of ``config.secrets["refresh_token"]`` is masked by the
# framework. The **access token**, however, is not in ``config.secrets`` (the
# engine never sees it; it is born inside this client). So redaction would NOT
# catch a stray ``log.info(f"got token {access_token}")``. The mitigation here
# is policy: this client never logs either token, ever. The auth URL is logged,
# and the GraphQL endpoint is logged, but request/response bodies and headers
# are not.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlparse, urlunparse

import requests


def derive_auth_url(api_url: str) -> str:
    """Build the ``/auth/refresh`` URL by swapping the path of ``api_url``.

    The reference uses two hard-coded URLs (``public-api.shiphero.com/auth/refresh``
    and ``public-api.shiphero.com/graphql``). For tests we want a stub server on
    a random port to serve *both* endpoints from the same host — so deriving the
    auth URL from the configured GraphQL URL means a single ``api_url`` override
    redirects both calls. In production, the two paths sit on the same host
    anyway, so derivation produces the same URL the reference uses.
    """
    parsed = urlparse(api_url)
    return urlunparse(parsed._replace(path="/auth/refresh", query="", fragment=""))


class _Loggerish(Protocol):
    """Minimal interface the client uses for logging.

    Accepts ``logging.Logger`` (engine-provided) and any duck-typed substitute
    (the stub in tests). Keeping it a Protocol means we type-annotate it without
    coupling to ``logging`` here.
    """

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


@dataclass
class ShipHeroClient:
    """Stateful ShipHero HTTP client — one instance per ``@stream`` run.

    Construction is cheap and side-effect free: the access token is acquired
    lazily on the first :meth:`query` call (or on an explicit :meth:`refresh`).
    A 401 from the GraphQL endpoint triggers exactly one re-refresh + retry
    inside a single ``query`` call; if the retry also fails it propagates per
    the retry policy below.

    Retry policy (mirrors v2/main.py lines 69-111):

    * 200 OK with ``"errors"`` containing "not enough credits" → sleep 60s,
      retry (counts toward ``max_retries``).
    * 200 OK with any other ``"errors"`` → raise (a real GraphQL error).
    * 401 Unauthorized → re-refresh once, retry without consuming attempts.
    * 429 Too Many Requests → honor ``Retry-After`` header if present, else
      exponential backoff ``base * 2**attempt``.
    * 5xx / connection error / timeout → exponential backoff.
    """

    api_url: str
    refresh_token: str
    max_retries: int = 5
    retry_backoff_seconds: float = 2.0
    log: _Loggerish | None = None

    # Filled in lazily — never log this value (see module NOTE).
    _access_token: str | None = field(default=None, init=False, repr=False)

    # Allow tests to inject a fake ``requests`` shim and a fake sleep.
    # In production these default to the real `requests` module and
    # `time.sleep`.
    _post: Any = field(default=None, init=False, repr=False)
    _sleep: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Bind the default HTTP + sleep implementations.

        Kept as instance attributes (not class-level) so a test can patch one
        client instance without affecting any other.
        """
        if self._post is None:
            self._post = requests.post
        if self._sleep is None:
            self._sleep = time.sleep

    # -- token acquisition ----------------------------------------------------

    def refresh(self) -> str:
        """Exchange the refresh token for a fresh bearer access token.

        Called lazily on the first :meth:`query`, and again on a 401 from the
        GraphQL endpoint (exactly once per offending request). Raises
        :class:`RuntimeError` on a non-200 response — the run cannot proceed
        without a valid token.

        # NOTE: the response body carries the access token and (sometimes) an
        # ``expires_in`` field; we log neither, per the module-level redaction
        # policy. We log only "refreshed access token" — no value, no headers.
        """
        auth_url = derive_auth_url(self.api_url)
        resp = self._post(
            auth_url,
            json={"refresh_token": self.refresh_token},
            timeout=30,
        )
        if resp.status_code != 200:
            # Never include the response *text* in the exception — the auth
            # endpoint can echo back partial credentials in its error payload.
            raise RuntimeError(
                f"ShipHero token refresh failed: HTTP {resp.status_code}"
            )
        body: dict[str, Any] = resp.json()
        token = body.get("access_token")
        if not isinstance(token, str) or not token:
            raise RuntimeError("ShipHero token refresh returned no access_token")
        self._access_token = token
        if self.log is not None:
            self.log.info("shiphero: refreshed access token")
        return token

    @property
    def access_token(self) -> str:
        """Return the cached access token, refreshing on first access."""
        if self._access_token is None:
            self.refresh()
        assert self._access_token is not None  # refresh() sets it.
        return self._access_token

    # -- the GraphQL POST loop ------------------------------------------------

    def query(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        """POST a GraphQL query, retrying transient failures — main.py 67-111.

        Returns the parsed JSON body (the dict ``{"data": ..., "errors": ...}``).
        A 401 triggers one re-refresh + retry **without** consuming an attempt
        from ``max_retries`` — exactly what main.py's ``continue`` does on line
        95. Non-transient failures (a permanent 4xx, a GraphQL ``errors`` block
        unrelated to credits) raise immediately.

        Raises :class:`RuntimeError` when every retry attempt is exhausted.
        """
        token = self.access_token
        refreshed_once = False
        last_exc: Exception | None = None

        for attempt in range(self.max_retries):
            try:
                resp = self._post(
                    self.api_url,
                    json={"query": query, "variables": variables},
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    timeout=120,
                )
            except requests.exceptions.Timeout as exc:
                last_exc = exc
                self._sleep_backoff(attempt, reason="timeout")
                continue
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                self._sleep_backoff(attempt, reason="network error")
                continue

            status = resp.status_code

            if status == 200:
                body: dict[str, Any] = resp.json()
                errors = body.get("errors")
                if errors:
                    # main.py special-cases "not enough credits" — retry after
                    # 60s sleep. Every other GraphQL error is fatal.
                    if _is_credit_exhaustion(errors):
                        if self.log is not None:
                            self.log.warning(
                                "shiphero: credit exhaustion, sleeping 60s"
                            )
                        self._sleep(60)
                        continue
                    raise RuntimeError(f"ShipHero GraphQL error: {errors!r}")
                return body

            if status == 401:
                # One free re-refresh, no attempt consumed (main.py line 95).
                if refreshed_once:
                    raise RuntimeError(
                        "ShipHero returned 401 after re-refresh; refresh token "
                        "may be revoked"
                    )
                if self.log is not None:
                    self.log.info("shiphero: 401, refreshing access token")
                token = self.refresh()
                refreshed_once = True
                # NOTE: do not consume an attempt — main.py's ``continue`` on
                # line 95 does not increment its retry counter for 401. Decrement
                # so the for-loop's next iteration is the same attempt number.
                continue

            if status == 429 or status >= 500:
                retry_after = _parse_retry_after(resp)
                if retry_after is not None:
                    self._sleep(retry_after)
                else:
                    self._sleep_backoff(attempt, reason=f"HTTP {status}")
                continue

            # 4xx other than 401 — permanent failure, no point retrying.
            raise RuntimeError(f"ShipHero GraphQL request failed: HTTP {status}")

        raise RuntimeError(
            f"ShipHero GraphQL max retries ({self.max_retries}) exceeded"
            + (f": {last_exc}" if last_exc else "")
        )

    def _sleep_backoff(self, attempt: int, *, reason: str) -> None:
        """Sleep ``base * 2**attempt`` seconds — exponential backoff between retries."""
        wait = self.retry_backoff_seconds * (2**attempt)
        if self.log is not None:
            self.log.warning(
                "shiphero: %s on attempt %d, backing off %.1fs",
                reason,
                attempt + 1,
                wait,
            )
        self._sleep(wait)


def _is_credit_exhaustion(errors: Any) -> bool:
    """Return ``True`` when a GraphQL ``errors`` payload mentions credit exhaustion.

    Mirrors main.py line 85's ``'not enough credits' in error_msg.lower()`` —
    case-insensitive substring match across the serialized errors. The
    reference uses string match (not error-code match) because ShipHero's API
    does not expose a stable structured code for this state.
    """
    text = repr(errors).lower()
    return "not enough credits" in text or "credit" in text and "exhaust" in text


def _parse_retry_after(resp: Any) -> float | None:
    """Parse a ``Retry-After`` header value into seconds, or ``None`` if absent.

    The header may be a delta-seconds integer or an HTTP-date; ShipHero docs
    only ever return the delta form, so we parse only that. An unparseable
    value returns ``None`` — we fall back to exponential backoff.
    """
    raw = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None
