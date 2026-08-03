"""Chargebacks911 CBAPIv2 HTTP client — thin wrapper over `requests`.

Two surfaces:

* `paginate(path, params)` — for list endpoints (/clients/{id}/alerts,
  /clients/{id}/chargebacks). CB911 paginates with `limit` + `page`
  (1-based) query params; the walk ends when a page returns fewer than
  `limit` rows.
* `get(path, params)` — one-shot GET, returns the unwrapped payload.

Auth is CB911's unusual two-step scheme:

* Basic auth (username + password) on ``GET /auth`` returns
  ``{"success": true, ..., "data": {"accessToken": "..."}}``. Every other
  request carries ``Authorization: Bearer <accessToken>``.
* The bearer token expires after ~60 minutes AND minting a new token
  invalidates the previous one (last-mint-wins). The client therefore:
  - mints lazily on the first data request (no token at construction);
  - proactively re-mints when the token is older than ~50 minutes, so a
    long extraction never sails past the 60-minute TTL mid-walk;
  - on a 401 mid-run (expired, or another process minted and stole the
    token), re-mints ONCE and retries the request. A second consecutive
    401 raises — retrying further would just fight the other holder.

Response envelope: CB911 nominally wraps payloads as
``{success, code, message, data}`` — but live production responses
sometimes return a bare JSON list directly. `_unwrap` handles both:
a dict is checked for ``success`` and unwrapped to its ``data``; a list
is used as-is. Non-2xx or ``success: false`` is an error.

Retry policy (shared by the auth and data legs): HTTP 429 honors
``Retry-After`` but still increments the attempt counter (a sustained
rate limit must not wedge the run forever); 5xx and network-level errors
(timeout, connection reset, broken chunked encoding) back off
exponentially; all three paths are bounded by ``max_retries``. Any other
4xx raises immediately — except 401 on a bearer request, which routes
through the re-mint-once path above.

Credentials never appear in logs or error messages: the username /
password fields are declared ``repr=False``, the client does no logging,
and every raised message contains only URLs, status codes, and the
server's own ``message`` string.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests

# (connect_timeout, read_timeout) seconds. The connect leg should be fast;
# the read leg is the dangerous one — a single hung response would block
# the whole run. 60s is generous for slow report queries without trapping
# a dead socket forever.
_TIMEOUT: tuple[float, float] = (10.0, 60.0)

# Proactive re-mint threshold. CB911 tokens live ~60 minutes; re-minting
# at ~50 keeps a safety margin so a request never departs with a token
# that expires while the response is streaming.
_TOKEN_MAX_AGE_SECONDS: float = 50 * 60


class _Unauthorized(Exception):
    """Internal marker: a bearer-authed request came back 401.

    Routed to the re-mint-once path in `_bearer_get`; never escapes the
    client (a persistent 401 surfaces as `RuntimeError`).
    """


@dataclass
class Chargebacks911Client:
    username: str = field(repr=False)
    password: str = field(repr=False)
    base_url: str = "https://api.cbresponseservices.com/v2"
    max_retries: int = 5
    _session: requests.Session = field(
        default_factory=requests.Session, init=False, repr=False
    )
    _token: str | None = field(default=None, init=False, repr=False)
    _token_minted_at: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._session.headers.update({"Accept": "application/json"})

    # -- public surface ------------------------------------------------------

    def paginate(
        self, path: str, params: dict[str, Any] | None = None
    ) -> Iterator[dict]:
        """Yield every row from a CB911 list endpoint.

        CB911 paginates with ``limit`` + ``page`` (1-based). The walk stops
        when a page returns fewer than ``limit`` rows — there is no
        next-page token or total count in the envelope.
        """
        query = dict(params or {})
        limit = int(query.get("limit") or 500)
        query["limit"] = limit
        page = 1
        while True:
            query["page"] = page
            rows = self._unwrap_list(
                self._bearer_get(f"{self.base_url}{path}", query), path
            )
            yield from rows
            if len(rows) < limit:
                return
            page += 1

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """One-shot GET on a non-list endpoint. Returns the unwrapped payload."""
        body = self._bearer_get(f"{self.base_url}{path}", params)
        if isinstance(body, dict) and "data" in body:
            return body["data"]
        return body

    # -- auth ----------------------------------------------------------------

    def _ensure_token(self) -> None:
        """Mint lazily on first use; proactively re-mint past the age cap."""
        if (
            self._token is None
            or time.monotonic() - self._token_minted_at > _TOKEN_MAX_AGE_SECONDS
        ):
            self._mint_token()

    def _mint_token(self) -> None:
        """GET /auth with Basic auth; store data.accessToken as the bearer.

        Minting invalidates any previously issued token account-wide —
        callers must treat this as a last-mint-wins operation.
        """
        body = self._request(
            f"{self.base_url}/auth",
            params=None,
            auth=(self.username, self.password),
            bearer=False,
        )
        data = body.get("data") if isinstance(body, dict) else None
        token = data.get("accessToken") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError(
                "chargebacks911: /auth succeeded but returned no data.accessToken"
            )
        self._token = str(token)
        self._token_minted_at = time.monotonic()

    def _bearer_get(self, url: str, params: dict[str, Any] | None) -> Any:
        """Bearer-authed GET with the 401-re-mint-once policy."""
        self._ensure_token()
        try:
            return self._request(url, params)
        except _Unauthorized:
            # Token expired early, or a concurrent run minted a new token
            # and invalidated ours. Re-mint ONCE and retry the request.
            self._mint_token()
            try:
                return self._request(url, params)
            except _Unauthorized:
                raise RuntimeError(
                    f"chargebacks911: still unauthorized on {url} after "
                    "re-minting the access token — is another process "
                    "minting tokens for the same account?"
                ) from None

    # -- transport -----------------------------------------------------------

    def _request(
        self,
        url: str,
        params: dict[str, Any] | None,
        *,
        auth: tuple[str, str] | None = None,
        bearer: bool = True,
    ) -> Any:
        """One GET with bounded retries for 429 / 5xx / network errors.

        401 semantics differ by leg: a bearer-authed 401 raises the internal
        `_Unauthorized` marker (handled by the re-mint path); a Basic-auth
        401 on /auth means the credentials themselves are wrong and raises
        immediately (re-minting cannot fix it). Any other 4xx raises via
        `raise_for_status`. A parsed dict body with ``success: false`` is
        also an error even under HTTP 200.
        """
        attempt = 0
        while True:
            headers: dict[str, str] = {}
            if bearer:
                headers["Authorization"] = f"Bearer {self._token}"
            try:
                resp = self._session.get(
                    url, params=params, headers=headers, auth=auth, timeout=_TIMEOUT
                )
            except requests.exceptions.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(min(2**attempt, 60))
                    attempt += 1
                    continue
                raise RuntimeError(
                    f"chargebacks911: network failure after {self.max_retries} "
                    f"retries on {url}: {exc}"
                ) from exc

            if resp.status_code == 429:
                # Honor the server's Retry-After directive, but ALSO bound
                # the loop — a sustained rate limit must not hang the run.
                if attempt >= self.max_retries:
                    raise RuntimeError(
                        f"chargebacks911: rate-limited after {self.max_retries} "
                        f"retries on {url}; "
                        f"Retry-After={resp.headers.get('Retry-After')}"
                    )
                time.sleep(int(resp.headers.get("Retry-After", 60)))
                attempt += 1
                continue
            if resp.status_code in (500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(min(2**attempt, 60))
                attempt += 1
                continue
            if resp.status_code == 401:
                if bearer:
                    raise _Unauthorized()
                raise RuntimeError(
                    "chargebacks911: authentication failed on /auth — check "
                    "the CHARGEBACKS911_USERNAME / CHARGEBACKS911_PASSWORD "
                    "credentials"
                )
            resp.raise_for_status()

            body: Any = resp.json()
            if isinstance(body, dict) and body.get("success") is False:
                raise RuntimeError(
                    f"chargebacks911: API error on {url}: "
                    f"code={body.get('code')} message={body.get('message')!r}"
                )
            return body

    # -- envelope ------------------------------------------------------------

    @staticmethod
    def _unwrap_list(body: Any, path: str) -> list[dict]:
        """Unwrap a list payload from either envelope shape.

        Nominal: ``{"success": true, ..., "data": [...]}``. Live production
        responses sometimes skip the envelope and return the bare list.
        A null ``data`` counts as an empty page.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            data = body.get("data")
            if data is None:
                return []
            if isinstance(data, list):
                return data
        raise RuntimeError(
            f"chargebacks911: unexpected response shape on {path}: "
            f"expected a list (bare or under 'data'), got {type(body).__name__}"
        )
