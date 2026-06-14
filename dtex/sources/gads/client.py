"""Google Ads API REST client — thin wrapper over ``requests``.

The connector's single extraction surface is GAQL (the Google Ads Query
Language) run against ``GoogleAdsService.searchStream``. This client owns
three HTTP concerns and nothing else:

* **OAuth2 token minting.** Google Ads REST authenticates every call with a
  short-lived Bearer *access token*, which the caller does not hold directly
  — they hold a long-lived *refresh token*. ``_access_token()`` exchanges the
  refresh token (+ client id/secret) at Google's token endpoint and caches
  the result until ~60s before it expires, so a whole run typically mints
  exactly one token. (Verified June 2026 against the official REST auth docs:
  ``POST https://www.googleapis.com/oauth2/v3/token`` with
  ``grant_type=refresh_token``.)

* **The three required headers.** Every Google Ads call needs
  ``Authorization: Bearer <token>``, a ``developer-token`` header, and —
  when a manager (MCC) account calls on behalf of a client — a
  ``login-customer-id`` header with hyphens stripped. The developer token
  and the bearer token are set per request and NEVER appear in a log line.

* **searchStream.** ``search_stream(customer_id, query)`` POSTs the GAQL body
  to ``/v{version}/customers/{cid}:searchStream`` and yields each
  ``GoogleAdsRow`` dict. searchStream returns a JSON *array* (one element per
  streamed chunk), each element carrying a ``results`` list — unlike the
  paged ``:search`` method there are no page tokens.

Retry/timeout handling mirrors the baked ``revenuecat`` client (which fixed
three real hangs): a ``(connect, read)`` timeout tuple so a dead socket can't
block a run forever; network-level ``RequestException``s caught around the
call (they raise before any ``resp`` exists); and 429 / 5xx both routed
through one bounded, capped exponential backoff. Google Ads signals
rate-limiting with 429 + ``RESOURCE_EXHAUSTED``; same path.

This module does NOT import from ``dtex`` and does NOT know which streams
exist — pure HTTP, easy to unit-test by pointing it at a stub server.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests

# (connect_timeout, read_timeout) seconds. The connect leg should be fast;
# the read leg is the dangerous one — a single hung searchStream response
# would otherwise block the whole run. 120s read is generous for a large
# streamed report without trapping a dead socket forever.
_TIMEOUT: tuple[float, float] = (10.0, 120.0)

# Refresh the access token this many seconds BEFORE its stated expiry, so a
# long-running stream never tries to authenticate with a token that lapses
# mid-flight.
_TOKEN_REFRESH_SKEW_SECONDS = 60.0


@dataclass
class GoogleAdsClient:
    developer_token: str
    client_id: str
    client_secret: str
    refresh_token: str
    login_customer_id: str | None = None
    api_version: str = "v24"
    base_url: str = "https://googleads.googleapis.com"
    token_url: str = "https://www.googleapis.com/oauth2/v3/token"
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0
    log: logging.Logger | logging.LoggerAdapter[Any] | None = None

    _session: requests.Session = field(
        default_factory=requests.Session, init=False, repr=False
    )
    # Cached (access_token, monotonic_expiry) — minted lazily on first call.
    _access_token_value: str | None = field(default=None, init=False, repr=False)
    _access_token_expiry: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        # The login-customer-id header must carry digits only — Google Ads
        # account IDs are commonly written hyphenated (123-456-7890) but the
        # header rejects the hyphens.
        if self.login_customer_id:
            self.login_customer_id = self.login_customer_id.replace("-", "")

    # -- OAuth -------------------------------------------------------------

    def _access_token(self) -> str:
        """Return a valid access token, minting/refreshing as needed.

        Cached until ``_TOKEN_REFRESH_SKEW_SECONDS`` before its expiry. The
        refresh token, client secret, and minted access token are never
        logged — only the fact of a refresh is.
        """
        now = time.monotonic()
        if self._access_token_value is not None and now < self._access_token_expiry:
            return self._access_token_value

        if self.log is not None:
            self.log.info("gads: refreshing OAuth access token")

        token = self._mint_access_token()
        return token

    def _mint_access_token(self, _attempt: int = 0) -> str:
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }
        try:
            resp = self._session.post(self.token_url, data=payload, timeout=_TIMEOUT)
        except requests.exceptions.RequestException as exc:
            if _attempt < self.max_retries:
                time.sleep(self._backoff(_attempt))
                return self._mint_access_token(_attempt + 1)
            raise RuntimeError(
                f"gads: OAuth token endpoint network failure after "
                f"{self.max_retries} retries: {exc}"
            ) from exc

        if resp.status_code in (429, 500, 502, 503, 504) and _attempt < self.max_retries:
            time.sleep(self._backoff(_attempt))
            return self._mint_access_token(_attempt + 1)
        if resp.status_code != 200:
            # Do not echo the response body verbatim — it can contain the
            # client_id. A status code + the request-less message is enough.
            raise RuntimeError(
                f"gads: OAuth token exchange failed with HTTP {resp.status_code}"
            )

        data = resp.json()
        access_token = data.get("access_token")
        if not access_token:
            raise RuntimeError("gads: OAuth token response had no access_token")
        # `expires_in` is seconds-from-now; default to a conservative hour if
        # the field is missing.
        expires_in = float(data.get("expires_in", 3600))
        self._access_token_value = str(access_token)
        self._access_token_expiry = (
            time.monotonic() + max(expires_in - _TOKEN_REFRESH_SKEW_SECONDS, 0.0)
        )
        return self._access_token_value

    def _headers(self) -> dict[str, str]:
        headers = {
            "Authorization": f"Bearer {self._access_token()}",
            "developer-token": self.developer_token,
            "Content-Type": "application/json",
        }
        if self.login_customer_id:
            headers["login-customer-id"] = self.login_customer_id
        return headers

    # -- searchStream ------------------------------------------------------

    def search_stream(self, customer_id: str, query: str) -> Iterator[dict]:
        """Run a GAQL ``query`` against one customer; yield each GoogleAdsRow.

        searchStream returns a JSON array whose elements each carry a
        ``results`` list of GoogleAdsRow objects (see the REST search docs —
        "the results of a SearchStream API call are wrapped in a JSON
        array"). We buffer the full array and iterate it. For very large
        pulls this holds the whole response in memory; the connector batches
        downstream and the engine commits per batch, so memory pressure is on
        the response buffer alone. If that ever bites, switch to an
        incremental JSON-array parser here — the yield contract is unchanged.
        """
        cid = customer_id.replace("-", "")
        # The REST path carries a `/googleAds` resource segment before the
        # `:searchStream` custom verb — verified live against the API (the
        # bare `/customers/{cid}:searchStream` form 404s at Google's edge).
        url = (
            f"{self.base_url}/{self.api_version}/customers/{cid}"
            f"/googleAds:searchStream"
        )
        body = self._post(url, {"query": query})

        # `body` is a list of chunk objects: [{"results": [...], ...}, ...].
        # Some Google Ads responses for an empty result set return an empty
        # array or a single chunk with no `results` key — both are fine.
        if not isinstance(body, list):
            raise RuntimeError(
                f"gads: searchStream expected a JSON array, got {type(body).__name__}"
            )
        for chunk in body:
            yield from chunk.get("results", []) or []

    def list_child_accounts(
        self, manager_id: str, *, max_depth: int = 1
    ) -> list[str]:
        """Return the enabled, non-manager (leaf) account ids under an MCC.

        Queries the ``customer_client`` resource against the manager account
        — this enumerates the whole tree below it, with ``customer_client.id``
        the child id, ``customer_client.level`` the depth (1 = direct child),
        ``customer_client.manager`` whether the child is itself a manager, and
        ``customer_client.status`` its lifecycle state. The filters are pushed
        server-side (verified live), so only leaf, ENABLED accounts within
        ``max_depth`` come back. The manager itself (level 0) is excluded both
        by the ``level >= 1`` floor and the ``manager = false`` filter.

        Runs WITH ``login-customer-id`` set to ``manager_id`` for the duration
        of the call — that header is what authorizes a manager to read its
        tree — then restores whatever was configured before.
        """
        mid = manager_id.replace("-", "")
        query = (
            "SELECT customer_client.id, customer_client.level, "
            "customer_client.manager, customer_client.status "
            "FROM customer_client "
            f"WHERE customer_client.level >= 1 "
            f"AND customer_client.level <= {int(max_depth)} "
            "AND customer_client.manager = false "
            "AND customer_client.status = 'ENABLED'"
        )
        # The tree query must be issued AS the manager (login-customer-id),
        # regardless of any login id the run otherwise uses.
        previous_login = self.login_customer_id
        self.login_customer_id = mid
        try:
            rows = list(self.search_stream(mid, query))
        finally:
            self.login_customer_id = previous_login

        ids: list[str] = []
        for row in rows:
            cc = row.get("customerClient") or {}
            child_id = cc.get("id")
            if child_id:
                ids.append(str(child_id))
        return ids

    def _post(self, url: str, json_body: dict, _attempt: int = 0) -> Any:
        try:
            resp = self._session.post(
                url, json=json_body, headers=self._headers(), timeout=_TIMEOUT
            )
        except requests.exceptions.RequestException as exc:
            if _attempt < self.max_retries:
                time.sleep(self._backoff(_attempt))
                return self._post(url, json_body, _attempt + 1)
            raise RuntimeError(
                f"gads: network failure after {self.max_retries} retries on {url}: {exc}"
            ) from exc

        if resp.status_code == 429:
            # Google Ads rate-limit (RESOURCE_EXHAUSTED). Honor Retry-After
            # if present, but bound the loop so a sustained limit can't wedge
            # the run forever.
            if _attempt >= self.max_retries:
                raise RuntimeError(
                    f"gads: rate-limited after {self.max_retries} retries on {url}"
                )
            wait = float(resp.headers.get("Retry-After", self._backoff(_attempt)))
            time.sleep(wait)
            return self._post(url, json_body, _attempt + 1)
        if resp.status_code in (500, 502, 503, 504) and _attempt < self.max_retries:
            time.sleep(self._backoff(_attempt))
            return self._post(url, json_body, _attempt + 1)
        if resp.status_code != 200:
            # Surface the Google Ads error payload — it carries the GAQL
            # syntax/permission detail an operator needs, and contains no
            # secret (the token lives in the request, not the response).
            raise RuntimeError(
                f"gads: searchStream HTTP {resp.status_code} on {url}: {resp.text}"
            )
        return resp.json()

    def _backoff(self, attempt: int) -> float:
        """Capped exponential backoff: base * 2**attempt, ceiling 60s."""
        return min(self.retry_backoff_seconds * (2**attempt), 60.0)
