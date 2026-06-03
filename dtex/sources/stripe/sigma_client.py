# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Stripe Sigma Query Run client — submit, poll, download.

The Sigma API at /v2/data/reporting/query_runs is asynchronous: you POST a
SQL string + parameters, get back a query-run ID, then GET the run's status
until it reaches `succeeded` (or `failed`/`canceled`). The succeeded response
carries a short-lived URL (~5 min TTL) to the CSV result blob.

This client wraps that loop. It is intentionally HTTP-level: no Stripe SDK
dependency (the Sigma API isn't well-supported by the SDK at the preview
stage; raw requests gives us deterministic version pinning).

Auth: Bearer <restricted-key> on every call. The Stripe-Version header is
pinned to the api_version param (default `2026-04-22.preview`).

Retry policy: 429 honors Retry-After; 5xx uses exponential backoff up to
max_retries; any other 4xx raises immediately (auth / permission / bad SQL
errors don't get better with retries). Polling between submit and download
uses a flat sleep (poll_interval_seconds), bounded by poll_timeout_seconds.
"""

from __future__ import annotations

import csv
import os
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import requests

# Bytes pulled off the download socket per read. Large enough that an 89 MB
# CSV is a few hundred reads (not tens of thousands of syscalls), small enough
# that a single read stays well inside any server idle window.
_DOWNLOAD_CHUNK_BYTES = 1 << 20  # 1 MiB


@dataclass
class SigmaClient:
    """Single-instance, single-stream-at-a-time Sigma client.

    Built per stream call by `source.py`; not shared across streams (the
    base URL + api version are immutable per run anyway, but the auth
    header is the secret that should not outlive the run).
    """

    base_url: str
    api_key: str
    api_version: str
    account_id: str = ""
    poll_interval_seconds: float = 2.0
    poll_timeout_seconds: int = 600
    max_retries: int = 5
    retry_backoff_seconds: float = 1.0
    timeout_seconds: float = 30.0

    def _headers(self) -> dict[str, str]:
        # NOTE: Stripe's v1 REST API uses application/x-www-form-urlencoded,
        # but the v2 Sigma endpoints require application/json (per the API's
        # explicit 415 response: "For v2 API endpoints, only JSON is
        # supported"). The requests library sets Content-Type automatically
        # when we pass `json=...`; we drop the explicit header so requests
        # can do that without an Accept-Encoding-style mismatch.
        #
        # NOTE: Stripe v2 API endpoints require the `Stripe-Context` header
        # to identify which account the call operates against. Even for a
        # single-account user, Stripe rejects the call with HTTP 403
        # "Permission denied. Api key does not have permission to access
        # account." without it. Sourced from the connector's `account_id`
        # param in register.yaml.
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Stripe-Version": self.api_version,
        }
        if self.account_id:
            headers["Stripe-Context"] = self.account_id
        return headers

    # ----------------------------------------------------------------
    # Public — submit + poll + download as one call
    # ----------------------------------------------------------------

    def run_query(self, sql: str, *, log: Any | None = None) -> Iterator[dict[str, Any]]:
        """Submit a SQL Query Run, poll until done, yield each result row as a dict.

        Stripe returns CSV; this method streams CSV → dict rows so the caller
        can batch without ever loading the entire result into memory. The
        `log` parameter, if supplied, accepts an injected dtex logger that we
        write progress events to.
        """
        run_id = self._submit(sql, log=log)
        download_url = self._poll_until_done(run_id, log=log)
        yield from self._stream_csv(download_url, log=log)

    # ----------------------------------------------------------------
    # Internals
    # ----------------------------------------------------------------

    def _submit(self, sql: str, *, log: Any | None = None) -> str:
        """POST /v2/data/reporting/query_runs — returns the run id.

        Stripe's v2 API endpoints require ``application/json`` (v1 uses
        form encoding, which is the common confusion). The `sql` field
        of the JSON body carries the literal SQL. We do NOT include
        parameters via the API's binding facility (preview-only); the
        cursor floor is baked into the SQL by source.py via the
        ``{since}`` placeholder substitution.
        """
        url = f"{self.base_url}/v2/data/reporting/query_runs"
        if log is not None:
            log.info("sigma: submit query (%d chars)", len(sql))
        response = self._post_with_retry(url, json_body={"sql": sql})
        body = response.json()
        run_id = body.get("id")
        if not run_id:
            raise RuntimeError(f"sigma: submit succeeded but response had no `id` field: {body!r}")
        if log is not None:
            log.info("sigma: query submitted: run_id=%s status=%s", run_id, body.get("status"))
        return run_id

    def _poll_until_done(self, run_id: str, *, log: Any | None = None) -> str:
        """GET /v2/data/reporting/query_runs/{id} until success → return CSV URL.

        Stripe's terminal statuses: `succeeded`, `failed`, `canceled`,
        `internal_error`. `pending` and `running` mean keep polling. We
        sleep `poll_interval_seconds` between polls and cap the total wait
        at `poll_timeout_seconds`.
        """
        url = f"{self.base_url}/v2/data/reporting/query_runs/{run_id}"
        deadline = time.monotonic() + self.poll_timeout_seconds
        while True:
            response = self._get_with_retry(url)
            body = response.json()
            status = body.get("status")
            if status == "succeeded":
                # Stripe's v2 Sigma success response shape, observed
                # 2026-05-29:
                #   {result: {file: {download_url: {url: "https://...", expires_at: "..."},
                #                    content_type: "csv", size: "2114"},
                #             "type": "file"}}
                # — verified end-to-end against a live Sigma account.
                result = body.get("result") or {}
                file_block = result.get("file") if isinstance(result, dict) else None
                download_block = (
                    file_block.get("download_url")
                    if isinstance(file_block, dict)
                    else None
                )
                csv_url = (
                    download_block.get("url")
                    if isinstance(download_block, dict)
                    else None
                )
                if not csv_url:
                    raise RuntimeError(
                        f"sigma: status=succeeded but no result.file.download_url.url: {body!r}"
                    )
                if log is not None:
                    size = (
                        file_block.get("size")
                        if isinstance(file_block, dict)
                        else None
                    )
                    log.info(
                        "sigma: query %s succeeded; csv ready (size=%s bytes)",
                        run_id,
                        size,
                    )
                return str(csv_url)
            # Failure statuses on v2: either at the top level (`failed`,
            # `canceled`, etc.) or under `status_details.failed.*`. We
            # treat the absence of a recognised running status as failure.
            if status in ("failed", "canceled", "internal_error"):
                # Surface `status_details.failed.error_message` when
                # present — it carries the actual Presto error.
                details = body.get("status_details") or {}
                failed = (
                    details.get("failed") if isinstance(details, dict) else None
                )
                err = failed if isinstance(failed, dict) and failed else body.get("error") or body
                raise RuntimeError(
                    f"sigma: query {run_id} ended with status={status!r}: {err!r}"
                )
            # Still pending / running — wait + check the deadline.
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"sigma: query {run_id} did not finish within "
                    f"{self.poll_timeout_seconds}s (last status={status!r})"
                )
            time.sleep(self.poll_interval_seconds)

    def _stream_csv(self, url: str, *, log: Any | None = None) -> Iterator[dict[str, Any]]:
        """Download the CSV at `url` to a temp file, then yield each row as a dict.

        The CSV download is a separate Stripe-hosted URL (often S3-backed);
        it does NOT carry the Authorization header (the URL itself is the
        capability).

        We download the *whole* body to a local temp file first, then parse
        it, rather than parsing straight off the socket. This is deliberate:

        - The caller (`source.py`) pulls rows lazily — each 500-row batch is
          staged to GCS and run through a BigQuery LOAD job *before* the next
          batch is requested. Parsing off the socket means the HTTP connection
          sits idle for the full duration of every LOAD job. Across an 89 MB
          result that idle time is enough for Stripe's CDN to close the
          connection mid-body — the `IncompleteRead` / `ChunkedEncodingError`
          we were seeing. Draining the socket in one continuous read decouples
          download speed from BigQuery's load pace and removes that root cause.
        - It also makes retry-on-connection-drop *correct*: no rows are yielded
          until the full CSV is in hand, so a failed download is retried from
          scratch with zero duplicate rows handed downstream. That matters for
          `replace`/`merge` staging tables, not just `append`.

        The temp file is bounded disk, not memory — the in-memory invariant the
        connector promises is preserved (we parse the file streaming, one row
        at a time).
        """
        tmp_path = self._download_to_tempfile(url, log=log)
        try:
            # newline="" per the csv module's contract: let the csv parser
            # handle line endings (including quoted-newline cells) itself.
            with open(tmp_path, encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                n = 0
                for row in reader:
                    n += 1
                    yield row
            if log is not None:
                log.info("sigma: CSV streamed: %d row(s)", n)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def _download_to_tempfile(self, url: str, *, log: Any | None = None) -> str:
        """Stream the CSV body to a temp file; retry the whole download on a drop.

        Connection breaks (`ChunkedEncodingError`, `ConnectionError`) surface
        while the *body* is being consumed, never at `requests.get()` — so the
        retry has to wrap the chunk-streaming loop, not just the request. We
        retry from byte zero (no Range-resume): the result blob persists
        server-side and a fresh continuous download is fast, so a clean re-pull
        is simpler and avoids partial-file stitching bugs.

        Mirrors `_request_with_retry`'s policy (max_retries + exponential
        backoff via `_sleep_for_retry`). Returns the temp file path; the caller
        owns deleting it.
        """
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            # Fresh temp file per attempt — a half-written file from a dropped
            # download must never be parsed.
            fd, tmp_path = tempfile.mkstemp(prefix="sigma-csv-", suffix=".csv")
            try:
                # `os.fdopen` takes ownership of `fd` immediately, so wrapping
                # the whole request in the `with` guarantees the descriptor is
                # closed however we leave — including a connect-time
                # ConnectionError from `requests.get` itself, before any bytes
                # arrive. That leaves the except branches needing only to
                # unlink the (now-closed) temp file.
                with os.fdopen(fd, "wb") as out:
                    # The download URL does NOT need Stripe auth — it's a
                    # signed short-lived URL with the credential baked in.
                    # Keeping Stripe-Version off avoids any chance of the
                    # signing proxy rejecting the request.
                    response = requests.get(url, stream=True, timeout=self.timeout_seconds)
                    response.raise_for_status()
                    written = 0
                    for chunk in response.iter_content(chunk_size=_DOWNLOAD_CHUNK_BYTES):
                        if chunk:
                            out.write(chunk)
                            written += len(chunk)
                if log is not None:
                    log.info("sigma: CSV downloaded: %d bytes", written)
                return tmp_path
            except (requests.exceptions.ChunkedEncodingError, requests.ConnectionError) as exc:
                # Transient connect-time or mid-body drop — clean up the
                # partial file and retry the whole download from scratch.
                _unlink_quiet(tmp_path)
                last_error = exc
                if log is not None:
                    log.warning(
                        "sigma: CSV download dropped (attempt %d/%d): %s",
                        attempt + 1,
                        self.max_retries + 1,
                        exc,
                    )
                self._sleep_for_retry(attempt, retry_after=None)
                continue
            except BaseException:
                # Any other error (incl. raise_for_status 4xx/5xx): don't
                # leak the temp file. The `with` already closed the fd.
                _unlink_quiet(tmp_path)
                raise
        # Exhausted retries on connection drops.
        assert last_error is not None
        raise last_error

    def _post_with_retry(
        self, url: str, *, json_body: dict[str, Any]
    ) -> requests.Response:
        return self._request_with_retry("POST", url, json_body=json_body)

    def _get_with_retry(self, url: str) -> requests.Response:
        return self._request_with_retry("GET", url)

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> requests.Response:
        """One HTTP call with exponential backoff on 429/5xx; raise on other 4xx.

        Stripe's v2 API endpoints want ``application/json`` (not form
        encoding); we pass the body via ``json=…`` so requests sets the
        Content-Type header automatically and serializes the dict.
        """
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=json_body,
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                self._sleep_for_retry(attempt, retry_after=None)
                continue
            if response.status_code < 300:
                return response
            if response.status_code in (429,) or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")
                self._sleep_for_retry(attempt, retry_after=retry_after)
                last_error = _http_error(response)
                continue
            # 4xx other than 429: bad request / unauthenticated / forbidden /
            # not found — these are not transient. Surface immediately.
            raise _http_error(response)
        # Exhausted retries.
        assert last_error is not None
        raise last_error

    def _sleep_for_retry(self, attempt: int, *, retry_after: str | None) -> None:
        """Sleep before retry: honor Retry-After if present, else exponential."""
        if retry_after:
            try:
                seconds = float(retry_after)
            except ValueError:
                # HTTP-date form (rare for Stripe) — fall back to backoff.
                seconds = self.retry_backoff_seconds * (2**attempt)
        else:
            seconds = self.retry_backoff_seconds * (2**attempt)
        time.sleep(seconds)


def _unlink_quiet(path: str) -> None:
    """Best-effort temp-file removal; never raise from a cleanup path."""
    try:
        os.unlink(path)
    except OSError:
        pass


def _http_error(response: requests.Response) -> RuntimeError:
    """Build a RuntimeError that names the HTTP status + body but not the API key.

    The Authorization header is on the request, not the response — so we
    don't need to redact it here; we just include the status and body for
    diagnostics. Stripe error bodies are JSON-shaped: `{error: {...}}`.
    """
    try:
        body = response.json()
    except ValueError:
        body = response.text[:500]
    return RuntimeError(
        f"sigma: HTTP {response.status_code} on {response.request.method} "
        f"{response.request.url}: {body!r}"
    )
