#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""One-shot helper: mint a Google Ads API refresh token via a loopback flow.

Google Ads REST authenticates each call with a short-lived Bearer *access*
token, which the connector mints automatically at run time. But to mint
access tokens it needs a long-lived *refresh* token, and a refresh token can
only be produced by a one-time interactive consent: a human logs in and
approves the ``adwords`` scope in a browser. This script runs that flow once.

How it works (the "loopback" / installed-app OAuth flow):

  1. Spin up a tiny ``http.server`` on ``localhost:<port>``.
  2. Open the browser to Google's consent screen, with this local server's
     ``http://localhost:<port>/`` as the redirect URI.
  3. After you click *Allow*, Google redirects back to the local server with
     a one-time ``?code=...``; the server captures it.
  4. Exchange that code (+ client id/secret) for a refresh token at Google's
     token endpoint and print the refresh token.

By default the refresh token is WRITTEN to a git-ignored file
(``.secrets/gads_refresh_token``, mode 0600) and never printed — so the
secret stays on your disk, out of terminal scrollback and out of any chat
transcript. Feed it to the connector by reading the file into the env var::

    export GADS_REFRESH_TOKEN="$(cat .secrets/gads_refresh_token)"

Pass ``--print`` if you really want it on stdout instead.

Redirect-URI requirement
------------------------
The redirect URI passed here MUST be registered on the OAuth client in the
Google Cloud Console:
  * **Desktop app** OAuth clients allow ``http://localhost`` (any port)
    automatically — nothing to configure. This is the recommended client type.
  * **Web application** clients must have the exact
    ``http://localhost:<port>/`` added under *Authorized redirect URIs*.
Use ``--port`` to match whatever you registered (default 8080).

Usage
-----
    # creds from env (GADS_CLIENT_ID / GADS_CLIENT_SECRET):
    python -m dtex.sources.gads.scripts.get_refresh_token

    # or pass them explicitly:
    python -m dtex.sources.gads.scripts.get_refresh_token \\
        --client-id XXX.apps.googleusercontent.com --client-secret YYY

    # custom loopback port (must match the GCP redirect URI for Web clients):
    python -m dtex.sources.gads.scripts.get_refresh_token --port 8765

    # write somewhere else, or print to stdout instead of a file:
    python -m dtex.sources.gads.scripts.get_refresh_token --out path/to/token
    python -m dtex.sources.gads.scripts.get_refresh_token --print

Stdlib only — no dependency on ``requests`` or the rest of dtex, so it runs
even in a bare interpreter.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Where the refresh token is written by default. A dotted, conventionally
# git-ignored folder so the secret never lands in the chat, the terminal
# scrollback, or version control. The connector reads it back from here.
_DEFAULT_OUT = ".secrets/gads_refresh_token"

# Google OAuth 2.0 endpoints (verified June 2026 against the Google Ads REST
# auth docs). The token endpoint matches the one the connector's client uses.
_AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"
_TOKEN_ENDPOINT = "https://www.googleapis.com/oauth2/v3/token"

# The single OAuth scope the Google Ads API requires.
_SCOPE = "https://www.googleapis.com/auth/adwords"

_SUCCESS_HTML = (
    b"<html><body><h2>dtex: Google Ads authorization complete.</h2>"
    b"<p>You can close this tab and return to the terminal.</p></body></html>"
)
_ERROR_HTML = (
    b"<html><body><h2>dtex: authorization failed.</h2>"
    b"<p>See the terminal for details. You can close this tab.</p></body></html>"
)


class _CallbackHandler(BaseHTTPRequestHandler):
    """Captures the single OAuth redirect, stashing code/error on the server."""

    def log_message(self, *_args: object) -> None:
        return  # silence default request logging

    def do_GET(self) -> None:  # noqa: N802 — required by stdlib
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        def first(key: str) -> str | None:
            values = params.get(key)
            return values[0] if values else None

        # Stash results on the server instance for the main thread to read.
        self.server.oauth_code = first("code")  # type: ignore[attr-defined]
        self.server.oauth_error = first("error")  # type: ignore[attr-defined]
        self.server.oauth_state = first("state")  # type: ignore[attr-defined]
        ok = self.server.oauth_code is not None  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(_SUCCESS_HTML if ok else _ERROR_HTML)


def _capture_code(redirect_uri: str, auth_url: str, port: int) -> str:
    """Open the browser, wait for the single redirect, return the auth code."""
    server = HTTPServer(("127.0.0.1", port), _CallbackHandler)
    server.oauth_code = None  # type: ignore[attr-defined]
    server.oauth_error = None  # type: ignore[attr-defined]
    server.oauth_state = None  # type: ignore[attr-defined]

    print(f"Opening the consent screen in your browser (redirect: {redirect_uri}) ...")
    print(f"If it doesn't open, paste this URL manually:\n\n{auth_url}\n")
    webbrowser.open(auth_url)

    # Handle exactly one request — the OAuth redirect — then stop.
    server.handle_request()
    server.server_close()

    if server.oauth_error:  # type: ignore[attr-defined]
        raise SystemExit(f"OAuth consent returned an error: {server.oauth_error}")  # type: ignore[attr-defined]
    code = server.oauth_code  # type: ignore[attr-defined]
    if not code:
        raise SystemExit("No authorization code received from the redirect.")
    return str(code)


def _exchange_code(
    *, code: str, client_id: str, client_secret: str, redirect_uri: str
) -> dict:
    """Exchange the one-time code for tokens at Google's token endpoint."""
    data = urllib.parse.urlencode(
        {
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }
    ).encode()
    req = urllib.request.Request(_TOKEN_ENDPOINT, data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 — fixed Google URL
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:  # pragma: no cover — network path
        body = exc.read().decode(errors="replace")
        raise SystemExit(f"Token exchange failed (HTTP {exc.code}): {body}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Mint a Google Ads API refresh token via a loopback OAuth flow."
    )
    parser.add_argument(
        "--client-id",
        default=os.environ.get("GADS_CLIENT_ID"),
        help="OAuth client id (default: $GADS_CLIENT_ID).",
    )
    parser.add_argument(
        "--client-secret",
        default=os.environ.get("GADS_CLIENT_SECRET"),
        help="OAuth client secret (default: $GADS_CLIENT_SECRET).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Loopback port for the redirect URI (must match the GCP OAuth "
        "client for Web app clients; any port works for Desktop clients). "
        "Default 8080.",
    )
    parser.add_argument(
        "--out",
        default=_DEFAULT_OUT,
        help=f"File to write the refresh token to, mode 0600 "
        f"(default: {_DEFAULT_OUT}). Ignored when --print is given.",
    )
    parser.add_argument(
        "--print",
        dest="print_token",
        action="store_true",
        help="Print the refresh token to stdout instead of writing a file "
        "(NOT recommended — it lands in terminal scrollback).",
    )
    args = parser.parse_args(argv)

    if not args.client_id or not args.client_secret:
        parser.error(
            "client id and secret are required — pass --client-id/--client-secret "
            "or set GADS_CLIENT_ID / GADS_CLIENT_SECRET."
        )

    redirect_uri = f"http://localhost:{args.port}/"
    # `state` guards against a stray/forged redirect hitting the local server.
    state = secrets.token_urlsafe(16)
    auth_url = _AUTH_ENDPOINT + "?" + urllib.parse.urlencode(
        {
            "client_id": args.client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": _SCOPE,
            # offline + consent are what actually return a refresh token (and
            # force one even if the user previously consented).
            "access_type": "offline",
            "prompt": "consent",
            "state": state,
        }
    )

    code = _capture_code(redirect_uri, auth_url, args.port)
    tokens = _exchange_code(
        code=code,
        client_id=args.client_id,
        client_secret=args.client_secret,
        redirect_uri=redirect_uri,
    )

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        # Most common cause: the user already granted consent and Google
        # withheld a new refresh token. `prompt=consent` above should prevent
        # this, but surface a clear message just in case.
        print(
            "No refresh_token in the response. Revoke the app's access at "
            "https://myaccount.google.com/permissions and re-run.",
            file=sys.stderr,
        )
        return 1

    if args.print_token:
        print("\n" + "=" * 64)
        print("SUCCESS — your Google Ads refresh token:\n")
        print(refresh_token)
        print("=" * 64)
        return 0

    # Default: write to a 0600 file and never echo the value.
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(refresh_token + "\n")
    out_path.chmod(stat.S_IRUSR | stat.S_IWUSR)  # 0600 — owner read/write only

    print("\n" + "=" * 64)
    print("SUCCESS — refresh token written (mode 0600) to:\n")
    print(f"    {out_path}")
    print("\nFeed it to the connector by reading the file into the env var:\n")
    print(f"    export GADS_REFRESH_TOKEN=\"$(cat {out_path})\"")
    print("\nMake sure the path is git-ignored (add it to .gitignore if needed).")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
