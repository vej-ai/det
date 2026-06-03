---
name: dtex-write-connector
description: Author or edit a dtex source connector (sources/<name>/). Use whenever the user is creating, editing, or asking about a source — including questions about `register.yaml`, `@stream`, `cursor.observe`, schemas, incremental cursors, secrets, or pagination. Also covers destinations at a higher level.
---

# Authoring a dtex source connector

A **source connector** is a folder under `sources/<name>/` with this shape:

```
sources/<name>/
  __init__.py        # marker — makes the folder an explicit Python package
  register.yaml      # the declarative contract
  source.py          # @stream functions (and any helpers)
  client.py          # optional — HTTP client / SDK wrapper
  queries/           # optional — .sql files for SQL-as-stream sources
```

Scaffold with `dtex new source <name>`.

## The three-file separation of concerns

Keep these layers separate. Mixing them produces hard-to-test code.

### `register.yaml` — the declarative contract

What the connector IS — name, version, parameters, secrets, streams, schemas.
Pure declaration, no logic. The engine validates against this at discovery
time before any HTTP calls happen.

### `client.py` — the HTTP / SDK wrapper

HTTP concerns and nothing else. Knows about auth headers, pagination,
retries, rate-limiting. Does NOT import from `dtex`. Does NOT know which
streams exist. Easy to unit-test by mocking `requests`.

### `source.py` — the `@stream` functions

The glue between `client.py` and the engine. One function per stream
declared in `register.yaml`, each decorated with `@stream(name="...")`.
Constructs the client from `config.secrets`, calls `cursor.observe(...)`
per record, yields batches.

## The `register.yaml` schema

```yaml
name: my_source
kind: source
version: "0.1.0"
summary: One-line description of what this extracts.
tags: []

params:
  base_url:
    type: string
    default: "https://api.example.com"
    description: API base URL.
  page_size:
    type: int
    default: 1000
  account_id:
    type: string
    required: true       # no default; config must supply

secrets:
  - name: api_key
    ref: ${env.MY_SOURCE_API_KEY}
    # or: secret://gcp-secret-manager/projects/X/secrets/Y/versions/latest

streams:
  - name: customers
    table: customers
    primary_key: id
    write_disposition: merge   # append | merge | replace
    incremental:
      cursor_field: updated_at
      cursor_type: timestamp   # timestamp | date | integer | string
      initial_value: "2024-01-01T00:00:00Z"
      lookback: 6h             # optional — overlap to catch late-arriving data
    schema:
      - {name: id,         type: STRING, mode: REQUIRED}
      - {name: updated_at, type: TIMESTAMP, mode: REQUIRED}
      - {name: email,      type: STRING}
```

### Stream rules

- **`primary_key` is required for `write_disposition: merge`.** Without one,
  use `append`.
- **`incremental.cursor_field` MUST be a column the stream yields** AND
  MUST be in the `schema:` block. Otherwise `cursor.observe()` reads `None`
  and state never advances.
- **`schema` is optional** but recommended. Without it the engine infers
  from the first batch. With it the engine type-coerces every value via
  the NORMALIZE step (string `"1599"` → `INTEGER 1599`, ISO timestamps →
  `datetime`, etc.).
- **`write_disposition: replace` deletes the prior table contents** every
  run. Best for small full-refresh streams (e.g. a small lookup table).
  Big or partitioned tables want `merge` with `primary_key`.

## The `@stream` function

```python
from collections.abc import Iterator
from dtex import Batch, Config, Cursor, State, stream
from .client import MyClient


@stream(name="customers")
def customers(config: Config, state: State, cursor: Cursor, log) -> Iterator[Batch]:
    client = MyClient(
        api_key=config.secrets["api_key"],
        base_url=str(config.get("base_url")),
    )
    params = {"limit": int(config.get("page_size") or 1000)}
    since = cursor.start_value()
    if since:
        params["updated_at_gte"] = since   # check the API's exact filter name

    batch: list[dict] = []
    for row in client.paginate("/customers", params):
        cursor.observe(row.get("updated_at"))   # MUST happen per record
        batch.append(row)
        if len(batch) >= 500:
            yield batch
            batch = []
    if batch:
        yield batch
```

### Injected arguments

The engine declares which arguments it can inject — `config`, `state`,
`cursor`, `log`, `stream_def` — and supplies only the ones the function
names. List only what you use; the engine matches by name.

- **`config`** — the resolved `Config` for this run. Read params via
  `config.get("key")` or attribute access. Read secrets via
  `config.secrets["key"]`.
- **`cursor`** — for incremental streams only. Drop the arg if the stream
  has no `incremental:` block.
- **`state`** — the per-stream JSON state blob. Use for stateful streams
  that need to remember more than a cursor.
- **`log`** — a standard logging-style logger. `log.info(...)`,
  `log.warning(...)`.
- **`stream_def`** — the parsed `StreamDef` for this stream — its own
  `register.yaml` entry. Lets a connector introspect its own
  declaration. Useful when one connector serves more than one
  extraction surface (REST + Sigma SQL, GA + Reporting API, etc.) and
  the surface choice is declared in the manifest. See "Dual-API
  connectors" below.

### Three rules to drill in

**1. `cursor.observe(...)` is mandatory for incremental streams.**
Forget it and the cursor never advances. Every run pulls everything
since `initial_value` again. The engine commits `cursor.observed_max`
as the new state after each batch lands.

**2. The engine commits state per batch, not per stream.** Smaller
batches mean finer-grained recovery on failure (a 3-batch stream that
fails on batch 3 keeps batches 1 and 2 committed). 500 records is a
reasonable default.

**3. Flatten nested API responses in the connector, not the destination.**
The engine's NORMALIZE step coerces *values* to the declared FieldType,
but it does not reshape nested dicts into flat columns. If the API returns
`{"attributes": {"country": "US"}}` and your schema declares
`country: STRING`, you must flatten in the `@stream` function before yielding.

## Dual-API connectors — when one source serves two surfaces

Some APIs come in two flavors that the same operator pulls from in one
pipeline. Stripe is the canonical example: a standard REST API
(resource-as-stream, GA) AND a Sigma SQL API (query-as-stream, paid,
~3-hour lag). Both belong to one "Stripe" connector, not two.

The dtex pattern is **one source folder, two extraction surfaces, per-stream
opt-in**. The baked `stripe` connector demonstrates this:

```yaml
# dtex/sources/stripe/register.yaml — abbreviated
streams:
  - name: charges            # REST — no `sigma:` block
    table: stripe_charges
    incremental: {cursor_field: created, cursor_type: int}
    schema: [...]

  - name: charges_daily      # Sigma — opted in via `sigma:` block
    table: charges
    sigma:
      query: queries/charges_daily.sql
    incremental: {cursor_field: created, cursor_type: timestamp}
    schema: [...]
```

In `source.py`, each `@stream` function knows which surface it belongs
to by which helper it calls. The Sigma functions take the `stream_def`
injectable, read `stream_def.sigma.query`, and load the SQL from the
named file:

```python
@stream(name="charges_daily")
def charges_daily(
    stream_def: StreamDef, config: Config, cursor: Cursor, log
) -> Iterator[Batch]:
    yield from _extract_sigma_stream(stream_def, config, cursor, log)

# _extract_sigma_stream(stream_def, ...) reads stream_def.sigma.query
# from the connector folder, binds {since} from the cursor, submits to
# Sigma, downloads CSV, yields batches.
```

The decision rules:

* **One connector, not two.** Users binding `source: stripe` see one
  surface; the `sigma:` block is the opt-in marker per stream. Don't
  fork into `stripe` and `stripe_sigma`.
* **Sigma streams declare `sigma: {query: <path>}`.** Path is relative
  to the connector folder. The `@stream` function reads it from
  `stream_def.sigma.query` (via the `stream_def` injectable) and
  loads the SQL.
* **REST streams don't change.** No `sigma:` block, no `stream_def`
  arg required — existing connectors stay shaped exactly as they were.
* **Shared client params, shared auth.** A single Stripe restricted
  key with both REST + Sigma scopes drives both surfaces from one
  `api_key` secret. Keep it that way; don't introduce per-surface
  secrets unless the API genuinely demands separate keys.

The same pattern fits other APIs with a "live REST + analytical SQL"
shape — Shopify GraphQL + Shopify Bulk Operations, Salesforce REST +
SOQL, GA Data API + BigQuery export, etc. The `sigma:` block is
Stripe-specific naming today; future dual-surface connectors should
either reuse it (if they have similar SQL semantics) or add a parallel
marker (`bulk:`, `soql:`) following the same opt-in shape.

## The `client.py` pattern

```python
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any
import time
import requests


@dataclass
class MyClient:
    api_key: str
    base_url: str = "https://api.example.com"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict]:
        """Yield every item from a paginated list endpoint."""
        url = f"{self.base_url}{path}"
        query = dict(params or {})
        while True:
            resp = requests.get(url, headers=self._headers(), params=query, timeout=60)
            if resp.status_code == 429:
                time.sleep(float(resp.headers.get("Retry-After", "2")))
                continue
            resp.raise_for_status()
            body = resp.json()
            for item in body.get("items", []):
                yield item
            nxt = body.get("next_page")
            if not nxt or not nxt.get("starting_after"):
                return
            query["starting_after"] = nxt["starting_after"]
```

Things the client should NOT do:
- Import from `dtex`
- Know which streams exist
- Apply business logic

## Project-local connectors — multi-file rules

A source folder can split helpers into sibling files (`client.py`,
`helpers.py`, etc.) and use relative imports (`from .client import X`).
The folder MUST contain `__init__.py` for the engine's package loader
to discover it. `dtex new source` scaffolds this for you.

## Secrets — the two forms

In `register.yaml`:

```yaml
secrets:
  - name: api_key
    ref: ${env.MY_KEY}                  # env-var form — works without extras
  - name: db_password
    ref: secret://gcp-secret-manager/projects/X/secrets/Y/versions/latest
```

The `${env.X}` form requires nothing extra. The `secret://` form requires
the matching extra: `pip install 'dtex[gcp-secrets]'` (or `[aws-secrets]`
or `[vault]`).

The resolver runs once per run and registers the value with the per-run
Redactor — secrets never appear in stdout, JSONL logs, or run records.

## Discovery-time validation

`dtex validate` runs every check that doesn't need a network call:
schema shape, decorator signatures, register.yaml ↔ source.py
agreement, manifest typing. Run this BEFORE `dtex run` whenever you
edit `register.yaml` or `source.py` — it's faster than a real run and
catches the same authoring errors.

## Common stumbling blocks

| Symptom | Cause | Fix |
|---|---|---|
| `attempted relative import with no known parent package` | Missing `__init__.py` in the connector folder | Create an empty `__init__.py` |
| `ArrowInvalid: Could not convert ...` | A column the API returns as a nested dict is declared as a flat type | Flatten in the connector before yielding |
| State doesn't advance between runs | Forgot `cursor.observe(...)` inside the per-row loop | Add it |
| First run is huge and slow | `initial_value` too far back | Bump it; in dev use `since:` in the config for a tighter floor |
| HTTP 401 unauthorized | Wrong key or wrong API version | Check the API docs; for v2 APIs, ensure the v2 key is used |
