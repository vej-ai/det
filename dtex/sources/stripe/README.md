# Stripe — baked source connector

One Stripe connector, **two extraction surfaces**:

* **REST** (default) — resource-as-stream over Stripe's GA REST API at
  `https://api.stripe.com/v1`. One stream per resource type
  (`charges`, `invoices`, `customers`, `subscriptions`), incremental on
  the Unix `created` timestamp, paginated via Stripe's cursor model
  (`starting_after` + `has_more`). Cheap, near-live, no extra
  subscription.
* **Sigma SQL** (opt-in per stream) — query-as-stream over Stripe's
  Sigma API. Each Sigma stream's `register.yaml` entry carries a
  `sigma: {query: queries/<name>.sql}` block; the connector reads the
  SQL body, submits it via `POST /v2/data/reporting/query_runs`, polls
  to completion, downloads the CSV result, and yields batches. Lets a
  single stream JOIN/aggregate across Stripe's internal tables in
  Stripe-side compute. Requires a **paid Sigma subscription** and
  carries a **~3-hour data lag**.

The two surfaces share one source folder, one set of connector params,
and one `api_key` — a single Stripe restricted key with the relevant
read scopes drives both. Configs mix REST and Sigma streams freely.

## Authentication

The connector reads a **restricted Stripe secret key** with **read-only
scopes** from the `STRIPE_API_KEY` environment variable:

```sh
export STRIPE_API_KEY="rk_live_..."
```

Recommended restricted-key scopes:

| Surface | Resource | Scope |
|---|---|---|
| REST | `/charges` | `charge:read` |
| REST | `/invoices` | `invoice:read` |
| REST | `/customers` | `customer:read` |
| REST | `/subscriptions` | `subscription:read` |
| Sigma | `POST /v2/data/reporting/query_runs` | `sigma:read`, `reporting:read` |

If your config only uses REST streams you don't need the Sigma scopes,
and vice versa.

The key is sent as `Authorization: Bearer <key>` on every request. It is
**never logged** — the `Authorization` header is redacted at each client's
single logging choke point.

## Supported resources (v1)

Four streams, all `write_disposition: merge` on `primary_key: id`:

- **`charges`** → `stripe_charges`
- **`invoices`** → `stripe_invoices`
- **`customers`** → `stripe_customers`
- **`subscriptions`** → `stripe_subscriptions`

Each declares an explicit schema covering the common columns (`id`, `object`,
`created`, `livemode`, …) plus the resource-specific scalars. Nested
sub-objects (e.g. `payment_method_details`, `lines`, `items`, `metadata`)
land as `JSON` columns — the dtex convention for nested data
(docs/04 §"Nested data").

## Incremental model

Every stream uses:

```yaml
incremental:
  cursor_field: created
  cursor_type: int          # Stripe `created` is a Unix-timestamp integer
  lookback: 6h              # declared hint
  initial_value: "1704067200"   # 2024-01-01 UTC
```

On each run the connector translates the engine's `cursor.start_value()` into
a `created[gte]=<ts>` query parameter, walks every page with
`starting_after=<last_id>`, and `cursor.observe()`s each record so the engine
can advance the cursor.

Stripe lists are returned **newest first** within the new-records window;
`cursor.observe()` takes the *max*, so the final cursor is correct regardless
of arrival order. Per dtex's commit model the cursor is committed only
after every batch durably lands, so a mid-stream crash safely re-runs the
whole window — no lost or skipped rows.

`merge` write disposition means re-fetched objects (an invoice whose status
changed after it was first loaded) upsert in place, so a periodic
`lookback`-style re-fetch is idempotent.

## Usage

`register.yaml` declares the connector; the project binds it to a
destination via its `destination:` block or via the project-level
`default_destination`. Running it is the standard dtex one-liner:

```python
import dtex

result = dtex.run(
    connector="stripe",
    target="prod",
    # Run a single stream:
    select=("charges",),
    # Or override params per invocation:
    params={"page_size": 100, "requests_per_second": 50},
)
print(result.status, result.rows_loaded)
```

CLI equivalent:

```sh
dtex run stripe --target prod --select charges
```

## Operational notes

- **Rate limit.** Default `requests_per_second: 25` is conservative against
  Stripe's documented ~100/sec live-mode limit. Tune up via `params` if you
  control the account's throttling budget.
- **Retries.** `429` honors `Retry-After` exactly; `5xx` retries with
  exponential backoff up to `max_retries: 5`; other `4xx` (`401`, `403`,
  `404`) raise immediately — a bad request will not get better on its own.
- **Dependencies.** `requests>=2.31` (declared in `pyproject.toml`); no
  Stripe SDK — raw HTTP keeps the dependency surface minimal and avoids
  version coupling to the SDK's release cadence.
- **API version pin.** `api_version` defaults to `2024-12-18.acacia`; see
  [docs/connectors/stripe-research.md](../../../docs/connectors/stripe-research.md)
  for the source of that pin and where to check for the current GA.

## Sigma SQL streams

Three Sigma streams ship in `register.yaml`:

- **`charges_daily`** → `charges` (incremental on `created`)
- **`subscriptions_active`** → `subscriptions` (full refresh)
- **`invoices_paid`** → `invoices` (incremental on `status_transitions_paid_at`)

The SQL bodies live under `queries/<stream_name>.sql`. The connector binds
the `{since}` placeholder from each stream's `Cursor.start_value()` (or
the connector-level `sigma_initial_since` for non-incremental streams) as a
Presto `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` literal — Sigma rejects ISO-8601's
`T` separator and timezone suffixes.

### Adding a 4th Sigma stream

Two files + one function:

1. Drop your query at `queries/<new_stream>.sql`. Use `{since}` if it's
   incremental.
2. Append a stream to `register.yaml` declaring the schema and the
   `sigma:` block:
   ```yaml
   - name: my_new_stream
     table: my_new_table
     primary_key: id
     write_disposition: merge
     sigma:
       query: queries/my_new_stream.sql
     incremental:
       cursor_field: <col>
       cursor_type: timestamp
       initial_value: "2024-01-01T00:00:00Z"
     schema:
       - {name: id,        type: STRING, mode: REQUIRED}
       - {name: <col>,     type: TIMESTAMP, mode: REQUIRED}
       - ...
   ```
3. Add a one-line `@stream` wrapper in `source.py` that delegates to
   `_extract_sigma_stream` (the existing functions show the pattern).

### Operational notes (Sigma-specific)

- **Account ID required.** The config must set `params.account_id` to the
  `acct_...` ID of the Stripe account being queried. Stripe rejects every
  Sigma request with HTTP 403 otherwise.
- **API version pin.** `sigma_api_version` defaults to
  `2026-04-22.preview`; the Query Run API is preview. Bump deliberately.
- **Polling.** `sigma_poll_interval_seconds` (default `2.0`) is the gap
  between status polls; `sigma_poll_timeout_seconds` (default `600`) is the
  hard ceiling per query. Heavy queries on large accounts approach the
  ceiling; bump if needed.
- **Download retries.** A truncated CSV mid-body retries the whole download
  up to `max_retries` (default `5`) with exponential backoff
  (`retry_backoff_seconds`, default `1.0`). Duplicate-free because no rows
  are yielded until the full CSV is in hand.
