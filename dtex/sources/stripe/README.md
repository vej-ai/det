# Stripe — baked source connector

Extracts data from [Stripe](https://stripe.com)'s standard REST API at
`https://api.stripe.com/v1`. One stream per resource type
(`charges`, `invoices`, `customers`, `subscriptions`), incremental on the
Unix `created` timestamp, paginated via Stripe's cursor model
(`starting_after` + `has_more`).

## Authentication

The connector reads a **restricted Stripe secret key** with **read-only
scopes** from the `STRIPE_API_KEY` environment variable:

```sh
export STRIPE_API_KEY="rk_live_..."
```

Recommended restricted-key scopes for the v1 streams:

| Resource | Scope |
|---|---|
| `/charges` | `charge:read` |
| `/invoices` | `invoice:read` |
| `/customers` | `customer:read` |
| `/subscriptions` | `subscription:read` |

The key is sent as `Authorization: Bearer <key>` on every request. It is
**never logged** — the `Authorization` header is redacted at the client's
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

## What's not in v1 (deferred)

The Stripe **Sigma API** (`POST /v2/data/reporting/query_runs`) lets you run
SQL against ~100+ Stripe objects. It is:

- **preview**: `Stripe-Version: 2026-04-22.preview`;
- **paid**: requires an active **Sigma subscription**;
- **lagged**: data freshness ~3 hours;
- async: POST → poll → download CSV from a 5-minute-TTL URL.

Because most users lack a Sigma subscription and the API is preview, v1 ships
**only** the resource-as-stream model. A future opt-in
`sigma_query`-typed stream is sketched in
[docs/connectors/stripe-research.md](../../../docs/connectors/stripe-research.md)
§"How query-as-stream would map"; see the locked design decision there.
