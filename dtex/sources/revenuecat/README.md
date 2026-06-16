# RevenueCat — baked source connector

Extracts data from [RevenueCat](https://www.revenuecat.com)'s v2 API at
`https://api.revenuecat.com/v2`. Three streams covering RC's three
distinct extraction surfaces:

* **`customers`** — every customer in a project. NON-incremental.
* **`subscriptions`** — per-customer subscriptions. NON-incremental,
  fan-out across the customer list.
* **`metrics_daily`** — daily chart metrics (revenue, MRR, churn, etc.).
  REAL incremental on cohort date, long-format output.

The connector targets v2 specifically. v2 keys are **separate** from v1
keys and the two APIs are not interchangeable.

## Authentication

The connector reads a **v2 RevenueCat secret key** (`sk_...`) from the
`REVENUECAT_API_KEY` environment variable by default:

```sh
export REVENUECAT_API_KEY="sk_..."
```

Required scopes (set on the key in the RC dashboard):

| Stream | Scope |
|---|---|
| `customers` | `customer_information:customers:read` |
| `subscriptions` | `customer_information:subscriptions:read` |
| `metrics_daily` | `charts_metrics:overview:read` |

For production, override the secret ref per profile (`profiles.yml`)
with any resolver-backed `secret://` URL — GCP Secret Manager, AWS
Secrets Manager, or HashiCorp Vault:

```yaml
# profiles.yml
revenuecat:
  default_target: prod
  targets:
    prod:
      api_key: secret://gcp-secret-manager/projects/<proj>/secrets/<name>/versions/latest
```

(Requires the matching extra: `pip install 'dtex[gcp-secrets]'` /
`[aws-secrets]` / `[vault]`.)

The key never appears in log output — it's set on the `requests.Session`
header once and the connector's logging emits no header contents.

## The three streams

### `customers` — non-incremental

RC v2's `/customers` endpoint has **NO server-side date filter**
(verified against the docs + the airbyte issue 70315 + the RC community
forum, June 2026). Every run paginates the full customer list. The
prior v1 design that filtered client-side had a subtle data-loss bug
because RC does not guarantee response ordering; the baked connector
deliberately drops the `incremental:` block to avoid the same trap.
`write_disposition: merge` on `id` makes re-pulls upsert idempotently;
the cost is HTTP calls, not duplicate rows.

For a 195k-customer account, expect ~70 minutes wall time at the
default page_size of 100 (RC's customer-domain rate limit is 480
req/min; the practical floor is ~3 min/100k rows).

### `subscriptions` — non-incremental, per-customer fan-out

RC has no project-level `/subscriptions` endpoint, only
`/customers/{id}/subscriptions`. This stream iterates the customer list
and fetches per-customer subscriptions. Operationally expensive:
O(N+1) HTTP calls per run for N customers. On large accounts
this is the heaviest stream by far.

### `metrics_daily` — real incremental

RC v2's `/charts/{chart_name}` endpoint DOES take server-side
`start_date`/`end_date` filters and tags each per-day value with
`incomplete=true` when the day is still finalizing. This stream:

1. Computes `start_date = max(cursor.start_value(), today - lookback_days)`.
2. For each chart in `metrics_charts`, fetches the date range with
   `resolution=day`.
3. Flattens the response's `measures[]` × `values[]` arrays into
   long format: one row per `(cohort_date, chart_name, measure_name)`.
4. Observes the cursor only against rows where `incomplete=false`, so
   today's still-incomplete value gets re-pulled on the next run with
   the corrected (final) value.

Default charts: `revenue`, `mrr`, `actives`, `trials`. Override via
the `metrics_charts` param (comma-separated). Adding a new chart is
zero schema migration — it lands as new rows in the same long-format
table.

The `cohort_explorer` and `prediction_explorer` charts return a
different shape and would break the flattener; exclude them unless
you fork the connector.

## Config

A minimum config looks like this:

```yaml
# configs/revenuecat_bq.yml
name: revenuecat_bq
source: revenuecat
destination: bigquery
target: prod
params:
  project_id: "proj1ab2c3d4"          # required, no default
destination_params:
  dataset: revenuecat
streams: all                          # or list specific streams
```

To narrow to just the cheap stream:

```yaml
streams:
  metrics_daily:
```

## Rate limits and timeouts

* `/customers` and `/subscriptions` share the **Customer Information**
  domain with a 480 req/min default cap.
* `/charts/*` is in the **Charts & Metrics** domain with a 15 req/min
  cap — much tighter; 4 charts × ~7 days per run hits this fine.
* Client uses a `(10s connect, 60s read)` timeout to prevent the
  stale-socket hangs the prior version exhibited.
* Retries cover: HTTP 5xx (capped, exponential backoff), HTTP 429
  (honors `Retry-After`, also capped), and the network-exception
  family (Timeout, ConnectionError, ChunkedEncodingError). All capped
  by `max_retries`; the prior version had two uncapped retry paths
  that wedged on persistent rate-limits or dead sockets.

## Operational notes

- **First run on a populated account is long.** Plan for ~70 min wall
  time on ~200k-customer accounts. Subsequent runs are not faster
  unless you switch to `metrics_daily` only.
- **No incremental shortcut exists for customers.** RC's official
  guidance for "I need fresh data" is their S3/GCS export (a separate
  paid feature). The connector documents this honestly rather than
  pretending a client-side filter is incremental.
- **Long backfills need dtex 0.2.2+.** Earlier dtex BigQuery destinations
  could fail mid-run on a stale TCP socket; 0.2.2 fixed it.

## What's not in v1 of this connector

- `cohort_explorer` and `prediction_explorer` chart types (different
  response shape).
- Webhooks (no v2 endpoint at the time of writing).
- Transactions / activities / payments lists (RC doesn't expose them
  at the project level in v2 yet; staff has the feature request
  open).
- `non-subscription_purchases` chart (currently excluded from the
  defaults but should work — try it).
