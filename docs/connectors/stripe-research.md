# Stripe Connector — Research & Design Note

**Status:** Research complete · **Date:** 2026-05-22 · **Audience:** det connector authors

This note answers one question the owner raised: *does Stripe Sigma now accept
SQL-style queries via an API, so a connector could expose "custom queries as
streams" the way Google Ads GAQL does?* Short answer below, detail follows.

---

## Summary

- **Does a programmatic SQL query API exist? — YES, but in PREVIEW.** Stripe
  shipped the **Sigma API** (a.k.a. the v2 Reports *Query Run* API). You `POST`
  a SQL string, poll a `QueryRun` object until `status=succeeded`, then download
  a CSV from a short-lived URL. It is gated behind a paid **Sigma subscription**
  and is currently a **preview API** (`Stripe-Version: 2026-04-22.preview`) —
  not General Availability.
- **The owner's belief is essentially correct** — "custom SQL queries as
  streams" is now technically possible, analogous to GAQL. But the preview
  status + subscription gate mean it should **not** be the v1 default.
- **Recommended v1 design: resource-as-stream over the standard REST API.**
  One stream per Stripe object type (`charges`, `invoices`, `customers`,
  `subscriptions`, …), incremental on `created` via `created.gte` + cursor
  pagination. The REST API is GA, free, JSON-native, and near-real-time. The
  Sigma query-as-stream model is a **buildable optional second stream type** —
  ship it later, behind a subscription flag — so the connector can support
  **both**.

---

## API capabilities

Two distinct Stripe surfaces are relevant. They are independent products.

### A. The Sigma API — programmatic SQL query (PREVIEW)

This is the new capability. Stripe Sigma was historically a dashboard-only SQL
product; the Sigma API exposes the same query engine over HTTP.

**Endpoints** (API v2 — note the `/v2/` path and the `.preview` version):

| Operation | Method & path |
|---|---|
| Create a query run | `POST https://api.stripe.com/v2/data/reporting/query_runs` |
| Retrieve a query run | `GET https://api.stripe.com/v2/data/reporting/query_runs/{id}` |

**Auth:**
- `Authorization: Bearer <secret_key>` — a standard or restricted secret key.
- The key needs the **`reporting_write`** and **`sigma_api_write`** permissions
  (shown as "Sigma" and "Sigma API" when configuring a restricted key in the
  Dashboard).
- **`Stripe-Version: 2026-04-22.preview`** header is **required** — this is a
  preview API and will not respond on the stable version pin.
- Optional `Stripe-Context: {context_id}` header scopes an organization key to
  a single connected account; omitting it on an org key queries all direct
  accounts (an `account` column becomes available in results).
- Requires an **active Sigma subscription** on the account.

**Request body** (`POST .../query_runs`):

```json
{
  "sql": "SELECT id, amount, currency FROM balance_transactions LIMIT 10",
  "result_options": {
    "result_type": "file",
    "compress_file": false
  }
}
```

- `sql` (required) — a SQL statement in the **same syntax as the Sigma query
  editor**. Stripe documents this as **Trino SQL**. Sigma exposes ~100+ tables
  mirroring Stripe API objects (`charges`, `invoices`, `subscriptions`,
  `customers`, `balance_transactions`, …).
- `result_options.compress_file` (optional) — `true` for zip-compressed output.
- `result_options.result_type` — `"file"` (CSV).

**Async / polling model:**

1. `POST` returns a `QueryRun` object immediately with `status=running` and
   `result=null`.
2. Poll `GET .../query_runs/{id}` until `status` becomes `succeeded` or
   `failed`.
3. On `succeeded`, the `result` field is populated (see object shape below).
4. **Webhook alternative to polling:** Stripe emits
   `v2.data.reporting.query_run.succeeded` and
   `v2.data.reporting.query_run.failed` events. Webhooks need a public HTTP
   endpoint, which a CLI-invoked EL tool does not have — **polling is the
   realistic path for det.**

**The `QueryRun` object:**

```json
{
  "id": "qryrun_123",
  "object": "v2.data.reporting.query_run",
  "created": "2026-05-22T01:02:29.964Z",
  "sql": "SELECT ...",
  "status": "running | succeeded | failed",
  "result_options": { "result_type": "file", "compress_file": false },
  "result": null,
  "livemode": false
}
```

When `status=succeeded`, `result` is:

```json
"result": {
  "type": "file",
  "file": {
    "content_type": "csv",
    "size": "512",
    "download_url": {
      "url": "https://stripeusercontent.com/files/...",
      "expires_at": "2026-05-22T01:10:46.679Z"
    }
  }
}
```

> **Endpoint-shape caveat:** the exact JSON field names above
> (`result.file.download_url.url`, `expires_at`, `size`) are taken from Stripe's
> doc examples. They are a **preview** API and may change. The connector must
> not hard-assume them long-term — re-verify against live API responses at
> build time. The reference page that did not resolve cleanly during research
> was `docs.stripe.com/api/sigma/sigma-query-run/object` (HTTP 404 on direct
> fetch); the field set here is reconstructed from `docs/reports/query-runs` and
> `docs/stripe-data/sigma-api`, which did resolve.

**Result format & limits:**
- Result is a **CSV file** at a download URL that **expires in ~5 minutes** —
  the connector must parse-on-download, not defer. If the URL expires, re-`GET`
  the `QueryRun` to mint a fresh URL.
- Max result file size: **5 GB** (use `compress_file` or partition the query by
  date range to stay under it).
- Query execution timeout: **90 minutes**.
- Concurrency: **500** running query runs (livemode) / **100** (testmode) per
  account or organization.
- Result retention: **90 days**.
- Standard API v2 rate limits apply.
- **Data freshness: ~3 hours.** Sigma is not real-time — most transaction data
  is queryable ~3h after it occurs; derived datasets lag 24–120h. A `created`
  filter on "now" returns nothing useful. Sigma also exposes a `data_load_time`
  SQL variable marking the last processed timestamp.

**Cost model:** Not documented per-query. Sigma is a paid subscription product;
Stripe does not publish a public per-query price for the API. **Open question.**

There is also a **Scheduled Queries** concept in Sigma (queries that run on a
Stripe-side schedule and deliver results). This is largely superseded for our
purposes by the Query Run API + det's own `schedule:` hint, and is not part
of the recommended design.

### B. The standard REST API — resource extraction (GA)

This is the long-standing, stable path and the basis for the v1 connector.

- **List endpoints:** `GET https://api.stripe.com/v1/charges`,
  `/v1/invoices`, `/v1/customers`, `/v1/subscriptions`, `/v1/refunds`,
  `/v1/payment_intents`, `/v1/balance_transactions`, etc.
- **Auth:** HTTP Basic with the secret key as username
  (`-u "sk_live_...:"`), or `Authorization: Bearer sk_...`. A **restricted key
  with read scopes** is the right choice for an EL tool. No subscription
  required.
- **Incremental filter:** every list endpoint accepts a `created` parameter
  with `created.gt`, `created.gte`, `created.lt`, `created.lte`
  sub-parameters (Unix timestamps, seconds). Incremental extraction =
  `created.gte=<last_cursor>` each run.
- **Pagination — cursor-based:**
  - `limit` — 1–100, default 10. Use 100.
  - Response is `{"object": "list", "has_more": <bool>, "data": [...]}`.
  - To page forward: take the **last** object's `id` from `data`, pass it as
    `starting_after` on the next request. Repeat while `has_more == true`.
  - `ending_before` pages backward; mutually exclusive with `starting_after`.
- **Sort order:** list endpoints return **newest first** (reverse
  chronological). See the gotcha note in the design section below.
- **Response format:** JSON. No async, no polling, no file download — immediate.
- Official SDKs (`stripe-python`, etc.) provide **auto-pagination iterators**.
- The Search API (`GET /v1/charges/search` with Stripe's search query
  language) is a third option — more expressive filtering, but a different
  pagination model and eventual-consistency caveats. **Out of scope for v1.**

---

## Recommended connector design for det

### Verdict: resource-as-stream for v1; query-as-stream as an opt-in extension

| Model | Buildable in v1? | Why |
|---|---|---|
| **Resource-as-stream** (one stream per object type, incremental on `created`) | **Yes — ship this.** | REST API is GA, free, JSON-native, near-real-time, well-documented. Maps cleanly onto the det contract: each object type = one `streams[]` entry with an `incremental` block; one `@stream` generator paginates it. No subscription gate — works for every det user. |
| **Query-as-stream** (each stream = a user SQL query, GAQL-style) | **Buildable, but not v1.** | The Sigma API genuinely enables this. But it is **preview** (API may change), requires a **paid Sigma subscription** (most users lack it), and has **3h data lag**. Shipping it as the default would make the baked connector fail for the majority of users out of the box. |

**The connector can support BOTH** — they are not mutually exclusive. The
det contract already allows a connector to declare a heterogeneous set of
streams, and stream-scoped `params` let one stream carry a SQL string while
others carry nothing. Plan:

- **v1:** `resource-as-stream` only. Pre-declared streams for the common
  objects, incremental on `created`.
- **v1.x (later):** add an optional `sigma_query`-type stream. The user supplies
  the SQL via stream-scoped `params` (`sql:` + `cursor_column:`), exactly
  analogous to defining a GAQL query per report. Gate it behind a connector
  param like `enable_sigma: false` so it is inert unless the user has a Sigma
  subscription and opts in.

### How resource-as-stream maps to the contract

- **One `@stream` generator per object type.** Each calls the matching
  `GET /v1/<object>` endpoint.
- **Incremental:** `incremental.cursor_field: created`, `cursor_type:
  timestamp`. The generator reads `cursor.start_value()`, converts it to a Unix
  timestamp, and passes `created.gte=<ts>`. It calls `cursor.observe(record["created"])`
  for every record so the engine tracks the max.
- **Pagination:** loop — request with `limit=100` and `starting_after=<last id>`;
  yield a batch; continue while `has_more`.
- **Write disposition:** `merge` on `primary_key: id`. Stripe objects are
  mutable (an invoice's status changes after creation), so `merge` upserts the
  latest version. Pure `append` would duplicate.
- **Reverse-chronological gotcha.** Stripe lists newest-first. With
  `created.gte=<cursor>` and forward `starting_after` paging, the connector
  walks **newest → oldest within the new-records window**. `cursor.observe()`
  takes the **max**, so the final cursor is correct regardless of arrival
  order. The det engine only commits the cursor *after* all batches durably
  land (per chapter 03 §3.2), so a crash mid-stream safely re-runs the whole
  window — no lost or skipped rows. The connector may yield batches as it pages
  (low memory) and rely on this all-or-nothing cursor commit.
- **`lookback`.** A small `lookback: 1d` re-fetches the trailing day to catch
  objects whose `created` is near a previous run boundary and clock skew. With
  `merge` semantics the re-fetch is idempotent.
- **Nested objects** (e.g. a charge's `payment_method_details`, an invoice's
  `lines`) land as `JSON` columns — the ShipHero v2 precedent in chapter 03.

### How query-as-stream would map (the later extension)

A single `@stream` generator does the whole async dance synchronously:
`POST /v2/data/reporting/query_runs` → poll `GET .../{id}` until `succeeded` →
download the CSV from `result.file.download_url.url` **immediately** (5-min TTL)
→ parse CSV → yield batches of dicts. Incremental is the user's responsibility:
they write `WHERE created >= <cursor>` into their SQL, and declare which result
column is the cursor via stream params. A query whose result would exceed 5 GB
must be date-partitioned by the connector (submit N query runs over date
windows). This is all ordinary Python in the connector body — it fits `@stream`
fine; it is just more code than a REST stream.

---

## Example `register.yaml` sketch

v1 shape — resource-as-stream over the REST API. Schemas abbreviated; production
streams should declare explicit `schema` blocks per chapter 03 §2.2.1.

```yaml
# connectors/stripe/register.yaml
name: stripe
kind: source
version: "0.1.0"
summary: Stripe payments data via the standard REST API (resource-as-stream).
tags: [payments, fintech, rest]

params:
  page_size:
    type: int
    default: 100
    description: Records per API page (Stripe max is 100).
  start_date:
    type: string
    default: "2024-01-01"
    description: First-run cursor floor (ISO date).
  lookback_days:
    type: int
    default: 1
    description: Trailing window re-fetched each run to catch late/edge rows.
  enable_sigma:
    type: bool
    default: false
    description: >
      Opt-in flag for the future Sigma query-as-stream support. Requires a paid
      Stripe Sigma subscription. Inert in v1.

secrets:
  - name: api_key
    ref: ${env.STRIPE_API_KEY}      # restricted key, read scopes

streams:
  - name: charges
    table: stripe_charges
    primary_key: id
    write_disposition: merge
    partition_by: created
    incremental:
      cursor_field: created
      cursor_type: timestamp
      lookback: 1d
      initial_value: "2024-01-01"
    schema:
      - {name: id,        type: STRING,    mode: REQUIRED}
      - {name: amount,    type: INTEGER}
      - {name: currency,  type: STRING}
      - {name: status,    type: STRING}
      - {name: customer,  type: STRING}
      - {name: created,   type: TIMESTAMP}
      - {name: metadata,  type: JSON}

  - name: invoices
    table: stripe_invoices
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: created
      cursor_type: timestamp
      lookback: 1d
      initial_value: "2024-01-01"
    # schema omitted -> inferred on first batch (prototype mode)

  - name: customers
    table: stripe_customers
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: created
      cursor_type: timestamp
      lookback: 1d
      initial_value: "2024-01-01"

  - name: subscriptions
    table: stripe_subscriptions
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: created
      cursor_type: timestamp
      lookback: 1d
      initial_value: "2024-01-01"

  # ---- v1.x extension (NOT v1): a Sigma SQL query as a stream. ----
  # Enabled only when params.enable_sigma is true and the account holds a
  # Sigma subscription. The SQL and its cursor column are stream-scoped params,
  # making each such stream a user-defined query — the GAQL analogue.
  #
  # - name: revenue_by_day
  #   table: stripe_revenue_by_day
  #   write_disposition: append
  #   params:
  #     sql: |
  #       SELECT date(created) AS day, sum(amount) AS gross
  #       FROM balance_transactions
  #       WHERE created >= {{cursor}}
  #       GROUP BY 1
  #     cursor_column: day
  #   incremental:
  #     cursor_field: day
  #     cursor_type: date

destination:
  connector: bigquery
  dataset: stripe

schedule: "0 */6 * * *"
```

Note what stays out of YAML, per the chapter 03 litmus test: the REST endpoint
paths, the `starting_after` pagination loop, the timestamp conversion, and (for
the Sigma variant) the submit/poll/download logic all live in `source.py`. YAML
carries only the discovery contract. The Sigma `sql` string is the one
borderline case — it is genuinely *configuration the user supplies per stream*
(like a GAQL string), so it belongs in stream-scoped `params`, not hard-coded in
Python.

---

## Open questions

1. **Sigma API GA timeline.** It is pinned to `2026-04-22.preview`. Building a
   *baked* connector against a preview API is a maintenance risk — field names
   and the version header may change. When does it go GA? Until then the Sigma
   stream type should stay clearly marked experimental.
2. **Sigma cost model.** Stripe documents no per-query price for the API. Is it
   flat-rate within the Sigma subscription, or metered by rows/compute? Affects
   whether query-as-stream is safe to run on a 6-hour schedule.
3. **SDK vs raw HTTP.** The official `stripe` Python SDK gives free
   auto-pagination and typed objects but adds a dependency (`requires:` in
   `register.yaml`). Raw `httpx`/`requests` keeps deps minimal but reimplements
   pagination. Lean toward the SDK for REST streams; raw HTTP for the v2 preview
   endpoint (SDK preview support may lag).
4. **5 GB Sigma result cap.** For large query-as-stream results the connector
   must date-partition the SQL itself. What is the right default window, and
   should it be a connector param?
5. **Confirm the `QueryRun.result` shape against the live API.** The object
   reference page 404'd during research; the field names here come from the
   Reports/Sigma guide pages. Re-verify `result.file.download_url.url` and
   `expires_at` against an actual response before coding the Sigma stream.
6. **Search API (`/v1/charges/search`).** Not investigated in depth — a more
   expressive filtering alternative to plain list endpoints, with a different
   pagination model. Out of scope for v1; revisit if `created`-based incremental
   proves insufficient for some object type.
7. **Object coverage for v1.** Which Stripe objects ship as pre-declared streams
   in the first release — and do any lack a usable `created` field for
   incremental (events, balance, etc.)?

---

## Sources

- [Run a SQL query from the v2 Reports API — Stripe Docs](https://docs.stripe.com/reports/query-runs)
- [Use the Sigma API — Stripe Docs](https://docs.stripe.com/stripe-data/sigma-api)
- [Data freshness — Stripe Docs](https://docs.stripe.com/stripe-data/available-data)
- [Write queries (Sigma SQL) — Stripe Docs](https://docs.stripe.com/stripe-data/write-queries)
- [Create a Query Run — Stripe API Reference](https://docs.stripe.com/api/sigma/sigma-query-run/create)
- [The Query Run object — Stripe API Reference](https://docs.stripe.com/api/sigma/sigma-query-run/object) *(direct fetch returned HTTP 404 during research — field shape reconstructed from the Reports/Sigma guide pages above)*
- [Scheduled Queries — Stripe API Reference](https://docs.stripe.com/api/sigma/scheduled_queries)
- [Pagination — Stripe API Reference](https://docs.stripe.com/api/pagination)
- [List all charges — Stripe API Reference](https://docs.stripe.com/api/charges/list)
- [Search charges — Stripe API Reference](https://docs.stripe.com/api/charges/search)
- [Stripe Sigma product page](https://stripe.com/sigma)
