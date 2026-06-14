# Google Ads — baked source connector

Extracts data from the [Google Ads API](https://developers.google.com/google-ads/api)
(v24 REST) via **GAQL** — the Google Ads Query Language. Each stream is a
GAQL query (`SELECT … FROM … WHERE …`) submitted to the
`GoogleAdsService.searchStream` endpoint; the rows it returns drive the
columns. This is the same query-as-stream shape the baked `stripe` connector
uses for Sigma SQL, so adding your own report is just a `.gaql` file plus a
stream entry.

Five streams ship by default:

| Stream | Grain | Incremental |
|---|---|---|
| `campaigns` | one row per campaign (entity list) | no — full `replace` each run |
| `campaign_daily_stats` | campaign × day | yes — on `segments.date` |
| `ad_group_daily_stats` | ad group × day | yes — on `segments.date` |
| `ad_daily_stats` | ad × day | yes — on `segments.date` |
| `keyword_daily_stats` | keyword × day | yes — on `segments.date` |

Money fields are in **micros** (1/1,000,000 of the account currency) — e.g.
`cost_micros = 419040369` is $419.04. Convert in dbt downstream, not here.

## Authentication

Google Ads has **no API key**. A call needs **four** credentials (the
connector mints the short-lived access token itself from your refresh token):

| Secret (env default) | What it is | Where it comes from |
|---|---|---|
| `GADS_DEVELOPER_TOKEN` | identifies your app to Google Ads | Google Ads UI → API Center (needs a manager account) |
| `GADS_CLIENT_ID` | OAuth 2.0 client | GCP Console → APIs & Services → Credentials |
| `GADS_CLIENT_SECRET` | OAuth 2.0 client secret | same OAuth client |
| `GADS_REFRESH_TOKEN` | proves a user consented | minted once — see below |

```sh
export GADS_DEVELOPER_TOKEN="…"
export GADS_CLIENT_ID="….apps.googleusercontent.com"
export GADS_CLIENT_SECRET="GOCSPX-…"
export GADS_REFRESH_TOKEN="…"
```

### Minting the refresh token (one time)

A refresh token requires a one-time browser consent. The connector ships a
helper that runs the loopback OAuth flow and writes the token to a
git-ignored file — it never prints the secret:

```sh
python -m dtex.sources.gads.scripts.get_refresh_token \
    --client-id "$GADS_CLIENT_ID" --client-secret "$GADS_CLIENT_SECRET"
# → opens the browser, you click Allow, writes .secrets/gads_refresh_token (0600)

export GADS_REFRESH_TOKEN="$(cat .secrets/gads_refresh_token)"
```

The redirect URI is `http://localhost:8080/` by default. **Desktop-app**
OAuth clients allow `http://localhost` automatically; **Web-app** clients
must have `http://localhost:8080/` (or your `--port`) registered under
*Authorized redirect URIs* in the GCP Console. Pass `--out <path>` to write
elsewhere or `--print` to print to stdout instead (not recommended).

### Production secrets

For production, override any secret per profile with a resolver-backed
`secret://` URL — GCP Secret Manager, AWS Secrets Manager, or Vault:

```yaml
# profiles.yml
gads:
  default_target: prod
  targets:
    prod:
      refresh_token: secret://gcp-secret-manager/projects/<proj>/secrets/<name>/versions/latest
```

(Requires the matching extra: `pip install 'dtex[gcp-secrets]'` /
`[aws-secrets]` / `[vault]`.) The developer token and minted access token are
set on request headers only and never appear in log output.

## Choosing which accounts to pull

Google Ads is per-customer. You supply accounts in one of two ways:

### Explicit list

```yaml
params:
  customer_ids: "111-222-3333, 444-555-6666"   # hyphens optional
```

If the accounts sit under a manager (MCC), also set `login_customer_id` to
the MCC id so the manager can read them.

### MCC auto-discovery

Leave `customer_ids` empty and name a manager — the connector expands its
tree and pulls every **ENABLED, non-manager (leaf)** account under it:

```yaml
params:
  auto_discover_from_manager: "777-888-9999"   # the MCC id
  max_discovery_depth: 1                        # 1 = direct children (default)
```

The MCC itself is excluded, and the manager id doubles as
`login_customer_id` automatically — you only name it once. An explicit
`customer_ids` always wins over auto-discovery if both are set.

## Streams

### `campaigns` — entity list, full replace

Pulls the current campaign list (id, name, status, channel type, budget).
Campaign attributes are mutable, so the stream uses
`write_disposition: replace` — every run pulls the full current set and
swaps the table. No cursor.

### `*_daily_stats` — incremental on `segments.date`

Each metrics stream:

1. Computes the window `start = max(cursor, today - segments_lookback_days)`,
   `end = today`, and binds it into the GAQL `WHERE segments.date BETWEEN
   '{since}' AND '{until}'` clause.
2. Fans out across every resolved customer id, flattening each nested
   `GoogleAdsRow` into flat snake_case columns.
3. Observes the cursor only against days **before today** — today's metrics
   are still settling, so the lookback re-pulls recent days on the next run
   with corrected (final) values. `write_disposition: merge` on the natural
   per-day key makes those re-pulls upsert idempotently.

Defaults: `segments_initial_since_date = 2024-01-01` (first run only),
`segments_lookback_days = 7`.

## Config

```yaml
# configs/gads_bq.yml
name: gads_bq
source: gads
destination: bigquery
target: prod
params:
  auto_discover_from_manager: "777-888-9999"
destination_params:
  dataset: gads
streams: all                         # or list specific streams
```

Narrow to one stream:

```yaml
streams:
  campaign_daily_stats:
    params:
      segments_lookback_days: 14
```

## Adding your own GAQL report

The connector's power is custom GAQL. To add a report:

1. Drop a `.gaql` file under `queries/` (use `{since}` / `{until}` for the
   date window if it's incremental).
2. Add a stream entry in `register.yaml` with a `gaql: {query: queries/x.gaql}`
   block, a `schema:`, and (for incremental) an `incremental:` block on
   `date`.
3. Add the GoogleAdsRow-path → column mapping for the new stream in
   `_FIELD_MAP` in `source.py`, and a one-line `@stream` wrapper.

GAQL field names: see Google's
[GAQL field reference](https://developers.google.com/google-ads/api/fields/v24/overview).
Note `GoogleAdsRow` comes back nested + camelCased (`metrics.costMicros`,
`adGroupAd.ad.id`); the `_FIELD_MAP` entry uses that dotted camelCase path.

## Rate limits, retries, timeouts

* Auth is one access-token mint per run (cached until ~60s before expiry).
* The client uses a `(10s connect, 120s read)` timeout — the read leg is
  generous for large streamed reports without trapping a dead socket.
* Retries cover HTTP 429 (`RESOURCE_EXHAUSTED`, honors `Retry-After`),
  HTTP 5xx (capped exponential backoff), and the network-exception family —
  all bounded by `max_retries` (default 5).

## What's not in v1 of this connector

- Conversion-action / change-event / audience streams (add via custom GAQL).
- The paged `:search` method — the connector uses `:searchStream` only.
- Streaming JSON parse: the searchStream response is buffered fully in
  memory per customer (fine for typical reports; swap to an incremental
  parser in `client.py` if a single account's report is enormous).
