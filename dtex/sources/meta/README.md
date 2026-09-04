# meta — Meta (Facebook / Instagram) Ads insights

Baked dtex source for Meta's **Marketing API Insights edge**
(`GET /v21.0/act_<id>/insights`). It lands ad-level daily performance and
campaign-level hour-of-day performance for any number of ad accounts,
through Meta's **asynchronous report jobs**, with paced requests, hard
timeouts, jittered bounded retries and loud failure.

Created by [Auste Luksaite](https://www.linkedin.com/in/austeluksaite/).

## Streams

| stream | grain | key | cursor |
|---|---|---|---|
| `ads_insights` | one (day, ad account, ad) row with delivery — `level=ad`, `time_increment=1` | `(date_start, account_id, ad_id)`, `write_disposition: merge` | `date_start` (DATE), re-walked `lookback_days` (28) behind on every run |
| `insights_hourly` | one (day, hour-of-day, ad account, campaign) row with delivery — `level=campaign`, `time_increment=1`, `breakdowns=hourly_stats_aggregated_by_advertiser_time_zone` | `(date_start, hour, account_id, campaign_id)`, `write_disposition: merge` | own `date_start` cursor, same lookback rule |

Days on which an ad (or campaign-hour) had no delivery are simply absent:
Meta does not return zero rows.

### `ads_insights`

Landed columns: account / campaign / adset / ad ids + names, `objective`,
`optimization_goal`, `impressions`, `reach`, `frequency`, `clicks`,
`unique_clicks`, `spend`, `social_spend`, `full_view_impressions`, and the
`actions` / `action_values` / `unique_actions` / `conversions` /
`conversion_values` / `video_p*_watched_actions` /
`video_time_watched_actions` arrays as JSON (`[{action_type, value}, …]`).
The requested field list is the `fields` param — a field Meta retires in
a newer API version is dropped in config, no code change. Fields not
declared in the stream schema still land via schema evolution.

### `insights_hourly` — hour-of-day spend in the advertiser timezone

Meta's hourly breakdown value arrives as the string `"HH:00:00 - HH:59:59"`;
the connector parses it into the INTEGER `hour` (0..23, part of the key)
and keeps the raw string as `hour_range`. **The hour is in the AD
ACCOUNT's timezone**, not UTC: once per run the stream calls
`GET /v21.0/act_<id>?fields=timezone_name,timezone_offset_hours_utc` for
every account and stamps `timezone_name` (IANA, DST-correct) +
`timezone_offset_hours_utc` (informational) on every row. Convert
downstream, e.g. in BigQuery:

```sql
DATETIME(TIMESTAMP(DATETIME(date_start, TIME(hour, 0, 0)), timezone_name), 'Europe/London')
```

Fields: `account_id, account_name, account_currency, campaign_id,
campaign_name, date_start, spend, impressions, clicks, actions,
action_values` (fixed in `source.py` `HOURLY_FIELDS`, independent of the
`fields` param). A row without a parsable hour is unkeyable and dropped
(counted in the log).

Campaign × hour is ~100× smaller per day than ad-level daily, so a wider
`window_days` and a later `start_date` for this stream only are a good
idea — see the config example below. Each stream has its own cursor row
in `_dtex_state`.

## Auth — access token

One credential: an access token with the **`ads_read`** permission on
every configured ad account. For a scheduled pipeline use a
**Business Manager system-user token** — it never expires (verify with
`GET /debug_token`: `type: SYSTEM_USER`, `expires_at: 0`); a user token
expires in ~60 days.

| env var | value |
|---|---|
| `META_ACCESS_TOKEN` | the token (`EAA…`) |

The token rides as the `access_token` query param and is stripped from
every logged URL / raised message. Never commit it. To rotate: Business
Settings → System users → your user → Generate new token (same app,
`ads_read`, expiry Never); the connector picks up the new value on the
next run. Override the secret ref per deployment (`secret://…`) for a
managed secret store.

## Config

```yaml
# configs/meta_prod.yml
name: meta_prod
source: meta
destination: bigquery
target: prod

destination_params:
  dataset: meta

params:
  # With or without the act_ prefix.
  account_ids: "1234567890,act_9876543210"
  # First day ever pulled (virgin state). Insights history is reachable
  # for 37 months at most.
  start_date: "2025-01-01"

streams:
  ads_insights:                 # defaults: 7-day windows from start_date
  insights_hourly:
    params:
      window_days: 30           # ~100x fewer rows per day than ad level
      start_date: "2025-09-01"
```

A new ad account must ALSO be granted to the token's user in Business
Manager (Assign assets → Ad accounts → View performance) — otherwise the
run fails with error code 200/10 and a hint in the message. All accounts
of a stream share one cursor: a new account's history is backfilled by a
one-shot `since:` override (or `dtex state reset`), not automatically.

## How to run — backfill vs incremental

Same config, no mode switch; the persisted cursor decides:

- **Backfill** = the virgin run: walks `start_date` → today in
  `window_days` windows × accounts. One async job per (window, account),
  a few result pages each, at `page_delay_seconds` pacing.
- **Incremental** = every later run: resumes from `cursor − lookback_days`.
  Meta restates recent days (attribution up to 28 days, late conversions,
  deleted/rejected ads), and merge on the natural grain makes the re-pull
  idempotent. Today is included and partial on purpose; the next run
  overwrites it.

```bash
export META_ACCESS_TOKEN="EAA..."
dtex validate
dtex run -p meta_prod                            # both streams
dtex run -p meta_prod --select insights_hourly   # one stream only
```

A restatement older than `lookback_days` is NOT caught automatically —
re-pull the span with a one-shot `since:` under `streams:` (or
`dtex state reset`).

## Why async jobs

A synchronous `GET /act_<id>/insights` at ad level over a week of a large
account (~10k ads) dies with Meta's generic `code 1 / subcode 99` — the
API's way of saying "too much data for a sync call". The client submits
an **async report job** per (window, account) (`POST /act_<id>/insights`
→ `report_run_id`, poll `async_status`, page `/<report_run_id>/insights`),
which is Meta's documented remedy. A job that ends `Job Failed` /
`Job Skipped` is resubmitted (bounded by `max_retries`); if it keeps
failing, the window is **bisected** and the halves re-requested — the job
fails before any page is fetched, so nothing is double-landed. A 1-day
window that still fails is a real error and stops the run. A job still
running after `job_timeout_seconds` (30 min) fails the run red.

## Rate limiting

Meta's Insights budget is per ad account (rolling window) and reported in
the `x-business-use-case-usage` / `x-ad-account-usage` headers; the
client logs the percentage per (window, account), pauses 60s proactively
above 80%, and backs off with jitter on the limiter's error codes
(4 / 17 / 32 / 613 / 80000–80014, or a plain 429), bounded by
`max_retries` then a red run. If those appear, raise
`page_delay_seconds` in the config — never lower it against the live API.

## Sanity check — hourly vs daily

Per account and day, the hourly stream's spend should sum to the daily
ad-level total (small deltas are Meta's own hourly-vs-daily rounding):

```sql
WITH h AS (
  SELECT date_start, account_id, ROUND(SUM(spend), 2) spend_h
  FROM insights_hourly GROUP BY 1, 2
), d AS (
  SELECT date_start, account_id, ROUND(SUM(spend), 2) spend_d
  FROM ads_insights GROUP BY 1, 2
)
SELECT date_start, account_id, spend_h, spend_d, spend_h / spend_d AS coverage
FROM d LEFT JOIN h USING (date_start, account_id)
ORDER BY coverage NULLS FIRST, date_start;
```

## Local dev

```bash
pip install dtex
export META_ACCESS_TOKEN=x        # any non-empty value for validate
dtex validate
pytest tests/connectors/test_meta.py   # stubbed HTTP, no network
```
