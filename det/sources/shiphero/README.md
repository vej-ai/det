# ShipHero baked source connector

A det source connector that extracts ShipHero fulfillment data via the
public GraphQL API. This is the proof case docs/03 §2.8 designed the contract
around — ported from an internal hand-written ShipHero connector that's been
running in production.

## Authentication

ShipHero uses a long-lived **refresh token** to mint short-lived bearer
**access tokens**. The connector declares one secret:

```yaml
secrets:
  - name: refresh_token
    ref: ${env.SHIPHERO_REFRESH_TOKEN}
```

The client (`client.py`) exchanges the refresh token for an access token via
`POST https://public-api.shiphero.com/auth/refresh` on the first GraphQL call,
and re-refreshes once on a 401 response.

To run:

```sh
export SHIPHERO_REFRESH_TOKEN="<your-refresh-token>"
det run shiphero --target prod
```

If you prefer to keep secret material in `profiles.yml`, fork the connector and
swap the ref form to `${profile.shiphero.refresh_token}`.

## Streams

Three streams are declared, all merge-on-`id`, all incremental:

| Stream    | Cursor field   | Notes |
|-----------|----------------|-------|
| `shipments` | `created_date` | Nested `shipping_labels` and `line_items` land as JSON columns. |
| `orders`    | `order_date`   | Nested `line_items` as JSON. |
| `products`  | `updated_at`   | Nested `warehouse_products` as JSON. |

Each table is partitioned (where the destination supports it) on the cursor
field.

## The lookback / step strategy

ShipHero's GraphQL has hard server-side limits on a single date-range query, so
the connector splits a long backfill into fixed-width **date windows** and
paginates *within* each window. Configuration knobs (`register.yaml params`):

- `lookback_days` (default 2) — on resume, go back this many days from the
  persisted cursor to catch late-arriving rows.
- `step_days` (default 10) — width of each date window.
- `page_size` (default 50) — GraphQL records per page.
- `batch_size` (default 200) — records per det batch (the destination
  commits between batches).
- `max_retries` (default 5), `retry_backoff_seconds` (default 2.0) — HTTP
  retry policy.
- `start_date` (default `"2024-01-01"`) — fallback initial cursor for streams
  that omit `incremental.initial_value`.
- `api_url` — the GraphQL endpoint; overridden in tests to point at a stub.

## Example

Backfill 90 days of shipments with a tight per-window step:

```sh
SHIPHERO_REFRESH_TOKEN=... det run shiphero --target dev \
    --param start_date=2025-01-01 --param step_days=5 --param page_size=100
```

## Ported from

The connector started life as a hand-written internal script with a
`main.py` and `config.json`. The port mapped:

- `main.py` — `refresh_access_token`, `execute_graphql`, `sync_table`,
  `extract_records` → split across `client.py`, `pagination.py`, `source.py`.
- `config.json` — the `tables.*` blocks became `streams[]` in
  `register.yaml`; strategy knobs became `params`.

What moved out of the connector (now the engine / destination does it):

- `get_checkpoint` / `save_checkpoint` → the engine's `Cursor` + `_det_state`.
- `ensure_tables_exist`, `merge_records` → the DuckDB destination connector.
- `upsert_records` `MERGE` → `write_disposition: merge` in `register.yaml`.

What stayed (the genuinely ShipHero-specific logic):

- GraphQL query strings — `queries.py`.
- Date-window stepping with lookback — `windows.py`.
- GraphQL cursor pagination — `pagination.py`.
- Token refresh + retry loop — `client.py`.

See the connector files' module docstrings for the line-by-line mapping back
to `main.py`.
