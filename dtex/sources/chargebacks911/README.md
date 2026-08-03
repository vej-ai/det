# Chargebacks911 — baked source connector

Extracts data from [Chargebacks911](https://chargebacks911.com)'s CBAPIv2
at `https://api.cbresponseservices.com/v2` (sandbox:
`https://sandbox.cbresponseservices.com/v2`, via the `base_url` param).
Two streams:

* **`alerts`** — prevention alerts (Ethoca / Verifi / RDR etc.) from
  `/clients/{client_id}/alerts`. Incremental on `dateUpdated`.
* **`chargebacks`** — chargeback cases from
  `/clients/{client_id}/chargebacks`. Incremental on `date_updated`.

Both endpoints take a real server-side date filter
(`date_column` + `start_date` + `end_date`), so the incremental cursor is
real: every run pulls `(cursor date − lookback_days)` → today, and
`write_disposition: merge` on `id` makes the overlap upsert idempotently.

## Authentication

CB911 uses an unusual two-step scheme:

1. Basic auth (username + password) on `GET /auth` mints a bearer token.
2. Every other request carries `Authorization: Bearer <token>`.

The token expires after ~60 minutes, **and minting a new token invalidates
the previous one** (last-mint-wins, account-wide). The connector handles
this automatically: it mints lazily on the first request, proactively
re-mints when the token is older than ~50 minutes (so long extractions
survive the TTL), and on a mid-run 401 re-mints once and retries.

The credentials are **issued by the Chargebacks911 setup team** — they are
not self-service; ask your CB911 account contact. The connector reads them
from two environment variables by default:

```sh
export CHARGEBACKS911_USERNAME="..."
export CHARGEBACKS911_PASSWORD="..."
```

For production, override the secret refs per profile with any
resolver-backed `secret://` URL — GCP Secret Manager, AWS Secrets Manager,
or HashiCorp Vault:

```yaml
# profiles.yml
chargebacks911:
  default_target: prod
  targets:
    prod:
      username: secret://gcp-secret-manager/projects/<proj>/secrets/cb911-username/versions/latest
      password: secret://gcp-secret-manager/projects/<proj>/secrets/cb911-password/versions/latest
```

(Requires the matching extra: `pip install 'dtex[gcp-secrets]'` /
`[aws-secrets]` / `[vault]`.)

The credentials never appear in log output or error messages — the client
fields are `repr=False`, the client does no logging, and raised errors
carry only URLs, status codes, and the server's own `message` string.

## Params

| Param | Default | Meaning |
|---|---|---|
| `base_url` | `https://api.cbresponseservices.com/v2` | Production API; set the sandbox URL for testing. |
| `client_id` | `"my"` | The API accepts the literal alias `my` for the caller's own account; set a numeric id for a sub-client. |
| `page_size` | `500` | Rows per page (`limit`). The chargebacks endpoint caps at 2500. |
| `lookback_days` | `7` | Overlap window on every incremental run — catches late updates; merge makes the re-pull idempotent. |

## The two streams

### `alerts`

`GET /clients/{client_id}/alerts`, paginated with `limit` + `page`
(1-based; the walk stops on a short page). Field names are **camelCase**
exactly as the API returns them. Amounts arrive as strings (`"104.97"`);
the engine coerces them to the declared FLOAT.

Schema highlights (full list in `register.yaml`): `id` INTEGER (primary
key), `clientId`, `level`, `outcomeId`, `currencyId`, `cc_id` INTEGER;
`completed`, `isDisputed`, `isRefund`, `isValid` BOOLEAN; `amount`,
`refundAmount` FLOAT; and STRING for the mid/descriptor/case/card/customer
fields plus every date field (`dateUpdated` is the cursor; the API
serves dates as strings and the connector stores them as-is).

### `chargebacks`

`GET /clients/{client_id}/chargebacks`, same pagination (API max
2500/page), same date-filter mechanism (`date_column` accepts
`date_trans|date_created|date_post|date_updated`; the connector filters on
`date_updated`, its cursor). Field names are **snake_case** on this
endpoint.

Schema highlights (full list in `register.yaml`): `id` STRING (primary
key — declared STRING deliberately, since the id shape is not pinned down
across accounts and STRING is the safe merge key either way);
`chargeback_amount`, `dispute_amount`, `disputed_amount` FLOAT;
`crm_gateway_id`, `platform_id` INTEGER; `order_api_generated`,
`partial_chargeback` BOOLEAN; `status_history` lands as a **JSON column**
(nested array of status objects); everything else STRING.

## Config

A minimum config:

```yaml
# configs/chargebacks911_bq.yml
name: chargebacks911_bq
source: chargebacks911
destination: bigquery
target: prod
destination_params:
  dataset: chargebacks911
streams: all            # or list specific streams
```

## Known limitations

* **Token invalidation vs concurrency.** Minting a token invalidates the
  previous one account-wide. Two concurrent runs (or the connector plus
  any other integration minting tokens with the same credentials) will
  fight over the token — each mint 401s the other side, which re-mints,
  which 401s the first… Run one pipeline at a time per credential pair,
  and don't share the pair with other tooling.
* **Alerts include customer PII** — `customerName`, `customerEmail`,
  `firstName`, `lastName`, partial card numbers. Land them in a dataset
  with appropriate access controls, and consider dropping the columns
  downstream if you don't need them.
* **Envelope drift.** The API nominally wraps responses in
  `{success, code, message, data}` but live production responses sometimes
  return a bare JSON list. The client unwraps both shapes defensively.
* **Cursor dates are strings.** `dateUpdated` / `date_updated` are stored
  and compared as strings. They are ISO-shaped so string order equals
  chronological order; the `lookback_days` overlap absorbs any same-day
  edge effects.
