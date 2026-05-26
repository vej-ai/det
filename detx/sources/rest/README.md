# `rest` — Generic REST source connector

A baked source connector for any paginated REST/JSON API. Talks to your API
via configurable auth + pagination + cursor — no per-API Python required for
common patterns. Reach for this connector when "I have an API and just want
the data" and only write a custom connector when this one cannot stretch.

## What it supports

- **Auth**: `none`, `bearer`, `basic`, `api_key_header`, `api_key_query`.
  The credential lives in the connector's `${env.REST_API_TOKEN}` secret (or
  whichever env var you reroute it to).
- **Pagination**: `cursor` (opaque body token), `offset` (offset+limit),
  `page` (1-indexed page+per_page), `link_header` (RFC 5988 `rel="next"`),
  `none` (single-shot endpoints).
- **Record extraction**: `record_path` (list of keys, with `*` for arrays of
  envelopes) walks the response down to the records list.
- **Incremental cursor**: the engine's standard `Cursor` — your stream sends
  `cursor.start_value()` as the configured `cursor_query_param` on the first
  request and `observe()`s each record's cursor field.
- **Retry on 429/5xx**: via `urllib3.util.Retry` with `Retry-After` honored.
- **Rate limit**: `requests_per_second` cap (`0` = unlimited).

## How a stream is declared (the pattern)

detx binds `streams[].name` in `register.yaml` to a `@stream(name=...)`
function 1-to-1 (docs/03 §7 rule 7). A *generic* connector cannot guess your
endpoints, so adding a stream is **two coordinated edits**:

### 1. Add a `streams[]` entry to `register.yaml`

```yaml
streams:
  - name: orders
    table: rest_orders
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: updated_at
      cursor_type: int           # see "Known limitations" below before using "timestamp"
      initial_value: "0"
    schema:
      - {name: id,         type: STRING, mode: REQUIRED}
      - {name: status,     type: STRING}
      - {name: updated_at, type: INTEGER}
```

The YAML carries only what the detx contract supports (name / table /
primary_key / write_disposition / incremental / schema / partition_by).

### 2. Add a matching `@stream` function in `source.py`

```python
@stream(name="orders")
def orders(config, cursor, log):
    yield from extract_stream(
        config=config,
        cursor=cursor,
        log=log,
        endpoint="/v3/orders",
        record_path=["data", "items"],
        cursor_query_param="updated_since",
        next_cursor_path="meta.next_cursor",
        extra_query_params={"include": "line_items"},
    )
```

The wrapper is intentionally tiny — it forwards to `extract_stream`, the
shared driver that owns paginating + extracting + observing the cursor.

### Why per-stream API config is in Python, not YAML

The detx contract type `ParamSpec` (docs/03 §2.4) only allows scalar
`string|int|float|bool` values, so `record_path: ["data","items"]` or
`extra_query_params: {"x": "y"}` cannot live in `streams[].params`. Per
`CONTRIBUTING.md` ("code is source of truth"), the connector keeps that
config in Python — it stays type-checked by `mypy`, testable in isolation,
and free of YAML-string escape hatches.

## Connector-level config (`register.yaml` `params`)

| Param                   | Type   | Default          | Meaning |
|-------------------------|--------|------------------|---------|
| `base_url`              | string | *required*       | e.g. `https://api.example.com/v3` |
| `auth_type`             | string | `none`           | `none` / `bearer` / `basic` / `api_key_header` / `api_key_query` |
| `auth_header_name`      | string | `Authorization`  | header name for `api_key_header` |
| `auth_query_param`      | string | `api_key`        | query param name for `api_key_query` |
| `pagination_strategy`   | string | `cursor`         | `cursor` / `offset` / `page` / `link_header` / `none` |
| `page_size`             | int    | `100`            | records per page |
| `max_retries`           | int    | `5`              | retries on 429 + 5xx (Retry-After honored) |
| `retry_backoff_seconds` | float  | `1.0`            | `urllib3.util.Retry` backoff factor |
| `requests_per_second`   | float  | `0`              | rate cap (0 = unlimited) |
| `timeout_seconds`       | float  | `30.0`           | per-request timeout |

## Pagination strategies

Each strategy reads any per-stream knobs from the `extract_stream` kwargs and
drives the request loop:

- **`cursor`** — needs `cursor_query_param` + `next_cursor_path`. Sends the
  previous response's `next_cursor_path` value as the next request's
  `cursor_query_param`. Stops when the field is absent/empty.
- **`offset`** — sends `offset=N, limit=page_size`. Stops on a short page.
- **`page`** — sends `page=N, per_page=page_size`. Stops on a short page.
- **`link_header`** — follows the RFC 5988 `Link: <url>; rel="next"` URL until
  it is absent. Refuses to cross to a different origin (token leak guard).
- **`none`** — exactly one request.

## Security

- The `Authorization` header is **never** logged. The HTTP client logs URLs +
  *parameter keys* only, never values (a value could carry a token under
  `api_key_query`). The engine's `RedactingFilter` is a second defence: any
  resolved secret value that does appear in a log message is masked to `***`.
- Set `REST_API_TOKEN` in the environment; `register.yaml`'s `${env.REST_API_TOKEN}`
  resolves it lazily at run time.

## Example run

```bash
export REST_API_TOKEN=sk_live_…
detx run rest --target dev
```

## Known limitations

- **String cursor values (`cursor_type: timestamp` / `date` / `string`)** are
  not safely committed by the v1 DuckDB destination — it stores
  `cursor_value` in a JSON column and writes bare strings as raw text, which
  DuckDB then rejects on commit (Conversion Error: Malformed JSON). Use
  `cursor_type: int` for now (epoch ms / id) or bind to a destination that
  JSON-encodes scalars correctly. Reported separately as a destination fix.

