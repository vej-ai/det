# postgres — pre-baked Postgres source connector

A detx source connector that extracts rows from a Postgres database. Each
declared stream maps to a table (or a SQL query coded in `source.py`); rows
are paginated by **keyset** (cursor + primary key) when incremental, or by a
**server-side `DECLARE … CURSOR` / `FETCH FORWARD`** loop when not — never
`LIMIT N OFFSET M` (which scales O(N²) on large tables).

This is a pre-baked connector and lives at
`detx/connectors/postgres/`. A user project can shadow it by dropping a
same-named folder under its `connector_paths` (docs/03 §5).

## Supported types — `type_mapping.postgres_to_field_type`

Every column the connector introspects (or you declare in `register.yaml`'s
`schema:`) maps to one of detx's `FieldType` members (docs/03 §2.2.1):

| Postgres type | `FieldType` |
|---|---|
| `text`, `varchar`, `character varying`, `char`, `character`, `bpchar` | `STRING` |
| `smallint`, `int2`, `integer`, `int`, `int4`, `bigint`, `int8`, `serial`, `bigserial`, `smallserial` | `INTEGER` |
| `numeric`, `decimal`, `real`, `float4`, `double precision`, `float8` | `FLOAT` |
| `boolean`, `bool` | `BOOLEAN` |
| `timestamp`, `timestamp without time zone`, `timestamptz`, `timestamp with time zone` | `TIMESTAMP` |
| `date` | `DATE` |
| `json`, `jsonb` | `JSON` |
| `bytea` | `BYTES` |
| `uuid` | `STRING` |

An unknown Postgres type raises a clear `ValueError`. Add it to
`_PG_TO_FIELD_TYPE` in `type_mapping.py` or declare the column explicitly in
`schema:`.

## YAML config surface

Connector-level `params` (connection): `host` (required), `port` (default
5432), `database` (required), `user` (required), `sslmode` (default `prefer`),
`application_name` (default `detx`), `connect_timeout_seconds` (default
30), `batch_size` (default 5000 — rows per server round-trip and per yielded
batch).

Connector-level `secrets`: `password`, referenced via `${env.POSTGRES_PASSWORD}`
or `${profile.postgres.password}`.

Per-stream contract fields are the usual ones (docs/03 §2.2): `name`, `table`,
`primary_key`, `write_disposition`, `incremental`, `schema`. **Per-stream
Postgres knobs (schema_name, table_name, query, cursor_field, primary_key)
live in `source.py`**, not in YAML — see the `source.py` module docstring for
why (engine builds one Config per connector; `stream_def.params` is not
merged into per-stream calls).

## Example: minimal `register.yaml`

```yaml
name: postgres
kind: source
version: "1.0.0"

params:
  host:     {type: string, required: true}
  database: {type: string, required: true}
  user:     {type: string, required: true}
secrets:
  - name: password
    ref: ${env.POSTGRES_PASSWORD}

streams:
  - name: users
    table: pg_users
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: updated_at
      cursor_type: timestamp
      initial_value: "1970-01-01T00:00:00"
    schema:
      - {name: id,         type: INTEGER, mode: REQUIRED}
      - {name: email,      type: STRING}
      - {name: updated_at, type: TIMESTAMP, mode: REQUIRED}
```

## Example: a corresponding `source.py`

```python
from detx import stream
from detx.sources.postgres.source import extract_stream

@stream(name="users")
def users(config, cursor, log):
    yield from extract_stream(
        stream_name="users",
        config=config, cursor=cursor, log=log,
        schema_name="public",
        table_name="users",
        cursor_field="updated_at",
        primary_key=("id",),
    )
```

For a `query`-mode stream, pass `query="SELECT id, total, updated_at FROM
orders WHERE status = 'active'"` instead of `table_name=` — the query is
wrapped as a subquery and a keyset `WHERE / ORDER BY / LIMIT` is applied
around it. The cursor field must be selectable as a top-level column from the
user's query.

For a non-incremental full scan, drop `incremental:` from the YAML stream and
drop `cursor` from the `@stream` signature (the engine then injects no
cursor) and pass only `table_name=` / `schema_name=` to `extract_stream` —
the helper takes the `DECLARE … CURSOR` path.
