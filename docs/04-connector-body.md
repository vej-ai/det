# 04 — The Connector Body

Chapter 03 defined the *contract* — the folder, `register.yaml`, and decorators.
This chapter is about what goes *inside*: how a connector's Python is organized,
and the four concepts every connector author works with — **state, destination,
source, streams**.

## The four concepts

A connector body is built from exactly four ideas. Three of them are things the
connector *contains*; one is something the connector *targets*.

| Concept | What it is | Where it lives |
|---|---|---|
| **Source** | The logic that reads records from the outside world. The body of a `kind: source` connector. | `source.py` (`@stream` functions) |
| **Stream** | One output table. A source produces one or more streams. Each maps to a `streams[]` entry in `register.yaml`. | One `@stream` function per stream |
| **Destination** | The logic that writes records to a warehouse/file/db. The body of a `kind: destination` connector. | `destination.py` (`@destination` hooks) |
| **State** | Per-stream persisted memory — incremental cursors plus free-form scratch space — that survives between runs. | The `_dtex_state` table in the destination |

The relationship in one sentence:

> A **source** connector produces one or more **streams**; each batch of records
> flows to a **destination** connector; **state** records how far each stream has
> progressed so the next run resumes correctly.

Note the asymmetry that the symmetric *contract* hides: source and destination
are **separate connectors**, each its own folder. They are never two halves of
one connector. The link between them is a **pipeline config** (chapter 12) —
a small YAML file that names one source + one destination + one target +
params. It is a name-binding, not an import. This keeps a source reusable
across destinations and a destination reusable across sources — the dbt-style
decoupling.

## Recommended file layout

The engine imposes only one rule: a `register.yaml` plus at least one `.py` that
defines the decorated functions. Everything below is **convention** — the layout
`dtex new` scaffolds and the layout the handbook recommends.

### A source connector

```
shiphero/
├── register.yaml      # manifest (chapter 03)
├── source.py          # @stream functions — the entry points
├── streams.py         # (optional) extra @stream functions, when source.py gets long
├── schema.py          # (optional) shared schema constants / record-shaping helpers
├── client.py          # (optional) the API client — auth, pagination, retries
└── requirements.txt   # (optional) extra deps
```

Guidance on the split:

- **`source.py`** holds the `@stream` functions. For a small connector this is
  the *only* Python file. It is the file a reader opens first.
- **`client.py`** holds everything about *talking to the API* — token refresh,
  HTTP/GraphQL calls, retry/backoff, rate-limit handling. It has **no
  decorators**: it is an ordinary module. In the baked ShipHero connector,
  this is `refresh_access_token`, `execute_graphql`, and the retry loop.
- **`streams.py`** exists only when one file of `@stream` functions becomes hard
  to scan. Splitting is by readability, not by rule.
- **`schema.py`** holds shaping logic: turning a raw API payload into the flat
  record dict the declared `schema` expects (extracting `node` from GraphQL
  edges, navigating `field_path`, coercing types). In the baked ShipHero
  connector, this is `extract_records` and the field-path walk.

A connector author should be able to delete `streams.py` and `schema.py` and
fold their contents into `source.py` with no behavior change. They are
organizational, not structural.

### A destination connector

```
bigquery/
├── register.yaml      # manifest, kind: destination
├── destination.py     # the @destination hooks — the entry points
├── ddl.py             # (optional) table create / schema-evolution helpers
└── requirements.txt   # (optional) extra deps (e.g. google-cloud-bigquery)
```

- **`destination.py`** holds the `@destination` hooks (`open`, `write_batch`,
  `ensure_schema`, `commit_state`/`read_state`, `close` — see chapter 03 §3.4).
- **`ddl.py`** holds create-table / add-column / partitioning logic — the
  destination-shaped concern. In the baked BigQuery destination, this is
  `ensure_tables_exist` and `get_bq_schema`.

A destination's `register.yaml` uses the **same manifest format** as a source —
it just sets `kind: destination`, declares no `streams`, and exposes the
routing/credential knobs it needs as `params` and `secrets`:

```yaml
# destinations/bigquery/register.yaml  (or the pre-baked equivalent)
name: bigquery
kind: destination
version: "1.0.0"
summary: Loads batches into Google BigQuery (MERGE / append / replace).
tags: [warehouse, gcp]

params:
  dataset:  {type: string, required: true}   # supplied per-pipeline via a config
  location: {type: string, default: "US"}

secrets:
  - name: credentials_file
    ref: ${profile.bigquery.credentials_file}
```

The `dataset` param is supplied per-pipeline via the active config's
`destination_params:` block (chapter 12); `location` and `credentials_file`
come from the active target's `profiles.yml` (chapter 06). The destination
connector itself is identical across every source and every environment that
uses it.

### A combined connector (rare)

A single connector that is *both* source and destination is not expressible —
`kind` is a single enum value, by design. The simple, recommended path for "read
X, write X" is two connectors in the same project: a `kind: source` folder and a
`kind: destination` folder. If a genuine reason to fuse them ever appears (a
peer-to-peer sync tool, say), it would be the class-based escape hatch's job,
not the decorator contract's. The handbook position: **do not build combined
connectors.** Two folders is simpler and more reusable.

## The record shape — the envelope between source and destination

Source and destination are decoupled connectors that never import each other.
They agree only on the shape of the data that crosses between them. That shape
is deliberately boring.

### A record is a flat `dict`

One record is a plain Python `dict`. Keys are column names; values are
JSON-serializable scalars or — for nested data — a `dict`/`list` destined for a
`JSON` column.

```python
{
    "id": "ship_8842",
    "order_id": "ord_551",
    "created_date": "2025-12-14T09:31:00Z",
    "dropshipment": False,
    "shipping_labels": [                    # -> JSON column
        {"tracking_number": "1Z…", "carrier": "ups"}
    ],
}
```

### A batch is a `list[dict]`

A source `@stream` yields **batches** — `list[dict]`. The destination's
`@destination.write_batch` hook receives one batch per call. There is no heavier
"envelope" object: the *batch* is the unit, and the per-stream metadata a
destination needs (table name, primary key, write disposition, schema) arrives
as the hook's own arguments (`table`, `disposition`, `schema` — chapter 03
§3.4), resolved from `register.yaml`. Keeping records as bare dicts
means a connector author debugging a stream sees exactly the JSON the API
returned, with no framework wrapper in the way.

The engine's only contributions to a record are:

- It appends `_dtex_synced_at` (`TIMESTAMP`) to every record at load time.
- It validates each record against the stream's declared `schema` (when one is
  declared) before handing the batch to the destination.

### Nested data

The baked ShipHero connector stores `shipping_labels` and `line_items` as
`JSON` columns rather than flattening them into child tables. dtex supports
both: declare the column as `type: JSON` to keep nested structure, or shape
it flat in `schema.py` to spread it across columns. The handbook default is
**JSON column for nested objects** — it is simplest and keeps one stream =
one table.

Whether a connector should be able to declare a *child stream* (e.g.
`line_items` as its own table keyed back to `shipments`) declaratively in
`register.yaml` is a v2 question — current lean is "a second `@stream` is
enough; child tables are just streams." See
[chapter 11 Q4](./11-open-questions.md).

## How state relates to a stream

`state` and `cursor` (chapter 03 §3.1–3.5) are the connector's memory. Inside the
body:

- **`cursor`** is the *typed* slice of state — the incremental position. The
  connector reads it (`cursor.start_value()`) and reports progress
  (`cursor.observe(...)`); the engine persists the max. The connector never
  writes the cursor itself.
- **`state`** is *untyped* scratch space — a dict-like object for anything else a
  stream needs to remember between runs (a vendor-side pagination token that
  outlives a run, a high-water id the API exposes instead of a timestamp). The
  connector reads and writes it freely; the engine persists it as `state_blob`.

Both are scoped **per stream**, keyed `(connector, stream)` in `_dtex_state`.
Two streams in one connector have independent state and can be at different
cursor positions.

## A complete annotated example connector

A full source connector folder, end to end. This is the baked ShipHero
connector, organized into the recommended layout.

### `shiphero/register.yaml`

See chapter 03 §2.8 for the full manifest. In brief: `kind: source`, one
`shipments` stream, `write_disposition: merge` on `primary_key: id`,
incremental on `created_date` with a `2d` lookback. The destination is named
by the pipeline config (chapter 12), not in the source manifest.

### `shiphero/client.py` — the API client (no decorators)

```python
"""ShipHero GraphQL client: auth, calls, retry. Plain module, no contract."""
import time, requests

AUTH_URL = "https://public-api.shiphero.com/auth/refresh"
GRAPHQL_URL = "https://public-api.shiphero.com/graphql"


def refresh_access_token(refresh_token: str) -> str:
    resp = requests.post(AUTH_URL, json={"refresh_token": refresh_token}, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def execute_graphql(query, variables, token, max_retries, log):
    """POST a GraphQL query with retry/backoff on 429s and credit exhaustion."""
    for attempt in range(max_retries):
        resp = requests.post(
            GRAPHQL_URL,
            json={"query": query, "variables": variables},
            headers={"Authorization": f"Bearer {token}"},
            timeout=120,
        )
        if resp.status_code == 429:
            wait = min(2 ** attempt * 10, 300)
            log.warning(f"rate limited, sleeping {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("max retries exceeded")
```

### `shiphero/schema.py` — record shaping (no decorators)

```python
"""Turn raw GraphQL payloads into flat records matching the declared schema."""

SHIPMENTS_QUERY = """query Shipments($first:Int,$after:String,
  $dateFrom:ISODateTime,$dateTo:ISODateTime){ shipments(...) }"""   # full query

FIELD_PATH = ["data", "shipments", "data", "edges"]


def extract_nodes(response: dict) -> list[dict]:
    """Navigate field_path and unwrap GraphQL edge nodes into plain dicts."""
    cursor = response
    for key in FIELD_PATH:
        cursor = cursor.get(key, {})
    return [edge.get("node", edge) for edge in cursor if edge]


def page_info(response: dict) -> dict:
    cursor = response
    for key in FIELD_PATH[:-1]:        # parent of `edges`
        cursor = cursor.get(key, {})
    return cursor.get("pageInfo", {})
```

### `shiphero/source.py` — the streams (the entry point)

```python
"""ShipHero source: the @stream functions the engine discovers and runs."""
from datetime import timedelta
from dtex import stream

from .client import refresh_access_token, execute_graphql
from .schema import SHIPMENTS_QUERY, extract_nodes, page_info


def _date_windows(start, step_days):
    """Yield (from, to) windows of `step_days` from `start` to now."""
    from datetime import datetime, timezone
    end = datetime.now(timezone.utc)
    while start < end:
        nxt = min(start + timedelta(days=step_days), end)
        yield start, nxt
        start = nxt


@stream(name="shipments")
def shipments(config, state, cursor, log):
    """Extract ShipHero shipments incrementally, yielding batches of records."""
    # secrets and params arrive resolved on `config` — no env vars, no file reads
    token = refresh_access_token(config.secrets["refresh_token"])

    # incremental start: last persisted cursor minus the 2d lookback,
    # or initial_value ("2025-01-01") on the first ever run
    start = cursor.start_value()
    log.info(f"shipments: resuming from {start}")

    batch: list[dict] = []
    for win_from, win_to in _date_windows(start, config.step_days):
        after = None
        while True:
            variables = {
                "first": config.page_size, "after": after,
                "dateFrom": win_from.isoformat(), "dateTo": win_to.isoformat(),
            }
            resp = execute_graphql(
                SHIPMENTS_QUERY, variables, token, config.max_retries, log
            )
            for node in extract_nodes(resp):
                batch.append(node)
                cursor.observe(node["created_date"])     # advance the cursor
                if len(batch) >= config.batch_size:
                    yield batch                          # hand a batch to load
                    batch = []

            info = page_info(resp)
            if not info.get("hasNextPage"):
                break
            after = info.get("endCursor")

    if batch:
        yield batch        # final partial batch
```

### What lives where

| Concern | dtex location |
|---|---|
| Per-table extraction config | `register.yaml` `streams[]` |
| Connector knobs (`page_size`, `step_days`, …) | `register.yaml` `params` (defaults) |
| API auth & HTTP / GraphQL plumbing | `client.py` (plain module) |
| Record shaping (`extract_records`, `field_path` walk) | `schema.py` (plain module) |
| The per-stream extraction loop | `source.py` `@stream` function |
| Checkpoint read / write | the engine, via `cursor` + `_dtex_state` |
| Table creation & MERGE / upsert | the destination connector (e.g. `bigquery`) |
| Resolving `write_disposition: merge` to SQL | the engine + the destination |

Everything that would have been *plumbing* in a hand-rolled script —
checkpoint reads/writes, table creation, the MERGE — moves out of the
connector and into the engine or the destination. What remains in the source
body is only what is genuinely connector-specific: its query, its
pagination, its date-windowing. That is the measure of a good connector
body.

---

**Next:** chapter **06 — Project Anatomy** shows the dbt-style project a *user*
creates to hold connectors, destinations, and their credentials.
