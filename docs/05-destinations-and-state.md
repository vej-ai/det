# 05 — Destinations and State

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.
> Prerequisites: [03 — The Connector Contract](./03-connector-contract.md), [04 — The Connector Body](./04-connector-body.md).

This section specifies how det **loads** data and how it remembers **where it left off**. It is the architectural keystone of the tool: state is what makes incremental extraction correct, and the destination is where state lives.

> # NOTE: post-8.B, the *binding* of a source to a destination lives in a
> **pipeline config** (chapter 12), not in the source's `register.yaml`. The
> destination contract specified in this chapter — the `@destination` hooks,
> the capability tiers, the state design — is unchanged; only where the
> source decides which destination to write to changed. Connection params
> (Tier A destinations' DB endpoints and credentials) live in `profiles.yml`,
> destination-keyed (chapter 06).

---

## 1. A destination is a connector

There is no separate "destination plugin" abstraction. A destination is a connector folder, exactly like a source — same layout, same `register.yaml`, same decorator functions. This is the #1 simplicity principle in practice: one mental model, one set of authoring docs, one test harness.

```
bigquery/                  # a destination connector
  register.yaml
  destination.py           # @destination-decorated functions
  requirements.txt         # optional, extra deps
```

What makes a connector a *destination* rather than a *source* is the **direction of the contract** it implements. A source connector exposes `@stream` / `@resource` functions that **yield** records. A destination connector exposes `@destination`-decorated functions that **accept** records and persist them.

### 1.1 The `Destination` interface

A destination connector implements a small set of decorated functions. det discovers them by decorator, the same way it discovers `@stream` functions in a source.

```python
# bigquery/destination.py
from det import destination, Capability, Config, Batch, StreamMeta, StateRecord

@destination.capabilities
def capabilities() -> set[Capability]:
    """Declare what this destination can do. Drives engine behavior."""
    return {
        Capability.STATE,             # Tier A: can store state in itself
        Capability.MERGE,             # supports upsert write disposition
        Capability.SCHEMA_EVOLUTION,  # can ALTER TABLE ADD COLUMN
        Capability.RUN_RECORDS,       # hosts the _det_runs audit table
        # NOTE: TRANSACTIONAL_LOAD is intentionally NOT declared in v1 —
        # BigQuery's atomic unit is one LOAD / one MERGE, so the engine
        # gets per-batch atomicity (each batch commits cleanly; the
        # cursor only advances after all batches of a stream succeed).
        # The honest design — a buffer-then-MERGE-once alternative would
        # break the streaming-load promise. A future opt-in
        # `staged_merge: true` param could change this.
    }

@destination.open
def open(config: Config) -> "Connection":
    """Acquire a connection/handle. Called once per run."""
    ...

@destination.ensure_schema
def ensure_schema(conn, stream: StreamMeta) -> None:
    """Create the table if absent; ALTER it for additive schema changes."""
    ...

@destination.write_batch
def write_batch(conn, batch: Batch, stream: StreamMeta) -> int:
    """Persist one batch. Return rows written. Engine calls this repeatedly."""
    ...

@destination.commit_state
def commit_state(conn, run_id: str, records: list[StateRecord]) -> None:
    """Persist cursor state. Called ONLY after all batches succeed."""
    ...

@destination.read_state
def read_state(conn, connector: str) -> list[StateRecord]:
    """Load prior cursor state at the start of a run."""
    ...

@destination.close
def close(conn) -> None:
    """Flush and release. Always called, even on failure."""
    ...
```

`ensure_schema` and `write_batch` each receive a single **`StreamMeta`** — a frozen object carrying all per-stream metadata the hook needs (`table`, `write_disposition`, `schema`, `primary_key`, `partition_by`, `schema_contract`). New per-stream concerns are added as `StreamMeta` fields, never as new hook parameters, so the destination contract stays stable as the engine grows. The engine builds one `StreamMeta` per stream from the resolved `StreamDef`.

Only `capabilities`, `open`, `write_batch`, `ensure_schema`, and `close` are mandatory. `commit_state` / `read_state` are mandatory **only** if the destination declares `Capability.STATE`; otherwise the engine routes state to a companion state backend (see §6). `transaction` is mandatory **only** if the destination declares `Capability.TRANSACTIONAL_LOAD`. `write_run_record` is mandatory **only** if the destination declares `Capability.RUN_RECORDS` (docs/09 §4) — the engine calls it once per run, after streams finish and before `close`, with a fully-built `RunRecord`; without the capability the engine skips it and the per-run JSONL log file remains the durable history. Ten hooks total.

`@destination.transaction` is a **context-manager hook** — a destination that declares `Capability.TRANSACTIONAL_LOAD` provides it, and the engine enters it once per stream, wrapping that stream's `[write_batch… → commit_state]` block (but **not** `ensure_schema`, whose DDL may implicitly commit). On a clean exit the data and the advanced cursor flip atomically; on any exception the partial load rolls back. This is what makes an `append` stream crash-safe — without it, every mid-stream crash would leave half-written rows that the re-run duplicates. Per-stream scope matches det's per-stream commit model (§5.3).

```python
@destination.transaction
@contextmanager
def transaction(conn, stream: StreamMeta):
    conn.begin()
    try:
        yield
    except Exception:
        conn.rollback(); raise
    else:
        conn.commit()
```

The engine never calls these functions out of order. The lifecycle per run is:

```
open → read_state → [ ensure_schema → ⟨transaction: write_batch ... → commit_state⟩ ]* → write_run_record → close
```

`write_run_record` is the per-run audit row hook (docs/09 §4) — called once per run when the destination declares `Capability.RUN_RECORDS`. `close` is guaranteed to run even if any step raises.

---

## 2. Pre-baked destination catalog

det ships destinations **inside the `det` package**. They are referenced by short name in `profiles.yml` (`type: bigquery`) — no folder needed in the user's project. Users can also author **custom destinations** as project-local connector folders (§7).

| Destination | How it loads | Tier | v1? |
|---|---|---|---|
| **DuckDB** | Local `.duckdb` file; `INSERT` / `INSERT ... ON CONFLICT`. Zero-config dev default. | A | **v1** |
| **BigQuery** | Batches staged as Parquet to a temp GCS prefix, then `LOAD` job; `replace` uses `WRITE_TRUNCATE` on the first batch then `WRITE_APPEND`; `merge` loads to a per-batch staging table then runs `MERGE INTO ... ON pk`. Declares 4 of 5 capabilities (no `TRANSACTIONAL_LOAD` — see chapter 05 §5.3 and the destination README). | A | **v1** |
| **Postgres** | `COPY` into a staging table, then `INSERT`/`MERGE` into target. | A | v2 |
| **Snowflake** | `PUT` Parquet to internal stage, `COPY INTO`, merge via `MERGE`. | A | v2 |
| **ClickHouse** | Native protocol batch `INSERT`; merge via `ReplacingMergeTree` + `FINAL` or insert-then-dedup. | A | v2 |
| **Filesystem (GCS / S3 / local)** | Writes Parquet or JSONL objects under a prefix; one object per batch. | **B** | v2 |
| **SQLAlchemy (generic)** | Any SQLAlchemy-supported DB via `executemany` / bulk insert. Fallback for long-tail warehouses. | A | v2 |

> **v1 ships DuckDB + BigQuery only.** This keeps the v1 surface honest and testable. The rest are designed now (the interface must accommodate them) but land in v2. See [10 — Roadmap and Scope](./10-roadmap-and-scope.md).

### Tier definitions

- **Tier A — state-capable.** The destination can store rows in itself, so it owns the `_det_state` table. State and data live in the same system; a load and its state commit can be made consistent.
- **Tier B — stateless storage.** Object stores (GCS/S3) have no tables. They cannot answer "what was the last cursor value?" cheaply or transactionally. They require a **companion state backend** (§6).

This single distinction — driven by the `Capability.STATE` flag — is the only place destination heterogeneity leaks into the engine.

---

## 3. Schema handling

### 3.1 From declared schema to DDL

A source stream declares its schema (see [03 — The Connector Contract](./03-connector-contract.md)). det carries this as a `Schema` object: an ordered list of `(name, type, nullable)` fields, plus optional `primary_key`. The destination's `ensure_schema` translates `Schema` into native DDL.

det uses a small, **portable type system**. Connectors never emit native warehouse types directly:

| det type | BigQuery | DuckDB | Postgres | Snowflake |
|---|---|---|---|---|
| `string` | `STRING` | `VARCHAR` | `text` | `VARCHAR` |
| `int` | `INT64` | `BIGINT` | `bigint` | `NUMBER(38,0)` |
| `float` | `FLOAT64` | `DOUBLE` | `double precision` | `FLOAT` |
| `bool` | `BOOL` | `BOOLEAN` | `boolean` | `BOOLEAN` |
| `timestamp` | `TIMESTAMP` | `TIMESTAMP` | `timestamptz` | `TIMESTAMP_TZ` |
| `date` | `DATE` | `DATE` | `date` | `DATE` |
| `json` | `JSON` | `JSON` | `jsonb` | `VARIANT` |
| `bytes` | `BYTES` | `BLOB` | `bytea` | `BINARY` |

If a stream does not declare a schema, det **infers** one from the first batch and treats every field as nullable. Inference is convenient for prototyping but a declared schema is recommended for production — it makes schema drift a *decision*, not an accident.

### 3.2 Schema evolution policy

det keeps schema evolution deliberately minimal. The default policy:

- **Additive columns — automatic.** A new field appearing in the source is added with `ALTER TABLE ADD COLUMN`, nullable. Existing rows get `NULL`.
- **Widening type changes — automatic where the destination allows it** (`int` → `float`, `string` length). Done via the destination's native type-relaxation; skipped if unsupported.
- **Dropped columns — ignored.** A field that disappears from the source is left in the destination table (now always `NULL` for new rows). det never drops columns.
- **Incompatible type changes — hard error.** `string` → `int` on an existing column fails the run with a clear message. The fix is an explicit `--full-refresh` (recreates the table) or a manual migration.

This is governed by `Capability.SCHEMA_EVOLUTION`. A destination without it (rare) fails any run whose schema differs from the existing table.

> [Open question: should additive evolution be opt-in per stream via a `schema_contract: strict|evolve` setting in `register.yaml`? dbt-style strictness argues for `strict` as the default in production. Proposal: default `evolve`, allow `strict` opt-in. Decide before v1 freeze.]

### 3.3 Partitioning

Partitioning is the destination's physical-layout knob. On BigQuery a well-chosen partition cuts the scanned bytes of every query against the table (and therefore the bill) by orders of magnitude; on warehouses without native partitioning (DuckDB) the field is informational and the destination ignores it. det's contract is a single per-stream `partition_by` declaration in `register.yaml`, mirrored by an optional per-pipeline `partition_overrides:` block in a config; the engine resolves the two against the cursor type into a single `PartitionConfig` and hands it to `ensure_schema`.

**Which destinations honor it (today and roadmap):**

| Destination | Honored? | Mapping |
|---|---|---|
| BigQuery | Yes | `TIME_PARTITIONING(type, field)` / `RANGE_PARTITIONING(field, range)` |
| DuckDB | No | Ignored — DuckDB has no native table partitioning |
| Snowflake (future) | Planned | Clustering keys (v2+) |
| ClickHouse (future) | Planned | `PARTITION BY` (v2+) |

**Short form** (the existing column-name form):

```yaml
streams:
  - name: shipments
    partition_by: created_date   # defaults to TIME + DAY
```

**Long form** (new in stage 8c — for `range`, `ingestion`, or non-`day` granularities):

```yaml
streams:
  - name: charges
    partition_by:
      field: created          # the column; null for ingestion-time
      type: time              # time | range | ingestion
      granularity: day        # for time: hour | day | month | year (default: day)
  - name: events
    partition_by:
      field: ts
      type: range
      range:
        start: 0
        end: 10000000000
        interval: 86400
  - name: raw_logs
    partition_by:
      type: ingestion         # field omitted — BigQuery binds to _PARTITIONTIME
```

**Cursor-based auto-default** (when `partition_by` is unset):

| Cursor type | Default |
|---|---|
| `timestamp` | `TIME` partitioning on the cursor field by `DAY` |
| `date` | `TIME` partitioning on the cursor field by `DAY` |
| `int` | unpartitioned + WARNING — declare `partition_by:` explicitly with `type=range` or `type=ingestion` |
| `string` | unpartitioned + WARNING — same suggestion |

A full-refresh stream (no `incremental` block) without a declared `partition_by` is left unpartitioned silently — there is no cursor to anchor an auto-default to.

The resolved partition is logged on the run's `stream_start` event (chapter 9) so the choice is observable.

**Per-config overrides — `partition_overrides:`:** a `configs/<name>.yml` may override the source's `partition_by` on a per-stream basis. Same precedence as other per-config layers — the config wins on conflict.

```yaml
# configs/stripe_bq.yml
name: stripe_bq
source: stripe
destination: bigquery
target: dev
partition_overrides:
  charges:
    field: created
    type: range
    range: {start: 1577836800, end: 1893456000, interval: 86400}
  invoices: created      # short-form override per stream
```

The full resolution chain (highest → lowest precedence): `partition_overrides[stream]` → `StreamDef.partition_by` → cursor-based auto-default → `None` (no partition).

**Backward compat for the short form on a non-time column:** several pre-existing sources (Stripe, ShipHero) declare a short-form `partition_by` against a column that is an INTEGER (a Unix epoch under `cursor_type: int`) or otherwise not time-typed. Before stage 8c the destination ignored `partition_by` entirely; once the BigQuery destination starts honoring it, naively mapping the short form to `TIME+DAY` on an INTEGER column would crash every existing run. The engine therefore **degrades** a short-form declaration to "no partition + WARNING" when the column type isn't a `TIMESTAMP` / `DATE` (and emits the explicit-declaration suggestion). Long-form declarations and per-config overrides are *never* degraded — those are explicit decisions.

**Partition drift on existing tables — hard error.** BigQuery cannot change an existing table's partitioning in place. If a config requests a partition spec that does not match the existing table, `ensure_schema` raises with a message naming both specs and the suggested operator action:

```
table 'charges' already exists with partitioning=created (TIME/DAY); new
config says created (TIME/HOUR). BigQuery cannot change an existing table's
partitioning in place. To resolve: either (a) run `det state reset -p
<config> --recreate-table` after backing up the table to recreate it with
the new partition spec, or (b) change the config to match the existing
partition spec.
```

(The `--recreate-table` flag on `det state reset` is a planned future stage; today's manual equivalent is `CREATE TABLE bak AS SELECT * ...` → `DROP TABLE` → `det state reset -p <config>` → re-run.)

This also fires when the existing table is **unpartitioned** and the config requests a partition (and vice versa): silently ignoring the conflict would let writes drift from intent, so the rule is symmetric.

---

## 4. Write dispositions

Every stream declares a `write_disposition`. The engine passes it to `write_batch`; the destination implements it natively.

| Disposition | Meaning | Requires |
|---|---|---|
| `append` | Add all rows. Duplicates are the source's problem. | — |
| `merge` | Upsert: insert new rows, overwrite existing rows matched on `primary_key`. | `primary_key` declared; `Capability.MERGE` |
| `replace` | Truncate the table, then load. Full snapshot semantics. | — |

How each destination implements them:

- **Warehouses (BQ / Snowflake / Postgres):** `append` = plain insert. `merge` = stage the batch, run a native `MERGE` keyed on `primary_key`. `replace` = load into a staging table for the *whole run*, then atomically swap (`CREATE OR REPLACE` / rename) so readers never see a half-empty table.
- **ClickHouse:** `merge` uses `ReplacingMergeTree` keyed on `primary_key` (eventual dedup) or insert-then-`OPTIMIZE`. `replace` swaps partitions.
- **DuckDB:** `merge` = `INSERT ... ON CONFLICT DO UPDATE`. `replace` = `CREATE OR REPLACE TABLE`.
- **Filesystem (Tier B):** `append` writes new objects. `merge` is **not supported** — object stores can't update in place; the engine rejects `merge` against a destination lacking `Capability.MERGE`. `replace` deletes the prefix, then writes. (Merge-on-object-storage is a v2+ topic, likely via Iceberg/Delta — explicitly out of v1 scope.)

If a stream requests a disposition the destination cannot satisfy, the run **fails fast at planning time**, before any extraction — never mid-load.

---

## 5. State design

State is what makes incremental loads correct. det stores it **in the destination** (Tier A) so that data and the record of "what we loaded" live in one system and advance together.

### 5.1 The `_det_state` table

One row per `(connector, stream)`. The cursor value is stored as JSON so it can hold a timestamp, an integer ID, an opaque pagination token, or a composite. This is the canonical schema — eight columns; `det/types.py` is the source of truth and this table follows it.

| Column | Type | Description |
|---|---|---|
| `connector` | `string` | Connector name, e.g. `stripe`. |
| `stream` | `string` | Stream name within the connector, e.g. `charges`. |
| `cursor_value` | `json` | Last successfully loaded cursor value. The resume point. `NULL` for full-refresh streams. |
| `cursor_type` | `string` | `timestamp` / `date` / `int` / `string` — how to deserialize `cursor_value`. `NULL` when no cursor. |
| `state_blob` | `json` | The per-stream `State` scratch space (free-form key/value), persisted between runs. |
| `last_run_id` | `string` | `run_id` of the run that last advanced this row. Joins to `_det_runs` for the full audit chain. |
| `rows_total` | `int` | Cumulative rows ever loaded for this stream (informational). |
| `updated_at` | `timestamp` | When this row was last committed. |

`cursor_field` is **not** a column — it is recoverable from the stream's manifest. `last_run_at` is **not** a column — it is recoverable by joining `_det_runs` on `last_run_id`.

Primary key: `(connector, stream)`. The table is created lazily by `ensure_schema` on first run, in the same dataset/schema as the loaded tables, prefixed `_det_` so it sorts away from user tables.

### 5.2 The `StateRecord`

In the library, state is a typed object passed between the engine and the destination — one `StateRecord` per `_det_state` row. It is **mutable**: the engine advances `rows_total` / `updated_at` in place across a run, then `to_row()` / `from_row()` form the persistence boundary.

```python
@dataclass
class StateRecord:
    connector: str
    stream: str
    cursor_value: Any | None        # JSON-serializable
    cursor_type: CursorType | None
    state_blob: Mapping[str, Any]
    last_run_id: str | None
    rows_total: int
    updated_at: datetime | None
```

### 5.3 Lifecycle and transactionality

The non-negotiable rule: **state is committed only after the data load fully succeeds.** This guarantees at-least-once delivery with no silent data loss. If a run crashes mid-load, state is unchanged, and the next run re-extracts from the last *committed* cursor. Combined with `merge` dispositions, re-extraction is idempotent; with `append`, re-extraction may duplicate rows on the overlap window — documented and expected.

```
1. open + read_state          → engine learns each stream's resume cursor
2. extract + write_batch...    → all batches for all streams persisted
3. commit_state                → _det_state updated, one transaction if possible
4. close
```

`commit_state` receives **all** stream state records for the run and writes them in a single transaction where the destination supports it (`Capability.TRANSACTIONAL_LOAD`). For `replace`-disposition streams using staging-table swap, the state commit is folded into the same transaction as the swap, so data and cursor flip together.

Crash semantics: if step 3 fails after step 2 succeeded, the run is reported `failed`; the next run re-loads the same window. Safe for `merge`/`replace`, at-least-once for `append`.

> # NOTE: BigQuery in v1 does NOT declare `TRANSACTIONAL_LOAD`. BigQuery has
> no general `BEGIN`/`COMMIT` spanning multiple jobs — each LOAD / MERGE is
> the natural atomic unit. det chooses per-batch atomicity (the engine wraps
> each stream in `nullcontext()`) over a buffer-then-MERGE-once alternative
> that would break the streaming-load promise. A future opt-in
> `staged_merge: true` param could change this.

#### BigQuery — the load mechanism in detail

This is non-obvious for someone reading the destination contract for the
first time. The pre-baked BigQuery destination implements `write_batch`
per the catalog row above:

1. Serialize the batch to Parquet bytes (in-memory, via `pyarrow`).
2. Upload to `gs://{staging_bucket}/{staging_prefix}/{run_suffix}/{table}/batch-{uuid}.parquet`
   (object names use opaque uuids so an HTTP error message never leaks
   record content; the per-run `run_suffix` keeps concurrent runs apart).
3. Trigger a BigQuery `LOAD` job from that URI:
   - `append` → target table, `WRITE_APPEND`.
   - `replace`, first batch of run → target table, `WRITE_TRUNCATE`;
     subsequent batches → `WRITE_APPEND` (so a multi-batch replace
     stream truncates exactly once).
   - `merge` → a per-batch staging table `{target}__staging_{run_suffix}_{uuid}`,
     `WRITE_TRUNCATE`; then run `MERGE INTO target USING staging ON pk
     WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...`; then drop
     the staging table in a `finally`.
4. Wait for job completion with exponential-backoff retry on transient
   `429 / 500 / 502 / 503 / 504`. A non-retryable 4xx surfaces
   immediately.
5. On success: delete the staging Parquet object from GCS.
   On failure: **leave it in place** for forensics — an operator can
   inspect the source the failed LOAD pulled from. The MERGE staging
   *table* is always dropped (success or failure) so a failed run does
   not leak per-batch tables in BigQuery.

Engine-owned tables on BigQuery: `_det_state` and `_det_runs`, in the
destination's dataset, prefixed `_det_` so they sort away from user
tables. Same column names + types as the DuckDB destination so an admin
/ UI / cross-warehouse tool queries both backends identically.

> [Open question: per-batch ("streaming") state commit for very large backfills, so a crash 90% through doesn't restart from zero. This trades the clean all-or-nothing model for partial progress. Proposal: keep whole-run commit in v1; add opt-in `state_granularity: batch` in v2 for streams that declare a monotonic cursor.]

### 5.4 The capability-tier model and `state_backend()`

Tier B destinations (object storage) cannot host `_det_state`. The engine resolves this through a **state backend** — a small interface, separate from the destination, that owns only state I/O:

```python
class StateBackend(Protocol):
    def read_state(self, connector: str) -> list[StateRecord]: ...
    def commit_state(self, run_id: str, records: list[StateRecord]) -> None: ...
```

The `Destination` interface exposes:

```python
@destination.state_backend
def state_backend(conn, config: dict) -> StateBackend | None:
    """Return the state backend for this destination, or None if it is
    state-capable itself (Tier A)."""
```

Resolution at run start:

1. If the destination declares `Capability.STATE` → it **is** its own state backend; the engine calls `read_state` / `commit_state` directly on it.
2. Otherwise the engine calls `state_backend(conn, config)` to obtain one.

The **default** Tier-B backend is the **sidecar JSON file** — `_det_state.json` written next to the data in the same bucket/prefix:

```
gs://my-bucket/exports/stripe/charges/part-0001.parquet
gs://my-bucket/exports/_det_state.json     ← sidecar state
```

The sidecar is read at run start and rewritten (whole file, last-write-wins) at `commit_state`. It is simple, needs no extra infrastructure, and is co-located with the data. Its limit is **concurrency**: two runs writing the same bucket can clobber each other's state. det mitigates with a best-effort lock object (`_det_state.lock`) and documents that concurrent runs to one Tier-B prefix are unsupported.

For users who need stronger guarantees, `StateBackend` is **pluggable** — `profiles.yml` may point a Tier-B destination at an explicit backend:

```yaml
# profiles.yml — Tier B destination with an explicit state backend
exports:
  type: filesystem
  bucket: gs://my-bucket/exports
  format: parquet
  state_backend:
    type: postgres            # reuse a small Postgres for state only
    dsn: ${SIMPLE_E_STATE_DSN}
```

A `postgres` / `duckdb` / `dynamodb` state backend gives transactional, concurrency-safe state without forcing all the *data* into a warehouse. In v1 only the **sidecar JSON** backend ships; the pluggable interface exists so external backends can be added without an engine change.

---

## 6. Custom destination authoring — worked example

Custom destinations live in the user's project as a connector folder and are picked up automatically — same discovery as a source connector. By convention they sit under `destinations/` rather than `connectors/`, but this is a readability convention only: both directories are scanned via `connector_paths`, and what makes a connector a destination is `kind: destination`, not its folder (see [06 — Project Anatomy](./06-project-anatomy.md)). Here is a minimal **append-only HTTP destination** that POSTs each batch to a webhook.

```
my_project/
  destinations/
    webhook_sink/
      register.yaml
      destination.py
```

```yaml
# destinations/webhook_sink/register.yaml
name: webhook_sink
kind: destination
description: POST each batch as JSON to an HTTP endpoint.
config:
  url:     { required: true }
  headers: { required: false, default: {} }
```

```python
# destinations/webhook_sink/destination.py
import json
import urllib.request
from det import destination, Capability

@destination.capabilities
def capabilities():
    # No STATE, no MERGE, no SCHEMA_EVOLUTION — append-only, stateless.
    return set()

@destination.open
def open(config):
    return {"url": config["url"], "headers": config.get("headers", {})}

@destination.ensure_schema
def ensure_schema(conn, stream):
    # No tables to create — the endpoint is schemaless. No-op.
    pass

@destination.write_batch
def write_batch(conn, batch, stream):
    if stream.write_disposition.value != "append":
        raise ValueError("webhook_sink supports only append")
    payload = json.dumps({"table": stream.table, "rows": batch}).encode()
    req = urllib.request.Request(
        conn["url"], data=payload, method="POST",
        headers={"Content-Type": "application/json", **conn["headers"]},
    )
    urllib.request.urlopen(req).read()
    return len(batch)

@destination.close
def close(conn):
    pass  # nothing to release
```

Because `capabilities()` returns an empty set, the engine knows this destination is Tier B with no merge support. It will:

- reject any stream that requests `merge` or `replace` at planning time, with a clear message;
- require a state backend for incremental streams — since none is configured, it falls back to the **sidecar** backend, which a pure-HTTP destination has no place to write. So this destination is only valid for **full-refresh** streams. det surfaces exactly that constraint at planning time rather than failing mysteriously mid-run.

This is the whole point of the capability model: a destination author declares what they can do in one function, and the engine does the rest.

### Reference

- Connector folder layout & `register.yaml` schema → [03 — The Connector Contract](./03-connector-contract.md)
- Targets, `profiles.yml`, config precedence → [07 — CLI and Library API](./07-cli-and-library-api.md)
- Credentials for destinations → [08 — Security](./08-security.md)
- Run records (`_det_runs`) → [09 — Logging and Observability](./09-logging-and-observability.md)
