# DuckDB destination — dtex v1

The **zero-config dev default**. Writes batches to a local DuckDB file
(`append` / `merge` / `replace`) — no external service to set up, no
credentials, no network. Tier A: DuckDB hosts its own state and audit
tables (`_dtex_state`, `_dtex_runs`) alongside the loaded data, so a
fresh checkout of a project resumes correctly with no extra files.

See [docs/05 — Destinations & State](../../../docs/05-destinations-and-state.md)
for the destination contract; this README is the operator's quick reference.

## Install

```bash
pip install dtex
```

DuckDB ships in dtex's base dependencies — no extras needed.

## Config

Default profile is fine for most projects:

```yaml
# profiles.yml
duckdb:
  default_target: dev
  targets:
    dev:
      path: ".dtex/warehouse.duckdb"     # the default — gitignored by `dtex init`
    prod:
      path: "/var/data/dtex/warehouse.duckdb"
```

The `.duckdb` file is created on first run. `dtex init` scaffolds the
above + a `.gitignore` rule for `.dtex/`. For ephemeral / test runs use
`path: ":memory:"`.

### Per-pipeline schema override

A config can land each pipeline's tables under a DuckDB schema by setting
`destination_params.dataset`:

```yaml
# configs/my_pipeline.yml
destination_params:
  dataset: my_pipeline     # creates schema if absent; all tables land here
```

Without `dataset` the destination uses DuckDB's default schema.

## Capabilities

DuckDB implements all five destination capabilities:

| Capability | What it does |
|---|---|
| `append` | INSERT rows as-is. |
| `merge` | UPSERT on `primary_key` (requires non-empty PK). |
| `replace` | TRUNCATE then INSERT (full-refresh streams). |
| `state` | Stores `_dtex_state` rows for cursor resume. |
| `run_records` | Writes `_dtex_runs` per pipeline invocation. |

## State + run records

Two tables live alongside the loaded data:

- **`_dtex_state`** — one row per (connector, stream) with the committed
  cursor value, last run id, and cumulative `rows_total`. Read at every
  run to resume incremental streams.
- **`_dtex_runs`** — one row per pipeline invocation with start/end time,
  status, rows loaded, and (for failures) the error type + message.
  Query history with `dtex runs list` or directly.

## Operational notes

- **Concurrent writes are NOT supported.** DuckDB locks the database file
  for the duration of a write transaction; two `dtex run` processes
  targeting the same file will serialize at best, error at worst. For
  production multi-pipeline use, target BigQuery instead.
- **Single-machine.** A DuckDB file lives on one disk. dtex doesn't
  replicate it; if you want a queryable copy on another box, copy the
  file or switch to BigQuery.
- **Memory.** DuckDB streams query results, so MERGE operations work fine
  on large tables — but VERY large in-memory ANALYZE / aggregation jobs
  can OOM. Adjust `PRAGMA memory_limit` via a connection callback if you
  hit it.

## What's not in v1

- No clustering / partitioning at the storage layer (DuckDB doesn't
  expose physical partitioning the way warehouses do; `partition_by`
  declarations on streams are recorded in metadata but don't influence
  the physical layout). Use BigQuery if cursor-based partitioning is a
  hard requirement.
- No cross-database joins from the warehouse file (a pipeline can read
  via the `filesystem` source's DuckDB-backed Parquet paths, but the
  destination itself doesn't host federation).
