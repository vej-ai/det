# BigQuery destination — det v1

The production warehouse det was always meant to land in. Tier A: BigQuery
hosts its own state and audit tables (`_det_state`, `_det_runs`) alongside
the loaded data, so a fresh checkout of a project resumes correctly with no
local files.

See [docs/05 — Destinations & State](../../../docs/05-destinations-and-state.md)
for the destination contract; this README is the operator's quick reference.

## Install

```bash
pip install "det[bigquery]"
```

The `[bigquery]` extra pulls in `google-cloud-bigquery`, `google-cloud-storage`
and `pyarrow`. A base `det` install does NOT load these — the BigQuery
destination's modules import the SDKs lazily, so `import det` and the rest of
the package work without the extra.

## Auth

Two paths, in priority order:

1. **Service-account JSON file** — set `credentials_path: /path/to/sa.json`
   on the profile. Useful for non-interactive deploys (cron, CI).
2. **Application Default Credentials (ADC)** — leave `credentials_path`
   empty (the default). det then picks up `GOOGLE_APPLICATION_CREDENTIALS`,
   or `gcloud auth application-default login` credentials.

The service account needs:

- `roles/bigquery.dataEditor` on the destination dataset (LOAD, MERGE, table
  create / alter).
- `roles/bigquery.jobUser` on the project (run LOAD + query jobs).
- `roles/storage.objectAdmin` on the **staging bucket** only (create + delete
  the per-batch Parquet objects). NOT on any other bucket.

det never logs credentials. Service-account paths may appear in error
messages ("file not found") but never the contents.

## Params (profiles.yml)

```yaml
bigquery:
  default_target: prod
  targets:
    prod:
      project: my-gcp-project          # REQUIRED
      dataset: analytics_raw           # REQUIRED (created lazily)
      staging_bucket: my-det-staging   # REQUIRED (you create it; det never does)
      location: US                     # default "US"
      staging_prefix: det/staging      # default
      credentials_path: ""             # empty = ADC
      job_timeout_seconds: 300         # per BigQuery job
      retry_max_attempts: 5            # transient 429/5xx retries
      retry_backoff_seconds: 1.0       # exponential base
```

The staging bucket is your concern: pick a region close to the BigQuery
dataset, set a 1-day lifecycle rule on the staging prefix as a safety net
(det cleans up on success; on failure it leaves the Parquet for forensics).

## The load mechanism

This is the non-obvious bit; it is worth knowing if you ever inspect a
failed run.

Per batch (one `write_batch` call):

1. Serialize the batch to Parquet bytes (in-memory, via `pyarrow`).
2. Upload to `gs://{staging_bucket}/{staging_prefix}/{run_suffix}/{table}/batch-{uuid}.parquet`.
3. Trigger a BigQuery `LOAD` job from that URI. The write disposition
   depends on the stream's `write_disposition`:

   | `write_disposition` | LOAD target | LOAD write disposition | Post-LOAD step |
   |---|---|---|---|
   | `append` | the target table | `WRITE_APPEND` | — |
   | `replace` (first batch of run) | the target table | `WRITE_TRUNCATE` | — |
   | `replace` (later batches) | the target table | `WRITE_APPEND` | — |
   | `merge` | a per-batch staging table | `WRITE_TRUNCATE` | `MERGE INTO target USING staging ON pk WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...`, then drop staging table |

4. Wait for the job, with exponential-backoff retry on transient
   `429 / 500 / 502 / 503 / 504`. A real 4xx (bad SQL, missing perms)
   surfaces immediately.
5. On success: delete the staging Parquet object from GCS.
   On failure: **leave it in place** so an operator can inspect the source.
   MERGE staging *tables* are always dropped (success or failure) so a
   failed run does not leak per-batch tables.

The MERGE staging table name embeds `{run_suffix}_{uuid}` so two concurrent
runs (or two sibling batches inside one run) never collide.

## Transactionality — what BigQuery does and does not promise

BigQuery has no general `BEGIN` / `COMMIT` spanning multiple jobs. det v1
therefore makes the honest choice: **per-batch atomicity** is the natural
granularity. Each LOAD / MERGE either commits cleanly or doesn't; a crash
mid-stream leaves landed batches in place, but the cursor never advances
(the engine commits state only *after* all batches of a stream succeed).
On the next run the stream resumes from the last committed cursor. For
`append` streams this means re-extraction of the overlap window can
duplicate rows — same caveat as DuckDB without TRANSACTIONAL_LOAD, same
caveat as the filesystem destination. For `merge` and `replace` it is
idempotent.

Capabilities the destination declares (4 of 5):
`STATE`, `MERGE`, `SCHEMA_EVOLUTION`, `RUN_RECORDS`. **NOT
`TRANSACTIONAL_LOAD`** — see the `@destination.capabilities` and module
docstrings for the design rationale (a v2 opt-in `staged_merge: true`
param could change this).

## Schema evolution

`evolve` (the default) → a new field appearing on the source is added
with `client.update_table(table, ["schema"])` (BigQuery's additive ALTER
equivalent). Existing rows get `NULL`. Type widening and column drops
follow the rules in [docs/05 §3.2](../../../docs/05-destinations-and-state.md#32-schema-evolution-policy).

`strict` → any schema difference fails the run, before any data lands.
The engine enforces the contract; `ensure_schema` itself is always
additive.

## Partitioning

This destination honors `partition_by` declarations on a stream (or
`partition_overrides:` in a config — see [docs/05 §3.3](../../../docs/05-destinations-and-state.md#33-partitioning) for the full chain).
The engine resolves the per-stream declaration + the per-config override +
the cursor-based auto-default into a single `PartitionConfig` and hands it
to `ensure_schema`. The destination then sets either:

- `table.time_partitioning = TimePartitioning(type_=DAY|HOUR|..., field=...)`
  for `type=time` (also for `type=ingestion`, with `field=None` so BigQuery
  binds the `_PARTITIONTIME` pseudo-column), or
- `table.range_partitioning = RangePartitioning(field=..., range_=PartitionRange(start, end, interval))`
  for `type=range`.

The partition spec is **fixed at table-create time**. BigQuery cannot change
a table's partitioning in place, so any subsequent `ensure_schema` call
whose resolved `PartitionConfig` does not exactly match the existing
table's partitioning **raises `PartitionDriftError`** — a clear
`RuntimeError` subclass whose message names both partition specs and the
suggested operator action (today: back the table up + drop it + run
`det state reset -p <config>`; tomorrow: `det state reset --recreate-table`,
which is wired into the message text now so it stays actionable once
the flag lands).

For the cursor-based auto-default policy (`timestamp` / `date` →
`TIME+DAY`; `int` / `string` → unpartitioned + WARNING), see
[docs/05 §3.3](../../../docs/05-destinations-and-state.md#33-partitioning).

## Engine tables this destination creates

- `_det_state` — one row per `(connector, stream)` resume point. Eight
  columns, identical names + types as the DuckDB destination so an
  admin / UI / cross-warehouse tool queries both backends identically.
- `_det_runs` — one row per run; `run_id` is the upsert key.
  ([docs/09 §4](../../../docs/09-logging-and-observability.md))

Both prefixed `_det_` so they sort away from user tables.
