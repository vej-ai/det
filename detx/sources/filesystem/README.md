# filesystem — flat-file source connector

A baked detx source that reads flat files (CSV, JSONL, Parquet) from a
local directory or an object-storage URI prefix. Zero-credential for local
reads; GCS / S3 use application default credentials (ADC) via the underlying
SDK.

## What it reads

Point `path` at either a local directory or a `gs://` / `s3://` URI prefix:

```yaml
params:
  path: /data/exports/        # local
  # path: gs://my-bucket/exports/
  # path: s3://my-bucket/exports/
  glob: "**/*.csv"            # recursive, fnmatch-compatible
  format: auto                # or: csv | jsonl | parquet
```

Files are sorted by their cursor key (file mtime ISO timestamp, or file name)
so runs are reproducible and the cursor advances monotonically.

## Incremental loading

Set an `incremental` block on the stream and the connector will skip files
whose cursor key is at or below the committed cursor — so a re-run reads only
files added since the last successful run. The cursor key lives on every
record as `_detx_file_cursor` (a synthetic field the source attaches), so
the engine's standard `Cursor` + `_detx_state` machinery just works.

* `cursor_strategy: mtime` (default) — file modification time, encoded as a
  UTC ISO 8601 string. Lex compare equals chronological compare.
* `cursor_strategy: name` — the file basename, lex-compared. Useful for
  time-prefixed exports like `2026-05-23T1200_orders.csv`.

Either way the cursor type is `string`; both lex-compare correctly.

## Supported URIs and install extras

| Scheme    | Backend          | Install                                    |
|-----------|------------------|--------------------------------------------|
| (no scheme) / `file://` | `LocalBackend`   | base (always available)      |
| `gs://`   | `GcsBackend`     | `pip install 'detx[gcs]'`                   |
| `s3://`   | `S3Backend`      | `pip install 'detx[s3]'`                    |

Parquet files work out of the box — `pyarrow` ships with the base install
(the BigQuery destination also needs it).

Cloud-storage SDKs (`gs://`, `s3://`) are the only filesystem-source pieces
behind extras, and they are lazy-imported — you only pay for what you use.
A missing optional dep raises an `ImportError` naming the extra.

## What a malformed file looks like

A file that fails to parse raises with the file path attached. The engine's
per-stream transaction rolls back the partial load, so a retry starts the
file cleanly.

## Schema

The example stream omits `schema:` — the engine infers from the first batch.
For production, declare a `schema` on the stream **and include
`_detx_file_cursor` (STRING)** in it (the contract requires any declared
schema to contain the cursor field, see `detx/types.py::StreamDef`).

## Secrets

None in v1. Local reads use OS credentials; remote reads use ADC. A future
revision may add explicit credential secrets per backend.
