# det

**det** ("data extraction tool") is an open-source, self-hosted Python
**extract-load (EL)** tool. It moves data from a **source** (an API, a
database, a file drop) into a **destination** (a warehouse, a database, an
object store) — and nothing more. Transformation is dbt's job. The pitch in
one line: **a CLI-first, dbt-shaped extract-load tool — pipelines are configs,
connectors are folders, no UI blackbox.** The #1 principle is to keep it as
simple as possible.

## Install

```sh
pip install det                          # every baked connector, ready
pip install 'det[gcs,s3]'                # add gs:// / s3:// filesystem reads
pip install 'det[gcp-secrets]'           # add the GCP Secret Manager resolver
```

`pip install det` ships every baked source and destination — DuckDB,
BigQuery, the filesystem source's local + Parquet path, the REST / Postgres
/ ShipHero / Stripe sources, the engine, the CLI. Extras stay opt-in for the
cloud-storage paths of the filesystem source (`gs://` / `s3://`) and for
secret managers (only relevant if your `profiles.yml` uses `secret://` URLs).

det requires Python 3.11+. It installs both a CLI (`det`) and an importable
library (`import det`).

## Usage

```sh
det init my_project                      # scaffold a project
cd my_project
det new source my_api                    # scaffold a source connector
det new config my_pipeline               # scaffold a pipeline config
det validate                             # check everything
det run -p my_pipeline                   # run the pipeline
det runs list -p my_pipeline             # show recent run history
```

A *pipeline* is one config file binding a source + a destination + a target +
params. Run it with `det run -p <config>`. The library equivalent is
`det.run(config="my_pipeline")` and returns a structured `RunResult`.

## What ships in v0.1

**Sources (baked):** `filesystem` (CSV/JSONL/Parquet from local, GCS, or S3),
`rest` (paginated REST APIs — 4 pagination strategies, 4 auth modes),
`postgres` (keyset pagination, no `OFFSET`), `shiphero` (GraphQL),
`stripe` (resource-as-stream over the REST API).

**Destinations (baked):** `duckdb` (zero-config dev default, all 5 capabilities),
`bigquery` (production warehouse, Parquet-staged via GCS + LOAD jobs, MERGE
upserts, smart cursor-based partitioning).

**Engine:** per-stream commit + atomic transactions (rollback on failure),
state in the destination's `_det_state` table, run records in `_det_runs`,
structured JSON-lines logs per run, secret redaction, schema evolution
(`evolve` default, `strict` opt-in).

## Documentation

The full design handbook lives in [`docs/`](./docs/README.md). Start with
[00 — Vision & Naming](./docs/00-vision-and-naming.md),
[02 — Architecture](./docs/02-architecture.md),
[06 — Project Anatomy](./docs/06-project-anatomy.md),
[12 — Configs](./docs/12-configs.md), and
[10 — Roadmap and Scope](./docs/10-roadmap-and-scope.md).

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
