# dtex

[![PyPI version](https://img.shields.io/pypi/v/dtex.svg)](https://pypi.org/project/dtex/)
[![Python versions](https://img.shields.io/pypi/pyversions/dtex.svg)](https://pypi.org/project/dtex/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/vej-ai/dtex/actions/workflows/ci.yml/badge.svg)](https://github.com/vej-ai/dtex/actions/workflows/ci.yml)

**dtex** ("data extraction tool") is an open-source, self-hosted Python
**extract-load (EL)** tool. It moves data from a **source** (an API, a
database, a file drop) into a **destination** (a warehouse, a database, an
object store) — and nothing more. Transformation is dbt's job.

The pitch in one line: **a CLI-first, dbt-shaped extract-load tool —
pipelines are configs, connectors are folders, no UI blackbox.** The #1
principle is to keep it as simple as possible.

## Install

```sh
pip install dtex                          # every baked connector, ready
pip install 'dtex[gcs,s3]'                # add gs:// / s3:// filesystem reads
pip install 'dtex[gcp-secrets]'           # add the GCP Secret Manager resolver
pip install 'dtex[aws-secrets]'           # add the AWS Secrets Manager resolver
pip install 'dtex[vault]'                 # add the HashiCorp Vault resolver
```

`pip install dtex` ships every baked source and destination — DuckDB,
BigQuery, the filesystem source's local + Parquet path, the REST / Postgres
/ ShipHero / Stripe sources, the engine, the CLI. Extras stay opt-in for the
cloud-storage paths of the filesystem source (`gs://` / `s3://`) and for
secret managers (only relevant if your `profiles.yml` uses `secret://` URLs).

dtex requires Python 3.11+. It installs both a CLI (`dtex`) and an importable
library (`import dtex`).

## Usage

```sh
dtex init my_project                      # scaffold a project
cd my_project
dtex new source my_api                    # scaffold a source connector
dtex new config my_pipeline               # scaffold a pipeline config
dtex validate                             # check everything
dtex run -p my_pipeline                   # run the pipeline
dtex runs list -p my_pipeline             # show recent run history
```

A *pipeline* is one config file binding a source + a destination + a target +
params. Run it with `dtex run -p <config>`. The library equivalent is
`dtex.run(config="my_pipeline")` and returns a structured `RunResult`.

## Pre-baked connectors

**Sources:** `filesystem` (CSV/JSONL/Parquet from local, GCS, or S3),
`rest` (paginated REST APIs — 4 pagination strategies, 4 auth modes),
`postgres` (keyset pagination, no `OFFSET`), `shiphero` (GraphQL),
`stripe` (resource-as-stream over the REST API).

**Destinations:** `duckdb` (zero-config dev default, all 5 capabilities) and
`bigquery` (production warehouse — Parquet-staged via GCS + LOAD jobs,
MERGE upserts, cursor-based partitioning).

**Engine:** per-stream commit + atomic transactions (rollback on failure),
state in the destination's `_dtex_state` table, run records in `_dtex_runs`,
structured JSON-lines logs per run, secret redaction, schema evolution
(`evolve` default, `strict` opt-in), pipeline-level parallelism with
per-destination caps.

**Secret managers:** GCP Secret Manager, AWS Secrets Manager, HashiCorp
Vault — each as an opt-in extra.

## Documentation

The full design handbook lives in [`docs/`](./docs/README.md). Start with
[00 — Vision & Naming](./docs/00-vision-and-naming.md),
[02 — Architecture](./docs/02-architecture.md),
[06 — Project Anatomy](./docs/06-project-anatomy.md),
[12 — Configs](./docs/12-configs.md), and
[10 — Roadmap and Scope](./docs/10-roadmap-and-scope.md).

## Security · Contributing · Code of Conduct

- [Security policy](./SECURITY.md) — how to report a vulnerability.
- [Contributing](./CONTRIBUTING.md) — dev setup, PR process, how to add a connector.
- [Code of Conduct](./CODE_OF_CONDUCT.md).
- [Changelog](./CHANGELOG.md).

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
