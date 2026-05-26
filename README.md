# detx

[![PyPI version](https://img.shields.io/pypi/v/detx.svg)](https://pypi.org/project/detx/)
[![Python versions](https://img.shields.io/pypi/pyversions/detx.svg)](https://pypi.org/project/detx/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![CI](https://github.com/vej-ai/detx/actions/workflows/ci.yml/badge.svg)](https://github.com/vej-ai/detx/actions/workflows/ci.yml)

**detx** ("data extraction tool") is an open-source, self-hosted Python
**extract-load (EL)** tool. It moves data from a **source** (an API, a
database, a file drop) into a **destination** (a warehouse, a database, an
object store) — and nothing more. Transformation is dbt's job.

The pitch in one line: **a CLI-first, dbt-shaped extract-load tool —
pipelines are configs, connectors are folders, no UI blackbox.** The #1
principle is to keep it as simple as possible.

## Install

```sh
pip install detx                          # every baked connector, ready
pip install 'detx[gcs,s3]'                # add gs:// / s3:// filesystem reads
pip install 'detx[gcp-secrets]'           # add the GCP Secret Manager resolver
pip install 'detx[aws-secrets]'           # add the AWS Secrets Manager resolver
pip install 'detx[vault]'                 # add the HashiCorp Vault resolver
```

`pip install detx` ships every baked source and destination — DuckDB,
BigQuery, the filesystem source's local + Parquet path, the REST / Postgres
/ ShipHero / Stripe sources, the engine, the CLI. Extras stay opt-in for the
cloud-storage paths of the filesystem source (`gs://` / `s3://`) and for
secret managers (only relevant if your `profiles.yml` uses `secret://` URLs).

detx requires Python 3.11+. It installs both a CLI (`detx`) and an importable
library (`import detx`).

## Usage

```sh
detx init my_project                      # scaffold a project
cd my_project
detx new source my_api                    # scaffold a source connector
detx new config my_pipeline               # scaffold a pipeline config
detx validate                             # check everything
detx run -p my_pipeline                   # run the pipeline
detx runs list -p my_pipeline             # show recent run history
```

A *pipeline* is one config file binding a source + a destination + a target +
params. Run it with `detx run -p <config>`. The library equivalent is
`detx.run(config="my_pipeline")` and returns a structured `RunResult`.

## Pre-baked connectors

**Sources:** `filesystem` (CSV/JSONL/Parquet from local, GCS, or S3),
`rest` (paginated REST APIs — 4 pagination strategies, 4 auth modes),
`postgres` (keyset pagination, no `OFFSET`), `shiphero` (GraphQL),
`stripe` (resource-as-stream over the REST API).

**Destinations:** `duckdb` (zero-config dev default, all 5 capabilities) and
`bigquery` (production warehouse — Parquet-staged via GCS + LOAD jobs,
MERGE upserts, cursor-based partitioning).

**Engine:** per-stream commit + atomic transactions (rollback on failure),
state in the destination's `_detx_state` table, run records in `_detx_runs`,
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
