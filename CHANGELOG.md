# Changelog

All notable changes to **detx** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For what is *planned* — versus what has shipped — see
[`docs/10-roadmap-and-scope.md`](./docs/10-roadmap-and-scope.md).

## [Unreleased]

### Added
- GitHub Actions CI: pytest matrix on Python 3.11/3.12/3.13, ruff + mypy lint.
- PyPI Trusted Publishing workflow triggered by `v*` tags.

## [0.1.0] — Unreleased

The first public release.

### Added

- **Engine.** Run lifecycle with discovery, config resolution, per-stream
  commit, transactional loads with rollback-on-failure, schema evolution
  (`evolve` default, `strict` opt-in), structured JSON-lines logs per run,
  a `_detx_runs` audit table, and pipeline-level parallel execution with
  per-destination concurrency caps.
- **CLI** (`detx`): `run`, `list`, `validate`, `init`, `new` (source /
  destination / config), `state`, `runs`, `secrets test`.
- **Library API.** `detx.run(config=...)` as the engine entry point;
  `@stream`, `@resource`, `@destination`, `Connector`, `stream_method`,
  and the contract types (`Capability`, `Schema`, `Field`, `Config`,
  `State`, `Cursor`, `Batch`, `StateRecord`) for connector authors.
- **Project layout.** dbt-style `detx_project.yml` + `profiles.yml`, with
  pipelines defined as configs under `configs/`. `detx init` scaffolds
  the layout; `detx new` scaffolds individual sources, destinations, and
  configs.
- **Baked source connectors.** `filesystem` (CSV / JSONL / Parquet from
  local, GCS, or S3 with `[gcs]` / `[s3]` extras), `rest` (paginated
  REST APIs — four pagination strategies, four auth modes), `postgres`
  (keyset pagination, no `OFFSET`), `shiphero` (GraphQL), `stripe`
  (resource-as-stream over the REST API).
- **Baked destination connectors.** `duckdb` (zero-config dev default,
  all five capabilities) and `bigquery` (production warehouse — Parquet
  staging via GCS plus `LOAD` jobs, `MERGE` upserts, smart cursor-based
  partitioning).
- **State and run records.** Engine state lives in the destination's
  `_detx_state` table alongside the data it tracks; per-run audit lives
  in `_detx_runs`.
- **Secret-manager resolvers.** Pluggable `secret://<scheme>/<path>` URL
  form plus three production resolvers — GCP Secret Manager
  (`detx[gcp-secrets]`), AWS Secrets Manager (`detx[aws-secrets]`), and
  HashiCorp Vault (`detx[vault]`) — each opt-in via extras. Custom
  resolvers register via entry-point or a project-local
  `detx_plugins.py`.

### Security

- **Redaction filter.** Secrets declared with `secret: true` in
  `register.yaml`, and any value resolved via `${env.X}` or `secret://`,
  are redacted to `***` in stdout, `.detx/logs/`, run records,
  `--dry-run` config dumps, and exception messages. Redaction is by
  value, not just by key.
- **Trust model.** The threat model for running third-party connector
  code in-process is documented in
  [`docs/08-security.md`](./docs/08-security.md). detx does not sandbox
  connector code in v1; the provenance / least-privilege guidance is
  spelled out so operators can plan accordingly.
- **Fresh-every-run secret resolution.** No on-disk cache of resolved
  secret values. The per-process resolver client is reused across calls
  within a single run, but every value is re-fetched on every run.
- **Vulnerability reporting.** [`SECURITY.md`](./SECURITY.md) documents
  the private-disclosure channel and response timelines.

[Unreleased]: https://github.com/vej-ai/detx/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/vej-ai/detx/releases/tag/v0.1.0
