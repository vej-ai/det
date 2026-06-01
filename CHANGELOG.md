# Changelog

All notable changes to **dtex** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For what is *planned* — versus what has shipped — see
[`docs/10-roadmap-and-scope.md`](./docs/10-roadmap-and-scope.md).

## [Unreleased]

## [0.1.5] — 2026-06-01

### Added
- **Engine NORMALIZE step now coerces values to declared schema types.**
  The docs have always described the engine's pipeline as
  "extract → normalize → load" with per-type value coercion as the engine's
  job. In practice the engine only did *schema* normalization (declared vs
  inferred; strict vs evolve) and passed dicts through to the destination
  unchanged — a connector yielding all-string records (a CSV-backed source
  like the Stripe Sigma connector) crashed the BigQuery destination with
  `ArrowInvalid: Could not convert '1599' with type str: tried to convert
  to int64`. The fix lives in the engine as a new
  `dtex.engine.normalize.coerce_value` / `normalize_batch` pair: every cell
  in every batch is coerced to the canonical Python representation of its
  declared `FieldType` before reaching `write_batch`. Per-type rules cover
  the common alternate input shapes (digit strings → `INTEGER`,
  `true`/`false`/`yes`/`no`/`1`/`0` → `BOOLEAN`, ISO-8601 / Unix-epoch →
  tz-aware UTC `datetime`, base64 / utf-8 → `BYTES`, etc.). Empty string
  becomes `None` for non-`STRING` types (the CSV "no value" idiom).
  Uncoercible values raise the new `dtex.CoercionError` (a `ValueError`
  subclass) naming the column, value, source type, and target
  `FieldType` — and roll back the partial load via the destination's
  `transaction` hook. Connectors that already yield canonical Python
  types see zero behavior change. Destinations no longer need per-
  destination coercion: by the time they see a batch, every cell is the
  type their writer expects. See [docs/02 §The extract → normalize → load
  pipeline](./docs/02-architecture.md#the-extract--normalize--load-pipeline)
  for the per-FieldType coercion table.

## [0.1.4] — 2026-05-29

### Fixed
- **Secret-resolver errors now include the SDK's actual message.** Previously,
  catch-all branches in the GCP, AWS, and Vault resolvers surfaced only the
  exception class name (e.g. `RetryError`, `_FakeClientError`,
  `_FakeForbidden`) — defensive paranoia that the SDK message body might
  leak secret-adjacent metadata. In practice the SDK has not yet received
  the secret value at the point those exceptions raise, so the message
  bodies carry only operator-diagnostic text ("Reauthentication is needed",
  "permission denied", "Could not connect to the endpoint URL", etc.).
  Operators were left to manually debug 57-second hangs with no actionable
  output. The engine's per-run Redactor remains the safety net for any
  value that does slip into a log line.

## [0.1.3] — 2026-05-28

### Added
- **Multi-file project-local connectors.** A project-local connector folder
  may split helpers into sibling files (`client.py`, `helpers.py`, etc.) and
  use relative imports like `from .client import SigmaClient` in `source.py`
  / `destination.py`. Previously failed with `ImportError: attempted relative
  import with no known parent package` because the engine loaded each `.py`
  as a standalone module. The engine now loads a connector folder as a
  synthetic Python package; the existing baked connectors are unaffected.
- **Scaffolds emit `__init__.py`.** `dtex new source <name>` and
  `dtex new destination <name>` now write an empty `__init__.py` to make
  the package shape explicit. Folders without one still work via PEP 420
  namespace packages — fully backward-compatible.
- GitHub Actions CI: pytest matrix on Python 3.11/3.12/3.13, ruff + mypy lint.
- PyPI Trusted Publishing workflow triggered by `v*` tags.

## [0.1.2] — 2026-05-27

### Fixed
- `dtex --version` reported `0.1.0` regardless of the installed version
  because `dtex/__init__.py` hardcoded the version string and drifted from
  `pyproject.toml`'s `[project] version`. `__version__` is now read from
  the installed-package metadata via `importlib.metadata` — one source of
  truth, no drift possible.

The 0.1.1 release on PyPI is yanked alongside 0.1.0; install `dtex==0.1.2`
or later for an accurate `--version` output.

## [0.1.1] — 2026-05-27

### Fixed
- README links rendered as broken on the PyPI project page. PyPI does not
  resolve relative paths against a base URL the way GitHub does, so the
  Documentation / Security / Contributing / Code of Conduct / Changelog /
  LICENSE links pointed at nothing. All converted to absolute
  `https://github.com/vej-ai/dtex/...` URLs.

The 0.1.0 release on PyPI is yanked; install `dtex==0.1.1` or later.

## [0.1.0] — 2026-05-27

The first public release.

### Added

- **Engine.** Run lifecycle with discovery, config resolution, per-stream
  commit, transactional loads with rollback-on-failure, schema evolution
  (`evolve` default, `strict` opt-in), structured JSON-lines logs per run,
  a `_dtex_runs` audit table, and pipeline-level parallel execution with
  per-destination concurrency caps.
- **CLI** (`dtex`): `run`, `list`, `validate`, `init`, `new` (source /
  destination / config), `state`, `runs`, `secrets test`.
- **Library API.** `dtex.run(config=...)` as the engine entry point;
  `@stream`, `@resource`, `@destination`, `Connector`, `stream_method`,
  and the contract types (`Capability`, `Schema`, `Field`, `Config`,
  `State`, `Cursor`, `Batch`, `StateRecord`) for connector authors.
- **Project layout.** dbt-style `dtex_project.yml` + `profiles.yml`, with
  pipelines defined as configs under `configs/`. `dtex init` scaffolds
  the layout; `dtex new` scaffolds individual sources, destinations, and
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
  `_dtex_state` table alongside the data it tracks; per-run audit lives
  in `_dtex_runs`.
- **Secret-manager resolvers.** Pluggable `secret://<scheme>/<path>` URL
  form plus three production resolvers — GCP Secret Manager
  (`dtex[gcp-secrets]`), AWS Secrets Manager (`dtex[aws-secrets]`), and
  HashiCorp Vault (`dtex[vault]`) — each opt-in via extras. Custom
  resolvers register via entry-point or a project-local
  `dtex_plugins.py`.

### Security

- **Redaction filter.** Secrets declared with `secret: true` in
  `register.yaml`, and any value resolved via `${env.X}` or `secret://`,
  are redacted to `***` in stdout, `.dtex/logs/`, run records,
  `--dry-run` config dumps, and exception messages. Redaction is by
  value, not just by key.
- **Trust model.** The threat model for running third-party connector
  code in-process is documented in
  [`docs/08-security.md`](./docs/08-security.md). dtex does not sandbox
  connector code in v1; the provenance / least-privilege guidance is
  spelled out so operators can plan accordingly.
- **Fresh-every-run secret resolution.** No on-disk cache of resolved
  secret values. The per-process resolver client is reused across calls
  within a single run, but every value is re-fetched on every run.
- **Vulnerability reporting.** [`SECURITY.md`](./SECURITY.md) documents
  the private-disclosure channel and response timelines.

[Unreleased]: https://github.com/vej-ai/dtex/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/vej-ai/dtex/releases/tag/v0.1.5
[0.1.4]: https://github.com/vej-ai/dtex/releases/tag/v0.1.4
[0.1.3]: https://github.com/vej-ai/dtex/releases/tag/v0.1.3
[0.1.2]: https://github.com/vej-ai/dtex/releases/tag/v0.1.2
[0.1.1]: https://github.com/vej-ai/dtex/releases/tag/v0.1.1
[0.1.0]: https://github.com/vej-ai/dtex/releases/tag/v0.1.0
