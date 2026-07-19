# Changelog

All notable changes to **dtex** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For what is *planned* — versus what has shipped — see
[`docs/10-roadmap-and-scope.md`](./docs/10-roadmap-and-scope.md).

## [Unreleased]

### Added

- **`partition_by: none` — explicit partitioning opt-out.** A timestamp- or
  date-cursor stream is auto-promoted to TIME/DAY partitioning on the cursor
  column; there was no way to refuse. That default is hazardous for
  backfill-heavy streams: a bootstrap sweep writes the whole table history
  into every batch, each load touches hundreds of day-partitions, and
  BigQuery's partition-modification quota (5,000/day/table) fails the run.
  `partition_by: none` (register.yaml) and `partition: none` (per-stream
  config override) now resolve to "explicitly unpartitioned" and suppress
  the auto-default. The cockroachdb connector README documents when to
  choose `none` vs `{type: ingestion}` for large first syncs.

### Fixed

- **BigQuery GCS staging uploads use chunked resumable transfer.** The
  staging Parquet blob was uploaded single-shot with the SDK's default
  60-second timeout; a wide-row batch serializing to hundreds of MB timed
  out on every attempt on slower uplinks. Uploads now set an 8 MiB
  `chunk_size` (resumable protocol, per-chunk timeout) and inherit the
  destination's `job_timeout_seconds`.

## [0.4.0] — 2026-07-17

Adds `cockroachdb` — a baked CockroachDB source connector built for the
realities of Cockroach Cloud: primary-key-keyset bootstrap that stays inside
fixed SQL memory budgets at any table size, resumable page-capped first
syncs, `AS OF SYSTEM TIME` follower reads, and hidden-column (`crdb_region`)
handling for REGIONAL BY ROW tables. Verified end-to-end against a live
Cockroach Cloud Standard cluster, including tables in the hundred-million-row
range. Also fixes two BigQuery load-path defects the live verification
surfaced.

### Fixed

- **BigQuery loads work for streams with `JSON` fields.** The LOAD job
  carried an inline schema, and BigQuery rejects `JSON` fields there
  (`400 Unsupported field type: JSON`); schema-less Parquet loads then
  type-mismatch against native JSON columns too. Stream tables now declare
  `JSON` fields as `STRING` columns carrying JSON text (the same shape
  Airbyte/Fivetran land `jsonb` as; the internal `_dtex_state` /
  `_dtex_runs` tables keep native JSON — they are written via
  parameterized DML, where it works). The load path was reworked
  alongside: the destination creates/patches the load target (including
  per-batch merge staging tables) through the tables API first and runs
  the LOAD without an inline schema, mapping Parquet columns to the table
  by name; the Parquet file carries per-field nullability so `REQUIRED`
  columns survive the round-trip; `WRITE_TRUNCATE` dispositions became an
  explicit `TRUNCATE TABLE` + append-load (a schema-less truncate-load
  would have replaced the table schema with the Parquet-inferred one);
  and merge staging loads became plain appends into their freshly-created
  per-batch tables.
- **`FLOAT` normalization accepts `decimal.Decimal`.** Postgres-protocol
  drivers (psycopg — the `postgres` and `cockroachdb` sources) yield
  `Decimal` for `NUMERIC`/`DECIMAL` columns; normalize previously raised
  `CoercionError` on the first such value. A `Decimal` now coerces through
  `float()` like `int` does.

### Added

- **`cockroachdb` baked source connector.** CockroachDB over the Postgres
  wire protocol (`psycopg`), shaped like the `postgres` source but built for
  CockroachDB's extraction realities. The first sync of an incremental
  stream sweeps the table by **primary-key keyset** (`WHERE (pk...) > (...)
  ORDER BY pk LIMIT n`) instead of cursor order — every page a constrained
  index scan, safe on Cockroach Cloud's fixed per-tenant SQL memory budget
  where an unbounded `ORDER BY cursor_field` dies with "memory budget
  exceeded". Bootstrap progress (last PK, running cursor max) persists in
  stream state, so an interrupted or page-capped sweep (`bootstrap_max_pages`)
  resumes instead of restarting, and the cursor handed to the engine on
  completion is the true global max. Steady-state incremental runs use the
  same cursor keyset as `postgres`. Reads can be pinned with
  `AS OF SYSTEM TIME` (`as_of_system_time` param; e.g.
  `follower_read_timestamp()`) for contention-free follower reads. Cockroach
  Cloud plumbing: `sslmode` defaults to `verify-full` with
  `sslrootcert=system` (public-CA certs), `options` passes `--cluster=`
  routing for non-SNI clients. Type mapping extends the Postgres set with
  the shapes CockroachDB's `information_schema` emits: `ARRAY` → `JSON`,
  `USER-DEFINED` (enums, incl. `crdb_region`) → `STRING`, `inet` /
  `interval` / the `time` family → `STRING`, `oid` → `INTEGER`.

## [0.3.1] — 2026-06-16

Hygiene patch — removes example text that named a specific account from
the packaged docs and tests. No behavior, API, or connector change.

### Changed

- **Generic example wording in `revenuecat`'s README and a config test.**
  Replaced a customer-specific example reference with neutral phrasing.
  Packaging-only; nothing about the connectors or the engine changes.

## [0.3.0] — 2026-06-15

Adds `gads` — a baked Google Ads source connector built on GAQL (the
Google Ads Query Language). Each stream is a GAQL query submitted to the
`searchStream` endpoint, the same query-as-stream shape the `stripe`
connector uses for Sigma SQL. Ships five streams (campaigns + per-campaign,
per-ad-group, per-ad, and per-keyword daily stats), runtime OAuth2 token
minting with a one-time refresh-token helper, and MCC account
auto-discovery. Verified end-to-end against the live Google Ads API.

### Added

- **`gads` baked source connector.** Google Ads API v24 over REST, GAQL
  as the single extraction surface. Streams: `campaigns` (entity list,
  full replace) and `campaign_daily_stats` / `ad_group_daily_stats` /
  `ad_daily_stats` / `keyword_daily_stats` (incremental on `segments.date`
  with a lookback window that re-pulls recent days to absorb late
  conversions and attribution restatements). Nested `GoogleAdsRow`
  responses are flattened to snake_case columns; money stays in micros.
  See the [connector README](https://github.com/vej-ai/dtex/blob/main/dtex/sources/gads/README.md).

- **Google Ads OAuth refresh-token helper.** `python -m
  dtex.sources.gads.scripts.get_refresh_token` runs the one-time loopback
  OAuth consent flow and writes the refresh token to a git-ignored file
  (mode 0600), so the secret never lands in terminal scrollback. The
  connector mints short-lived access tokens from it at run time.

- **MCC account auto-discovery.** Leave `customer_ids` empty and set
  `auto_discover_from_manager` to a manager (MCC) id; the connector
  expands the manager's tree via the `customer_client` resource and pulls
  every ENABLED, non-manager (leaf) account under it, up to
  `max_discovery_depth` (default 1). An explicit `customer_ids` always
  wins; the manager id doubles as `login_customer_id` automatically.

- **`GaqlConfig` SDK type.** A new public contract type
  (`dtex.GaqlConfig`) and a `StreamDef.gaql` field, mirroring the existing
  `SigmaConfig`/`StreamDef.sigma`. Marks a stream as GAQL-driven and names
  its `.gaql` query file. Purely additive — existing connectors and
  manifests are unaffected.

## [0.2.4] — 2026-06-10

Documentation-only patch release. The "Pre-baked connectors" section
in the main README was getting unwieldy as more connectors shipped
(6 sources, 2 destinations) — restructured to a compact table of
per-connector READMEs so adding a new connector is one row, not a
prose update. Also adds the missing DuckDB destination README.

### Changed

- **Main README "Pre-baked connectors" section restructured.** Replaced
  the inline-prose list with two compact tables (sources + destinations)
  linking to each connector's own README. Operator-friendly format for
  "show me what's available + tell me more." Same authoring story for
  every future connector — add one row, no prose churn.

### Added

- **DuckDB destination README.** Every other baked connector ships
  with its own README; DuckDB was the only gap. Covers install +
  config + the five capabilities + state/run-records semantics + the
  concurrent-write caveat. No behavior change — DuckDB just gets the
  same doc surface every other connector has.

## [0.2.3] — 2026-06-10

A patch release adding `revenuecat` as a baked source connector — the v2
API across customers, subscriptions, and daily chart metrics. The
metrics_daily stream uses RC's own `incomplete=true` flag to handle the
partial-today caveat without bespoke lookback heuristics.

### Added

- **Baked `revenuecat` source connector for the v2 API.** Three streams:
  - **`customers`** — every customer in the project. NON-incremental:
    RC v2 has no server-side date filter on /customers (verified against
    the docs + airbyte issue 70315 + RC community forum). Every run
    paginates the full customer list. `write_disposition: merge` on `id`
    makes re-pulls upsert idempotently. For a ~200k-customer account
    expect ~70 minutes wall at the default page_size against RC's
    480-req/min Customer Information rate limit.
  - **`subscriptions`** — per-customer fan-out: RC has no project-level
    /subscriptions endpoint, only /customers/{id}/subscriptions. O(N+1)
    HTTP calls per run for N customers.
  - **`metrics_daily`** — RC v2's Charts API takes server-side
    `start_date`/`end_date` filters and tags per-day values with
    `incomplete=true` when finalizing. Long-format output
    (`cohort_date × chart_name × measure_name`) — adding a new chart
    needs zero schema migration. The cursor advances only past days
    RC marked complete, so partial-today re-pulls cleanly on the
    next run with the corrected (final) value.

  Default charts are `revenue,mrr,actives,trials` — the four that
  return the standard `{cohort, incomplete, measure, value}` shape.
  `cohort_explorer` and `prediction_explorer` return a different shape
  and would break the flattener; they're excluded.

  Auth via the `REVENUECAT_API_KEY` env var by default (any
  resolver-backed `secret://` ref works in a profile). The client uses
  a bounded `(10s connect, 60s read)` timeout, retries on
  `requests.exceptions.RequestException` family + 5xx + 429 (all
  capped by `max_retries` — the 429 path explicitly bounded to avoid
  the infinite-loop hang the prior project-local version had).

  Verified live: 195,200-customer backfill, ~71 min wall, clean run.

## [0.2.2] — 2026-06-09

A patch release. BigQuery destination is now robust against the
stale-TCP-socket failures that long-running runs (hundreds of
sequential LOAD jobs over an hour-plus) routinely hit. Plus the
README finally mentions the bundled Claude skills feature that has
shipped since 0.2.0.

### Fixed

- **BigQuery destination: retry on statusless network failures.**
  `run_with_retries` only retried when the exception carried an
  HTTP status code in `_RETRYABLE_STATUS` (429/5xx). A connection-
  level failure — `requests.exceptions.ConnectionError` wrapping
  `http.client.RemoteDisconnected` from a stale keep-alive socket —
  has no `.code` attribute, so the retry path re-raised on the
  first attempt and the whole stream aborted. This was latent in
  every prior release; it surfaced reliably during a 73-minute
  RevenueCat customers backfill (300+ sequential MERGE-via-staging
  LOAD jobs against a single connector instance). The fix adds
  `_is_retryable_network_error` recognising concrete network-class
  exception types (`ConnectionError`, `Timeout`,
  `ChunkedEncodingError`, `google.api_core.exceptions.RetryError`);
  programming errors without a status code (`KeyError`, `TypeError`)
  still surface on the first attempt — the broadened catch is
  precisely typed, not "any statusless exception."

### Changed

- **README mentions the bundled Claude skills feature.** Skills
  shipped in 0.2.0 but were undiscoverable from the PyPI project
  page or the GitHub repo README. Adds a "Bundled Claude skills"
  section between the connector inventory and the docs link, plus
  updates the stripe one-liner to mention the Sigma surface added
  in 0.2.1. Documentation-only — no behavior change. Skills users
  who installed 0.2.0/0.2.1 already have the feature working.

## [0.2.1] — 2026-06-04

A patch release that lands Stripe Sigma SQL-as-stream extraction inside
the existing `stripe` connector, with a new `stream_def` injectable on
`@stream` functions that makes the dual-surface dispatch clean.

### Added

- **Stripe Sigma SQL-as-stream folded into the `stripe` connector.** One
  baked connector, two extraction surfaces: REST (default) and Sigma SQL
  (opt-in per stream via a new `sigma: {query: <path>}` block in
  `register.yaml`). The shipped Sigma streams (`charges_daily`,
  `subscriptions_active`, `invoices_paid`) each reference a `.sql` file
  under `dtex/sources/stripe/queries/`; the connector submits the SQL via
  Stripe's async Query Run API (`POST /v2/data/reporting/query_runs`),
  polls, downloads the CSV result, and yields batches of dicts.

  The CSV is downloaded to a temp file in one continuous pass rather than
  parsed straight off the socket — the engine pulls batches lazily and
  loads each into the destination before requesting the next, so
  socket-side parsing would leave the download connection idle for the
  duration of every load job and Stripe's CDN would close it mid-body on
  large results (`ChunkedEncodingError: IncompleteRead`). Draining the
  socket up front decouples download speed from load pace; a dropped
  download retries from scratch up to `max_retries` with the same
  exponential backoff as the submit/poll calls, and is duplicate-free for
  `replace`/`merge` streams since no rows are yielded until the full CSV
  is in hand.

  A single Stripe restricted key with both REST + Sigma+Reporting scopes
  drives both surfaces; the merged connector reads it from `STRIPE_API_KEY`
  by default (any resolver-backed `secret://` ref also works). REST streams
  are unchanged from the prior `stripe` connector — same names, same schema,
  same `cursor_type: int`. Existing configs continue to work; configs that
  want Sigma data declare the corresponding stream in their `streams:` block.

- **`stream_def` is now an engine-injected parameter on `@stream` functions.**
  A connector can introspect its own `StreamDef` declaration — e.g. the
  merged `stripe` connector dispatches to Sigma extraction when
  `stream_def.sigma is not None`, reading the SQL filename from the
  `sigma:` block in `register.yaml`. New injectable, alongside the existing
  `config` / `state` / `cursor` / `log`. No existing function signatures
  change; declare `stream_def` only when you need it.

### Changed

- **`stripe` connector version bumped to 2.0.0.** The REST half is unchanged
  but the connector now also serves Sigma streams. The standalone
  `stripe_sigma` source connector (briefly developed during 0.2.0 dev but
  never released as a separate package) is removed in favor of the merged
  shape — anyone iterating on that standalone version should switch their
  config from `source: stripe_sigma` to `source: stripe`.

## [0.2.0] — 2026-06-03

A breaking release. The config schema gained a mandatory `streams:` block
that subsumes the prior `select:` and `partition_overrides:` keys (both
hard-removed). `--full-refresh` no longer resets state. Every existing
config needs a `streams:` line — `streams: all` matches the prior
default behavior.

### Added

- **`dtex init --with <destination>`** scaffolds a starter `profiles.yml`
  block for a baked destination alongside the always-scaffolded `duckdb`
  block. Repeatable: `dtex init --with bigquery --with duckdb`. Unknown
  names fail with a clean error listing the valid options. New users no
  longer have to copy a BigQuery block out of the docs to get started.

- **BigQuery destination: new `auth_type` param.** Two modes —
  `oauth` (default; uses Application Default Credentials, no path
  needed) and `service_account` (requires a non-empty `credentials_path`
  pointing at the JSON key). The scaffolded BigQuery block no longer
  includes an empty `credentials_path:` field, which was confusing — the
  oauth path has no path to set.

- **Mandatory per-pipeline `streams:` block, with per-stream overrides.**
  Every config now declares which streams the pipeline runs and how.
  Two shapes: `streams: all` (the explicit catch-all opt-in — runs every
  stream the source declares) and `streams: {<stream_name>: {...}}` (an
  explicit mapping with optional per-stream `mode`, `since`, `params`,
  and `partition` overrides). A typo in a stream name is a hard error
  listing the valid streams; setting `mode: incremental` on a stream
  whose source declares no cursor is a hard error. The block subsumes
  the prior `select:` and `partition_overrides:` keys — both of which
  are now removed (see *Removed* below).

  The new design recognizes that a config is the pipeline blueprint, not
  the schema. Stream identity (cursor, primary key, schema, extraction
  logic) stays in `register.yaml`; the per-pipeline shape (which streams,
  what mode, what params, what partition) lives in the config. This lets
  a `dev` config run one stream as `full_refresh` while a sibling `prod`
  config keeps it incremental, without forking the source. See
  [docs/12 §3.1](https://github.com/vej-ai/dtex/blob/main/docs/12-configs.md)
  for the full surface and the per-stream knob reference.

- **Per-stream source-param overlay (precedence layer 4).** A config's
  `streams[<name>].params:` block sits between the config's top-level
  `params:` (layer 3) and the env-var layer (layer 5). Lets one stream
  use a different `page_size` than the others without bumping the
  default for the whole pipeline.

- **Bundled Claude skills + `dtex skills install` + first-run hint.**
  Three skill files ship inside the wheel at `dtex/skills/*.md` —
  `dtex-write-config.md`, `dtex-write-connector.md`, and
  `dtex-debug.md`. `dtex skills install [DIRECTORY]` copies them into
  `<DIRECTORY>/.claude/skills/dtex/` (default DIRECTORY is `.`), with
  `--force` for re-installs. Any `dtex` command run inside a project
  that lacks installed skills AND hasn't yet been prompted emits a
  single-line stderr hint pointing at the install command — fires once,
  suppressed by a `.dtex/skills-prompted` marker, and silenced
  entirely for `dtex skills *` invocations. Skills are discovered at
  runtime via `importlib.resources`, so the mechanism works for
  wheels, sdists, editable installs, and zipped envs.

### Changed

- **BigQuery destination: `auth_type` is authoritative.** With
  `auth_type=oauth` any value in `credentials_path` is ignored (ADC
  always wins). With `auth_type=service_account` a missing
  `credentials_path` fails with a clear message at `open()` time. An
  unknown `auth_type` fails listing the valid options.

- **`--full-refresh` no longer resets `_dtex_state`.** The new rule
  (docs/12 §3.1): a full-refresh run does NOT read, advance, or
  reset the cursor row. The prior cursor stays intact, so a sibling
  incremental config sharing this source keeps its cursor. To actually
  clear state, use `dtex state reset <stream>` — that operation is
  unchanged. This is technically a behavior change to an existing
  flag; it matches the new per-stream `mode: full_refresh` semantics
  and was the only safe default given that state is source-keyed
  (multiple configs share the same `_dtex_state` row).

- **`--select` narrows the config's `streams:` block.** Previously it
  *replaced* the config's `select:` list. Now `--select` is an
  intersection: a `--select` name that isn't in the config's
  `streams:` block is a hard error listing the in-scope names. You
  can't materialize a stream the pipeline blueprint doesn't list.

- **`dtex list` shows a `STREAMS` column** (was `SELECT`).
  `(all)` for `streams: all`, comma-separated names for an explicit
  mapping.

- **Scaffolded config templates teach the new schema.** Both
  `_CONFIG_YML` (`dtex new config`) and `_EXAMPLE_CONFIG_YML` (the
  example seeded by `dtex init`) now declare `streams:` explicitly,
  with commented examples of the per-stream override forms.
  `dtex new source` adds a comment to the source template noting
  that the example config references streams via `streams: all`,
  so renaming streams is safe.

- **New acceptance test `test_scaffold_chain_validates_clean`** runs
  `init` → `new source` → `new config` → `validate` end-to-end and
  asserts exit-zero. Catches the class of bugs where a template
  drifts from the parser's schema.

### Removed

- **`select:` config key.** Subsumed by `streams:` (a listed stream
  is automatically selected). Parsing a config with `select:` fails
  with a friendly error pointing at the new location.

- **`partition_overrides:` config key.** Subsumed by
  `streams[<name>].partition`. Same friendly redirect on parse.

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

[Unreleased]: https://github.com/vej-ai/dtex/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/vej-ai/dtex/releases/tag/v0.4.0
[0.3.1]: https://github.com/vej-ai/dtex/releases/tag/v0.3.1
[0.3.0]: https://github.com/vej-ai/dtex/releases/tag/v0.3.0
[0.2.4]: https://github.com/vej-ai/dtex/releases/tag/v0.2.4
[0.2.3]: https://github.com/vej-ai/dtex/releases/tag/v0.2.3
[0.2.2]: https://github.com/vej-ai/dtex/releases/tag/v0.2.2
[0.2.1]: https://github.com/vej-ai/dtex/releases/tag/v0.2.1
[0.2.0]: https://github.com/vej-ai/dtex/releases/tag/v0.2.0
[0.1.5]: https://github.com/vej-ai/dtex/releases/tag/v0.1.5
[0.1.4]: https://github.com/vej-ai/dtex/releases/tag/v0.1.4
[0.1.3]: https://github.com/vej-ai/dtex/releases/tag/v0.1.3
[0.1.2]: https://github.com/vej-ai/dtex/releases/tag/v0.1.2
[0.1.1]: https://github.com/vej-ai/dtex/releases/tag/v0.1.1
[0.1.0]: https://github.com/vej-ai/dtex/releases/tag/v0.1.0
