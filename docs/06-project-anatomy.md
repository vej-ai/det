# 06 — Project Anatomy

A det **project** is the directory a user creates to run extract-load
pipelines. It is modelled on a dbt project: a small declarative root file, a
credentials file kept separate from code, folders of components, and a
disposable working directory. Sources, destinations, and pipeline *configs*
live in their own folders, and a config (not a connector) is the runtime
unit (chapter 12).

## The dbt analogy at a glance

| dbt | det | Purpose |
|---|---|---|
| `dbt_project.yml` | `det_project.yml` | Project config: name, paths, project-wide vars. Committed. |
| `~/.dbt/profiles.yml` | `profiles.yml` | Per-destination connection params, per target. **Not committed.** |
| `models/` | `sources/` + `destinations/` + `configs/` | The work the project does. Committed. |
| `target/` | `.det/` | Disposable build/cache/log output. Git-ignored. |
| `dbt run --select my_model` | `det run -p my_pipeline` | Run one pipeline. |
| `dbt outputs` in `profiles.yml` | top-level destination blocks in `profiles.yml` | Per-environment connection rows. |

## A complete project tree

```
acme_el/
├── det_project.yml         # project config (the dbt_project.yml analog)
├── profiles.yml            # per-destination connection params (NOT committed)
├── det_plugins.py          # OPTIONAL — project-local secret-resolver plugins
├── .gitignore
│
├── sources/                # custom SOURCE connectors (kind: source)
│   ├── shiphero/
│   │   ├── register.yaml
│   │   ├── source.py
│   │   ├── client.py
│   │   └── schema.py
│   └── internal_api/
│       ├── register.yaml
│       └── source.py
│
├── destinations/           # custom DESTINATION connectors (kind: destination)
│   └── snowflake_eu/
│       ├── register.yaml
│       └── destination.py
│
├── configs/                # pipeline configs (one config = one pipeline)
│   ├── shiphero_dev.yml    # one config per file
│   ├── shiphero_prod.yml
│   └── internal.yml        # ...OR many under a `configs:` list per file
│
└── .det/                   # working dir (git-ignored — the target/ analog)
    ├── manifest.json
    ├── logs/
    │   └── run-2026-05-21T09-30-00.log
    └── cache/
```

The optional **`det_plugins.py`** file sits next to `det_project.yml`. If present, det imports it once at engine startup so the file's `det.register_secret_resolver(...)` calls register custom `secret://` schemes for the project. The file is arbitrary Python — same trust model as the connector folders. See [08 — Security §3](./08-security.md) for the resolver protocol and the registration pattern.

Sources and destinations live in their own top-level folders (`sources/` for
`kind: source`, `destinations/` for `kind: destination`); `configs/` holds the
pipeline configs that bind a source to a destination. Configs are the
runtime unit: `det run -p <name>` names a config, not a connector.

## `det_project.yml` — the project config

The root file. Committed to version control. Pure declaration — no logic, no
credentials.

```yaml
# det_project.yml
name: acme_el
version: "1.0.0"

# Where the engine looks for project-local components, relative to this file.
# Project-local folders shadow same-named baked components (chapter 03 §5).
source_paths:
  - sources
destination_paths:
  - destinations
config_paths:
  - configs

# Project-wide variables. Override register.yaml param defaults for every
# connector in the project. Lower precedence than the active config's
# `params:` block; higher than register.yaml param defaults. See chapter 03 §6.
vars:
  start_date: "2025-01-01"
  page_size: 100
```

### Full schema

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `name` | string | **Yes** | — | Project identifier. `snake_case`. |
| `version` | string (semver) | No | `"0.1.0"` | Project version. Surfaced in logs and the manifest. |
| `source_paths` | list[string] | No | `["sources"]` | Directories scanned for project-local sources. |
| `destination_paths` | list[string] | No | `["destinations"]` | Directories scanned for project-local destinations. |
| `config_paths` | list[string] | No | `["configs"]` | Directories scanned for pipeline configs (chapter 12). |
| `vars` | map[string → scalar] | No | `{}` | Project-wide param overrides applied to every connector. |
| `working_dir` | string | No | `.det` | Where the engine writes the manifest cache, logs, and scratch. |

> There is no `default_destination` key (a source's `register.yaml` does not
> declare a destination; a config does) and no top-level `default_target`
> (each destination block in `profiles.yml` carries its own `default_target`).
> A legacy `connector_paths` key is still parsed as a fallback for
> `source_paths` and `destination_paths` so older project files don't break,
> but the canonical form is the split.

## `profiles.yml` — per-destination connection params

`profiles.yml` holds everything that *changes between environments* and
everything *secret*. **Never committed** — it goes in `.gitignore`, and
CI/production supply it out of band (mounted file, env-templated, or a secret
manager).

The file is **destination-keyed** (dbt-outputs style). Each top-level key is
the name of a destination connector; under it sits the destination's
`default_target` and a `targets:` map of named-environment connection
params. A parallel top-level `profiles:` block, keyed by target name, carries
source-secret rows so `${profile.<block>.<key>}` refs resolve (chapter 03 §2.5).

```yaml
# profiles.yml  --  NOT committed to version control

# Project-wide pipeline-level concurrency budget. Default 1 (sequential —
# opt in to parallelism). dbt's `threads:` knob, same semantics. Honoured
# by `det run --tag <T>`; each destination's @destination.max_concurrent_writes
# hook caps further. See chapter 02 §Concurrency model + chapter 07 §`--threads`.
threads: 4

# The pre-baked DuckDB destination. `path` is the .duckdb file location.
duckdb:
  default_target: dev
  targets:
    dev:
      path: ".det/warehouse.duckdb"
    prod:
      path: "/var/data/det/warehouse.duckdb"

# A second destination — illustrative.
bigquery:
  default_target: dev
  targets:
    dev:
      project: acme-data-dev
      location: US
      credentials: ${env.GOOGLE_APPLICATION_CREDENTIALS}
    prod:
      project: acme-data-prod
      location: US
      credentials: ${env.GAC_PROD}

# Per-target source-secret blocks. Resolved by ${profile.<block>.<key>}
# refs in any source's register.yaml.
profiles:
  dev:
    shiphero:
      refresh_token: "dev-refresh-token-xxxx"
    rest:
      api_token: ${env.REST_API_TOKEN_DEV}
  prod:
    shiphero:
      refresh_token: ${env.SHIPHERO_REFRESH_TOKEN}
    rest:
      api_token: ${env.REST_API_TOKEN_PROD}
```

### Schema

| Top-level key | Type | Purpose |
|---|---|---|
| `threads` | positive integer | Pipeline-level concurrency budget for `det run --tag`. Default 1. Each destination's `@destination.max_concurrent_writes` caps further. dbt-style. |
| `<destination name>` | mapping with `targets:` (+ optional `default_target:`) | One block per destination connector. The block's `targets.<name>` rows supply the destination's connection params for each named environment. |
| `profiles` | map[string → map[string → map]] | Per-target source-secret blocks. `profiles.<target>.<block>.<key>` is what `${profile.<block>.<key>}` resolves to (after the engine picks the active target from the config). |

Values may embed `${env.VAR}` so the file itself stays free of literal secrets —
the recommended pattern for `prod`.

### Why DuckDB clamps to 1 thread

A `.duckdb` database file is protected by a single OS-level file lock —
two writer connections on the same file at the same time would corrupt
it. The DuckDB destination therefore declares
`@destination.max_concurrent_writes() -> 1`, and the engine honors that
cap unconditionally: a user with `threads: 8` running `det run --tag X`
against an all-DuckDB project gets serial execution against the DuckDB
file, even while other destinations in the same sweep run in parallel.
This is the destination's honesty about its own model, not a soft hint —
the cap is enforced by a `threading.Semaphore(1)` keyed by destination
name. Use BigQuery (or any future Tier-A warehouse with networked
multi-writer storage) when you want real per-destination parallelism.

## Configs — `configs/<name>.yml`

A config is a **pipeline**: source + destination + target + params. Chapter 12
covers configs in depth. The short form:

```yaml
# configs/shiphero_prod.yml
name: shiphero_prod
source: shiphero
destination: bigquery
target: prod
params:
  page_size: 100
  start_date: "2025-01-01"
select:
  - shipments
  - orders
```

Or, many configs grouped in one file under a `configs:` list:

```yaml
# configs/shiphero.yml
configs:
  - name: shiphero_dev
    source: shiphero
    destination: duckdb
    target: dev
  - name: shiphero_prod
    source: shiphero
    destination: bigquery
    target: prod
```

The CLI's primary selector is the config name:

```bash
det run -p shiphero_prod
det run --conf shiphero_prod          # --conf is the long-form alias
det run -p shiphero_dev --target prod # override the config's target
```

## Baked and custom components coexisting

A project draws each kind of component from two places, resolved by name
(chapter 03 §5):

1. **Custom** — folders under `source_paths` / `destination_paths`
   (`sources/`, `destinations/`). Authored and version-controlled by the
   user.
2. **Pre-baked** — folders shipped inside the installed `det` package
   (`det/sources/…`, `det/destinations/…`). Maintained by the det project.

**Project-local wins on a name collision.** To customize a baked component —
fix a bug, add a stream, change pagination — copy it into
`sources/<same_name>/` (or `destinations/<same_name>/`) and edit. The engine
finds the project-local copy first and the baked one is shadowed. No fork of
the `det` package, no patching: overriding is just a folder.

## Target selection

The active environment is chosen per invocation, with this precedence
(highest → lowest):

1. `--target` flag on the CLI / `target_override=` kwarg in the library.
2. The config's own `target:` field.
3. `profiles.yml[<destination>].default_target` (the destination's default).
4. The destination's only target, if it has exactly one.
5. Otherwise — the run fails with a clear error listing the targets the
   destination *does* define.

```bash
det run -p shiphero_prod                    # uses config's target:
det run -p shiphero_prod --target staging   # overrides
```

## The `.det/` working directory

The disposable build directory — the `target/` analog. Created and owned by
the engine, **git-ignored**, safe to delete at any time (the next run rebuilds
it).

| Path | Purpose |
|---|---|
| `.det/manifest.json` | Cached, validated catalog of every discovered connector. |
| `.det/logs/` | Per-run structured log files, timestamped. |
| `.det/cache/` | Per-run scratch. |

Nothing in `.det/` is a source of truth. Incremental **state** is *not*
here — it lives in the destination's `_det_state` table (chapter 03 §3.5),
so a fresh checkout on a new machine resumes correctly with an empty
`.det/`.

## Recommended `.gitignore`

```gitignore
# credentials — never commit
profiles.yml

# disposable working directory
.det/
```

## Creating a project

```bash
det init acme_el
```

Scaffolds the tree above: `det_project.yml`, a destination-keyed
`profiles.yml` template with a single `duckdb`/`dev` target, empty
`sources/` and `destinations/` folders, a `configs/` folder seeded with one
`example.yml` stub, and the `.gitignore`. From there:

```bash
det new source shiphero        # scaffold a custom source folder
det new config shiphero_dev    # scaffold a configs/shiphero_dev.yml stub
det validate                   # discovery-time validation
det run -p shiphero_dev        # run it
```
