# 06 — Project Anatomy

A det **project** is the directory a user creates to run extract-load
pipelines. It is modeled directly on a dbt project: a small declarative root
file, a credentials file kept separate from code, folders of connectors, and a
disposable working directory. If you know `dbt_project.yml`, `profiles.yml`, and
`target/`, you already know the shape of this chapter.

## The dbt analogy at a glance

| dbt | det | Purpose |
|---|---|---|
| `dbt_project.yml` | `det_project.yml` | Project config: name, paths, defaults, vars. Committed. |
| `~/.dbt/profiles.yml` | `profiles.yml` | Credentials per environment/target. **Not committed.** |
| `models/` | `connectors/` + `destinations/` | The work the project does. Committed. |
| `target/` | `.det/` | Disposable build/cache/log output. Git-ignored. |
| `dbt run --target prod` | `det run --target prod` | Select an environment. |

## A complete project tree

```
acme_el/
├── det_project.yml        # project config (the dbt_project.yml analog)
├── profiles.yml                # credentials per target (NOT committed)
├── .gitignore
│
├── connectors/                 # custom SOURCE connectors (kind: source)
│   ├── shiphero/
│   │   ├── register.yaml
│   │   ├── source.py
│   │   ├── client.py
│   │   └── schema.py
│   └── internal_api/
│       ├── register.yaml
│       └── source.py
│
├── destinations/               # custom DESTINATION connectors (kind: destination)
│   └── snowflake_eu/
│       ├── register.yaml
│       └── destination.py
│
└── .det/                  # working dir (git-ignored — the target/ analog)
    ├── manifest.json           # cached, validated catalog of all connectors
    ├── logs/
    │   └── run-2026-05-21T09-30-00.log
    └── cache/                  # per-run scratch (schema introspection, etc.)
```

`connectors/` and `destinations/` are **conventional folders, not a typed
distinction** — both hold connector folders, and what makes a connector a source
or a destination is the `kind:` key in its own `register.yaml` (chapter 03). The
two folders exist purely so a human can find things; the engine would be just as
happy with everything under one `connectors/`. Splitting them is the recommended
default for readability, and `connector_paths` (below) lists both.

## `det_project.yml` — the project config

The root file. Committed to version control. Pure declaration — no logic, no
credentials.

```yaml
# det_project.yml
name: acme_el
version: "1.0.0"

# Where the engine looks for custom connectors. Project-local folders.
# Order matters only for human grouping; resolution is by connector name.
connector_paths:
  - connectors
  - destinations

# The destination a source binds to when its register.yaml omits a
# `destination:` block. Names a connector discoverable on connector_paths
# or a pre-baked one.
default_destination: bigquery

# Default target if --target is not passed on the CLI.
default_target: dev

# Project-wide variables. Override register.yaml param defaults for every
# connector in the project. Lowest precedence after the connector's own
# defaults; see chapter 03 §6.
vars:
  start_date: "2025-01-01"
  page_size: 100
```

### Full schema

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `name` | string | **Yes** | — | Project identifier. `snake_case`. |
| `version` | string (semver) | No | `"0.1.0"` | Project version. Surfaced in logs and the manifest. |
| `connector_paths` | list[string] | No | `["connectors"]` | Directories scanned for custom connector folders, relative to the project root. |
| `default_destination` | string | No | `null` | Connector `name` used when a source's `register.yaml` has no `destination:` block. |
| `default_target` | string | No | first target in `profiles.yml` | Which `profiles.yml` target to use when `--target` is omitted. |
| `vars` | map[string → scalar] | No | `{}` | Project-wide param overrides applied to every connector. |
| `working_dir` | string | No | `.det` | Where the engine writes the manifest cache, logs, and scratch. |

Seven keys. Like `register.yaml`, the project file is kept deliberately small —
anything per-environment belongs in `profiles.yml`, anything per-connector
belongs in that connector's `register.yaml`.

## `profiles.yml` — credentials per target

`profiles.yml` holds everything that *changes between environments* and
everything *secret*. It is the `~/.dbt/profiles.yml` analog. **It is never
committed** — it goes in `.gitignore`, and CI/production supply it out of band
(mounted file, env-templated, or a secret manager).

A **target** is a named environment: `dev`, `staging`, `prod`. Each target
defines the credentials for the destinations and sources used in that
environment.

```yaml
# profiles.yml  --  NOT committed to version control
targets:

  dev:
    # Destination credentials, keyed by destination connector name.
    destinations:
      bigquery:
        project_id: acme-data-dev
        location: US
        credentials_file: ~/.config/gcloud/acme-dev-sa.json

    # Per-connector secret/profile blocks. A register.yaml secret ref of
    # ${profile.shiphero.refresh_token} resolves to the value below.
    profiles:
      shiphero:
        refresh_token: "dev-refresh-token-xxxx"

  prod:
    destinations:
      bigquery:
        project_id: acme-data-prod
        location: US
        # Resolve from the environment in prod rather than inlining a path.
        credentials_file: ${env.GOOGLE_APPLICATION_CREDENTIALS}
    profiles:
      shiphero:
        refresh_token: ${env.SHIPHERO_REFRESH_TOKEN}
```

### Schema

| Key | Type | Required | Purpose |
|---|---|---|---|
| `targets` | map[string → Target] | **Yes** | Named environments. The key is the target name passed to `--target`. |
| `targets.<t>.destinations` | map[string → map] | No | Destination credentials, keyed by destination connector `name`. The inner map is passed to that destination's `config` (its `params`/secrets). |
| `targets.<t>.profiles` | map[string → map] | No | Named credential blocks resolved by `${profile.<block>.<key>}` refs in any `register.yaml`. |

Values may embed `${env.VAR}` so the file itself stays free of literal secrets —
the recommended pattern for `prod`. This is the crucial separation the dbt model
buys: a source's `register.yaml` says *"I need a `refresh_token`"* and the
destination binding says *"write to `bigquery` dataset `shiphero`"* — neither
ever knows which GCP project or which token a given environment uses. That
knowledge lives only here, and only here changes between `dev` and `prod`.

> **Why credentials are not in `register.yaml`.** The ShipHero proof
> `config.json` mixed `project_id` / `dataset_id` (environment) with
> `cursor_field` / `schema` (contract). det splits them: contract →
> `register.yaml` (committed, portable), environment → `profiles.yml`
> (uncommitted, per-target). A connector folder is then identical across every
> environment it ever runs in.

## Baked and custom connectors coexisting

A project draws connectors from two places, resolved by name (chapter 03 §5):

1. **Custom** — folders under the project's `connector_paths` (`connectors/`,
   `destinations/`). Authored and version-controlled by the user.
2. **Pre-baked** — folders shipped inside the installed `det` package
   (`det/connectors/…`). Maintained by the det project.

They are invoked identically — the caller names a connector and does not care
where it came from:

```bash
det run meta_ads  --target prod      # pre-baked: ships in the package
det run shiphero  --target prod      # custom:   ./connectors/shiphero/
```

A run's `default_destination: bigquery` typically resolves to the **pre-baked**
BigQuery destination — most projects never write a destination connector at all
and only add custom *sources*. The `destinations/` folder in the tree above
exists only because that project happens to need a custom `snowflake_eu`
destination.

**Project-local wins on a name collision.** To customize a baked connector — fix
a bug, add a stream, change pagination — copy it into `connectors/<same_name>/`
and edit. The engine finds the project-local copy first and the baked one is
shadowed. No fork of the `det` package, no patching: overriding is just a
folder.

## Target selection

The active environment is chosen per invocation:

```bash
det run shiphero --target prod        # explicit
det run shiphero                      # uses det_project.yml default_target
```

```python
import det
det.run(connector="shiphero", target="prod")
```

The resolved target drives the full config layering from chapter 03 §6 —
`register.yaml` defaults, then `det_project.yml` `vars`, then the target's
`profiles.yml` blocks, then CLI/`run()` overrides — producing the immutable
`Config` the connector body receives. Switching `--target` changes credentials
and environment values without touching a single connector folder.

## The `.det/` working directory

The disposable build directory — the `target/` analog. Created and owned by the
engine, **git-ignored**, safe to delete at any time (the next run rebuilds it).

| Path | Purpose |
|---|---|
| `.det/manifest.json` | Cached, validated catalog of every discovered connector (its `register.yaml` parsed, streams, decorator bindings checked). Rebuilt when a `register.yaml` changes; speeds up repeat runs. |
| `.det/logs/` | Per-run structured log files, timestamped. |
| `.det/cache/` | Per-run scratch — destination schema introspection, temp artifacts. |

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

Scaffolds the tree above: `det_project.yml` with sensible defaults, a
`profiles.yml` template with a single `dev` target, empty `connectors/` and
`destinations/` folders, and the `.gitignore`. From there:

```bash
det new connector shiphero      # scaffold a custom source folder
det validate                    # discovery-time validation (chapter 03 §7)
det run shiphero --target dev   # run it
```

> [Open question: should `profiles.yml` live in the project root (visible,
> easy to template per-repo) or default to `~/.det/profiles.yml` like dbt's
> home-directory default (one credentials file shared across projects)? Current
> lean: project-root by default — explicit and CI-friendly — with a
> `--profiles-dir` flag for teams who prefer the dbt-style shared home file.]
