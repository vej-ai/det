# 07 — CLI and Library API

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.

det is invoked the way dbt is: `pip install`, then a CLI. But the CLI is a **thin shell over a real Python library** — every command is one function call away. This is deliberate. Orchestrators (Dagster, Airflow) and notebooks should never shell out; they import det. The CLI and the library are the *same engine*, exposed twice.

Stage 8.B made **pipeline configs** (chapter 12) the runtime unit. The CLI's
primary selector is now `-p / --conf <config_name>`; the connector-alone
selector (`-c / --connector`) is gone, because a source without a destination
binding cannot run.

---

## 1. Installation

```bash
pip install det
```

This installs the engine, the CLI entry point `det`, and the pre-baked
sources/destinations. Source connectors with heavy dependencies are installed
per-connector via each connector's `requirements.txt`.

---

## 2. CLI surface

All commands share a project root: the directory containing
`det_project.yml` (see [06 — Project Anatomy](./06-project-anatomy.md)). det
walks up from the CWD to find it, like dbt and git.

### `det init [<dir>]` — scaffold a project

```bash
det init my_pipelines
```

Creates the stage-8.B project tree:

```
my_pipelines/
  det_project.yml      # project name, version, *_paths, vars
  profiles.yml         # per-destination connection params (gitignored)
  sources/             # custom SOURCE connectors (kind: source)
  destinations/        # custom DESTINATION connectors (kind: destination)
  configs/             # pipeline configs (one source + one destination each)
    example.yml        # a starter config stub
  .det/                # run state, logs, cache (gitignored)
  .gitignore           # pre-populated
```

### `det new {source|destination|config} <name>` — scaffold a component

```bash
det new source stripe              # → sources/stripe/
det new destination my_warehouse   # → destinations/my_warehouse/
det new config stripe_dev          # → configs/stripe_dev.yml
```

Each subcommand writes a stub the user edits. A scaffolded source's
`register.yaml` carries an example stream; a scaffolded destination carries
the full `@destination` hook stub set; a scaffolded config binds a
placeholder source to the baked `duckdb` destination.

### `det list [--kind {source|destination|config}] [--tag <tag>]` — discover what exists

```bash
det list                       # sources, destinations, configs (all three)
det list --kind config         # just configs
det list --tag hourly          # filter every section by tag
det list --kind config --tag hourly
```

`--tag` filters each section (sources, destinations, configs) by its
own `tags:` field. One tag namespace per project, naturally partitioned:
running `det list --tag warehouse` typically shows destinations only
(those are warehouses); running `det list --tag hourly` typically shows
configs only (no source/destination would carry an operational schedule
tag). A section with no matches still shows its header with a `(no <kind>
match tag '<tag>')` placeholder so the user sees what was searched.

The CONFIGS table additionally carries a `TAGS` column (the config's own
`tags:` list — the field `det run --tag` selects on).

Output is grouped under three section headers:

```
$ det list
SOURCES
NAME      ORIGIN   #STREAMS  STREAMS                       TAGS
stripe    baked    4         charges, customers, ...       saas, payments
shiphero  project  3         shipments, orders, products   ecommerce

DESTINATIONS
NAME    ORIGIN  TAGS
duckdb  baked   warehouse, duckdb, local, tier-a

CONFIGS
NAME           SOURCE    DESTINATION  TARGET  SELECT
stripe_prod    stripe    bigquery     prod    (all)
shiphero_dev   shiphero  duckdb       dev     shipments, orders
```

`ORIGIN` is `baked` for a component shipped with det and `project` for one
under the project's `source_paths` / `destination_paths`.

### `det run -p <config> [...]` — extract and load

The core command. Runs **synchronously**: it blocks until the run succeeds or
fails, streaming logs to stdout. "Wait until it succeeds" is the contract —
no background jobs, no polling, no daemon.

```bash
det run -p stripe_prod                            # run the pipeline by config name
det run --conf stripe_prod                        # long alias
det run -p stripe_prod --select charges           # narrow streams (repeatable)
det run -p stripe_prod --target staging           # override the config's target
det run -p stripe_prod --full-refresh             # ignore state, reload
det run -p stripe_prod --param page_size=500      # override a source param
det run -p stripe_prod --destination-param dataset=raw   # override a dest param
det run --tag hourly                              # run every config tagged 'hourly'
```

Exactly one of `-p/--conf` or `--tag` must be supplied — they are mutually
exclusive. The `--tag` form runs every pipeline config whose `tags:` list
contains the tag, sequentially in alphabetical name order, **continuing
past per-config failures**. See [§ `det run --tag`](#det-run--tag-tag-multi-config-by-tag) below.

| Flag | Purpose |
|---|---|
| `-p, --conf <name>` | The pipeline config to run (under `configs/`). Mutually exclusive with `--tag`. |
| `--tag <tag>` | Run every config whose `tags:` list contains this tag. Mutually exclusive with `-p`. |
| `--target <name>` | Override the config's `target:`. Falls back to `profiles.yml[<destination>].default_target`. |
| `--select <stream>` | **Replace** (not union) the config's `select:`. Repeatable / comma-separated. |
| `--full-refresh` | Discard state for the selected streams; reload from the beginning. |
| `--param k=v` | Override a source param. Repeatable. Top precedence (chapter 03 §6). Not supported with `--tag`. |
| `--destination-param k=v` | Override a destination param. Repeatable. Top precedence (chapter 12 §5). |
| `--project-dir <dir>` | Project root (or any dir under it). Defaults to CWD. |

#### `det run --tag <tag>` — multi-config by tag

```bash
det run --tag hourly                                       # all configs tagged hourly
det run --tag hourly --target staging                      # uniform target
det run --tag hourly --destination-param path=/tmp/x.duckdb
```

Behavior:

* **Selection** — every config whose `tags:` list contains the tag.
  Case-insensitive (tags are lowercased at parse time and at match time).
  Selection is exact-match — no glob, no regex.
* **Order** — alphabetical by config name; reuses `det list --kind
  config` ordering for predictability.
* **Continue-on-failure** — a per-config failure does NOT stop the rest.
  Each config goes through the same `run()` path the `-p` form uses, which
  folds exceptions into a `FAILED` `RunResult`.
* **Uniform args** — `--target`, `--destination-param`, `--full-refresh`,
  `--select` apply to **every** matched config. `--param` is rejected with
  `--tag` because a source param override on a multi-source sweep would
  silently apply to configs whose source doesn't declare it (use `det run
  -p <config> --param k=v` per config when you need that).

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Every matched run succeeded. |
| `1` | At least one matched run failed. |
| `2` | No config matched the tag (usage error). Also: `-p` + `--tag` together, or `--param` + `--tag`. |

Summary output (printed after the per-config tables):

```
TAG hourly: ran 3 config(s), 2 succeeded, 1 failed in 12.4s
CONFIG              STATUS     ROWS   DURATION  ERROR
shiphero_hourly     succeeded  1234   3.2s      -
stripe_hourly       succeeded  567    2.1s      -
zendesk_hourly      failed     0      7.1s      ConnectionError: ...
```

Example run output:

```
$ det run -p stripe_prod
[info] running stream 'charges'
[info] stream 'charges' loaded 7310 row(s)
[info] running stream 'customers'
[info] stream 'customers' loaded 1840 row(s)
config stripe_prod: source stripe -> destination bigquery  (target: prod)
    STREAM     EXTRACTED  LOADED  CURSOR
ok  charges    7310       7310    2026-05-20T00:00:00Z -> 2026-05-21T00:00:00Z
ok  customers  1840       1840
run run-a1b9f3eb1234: succeeded - 9150 row(s), 41.20s
```

### `det validate` — validate every component

```bash
det validate
```

Walks every source, destination, and config the project can discover, runs
discovery-time validation (chapter 03 §7) on the connectors, and checks each
config's `source` and `destination` exist + its `target` is defined in
`profiles.yml`. Reports each problem found; exits non-zero if any component
fails — a useful CI / pre-commit gate.

### `det state {list|reset}` — inspect and reset state

State operations take a **config name**; the config resolves to a (source,
destination, target) triple. State rows in `_det_state` are keyed by source
name (chapter 12 §6), so two configs naming the same source share the same
state rows.

```bash
det state list -p stripe_prod                       # cursors for this config's source
det state reset -p stripe_prod                      # clear all cursors
det state reset -p stripe_prod --stream charges     # clear just one
```

`state reset` is the safe, surgical alternative to `--full-refresh`: it
clears the cursor without touching loaded data, so the next run re-extracts
the window. Both read/write the `_det_state` table described in
[05 — Destinations and State](./05-destinations-and-state.md).

### `det runs {list|show}` — inspect run history

Every run lands two artifacts: an audit row in `_det_runs` and a per-run
JSONL log at `.det/logs/<run_id>/run.jsonl` (see
[09 — Logging and Observability](./09-logging-and-observability.md)).
These commands read them back.

```bash
det runs list -p stripe_prod                  # recent runs (default --limit 20)
det runs list -p stripe_prod --limit 5        # the most recent five
det runs show <run_id> -p stripe_prod         # full record + JSONL events
det runs show abc123def -p stripe_prod        # short id (12-hex tail) also works
```

`-p <config>` is **required** — run records are stored per destination, and
the config disambiguates which destination's `_det_runs` to query (a
project with multiple destinations would otherwise need a multi-store
union v1 does not honour; a future `--destination <name>` is the natural
relaxation). `show` colors events by type on a TTY; piped output is plain
JSON-lines.

### `det --version`

Print the installed package version and exit 0.

---

## 3. Exit codes and synchronous semantics

`det run` blocks for the entire run. There is no async mode in the CLI — an
orchestrator that wants concurrency runs multiple `det` invocations, or uses
the library.

| Code | Meaning |
|---|---|
| `0` | Run succeeded. |
| `1` | Run failed — extract / load error, or config / discovery / validation problem stopped the run. |
| `2` | CLI usage error (missing flag, bad value). |
| `130` | Interrupted (Ctrl-C / SIGTERM). |

> # NOTE: docs/07 §3 originally specified a finer 0/1/2/3/130 table that split
> config errors from load errors. The engine's `det.run()` returns a uniform
> FAILED `RunResult` for every failure class and never raises (runner.py), so
> the CLI collapses to 0/1 — the code is source of truth (CONTRIBUTING.md
> precedence rule).

---

## 4. The Python library API

Everything the CLI does, the library does — because the CLI calls the
library. The importable surface is small and stable.

```python
import det

# Run a config. Blocks until done — same synchronous contract as the CLI.
result = det.run(
    config="stripe_prod",                        # the config NAME under configs/
    project_dir="./my_pipelines",                # walks up if omitted
    target_override="staging",                   # overrides the config's target
    params_override={"page_size": 500},          # source param overrides
    destination_params_override={"dataset": "raw"},  # destination param overrides
    full_refresh=False,
    select=("charges", "invoices"),              # replaces config's `select:`
)

print(result.status)        # RunStatus.SUCCEEDED | RunStatus.FAILED
print(result.run_id)        # "run-a1b9f3eb1234"
print(result.config)        # "stripe_prod"  — the pipeline that ran
print(result.connector)     # "stripe"        — the source name
print(result.destination)   # "bigquery"
print(result.target)        # "staging"
print(result.rows_loaded)   # 9150
for s in result.streams:
    print(s.name, s.rows_extracted, s.rows_loaded, s.cursor_after)

if result.status.value == "failed":
    raise result.error      # or call result.raise_for_status()
```

For a tag-based multi-run, use `det.run_tag(...)`:

```python
import det

# Returns a list[RunResult] — one per matched config, in alphabetical name order.
# Continue-on-failure: a per-config failure does NOT stop the rest.
results = det.run_tag(
    "hourly",
    project_dir="./my_pipelines",
    target_override="prod",                        # uniform across every matched config
    destination_params_override={"path": "/tmp/x.duckdb"},  # uniform
    full_refresh=False,
    select=("charges",),                           # uniform — replaces config.select
)

# Caller decides overall outcome — det.run_tag returns the list, never raises.
if not results:
    raise SystemExit(f"no configs matched the tag")
if any(r.status.value == "failed" for r in results):
    for r in results:
        if r.error is not None:
            print(f"{r.config}: {type(r.error).__name__}: {r.error}")
```

`det.run_tag` does NOT accept `params_override` — a source param override
on a multi-source sweep would silently apply to configs whose source
doesn't declare it. For per-config knobs, call `det.run(...)` per name.

### 4.1 The `RunResult` object

`det.run(...)` returns a `RunResult`:

```python
@dataclass
class StreamResult:
    name: str
    rows_extracted: int
    rows_loaded: int
    cursor_before: Any | None
    cursor_after: Any | None
    status: StreamStatus            # SUCCEEDED | FAILED | SKIPPED

@dataclass
class RunResult:
    run_id: str
    config: str                     # the pipeline config name (e.g. "stripe_prod")
    connector: str                  # the SOURCE name (e.g. "stripe")
    target: str
    destination: str
    status: RunStatus               # SUCCEEDED | FAILED
    started_at: datetime
    ended_at: datetime
    streams: list[StreamResult]
    rows_loaded: int
    full_refresh: bool
    error: BaseException | None
    log_path: str
```

`det.run()` does **not** raise on a failed run — it returns a `RunResult`
with `status=FAILED` and a populated `error`. A caller wanting exceptions
calls `result.raise_for_status()`.

### 4.2 Calling det from an orchestrator (Dagster)

```python
# dagster_pipeline.py
from dagster import op, job, Failure
import det

@op
def load_stripe():
    result = det.run(config="stripe_prod", project_dir="/opt/pipelines")
    if result.status.value == "failed":
        raise Failure(
            description=f"det run {result.run_id} failed",
            metadata={"run_id": result.run_id, "log": result.log_path},
        )
    return {s.name: s.rows_loaded for s in result.streams}

@job
def daily_ingest():
    load_stripe()
```

The same pattern works for Airflow (`PythonOperator`), Prefect, or a bare
cron + script.

---

## 5. Configuration precedence

det resolves every setting through a fixed precedence chain. Higher wins.

For a **source param** (lowest → highest):

1. The source's `register.yaml` `params[].default`.
2. The project's `det_project.yml` `vars:` block.
3. The active config's `params:` block.
4. `SIMPLE_E_PARAM_<NAME>` environment variable.
5. `det run --param k=v` flag / `det.run(params_override=)` kwarg.

For a **destination param** (lowest → highest):

1. The destination's `register.yaml` `params[].default`.
2. The project's `det_project.yml` `vars:` block.
3. The destination's `profiles.yml[<destination>].targets[<target>]` row.
4. The active config's `destination_params:` block.
5. `SIMPLE_E_PARAM_<NAME>` environment variable.
6. `det run --destination-param k=v` flag / `det.run(destination_params_override=)` kwarg.

### Reference

- Project layout, `det_project.yml`, `profiles.yml` → [06 — Project Anatomy](./06-project-anatomy.md)
- Configs in depth → [12 — Configs](./12-configs.md)
- `profiles.yml` format, secrets, `${ENV_VAR}` → [08 — Security](./08-security.md)
- Run logs and the run record → [09 — Logging and Observability](./09-logging-and-observability.md)
- Destinations, targets, state → [05 — Destinations and State](./05-destinations-and-state.md)
