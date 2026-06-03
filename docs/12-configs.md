# 12. Configs — the runtime unit

A **config** is the runtime unit of dtex. One config = one pipeline. It names:

- which **source** to run,
- which **destination** to write to,
- which **target** (i.e. which named connection row in `profiles.yml`) the destination uses,
- the **params** that customize both the source and the destination for this run,
- optionally, the **streams** to run (a subset of the source's declared streams),
- optionally, a **schedule** (advisory — surfaced to an external scheduler, the engine itself never acts on it).

A config separates *what a source can extract* (the source's `register.yaml`) and *what a destination can write to* (the destination's `register.yaml`) from *how this pipeline pairs them*. The same source can feed two pipelines (dev + prod, one DuckDB + one BigQuery, etc.) without forking the connector.

## 1. The file shape

A config lives under the project's `configs/` directory. Discovery scans every `*.yml` and `*.yaml` file there; the file name has no semantic role (a config's `name:` is the key the CLI's `-p/--conf` uses).

Two shapes are accepted interchangeably.

### 1.1. One config per file

```yaml
# configs/shiphero_prod.yml
name: shiphero_prod
source: shiphero
destination: bigquery
target: prod
schedule: "0 */6 * * *"        # advisory
params:
  page_size: 100
  start_date: "2025-01-01"
destination_params:
  # Per-pipeline destination overrides. Typically empty — the destination's
  # connection params live in profiles.yml's per-target row.
  dataset: shiphero_raw
streams:                       # mandatory — see §3 for the catch-all form
  shipments:
  orders:
```

### 1.2. Many configs per file

For a project with several related pipelines (e.g. one source feeding multiple environments) it's often cleaner to group them:

```yaml
# configs/shiphero.yml
configs:
  - name: shiphero_dev
    source: shiphero
    destination: duckdb
    target: dev
    streams: all
    params:
      page_size: 10
  - name: shiphero_prod
    source: shiphero
    destination: bigquery
    target: prod
    streams: all
    params:
      page_size: 100
```

The two shapes cannot mix in one file (a top-level `name:` plus a top-level `configs:` is rejected as ambiguous).

Duplicate config names — whether across files or within one multi-config file — are a hard error at discovery time.

## 2. Required keys

A config must declare:

| key | meaning |
|---|---|
| `name` | The config's identifier — what `dtex run -p <name>` matches. |
| `source` | The source connector's name (resolved project-local-first, then baked — docs/03 §5). |
| `destination` | The destination connector's name (same resolution rule). |
| `streams` | Which streams this pipeline runs. Either the catch-all sentinel `all` (or `"*"`) or a mapping of `{stream_name: per-stream-overrides}`. Never optional, never empty — see §3.1. |

Everything else is optional with documented defaults (see §3).

Unknown top-level keys are a hard error (catches typos like `destintion`). The legacy keys `select:` and `partition_overrides:` are **hard-removed** — both were subsumed by `streams:` (see §3.1).

## 3. Optional keys and defaults

| key | default | meaning |
|---|---|---|
| `target` | `profiles.yml[<destination>].default_target` (or the destination's only target if it has just one, else error) | Which row of `profiles.yml[<destination>].targets` supplies the destination's connection params. |
| `params` | `{}` | Per-pipeline source param overrides — precedence layer 3 (docs/03 §6). |
| `destination_params` | `{}` | Per-pipeline destination param overrides — layered on top of the destination's `profiles.yml` row. |
| `schedule` | `null` | Advisory cron expression. The engine itself never acts on it (docs/03 §2.6). |
| `tags` | `[]` | Bare list of strings used by `dtex run --tag <tag>` to select every matching config (and by `dtex list --tag` for catalog filtering). Lowercased + deduplicated at parse time (see §3.2). |

### 3.1 The `streams:` block

`streams:` is mandatory and accepts two shapes:

**Catch-all sentinel.** `streams: all` (or `streams: "*"`, case-insensitive, whitespace-tolerant) means "include every stream the source declares." Expansion happens at run time, so a new stream added to the source flows through automatically.

```yaml
streams: all
```

**Explicit mapping.** `streams: {stream_name: per-stream-overrides}` lists each stream the pipeline runs, with optional per-stream overrides. Each value is one of:

* `null` / empty mapping (`my_stream:`) — include with defaults.
* a bare string (`my_stream: full_refresh`) — shorthand for `{mode: <string>}`.
* a mapping with any subset of `mode`, `since`, `params`, `partition`.

The two shapes are mutually exclusive. `streams: all` plus per-stream entries is rejected; an empty mapping (`streams: {}`) is rejected with `'streams' must not be empty`.

Per-stream knobs:

| knob | type | meaning |
|---|---|---|
| `mode` | `incremental` \| `full_refresh` | This run's mode for this stream. Default = the stream's natural mode (incremental if the source declares an `incremental:` block, full_refresh otherwise). `mode: incremental` on a stream that has no cursor is a hard error. |
| `since` | timestamp / int / string matching `cursor_type` | One-shot cursor floor for this run. Ignored when the effective mode is full_refresh. **Replaces** the seed verbatim (not `max(since, prior_state)`). Does NOT mutate `_dtex_state`. |
| `params` | mapping | Per-stream source-param overlay — precedence layer 4 (between config-level `params:` and env vars). |
| `partition` | string \| mapping | Destination partition spec. Short string form defaults to `TIME+DAY` on the named column. Long form gives full control. Replaces the source's `register.yaml` `partition_by` for this stream. |

**The full-refresh state rule.** `mode: full_refresh` (and the run-wide `--full-refresh` flag) DOES NOT read, advance, or reset `_dtex_state`. The prior cursor row stays intact, so a sibling incremental config sharing this source keeps its cursor. Full-refresh is a per-run *behavior*, not a state mutation. To actually clear state, use `dtex state reset` — that's the explicit operation.

### 3.2 Worked example — per-stream partition + mode + params

A pipeline that lands Stripe into BigQuery wants `charges` partitioned by an integer-epoch range (Stripe's `created` is an INT cursor) and `invoices` partitioned by the date the engine writes. The dev variant runs `subscriptions` as full_refresh while prod keeps it incremental:

```yaml
# configs/stripe_bq.yml
name: stripe_bq
source: stripe
destination: bigquery
target: dev
destination_params:
  dataset: stripe_data
streams:
  charges:
    partition:
      field: created
      type: range
      range:
      start: 1577836800        # 2020-01-01T00:00:00Z
      end:   1893456000        # 2030-01-01T00:00:00Z
      interval: 86400          # one day per bucket
  invoices:
    partition: created         # short form — TIME+DAY on `created`
  subscriptions:
    mode: full_refresh         # dev re-pulls; prod's incremental cursor stays intact
    params:
      page_size: 100            # this stream only — smaller page size for testing
```

Per-stream `partition` accepts both the short string form (`partition: created`) and the long-form mapping. A stream not named here keeps whatever the source's `register.yaml` declared (or the cursor-based auto-default if nothing was declared — docs/05 §3.3). A stream listed without overrides (e.g. `customers:` with no value) is included with defaults.

### 3.3 Tag-based multi-run — `tags:` + `dtex run --tag`

A config carries an optional `tags:` field — a bare list of strings, shape-equivalent to dbt's model `tags:`. `dtex run --tag <tag>` then runs every config whose `tags:` list contains that tag.

```yaml
# configs/hourly.yml
configs:
  - name: shiphero_hourly
    source: shiphero
    destination: bigquery
    target: prod
    streams: all
    tags: [hourly, ops]
  - name: stripe_hourly
    source: stripe
    destination: bigquery
    target: prod
    streams: all
    tags: [hourly, finance]
  - name: zendesk_hourly
    source: zendesk
    destination: bigquery
    target: prod
    streams: all
    tags: [hourly, support]
```

```
$ dtex run --tag hourly
... (per-config output for each of the three) ...

TAG hourly: ran 3 config(s), 3 succeeded, 0 failed in 12.4s
CONFIG            STATUS     ROWS   DURATION  ERROR
shiphero_hourly   succeeded  1234   3.2s      -
stripe_hourly     succeeded  567    2.1s      -
zendesk_hourly    succeeded  890    7.1s      -
```

Semantics:

* **Order** — alphabetical by config name (predictable, stable across runs).
* **Continue-on-failure** — a per-config failure does NOT stop the rest. The CLI exits `1` if any run failed, `0` if all succeeded.
* **Zero matches** — exit `2` with a `no configs match tag '<tag>'` message (usage error).
* **Mutual exclusion** — `-p/--conf` and `--tag` cannot be combined; exactly one selector per invocation.
* **Uniform args** — `--target`, `--destination-param`, `--full-refresh`, `--select` all apply to every matched config. `--param` is NOT supported with `--tag` (a source param override would silently apply to every config whether or not its source declares it; use `dtex run -p <config> --param k=v` for per-config knobs). `--full-refresh` follows the §3.1 don't-touch-state rule — it ignores the cursor for this invocation without resetting `_dtex_state`.

Tags are normalized to lowercase at parse time and deduplicated, so `tags: [Hourly, hourly]` parses to `("hourly",)` and `--tag Hourly` matches `tags: [hourly]`. Selection is by exact match (no glob/regex).

Tags on a source's or destination's `register.yaml` are a separate namespace — they describe what the connector IS (catalog metadata for `dtex list --tag`); they never drive `dtex run --tag`, which is strictly about configs.

## 4. Target resolution

The engine picks the active target through this precedence chain (highest → lowest):

1. The CLI `--target` flag / library `target_override=` kwarg.
2. The config's `target:` field.
3. `profiles.yml[<destination>].default_target`.
4. If the destination block has exactly one entry under `targets:`, that target.
5. Otherwise — the run fails with a clear error listing the targets the destination *does* define.

Step 4 is a convenience: a destination with only a `dev` row needs no `default_target` for a config that omits `target:` to pick `dev`.

## 5. Override precedence (full picture)

For a **source param** (lowest → highest):

1. The source's `register.yaml` `params[].default`.
2. The project's `dtex_project.yml` `vars:` block.
3. The active config's `params:` block.
4. The active config's `streams[<stream_name>].params:` block — per-stream overlay (docs/12 §3.1).
5. The environment variable `SIMPLE_E_PARAM_<NAME>`.
6. The CLI `--param k=v` flag / library `params_override=` kwarg.

For a **destination param** (lowest → highest):

1. The destination's `register.yaml` `params[].default`.
2. The project's `dtex_project.yml` `vars:` block (a project-wide knob that happens to share the param name).
3. The destination's `profiles.yml[<destination>].targets[<target>]` row.
4. The active config's `destination_params:` block.
5. The environment variable `SIMPLE_E_PARAM_<NAME>`.
6. The CLI `--destination-param k=v` flag / library `destination_params_override=` kwarg.

The CLI `--select` flag **narrows** the config's `streams:` block — it is a per-invocation intersection, not an additive set. A `--select` name that isn't in `streams:` is a hard error listing the in-scope stream names.

## 6. State + configs

`_dtex_state` rows are keyed by the **source** name, not the config name. A different config that points at the same source will resume off the same cursor rows — state is a property of where the data was extracted from, not which pipeline ran the extract.

This is intentional: a `shiphero_dev` and a `shiphero_prod` config pointing at the same ShipHero account will both see the same incremental cursor advance. Two pipelines that need independent state should bind to different source folders (a project-local fork is the usual way).

**Full-refresh interaction.** Because state is shared across configs, the §3.1 `mode: full_refresh` rule is load-bearing: a full-refresh run does NOT read, advance, or reset the shared cursor row. A `shiphero_dev` config running a single stream as full_refresh leaves `shiphero_prod`'s cursor intact. To actually clear state, use `dtex state reset <stream>` — that's the explicit operation.

The same rule applies to the run-wide CLI `--full-refresh` flag: it ignores the cursor for this invocation without touching `_dtex_state`. Operators who actually want to start over should reach for `dtex state reset`, not `--full-refresh`.

## 7. Discovery and lookup

| function | purpose |
|---|---|
| `dtex.engine.configs.discover_configs(project_root, config_paths)` | Walks each `config_paths` dir, parses every `*.yml`/`*.yaml`, returns `{name: PipelineConfig}`. Hard-errors on duplicate names. |
| `dtex.engine.configs.load_config(name, project_root, config_paths)` | Returns the named `PipelineConfig` or raises a clear `ConfigError` listing the configs the project does define. |

The engine calls `load_config` in step 1 (DISCOVER) of the run lifecycle (docs/02).

## 8. The runtime type

The parsed config is exposed as `dtex.types.PipelineConfig` — a frozen dataclass. The engine builds one of these per run; connector authors never construct them. Re-exported as `dtex.PipelineConfig` for advanced library use.
