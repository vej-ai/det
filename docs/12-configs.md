# 12. Configs — the runtime unit

A **config** is the runtime unit of detx. One config = one pipeline. It names:

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
select:
  - shipments
  - orders
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
    params:
      page_size: 10
  - name: shiphero_prod
    source: shiphero
    destination: bigquery
    target: prod
    params:
      page_size: 100
```

The two shapes cannot mix in one file (a top-level `name:` plus a top-level `configs:` is rejected as ambiguous).

Duplicate config names — whether across files or within one multi-config file — are a hard error at discovery time.

## 2. Required keys

A config must declare:

| key | meaning |
|---|---|
| `name` | The config's identifier — what `detx run -p <name>` matches. |
| `source` | The source connector's name (resolved project-local-first, then baked — docs/03 §5). |
| `destination` | The destination connector's name (same resolution rule). |

Everything else is optional with documented defaults (see §3).

Unknown top-level keys are a hard error (catches typos like `destintion`).

## 3. Optional keys and defaults

| key | default | meaning |
|---|---|---|
| `target` | `profiles.yml[<destination>].default_target` (or the destination's only target if it has just one, else error) | Which row of `profiles.yml[<destination>].targets` supplies the destination's connection params. |
| `params` | `{}` | Per-pipeline source param overrides — precedence layer 3 (docs/03 §6). |
| `destination_params` | `{}` | Per-pipeline destination param overrides — layered on top of the destination's `profiles.yml` row. |
| `partition_overrides` | `{}` | Per-stream physical-partition overrides — wins over the source's `register.yaml` `partition_by` (docs/05 §3.3). |
| `select` | `[]` (= all streams) | Streams to run. Empty means every stream. |
| `schedule` | `null` | Advisory cron expression. The engine itself never acts on it (docs/03 §2.6). |
| `tags` | `[]` | Bare list of strings used by `detx run --tag <tag>` to select every matching config (and by `detx list --tag` for catalog filtering). Lowercased + deduplicated at parse time (see §3.2). |

### 3.1 Worked example — `partition_overrides:`

A pipeline that lands Stripe into BigQuery wants `charges` partitioned by an integer-epoch range (Stripe's `created` is an INT cursor) and `invoices` partitioned by the date the engine writes. Neither needs forking the Stripe connector — the override block lives in the config:

```yaml
# configs/stripe_bq.yml
name: stripe_bq
source: stripe
destination: bigquery
target: dev
destination_params:
  dataset: stripe_data
partition_overrides:
  charges:
    field: created
    type: range
    range:
      start: 1577836800        # 2020-01-01T00:00:00Z
      end:   1893456000        # 2030-01-01T00:00:00Z
      interval: 86400          # one day per bucket
  invoices: created            # short form — TIME+DAY on `created`
```

`partition_overrides:` is a mapping `{stream_name: partition_spec}`. Each entry accepts both the short string form and the long-form mapping. A stream not named here keeps whatever the source's `register.yaml` declared (or the cursor-based auto-default if nothing was declared — docs/05 §3.3).

### 3.2 Tag-based multi-run — `tags:` + `detx run --tag`

A config carries an optional `tags:` field — a bare list of strings, shape-equivalent to dbt's model `tags:`. `detx run --tag <tag>` then runs every config whose `tags:` list contains that tag.

```yaml
# configs/hourly.yml
configs:
  - name: shiphero_hourly
    source: shiphero
    destination: bigquery
    target: prod
    tags: [hourly, ops]
  - name: stripe_hourly
    source: stripe
    destination: bigquery
    target: prod
    tags: [hourly, finance]
  - name: zendesk_hourly
    source: zendesk
    destination: bigquery
    target: prod
    tags: [hourly, support]
```

```
$ detx run --tag hourly
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
* **Uniform args** — `--target`, `--destination-param`, `--full-refresh`, `--select` all apply to every matched config. `--param` is NOT supported with `--tag` (a source param override would silently apply to every config whether or not its source declares it; use `detx run -p <config> --param k=v` for per-config knobs).

Tags are normalized to lowercase at parse time and deduplicated, so `tags: [Hourly, hourly]` parses to `("hourly",)` and `--tag Hourly` matches `tags: [hourly]`. Selection is by exact match (no glob/regex).

Tags on a source's or destination's `register.yaml` are a separate namespace — they describe what the connector IS (catalog metadata for `detx list --tag`); they never drive `detx run --tag`, which is strictly about configs.

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
2. The project's `detx_project.yml` `vars:` block.
3. The active config's `params:` block.
4. The environment variable `SIMPLE_E_PARAM_<NAME>`.
5. The CLI `--param k=v` flag / library `params_override=` kwarg.

For a **destination param** (lowest → highest):

1. The destination's `register.yaml` `params[].default`.
2. The project's `detx_project.yml` `vars:` block (a project-wide knob that happens to share the param name).
3. The destination's `profiles.yml[<destination>].targets[<target>]` row.
4. The active config's `destination_params:` block.
5. The environment variable `SIMPLE_E_PARAM_<NAME>`.
6. The CLI `--destination-param k=v` flag / library `destination_params_override=` kwarg.

The CLI `--select` flag **replaces** (not unions) the config's `select:` — it is a per-invocation narrowing, not an additive set.

## 6. State + configs

`_detx_state` rows are keyed by the **source** name, not the config name. A different config that points at the same source will resume off the same cursor rows — state is a property of where the data was extracted from, not which pipeline ran the extract.

This is intentional: a `shiphero_dev` and a `shiphero_prod` config pointing at the same ShipHero account will both see the same incremental cursor advance. Two pipelines that need independent state should bind to different source folders (a project-local fork is the usual way).

## 7. Discovery and lookup

| function | purpose |
|---|---|
| `detx.engine.configs.discover_configs(project_root, config_paths)` | Walks each `config_paths` dir, parses every `*.yml`/`*.yaml`, returns `{name: PipelineConfig}`. Hard-errors on duplicate names. |
| `detx.engine.configs.load_config(name, project_root, config_paths)` | Returns the named `PipelineConfig` or raises a clear `ConfigError` listing the configs the project does define. |

The engine calls `load_config` in step 1 (DISCOVER) of the run lifecycle (docs/02).

## 8. The runtime type

The parsed config is exposed as `detx.types.PipelineConfig` — a frozen dataclass. The engine builds one of these per run; connector authors never construct them. Re-exported as `detx.PipelineConfig` for advanced library use.
