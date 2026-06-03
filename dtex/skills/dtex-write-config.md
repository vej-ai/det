---
name: dtex-write-config
description: Author or edit a dtex pipeline config (configs/*.yml). Use whenever the user is creating, editing, debugging, or asking about a dtex config — including questions about streams, modes, since, partition, params, or destination_params. Covers the post-redesign schema where `streams:` is mandatory.
---

# Authoring a dtex pipeline config

A dtex **config** is the runtime unit: one config = one pipeline. A config
file lives under `configs/*.yml` (or `*.yaml`) and binds:

- a **source** (from `sources/`)
- a **destination** (baked or from `destinations/`)
- a **target** (which `profiles.yml[<destination>].targets[<target>]` row
  supplies connection params)
- the **streams** to run, with optional per-stream overrides
- the **params** that customize the source and destination for this pipeline

The CLI's `-p/--conf` resolves a config by its `name:` field, not its
filename.

## The mandatory shape

Every config MUST declare four top-level keys:

```yaml
name: revenuecat_dev_bq
source: revenuecat
destination: bigquery
streams: all       # or a mapping — see below
```

Optional top-level keys: `target`, `params`, `destination_params`,
`schedule`, `tags`.

Missing `streams:` is a hard error. Unknown top-level keys (typos like
`destintion`) are a hard error. The legacy keys `select:` and
`partition_overrides:` are **removed** — both were subsumed by `streams:`.

## The `streams:` block — two shapes

### Catch-all opt-in: `streams: all`

Runs every stream the source declares. Equivalent forms accepted:
`streams: all`, `streams: "*"`. Case-insensitive and whitespace-tolerant.

Use this when (a) you want everything and (b) you want a new stream
added to the source to flow through to this pipeline automatically.

```yaml
name: full_export
source: revenuecat
destination: bigquery
streams: all
```

### Explicit mapping: per-stream overrides

A mapping `{stream_name: per-stream-overrides}`. Each value can be:

- **null / empty mapping** — include the stream with defaults
- **a bare string** (`my_stream: full_refresh`) — shorthand for `{mode: <string>}`
- **a mapping** with any subset of `mode`, `since`, `params`, `partition`

```yaml
name: revenuecat_dev_bq
source: revenuecat
destination: bigquery
target: dev
streams:
  customers:                          # include with defaults
  subscriptions:
    mode: full_refresh                # this run treats it as full refresh
    since: "2026-05-01T00:00:00Z"     # one-shot cursor floor (incremental only)
    params:
      page_size: 100                  # per-stream source-param overlay
  transactions:
    partition:
      field: created_at
      type: time
      time: {granularity: day}
```

`streams: all` and per-stream entries are **mutually exclusive** — use
one shape or the other.

## Per-stream knobs

### `mode: incremental | full_refresh`

This run's mode for this stream. Default is the stream's natural mode
(incremental if the source declares an `incremental:` block in
`register.yaml`, full_refresh otherwise).

**`mode: full_refresh` does NOT touch `_dtex_state`.** It does not read,
advance, or reset the cursor row. A sibling incremental config sharing
this source keeps its cursor intact. To actually clear state, use
`dtex state reset` — that's a separate operation.

`mode: incremental` on a stream that has no `incremental:` block in the
source's `register.yaml` is a hard error.

### `since: <value>`

A one-shot cursor floor for this run only. Ignored when the effective
mode is full_refresh. Does NOT mutate `_dtex_state`. The engine uses
this VERBATIM as the seed for this run (not `max(since, prior_state)`)
— explicit "re-pull from here just this once" semantics. Type must
match the stream's `cursor_type` (timestamp → ISO-8601 string,
integer → int literal).

### `params: {key: value, ...}`

Per-stream source-param overrides. Sits at precedence layer 4 (between
config-level `params:` and `DTEX_PARAM_<NAME>` env vars). Use this when
one stream needs a different value — e.g. `transactions` is a heavy
stream that needs a smaller `page_size` than the rest.

### `partition: <string or mapping>`

Destination partition spec for this stream. Replaces what
`partition_overrides[<stream>]` did before the redesign. Short string
form (`partition: created`) defaults to TIME+DAY. Long-form mapping
gives full control over `type` / `range` / `granularity`.

## Param precedence (full picture)

For a **source param**, lowest → highest:

1. `register.yaml` `params[].default`
2. `dtex_project.yml` `vars:`
3. config `params:`
4. **config `streams[<name>].params:`** ← per-stream layer
5. `DTEX_PARAM_<NAME>` env var
6. CLI `--param k=v` / `params_override=`

For a **destination param**, lowest → highest:

1. destination's `register.yaml` `params[].default`
2. `dtex_project.yml` `vars:`
3. `profiles.yml[<destination>].targets[<target>]`
4. config `destination_params:`
5. env var
6. CLI `--destination-param k=v`

## Anti-patterns — what NOT to do

**DON'T pass params as CLI flags when authoring a config.** A common
LLM mistake is reaching for `dtex run -p X --param.page_size=100` to
test a configuration. That's ad-hoc — it doesn't persist, doesn't
document intent, and bypasses the config-as-blueprint contract.

**DO write a config file for each connection test.** "One config per
connection test (with possibly multiple streams)" is the idiomatic
pattern. If you want to try `page_size=100`, write a config that
declares it:

```yaml
name: revenuecat_smoke_bq
source: revenuecat
destination: bigquery
target: dev
streams:
  customers:
    params:
      page_size: 100
```

Then `dtex run -p revenuecat_smoke_bq`. Now the test is reproducible,
diffable, and reviewable.

**DON'T create one config per stream.** A config is a *pipeline*, not
a stream. One pipeline can run multiple streams. Per-stream overrides
live inside the config under `streams:`.

**DON'T use `select:` or `partition_overrides:`.** Both are
hard-removed. The parser errors with a message pointing at the new
location.

## Common scenarios

**"Run everything against the dev BQ project":**

```yaml
name: revenuecat_dev_bq
source: revenuecat
destination: bigquery
target: dev
streams: all
```

**"Run only customers, with a smaller page size":**

```yaml
name: revenuecat_customers_only
source: revenuecat
destination: bigquery
target: dev
streams:
  customers:
    params:
      page_size: 100
```

**"Re-pull the subscriptions stream from a specific date, just once":**

```yaml
name: revenuecat_subs_backfill
source: revenuecat
destination: bigquery
target: dev
streams:
  subscriptions:
    since: "2026-01-01T00:00:00Z"
```

**"Run a stream as full-refresh in dev without disturbing prod's cursor":**

```yaml
name: revenuecat_dev_smoke
source: revenuecat
destination: bigquery
target: dev
streams:
  customers:
    mode: full_refresh         # prod's _dtex_state stays intact
```

## When the user asks "which destination_params go in profiles vs config?"

- **`profiles.yml`** — auth, project ID, region, staging bucket. Per-machine,
  not committed to git, varies between dev/prod environments.
- **config `destination_params:`** — landing dataset/schema name. Per-pipeline
  routing. Committed to git.

If the user is putting BigQuery `project:` in a config, redirect them to
`profiles.yml`. If they're putting `dataset:` in `profiles.yml`, redirect
them to the config.
