---
name: dtex-debug
description: Diagnose a failing dtex run. Use when the user reports an error from `dtex run`, `dtex validate`, `dtex secrets test`, or a stuck/silently-misbehaving pipeline. Covers common failure modes and the right tools to reach for.
---

# Debugging a dtex run

The order matters. Run the cheap fast checks first; reach for live runs
only after the cheap checks pass.

## 1. Did the run produce a run record?

```bash
dtex runs                              # last N runs across the project
dtex runs show <run_id>                # full record including JSONL excerpt
```

`_dtex_runs` is the audit table that every run writes to. If the failing
run has a record, you have an error_type / error_message / traceback. If
it has NO record, the run failed before discovery completed (typo in
config name, can't find `dtex_project.yml`, etc.) — check stderr.

The full JSONL log is at `.dtex/logs/<run_id>/run.jsonl`. That's the
source of truth for what happened — every event, every batch, every
state commit, full tracebacks.

## 2. Did `dtex validate` pass?

```bash
dtex validate
```

This runs every check that doesn't need a network: schema shape,
decorator signatures, register.yaml ↔ source.py agreement, config
parsing, manifest typing. It's faster than a real run and catches the
same authoring errors. Run it BEFORE every `dtex run` when iterating
on a config or connector.

If validate fails, fix that first — the error message points at the
exact file and key.

## 3. Did `dtex secrets test` pass?

```bash
dtex secrets test                      # resolve every secret without running
dtex secrets test -p <config>          # one config only
```

Catches "env var not exported", "GCP/AWS auth expired", "secret URL
typo", "wrong project" before a real run wastes time. The output
prints ✓ / ✗ per secret without ever printing the value itself.

If a `${env.X}` ref fails, export the var. If a `secret://` ref fails
with a permission error, the SDK message body is now included in the
error (as of v0.1.4) — read it before guessing.

## 4. Common error patterns

### `ArrowInvalid: Could not convert '1599' with type str: tried to convert to int64`

A column the API returns as a string is declared as `INTEGER` in the
stream's schema. **This used to crash; now (post-stage-12) the engine's
NORMALIZE step coerces values to the declared FieldType automatically.**
If you still see it, the input shape is wrong — usually a nested dict
where the schema expects a flat column. Flatten in the connector before
yielding.

### `attempted relative import with no known parent package`

A project-local connector folder is missing `__init__.py`. The engine
loads connectors as packages; without the marker, `from .client import X`
fails. Add the empty file (`dtex new source` scaffolds it).

### `streams names stream(s) that do not exist on source <X>`

The config's `streams:` block references a stream the source doesn't
declare. Usually a typo (`chrages:` for `charges:`). The error lists
the valid stream names.

### `stream '<X>' has no incremental cursor`

The config sets `mode: incremental` on a stream whose `register.yaml`
has no `incremental:` block. Either add the incremental block to the
source's register.yaml (and yield a cursor column) or drop the `mode:`
override.

### State doesn't advance between runs

Three usual causes:

1. **Forgot `cursor.observe(...)`** in the `@stream` function's per-row
   loop. The engine commits `cursor.observed_max`; without `observe`,
   that stays `None` and the cursor never moves.
2. **Cursor field name mismatch** — `cursor.observe(row.get("updated_at"))`
   silently passes `None` if the API returns `updated` or `last_updated`.
   `dtex state` shows the actual cursor value committed last run; if
   it's `None`, this is the cause.
3. **The config has `mode: full_refresh`** for that stream. By design
   (post-redesign), full_refresh DOES NOT advance state. Switch to
   `mode: incremental` for it to advance.

### `ReadTimeout` on a large GCS upload (BigQuery destination)

Default GCS read timeout is 60s. Bump
`destination_params.job_timeout_seconds` in the config, or stage the
data in smaller batches by yielding more often from the `@stream`
function.

### "I ran `--full-refresh` and my sibling pipeline now re-pulls everything"

Pre-redesign behavior: `--full-refresh` reset the shared `_dtex_state`
cursor. **Post-redesign: it does NOT.** A run with `--full-refresh`
ignores the cursor for this invocation but leaves the row intact.
If you actually want to clear state for everyone, use
`dtex state reset <stream>` — that's the explicit operation.

## 5. Iterating quickly

```bash
dtex run -p <config> --select <one_stream>     # smaller scope
dtex run -p <config> --target dev              # confirm dev wiring first
```

For source authoring, prefer many small runs against one stream over
one big run against everything. The runner commits per-batch and per-
stream, so partial progress sticks even if a later stream fails.

## 6. When a run hangs

- Did the connector hit a long-poll endpoint without a timeout? Check
  `client.py` for missing `timeout=...` on the HTTP call.
- Is the connector waiting for an external resource (S3, GCS,
  Postgres) that's slow or down? Check the JSONL — every `batch_loaded`
  event is timestamped.
- Is the destination's `commit_state` waiting on a lock? Less common,
  but check the destination's logs.

`Ctrl+C` is safe — the per-stream transaction rolls back, prior
streams stay committed, and the run is marked `failed` in
`_dtex_runs` on the next launch.

## 7. When in doubt: read the JSONL

`.dtex/logs/<run_id>/run.jsonl` is structured, one event per line. The
event types you care about: `run_start`, `stream_start`, `batch_loaded`,
`stream_committed`, `stream_failed`, `run_end`. Each carries the data
needed to reconstruct what happened. Pipe through `jq` for readability:

```bash
jq -c '.event, .stream' .dtex/logs/<run_id>/run.jsonl
```

If the JSONL doesn't contain enough to diagnose, that's a logging gap
worth filing — open an issue.
