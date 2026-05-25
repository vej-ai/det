# 09 — Logging and Observability

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.

The operator's requirement is plain: **"I want to know exactly what is happening."** det meets it with one mechanism — structured per-run logging — used two ways: a JSON-lines file every run writes to disk, and a `_det_runs` audit row the destination persists. There is no separate metrics system, no agent, no telemetry. Logs *are* the observability layer.

---

## 1. Principles

- **Structured first.** Every event is a JSON object. Human-readable text is a *rendering* of that object for a TTY, not a separate code path.
- **Per-run scoped.** Every event carries the `run_id`. One run's story is fully reconstructable from its log file alone.
- **Self-hosted, no phone-home.** Logs are written to the local filesystem and (optionally) the destination. det sends nothing anywhere.
- **Two surfaces, one source of truth.** The JSONL file is the *narrative* (forensics). The `_det_runs` table is the *receipt* (queryability). The engine builds the table row from the same `RunResult` it returned to the caller.
- **Redaction is a property of the bus, not the call.** Both surfaces scrub through one shared redactor. Adding a secret value masks it everywhere from the next event onward.

---

## 2. Run lifecycle events (the JSONL taxonomy)

The engine emits a fixed, ordered set of events to `.det/logs/<run_id>/run.jsonl`. This is the contract a UI or alerting rule can rely on.

| Event | When | Key fields |
|---|---|---|
| `run_start` | After RESOLVE; the run has bound a source + destination + target | `config`, `source`, `destination`, `target`, `full_refresh` |
| `stream_start` | Each stream begins (after the engine seeds its cursor) | `stream`, `disposition`, `cursor_before` |
| `batch_loaded` | Each batch persisted via `write_batch` | `stream`, `rows`, `cumulative_rows` |
| `stream_committed` | Each stream's transaction closed (data + state durably committed) | `stream`, `rows_loaded`, `cursor_after` |
| `stream_failed` | A stream raised — written before the run unwinds | `stream`, `error_type`, `error_message`, `traceback` |
| `run_end` | The run finishes (success or failure) | `status`, `rows_loaded`, `duration_s`, `error_type`, `error_message` |
| `user` | Any `log.info(...)` / `log.warning(...)` / `log.error(...)` from a `@stream` function | `level`, `message`, `stream` (when raised inside a stream) |

Every event also carries `ts` (ISO-8601 UTC with offset — `2026-05-25T14:32:01.123456+00:00`), `run_id`, and `event`. The ordering mirrors the destination lifecycle in [05 §5.3](./05-destinations-and-state.md): `stream_committed` always follows the `batch_loaded`s for a stream. On a stream failure, `stream_failed` replaces `stream_committed` for that stream and the run unwinds.

`run_start` is emitted *after* DISCOVER + RESOLVE so every bound field (`config` / `source` / `destination` / `target`) is known. A failure before that point has no `run_start` and no JSONL file — failure to discover is fully captured by the CLI's exit code + stderr message, not the log.

The `user` event is the bridge between a connector author's `log.info(...)` and the JSONL bus: every stdlib log call from a `@stream` function is mirrored here as `event=user` with the right `level` and the active stream name, so the connector's chatter survives without conflicting with the engine's taxonomy.

---

## 3. Where logs go

### 3.1 stdout

While `det run` blocks (it always runs synchronously — see [07 §3](./07-cli-and-library-api.md)), it streams stdlib log records to stderr in the usual format:

```
2026-05-25 14:32:01,123 [INFO] det: running stream 'items'
```

A future `--log-level debug|info|warn|error` flag will control stdout verbosity; v1 ships at INFO.

### 3.2 The per-run JSONL log file

Independent of stdout verbosity, **every run writes a complete JSON-lines file** under the project's `.det/` directory:

```
.det/
  logs/
    run-a1b9f3.../          # one directory per run_id
      run.jsonl             # every event, JSON-lines
    run-9c2d10.../
      run.jsonl
```

`.det/logs/` is project-rooted (the engine creates it under the directory holding `det_project.yml`), so `det runs show` from any sub-directory finds the right file.

A line from `run.jsonl`:

```json
{"ts":"2026-05-25T03:14:07.882000+00:00","run_id":"run-a1b9f3...","event":"stream_committed","stream":"items","rows_loaded":5,"cursor_after":5}
```

The file is opened with line-buffering and each write ends with `\n`, so a crash mid-run leaves a partial-but-readable line-terminated file. The full traceback for a failed run lives here (in the `stream_failed` event) — it is the forensics surface for "what happened to this run".

Log retention is the operator's call: `.det/logs/` is gitignored. det does not auto-delete logs in v1; a future `--keep-logs N` flag is tracked as a v2 quality-of-life item.

---

## 4. The `_det_runs` audit table

Where the JSONL file is the *narrative*, the `_det_runs` row is the *receipt* — the structured summary one row carries per run, queryable with plain SQL.

The destination owns this table. A destination declaring `Capability.RUN_RECORDS` implements `@destination.write_run_record(conn, record)`; the engine calls it once per run, after streams finish and before `close`, with a fully-built `RunRecord`.

### 4.1 Schema

The logical schema (destination-agnostic):

| Column | Type | Description |
|---|---|---|
| `run_id` | string, **PK** | One row per run. Upsert key. |
| `config` | string | The pipeline config name (`-p / --conf` arg). |
| `source` | string | Source connector name. |
| `destination` | string | Destination connector name. |
| `target` | string | `profiles.yml` target used. |
| `status` | string | `succeeded` / `failed`. |
| `started_at` | timestamp | Run window open. |
| `ended_at` | timestamp | Run window close. |
| `duration_s` | float | Wall-clock seconds. Computed at write time. |
| `rows_loaded` | int | Total rows across streams. |
| `full_refresh` | bool | Whether prior state was discarded. |
| `error_type` | string | Exception class on failure (`HTTPError`); `NULL` on success. |
| `error_message` | string | Exception message on failure; `NULL` on success. |
| `streams_json` | json | Per-stream breakdown: `[{"name":..., "rows_loaded":..., "cursor_after":..., "status":...}, ...]`. Same shape as `StreamResult.to_dict()` so admin/UI drill-down needs no join. |

`run_id` is the primary key and the upsert target: writing the same `run_id` twice updates the row rather than duplicating it. The engine only writes once per run, but the upsert is the defensive guarantee — a future retry-on-transient cannot corrupt the audit chain.

### 4.2 What is deliberately NOT here

- **Traceback** — the full traceback is verbose, embeds filesystem paths, and bloats `_det_runs` in a way that confounds SQL filtering. It lives in the JSONL `stream_failed` event (the forensics surface).
- **The `error` object itself** — the `RunResult.error` is a live Python exception; only its string projection (`error_type`, `error_message`) crosses the table boundary. The JSONL also has the traceback.

### 4.3 Tier-A only, for now

`_det_runs` is hosted by the destination, in the same store as `_det_state` and the loaded data — so "show me every failed `echo_dev` run this week" is one `SELECT`. v1 ships DuckDB (declares `Capability.RUN_RECORDS`); BigQuery follows in stage 8b. A destination that does NOT declare the capability is fully valid — the engine simply skips the table write and the JSONL log file remains the durable record.

A destination that *does* declare `Capability.RUN_RECORDS` but does not implement `@destination.write_run_record` is rejected at run start with a clear `EngineError` — same conditional-mandatory pattern as `@destination.transaction` under `Capability.TRANSACTIONAL_LOAD` (see [05 §1](./05-destinations-and-state.md)).

---

## 5. Log levels and redaction

| Level | Use |
|---|---|
| `debug` | Per-request detail: API pages, SQL, retries. (Connector authors emit at this level via `log.debug`.) |
| `info` | Lifecycle events + connector "what I'm doing now" messages. The stdout default. |
| `warn` | Recoverable issues: a retried request, a skipped stream, a deprecated config key. |
| `error` | A failure that ends the run or fails a stream. |

**Redaction is enforced in the logging layer**, per the security contract in [08 §6](./08-security.md). Both sinks (stdlib + JSONL) share one `Redactor`: any value marked `secret: true` or resolved from `${env.X}` / `${profile.X.Y}` is replaced with `***` in *every* sink. Redaction is value-based: det scrubs known secret values out of the final rendered text (a stdlib message; a serialized JSONL line) before writing, so a secret accidentally interpolated into a URL, an HTTP error body, or a structured field is also caught.

The JSONL writer redacts *after* serialization — the entire JSON line is run through the redactor — so a secret in a nested field (`{"event":"user","message":"...","extra":{"token":"..."}}` ) is masked, not just one at the top level. This is best-effort against a hostile connector ([08 §7](./08-security.md)) but a reliable guard against accidental leakage.

---

## 6. The `det runs` CLI

Two commands read this layer back:

### `det runs list -p <config> [--limit N]`

Show recent runs from the destination's `_det_runs`. `-p <config>` is **required** — run records are per-destination, and the config disambiguates which destination's table to query. (In a project where every config targets the same destination, this redundancy is the price for not inventing a multi-destination union that v1 cannot honour. A future `--destination <name>` flag is the natural relaxation.) A target without `_det_runs` yet (a brand-new project) prints "no run records".

### `det runs show <run_id> -p <config>`

Show one run's full record + every event in its `.det/logs/<run_id>/run.jsonl`. Accepts the short id (`abc123def...`) or the long form (`run-abc123def...`). On a TTY, events are colored by type; piped output is plain.

Both commands open the destination via its own `@destination.open` / `@destination.close` hooks (same pattern as `det state list`) and run a parameterized `SELECT` on the connection. Like `det state reset`, this reaches past the destination hook contract — there is no `read_run_records` hook in v1, and SQL-direct querying is the v1 limitation. DuckDB is the only Tier-A destination shipping v1 with `Capability.RUN_RECORDS`; future Tier-A destinations follow the same pattern (the table is the contract; the implementation hands back rows the CLI shape).

---

## 7. Feeding future UI and orchestrators

The structured design is what makes deferring the UI (see [10 — Roadmap](./10-roadmap-and-scope.md)) safe — the data is already there when the UI arrives:

- **A future UI** reads `_det_runs` and `_det_state` straight from the destination, plus `run.jsonl` for drill-down. No new collection layer, no agent — the UI is a *reader* of data det already writes.
- **Orchestrators** consume the `RunResult` returned by `det.run()` ([07 §4.1](./07-cli-and-library-api.md)) — `run_id`, per-stream counts, `log_path`, the populated `error` on failure — and attach it as native run metadata.
- **Alerting** is just a query: a scheduled check over `_det_runs` for `status = 'failed'`, or a CI step that inspects the exit code ([07 §3](./07-cli-and-library-api.md)).

One mechanism — structured per-run logs plus a run record — serves the operator today and the UI and orchestrators later. That is the simplicity bar, met.

### Reference

- `RunResult` / `RunRecord` / `StreamResult` shapes → `det/types.py` (the source of truth)
- State lifecycle, capability tiers, `.det/` layout → [05 — Destinations and State](./05-destinations-and-state.md)
- The `@destination` hook namespace + `write_run_record` → [03 — The Connector Contract](./03-connector-contract.md) §3.4
- Redaction and the secret model → [08 — Security](./08-security.md)
- Deferred UI and orchestrator integration → [10 — Roadmap and Scope](./10-roadmap-and-scope.md)
