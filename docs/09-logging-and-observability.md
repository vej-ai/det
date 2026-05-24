# 09 — Logging and Observability

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.

The operator's requirement is plain: **"I want to know exactly what is happening."** det meets it with one mechanism — structured, per-run logging — used two ways: a human-readable stream while a run is in progress, and a machine-readable record after it. There is no separate metrics system, no agent, no telemetry. Logs *are* the observability layer.

---

## 1. Principles

- **Structured first.** Every log line is a JSON object. Human-readable text is a *rendering* of that object for a TTY, not a separate code path.
- **Per-run scoped.** Every line carries the `run_id`. One run's story is fully reconstructable from its log file alone.
- **Self-hosted, no phone-home.** Logs are written to the local filesystem and (optionally) the destination. det sends nothing anywhere.
- **Simple now, sufficient for later.** The same structured records that a human reads today are what a future UI and orchestrators consume tomorrow — without a new system.

---

## 2. Run lifecycle events

A run emits a fixed, ordered set of event types. This is the contract a UI or alerting rule can rely on.

| Event | When | Key fields |
|---|---|---|
| `run_start` | Run begins | `run_id`, `connector`, `target`, `destination`, `streams`, `full_refresh` |
| `stream_start` | Each stream begins | `stream`, `disposition`, `cursor_before` |
| `batch_written` | Each batch persisted | `stream`, `batch_n`, `rows` |
| `stream_end` | Each stream finishes | `stream`, `rows_extracted`, `rows_loaded`, `cursor_after`, `status` |
| `state_committed` | State persisted (whole run) | `cursors` (per-stream resume points) |
| `run_end` | Run finishes | `status`, `rows_loaded`, `duration_s`, `error` (if failed) |
| `warn` / `error` | Any time | `message`, `stream` (if applicable), `exc_type` |

The ordering mirrors the destination lifecycle in [05 §5.3](./05-destinations-and-state.md): `state_committed` always follows every `stream_end` and always precedes `run_end` on success. On failure, `state_committed` is **absent** — its absence in the log is itself the signal that state did not advance.

---

## 3. Where logs go

### 3.1 stdout

While `det run` blocks (it always runs synchronously — see [07 §3](./07-cli-and-library-api.md)), it streams events to stdout:

- **TTY** → human-rendered, one line per event, as shown in [07 §2](./07-cli-and-library-api.md).
- **Piped / not a TTY** (CI, orchestrator) → raw JSON-lines, one object per line. No rendering, fully parseable.

`--log-level debug|info|warn|error` controls stdout verbosity. `debug` adds per-request detail (paginated API calls, SQL issued, retries); `info` is the default and shows the lifecycle events above.

### 3.2 The per-run log file

Independent of stdout verbosity, **every run writes a complete `debug`-level JSON-lines file** under the project's `.det/` directory:

```
.det/
  logs/
    a1b9f3/                 # one directory per run_id
      run.jsonl             # every event, JSON-lines, always debug-level
    9c2d10/
      run.jsonl
  runs/                     # run records — see section 4 (Tier B fallback)
  state/                    # sidecar state for Tier B destinations — see 05
```

So a quiet `info` run on screen still leaves a full `debug` trace on disk for post-mortem. A line from `run.jsonl`:

```json
{"ts":"2026-05-21T03:14:07.882Z","run_id":"a1b9f3","level":"info","event":"batch_written","connector":"stripe","stream":"charges","batch_n":2,"rows":2310}
```

Log retention is the operator's call: `.det/logs/` is gitignored and can be cleaned by cron or by `det state` housekeeping. det does not auto-delete logs in v1.

> [Open question: a `det logs <run_id>` command to pretty-print a past run's `run.jsonl`, and a `--keep-logs <n>` retention flag. Both are small, both are quality-of-life, neither is v1-critical. Proposal: v2.]

---

## 4. The run record

A run record is the **structured summary** of one run — the same `RunResult` object the library returns ([07 §4.1](./07-cli-and-library-api.md)), persisted. Where the per-run log file is the *narrative*, the run record is the *receipt*.

```json
{
  "run_id": "a1b9f3",
  "connector": "stripe",
  "target": "prod",
  "destination": "bigquery",
  "status": "succeeded",
  "started_at": "2026-05-21T03:13:26Z",
  "ended_at":   "2026-05-21T03:14:07Z",
  "duration_s": 41.2,
  "rows_loaded": 9150,
  "full_refresh": false,
  "streams": [
    {"name":"charges","rows_extracted":7310,"rows_loaded":7310,
     "cursor_before":"2026-05-20T00:00:00Z","cursor_after":"2026-05-21T00:00:00Z","status":"succeeded"},
    {"name":"customers","rows_extracted":1840,"rows_loaded":1840,
     "cursor_before":null,"cursor_after":null,"status":"succeeded"}
  ],
  "error": null
}
```

### 4.1 Where run records are stored

Run records follow the **same capability-tier model as state** ([05 §5.4](./05-destinations-and-state.md)) — this keeps det's data-placement story consistent: data, state, and run records all sit together, wherever "together" is.

- **Tier A destinations** → a `_det_runs` table in the destination, alongside `_det_state`. One row per run. This makes run history *queryable with SQL* — "show me every failed stripe run this week", "rows loaded per day" — with no extra tooling. The table:

  | Column | Type | Description |
  |---|---|---|
  | `run_id` | `string` | Primary key. |
  | `connector` | `string` | Connector name. |
  | `target` | `string` | Profile target used. |
  | `status` | `string` | `succeeded` / `failed`. |
  | `started_at` / `ended_at` | `timestamp` | Run window. |
  | `rows_loaded` | `int` | Total rows across streams. |
  | `full_refresh` | `bool` | Whether state was discarded. |
  | `streams` | `json` | Per-stream array (as above). |
  | `error` | `string` | Error message + type, `NULL` on success. Redacted (§5). |

- **Tier B destinations** (object storage, no tables) → the run record is written as `.det/runs/<run_id>.json` locally, and optionally mirrored as a `_det_runs/<run_id>.json` object in the bucket. Symmetric with the Tier B sidecar-state design.

The run record is **always** written locally to `.det/runs/` regardless of tier, so run history survives even if the destination is unreachable for the write. The destination copy is the shared, queryable source of truth; the local copy is the durable fallback.

A run record is written even when a run **fails** — `status: "failed"` with a populated `error`. A failed run that produced no run record means det itself crashed before it could write one; that is the only ambiguous state, and the per-run log file (written incrementally) covers it.

---

## 5. Log levels and redaction

| Level | Use |
|---|---|
| `debug` | Per-request detail: API pages, SQL, retries, resolved (redacted) config. Always in `run.jsonl`. |
| `info` | Lifecycle events (§2). Default for stdout. |
| `warn` | Recoverable issues: a retried request, a skipped stream, a loose `profiles.yml` permission. |
| `error` | A failure that ends the run or fails a stream. |

**Redaction is enforced in the logging layer**, per the security contract in [08 §6](./08-security.md): any value marked `secret: true`, or resolved from `${ENV_VAR}` / `secret://`, is replaced with `***` in *every* sink — stdout, `run.jsonl`, and the `_det_runs` record. Redaction is value-based: det scrubs known secret values out of log strings before writing, so a secret echoed inside an HTTP error body is also caught. This is best-effort against a hostile connector ([08 §7](./08-security.md)) but a reliable guard against accidental leakage.

---

## 6. Feeding future UI and orchestrators

The structured design is what makes deferring the UI (see [10 — Roadmap](./10-roadmap-and-scope.md)) safe — the data is already there when the UI arrives:

- **A future UI** reads `_det_runs` and `_det_state` straight from the destination, plus `run.jsonl` for run drill-down. No new collection layer, no agent — the UI is a *reader* of data det already writes.
- **Orchestrators** consume the `RunResult` returned by `project.run()` ([07 §4.3](./07-cli-and-library-api.md)) — `run_id`, per-stream counts, `log_path` — and attach it as native run metadata (Dagster asset metadata, Airflow XCom). The orchestrator gets observability for free because the library hands it a structured result.
- **Alerting** is just a query: a scheduled check over `_det_runs` for `status = 'failed'`, or a CI step that inspects the exit code ([07 §3](./07-cli-and-library-api.md)).

One mechanism — structured per-run logs plus a run record — serves the operator today and the UI and orchestrators later. That is the simplicity bar, met.

### Reference

- `RunResult` / `StreamResult` shapes → [07 — CLI and Library API](./07-cli-and-library-api.md)
- State lifecycle, capability tiers, `.det/` layout → [05 — Destinations and State](./05-destinations-and-state.md)
- Redaction and the secret model → [08 — Security](./08-security.md)
- Deferred UI and orchestrator integration → [10 — Roadmap and Scope](./10-roadmap-and-scope.md)
