# 02 — Architecture

> Part of the **det** design handbook. This file describes the core engine.
> The **connector contract**, the **destination capability tiers**, and the
> **CLI surface** are referenced here but fully specified by other handbook
> files — this document defines how the engine *uses* them, not their details.

## The triad: engine / library / project

det borrows dbt's separation between *the tool* and *the work*. There are
three things, and keeping them distinct keeps the system simple.

```
                          det  (the pip-installed Python package)
   ┌──────────────────────────────────────────────────────────────────┐
   │  ENGINE          the run loop: discover, resolve, run, commit     │
   │  LIBRARY (API)   det.run(...) — equal first-class to the CLI │
   │  BAKED CONNECTORS  meta_ads/, stripe/, bigquery/, ...  (folders)  │
   └──────────────────────────────────────────────────────────────────┘
                                  ▲
                       reads &    │    runs
                       executes   │
                                  ▼
   my_data_project/   (the user-owned PROJECT folder — in their repo)
   ├── det_project.yml      project config: name, defaults, tags
   ├── profiles.yml              environment config + secrets refs
   └── connectors/
       ├── custom/               a custom SOURCE connector folder
       │   ├── register.yaml         manifest: streams, config schema
       │   └── streams.py            @stream / @resource generators
       └── my_warehouse/         a custom DESTINATION connector folder
           ├── register.yaml
           └── destination.py       @destination-decorated functions
```

| Component | dbt analogue | Responsibility |
|---|---|---|
| **Engine** | `dbt-core` internals | Discovery, config resolution, the run lifecycle, state, the run record. |
| **Library** | importable `dbt` | `from det import run` — programmatic entry, equal to the CLI. |
| **CLI** | the `dbt` binary | `det run -c <name>` / `--tag <tag>`. A thin shell over the library. |
| **Baked connectors** | dbt's built-in macros | Connector folders shipped *inside* `det`. |
| **Project** | a dbt project | User-owned folder: `det_project.yml`, `profiles.yml`, `connectors/`. |

The CLI and the library are **the same engine** with two front doors. `det
run` parses argv and calls the same `run()` the library exposes. Nothing the CLI
can do is unavailable to the library, and vice versa.

> [Open question: project config filename. `det_project.yml` is proposed
> for symmetry with dbt's `dbt_project.yml`; a shorter `det.yml` is the
> alternative. Pick one before v1 and never alias.]

## Connector resolution: baked vs custom

A connector is named, not pathed. The engine resolves a name by precedence:

1. **Project-local** — `connectors/<name>/` in the user's project. (`"custom"`.)
2. **Baked** — `<name>/` inside the `det` package. (`"meta_ads"`.)

Project-local wins on a name clash, so a user can shadow a baked connector with
their own fork. Resolution produces a **ConnectorHandle**: the folder path, the
parsed `register.yaml`, and the imported decorated callables. Sources and
destinations resolve through the *same* path — they are the same kind of object.

## Run lifecycle

A `det run` (or `det.run()`) is one synchronous pass through a fixed
sequence. It either reaches `commit` and exits `0`, or it fails and exits non-zero.

The runtime unit is a **config** (chapter 12) — a pipeline file under
`configs/` naming source + destination + target + params. The CLI's `-p` /
`--conf` arg names the config; the library's `det.run(config=...)` mirrors it.

```
  det run -p my_pipeline
        │
        ▼
  ┌─────────────────────────────────────────────────────────────────┐
  │ 1. DISCOVER    locate project; LOAD CONFIG by name; resolve     │
  │                its source + destination (project-local-first)    │
  │ 2. RESOLVE     merge register.yaml + det_project.yml vars +     │
  │                config params + profiles.yml row + env + CLI     │
  │                → one frozen RunConfig                            │
  │ 3. INIT DEST   open destination connector; ensure it is ready;   │
  │                determine its capability tier                    │
  │ 4. LOAD STATE  read _det_state from the destination (keyed by   │
  │                source name — chapter 12 §6)                     │
  │ 5. RUN STREAMS for each selected stream  (sequential in v1):     │
  │      a. EXTRACT    drive the @stream generator → batches         │
  │      b. NORMALIZE  infer/evolve schema; coerce types per batch   │
  │      c. LOAD       hand normalized batch to the destination      │
  │      d. COMMIT     persist this stream's cursor to state         │
  │ 6. RUN RECORD  build a RunRecord and call destination's          │
  │                @destination.write_run_record (if Capability      │
  │                .RUN_RECORDS) → close → emit run_end to the per-  │
  │                run JSONL log → return RunResult. See chapter 09. │
  └─────────────────────────────────────────────────────────────────┘
```

Stages 1–4 are setup; stage 5 is the real work; stage 6 is the audit trail.

- **Discover** — find the project root (walk up for `det_project.yml`),
  load `configs/` and look up the named config, resolve its source +
  destination via project-local-first lookup (chapter 03 §5).
- **Resolve** — merge every config layer. For a source param:
  `register.yaml` defaults → `det_project.yml` `vars:` → the active config's
  `params:` block → environment variables → CLI/`run()` overrides. For a
  destination param: register.yaml defaults → project `vars:` →
  `profiles.yml[<destination>].targets[<target>]` → config's
  `destination_params:` → env → CLI/`run()` overrides. The result is a
  **frozen `RunConfig`** — nothing reads ambient config after this point.
- **Init destination** — open the destination connector and confirm it can
  receive writes. This also fixes its **capability tier** (next section), which
  decides where state lives.
- **Load state** — read prior cursors from `_det_state` so incremental
  streams know where they left off. A cold destination simply yields empty state.
- **Run streams** — the loop below.
- **Run record** — a machine-readable record (and a human summary) of what
  happened. This is det's audit surface; it is *not* a metadata catalog.

### Commit granularity

State is committed **per stream** (step 5d), immediately after that stream's data
is durably written — not once at the end. If stream 7 of 10 fails, streams 1–6
keep their advanced cursors and the next run resumes mid-job. This favors
**crash-safety over whole-run atomicity**, the right trade for synchronous EL.

> [Open question: a `--atomic` flag could defer all cursor commits to step 6 for
> users who want all-or-nothing semantics. Default stays per-stream.]

## The extract → normalize → load pipeline

Every stream flows through three stages. The split is borrowed from dlt and is
the reason memory stays bounded.

```
  @stream generator        normalizer            destination connector
  ┌───────────────┐       ┌───────────────┐      ┌────────────────────┐
  │ yield batch ─ ─┼─────▶ │ infer schema  │      │  write_batch(...)  │
  │ yield batch ─ ─┼─────▶ │ evolve schema ┼────▶ │  ... (tier logic)  │
  │ yield batch ─ ─┼─────▶ │ coerce types  │      │  flush             │
  └───────────────┘       └───────────────┘      └────────────────────┘
        EXTRACT               NORMALIZE                  LOAD
```

- **Extract.** The connector's `@stream` function is a generator. The engine
  *pulls* from it; the generator decides batch size by how much it `yield`s.
- **Normalize.** The engine reconciles each batch against the stream's schema and
  coerces values to the destination's type system. When a stream declares an
  explicit `schema` in `register.yaml` — the **recommended default**, and what
  the ShipHero proof case does — that schema is authoritative. When `schema` is
  omitted, the engine infers it from the first batch and evolves it additively as
  later batches introduce new columns or wider types (a convenience for
  prototyping; see chapter 03 §2.2.1).
- **Load.** The normalized batch is handed to the destination connector, which
  appends/merges/replaces per the configured write disposition.

### How `@stream` generators keep memory bounded

A `@stream` function is a plain Python generator. It does not build a list of all
rows — it **`yield`s a batch and pauses**. The engine consumes that batch
(normalize + load) before requesting the next. Peak memory is therefore *one
batch*, not one dataset, regardless of source size. Sketch:

```python
@stream(name="orders", primary_key="id")
def orders(client, cursor):
    page = cursor.get("last_id", 0)
    while True:
        batch = client.fetch_orders(after=page)   # one page
        if not batch:
            return
        yield batch                               # engine drains this, then resumes
        page = batch[-1]["id"]
```

`@resource` is the simpler form for a connector that produces one logical table
without its own cursor; `@stream` is the incremental-aware form. Both are
generators; both are pulled the same way. (Their full contract — signature,
`register.yaml` declaration, the class escape hatch — is owned by the connector
handbook.)

## Pipeline selection — by config name (post-8.B)

det selects work by **pipeline config name** (chapter 12):

- `det run -p shiphero_prod` — run one pipeline by its config name.
- `det run --conf shiphero_prod` — same, long-form alias.

> # NOTE: pre-8.B det supported `-c <connector>` and `--tag <tag>`
> selection. Stage 8.B removed both. A connector alone is not a complete
> runtime unit (no destination, no target); a config is. The `tags` key on a
> source's `register.yaml` is still parsed (it appears in `det list`), but
> no longer drives selection — a project author groups pipelines by writing
> multiple configs.

Tags are still declared in each connector's `register.yaml` and resolved at the
**discover** stage into a concrete connector set before anything runs. Selection
is purely a *filter over discovered connectors* — it adds no runtime concept,
which is why it passes the det test.

## Destination capability tiers

State lives in the destination — but not every destination *can* hold a state
table. Destinations therefore have **capability tiers**, fixed at *init* time:

| Tier | Examples | State storage | Notes |
|---|---|---|---|
| **Tier A — Stateful warehouse** | BigQuery, Snowflake, Postgres | `_det_state` table *inside the destination* | The simple, default case. One destination, one place for everything. |
| **Tier B — Stateless storage** | S3, GCS, local files | A **companion state backend** alongside the data | Object storage cannot host a queryable state table; a sidecar is required. |

A Tier B destination resolves a companion state backend (declared via the
`@destination.state_backend` hook); the engine routes **load state** / **commit
state** there instead of to the data target. The stream-running logic is
identical across tiers — only the state I/O path differs. The v1 default Tier B
backend is a **sidecar JSON file** (`_det_state.json`) co-located with the
data; the backend interface is pluggable so a transactional store can be added
later without an engine change. (Full detail: *05 — Destinations & State*.)

## Concurrency model

det's concurrency stance is deliberately minimal — concurrency is the most
reliable place for an EL tool to acquire bugs, so v1 spends its complexity budget
elsewhere.

- **Across streams: sequential.** v1 runs selected streams one at a time, in
  declared order. Output is deterministic; failures are easy to localize;
  per-stream commit (above) already gives resumability without parallelism.
- **Within a stream: pipelined.** Extract, normalize, and load form a producer/
  consumer pipeline. While the destination writes batch *N*, the generator may
  already be producing batch *N+1*. This is bounded-buffer pipelining, not
  unbounded fan-out — peak memory stays a small number of in-flight batches.
- **Where parallelism *could* go later** (documented, not built in v1):
  1. **Independent streams in parallel** — streams touch disjoint cursors and
     (usually) disjoint tables, so a worker pool is natural. Needs a concurrency
     cap and per-destination connection limits.
  2. **Partitioned extract within one stream** — e.g. a date-ranged backfill
     split into sub-ranges. Requires the connector to declare partitionability.

  Both are additive: they change *how fast* step 5 runs, never the lifecycle or
  the contract. v1 ships sequential-across / pipelined-within and stops there.

> [Open question: whether v1 exposes a `--threads N` flag (dbt-style) as a no-op
> placeholder reserving the name, or omits it until parallel streams actually
> ship. Reserving the name avoids a future breaking change.]

## What this document does not own

By design, the architecture references three things specified elsewhere:

- **Connector contract** — the exact `register.yaml` schema, the `@stream` /
  `@resource` signatures, and the class escape hatch.
- **Destinations** — the per-destination write semantics and the Tier B
  companion-backend implementation.
- **CLI** — the full `det` command/flag surface.

The engine's promise to all three is constant: discover them, resolve their
config into a frozen `RunConfig`, drive their generators through extract →
normalize → load, keep state in the destination, and emit a run record.
