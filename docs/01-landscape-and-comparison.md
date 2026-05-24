# 01 — Landscape & Comparison

> Part of the **det** design handbook. This file surveys the existing EL
> tools, scores each against the *det test* defined in File 00 (five-minute
> readability, concept count, "could plain Python do it"), and ends with an
> honest accounting of what det steals and what it rejects.

det is not being born into an empty field. The EL space has mature tools.
The point of this survey is to be precise about *which* good idea comes from
*which* tool, and to avoid re-making mistakes that are already well documented.

## dlt (dlthub)

**Architecture model.** An embedded Python library. There is no server, no
daemon, no separate process. You write a `Pipeline` object and call `.run()`;
internally dlt executes three stages — **extract → normalize → load**. Extract
pulls data from resources; normalize infers and evolves the schema and writes
intermediary files; load ships those files to the destination.

**Connector authoring.** Plain Python. A data-producing function is decorated
with `@dlt.resource`; a `@dlt.source` groups resources. Resources are
**generators** — they `yield` rows or batches lazily, so memory stays bounded.
dlt builds an extraction DAG from resource dependencies and infers the schema
automatically, including evolution as upstream shapes drift.

**State handling.** State lives **in the destination**, in a `_dlt_pipeline_state`
table. On a cold start dlt restores state from there. State is scoped per
resource (private) or per source (shared). Incremental loading uses
`dlt.sources.incremental` cursors plus write dispositions: `replace`, `append`,
`merge` (upsert via `primary_key` / `merge_key`).

**Strengths.** Connectors are just Python — readable, testable, debuggable.
Schema inference/evolution is excellent. State-in-destination means no extra
infrastructure. Pip-installable, embeds anywhere.

**Weaknesses.** No project-level convention — dlt gives you a library, not a
"shape" for a repo. No CLI-first selector/tag workflow comparable to dbt. The
API surface is wide (sources, resources, transformers, the `Pipeline` object,
configuration providers) — powerful but more than the minimum.

## Airbyte

**Architecture model.** A platform: web UI, control plane, scheduler, and
connectors that run as **separate processes / Docker images**. Connectors speak
a **JSON-over-stdio protocol** (descended from Singer): `SPEC`, `CHECK`,
`DISCOVER`, then `RECORD` / `SCHEMA` / `STATE` messages.

**Connector authoring.** Three tiers. (1) **No-code** Connector Builder UI. (2)
**Low-code CDK** — a declarative `manifest.yaml` describing streams as composable
blocks: a *retriever* with a *requester* (URL, auth), a *paginator*, a *record
selector*, an optional *partition router* and *cursor*. (3) **Full Python CDK**
(`airbyte-cdk`) for logic the YAML cannot express. A connector folder also
carries `metadata.yaml`.

**State handling.** State is `STATE` messages emitted by the source, persisted by
the Airbyte platform, and replayed on the next sync. Incremental sync uses a
cursor field; state lives in Airbyte's control plane, not the destination.

**Strengths.** Huge catalog. The low-code building blocks genuinely capture the
formulaic 80% of REST APIs (auth, pagination, rate limits). UI lowers the entry
bar for non-engineers.

**Weaknesses (and the cautionary tale).** The declarative `manifest.yaml`
encodes *stream logic* in YAML; the moment an API needs a real conditional you
either drop to the Python CDK or fight the YAML. Container-per-connector adds an
operational tax (image builds, registries, the stdio protocol). The UI as
source-of-truth means connector definitions live in a database, not a repo —
hard to review, hard to diff. This is the model det exists to reject.

## Fivetran

**Architecture model.** A fully managed SaaS. Fivetran owns the infrastructure,
the scheduling, the scaling, the monitoring. You configure connectors; you do
not run them.

**Connector authoring.** Mostly you don't — you use Fivetran's catalog. The
**Connector SDK** (2025) lets you write a custom connector in Python and *deploy
it into Fivetran's managed runtime*, where Fivetran handles orchestration,
retries, and scaling.

**State handling.** Entirely managed and opaque. Cursors, checkpoints, and
incremental bookkeeping are Fivetran's internal concern; you do not see or
control the state store.

**Strengths.** Zero operational burden. Reliable. Broad catalog.

**Weaknesses.** Closed, proprietary, usage-priced. No local execution, no
self-hosting, the connector runtime is a blackbox. For det's purpose
Fivetran is the **foil**: it is everything an open-source, repo-native,
locally-runnable tool is defined against. It contributes no positive idea.

## Meltano / Singer

**Architecture model.** Singer is a **protocol**, not a runtime: extractors
(**taps**) and loaders (**targets**) are independent executables that communicate
via **stdin/stdout** using newline-delimited JSON — `SCHEMA`, `RECORD`, `STATE`
messages. A tap accepts a config file and optional `state` and `catalog` files.
**Meltano** is the orchestrator on top: a project tool (`meltano.yml`) that
installs taps/targets, wires config, manages selection, and tracks state per
job. The **Singer SDK** (Meltano) is a Python framework for writing taps/targets
without hand-rolling the protocol.

**Connector authoring.** A tap or target is a standalone CLI program. With the
Singer SDK you subclass `Tap` / `Stream` (or `Target`). The uniform tap/target
shape means *any* tap pairs with *any* target.

**State handling.** State is a `STATE` message: the tap emits it, the target
forwards it once data is persisted, Meltano stores it keyed by job ID and feeds
it back on the next run.

**Strengths.** The **tap/target uniformity** is the durable good idea — sources
and destinations are the same kind of thing, freely composable. `meltano.yml` as
a project manifest is a clean convention. Large ecosystem of taps.

**Weaknesses.** The stdio JSON protocol is a per-process, per-row serialization
tax and an awkward debugging surface (two processes piped together). Tap quality
varies wildly across the community. `catalog.json` selection is verbose. The
process-boundary protocol is exactly what det removes by running connectors
in-process.

## Sling

**Architecture model.** A single **Go binary** with a streaming engine that holds
minimal data in memory. CLI-first; also embeddable. No server.

**Connector authoring.** Sling is **not** connector-authoring oriented — it does
not have a connector SDK. It excels at **database↔database↔file** movement using
built-in drivers. Work is described in a **replication YAML**: `source`,
`target`, a `defaults` block, and a `streams` map. Streams support **wildcards**
(`my_schema.*`) and per-stream overrides of the defaults. Modes:
`full-refresh`, `truncate`, `incremental` (merge/append), `snapshot`, `backfill`.

**State handling.** Incremental uses a `primary_key` + `update_key` on the
target; Sling reads the max cursor from the target itself — state is effectively
in the destination, no separate store.

**Strengths.** Superb CLI ergonomics — one binary, fast, streaming. The
`defaults` + per-stream-override YAML pattern is concise and DRY. Wildcard stream
selection is excellent for "replicate this whole schema."

**Weaknesses.** Closed engine internals (Go, not the user's language). Weak story
for **custom API connectors** — there is no `@stream`-style authoring contract;
you are mostly limited to the connectors Sling ships. Less schema-evolution
sophistication than dlt.

## Comparison

| Dimension | dlt | Airbyte | Fivetran | Meltano/Singer | Sling | **det** |
|---|---|---|---|---|---|---|
| **Form factor** | Python library | Platform + UI + control plane | Managed SaaS | Project tool over CLI taps | Single Go binary | Python library + CLI + project |
| **Connector runs** | In-process | Separate process / Docker | Fivetran's cloud | Separate process (stdio) | In-process (Go) | **In-process (Python)** |
| **Authoring model** | `@dlt.resource` generators | YAML manifest or Python CDK | Connector SDK (Python, managed) | `Tap`/`Stream` subclass | None (built-in drivers) | **Folder + `register.yaml` + `@stream`/`@resource`** |
| **Custom API connectors** | Excellent | Good (low-code) → Python | Good (deploy to FT) | Good (Singer SDK) | Weak | **Excellent (the core use case)** |
| **Source/dest uniformity** | Partial | Yes (protocol) | N/A (managed) | **Yes** (tap/target) | Implicit | **Yes** (one contract) |
| **State location** | Destination table | Control plane | Opaque managed | Meltano store (job-keyed) | Destination (cursor read) | **Destination (`_det_state`)** |
| **Schema inference** | Strong + evolution | Per-connector | Managed | Per-tap | Moderate | Inherit dlt-style inference |
| **Project convention** | None | DB-backed | SaaS config | `meltano.yml` | Replication YAML | **dbt-style project folder** |
| **Selection model** | Code | UI toggles | UI toggles | `catalog.json` | YAML wildcards | **Tags + `-c` + wildcards** |
| **Invocation** | `pipeline.run()` | Scheduler/UI | Managed schedule | `meltano run` | `sling run -r` | `det run` / `import` |
| **Open source** | Yes | Yes (+ paid cloud) | No | Yes | Core yes | **Yes** |
| **Passes the det test** | Mostly | No (YAML logic, blackbox UI) | No (closed SaaS) | Partly (protocol tax) | Mostly (but no authoring) | — (by construction) |

## What det steals — and what it rejects

The influences are not equal. Ranked honestly:

**dlt — the closest cousin. Steal the core.**
- ✅ Connectors are Python **generators** decorated with `@stream` / `@resource`.
- ✅ **State lives in the destination** (`_det_state`, cf. `_dlt_pipeline_state`).
- ✅ Schema **inference and evolution**, and the **extract → normalize → load**
  stage split.
- ❌ Reject the lack of a project shape. det adds the dbt-style project so a
  team has *one* obvious layout, not a library and a blank page.

**dbt — the architectural template. Steal the shape.**
- ✅ A **project folder** of plain files, under version control.
- ✅ **CLI verbs** (`det run`), **profiles** for environment config, and a
  **tag-based selector** (`--tag`, `-c`).
- ✅ The **core/library/project triad**: engine = `dbt-core`, project = a dbt
  project. (Detailed in File 02.)

**Sling — steal the CLI ergonomics.**
- ✅ **Single-command CLI** that "just runs," fast and synchronous.
- ✅ The **`defaults` + per-stream override** YAML pattern and **wildcard stream
  selection** — adopted into `register.yaml` / project config.
- ❌ Reject the closed engine and the missing authoring contract — that gap is
  precisely det's reason to exist.

**Singer / Meltano — steal the *idea*, not the protocol.**
- ✅ **Sources and destinations are the same kind of thing** — one contract,
  freely composable. det's "destinations use the same folder+yaml+decorator
  contract" is this idea.
- ✅ A **project manifest** convention (`meltano.yml` → det project config).
- ❌ Reject the **stdin/stdout JSON protocol** and the process boundary. det
  runs connectors **in-process**: no serialization tax, real stack traces.

**Airbyte — the cautionary tale. Steal almost nothing.**
- ✅ Keep one narrow idea: a **YAML manifest for connector *metadata and
  config*** (`register.yaml`, cf. `metadata.yaml`).
- ❌ Reject **YAML for stream *logic*** — the low-code `manifest.yaml` is the
  anti-pattern. Pagination and conditionals belong in Python.
- ❌ Reject the **UI as source of truth** — connectors live in a repo.
- ❌ Reject **container-per-connector** and the control-plane state store.

**Fivetran — the foil. Steal nothing.** Closed, managed, opaque. det is
defined in opposition: open, local-runnable, repo-native, debuggable.

> **One-line summary:** det = dlt's connector core + dbt's project shape +
> Sling's CLI ergonomics + Singer's source/destination uniformity — with
> Airbyte's YAML-as-logic and Fivetran's closed SaaS deliberately left out.
