# 11 — Open Questions

This chapter consolidates every decision the handbook deliberately left open
during the v1 build. Most of them were settled while v1 shipped; a few remain
genuinely deferred. The page is split accordingly:

- **Resolved during the v1 build** — questions that have been answered by the
  code as it shipped. Kept in the chapter so a reader can see *why* the
  question was worth asking and *how* it was settled.
- **Deferred** — questions that genuinely stayed open past v1. Each carries a
  current lean and the point past which it gets expensive to change.

When this list reaches zero open items, declare v1 freeze.

## How to read this list

- **Lean** — the handbook's current recommendation. A lean is a default, not a
  decision.
- **Stakes** — what the choice actually affects, so you can tell the
  load-bearing questions from the cosmetic ones.

---

## Resolved during the v1 build

### Q1 — Project config filename
*Source: [02 — Architecture](./02-architecture.md), §The triad*

`det_project.yml` (symmetry with dbt's `dbt_project.yml`) vs. a shorter
`det.yml`.

- **Resolved:** `det_project.yml` — the dbt mirror aids the "if you know dbt,
  you know this" pitch. Never aliased.

### Q2 — `profiles.yml` location
*Source: [06 — Project Anatomy](./06-project-anatomy.md), §profiles.yml*

Project root (visible, version-controlled-by-default, easy to find) vs. a
user-home location like `~/.det/profiles.yml` (dbt's model — credentials
physically separated from the repo).

- **Resolved:** project root, with a hard `.gitignore` entry. Keeps everything
  for a project in one place; the security chapter's redaction and permissions
  guidance covers the exposure ([08](./08-security.md) §§4–6).

### Q3 — Secret resolver forms
*Source: [03 — Connector Contract](./03-connector-contract.md), §2.5*

Keep only `${env.X}` and `${profile.X.Y}`, or also add a `${vault...}` /
secret-manager resolver form.

- **Resolved:** add a **third** form — `secret://<scheme>/<path>[#<field>]` —
  alongside the original two, and make it the plugin surface (the URL syntax
  is in the contract; the resolvers themselves are loaded via entry-points
  OR a project-local `det_plugins.py`). The `${env.X}` and `${profile.X.Y}`
  built-ins stay unchanged. See [08 §3](./08-security.md) for the protocol
  and registration. The `det secrets test` command verifies wiring without
  leaking values.

### Q6 — Schema evolution: `strict` vs `evolve` default
*Source: [05 — Destinations & State](./05-destinations-and-state.md), §3.2*

Should additive schema evolution be opt-in per stream via
`schema_contract: strict|evolve` in `register.yaml`?

- **Resolved:** default `evolve`, allow `strict` opt-in. A stream with no
  declared schema infers it from the first batch and evolves additively; a
  `strict` stream fails the run if a batch diverges from the declared schema.
  The `SchemaContract` enum is part of the public `det.types` API.

### Q7 — Commit granularity: per-stream vs whole-run
*Source: [02 — Architecture](./02-architecture.md), §Commit granularity*

A `--atomic` flag that defers **all** cursor commits to the end of the run, for
all-or-nothing semantics. (The companion per-batch `state_granularity` question
is in the deferred section below.)

- **Resolved:** per-stream commit is the v1 default. A stream's cursor is
  written immediately after its batches durably land, so a crash on stream 7
  of 10 keeps streams 1–6's advanced cursors and the next run resumes
  mid-job. An `--atomic` whole-run mode remains a future flag.

### Q8 — `--threads N` flag
*Source: [02 — Architecture](./02-architecture.md), §Concurrency model*

Expose a dbt-style `--threads N` flag in v1 as a reserved no-op, or omit it
until parallel streams actually ship.

- **Resolved:** the flag shipped as a working knob, not a reserved no-op.
  `det run --tag <T> --threads N` runs matched configs through a
  `ThreadPoolExecutor` sized at `N`, capped per destination by each
  destination's `@destination.max_concurrent_writes` hook (DuckDB clamps to
  1, BigQuery defaults to 10). `profiles.yml` carries a project-wide
  `threads:` default. Stream-level parallelism within one pipeline stays
  deferred.

### Q10a — `det logs` command
*Source: [09 — Logging & Observability](./09-logging-and-observability.md)*

A `det logs <run_id>` command to pretty-print a past run's `run.jsonl`.

- **Resolved:** shipped as `det runs show <run_id> -p <config>` — prints the
  `_det_runs` summary AND the events from `run.jsonl`, which is the strictly
  stronger form. (Retention is in the deferred section.)

### Q13 — The v1 baked source connectors
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md), §v1 scope*

Which sources ship in v1. The handbook placeholder said "~3."

- **Resolved:** v1 ships **5 baked source connectors** — `filesystem`
  (CSV/JSONL/Parquet from local, GCS, or S3), `rest` (paginated REST APIs
  with 4 pagination strategies and 4 auth modes), `postgres` (keyset
  pagination, no `OFFSET`), `shiphero` (GraphQL), `stripe` (resource-as-
  stream over the REST API). Two baked destinations: `duckdb` (the
  zero-config dev default) and `bigquery` (the production warehouse).

### Q16 — License
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md)*

Apache-2.0 vs. MIT.

- **Resolved:** **Apache-2.0**. The explicit patent grant is the safer choice
  for a tool meant to be embedded and have connectors freely shared. See
  [LICENSE](../LICENSE).

---

## Deferred

### Q4 — Child / nested streams
*Source: [04 — Connector Body](./04-connector-body.md), §The record shape*

Should a connector declare a *child stream* whose extraction is parameterized
by each record of a parent stream (e.g. `orders` → `order_line_items`)?

- **Lean:** not in v1 — it adds a dependency graph to an otherwise flat stream
  list. Authors can express the same thing with explicit Python today (a
  second `@stream` that paginates the child resource).
- **Stakes:** whether `streams` stays a flat list or becomes a small DAG.
  This is a contract-shape decision, not an additive feature.
- **Decide by:** v2.

### Q5 — CDC within the `@stream` contract
*Source: [00 — Vision & Naming](./00-vision-and-naming.md), Non-goals*

Can a connector author implement change-data-capture (log-based replication)
inside the `@stream` generator model, or does CDC need a distinct contract?

- **Lean:** v2 revisits this. v1 is cursor-based incremental only. The
  baked `postgres` source uses keyset pagination, not logical replication.
- **Stakes:** whether the database-source connector can do log-based
  replication or stays limited to cursor polling.
- **Decide by:** v2 design.

### Q7b — Per-batch state commit
*Source: [05 — Destinations & State](./05-destinations-and-state.md), §5.3*

An opt-in `state_granularity: batch` so a crash 90% through a large backfill
does not restart from zero.

- **Lean:** v2 — add it for streams with a monotonic cursor.
- **Stakes:** crash-recovery behaviour and the resumability guarantee for
  very large backfills.
- **Decide by:** v2.

### Q9 — A streaming / iterator library API
*Source: [07 — CLI & Library API](./07-cli-and-library-api.md)*

Should the library expose `for batch in det.extract("stripe", "charges"): ...`
for users who want to handle loading themselves, in addition to whole-run
`det.run()`?

- **Lean:** v1 stays whole-run only; revisit after real demand. The public
  library surface today is `det.run()` and `det.run_tag()`.
- **Stakes:** supported API surface area and the "the library is the product"
  promise — a wider surface is a wider maintenance commitment.
- **Decide by:** post-v1.

### Q10b — Log retention
*Source: [09 — Logging & Observability](./09-logging-and-observability.md)*

A `--keep-logs <n>` retention flag for `.det/logs/`.

- **Lean:** v2 quality-of-life. `.det/logs/` is gitignored and operator-managed
  today.
- **Stakes:** operator ergonomics only.
- **Decide by:** v2.

### Q11 — Resolved-secret caching
*Source: [08 — Security](./08-security.md), §3*

Cache resolved secrets on disk (encrypted) to avoid a secret-manager round-trip
per run, or always fetch fresh.

- **Lean:** fresh-every-run. The current implementation caches the per-process
  resolver **instance** (so a GCP / AWS / Vault SDK init only runs once per
  process), but every reference value is re-fetched on every run. No on-disk
  cache.
- **Stakes:** run latency vs. attack surface at high invocation rates.
- **Decide by:** revisit only if round-trip cost becomes real.

### Q12 — Subprocess isolation for connectors
*Source: [08 — Security](./08-security.md), §7*

Run each connector in a child process with a scrubbed environment, no
`profiles.yml` access, only its own resolved config passed in.

- **Lean:** prototype after v1; decide based on whether a real
  community-connector ecosystem emerges. It contains accidents and shrinks
  blast radius but breaks in-process simplicity and adds IPC.
- **Stakes:** the trust model for third-party connectors — the central tension
  between "connectors are just Python" and "self-hosted users run untrusted
  code."
- **Decide by:** v2 design.

### Q14 — UI hosting model
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md)*

A local `det ui` serving from `.det/` and the destination, vs. a deployable
service, vs. both.

- **Lean:** local-first — keeps the self-hosted, no-backend promise intact.
- **Stakes:** whether det stays a pure CLI/library tool or grows a service to
  operate.
- **Decide by:** v3, or when UI work starts.

### Q15 — Connector registry: curated vs open
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md)*

A curated, reviewed connector registry vs. an open index — curation builds
trust but adds maintainer burden; an open index scales but pushes auditing
onto users.

- **Lean:** a curated "verified" tier layered over an open index.
- **Stakes:** community-ecosystem trust model and project maintainer load.
  Interacts with Q12.
- **Decide by:** when an ecosystem story is needed — post-v1.

---

## Priority of remaining work

The deferred questions that will most influence the next contract revision:

| # | Question | Why it is load-bearing |
|---|---|---|
| Q4  | Child streams | Flat list vs. stream DAG — a contract-shape change |
| Q5  | CDC in `@stream` | Affects whether `postgres` can do log-based replication |
| Q7b | Per-batch state commit | Crash-recovery on very large backfills |
| Q12 | Subprocess isolation | Trust model for community connectors |

The rest are flags, defaults, or roadmap items that can be settled as the
post-v1 work proceeds without reworking the architecture.
