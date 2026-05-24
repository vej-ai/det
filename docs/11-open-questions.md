# 11 — Open Questions

This chapter consolidates every decision the handbook **deliberately left open**.
None of them block writing the plan; all of them should be resolved before the
**v1 freeze**. Each entry points back to the chapter that raised it, states the
current lean, and notes what is at stake.

This is the page to read with a pen — the rest of the handbook proposes a
coherent design; this page is where your judgement is explicitly invited.

## How to read this list

- **Lean** — the handbook's current recommendation. A lean is a default, not a
  decision; overrule freely.
- **Stakes** — what the choice actually affects, so you can tell the load-bearing
  questions from the cosmetic ones.
- **Decide by** — the point past which the answer gets expensive to change.

---

## A. Naming & project shape

### Q1 — Project config filename
*Source: [02 — Architecture](./02-architecture.md), §The triad*

`det_project.yml` (symmetry with dbt's `dbt_project.yml`) vs. a shorter
`det.yml`.

- **Lean:** `det_project.yml` — the dbt mirror aids the "if you know dbt,
  you know this" pitch.
- **Stakes:** Cosmetic, but it is the first file every user sees and renaming it
  later is a breaking change for every project.
- **Decide by:** v1 freeze. Never alias — pick one.

### Q2 — `profiles.yml` location
*Source: [06 — Project Anatomy](./06-project-anatomy.md), §profiles.yml*

Project root (visible, version-controlled-by-default, easy to find) vs. a
user-home location like `~/.det/profiles.yml` (dbt's model — credentials
physically separated from the repo).

- **Lean:** project root, with a hard `.gitignore` entry — keeps everything for a
  project in one place; the security chapter's redaction/permissions guidance
  covers the exposure.
- **Stakes:** Security posture and the credential trust model ([08](./08-security.md)).
- **Decide by:** v1 freeze.

---

## B. The connector contract

### Q3 — Secret resolver forms
*Source: [03 — Connector Contract](./03-connector-contract.md), §2.5*

Keep only `${env.X}` and `${profile.X.Y}`, or also add a `${vault...}` /
secret-manager resolver form.

- **Lean:** keep just the two; let a profile point at a manager-backed file
  rather than teaching `register.yaml` about every secret manager.
- **Stakes:** Connector portability and the secret-manager integration story
  ([08 §resolvers](./08-security.md)).
- **Decide by:** v1 freeze — the resolver grammar is part of the manifest contract.

### Q4 — Child / nested streams
*Source: [04 — Connector Body](./04-connector-body.md), §172*

Should a connector declare a *child stream* whose extraction is parameterized by
each record of a parent stream (e.g. `orders` → `order_line_items`)?

- **Lean:** not in v1 — it adds a dependency graph to an otherwise flat stream
  list. Authors can express the same thing with explicit Python today.
- **Stakes:** Whether `streams` stays a flat list or becomes a small DAG. This is
  a contract-shape decision, not an additive feature.
- **Decide by:** v1 freeze.

### Q5 — CDC within the `@stream` contract
*Source: [00 — Vision & Naming](./00-vision-and-naming.md), §142*

Can a connector author implement change-data-capture (log-based replication)
inside the `@stream` generator model, or does CDC need a distinct contract?

- **Lean:** treat CDC as a v2 question; v1 is cursor-based incremental only.
- **Stakes:** Whether the database-source connector (a likely v1 pre-baked
  connector — see Q12) can do log-based replication or is limited to cursor polling.
- **Decide by:** v2 design — but flag now, because a v1 Postgres source built
  cursor-only may need rework.

---

## C. State & loading semantics

### Q6 — Schema evolution: `strict` vs `evolve` default
*Source: [05 — Destinations & State](./05-destinations-and-state.md), §141*

Should additive schema evolution be opt-in per stream via
`schema_contract: strict|evolve` in `register.yaml`?

- **Lean:** default `evolve`, allow `strict` opt-in. (Note: dbt-style production
  strictness is an argument for `strict` as the *default* — worth weighing.)
- **Stakes:** Production safety. A surprise column in a strict pipeline should
  arguably fail loudly rather than silently `ALTER`.
- **Decide by:** v1 freeze.

### Q7 — Commit granularity: whole-run vs `--atomic` vs per-batch
*Source: [02 — Architecture](./02-architecture.md), §116 and [05](./05-destinations-and-state.md), §215*

Two related questions:
- A `--atomic` flag that defers **all** cursor commits to the end of the run, for
  all-or-nothing semantics (architecture currently commits per-stream).
- An opt-in `state_granularity: batch` so a crash 90% through a large backfill
  does not restart from zero.

- **Lean:** v1 ships per-stream commit; add `state_granularity: batch` in v2 for
  streams with a monotonic cursor; treat `--atomic` as a later flag.
- **Stakes:** Crash-recovery behavior and the resumability guarantee for large
  backfills.
- **Decide by:** `--atomic` — post-v1. `state_granularity` — v2.

---

## D. CLI & library surface

### Q8 — `--threads N` flag
*Source: [02 — Architecture](./02-architecture.md), §227*

Expose a dbt-style `--threads N` flag in v1 as a reserved no-op (so adding
parallelism later is not a breaking CLI change), or omit it until parallel
streams actually ship.

- **Lean:** reserve the name as a documented no-op — cheap insurance against a
  future breaking change.
- **Stakes:** CLI forward-compatibility only.
- **Decide by:** v1 freeze.

### Q9 — A streaming / iterator library API
*Source: [07 — CLI & Library API](./07-cli-and-library-api.md), §248*

Should the library expose `for batch in project.extract("stripe", "charges"): ...`
for users who want to handle loading themselves, in addition to whole-run
`project.run()`?

- **Lean:** v1 stays whole-run only; revisit after real demand.
- **Stakes:** Supported API surface area and the "the library is the product"
  promise — a wider surface is a wider maintenance commitment.
- **Decide by:** post-v1.

### Q10 — `det logs` and log retention
*Source: [09 — Logging & Observability](./09-logging-and-observability.md), §70*

A `det logs <run_id>` command to pretty-print a past run's `run.jsonl`, and
a `--keep-logs <n>` retention flag.

- **Lean:** both v2 — quality-of-life, neither v1-critical.
- **Stakes:** Operator ergonomics only.
- **Decide by:** v2.

---

## E. Security

### Q11 — Resolved-secret caching
*Source: [08 — Security](./08-security.md), §105*

Cache resolved secrets on disk (encrypted) to avoid a secret-manager round-trip
per run, or always fetch fresh.

- **Lean:** fresh-every-run for v1/v2 — simpler and safer; caching only matters
  at high run frequency.
- **Stakes:** Run latency vs. attack surface at high invocation rates.
- **Decide by:** revisit only if round-trip cost becomes real.

### Q12 — Subprocess isolation for connectors
*Source: [08 — Security](./08-security.md), §173*

Build opt-in subprocess isolation in v2 — run each connector in a child process
with a scrubbed environment, no `profiles.yml` access, only its own resolved
config passed in.

- **Lean:** prototype after v1; decide based on whether a real community-connector
  ecosystem emerges. It contains accidents and shrinks blast radius but breaks
  in-process simplicity and adds IPC.
- **Stakes:** The trust model for third-party connectors — the central tension
  between "connectors are just Python" and "self-hosted users run untrusted code."
- **Decide by:** v2 design.

---

## F. Roadmap & release

### Q13 — The three v1 pre-baked source connectors
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md), §18*

Which ~3 sources ship in v1. Handbook's placeholder set: a database source
(Postgres), a SaaS API (Stripe), a flat-file source (filesystem CSV/Parquet/JSONL).

- **Lean:** fix the exact three from what early users actually need — do not
  guess now.
- **Stakes:** v1 credibility — the baked connectors are the worked proof that the
  contract holds. (Interacts with Q5: a Postgres source's CDC story.)
- **Decide by:** v1 freeze.

### Q14 — UI hosting model
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md), §56*

A local `det ui` serving from `.det/` and the destination, vs. a
deployable service, vs. both.

- **Lean:** local-first — it keeps the self-hosted, no-backend promise intact.
- **Stakes:** Whether det stays a pure CLI/library tool or grows a service to
  operate. Deferred entirely past v1.
- **Decide by:** v3 / when UI work starts.

### Q15 — Connector registry: curated vs open
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md), §57*

A curated, reviewed connector registry vs. an open index — curation builds trust
but adds maintainer burden; an open index scales but pushes auditing onto users.

- **Lean:** a curated "verified" tier layered over an open index.
- **Stakes:** Community-ecosystem trust model and project maintainer load
  (interacts with Q12).
- **Decide by:** when an ecosystem story is needed — post-v1.

### Q16 — License
*Source: [10 — Roadmap & Scope](./10-roadmap-and-scope.md), §66*

Apache-2.0 vs. MIT.

- **Lean:** Apache-2.0 — its explicit patent grant is the safer choice for a tool
  meant to be embedded and have connectors freely shared.
- **Stakes:** Adoption and contribution terms.
- **Decide by:** before the public repo goes live.

---

## Priority summary

The questions that **change the design** (resolve first):

| # | Question | Why it is load-bearing |
|---|---|---|
| Q3 | Secret resolver grammar | Part of the `register.yaml` contract |
| Q4 | Child streams | Flat list vs. stream DAG — a contract-shape change |
| Q6 | `strict` vs `evolve` default | Production-safety default behavior |
| Q7 | Commit granularity | Crash-recovery guarantee |
| Q13 | The three v1 connectors | Determines the v1 proof surface |

The rest are flags, defaults, or roadmap items that can be settled as v1
implementation proceeds without reworking the architecture.
