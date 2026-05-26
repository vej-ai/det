# 10 — Roadmap and Scope

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.

This section draws the line around **v1** — what ships, what explicitly does not — and lays out the phased path beyond it. The governing principle is unchanged: **keep it as simple as possible.** A scope decision that adds ceremony has to earn its place.

---

## 1. v1 scope

v1 is the smallest thing that is genuinely useful in production: a dbt-style CLI and Python library that extracts from a few real sources and loads, incrementally and correctly, into a warehouse.

**v1 ships:**

- **Core engine** — connector discovery, the `@stream` / `@resource` / `@destination` contract, incremental state, write dispositions, additive schema evolution (with `strict` opt-in). ([03](./03-connector-contract.md), [04](./04-connector-body.md), [05](./05-destinations-and-state.md))
- **CLI** — `init`, `new {source,destination,config}`, `list`, `validate`, `run` (with `-p` and `--tag`), `state`, `runs`, `secrets test`. Synchronous, scriptable exit codes. ([07](./07-cli-and-library-api.md))
- **Python library** — `det.run()`, `det.run_tag()`, `RunResult`. CLI and library are the same engine. ([07](./07-cli-and-library-api.md))
- **5 baked source connectors** — `filesystem` (CSV/JSONL/Parquet from local, GCS, or S3), `rest` (paginated REST APIs — 4 pagination strategies, 4 auth modes), `postgres` (keyset pagination, no `OFFSET`), `shiphero` (GraphQL), `stripe` (resource-as-stream over the REST API).
- **2 baked destinations** — **DuckDB** (zero-config local dev, the default `dev` target) and **BigQuery** (production warehouse, Parquet-staged via GCS + LOAD jobs, MERGE upserts, smart cursor-based partitioning). ([05 §2](./05-destinations-and-state.md))
- **State in the destination** — `_det_state` table; sidecar JSON for the (single) Tier-B path. ([05 §5](./05-destinations-and-state.md))
- **Structured logging + run records** — per-run JSON-lines logs, `_det_runs` table on both baked destinations. ([09](./09-logging-and-observability.md))
- **Security baseline + 3 secret-manager resolvers** — `profiles.yml`, `${env.X}` / `${profile.X.Y}` interpolation, `secret://` URL plugin surface, GCP Secret Manager / AWS Secrets Manager / HashiCorp Vault adapters (each as an opt-in extra), log redaction, `.gitignore` defaults, an honest trust model. ([08](./08-security.md))
- **Pipeline-level parallelism** — `det run --tag <T> --threads N` runs matched configs concurrently, with per-destination caps (DuckDB clamps to 1, BigQuery defaults to 10). ([02 §Concurrency](./02-architecture.md))

**v1 explicitly does NOT include:**

- **No UI.** None. The whole "not Airbyte" thesis is no blackbox; the UI is deferred until the CLI/library are proven (§2, v3).
- **No orchestrator adapters.** det runs *under* an orchestrator via the plain library API; no `dagster-det` package yet.
- **No connector sandboxing.** Connectors run in-process as trusted code. ([08 §7](./08-security.md))
- **No CDC / log-based replication.** Cursor-based incremental only. ([Q5](./11-open-questions.md))
- **No streaming/iterator library API, no per-batch state commit, no stream-level parallelism, no merge-on-object-storage.** All deferred — see [chapter 11](./11-open-questions.md).

The test of v1: a senior data engineer can `pip install det`, `det init`, write or pick a connector, and have an incremental BigQuery pipeline running under cron or Dagster the same afternoon — with no UI and no surprises.

---

## 2. Phased roadmap

### v1 — Core engine + CLI

The foundation above. Goal: **correct incremental EL, dbt-simple, from the terminal.** Success looks like a handful of real pipelines running in production on cron, and contributors able to write a connector by reading [03](./03-connector-contract.md)/[04](./04-connector-body.md) alone.

### v2 — Breadth and integration

Once the core is proven, widen it — without changing the contract:

- **More destinations** — Snowflake, ClickHouse, Postgres, the GCS/S3 filesystem destination, generic SQLAlchemy. The capability-tier model ([05 §2](./05-destinations-and-state.md)) was designed for exactly this; adding a destination is adding a connector, not changing the engine.
- **More source connectors** — driven by community contribution and demand.
- **Orchestrator integration** — an official thin `dagster-det` helper (assets/ops wrapping `det.run()`), and documented Airflow/Prefect patterns. The library API is already orchestrator-ready ([07 §4.2](./07-cli-and-library-api.md)); this is convenience, not capability.
- **CDC** — log-based replication for the `postgres` source (and similar). ([Q5](./11-open-questions.md))
- **Schema evolution, expanded** — type-widening across more destinations, clearer migration errors.
- **Pluggable state backends** — a real Postgres/DynamoDB `StateBackend` for concurrency-safe Tier-B state. ([05 §5.4](./05-destinations-and-state.md))
- **Quality-of-life** — log retention flags (`--keep-logs N`), opt-in per-batch state commit for large backfills ([Q7b](./11-open-questions.md)), stream-level parallelism within one pipeline.

### v3 — UI and ecosystem

- **A UI** — a *reader* over the data det already writes (`_det_runs`, `_det_state`, `run.jsonl`): run history, stream health, state inspection, log drill-down. It deliberately does not become a connector-authoring blackbox — connectors stay as folders of Python. Hosting model is open — local-first vs. deployable service — see [chapter 11 Q14](./11-open-questions.md).
- **Connector registry / marketplace** — a discoverable index of community connectors, with signing and checksum-pinning so audited code is the code that runs ([08 §7](./08-security.md)). Curated vs. open is still open — see [chapter 11 Q15](./11-open-questions.md).
- **Possible managed/hosted offering** — out of scope for the open-source roadmap; noted only so the architecture does not foreclose it.

Roadmap ordering is a hypothesis, not a contract. v2's exact connector set follows real usage.

---

## 3. Open-source release considerations

- **License** — **Apache-2.0**. The explicit patent grant is the safer choice for a tool meant to be embedded and have connectors freely shared. See [LICENSE](../LICENSE).
- **Repository structure** — a single repo: the `det` engine package, the baked sources under `det/sources/` and baked destinations under `det/destinations/`, the docs in `docs/` (this handbook). One repo keeps the engine and its baked connectors versioned together and contribution friction low. Community connectors live in their own repos (and, later, the registry).
- **Contribution model for connectors** — the connector contract ([03](./03-connector-contract.md)) is the public API. A connector is a folder; contributing one is a small, reviewable PR or an independently published package. The scaffolding commands (`det new source`, `det new destination`) and `det validate` make a contribution self-validating before review.
- **Registry / marketplace** — see v3. The near-term path is a curated list in the docs; the registry is the scaled version once enough connectors exist to warrant it.
- **Governance** — start owner-led with clear `CONTRIBUTING.md` and a connector style guide; formalize only if the contributor base grows enough to need it. Do not build governance ceremony ahead of the community that needs it.

---

## 4. Risks — what could kill the project

An honest list. Each pairs a real failure mode with the mitigation already built into the design.

- **The connector long tail.** EL lives or dies on connector coverage; this is Airbyte's moat and a solo/small project cannot match it. *Mitigation:* make connectors trivially cheap to write (the entire thesis of [03](./03-connector-contract.md)/[04](./04-connector-body.md)) and lean on community contribution. det does not need 300 connectors — it needs the 10 a given team actually uses to be a one-afternoon job.
- **Trust in third-party connectors.** Arbitrary in-process Python is a real attack surface ([08 §7](./08-security.md)). A single supply-chain incident in a popular community connector could brand the tool unsafe. *Mitigation:* honesty now, least-privilege guidance, the `--allow-unsafe-connectors` gate (v2), and a signed registry (v3).
- **Scope creep into Airbyte.** Every "add a UI / a scheduler / a server" request erodes the simplicity that is the entire reason to choose det. *Mitigation:* this document. The UI is a deferred *reader*, scheduling is the orchestrator's job, and "keep it simple" is a stated veto on features.
- **dlt / Airbyte / Fivetran move into the niche.** dlt is the closest competitor and is well-funded. *Mitigation:* the differentiator is the *dbt-shaped* developer experience — projects, profiles, `run`/`test`, connectors-as-folders — aimed squarely at teams already living in dbt. That ergonomic fit, not raw connector count, is the wedge.
- **Maintainer bandwidth.** A small team cannot review every connector PR and chase every warehouse quirk. *Mitigation:* the self-validating contribution model (template + `test` harness), a tight pre-baked set the core team actually owns, and resisting the temptation to absorb every community connector into the main repo.
- **Incremental-state correctness bugs.** Silent data loss or duplication from a state bug would be fatal to trust. *Mitigation:* the conservative "commit state only after a fully successful load" rule ([05 §5.3](./05-destinations-and-state.md)), at-least-once semantics, and state correctness as the first thing the test suite proves.

If det stays small, makes connectors cheap, and never becomes a blackbox, none of these is fatal. The biggest self-inflicted risk is forgetting principle #1.

### Reference

- The connector contract that makes connectors cheap → [03 — The Connector Contract](./03-connector-contract.md)
- Destination capability tiers (how v2 destinations slot in) → [05 — Destinations and State](./05-destinations-and-state.md)
- CLI and library surface (the v1 product) → [07 — CLI and Library API](./07-cli-and-library-api.md)
- Trust model and the registry direction → [08 — Security](./08-security.md)
