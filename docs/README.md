# det — Design Handbook

**det** (short for "data extraction tool") is an open-source, self-hosted Python **extract-load (EL)** tool. The pitch in one line: **a CLI-first, dbt-shaped extract-load tool — pipelines as configs, connectors as folders, no UI blackbox.**

The #1 design principle is **keep it as simple as possible**. The architectural inspiration is **dbt**: a `pip install`, a project folder, `profiles.yml`, and a CLI you `run` and `test`. Connectors — sources *and* destinations alike — are folders of plain Python with a `register.yaml` and decorated functions. Nothing is hidden behind a UI or a server.

## How to read this handbook

Read it in order. Sections **00–02** set the *why* — the vision, the competitive landscape, and the high-level architecture. Sections **03–06** define the *contract* — how a connector is written, what its body looks like, and how a project is laid out. Sections **07–10** cover *operating and shipping* the tool — the CLI and library, security, observability, and the roadmap. Section **11** consolidates every decision deliberately left open for review. Each section is self-contained but cross-links the others; later sections assume the contract from 03–06. If you are evaluating det, read 00–02, 10, and 11 first. If you are writing a connector, 03–05 are the working reference. If you are running it, 07–09. If you are reviewing this plan to approve or change it, **start with 11**.

## Table of contents

| # | Section | What it covers |
|---|---|---|
| 00 | [Vision & Naming](./00-vision-and-naming.md) | Why det exists, the "dbt for EL" thesis, the simplicity mandate, and the naming. |
| 01 | [Landscape & Comparison](./01-landscape-and-comparison.md) | Where det sits vs. Airbyte, Fivetran, dlt, Meltano, and custom scripts. |
| 02 | [Architecture](./02-architecture.md) | The engine, the run loop, how connectors, projects, and destinations fit together. |
| 03 | [The Connector Contract](./03-connector-contract.md) | The connector folder, `register.yaml`, the `@stream` / `@resource` / `@destination` decorators. |
| 04 | [The Connector Body](./04-connector-body.md) | Writing extraction logic — pagination, cursors, incremental records, batching. |
| 05 | [Destinations and State](./05-destinations-and-state.md) | The destination interface, the pre-baked catalog, schema handling, write dispositions, and the `_det_state` design with the capability-tier model. |
| 06 | [Project Anatomy](./06-project-anatomy.md) | `det_project.yml`, the `sources/`, `destinations/`, and `configs/` directories, `.det/`, the destination-keyed `profiles.yml`. |
| 07 | [CLI and Library API](./07-cli-and-library-api.md) | The full `det` command surface (`run -p <config>`, `list --kind`, `new {source,destination,config}`, `state -p <config>`), exit codes, the importable Python library, orchestrator use, config precedence. |
| 08 | [Security](./08-security.md) | Credentials, `profiles.yml`, `${ENV_VAR}` interpolation, secret managers, `.gitignore` defaults, log redaction, and the third-party-connector trust model. |
| 09 | [Logging and Observability](./09-logging-and-observability.md) | Structured per-run logs, run lifecycle events, the run record and `_det_runs`, log levels and redaction. |
| 10 | [Roadmap and Scope](./10-roadmap-and-scope.md) | v1 scope, the v1→v2→v3 phased plan, open-source release considerations, and project risks. |
| 11 | [Open Questions](./11-open-questions.md) | Every decision the handbook deliberately left open, with current leans and what is at stake. Resolve before the v1 freeze. |
| 12 | [Configs](./12-configs.md) | The pipeline-config concept — one config = one pipeline (source + destination + target + params). The CLI's `-p/--conf` arg names a config. |
