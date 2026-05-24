# det

**det** ("data extraction tool") is an open-source, self-hosted Python
**extract-load (EL)** tool. It moves data from a **source** (an API, a
database, a file drop) into a **destination** (a warehouse, a database, an
object store) — and nothing more. Transformation is dbt's job. The pitch in one
line: *dlt meets dbt*, and explicitly **not Airbyte** — no UI blackbox.
Connectors are folders of plain Python; your project is a folder of plain files
in your repo, run from a CLI. The #1 principle is to keep it as simple as
possible.

> **Status:** early skeleton. The engine, connectors, and CLI commands are not
> built yet — this repository currently contains the project scaffolding and
> the [design handbook](./docs/README.md).

## Install

```sh
pip install det
```

det requires Python 3.11+. It installs both a CLI (`det`) and an
importable library (`import det`).

## Usage

```sh
det run -c meta_ads
```

(Skeleton stub for now — see the [design handbook](./docs/README.md) for the
full planned CLI and library surface.)

## Documentation

The full design handbook lives in [`docs/`](./docs/README.md). Start with
[00 — Vision & Naming](./docs/00-vision-and-naming.md),
[02 — Architecture](./docs/02-architecture.md), and
[10 — Roadmap and Scope](./docs/10-roadmap-and-scope.md).

## License

Apache License 2.0 — see [LICENSE](./LICENSE).
