# Contributing to detx

Thanks for your interest in detx. The governing principle is the
[design handbook](./docs/00-vision-and-naming.md): **keep it as simple as
possible.** This file covers dev setup, the PR process, and how to add a
new connector or secret-manager resolver.

By participating in this project you agree to abide by the
[Code of Conduct](./CODE_OF_CONDUCT.md).

## Dev setup

detx targets Python 3.11+. Clone the repo, then create a virtual environment
and install the package in editable mode with the `dev` extras:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Run the tests

```sh
pytest -q
```

## Run the linter and type checker

```sh
ruff check .
mypy detx
```

Please make sure `pytest`, `ruff check`, and `mypy` all pass before opening a
pull request.

## Spec precedence: code is the source of truth

The [design handbook](./docs/) (`docs/00`–`docs/12`) was written before the
implementation and remains the canonical *intent*. But once a contract is
implemented in code, **the code is the source of truth** — if the handbook and
the code diverge, fix the handbook to match the code, not the other way around.
This avoids re-litigating handbook ambiguities at every stage.

Worked examples already in the tree:

- `detx/types.py::StateRecord` defines the canonical `_detx_state` schema
  (docs/03 §3.5 and docs/05 §5.1 follow it).
- **Configs are the runtime unit.** The CLI's `-p / --conf` arg and
  `detx.run(config=...)` library entry point both take a *config name* —
  there is no source-alone selector. Documents `06`, `07`, and `12` follow
  this.

## PR process

1. Branch off `main`.
2. Make one logical change per PR — small PRs review faster and revert cleaner.
3. Run the local checks before pushing:
   ```sh
   pytest -q && ruff check . && mypy detx
   ```
4. Add or update tests for any new code path. Bug-fix PRs should land a
   regression test alongside the fix.
5. If you touch the public surface (a contract type, a CLI flag, a baked
   connector's `register.yaml`, the secret-resolver protocol), update the
   relevant chapter under `docs/` in the same PR.
6. Add an entry under the `## [Unreleased]` section of
   [`CHANGELOG.md`](./CHANGELOG.md). Group it under **Added** / **Changed** /
   **Fixed** / **Removed** / **Deprecated** / **Security**.
7. Write a descriptive commit message and link any related issue in the PR
   description. Reviewers may request changes — that is the point of review,
   not a verdict on your work.

### Commit message style

Conventional-commit prefixes are **recommended but not enforced**:

- `feat:` — a user-visible new capability
- `fix:` — a bug fix
- `docs:` — documentation only
- `test:` — tests only
- `refactor:` — internal restructuring with no behavior change
- `chore:` — build, tooling, dependencies

Consistency helps readers skim the log; we will not bounce a PR for
deviating.

## Adding a new source connector

A source connector is a folder under `detx/sources/<name>/` (for a baked
one) or `connectors/sources/<name>/` (in a user project) containing:

1. **`register.yaml`** — the manifest. Declares connector name, capabilities,
   config keys (with `secret: true` for any sensitive field), and the streams
   it exposes. See [docs/03 — The Connector Contract](./docs/03-connector-contract.md).
2. **`source.py`** — the connector body. Generator functions decorated with
   `@stream` or `@resource` that yield records. See
   [docs/04 — Connector Body](./docs/04-connector-body.md).
3. **Tests** under `tests/sources/<name>/` — at minimum, a smoke test that
   exercises the happy path with a recorded fixture or a mocked HTTP layer.
   Live-service tests should be gated by an env var and marked with the
   `integration` pytest marker (see `pyproject.toml`).
4. **`README.md`** in the connector folder — what it extracts, how to
   authenticate, any operational notes.

The fastest scaffold is `detx new source <name>` — it writes a skeletal
`register.yaml` plus a `source.py` you fill in.

## Adding a new destination connector

Destinations follow the same folder + `register.yaml` + decorator shape, but
they accept records instead of yielding them. The folder lives under
`detx/destinations/<name>/`, and the body uses `@destination`-hooked
functions. The destination contract — capabilities, the state-table shape,
the transactional-load expectation, schema evolution — is in
[docs/05 — Destinations and State](./docs/05-destinations-and-state.md).

Scaffold: `detx new destination <name>`.

## Adding a new secret-manager resolver

A secret-manager resolver is any object whose class declares
`scheme: ClassVar[str]` and implements `resolve(path, field) -> str`. It is
distributed either as a third-party package via the
`detx.secret_resolvers` entry-point, or as a project-local
`detx_plugins.py`. The protocol, registration mechanics, and the URL form
(`secret://<scheme>/<path>[#<field>]`) are in
[docs/08 §3 — Secret references and pluggable secret managers](./docs/08-security.md).

## Issue templates

Use the templates under
[`.github/ISSUE_TEMPLATE/`](./.github/ISSUE_TEMPLATE) to file a bug
report, feature request, or connector request. Security issues go through
the private channel described in [`SECURITY.md`](./SECURITY.md), **not**
the issue tracker.

## License and contribution sign-off

By submitting a pull request you agree that your contribution is licensed
under the [Apache License 2.0](./LICENSE), the same license as the rest of
the project.

A Developer Certificate of Origin sign-off (`git commit -s`) is
**recommended** in v0.1 and will become **required** for v1.0. Adopting it
now is the lowest-friction way to ramp up.
