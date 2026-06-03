# Plan — `streams:` block becomes the mandatory pipeline surface

Author: Claude (handed to Albinas for skim before implementation)
Date: 2026-06-03
Target diff: ~1500-2000 LOC across engine, types, CLI, docs, tests
Status: **draft, awaiting approval**

---

## 1. Goal in one paragraph

A config today is "source + destination + target + params"; streams are
**implicit** (run everything the source declares) and the only per-stream
knob is `partition_overrides:`. After this change, a config is "source +
destination + target + params + **the exact set of streams to run, with
per-stream run-shape overrides**." Streams become **mandatory** in every
config (with a `streams: all` explicit catch-all opt-in). The new block
subsumes both `select:` and `partition_overrides:` into one surface, and
adds per-stream `mode` / `since` / `params` so a config can run a single
stream as `full_refresh` without forking the source or mutating the
shared `_dtex_state`.

This is the design we settled on in chat (mandatory + `streams: all`
escape hatch + full_refresh ignores cursor without resetting it).

---

## 2. The target schema

### 2.1 Long form (the canonical shape)

```yaml
name: revenuecat_dev_bq
source: revenuecat
destination: bigquery
target: dev

params:
  project_id: proj_xxx       # source-level (still works as today)

destination_params:
  dataset: revenuecat_dev    # destination-level (still works as today)

streams:
  customers: {}                            # include with defaults
  subscriptions:
    mode: full_refresh                     # override
    since: "2026-05-01T00:00:00Z"          # override cursor floor
    params:
      page_size: 100                       # per-stream source-param override
    partition:
      field: starts_at
      type: time
      time:
        granularity: day
  transactions:
    mode: incremental                      # explicit (= register default)
```

### 2.2 Short forms

```yaml
streams:
  customers:                  # no value at all = include with defaults
  subscriptions: full_refresh # bare string at the value = mode shorthand
```

A bare string is interpreted as `{mode: <string>}`. Only `full_refresh`
and `incremental` are valid bare strings.

### 2.3 The "all streams" escape hatch

The literal `streams: all` (a string at the top of the block, not a
mapping) means "include every stream the source declares, all with
defaults." This is the *explicit* opt-in to the catch-all; it cannot be
combined with per-stream overrides.

```yaml
streams: all
```

`streams: "*"` is also accepted as a synonym (more YAML-friendly in some
editors).

### 2.4 What `streams:` is NOT

- Not a list (`streams: [a, b]`). Lists can't carry per-stream
  overrides; we'd grow into the mapping form on the first override
  request and pay the migration cost twice. Pick the long form now.
- Not optional. Empty / missing → hard error at `dtex validate`.
- Not a place to redeclare the schema or cursor field. Those are stream
  *identity* and stay in `register.yaml`. Config only carries run-shape
  overrides.

### 2.5 Removed config keys

- `select:` — superseded by `streams:` (a stream you list IS selected).
- `partition_overrides:` — superseded by the per-stream `partition:`
  field in `streams:`.

Both are **hard-removed** (not soft-deprecated). Alpha, single live
project (the playground), opt for clarity over compatibility. A config
file with either of these keys fails at parse time with a clear message
naming the new location (see §5).

---

## 3. Per-stream override semantics

| key | type | meaning | default |
|---|---|---|---|
| `mode` | `incremental` \| `full_refresh` | This run's mode for this stream | `incremental` if stream has an `incremental:` block in `register.yaml`, else `full_refresh` |
| `since` | timestamp / int / string | Override the cursor floor for this run only (ignored if `mode=full_refresh`) | none |
| `params` | mapping | Per-stream source-param overrides (e.g. `page_size`) | `{}` |
| `partition` | string \| mapping | What `partition_overrides[stream]` did before | none |

### 3.1 `mode: full_refresh` — the state question we settled

**Settled in chat (2026-06-02):** `mode: full_refresh` in a config means

- Do **not** read the `_dtex_state` cursor row for this stream this run
  (engine starts from the stream's `register.yaml` `initial_value`, or
  `since` if provided).
- Do **not** advance the cursor on successful completion.
- Do **not** reset the cursor either.

Net effect: a sibling `revenuecat_prod` config running `subscriptions`
incrementally keeps its cursor intact through any `revenuecat_dev` run.
Full-refresh becomes a per-run *behavior*, not a state mutation.

This is materially different from today's CLI `--full-refresh` which
*resets* the cursor (see §6 for what we do about the flag).

### 3.2 `mode` interaction with the source declaration

| register.yaml | config `mode` | runtime |
|---|---|---|
| has `incremental:` | (unset) | incremental |
| has `incremental:` | `incremental` | incremental |
| has `incremental:` | `full_refresh` | full refresh (per §3.1) |
| no `incremental:` | (unset) | full refresh |
| no `incremental:` | `incremental` | **hard error** — stream has no cursor field |
| no `incremental:` | `full_refresh` | full refresh |

### 3.3 `since`

- Only meaningful when `mode=incremental` (the run will *read* a cursor).
- Format: matches the stream's `incremental.cursor_type` (timestamp →
  ISO-8601 string; integer → int literal).
- **Does NOT mutate `_dtex_state`** — this is a one-shot floor for the
  run. The actual seed picked by the engine is `max(since, prior_state)`
  so the override can only move the floor *earlier* than what state
  already covers? — **No.** Settled wrong; the user's expectation is
  "use *this* value, period." So: `since` *replaces* the seed for this
  run, no max. State still advances per usual at the end on success.
  This lets an operator say "re-pull from 2026-01-01 just this once."

### 3.4 `params`

Per-stream `params:` is a new precedence layer between config-level
`params:` and CLI `--param`:

1. register.yaml `params[].default`
2. dtex_project.yml `vars`
3. config `params`
4. **config `streams[<name>].params`** ← new
5. env `DTEX_PARAM_<NAME>`
6. CLI `--param k=v` / `params_override=`

Rationale: a config might bump `page_size` for `transactions` (a heavy
stream) but leave others at the source default. Today this requires
either bumping every stream (config-level params) or forking the source.

### 3.5 `partition`

Identical shape and semantics to today's `partition_overrides[<stream>]`
— short string form OR long-form mapping. Just relocated.

---

## 4. Migration of every existing config + test

### 4.1 Live configs

| file | streams declared in source | migration |
|---|---|---|
| `~/dev/det_playground/configs/stripe_sigma_bq.yml` | charges_daily, subscriptions_active, invoices_paid | add `streams: all` (closest to today's behavior) |
| `~/dev/det_playground/configs/products_bq.yml` | (check) | likely `streams: all` |
| `~/dev/simple_e/tests/fixtures/configs/echo.yml` | (check fixture source) | mirror its current `select:` if any, else `streams: all` |
| `~/dev/simple_e/dtex/cli/_scaffold.py::_CONFIG_YML` | n/a (template) | template with mapping form + commented `streams: all` alternative |

### 4.2 Tests that touch `select:` or `partition_overrides:`

`grep -rn "select:\|partition_overrides" tests/` — survey before coding.
Each needs to be moved into the new `streams:` shape. Expect the test
diff to be the largest single chunk of the PR.

### 4.3 Every scaffold template must reflect the new schema

This is its own checklist item, not an afterthought. Three templates
collectively produce a project that needs to pass `dtex validate` on
the first try after `dtex init && dtex new source X && dtex new
config Y` — if any template is out of sync, the new-user experience
breaks immediately.

**4.3.1 `_CONFIG_YML` — `dtex new config <name>`**

```yaml
# {name} - a dtex pipeline config (docs/12).
name: {name}
source: my_source            # rename to a real source under sources/
destination: duckdb
target: dev

params: {{}}
destination_params: {{}}

# Streams are explicit and required. Either list each stream by name
# (with optional per-stream overrides) or use `streams: all` to include
# every stream the source declares.
streams:
  my_stream: {{}}              # remove me; list real streams instead
  # other_stream:
  #   mode: full_refresh       # override this stream's mode this run
  #   since: "2026-01-01T00:00:00Z"   # one-shot cursor floor
  #   params:
  #     page_size: 100         # per-stream source-param override
  #   partition:
  #     field: created
  #     type: time
  #     time: {{granularity: day}}
```

**4.3.2 `_EXAMPLE_CONFIG_YML` — the example config `dtex init` seeds**

The seeded `configs/example.yml` must validate against the seeded
source. Today it binds `source: my_source` which doesn't exist until
`dtex new source my_source` runs. With mandatory `streams:`, the
example config can't be parseable unless either (a) the example
references a stream the example source actually declares, or (b) the
example uses `streams: all` and we trust the example source declares
something.

**Recommend (b)** — `streams: all` in the example is the lowest-friction
intro for a first-time user; it just works after `dtex new source X`
edits both sides. The example config gets a TODO comment pointing at
the schema:

```yaml
name: example
source: my_source            # rename to a source under sources/
destination: duckdb
target: dev
params: {{}}
destination_params: {{}}
streams: all                 # run every stream the source declares
# TODO: list streams explicitly for tighter control:
# streams:
#   my_stream: {{}}
```

**4.3.3 `_SOURCE_REGISTER_YML` — `dtex new source <name>`**

Already declares one example stream (`items`). No change required for
the source side, BUT verify: the example source stream name must
match the seeded example config when both come from defaults. Today
the source scaffold uses `items`; the config scaffold uses
`my_stream`. After this change, the example config uses `streams: all`
so the name mismatch no longer breaks parsing. **Document this
deliberate decoupling in the source template comment**: "the example
config references your streams via `streams: all`; rename or replace
streams freely."

**4.3.4 Acceptance test**

A single end-to-end test that runs the scaffold chain and asserts
`dtex validate` exits 0:

```python
def test_scaffold_chain_validates_clean(runner, tmp_path):
    project = tmp_path / "p"
    runner.invoke(cli, ["init", str(project)])
    runner.invoke(cli, ["new", "source", "my_source",
                         "--project-dir", str(project)])
    result = runner.invoke(cli, ["validate", "--project-dir", str(project)])
    assert result.exit_code == 0, result.output
```

If this test fails after a template edit, the template is broken.

---

## 5. Errors that need to be good

Each of these gets a unit test asserting the exact substring.

| trigger | error |
|---|---|
| missing `streams:` | `config 'X': 'streams' is required (use 'streams: all' to include every stream the source declares, or list streams by name with optional per-stream overrides)` |
| `streams: []` or `streams: {}` | same as above + `'streams' must not be empty` |
| `streams: all` plus per-stream entries | `config 'X': 'streams: all' cannot be combined with per-stream entries; use one or the other` |
| `streams.<unknown_stream>` | `config 'X': streams names stream(s) that <source> does not declare: <names>; valid streams: <list>` (mirrors today's `_validate_partition_overrides_stream_names`) |
| `streams.<name>.mode: incremental` on non-incremental stream | `config 'X': stream '<name>' has no incremental cursor in <source>/register.yaml; cannot set mode=incremental` |
| `streams.<name>.mode: invalid_value` | `config 'X': stream '<name>': unknown mode 'X'; valid: incremental, full_refresh` |
| legacy `select:` key | `config 'X': 'select' is no longer supported; list streams under 'streams:' instead` |
| legacy `partition_overrides:` key | `config 'X': 'partition_overrides' is no longer supported; move partition specs under streams.<name>.partition` |

---

## 6. CLI surfaces — what changes, what stays

### 6.1 `dtex run --select` — KEEP

Continues to work as a per-invocation **narrowing** of what's already in
the config's `streams:` block. If the config doesn't list a stream,
`--select <name>` errors (you can't materialize a stream that's not in
the pipeline blueprint).

### 6.2 `dtex run --full-refresh` — KEEP, semantics tightened

Per-run, applies to every stream the run touches. Today it **resets**
the cursor; from now on it follows the new §3.1 rule: don't read, don't
advance, don't reset. (Reset is a separate operation: `dtex state
reset` exists for that already.)

This is technically a behavior change to an existing flag — list it in
CHANGELOG `### Changed` with the new semantics + the rationale (sibling
configs).

### 6.3 New: `--stream-mode <name>=<mode>` — DEFER

A per-invocation per-stream override is *thinkable* but adds a flag
surface for a need no one has voiced yet. Mention in the plan and
explicitly punt to a follow-up. The config-level surface covers the
real ergonomic gap; CLI overrides for one-off operator runs can land
later.

### 6.4 `_dtex_runs.full_refresh` column

Today it's a `bool`. After this change, a run can be partially
full-refresh (some streams refresh, others incremental). Two options:

1. Keep the bool, meaning "every stream in the run was full-refresh"
   (true if `--full-refresh` was passed OR every config stream's
   resolved mode was full_refresh).
2. Change column to JSON `streams_mode: {customers: incremental,
   subscriptions: full_refresh}` for full fidelity.

**Recommend (1)** for now — schema change for run records is a separate
maintenance burden and the new `streams:` block in the config is already
the source of truth for who-ran-how. The audit column becomes a
denormalized hint, which it kind of already is.

---

## 7. Engine internals — touchpoint list

Concrete files + line ranges I expect to touch. Sized by lines because
that's how I'll plan the implementation order.

| file | change | est. LOC |
|---|---|---|
| `dtex/types.py` | `PipelineConfig.streams: Mapping[str, StreamRunConfig]`; new `StreamRunConfig` dataclass; remove `select`/`partition_overrides` fields; rewrite `from_dict` | ~250 |
| `dtex/engine/config.py` | param resolution gets a `stream_name` arg; merges layer 4 from `streams[<name>].params` | ~80 |
| `dtex/engine/runner.py` | `_pick_partition()` reads from `streams[<name>].partition` not `partition_overrides`; cursor seeding honors per-stream `mode` (§3.1); `selects()` reads from `streams:` not `select` | ~150 |
| `dtex/engine/configs.py` | unchanged structurally; gets the new error surfaces routed through | ~20 |
| `dtex/cli/__init__.py` | `--select` still narrows (now against `streams:` keys); `--full-refresh` semantics docstring update | ~40 |
| `dtex/cli/_scaffold.py` | new config template (see §4.3) | ~30 |
| `docs/12-configs.md` | full rewrite — the file is the source of truth for the new schema | ~250 |
| `docs/03-connector-contract.md` | a paragraph noting that per-stream config overrides flow through layer 4 of param precedence | ~20 |
| `tests/test_configs.py` | new — full parser coverage of every §5 error and every §3 semantic | ~400 |
| `tests/test_engine.py` | migrate all existing select/partition tests | ~200 |
| `tests/fixtures/configs/echo.yml` | migrate | ~5 |
| `~/dev/det_playground/configs/*.yml` | migrate (playground, not the repo) | ~20 |
| `~/dev/det_playground/workshop_revenuecat.md` | streams: now appears in Stage 7 (config wiring) | ~30 |
| `CHANGELOG.md` | `### Added` (streams block, per-stream overrides, skills + install command) + `### Changed` (full-refresh semantics, select narrowing surface) + `### Removed` (`select:`, `partition_overrides:`) | ~50 |
| `dtex/skills/dtex-write-config.md` | new — config-authoring skill (§11.1) | ~120 |
| `dtex/skills/dtex-write-connector.md` | new — connector-authoring skill (§11.1) | ~150 |
| `dtex/skills/dtex-debug.md` | new — debugging skill (§11.1) | ~100 |
| `dtex/cli/_skills.py` | new — `dtex skills install` command logic, importlib.resources lookup, copy-with-force-flag | ~80 |
| `dtex/cli/__init__.py` | wire the `skills` subcommand group | ~30 |
| `dtex/engine/discovery.py` (or new `_first_run.py`) | first-run hint: walk up for `dtex_project.yml`, check `.claude/skills/dtex/`, write `.dtex/skills-prompted` marker | ~40 |
| `tests/test_skills_install.py` | new — install copies all bundled skills, refuses overwrite without --force, first-run hint fires once | ~120 |
| `docs/_internal/release.md` | add "update skills/*.md if schema changed" to the runbook | ~10 |

**Total: ~2200 LOC** (was ~1500 before skills baked in).

---

## 8. Implementation order (one PR, sequenced commits)

I'll land this as **one PR** with commits sequenced for review and
bisectability. Order changed to bake skills in (was 5 commits, now 6):

1. **Commit A — types + parser.** New `StreamRunConfig`, rewrite
   `PipelineConfig.from_dict`, all §5 errors, full parser test
   coverage. Runner is unchanged at this point but won't link cleanly
   because it still reads `select`/`partition_overrides` — so this
   commit is **broken at runtime**, only the parser tests pass.
   (Intentional: keep the parser change atomic.)
2. **Commit B — engine + runner.** Wire `streams:` through the runner,
   cursor seeding honors §3.1, param-precedence gains layer 4, all
   engine tests migrated. After this commit, full test suite passes.
3. **Commit C — CLI + scaffolds.** All three templates per §4.3, the
   end-to-end scaffold-chain test, `--select` docstring update,
   `--full-refresh` docstring update.
4. **Commit D — skills bundle + install command.** Three skill files
   under `dtex/skills/`, `dtex skills install` command, first-run
   hint, full test coverage per §11.
5. **Commit E — docs + CHANGELOG.** Full rewrite of `docs/12-configs.md`,
   `CHANGELOG.md` entries (streams + skills together), workshop guide
   migrated to declare streams explicitly + a Stage 0.5 line about
   `dtex skills install`.
6. **Commit F (separate, not in dtex repo) — playground migration.**
   `~/dev/det_playground/configs/*.yml` migrated. Confirm `dtex
   validate` clean. Run the Sigma pipeline once to smoke-test the new
   per-stream code paths in anger.

Run the full test suite between every commit. Don't push or tag until
the user signs off the diff.

---

## 9. What I'm explicitly NOT doing in this change

- **No new CLI flags.** Per-stream mode overrides at the CLI level are
  deferred (§6.3).
- **No `state_isolation: pipeline` field.** The per-config state
  isolation alternative we considered in chat would be a separate
  schema change to `_dtex_state` and a separate diff. Stays in the
  follow-ups list.
- **No changes to `_dtex_runs.full_refresh` column.** Keep the bool,
  see §6.4 reasoning.
- **No deprecation period for `select:`/`partition_overrides:`.** Alpha,
  one live project, clarity > compat. Hard-remove with a clear error.
- **No support for streams from non-baked sources.** The validation
  in §5 ("streams names stream(s) that <source> does not declare")
  uses the discovered source's stream list, which already works for
  baked + project-local. No new discovery surface needed.

---

## 10. Open questions (none that block — but flag before commit B)

- **Should `streams: all` cache the source's stream list at config
  parse time, or expand at runtime?** Recommend runtime expansion: a
  source that gains a stream later automatically picks it up under the
  `all` opt-in. This matches today's empty-`select:` behavior and is
  what an opt-in catch-all *should* do.
- **Should `mode: full_refresh` on an `incremental` stream still
  *write* a row to `_dtex_state` if none existed before?** Recommend
  no — a config that's never run incremental shouldn't leave a phantom
  cursor row. If you later run it incremental, you start from
  `initial_value` like normal.
- **`streams: all` inside `configs:` list shape (multi-config files):**
  works trivially — each entry has its own `streams:` field. No
  special handling.

---

## 11. Ingrained Claude skills — bundled in this diff, not deferred

Per user direction (2026-06-03): the skills land **as part of this
diff**, not as a separate Step 3, so the schema and the skills that
teach it ship together. Without that, anyone installing dtex+skills
between the two PRs would get skills that teach a schema the installed
tool no longer accepts.

### 11.1 What the skills cover

Three skill files, each one targeted at a real friction point we've
observed in this project:

- `dtex-write-config.md` — the rules for authoring a config file.
  Teaches: streams are mandatory; one config = one connection test
  (possibly multiple streams); params belong in `config.params` or
  `config.streams[<name>].params`, **never** in `--param.x=` CLI
  invocations; per-test pattern is "one config file per pipeline,
  not one per stream."
- `dtex-write-connector.md` — the rules for authoring a source. The
  `register.yaml` schema, the `@stream` decorator surface,
  `cursor.observe(...)` discipline, the flatten-nested-shapes rule,
  the relative-imports + `__init__.py` rule.
- `dtex-debug.md` — the playbook when something fails. Read `dtex
  runs` first; what `ArrowInvalid` means now (post-stage-12); what to
  do when state doesn't advance; the `streams.<name>.mode:
  full_refresh` escape hatch for "re-pull just this one stream"
  scenarios.

Each skill ships as a `.md` file with the standard SKILL.md frontmatter
(name, description, model, allowed-tools).

### 11.2 Bundling — inside the wheel

The skill files live at `dtex/skills/*.md` in the repo. They get
included in the wheel via `pyproject.toml`'s `[tool.hatch.build]`
include glob (already includes `dtex/**`). After `pip install dtex`,
the files are on disk at
`<site-packages>/dtex/skills/dtex-write-config.md` etc.

No new dependency. No setuptools entry-point gymnastics. The files
just travel with the package.

### 11.3 Two ways skills become active in a user's project

**Way 1 — explicit:** `dtex skills install [DIRECTORY]` copies the
bundled skills into `<DIRECTORY>/.claude/skills/dtex/`. Default
directory is the project root. The command:

- locates the bundled skills via `importlib.resources` (works in
  wheels, sdists, editable installs, and zip imports);
- creates `.claude/skills/dtex/` if missing;
- copies each skill file, refusing to overwrite unless `--force` is
  passed (same convention as `dtex init`);
- echoes the list of skills installed and the directory.

**Way 2 — first-run prompt:** the first time any `dtex` command runs
inside a dtex project (detected by walking up for `dtex_project.yml`)
and there's no `.claude/skills/dtex/` directory present, print a
one-line hint:

    dtex: install Claude skills for this project? run `dtex skills install`

Not interactive. Just a hint. A `.dtex/skills-prompted` marker prevents
the hint from repeating after it's been shown once per project.

(We can NOT auto-install during pip install. pip intentionally does
not run post-install scripts. The first-run hint is the closest legal
substitute.)

### 11.4 Why this lands in this diff

- The skills *are* the schema teaching surface. Shipping schema
  without skills means the next time a non-author Claude sees the new
  config shape it'll guess at it.
- The `dtex skills install` command is tiny (~80 LOC + tests). It
  doesn't bloat the diff.
- The skill files themselves are the documentation we'd be writing
  for `docs/12-configs.md` anyway, restructured for an agent reader.
  They're not duplicate work.

### 11.5 Maintenance contract

Whenever the config schema, the connector contract, or the destination
contract changes, the relevant skill file must be updated **in the
same diff** (same rule as CHANGELOG.md). Add a one-line note to
`docs/_internal/release.md` codifying this so the release runbook
catches it.

---

## 12. After this lands

- **Per-invocation `--stream-mode <name>=<mode>` CLI flag.** If
  operators ask for it.
- **`state_isolation: pipeline` config field.** Per-config `_dtex_state`
  isolation, the alternative we considered in chat for the
  full-refresh semantics question. Separate schema change.
- **Skills become user-customizable.** `dtex skills install` could
  grow `--without dtex-debug` etc. when there's reason to.
