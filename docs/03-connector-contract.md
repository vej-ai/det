# 03 — The Connector Contract

This is the most important chapter in the handbook. It defines the single,
mandatory contract every dtex connector obeys. If you read only one section,
read this one.

## Design principle

> **YAML declares. Python implements.**

A connector is a **folder**. Inside it, `register.yaml` is the *declaration*: a
static manifest the engine scans to discover what the connector is, what streams
it produces, where data lands, and what each stream's table/keys/schema look
like. The Python files are the *implementation*: the actual extract/load logic,
wrapped in decorators.

This split mirrors dbt: `dbt_project.yml` and schema YAML declare structure;
the `.sql` files carry logic. dtex does the same with `register.yaml` and
`source.py`. We do **not** push request payloads, pagination paths, or query
strings into YAML — that road leads to the config-driven YAML blackbox, which
is explicitly what dtex is not.

The litmus test for every `register.yaml` key in this chapter:

> *Could the engine discover this without running Python?* If yes, it belongs in
> YAML. If it is logic, it belongs in a decorated function.

## 1. Connector folder anatomy

A connector is a directory. The engine discovers it by finding a `register.yaml`
file at its root. Everything else is convention.

```
meta_ads/
├── register.yaml        # MANDATORY — the discovery manifest
├── __init__.py          # marker — makes the folder an explicit Python package
├── source.py            # the connector body: @stream functions live here
├── streams.py           # (optional) additional streams, split for readability
├── schema.py            # (optional) shared schema definitions / helpers
├── client.py            # (optional) plain helper module — API client, no decorators
├── requirements.txt     # (optional) extra pip deps this connector needs
└── README.md            # (optional) human docs
```

| File | Required | Purpose |
|---|---|---|
| `register.yaml` | **Yes** | The manifest. The *only* file the engine needs to discover and validate the connector before importing any Python. |
| `*.py` (≥ 1) | **Yes** | The connector body. At least one file must define the decorated functions named in `register.yaml`. File names are free; `source.py` / `destination.py` are convention. |
| `__init__.py` | No | Empty marker. Makes the folder an *explicit* Python package, which `dtex new source` / `dtex new destination` scaffolds today. A folder without one still works (the engine treats it as a [PEP 420 namespace package](https://peps.python.org/pep-0420/)), so older project trees are not broken. |
| `requirements.txt` | No | Extra dependencies. Installed into the connector's environment at build time. Pre-baked connectors keep this minimal; the core engine's deps are always available. |
| `schema.py`, `client.py`, any helper | No | Ordinary Python modules. Imported by the body files. No decorators, no contract — just code. |
| `README.md` | No | Documentation. Ignored by the engine. |

The folder name is the connector's **directory id** but not its authoritative
name — the `name:` key in `register.yaml` is. They should match by convention.

> **The folder IS a Python package.** The engine loads each connector folder
> under a process-unique synthetic package name, so the body files are real
> submodules of one package. Splitting helpers into sibling files
> (`client.py`, `helpers.py`, `schema.py`) and pulling them in with
> `from .client import X` from `source.py` / `destination.py` is **idiomatic
> and fully supported**. The same pattern works in baked connectors (which
> are installed as real packages — e.g. `dtex.sources.stripe`) and in
> project-local connectors (which the engine wraps in a synthetic per-load
> package).

The internal layout of the body (`source.py` vs `streams.py` vs `client.py`) is
covered in detail in chapter **04 — Connector Body**. This chapter covers the
*contract*: the manifest and the decorators.

## 2. The `register.yaml` schema

`register.yaml` is parsed at discovery time, before any connector Python is
imported. It is pure data — no expressions, no logic.

### 2.1 Top-level keys

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `name` | string | **Yes** | — | Unique connector identifier. Lowercase, `snake_case`. Used as `connector="<name>"`. |
| `kind` | enum: `source` \| `destination` | **Yes** | — | Whether this connector reads from the outside world or writes to it. Sources and destinations share this manifest format. |
| `version` | string (semver) | No | `"0.1.0"` | Connector version. Bumped by the author; surfaced in logs and state. |
| `summary` | string | No | `""` | One-line human description. |
| `tags` | list[string] | No | `[]` | Free-form labels for catalog/search (e.g. `["ecommerce", "graphql"]`). Not interpreted by the engine. |
| `streams` | list[Stream] | Yes for `kind: source` | — | The tables this source produces. See §2.2. A destination omits this. |
| `params` | map[string → ParamSpec] | No | `{}` | Declared, typed configuration knobs for the connector. See §2.4. |
| `secrets` | list[SecretRef] | No | `[]` | Declared secret references the connector needs. See §2.5. |
| `schedule` | string (cron or alias) | No | `null` | A *hint* for how often this connector should run. See §2.6. |
| `requires` | list[string] | No | `[]` | pip requirement specifiers, mirrored from `requirements.txt` if present. The engine validates these are installable. |

That is the complete top-level key set. Ten keys — the source-to-destination
binding lives in a pipeline config (chapter 12), not in a source's
`register.yaml`. If a future feature needs an eleventh, it must justify the
ceremony.

> # NOTE: `register.yaml` may still carry an old-style `destination:` block
> from a legacy project; the parser preserves the field (a parsed
> `DestinationBinding` is still returned) but the engine logs a warning and
> ignores it at run time. The field is expected to be removed in a future
> release. The runtime unit is a config (chapter 12).

### 2.2 The `streams` list

Each entry in `streams` declares one output table.

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `name` | string | **Yes** | — | Stream identifier. Matches the `name=` argument of a `@stream` function in the body. |
| `table` | string | No | `name` | Destination table name. Lets stream id and physical table differ. |
| `primary_key` | string \| list[string] | Recommended | `null` | Unique key(s). Required when `write_disposition: merge`. |
| `write_disposition` | enum: `append` \| `merge` \| `replace` | No | `append` | How records land. `merge` = upsert on `primary_key`. `replace` = truncate then load. `append` = insert only. |
| `incremental` | Incremental block | No | `null` | Cursor-based incremental config. Absence ⇒ full table every run. See below. |
| `schema` | list[Field] | No | `null` (infer) | Explicit column definitions. See §2.2.1. |
| `partition_by` | string \| mapping | No | `null` | Physical-partition declaration. Short form: a bare column name (`partition_by: created_date`) — defaults to `TIME` partitioning at `DAY` granularity. Long form: a mapping with `field` / `type` (`time` / `range` / `ingestion`) / optional `granularity` / optional `range`. See [05 §3.3](./05-destinations-and-state.md#33-partitioning). A per-pipeline `partition_overrides:` block in a config can override this on a per-stream basis. Honored by BigQuery; ignored by destinations that lack native partitioning (DuckDB). |
| `params` | map[string → ParamSpec] | No | `{}` | Stream-scoped params, merged over connector-level `params`. |

#### The `incremental` block

One named block carries the cursor *contract*; *how* the connector turns a
cursor into an API request is Python logic in the body.

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `cursor_field` | string | **Yes** | — | Record field whose max value is the incremental cursor (e.g. `created_date`). |
| `cursor_type` | enum: `timestamp` \| `date` \| `int` \| `string` | No | `timestamp` | How the cursor value is compared and stored. |
| `lookback` | duration string | No | `null` | Re-fetch window to catch late-arriving rows (e.g. `2d`, `6h`). |
| `initial_value` | string | No | `null` | Where to start on the first run (e.g. `"2025-01-01"`). |

#### 2.2.1 The `schema` field list

Each entry has the shape `{name, type, mode}`.

| Key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `name` | string | **Yes** | — | Column name. |
| `type` | enum: `STRING` \| `INTEGER` \| `FLOAT` \| `BOOLEAN` \| `TIMESTAMP` \| `DATE` \| `JSON` | **Yes** | — | Logical type. The destination maps it to a physical type. |
| `mode` | enum: `NULLABLE` \| `REQUIRED` \| `REPEATED` | No | `NULLABLE` | Column nullability/repetition. `REPEATED` is for destinations with a native array type (e.g. BigQuery `ARRAY`/`RECORD`); on destinations that lack one it degrades to a `JSON` column. |
| `description` | string | No | `""` | Column documentation. |

**Schema philosophy.** Declaring `schema` explicitly is the **recommended
default** — it makes loads production-safe and lets the destination
create/evolve tables deterministically. Omitting `schema` opts into
`infer_schema` behaviour: the destination derives columns from the first
batch of records. Inference is a convenience for prototyping, not a free
lunch — it cannot see nullable columns absent from the sample, and type
drift between runs becomes a runtime error. Handbook guidance: prototype
with inference, ship with an explicit schema.

The engine always appends one column the connector author never declares:

- `_dtex_synced_at` (`TIMESTAMP`) — load timestamp, set by the engine.

### 2.3 The source-to-destination binding lives in a config

A source's `register.yaml` does **not** declare which destination its data
goes to. The binding lives in a **pipeline config** under `configs/` (chapter
12). A config file names the source + destination + target + params as one
pipeline; the CLI runs configs: `dtex run -p <config_name>`.

```yaml
# configs/shiphero_prod.yml
name: shiphero_prod
source: shiphero
destination: bigquery
target: prod
destination_params:
  dataset: shiphero
```

The destination-side `Destination` interface (chapter 05) is unchanged.

> A `DestinationBinding` dataclass remains in `dtex.types` for
> backwards-parsing compatibility — an older `register.yaml` carrying a
> `destination:` block still loads without error and the engine emits a
> warning + ignores the field at run time.

### 2.4 `params` — declared configuration

`params` declares typed knobs the connector exposes — things like `page_size`,
`step_days`, `batch_size`, `max_retries`. These are *strategy knobs the
connector chooses*, so the connector declares them with defaults rather than
the engine inventing them.

```yaml
params:
  page_size:
    type: int
    default: 50
    description: Records requested per API page.
  lookback_days:
    type: int
    default: 2
  start_date:
    type: string
    required: true
```

| ParamSpec key | Type | Required | Default | Purpose |
|---|---|---|---|---|
| `type` | enum: `string` \| `int` \| `float` \| `bool` | **Yes** | — | Value type, validated at run time. |
| `default` | matches `type` | No | `null` | Value used when caller supplies nothing. |
| `required` | bool | No | `false` | If `true` and no value/default, discovery-time validation fails. |
| `description` | string | No | `""` | Human docs. |

Resolved param values are injected into the connector body via the `config`
object (§3). Stream-level `params` override connector-level ones for that stream.

### 2.5 `secrets` — declared secret references

Secrets are **referenced**, never inlined. `register.yaml` lists the *logical
names* of secrets the connector needs; their *values* resolve at run time from
the environment or the active profile.

```yaml
secrets:
  - name: api_token
    ref: ${env.SHIPHERO_API_TOKEN}
  - name: refresh_token
    ref: ${profile.shiphero.refresh_token}
```

| SecretRef key | Type | Required | Purpose |
|---|---|---|---|
| `name` | string | **Yes** | Logical name. The body reads it as `config.secrets["api_token"]`. |
| `ref` | string | **Yes** | Resolution expression. `${env.X}` reads env var `X`; `${profile.X.Y}` reads key `Y` of profile block `X` (chapter 06); `secret://<scheme>/<path>[#<field>]` dispatches to a pluggable resolver (chapter 08 §3). |

The engine resolves refs lazily and never logs secret values.

The `secret://` form is the **third** resolver — the two `${...}` forms stay
built-in and universal; the `secret://` URL is the plugin surface for cloud
secret managers (GCP, AWS, Vault). See [08 §3](./08-security.md) for the
protocol and the registration pattern (entry-points or a project-local
`dtex_plugins.py`).

### 2.6 `schedule` — a hint, not a scheduler

```yaml
schedule: "0 */6 * * *"   # cron, or an alias: hourly | daily | weekly
```

`schedule` is advisory. dtex does not run a daemon. The value is surfaced to
whatever orchestrator invokes dtex (cron, Airflow, Cloud Scheduler, a CI job)
so the schedule lives next to the connector instead of in a separate system. The
engine reads it for `dtex list --schedules` but never acts on it.

### 2.7 Worked example A — a simple REST source

A single-stream connector against a paginated REST API.

```yaml
# connectors/exchange_rates/register.yaml
name: exchange_rates
kind: source
version: "1.0.0"
summary: Daily FX rates from the OpenRates REST API.
tags: [finance, rest]

params:
  base_currency:
    type: string
    default: "USD"

secrets:
  - name: api_key
    ref: ${env.OPENRATES_API_KEY}

streams:
  - name: rates
    table: fx_rates
    primary_key: [date, currency]
    write_disposition: merge
    incremental:
      cursor_field: date
      cursor_type: date
      initial_value: "2024-01-01"
    schema:
      - {name: date,      type: DATE,    mode: REQUIRED}
      - {name: currency,  type: STRING,  mode: REQUIRED}
      - {name: rate,      type: FLOAT}
      - {name: base,      type: STRING}

schedule: daily
```

The matching config (chapter 12):

```yaml
# configs/exchange_rates_prod.yml
name: exchange_rates_prod
source: exchange_rates
destination: bigquery
target: prod
destination_params:
  dataset: finance
```

### 2.8 Worked example B — a multi-stream GraphQL source (ShipHero)

The baked ShipHero connector, re-expressed in the contract. Note what *moved*:
the GraphQL `query` string, `field_path`, and `date_from_field` /
`date_to_field` are **not** in YAML — they are request-construction logic and
live in `source.py`. The YAML carries only the discovery contract.

```yaml
# connectors/shiphero/register.yaml
name: shiphero
kind: source
version: "2.0.0"
summary: ShipHero fulfillment data via the public GraphQL API.
tags: [ecommerce, fulfillment, graphql]

params:
  start_date:    {type: string, default: "2025-01-01"}
  lookback_days: {type: int,    default: 2}
  step_days:     {type: int,    default: 10}
  page_size:     {type: int,    default: 50}
  batch_size:    {type: int,    default: 200}
  max_retries:   {type: int,    default: 5}

secrets:
  - name: refresh_token
    ref: ${profile.shiphero.refresh_token}

streams:
  - name: shipments
    table: shipments
    primary_key: id
    write_disposition: merge
    partition_by: created_date
    incremental:
      cursor_field: created_date
      cursor_type: timestamp
      lookback: 2d
      initial_value: "2025-01-01"
    schema:
      - {name: id,                  type: STRING,    mode: REQUIRED}
      - {name: legacy_id,           type: INTEGER}
      - {name: order_id,            type: STRING}
      - {name: user_id,             type: STRING}
      - {name: warehouse_id,        type: STRING}
      - {name: pending_shipment_id, type: STRING}
      - {name: shipped_off_shiphero,type: BOOLEAN}
      - {name: dropshipment,        type: BOOLEAN}
      - {name: created_date,        type: TIMESTAMP}
      - {name: shipping_labels,     type: JSON}
      - {name: line_items,          type: JSON}

  - name: orders
    table: orders
    primary_key: id
    write_disposition: merge
    incremental:
      cursor_field: order_date
      cursor_type: timestamp
      lookback: 2d
      initial_value: "2025-01-01"
    # schema omitted -> inferred from first batch (prototype mode)

schedule: "0 */6 * * *"
```

The matching config:

```yaml
# configs/shiphero_prod.yml
name: shiphero_prod
source: shiphero
destination: bigquery
target: prod
destination_params:
  dataset: shiphero
```

The `shipping_labels` / `line_items` nested objects are declared as `JSON`
columns. *(See chapter 04 for the flatten-vs-JSON-column discussion.)*

The baked ShipHero connector splits its body across `source.py` (`@stream`
functions), `client.py` (auth + HTTP calls), and `schema.py` (record shaping)
and uses `from .client import refresh_access_token` / `from .schema import
extract_records` between them — it ships installed as a real package
(`dtex.sources.shiphero`). A *project-local* connector gets the same
capability via the engine's load-as-package mechanism (§1): the same
`from .client import …` pattern works in `<project>/sources/<name>/source.py`.

## 3. The decorator API

The connector body is plain Python decorated with dtex decorators. There is
**one default authoring style** and one escape hatch (§4).

### 3.1 `@stream` — the default for sources

`@stream` marks a generator function that produces records for one stream. It is
the only decorator a normal source author needs.

```python
# connectors/shiphero/source.py
from dtex import stream

@stream(name="shipments")
def shipments(config, state, cursor, log):
    """Yield batches of shipment records."""
    ...
```

The decorator argument:

| Arg | Type | Required | Purpose |
|---|---|---|---|
| `name` | string | **Yes** | Must match a `streams[].name` in `register.yaml`. This is the link between manifest and code. |

#### Injected arguments

The engine inspects the function signature and injects, **by name**, only the
objects the function asks for. A function that needs nothing extra can be
`def rates(config, cursor): ...`.

| Param name | Type | What it is |
|---|---|---|
| `config` | `Config` | Resolved params + secrets. `config.page_size`, `config.secrets["api_token"]`. Read-only. |
| `state` | `State` | Per-stream persisted state (chapter 04). Free-form key/value scratch space that survives between runs. |
| `cursor` | `Cursor` | Incremental cursor helper, present only if the stream declares an `incremental` block. See §3.2. |
| `log` | `Logger` | Structured logger. `log.info(...)`, `log.warning(...)`. Correlation ids attached by the engine. |

#### What a `@stream` function yields

A `@stream` generator **yields lists of dicts** — *batches*, not single records.
Each dict is one record; keys are column names. Batching is the connector's
call (ShipHero uses `batch_size`); the engine loads each yielded batch
independently and can checkpoint between them.

```python
@stream(name="shipments")
def shipments(config, state, cursor, log):
    token = refresh_token(config.secrets["refresh_token"])
    start = cursor.start_value()          # last cursor value, minus lookback
    batch = []

    for window in date_windows(start, step_days=config.step_days):
        for page in paginate(GRAPHQL_QUERY, window, token, config.page_size):
            for node in extract(page, FIELD_PATH):
                batch.append(node)
                cursor.observe(node["created_date"])   # advance cursor
                if len(batch) >= config.batch_size:
                    yield batch
                    batch = []
    if batch:
        yield batch
```

`GRAPHQL_QUERY`, `FIELD_PATH`, `date_windows`, `paginate`, `extract` are plain
Python in the connector body — exactly the logic that was *not* lifted into YAML.

### 3.2 The `Cursor` helper

`cursor` makes incremental loading a three-line affair instead of the manual
checkpoint bookkeeping in the ShipHero proof.

| Method | Returns | Purpose |
|---|---|---|
| `cursor.start_value()` | cursor type | Where to resume: last persisted cursor value, minus `lookback`. On first run, `incremental.initial_value`. |
| `cursor.observe(value)` | `None` | Report a record's cursor field value. The engine tracks the max. |
| `cursor.is_full_refresh` | `bool` | `True` when the run was invoked with `--full-refresh`. |

After the generator is exhausted *and* its batches are durably loaded, the
engine persists the observed max to the state table (§3.5). The connector never
writes cursor state itself — that is the engine's job, which is what makes
"resume after crash" correct by construction.

### 3.3 `@resource` — alias

`@resource` is a registered **alias** of `@stream`, provided so authors arriving
from dlt feel at home. It is identical in every respect. The handbook, examples,
and `dtex new` scaffolding all use `@stream`. `@resource` is mentioned once,
here, and never again — there is one authoring style, not two.

### 3.4 The `@destination` hooks — the destination contract

The symmetry between sources and destinations is **folder + `register.yaml` +
decorators** — *not* an identical decorator. A source has one job: *yield*. A
destination has several — open a connection, manage tables, persist batches,
hold state, close down — so it implements a small **namespace of `@destination`
hooks** rather than a single function. Same folder contract, same manifest, more
than one entry point because the job genuinely is more than one thing.

```python
# connectors/bigquery/destination.py
from dtex import destination, Capability, Schema, Batch, StateRecord

@destination.capabilities
def capabilities() -> set[Capability]:
    """Declare what this destination can do — drives engine behavior."""
    return {Capability.STATE, Capability.MERGE, Capability.SCHEMA_EVOLUTION}

@destination.open
def open(config):                       # acquire a connection — once per run
    ...

@destination.ensure_schema
def ensure_schema(conn, stream):         # create/ALTER the table
    ...

@destination.write_batch
def write_batch(conn, batch, stream) -> int:
    """Persist one batch produced by a source @stream. Return rows written."""
    ...

@destination.commit_state
def commit_state(conn, run_id, records): # persist cursors — after batches land
    ...

@destination.read_state
def read_state(conn, connector):         # load prior cursors at run start
    ...

@destination.close
def close(conn):                         # flush + release — always runs
    ...
```

| Hook | Mandatory | What it does |
|---|---|---|
| `@destination.capabilities` | **Yes** | Returns the `Capability` set the destination supports (`STATE`, `MERGE`, `SCHEMA_EVOLUTION`, …). Fixes the destination's capability tier at init time. |
| `@destination.open` | **Yes** | Acquires a connection/handle from `config`. Called once per run. |
| `@destination.ensure_schema` | **Yes** | Creates the target table if absent; performs additive `ALTER` for schema evolution. Receives a `StreamMeta`. |
| `@destination.write_batch` | **Yes** | Persists one batch (a `list[dict]` yielded by a source `@stream`) per the stream's `write_disposition`. Receives a `StreamMeta`. Returns rows written. |
| `@destination.commit_state` | If `Capability.STATE` | Writes cursor state to `_dtex_state`. Called **only** after all batches durably land. |
| `@destination.read_state` | If `Capability.STATE` | Loads prior cursor state at run start. |
| `@destination.state_backend` | If **not** `Capability.STATE` | Returns a companion state backend for Tier B (object-storage) destinations. |
| `@destination.transaction` | If `Capability.TRANSACTIONAL_LOAD` | A context-manager hook the engine wraps around each stream's `write_batch`+`commit_state` block, so data and cursor flip atomically. See chapter 05 §1. |
| `@destination.write_run_record` | If `Capability.RUN_RECORDS` | Persists one `RunRecord` row into `_dtex_runs` — the queryable audit table. Called once per run, after streams finish and before `close`. See chapter 09 §4. |
| `@destination.max_concurrent_writes` | No (optional) | Returns the maximum number of pipelines that may target this destination concurrently under `dtex run --tag --threads N`. Signature `(config: Config) -> int`. Absent ⇒ unlimited. DuckDB returns 1 (file lock); BigQuery returns 10 by default. See chapter 02 §Concurrency. |
| `@destination.close` | **Yes** | Flushes and releases resources. Runs even on failure. |

The `Capability` enum referenced above (`STATE`, `MERGE`, `SCHEMA_EVOLUTION`, …)
is defined in chapter **05 — Destinations & State** §1; a destination declares
which members it supports, and that set fixes its capability tier.

The engine never calls these out of order. The per-run lifecycle is:

```
open → read_state → [ensure_schema → write_batch ...]* → commit_state → write_run_record → close
```

(`write_run_record` is conditional on `Capability.RUN_RECORDS`; without that capability the engine simply skips it.)

A `@destination` hook does not yield — `write_batch` returns when the batch is
durably written; an exception means the batch failed and the engine retries per
its policy. The complete `Destination` interface, the `Capability` set, the
write-disposition implementations, and the Tier A/B state model are specified in
chapter **05 — Destinations & State**; this section defines only the decorator
contract a destination author binds to.

### 3.5 The state table — `_dtex_state`

Incremental state lives **in the destination**, in a table the engine owns.

| Column | Type | Purpose |
|---|---|---|
| `connector` | STRING | Connector `name`. |
| `stream` | STRING | Stream `name`. |
| `cursor_value` | JSON | Last observed max cursor value (serialized). |
| `cursor_type` | STRING | `timestamp` / `date` / `int` / `string` — how to deserialize `cursor_value`. |
| `state_blob` | JSON | Free-form per-stream `State` contents. |
| `last_run_id` | STRING | `run_id` of the run that last advanced this row — joins to `_dtex_runs`. |
| `rows_total` | INTEGER | Cumulative rows loaded for this stream. |
| `updated_at` | TIMESTAMP | When this row was last written. |

Primary key: `(connector, stream)`. Eight columns — the canonical schema; this
table and chapter **05 §5.1** both follow `dtex/types.py::StateRecord`, the
source of truth. The engine reads this row at the start of a run and writes it
after batches are durably loaded — never mid-batch in a way that could lose
data. Because state lives with the data, a fresh checkout of a project resumes
correctly with zero local files.

## 4. The class-based escape hatch

The decorator style covers the overwhelming majority of connectors. For
genuinely complex stateful sources — a shared auth/session pooled across many
streams, an SDK that must be opened and closed, cross-stream ordering
constraints — a connector may instead subclass `Connector`.

```python
# connectors/complex_erp/source.py
from dtex import Connector, stream_method

class ComplexERPSource(Connector):
    """Escape hatch: a long-lived session shared across streams."""

    def setup(self):
        # called once before any stream runs
        self.session = open_erp_session(self.config.secrets["api_token"])

    def teardown(self):
        # called once after all streams finish (even on error)
        self.session.close()

    @stream_method(name="invoices")
    def invoices(self, state, cursor, log):
        for batch in self.session.paginate("invoices", since=cursor.start_value()):
            yield batch

    @stream_method(name="customers")
    def customers(self, state, log):
        yield self.session.fetch_all("customers")
```

The `Connector` base class:

| Member | Purpose |
|---|---|
| `self.config` | Resolved `Config` — same object the decorators inject. |
| `setup()` | Optional. Runs once before streams. Open shared resources here. |
| `teardown()` | Optional. Runs once after all streams, including on failure. Close resources here. |
| `@stream_method(name=...)` | Marks an instance method as a stream. Same injection rules as `@stream`, minus `config` (use `self.config`). |

**Why it is discouraged for normal use:** a class invites state to sprawl across
methods, makes each stream harder to read in isolation, and tempts authors into
inheritance hierarchies. The decorator style keeps each stream a single,
self-contained, testable function. Reach for the class **only** when a
genuinely shared lifecycle (`setup`/`teardown`) cannot be expressed cleanly
per-function. If you are not writing `setup()`/`teardown()`, you do not need the
class. `dtex new` never scaffolds it.

## 5. Connector resolution — baked vs custom

A run names a connector. The engine resolves the name in a fixed order:

1. **Project-local connectors** — `connectors/<name>/` under the user's project
   (`connector_paths` in `dtex_project.yml`, chapter 06).
2. **Pre-baked connectors** — `dtex/connectors/<name>/` shipped inside the
   installed `dtex` package.

Project-local wins on a name collision, so a user can fork and override a baked
connector by dropping a same-named folder into their project.

```python
import dtex

# baked connector shipped in the package
dtex.run(connector="meta_ads", target="prod")

# project-local connector in ./connectors/custom/
dtex.run(connector="custom", target="prod")
```

There is no third category and no registry service — resolution is two
filesystem lookups. Custom and baked connectors are *identical in form*; "baked"
only means "ships in the package."

## 6. How params and config flow

Resolution is layered, lowest precedence first:

```
register.yaml params[].default      (the connector's own defaults)
      └─▶ profiles.yml vars for the target   (per-environment overrides)
            └─▶ dtex_project.yml vars     (project-wide overrides)
                  └─▶ CLI flags / run() kwargs  (per-invocation, highest)
```

```bash
# register.yaml default page_size: 50  ->  overridden for this run
dtex run shiphero --target prod --param page_size=100
```

```python
dtex.run(connector="shiphero", target="prod", params={"page_size": 100})
```

The engine resolves all layers, type-checks every value against its `ParamSpec`,
resolves `secrets` refs, and hands the connector body a single immutable
`Config` object. The connector never parses YAML, reads env vars, or touches
files itself — it receives `config` and `config.secrets[...]` ready to use.

## 7. Discovery-time validation

When the engine scans a connector folder (on `dtex run`, `dtex validate`,
or `dtex list`), it validates **before importing any connector Python**:

1. **`register.yaml` exists and parses** as YAML.
2. **Schema check** — every top-level key is known; every required key is
   present; every value matches its declared type. Unknown keys are a hard
   error (catches typos like `write_dispostion`).
3. **`kind` consistency** — `kind: source` ⇒ `streams` non-empty; `kind:
   destination` ⇒ no `streams`.
4. **Stream integrity** — stream `name`s unique; `write_disposition: merge` ⇒
   `primary_key` present; `incremental.cursor_field`, if a `schema` is declared,
   appears in that schema.
5. **Reference resolution** — `destination.connector` names a discoverable
   `kind: destination` connector; every `secrets[].ref` uses a known resolver
   form (`${env...}` / `${profile...}` / `secret://...`).
6. **`requires` well-formedness** — every declared dependency string parses as
   a PEP 440 requirement specifier (a syntax check; actual installation happens
   at build time, not discovery).

Then it imports the Python and validates the **code ↔ manifest** binding:

7. **Decorator coverage** — every `streams[].name` has exactly one matching
   `@stream`/`@stream_method`; every `@stream` has a manifest entry. An orphan on
   either side is an error.
8. **Signature check** — each decorated function's parameters are all drawn from
   its injectable set (`config`, `state`, `cursor`, `log` for `@stream`; the
   hook-specific arguments for each `@destination` hook). An unrecognized
   parameter name is an error, because the engine would not know what to inject.

Validation is fail-fast and reports every problem found, not just the first, so
`dtex validate` is a useful pre-commit / CI gate.

---

**Next:** chapter **04 — Connector Body** drills into how `state`, `source`,
`streams`, and `destination` are organized *inside* a connector folder.
