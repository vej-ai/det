# cockroachdb — pre-baked CockroachDB source connector

A dtex source connector that extracts rows from a CockroachDB cluster.
CockroachDB speaks the Postgres wire protocol (driver: `psycopg`), so this
connector shares its shape with the pre-baked `postgres` source — but it
changes the read strategy where CockroachDB changes the rules.

## Why not just the `postgres` connector?

Three CockroachDB realities, all learned the hard way on multi-million-row
production tables:

1. **Fixed SQL memory budgets.** On Cockroach Cloud Standard/Basic the
   per-tenant SQL memory pool is not operator-tunable. A first sync that runs
   `SELECT ... ORDER BY cursor_field` over a large table is killed with
   `memory budget exceeded` when the optimizer picks full-scan + in-memory
   sort. And a cursor-keyset from the epoch is no rescue on tables without a
   cursor-field index — every page repeats a full scan.
2. **Follower reads.** `AS OF SYSTEM TIME follower_read_timestamp()` makes
   extraction contention-free and cheaper — worth wiring in as a first-class
   config knob rather than a query hack.
3. **Cloud connection plumbing.** Public-CA certs (`sslrootcert=system`),
   `--cluster=` routing for non-SNI clients, port 26257.

## Read paths

| Situation | Strategy |
|---|---|
| First sync of an incremental stream ("bootstrap") | **Primary-key keyset sweep**: `WHERE (pk...) > (...) ORDER BY pk... LIMIT n`. Every page is a constrained primary-index scan — no sort, no cursor-field index needed, bounded memory at any table size. Observes `cursor_field` along the way and hands the engine the true global max on completion. |
| Subsequent incremental runs | **Cursor keyset** (same as `postgres`): `WHERE cursor_field > floor ORDER BY cursor_field, pk... LIMIT n`. |
| Non-incremental stream | Server-side `DECLARE ... CURSOR` / `FETCH FORWARD` full scan in one transaction (pinned with `SET TRANSACTION AS OF SYSTEM TIME` when configured). |
| `query` mode (author-written SELECT) | Wrapped-subquery cursor keyset, incremental-only — same contract as `postgres`. |

### Resumable, page-capped bootstrap

Bootstrap progress lives in the stream's `state`:

- `bootstrapped` — flipped to `true` when the sweep completes; switches the
  stream to the cursor-keyset path.
- `bootstrap_last_pk` — last primary-key tuple emitted; the resume point.
- `bootstrap_cursor_max` — running max of `cursor_field` across all bootstrap
  runs (PK order is uncorrelated with cursor order, so the final run's own
  observations are not the global max).

Set `bootstrap_max_pages` to cap each run's share of the sweep: each run
advances by `bootstrap_max_pages x batch_size` rows and commits, so a
100M-row backfill becomes a series of short, individually-committed runs
instead of one fragile multi-hour transaction of fate. `--full-refresh`
clears all three keys and restarts the sweep.

### Correctness with `AS OF SYSTEM TIME`

Follower reads are typically ~5s stale (and each bootstrap page reads at a
fresh follower timestamp). Give `incremental.lookback` enough slack to cover
(a) the staleness and (b) for very long bootstraps, the sweep duration — rows
updated *behind* the sweep position during the sweep carry a cursor value
below the observed max and are only caught by lookback re-reads.

## Supported types — `type_mapping.cockroachdb_to_field_type`

Everything the `postgres` mapping covers, plus the shapes CockroachDB's
`information_schema` actually emits:

| CockroachDB type | `FieldType` |
|---|---|
| `ARRAY` (any array column, e.g. `VARCHAR[]`) | `JSON` (driver yields a list; lands as JSON text) |
| `USER-DEFINED` (enums, incl. `crdb_region` of REGIONAL BY ROW tables) | `STRING` |
| `inet`, `interval`, `time` family | `STRING` |
| `oid` | `INTEGER` |

An unknown type raises a clear `ValueError` — extend `_CRDB_TO_FIELD_TYPE` or
declare the column explicitly in `schema:`.

## YAML config surface

Connector-level `params`: `host` (required), `port` (default 26257),
`database` (required), `user` (required), `sslmode` (default `verify-full`),
`sslrootcert` (default `system`), `options` (default empty; `--cluster=...`
for non-SNI routing), `as_of_system_time` (default empty; e.g.
`follower_read_timestamp()`), `application_name`, `connect_timeout_seconds`,
`batch_size` (default 5000), `bootstrap_max_pages` (default 0 = unlimited).

Secret: `password` (default ref `${env.COCKROACHDB_PASSWORD}`).

## REGIONAL BY ROW tables

A multi-region table's hidden `crdb_region` column is part of its primary
key. Declare it in the stream's `primary_key` and `schema` (it maps to
`STRING`), and pass it in the `primary_key` tuple in `source.py` — the
bootstrap's row-value comparison `(crdb_region, id) > (%s, %s)` binds the
enum label as text, which CockroachDB casts back in the comparison.

Note that with region replication each logical row can appear once per
region; de-duplicate downstream on the logical key if your cluster does this.

## Example streams

The two declared streams (`users`, `events`) are examples in the same spirit
as the `postgres` source — a real project overrides this connector (or, more
commonly, wraps `extract_stream` from a project-local connector) with its own
stream list. Import `dtex.sources.cockroachdb.extract` (not `.source` — importing that module registers its example streams) and see its docstring for the wrapper pattern.
