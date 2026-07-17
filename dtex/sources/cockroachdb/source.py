# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""CockroachDB source — ``@stream`` functions plus the shared extractor.

Each ``@stream`` function is a thin wrapper that delegates to
:func:`extract_stream`. The helper opens a connection lazily on first
iteration, paginates, yields ``batch_size`` records per ``yield``, and closes
the connection when the generator is exhausted or raises.

Read paths
----------

* **Bootstrap (first sync of an incremental stream)** — primary-key keyset:
  ``WHERE (pk...) > (...) ORDER BY pk... LIMIT batch_size``. Every page is a
  constrained scan of the primary index — no sort, no cursor-field index
  needed, bounded memory. This exists because CockroachDB (notably Cockroach
  Cloud Standard/Basic, where the per-tenant SQL memory budget is fixed and
  not operator-tunable) kills an unbounded ``ORDER BY cursor_field`` over a
  large table with "memory budget exceeded", and a cursor-keyset from the
  epoch degrades to one full scan *per page* on tables without a cursor-field
  index. The bootstrap observes ``cursor_field`` per row and records progress
  in ``state`` (``bootstrap_last_pk``), so a page-capped or interrupted
  bootstrap resumes where it left off instead of restarting.
* **Steady state (subsequent incremental runs)** — cursor keyset exactly like
  the ``postgres`` connector: ``WHERE cursor_field > floor ORDER BY
  cursor_field, pk... LIMIT batch_size``. The floor is recent, so the WHERE
  prunes to a small span.
* **Full scan (non-incremental streams)** — server-side ``DECLARE … CURSOR``
  / ``FETCH FORWARD`` inside one transaction.
* **Query mode** — an author-written SELECT wrapped for cursor-keyset
  pagination, unchanged from the ``postgres`` connector.

All table read paths accept an ``AS OF SYSTEM TIME`` expression from config
(``as_of_system_time``; e.g. ``follower_read_timestamp()``) — follower reads
don't contend with the production workload and are cheaper on Cockroach
Cloud. Combine with an ``incremental.lookback`` at least as long as the
staleness (and, for correctness across a long bootstrap, as long as the
bootstrap itself) so rows updated behind the read timestamp are re-read.

Per-stream CockroachDB details
------------------------------

# NOTE: per-stream knobs (``schema_name``, ``table_name``, ``query``,
# ``cursor_field``, ``primary_key``) are *hardcoded constants* per ``@stream``
# function below, NOT YAML ``params``. The engine constructs a single
# connector-level :class:`~dtex.types.Config` and injects only that into a
# ``@stream`` function; ``stream_def.params`` is never merged into it. So
# per-stream configuration lives in code — same contract as the ``postgres``
# connector (see its module docstring for the full rationale).

State keys
----------

The bootstrap records its progress under three ``state`` keys:

* ``bootstrapped`` — ``True`` once the initial PK sweep has completed; the
  switch that moves the stream to the cursor-keyset path.
* ``bootstrap_last_pk`` — the last primary-key tuple emitted (JSON-safe), the
  resume point for an interrupted or page-capped bootstrap.
* ``bootstrap_cursor_max`` — the running maximum ``cursor_field`` value seen
  across *all* bootstrap runs (JSON-safe). PK order is uncorrelated with
  cursor order, so the final run's own observations are not the global max;
  this key is what makes the handed-over cursor correct.

A ``--full-refresh`` run clears all three and starts the sweep over.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from dtex import Batch, Config, Cursor, State, stream
from dtex.sources.cockroachdb.extract import extract_stream

# ---------------------------------------------------------------------------
# The two example @stream functions — thin wrappers over the shared extractor
# ---------------------------------------------------------------------------


@stream(name="users")
def users(
    config: Config, state: State, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Extract ``public.users`` incrementally on ``updated_at``.

    Declares ``incremental`` in ``register.yaml`` — so ``cursor`` is injected
    (docs/03 §3.2). ``state`` carries the bootstrap progress. Yields batches
    of ``config.batch_size`` records.
    """
    yield from extract_stream(
        stream_name="users",
        config=config,
        state=state,
        cursor=cursor,
        log=log,
        schema_name="public",
        table_name="users",
        cursor_field="updated_at",
        primary_key=("id",),
    )


@stream(name="events")
def events(
    config: Config, state: State, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Extract ``public.events`` incrementally on ``occurred_at``."""
    yield from extract_stream(
        stream_name="events",
        config=config,
        state=state,
        cursor=cursor,
        log=log,
        schema_name="public",
        table_name="events",
        cursor_field="occurred_at",
        primary_key=("id",),
    )
