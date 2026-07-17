# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The pre-baked CockroachDB source connector.

A connector is a *folder* (docs/03 §1), not an importable API: the engine
discovers it by its ``register.yaml`` and imports its ``.py`` files inside a
:func:`~dtex.registry.registration_scope`. This package marker exists only
so the folder is importable by path; ``source.py`` (the ``@stream`` functions),
``client.py`` (connection + SQL helpers) and ``type_mapping.py`` (the
CockroachDB → :class:`~dtex.types.FieldType` mapping) are the connector body.

CockroachDB speaks the Postgres wire protocol, so this connector shares its
driver (``psycopg``) and general shape with the pre-baked ``postgres``
connector. It differs where CockroachDB differs:

* ``AS OF SYSTEM TIME`` follower reads — historical reads that don't contend
  with the production workload and cost less on Cockroach Cloud.
* A primary-key **bootstrap** path for the first sync of an incremental
  stream — CockroachDB's fixed per-tenant SQL memory budget (Cockroach Cloud
  Standard/Basic) kills unbounded ``ORDER BY cursor_field`` sorts, and a
  cursor-keyset from the epoch degrades to a full scan per page on tables
  without a cursor-field index. PK pagination is always index-backed.
* Cockroach Cloud connection plumbing (``sslrootcert=system``, ``options``
  for ``--cluster=`` routing on non-SNI clients).
"""
