# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The pre-baked DuckDB destination connector — docs/05 §2.

A connector is a *folder* (docs/03 §1), not an importable API: the engine
discovers it by its ``register.yaml`` and imports its ``.py`` files inside a
:func:`~dtex.registry.registration_scope`. This package marker exists only
so the folder is importable by path; ``destination.py`` (the ``@destination``
hooks) and ``ddl.py`` (helpers) are the connector body.
"""
