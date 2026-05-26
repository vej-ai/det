"""The pre-baked BigQuery destination connector — docs/05 §2.

A connector is a *folder* (docs/03 §1), not an importable API: the engine
discovers it by its ``register.yaml`` and imports its ``.py`` files inside a
:func:`~detx.registry.registration_scope`. This package marker exists only so
the folder is importable by path; ``destination.py`` (the ``@destination``
hooks), ``ddl.py`` (type mapping + identifier helpers) and ``client.py`` (the
BigQuery + GCS SDK wrappers) are the connector body.
"""
