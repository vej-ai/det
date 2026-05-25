"""The pre-baked Postgres source connector.

A connector is a *folder* (docs/03 §1), not an importable API: the engine
discovers it by its ``register.yaml`` and imports its ``.py`` files inside a
:func:`~det.registry.registration_scope`. This package marker exists only
so the folder is importable by path; ``source.py`` (the ``@stream`` functions),
``client.py`` (connection + SQL helpers) and ``type_mapping.py`` (the
Postgres → :class:`~det.types.FieldType` mapping) are the connector body.
"""
