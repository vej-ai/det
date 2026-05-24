"""Stripe baked source connector — resource-as-stream over the v1 REST API.

See ``register.yaml`` for the manifest and ``source.py`` for the ``@stream``
implementations. The design decision (resource-as-stream, not Sigma
query-as-stream) is recorded in ``docs/connectors/stripe-research.md``.
"""
