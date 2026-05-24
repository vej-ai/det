"""Filesystem baked source connector — flat files (CSV / JSONL / Parquet).

See ``register.yaml`` for the manifest and ``source.py`` for the entry points.
This package is intentionally empty so the engine's connector-folder import
harness (one ``.py`` file at a time, see ``simple_e/engine/discovery.py``)
remains the only path that runs the connector body.
"""
