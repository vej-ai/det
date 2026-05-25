"""Tests for the per-destination connector folders (DuckDB, BigQuery, …).

Top-level ``tests/test_<destination>_destination.py`` modules historically
covered the DuckDB destination one-off; new destinations split tests into
this subpackage so they grow per-destination without cluttering the suite
root.
"""
