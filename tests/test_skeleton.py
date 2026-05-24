"""Smoke test for the det package skeleton."""

import det


def test_package_imports_and_has_version() -> None:
    """The package imports cleanly and exposes a non-empty version string."""
    assert det.__version__
