"""Smoke test for the dtex package skeleton."""

import dtex


def test_package_imports_and_has_version() -> None:
    """The package imports cleanly and exposes a non-empty version string."""
    assert dtex.__version__
