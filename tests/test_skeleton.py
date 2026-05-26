"""Smoke test for the detx package skeleton."""

import detx


def test_package_imports_and_has_version() -> None:
    """The package imports cleanly and exposes a non-empty version string."""
    assert detx.__version__
