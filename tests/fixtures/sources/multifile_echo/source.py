"""The multifile_echo source — proves relative imports between sibling files work.

The single @stream function pulls its fixture data from a sibling module via
a relative import. Under the historical per-file standalone-module load this
file failed at import time with ``ImportError: attempted relative import with
no known parent package``; under the stage 11 load-as-package mechanism it
resolves cleanly.
"""

from __future__ import annotations

from collections.abc import Iterator

from dtex import Batch, stream

from .helper import FIXTURE_BATCHES


@stream(name="records")
def records() -> Iterator[Batch]:
    """Yield the fixture batches the sibling helper module defines."""
    yield from FIXTURE_BATCHES
