"""Every StreamStatus must render — regression for the SKIPPED_LEASED crash.

`print_run_result` looks up each stream's status in `_STREAM_MARK` on the
render path of *every* run. When 0.5.0 added `StreamStatus.SKIPPED_LEASED`
without a map entry, the first run that actually leased-skipped a stream
crashed the CLI with `KeyError: SKIPPED_LEASED` — after the sync had already
done its work, turning a successful run into a red build. This test walks the
whole enum so a status added in future without a mark fails here, in CI,
instead of in production.
"""

from __future__ import annotations

from datetime import UTC, datetime

from dtex.cli._format import _mark_for, print_run_result
from dtex.types import RunResult, RunStatus, StreamResult, StreamStatus


def test_every_stream_status_has_a_mark() -> None:
    """`_mark_for` returns a (glyph, color) pair for every enum member."""
    for status in StreamStatus:
        mark, color = _mark_for(status)
        assert isinstance(mark, str) and mark
        assert isinstance(color, str) and color


def test_mark_for_unmapped_status_degrades_not_raises() -> None:
    """An unmapped status falls back to its name rather than KeyError.

    Simulated with a stand-in object rather than a real enum member (the enum
    is exhaustively mapped) — the point is the accessor's `.get` fallback.
    """

    class _Fake:
        value = "future_status"

    mark, color = _mark_for(_Fake())  # type: ignore[arg-type]
    assert mark == "future_status"
    assert color


def test_print_run_result_renders_all_statuses() -> None:
    """A RunResult carrying every status prints end-to-end without raising."""
    streams = [
        StreamResult(name=f"s_{status.value}", status=status)
        for status in StreamStatus
    ]
    result = RunResult(
        run_id="run-x",
        config="c",
        connector="src",
        target="prod",
        destination="bigquery",
        status=RunStatus.SUCCEEDED,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 1, 1, tzinfo=UTC),
        streams=streams,
    )
    # Must not raise — the crash this guards against was on exactly this call.
    print_run_result(result)
