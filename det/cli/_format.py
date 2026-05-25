"""Human-readable output helpers for the ``det`` CLI.

The CLI is a thin shell over the engine; these functions turn the engine's
result objects (:class:`~det.types.RunResult`,
:class:`~det.types.StateRecord`) and discovery objects into aligned,
scannable text — no raw ``repr`` dumps. They contain no logic beyond
formatting.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Any

import click

from det.types import RunResult, RunStatus, StreamStatus

# Matches ANSI SGR escape sequences (the color codes click.style emits). Used
# to measure a cell's *visible* width so colored cells still align.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _visible_len(text: str) -> int:
    """Length of ``text`` ignoring ANSI color escape sequences."""
    return len(_ANSI_RE.sub("", text))

# Per-status glyphs/colors for stream + run lines — a quick visual scan signal.
_STREAM_MARK: dict[StreamStatus, tuple[str, str]] = {
    StreamStatus.SUCCEEDED: ("ok", "green"),
    StreamStatus.FAILED: ("FAIL", "red"),
    StreamStatus.SKIPPED: ("skip", "yellow"),
}


def render_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """Render ``rows`` under ``headers`` as space-aligned columns.

    Column width is the widest cell (header included). An empty ``rows`` still
    prints the header line, so the caller need not special-case "nothing
    found" — the table is simply empty under its headers.
    """
    cols = len(headers)
    widths = [_visible_len(str(h)) for h in headers]
    for row in rows:
        for i in range(cols):
            cell = str(row[i]) if i < len(row) else ""
            widths[i] = max(widths[i], _visible_len(cell))

    def _line(cells: Sequence[Any]) -> str:
        parts = []
        for i in range(cols):
            cell = str(cells[i]) if i < len(cells) else ""
            if i == cols - 1:
                # Last column is not padded — avoids a ragged trailing run.
                parts.append(cell)
            else:
                # Pad by the visible width: a colored cell's ANSI codes are
                # zero-width, so ljust on the raw string would over-pad.
                pad = widths[i] - _visible_len(cell)
                parts.append(cell + " " * pad)
        return "  ".join(parts).rstrip()

    out = [_line(headers)]
    out.extend(_line(row) for row in rows)
    return "\n".join(out)


def _fmt_cursor(value: Any) -> str:
    """Render a cursor value for display — ``None`` becomes an em dash."""
    return "-" if value is None else str(value)


def print_run_result(result: RunResult) -> None:
    """Print a per-stream summary and a final run summary for one run.

    Per stream: name, status, rows extracted, rows loaded, cursor advance.
    Then a final line: overall status, total rows, duration, run id. On a
    failed run the engine's ``error`` is surfaced as a clean one-line message
    (no traceback) — ``run()`` already folds the exception into ``RunResult``.
    """
    click.echo(
        f"config {result.config}: source {result.connector} -> "
        f"destination {result.destination}  (target: {result.target})"
    )

    stream_rows: list[list[str]] = []
    for s in result.streams:
        mark, color = _STREAM_MARK[s.status]
        cursor = ""
        if s.cursor_before is not None or s.cursor_after is not None:
            cursor = f"{_fmt_cursor(s.cursor_before)} -> {_fmt_cursor(s.cursor_after)}"
        stream_rows.append(
            [
                click.style(mark, fg=color),
                s.name,
                str(s.rows_extracted),
                str(s.rows_loaded),
                cursor,
            ]
        )
    if stream_rows:
        table = render_table(
            ["", "STREAM", "EXTRACTED", "LOADED", "CURSOR"], stream_rows
        )
        click.echo(table)

    if result.status is RunStatus.SUCCEEDED:
        summary = click.style("succeeded", fg="green")
    else:
        summary = click.style("FAILED", fg="red")
    click.echo(
        f"run {result.run_id}: {summary} - "
        f"{result.rows_loaded} row(s), {result.duration_s:.2f}s"
    )
    if result.status is RunStatus.FAILED and result.error is not None:
        err = result.error
        click.echo(
            click.style(f"  error: {type(err).__name__}: {err}", fg="red"), err=True
        )


def print_multi_run_summary(tag: str, results: list[RunResult]) -> None:
    """Print a one-table summary of a ``det run --tag`` multi-run — stage 8d.

    Header line names the tag and totals; then one row per RunResult with
    its config name, status (colored), rows loaded, duration, and a short
    error string when failed (empty cell otherwise). Order mirrors the
    list ``run_tag`` returned — alphabetical by config name. The summary
    is *additive*: each individual run already printed its own per-stream
    table via :func:`print_run_result`, so this is the final at-a-glance
    rollup.

    # NOTE: design decision — for a failed run we render the error as
    # ``<ExcType>: <truncated message>`` rather than the full traceback,
    # matching :class:`~det.types.RunResult.to_dict`. The full traceback
    # lives in the per-run JSONL log (docs/09 §3.2); the summary row is
    # the queryability surface, the JSONL is the forensics surface.
    """
    succeeded = sum(1 for r in results if r.status is RunStatus.SUCCEEDED)
    failed = sum(1 for r in results if r.status is RunStatus.FAILED)
    total_duration = sum(r.duration_s for r in results)
    click.echo()
    click.echo(
        click.style(
            f"TAG {tag}: ran {len(results)} config(s), "
            f"{succeeded} succeeded, {failed} failed in {total_duration:.1f}s",
            bold=True,
        )
    )

    rows: list[list[str]] = []
    for r in results:
        color = "green" if r.status is RunStatus.SUCCEEDED else "red"
        status_cell = click.style(r.status.value, fg=color)
        if r.error is None:
            error_cell = "-"
        else:
            err = r.error
            msg = str(err)
            # One-liner: collapse newlines and trim very long messages.
            msg = msg.replace("\n", " ").strip()
            if len(msg) > 80:
                msg = msg[:77] + "..."
            error_cell = f"{type(err).__name__}: {msg}"
        rows.append(
            [
                r.config,
                status_cell,
                str(r.rows_loaded),
                f"{r.duration_s:.1f}s",
                error_cell,
            ]
        )
    click.echo(
        render_table(
            ["CONFIG", "STATUS", "ROWS", "DURATION", "ERROR"], rows
        )
    )
