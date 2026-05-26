"""The structured logger injected into ``@stream`` functions — docs/03 §3.1, docs/09.

Two cooperating sinks share one redaction policy:

* **Stdlib logger** — a per-run :class:`logging.Logger` carrying
  :class:`RedactingFilter`. This is the ``log`` injectable a connector's
  ``@stream`` function receives.
* **JSON-lines run log** — :class:`RunLog`, a per-run file at
  ``.detx/logs/<run_id>/run.jsonl``. The engine emits structured lifecycle
  events here (``run_start`` / ``stream_start`` / ``batch_loaded`` /
  ``stream_committed`` / ``stream_failed`` / ``run_end``), and any
  ``log.info(...)`` from a connector body is mirrored as a tagged ``"user"``
  event so connector chatter survives without conflicting with engine
  taxonomy (docs/09 §2).

Both sinks scrub through the same :class:`Redactor` so a secret value masked
in one is masked in the other (docs/08, docs/09 §5). The redactor is
constructed BEFORE secret values are known (the JSONL file opens at run
start, before stage 2 RESOLVE), and secrets are *added* once resolved —
mutating one shared object instead of rebuilding both sinks.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO

# The mask substituted for any secret value found in a log record — docs/08.
_REDACTED = "***"

# Below this length a "secret" value is too short to redact safely: masking a
# 1-3 char value would corrupt ordinary log text far more than it protects.
# Real credentials are long; this only ever skips trivial test placeholders.
_MIN_REDACT_LEN = 4


class Redactor:
    """A mutable bag of secret values that masks them in any rendered text.

    Shared between :class:`RedactingFilter` (stdlib logger) and :class:`RunLog`
    (JSON-lines writer) so one ``add`` call covers both sinks. The redactor is
    built at run start with whatever secrets are known then (typically none —
    the JSONL file opens before stage 2 RESOLVE), and :meth:`add` is called
    after secret resolution to cover every subsequent write.

    # NOTE: thread-safe under a coarse lock. A run is single-threaded in v1,
    # but secret resolution and JSONL writes happen on independent code paths
    # — a lock here is microscopic insurance against a future parallel-streams
    # build introducing a torn read.
    """

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        """Seed the redactor with an initial set of secret values."""
        self._lock = threading.Lock()
        self._secrets: list[str] = []
        self.add(secrets)

    def add(self, secrets: Iterable[str]) -> None:
        """Extend the secret set; short values (<:data:`_MIN_REDACT_LEN`) are skipped."""
        with self._lock:
            for s in secrets:
                if isinstance(s, str) and len(s) >= _MIN_REDACT_LEN and s not in self._secrets:
                    self._secrets.append(s)

    def redact(self, text: str) -> str:
        """Return ``text`` with every known secret value replaced by ``***``."""
        with self._lock:
            secrets = tuple(self._secrets)
        if not secrets:
            return text
        out = text
        for secret in secrets:
            if secret in out:
                out = out.replace(secret, _REDACTED)
        return out


class RedactingFilter(logging.Filter):
    """Stdlib :class:`logging.Filter` that runs each record's message through a :class:`Redactor`.

    The engine attaches one of these to the per-run logger. Each record's
    final rendered text passes through the shared :class:`Redactor` — so a
    secret can never reach a handler, a file, or a console, however it got
    into the message (docs/08).

    Mutating ``record.msg`` and clearing ``record.args`` is deliberate: the
    redaction then applies uniformly no matter which handler/formatter
    consumes the record afterwards.
    """

    def __init__(self, redactor: Redactor) -> None:
        """Wire this filter to a shared :class:`Redactor`."""
        super().__init__()
        self._redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact secret values in ``record`` in place; always return ``True``."""
        message = record.getMessage()
        redacted = self._redactor.redact(message)
        if redacted != message:
            record.msg = redacted
            record.args = None
        return True


# ---------------------------------------------------------------------------
# RunLog — the per-run JSON-lines writer (docs/09 §3.2)
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    """Return the current UTC time as an ISO-8601 string with offset."""
    return datetime.now(UTC).isoformat()


class RunLog:
    """One ``.detx/logs/<run_id>/run.jsonl`` writer, with redaction baked in.

    docs/09 §3.2: every run writes a complete JSON-lines trace, independent
    of stdout verbosity, to ``.detx/logs/<run_id>/run.jsonl`` under the
    project root. Each line is one JSON object — one event. Engine
    lifecycle events (``run_start``, ``stream_start``, …) go through
    :meth:`emit`; mirrored connector ``log.info`` calls go through the
    same path with ``event="user"`` so the taxonomies do not collide.

    Every line is rendered to JSON text first, then passed through the
    shared :class:`Redactor`, then written to disk — so a secret that
    accidentally leaks into a structured field (a nested value, a URL
    fragment) is masked just like one in a top-level message string.

    The file is opened with line-buffering so each ``write`` flushes to disk
    on its trailing newline; a crash mid-run leaves a partial-but-readable
    line-terminated file (the durability bar in the task brief).

    # NOTE: thread-safe under one lock. Engine and connector code share the
    # writer; serializing keeps interleaved events from corrupting a line.
    """

    def __init__(self, run_id: str, log_dir: Path, redactor: Redactor) -> None:
        """Open ``log_dir/<run_id>/run.jsonl`` for the run, lazily creating dirs."""
        self.run_id = run_id
        self._redactor = redactor
        self._lock = threading.Lock()
        self.dir = log_dir / run_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "run.jsonl"
        # Line buffering: each write that ends in "\n" flushes (so a crash
        # mid-run still leaves a complete-up-to-the-last-newline file). text
        # mode + utf-8 is the cross-platform-safe encoding the rest of detx
        # uses for project-local files.
        self._fh = self.path.open("w", buffering=1, encoding="utf-8")
        # The stream name set by the engine before invoking a generator. Any
        # connector ``log.info`` raised while this is set becomes a "user"
        # event tagged with the stream, so the JSONL reader can group lines
        # without parsing free-form text.
        self.active_stream: str | None = None

    def emit(self, event: str, **fields: Any) -> None:
        """Write one JSON-lines event; redaction runs on the serialized text."""
        payload: dict[str, Any] = {
            "ts": _utcnow_iso(),
            "run_id": self.run_id,
            "event": event,
        }
        payload.update(fields)
        line = json.dumps(payload, default=str)
        line = self._redactor.redact(line)
        with self._lock:
            if self._fh.closed:  # pragma: no cover — paranoid guard.
                return
            self._fh.write(line + "\n")

    def close(self) -> None:
        """Flush and close the file. Safe to call twice; never raises."""
        with self._lock:
            try:
                if not self._fh.closed:
                    self._fh.flush()
                    self._fh.close()
            except Exception:  # noqa: BLE001 — close must never raise.
                pass

    # -- context manager so the engine can use ``with`` if convenient -------

    def __enter__(self) -> RunLog:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class _RunLogHandler(logging.Handler):
    """Bridge from the stdlib :class:`logging.Logger` to a :class:`RunLog`.

    Attached to the per-run logger so a connector body's ``log.info(...)`` /
    ``log.warning(...)`` / ``log.error(...)`` is mirrored to the JSONL as a
    tagged ``"user"`` event. Engine-emitted events bypass the stdlib logger
    and call :meth:`RunLog.emit` directly, so the two taxonomies stay
    distinct without coordination.
    """

    def __init__(self, run_log: RunLog) -> None:
        super().__init__()
        self._run_log = run_log

    def emit(self, record: logging.LogRecord) -> None:
        # Redaction is already applied to ``record.msg`` by RedactingFilter
        # (which precedes us on the same logger), so by the time we render
        # here the message text is safe. The JSONL writer also re-runs
        # redaction on the serialized line — defence in depth.
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover — pathological format args.
            message = str(record.msg)
        self._run_log.emit(
            "user",
            level=record.levelname.lower(),
            message=message,
            stream=self._run_log.active_stream,
        )


# ---------------------------------------------------------------------------
# build_logger — the engine's one wiring point
# ---------------------------------------------------------------------------


def build_logger(
    run_id: str,
    redactor: Redactor | None = None,
    *,
    run_log: RunLog | None = None,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Build the run logger handed to ``@stream`` functions — docs/03 §3.1, docs/09.

    Returns a :class:`logging.Logger` named per ``run_id`` (so concurrent runs
    in one process do not share handler state) carrying a
    :class:`RedactingFilter` over the shared :class:`Redactor`. When a
    :class:`RunLog` is supplied a :class:`_RunLogHandler` is also attached,
    so a connector body's ``log.info(...)`` mirrors into the JSONL as a
    ``"user"`` event.

    ``stream`` (stage 8e): the TextIO the stdlib :class:`StreamHandler`
    writes to. ``None`` (default) means stderr — today's behavior. The
    engine's parallel ``run_tag`` path passes a per-pipeline
    :class:`io.StringIO` here so each pipeline's stdout is buffered and
    flushed under a print-lock after the pipeline completes; interleaved
    output across pipelines is then impossible.

    A single :class:`StreamHandler` is attached on first build and reused on
    a repeat call for the same ``run_id`` — the logger is never given
    duplicate stdlib handlers, so a message is emitted exactly once.
    Repeat builds replace the redacting filter and the run-log handler so
    a freshly-resolved secret set / freshly-opened JSONL is the one in
    force.

    # NOTE: each ``run()`` invocation uses a fresh ``run_id`` (uuid hex,
    # see :func:`~detx.engine.run`), so ``logging.getLogger(f"detx.run.{id}")``
    # returns a fresh, handler-less logger per run — even in the parallel
    # path. The ``if not logger.handlers`` guard therefore always takes the
    # "attach" branch in practice; the ``stream`` argument is honored on
    # that first attachment. A future code path that reuses a run_id (none
    # planned) would inherit the original stream — call sites that need a
    # fresh stream must pass a fresh run_id.
    """
    if redactor is None:
        redactor = Redactor()

    logger = logging.getLogger(f"detx.run.{run_id}")
    logger.setLevel(logging.INFO)
    # A run logger emits on its own handler, not the root logger's — keep it
    # self-contained so importing detx never reconfigures a host app's logging.
    logger.propagate = False

    if not logger.handlers:
        # ``StreamHandler(None)`` is the explicit "stderr" signal in the
        # stdlib — passing ``stream`` through preserves that semantic
        # without a special case.
        handler = logging.StreamHandler(stream) if stream is not None else logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] detx: %(message)s")
        )
        logger.addHandler(handler)
    else:
        # Repeat build for the same run_id — drop stale bridge handlers so a
        # new JSONL file does not see events meant for the previous one.
        for existing_handler in list(logger.handlers):
            if isinstance(existing_handler, _RunLogHandler):
                logger.removeHandler(existing_handler)

    # Replace the redactor filter so the freshly-supplied one is in force.
    for existing_filter in list(logger.filters):
        if isinstance(existing_filter, RedactingFilter):
            logger.removeFilter(existing_filter)
    logger.addFilter(RedactingFilter(redactor))

    if run_log is not None:
        logger.addHandler(_RunLogHandler(run_log))

    return logger
