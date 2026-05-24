"""The structured logger injected into ``@stream`` functions — docs/03 §3.1, docs/09.

A ``@stream`` function may declare a ``log`` parameter (one of the four
:data:`~det.registry.STREAM_INJECTABLES`); the engine injects a logger
here. docs/09 specifies a full structured-logging layer with correlation ids
and JSON sinks — that is a later build stage. v1 ships a thin, dependable
wrapper over the stdlib :mod:`logging` module with the one non-negotiable
property the security chapter (docs/08) demands: **secret values are redacted**.

The redaction is defence-in-depth. The engine never deliberately logs a secret,
but a connector author's ``log.info(f"calling API with {token}")`` must not
leak. :class:`RedactingFilter` masks any resolved secret value that appears in
a formatted log record, so an accidental interpolation degrades to ``***`` in
the output rather than a credential on disk.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

# The mask substituted for any secret value found in a log record — docs/08.
_REDACTED = "***"

# Below this length a "secret" value is too short to redact safely: masking a
# 1-3 char value would corrupt ordinary log text far more than it protects.
# Real credentials are long; this only ever skips trivial test placeholders.
_MIN_REDACT_LEN = 4


class RedactingFilter(logging.Filter):
    """A :class:`logging.Filter` that masks known secret values in every record.

    The engine registers one of these on the run logger, seeded with the
    resolved secret values for the run (see :func:`build_logger`). For each log
    record it renders the final message and replaces every occurrence of a
    secret value with :data:`_REDACTED` — so a secret can never reach a handler,
    a file, or a console, however it got into the message (docs/08).

    Mutating ``record.msg`` and clearing ``record.args`` is deliberate: it makes
    the redaction apply uniformly no matter which handler/formatter consumes the
    record afterwards.
    """

    def __init__(self, secrets: Iterable[str]) -> None:
        """Create the filter from the run's resolved secret values.

        Only non-empty values at least :data:`_MIN_REDACT_LEN` long are masked;
        shorter ones are skipped (see the constant's rationale).
        """
        super().__init__()
        self._secrets: tuple[str, ...] = tuple(
            s for s in secrets if isinstance(s, str) and len(s) >= _MIN_REDACT_LEN
        )

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact secret values in ``record`` in place; always return ``True``.

        Returning ``True`` keeps the record (a filter that drops records is not
        the intent — this one only sanitizes). The message is rendered once via
        :meth:`logging.LogRecord.getMessage` so ``%``-style args are resolved
        before scanning, then ``args`` is cleared so the rendered, redacted text
        is what the handler emits.
        """
        if not self._secrets:
            return True
        message = record.getMessage()
        redacted = message
        for secret in self._secrets:
            if secret in redacted:
                redacted = redacted.replace(secret, _REDACTED)
        if redacted != message:
            record.msg = redacted
            record.args = None
        return True


def build_logger(run_id: str, secret_values: Iterable[str] = ()) -> logging.Logger:
    """Build the run logger handed to ``@stream`` functions — docs/03 §3.1, docs/09.

    Returns a :class:`logging.Logger` named per ``run_id`` (so concurrent runs
    in one process do not share handler state) carrying a :class:`RedactingFilter`
    seeded with ``secret_values``. A connector body calls ``log.info(...)`` /
    ``log.warning(...)`` / ``log.error(...)`` on it; the redaction guarantees no
    secret value reaches a sink.

    A single ``StreamHandler`` is attached on first build and reused on a
    repeat call for the same ``run_id`` — the logger is never given duplicate
    handlers, so a message is emitted exactly once.
    """
    logger = logging.getLogger(f"det.run.{run_id}")
    logger.setLevel(logging.INFO)
    # A run logger emits on its own handler, not the root logger's — keep it
    # self-contained so importing det never reconfigures a host app's logging.
    logger.propagate = False

    redactor = RedactingFilter(secret_values)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] det: %(message)s")
        )
        logger.addHandler(handler)
    else:
        # Repeat build for the same run_id — drop the stale redactor so the
        # freshly-resolved secret set is the one in force.
        for existing in list(logger.filters):
            if isinstance(existing, RedactingFilter):
                logger.removeFilter(existing)
    logger.addFilter(redactor)
    return logger
