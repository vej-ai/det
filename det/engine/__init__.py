"""The det engine — discovery, config resolution, the run lifecycle.

This package is stage 5 of the det build: the engine that turns a connector
NAME into a completed run. docs/02 §The triad places the engine alongside the
library and the CLI as one of three front doors onto the same machinery; the
engine is the machinery.

Module layout — one module per lifecycle concern, so each is small and testable
in isolation:

* :mod:`det.engine.discovery` — stage 1 (DISCOVER): find the project root,
  resolve a SOURCE / DESTINATION NAME to a loaded ``register.yaml`` +
  populated :class:`~det.registry.ConnectorRegistry` (project-local beats
  baked, docs/03 §5), run discovery-time validation (docs/03 §7).
* :mod:`det.engine.configs` — stage 8.B's parser for ``configs/*.yml``: the
  runtime unit is now a *config* (one source + one destination + one target),
  not a connector. Returns ``{name: PipelineConfig}``.
* :mod:`det.engine.config` — stage 2 (RESOLVE): parse
  ``det_project.yml`` / ``profiles.yml``, merge the layered param
  precedence (docs/03 §6, docs/12), resolve ``${env.X}`` / ``${profile.X.Y}``
  secrets, produce the frozen :class:`~det.types.RunConfig` and immutable
  per-connector :class:`~det.types.Config` objects.
* :mod:`det.engine.runner` — stages 3-6: INIT DEST, LOAD STATE, RUN STREAMS
  (per-stream commit, sequential), RUN RECORD. Exposes :func:`run`.
* :mod:`det.engine.logger` — the redacting structured logger injected as
  the ``log`` parameter of a ``@stream`` function.

The single public entry point is :func:`run`, re-exported from the top-level
``det`` package as :func:`det.run` -- the CLI and the library both call it.
"""

from __future__ import annotations

from det.engine.config import ConfigError
from det.engine.discovery import DiscoveryError
from det.engine.runner import EngineError, run, run_tag

__all__ = [
    "run",
    "run_tag",
    "ConfigError",
    "DiscoveryError",
    "EngineError",
]
