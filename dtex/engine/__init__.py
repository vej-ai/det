# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""The dtex engine — discovery, config resolution, the run lifecycle.

This package is stage 5 of the dtex build: the engine that turns a connector
NAME into a completed run. docs/02 §The triad places the engine alongside the
library and the CLI as one of three front doors onto the same machinery; the
engine is the machinery.

Module layout — one module per lifecycle concern, so each is small and testable
in isolation:

* :mod:`dtex.engine.discovery` — stage 1 (DISCOVER): find the project root,
  resolve a SOURCE / DESTINATION NAME to a loaded ``register.yaml`` +
  populated :class:`~dtex.registry.ConnectorRegistry` (project-local beats
  baked, docs/03 §5), run discovery-time validation (docs/03 §7).
* :mod:`dtex.engine.configs` — stage 8.B's parser for ``configs/*.yml``: the
  runtime unit is now a *config* (one source + one destination + one target),
  not a connector. Returns ``{name: PipelineConfig}``.
* :mod:`dtex.engine.config` — stage 2 (RESOLVE): parse
  ``dtex_project.yml`` / ``profiles.yml``, merge the layered param
  precedence (docs/03 §6, docs/12), resolve ``${env.X}`` / ``${profile.X.Y}``
  secrets, produce the frozen :class:`~dtex.types.RunConfig` and immutable
  per-connector :class:`~dtex.types.Config` objects.
* :mod:`dtex.engine.runner` — stages 3-6: INIT DEST, LOAD STATE, RUN STREAMS
  (per-stream commit, sequential), RUN RECORD. Exposes :func:`run`.
* :mod:`dtex.engine.logger` — the redacting structured logger injected as
  the ``log`` parameter of a ``@stream`` function.

The single public entry point is :func:`run`, re-exported from the top-level
``dtex`` package as :func:`dtex.run` -- the CLI and the library both call it.
"""

from __future__ import annotations

from dtex.engine.config import ConfigError
from dtex.engine.discovery import DiscoveryError
from dtex.engine.runner import (
    EngineError,
    last_run_tag_parallelism,
    run,
    run_tag,
)

__all__ = [
    "run",
    "run_tag",
    "last_run_tag_parallelism",
    "ConfigError",
    "DiscoveryError",
    "EngineError",
]
