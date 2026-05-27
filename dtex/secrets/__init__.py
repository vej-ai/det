# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Pluggable secret resolvers — the ``secret://<scheme>/<path>[#<field>]`` plugin surface.

docs/08 §3 specifies a typed reference syntax — ``secret://<resolver>/<path>``
— so a team can fetch credentials from a secret manager at run time without
the engine knowing which manager (GCP / AWS / Vault). Stage 9a builds the
**core**: the :class:`SecretResolver` :class:`typing.Protocol`, the
:func:`register_secret_resolver` registration call, the URL parser
(:func:`resolve_secret_url`), the project-local ``dtex_plugins.py``
import hook, and the ``importlib.metadata`` entry-points discovery.

Stage 9a ships NO live adapters: a `gcp`/`aws`/`vault` resolver is a 9b/9c
package or a project-local plugin. The core lives in this module so a future
package adds itself with a tiny ``[project.entry-points."dtex.secret_resolvers"]``
block — no engine change.

# NOTE: the existing ``${env.X}`` / ``${profile.X.Y}`` resolution lives in
# :mod:`dtex.engine.config` and stays unchanged. Those are string-interpolation
# forms baked into the ``register.yaml`` two-form contract (docs/03 §2.5);
# the ``secret://`` URL is a deliberately separate, additive third form that
# the resolver plugin layer owns. The split is intentional: ``${env.X}`` is
# the universal v1 baseline (no plugin needed), ``secret://`` is the
# extensibility surface (every cloud manager is a plugin).

Public API (re-exported from :mod:`dtex`):

* :class:`SecretResolver` — Protocol every resolver implements.
* :func:`register_secret_resolver` — project-local registration call.
* :class:`SecretResolutionError` — single exception class for every failure
  path (parse, unknown scheme, factory raised, resolver raised).

Internal-only:

* :func:`resolve_secret_url` — the parser + dispatch used by
  :mod:`dtex.engine.config`.
* :func:`is_secret_url` — the dispatch predicate.
* :func:`load_project_plugins` — the project-local ``dtex_plugins.py``
  importer (called once per project from :mod:`dtex.engine.discovery`).
* :func:`_reset_resolvers_for_testing` — test-only reset of the
  module-level registry.
"""

from __future__ import annotations

from dtex.secrets.resolvers import (
    SecretResolutionError,
    SecretResolver,
    _reset_resolvers_for_testing,
    is_secret_url,
    load_project_plugins,
    register_secret_resolver,
    resolve_secret_url,
)

__all__ = [
    "SecretResolutionError",
    "SecretResolver",
    "_reset_resolvers_for_testing",
    "is_secret_url",
    "load_project_plugins",
    "register_secret_resolver",
    "resolve_secret_url",
]
