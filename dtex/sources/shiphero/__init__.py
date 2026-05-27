# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""ShipHero baked source connector — ecommerce/fulfillment data via GraphQL.

See ``register.yaml`` for the manifest and ``source.py`` for the entry points.
This package is intentionally empty so the engine's connector-folder import
harness (one ``.py`` file at a time, see ``dtex/engine/discovery.py``)
remains the only path that runs the connector body.
"""
