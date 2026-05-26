# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Albinas Plesnys

"""Generic REST source connector — a paginated HTTP/JSON API to any destination.

A baked source connector for any paginated REST/JSON API. The connector body's
entry points live in :mod:`detx.sources.rest.source`; pagination
strategies in :mod:`detx.sources.rest.pagination`; the HTTP client (auth,
retry, rate-limit, redaction) in :mod:`detx.sources.rest.client`; and
the ``record_path`` walker in :mod:`detx.sources.rest.extractors`.

See ``register.yaml`` for the declared streams and ``README.md`` for the
authoring pattern (one thin ``@stream`` function per declared stream, each
calling :func:`~detx.sources.rest.source.extract_stream`).
"""
