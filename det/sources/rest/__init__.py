"""Generic REST source connector — a paginated HTTP/JSON API to any destination.

A baked source connector for any paginated REST/JSON API. The connector body's
entry points live in :mod:`det.sources.rest.source`; pagination
strategies in :mod:`det.sources.rest.pagination`; the HTTP client (auth,
retry, rate-limit, redaction) in :mod:`det.sources.rest.client`; and
the ``record_path`` walker in :mod:`det.sources.rest.extractors`.

See ``register.yaml`` for the declared streams and ``README.md`` for the
authoring pattern (one thin ``@stream`` function per declared stream, each
calling :func:`~det.sources.rest.source.extract_stream`).
"""
