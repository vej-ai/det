"""Generic REST source connector — a paginated HTTP/JSON API to any destination.

A baked source connector for any paginated REST/JSON API. The connector body's
entry points live in :mod:`simple_e.connectors.rest.source`; pagination
strategies in :mod:`simple_e.connectors.rest.pagination`; the HTTP client (auth,
retry, rate-limit, redaction) in :mod:`simple_e.connectors.rest.client`; and
the ``record_path`` walker in :mod:`simple_e.connectors.rest.extractors`.

See ``register.yaml`` for the declared streams and ``README.md`` for the
authoring pattern (one thin ``@stream`` function per declared stream, each
calling :func:`~simple_e.connectors.rest.source.extract_stream`).
"""
