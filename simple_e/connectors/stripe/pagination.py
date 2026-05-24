"""Stripe cursor pagination — ``starting_after`` + ``has_more`` loop.

This is the canonical Stripe pagination idiom and the **reference**
implementation other SaaS connectors should mirror (research note §A.5
"Pagination — cursor-based"). The shape:

* request the first page with ``limit=<page_size>``; Stripe returns
  ``{"object": "list", "data": [...], "has_more": <bool>}``;
* on the next request set ``starting_after=<last_object_id_from_prior_page>``;
* repeat while ``has_more`` is true.

Why this lives in its own module: the pagination loop is **independent** of
the resource endpoint (charges, invoices, customers, ...) and the cursor
filter (``created[gte]``). Pulling it out keeps :mod:`source` free to focus
on per-stream concerns (which endpoint, which extra params, which schema)
and keeps the loop testable in isolation.

Citation: docs/connectors/stripe-research.md §B "The standard REST API".
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from typing import Any

from simple_e.connectors.stripe.client import StripeAPIError, StripeClient

# Stripe's list-page max — every list endpoint shares this. The client's
# ``page_size`` param caps to this in practice; we do not enforce it here so
# a future API bump does not require a code change to take effect.
_DEFAULT_LIMIT = 100


def paginate(
    client: StripeClient,
    endpoint: str,
    base_query: Mapping[str, Any],
    log: logging.Logger | logging.LoggerAdapter[Any] | None = None,
) -> Iterator[list[dict[str, Any]]]:
    """Iterate every Stripe list page as ``list[dict]`` — the cursor loop.

    Yields **one page at a time** (the natural Stripe batch size; one yield
    per HTTP request keeps memory bounded). Pages are yielded *as they arrive*
    — the caller does not have to wait for the whole result set to start
    consuming, which is what makes streaming pagination cheap.

    The merge of query parameters per page:

    * ``base_query`` is the caller-supplied filter (typically
      ``{"limit": <n>, "created[gte]": <ts>}`` and any ``extra_query_params``);
    * the first page sends ``base_query`` unchanged;
    * subsequent pages add ``starting_after=<last_id>`` — the only key this
      function injects.

    Stops when Stripe responds with ``has_more: false`` (or omits it, which
    Stripe never does on a list endpoint but is treated as "no more" for
    safety). A page whose ``data`` is empty also stops the loop — a defensive
    guard against an infinite loop from a malformed stub.

    Raises :class:`StripeAPIError` straight through — the caller decides
    whether to abort the whole stream or recover at a higher level.
    """
    logger: logging.Logger | logging.LoggerAdapter[Any] = (
        log if log is not None else logging.getLogger(__name__)
    )
    query: dict[str, Any] = dict(base_query)
    query.setdefault("limit", _DEFAULT_LIMIT)
    page_no = 0
    while True:
        page_no += 1
        try:
            response = client.list(endpoint, query)
        except StripeAPIError:
            logger.exception(
                "stripe: pagination failed on %s page %d", endpoint, page_no
            )
            raise

        data = response.get("data")
        if not isinstance(data, list):
            # A 2xx with no ``data`` list is a contract violation; treat as
            # "nothing more" rather than guess what Stripe meant.
            return
        if not data:
            return

        yield data

        has_more = bool(response.get("has_more"))
        if not has_more:
            return

        # Cursor the next request on the last id of this page. Stripe always
        # returns object dicts with an ``id`` on list endpoints.
        last = data[-1]
        if not isinstance(last, dict) or "id" not in last:
            # Defensive: without an id there is no way to advance.
            return
        query["starting_after"] = last["id"]


__all__ = ["paginate"]
