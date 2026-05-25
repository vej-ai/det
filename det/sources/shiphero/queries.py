"""GraphQL query strings for each ShipHero stream — docs/04 "schema.py" role.

One constant per declared stream. Ported verbatim from the v2 reference's
`config.json` `tables.*.query` fields (with `orders` and `products` filled in
from the public ShipHero schema in the same shape). Keeping the query strings
in their own module — distinct from `source.py` (the @stream entry points) and
`client.py` (HTTP plumbing) — matches the docs/04 file-layout convention that
"shaping logic" lives alongside the body but not inside it.

Each query takes four variables:

* ``$first: Int``         — page size
* ``$after: String``      — opaque GraphQL cursor (the previous page's endCursor)
* ``$dateFrom: ISODateTime``
* ``$dateTo: ISODateTime``

ShipHero's response shape (and therefore the `FIELD_PATHS` walk used by
`source.py`) is always::

    {"data": {"<stream>": {"data": {"edges": [{"node": {...}}, ...],
                                    "pageInfo": {"hasNextPage": ..., "endCursor": ...}}}}}

The `*` wildcard in a field path means "for every element of this list, yield
its element" — that is how `source.extract_records` unwraps the `edges` array
into one node-dict per row.
"""

from __future__ import annotations

# NOTE: query strings carry no Python expressions — they are pure GraphQL text
# the client POSTs alongside its `variables` map. The `shipments` query was
# ported from a hand-written internal connector's config; `orders` / `products`
# match the public ShipHero schema in the same shape.

SHIPMENTS_QUERY: str = (
    "query Shipments($first:Int,$after:String,"
    "$dateFrom:ISODateTime,$dateTo:ISODateTime){ "
    "shipments(date_from:$dateFrom date_to:$dateTo){ "
    "request_id complexity "
    "data(first:$first,after:$after){ "
    "edges{ node{ "
    "id legacy_id order_id user_id warehouse_id pending_shipment_id "
    "shipped_off_shiphero dropshipment created_date "
    "shipping_labels{ id legacy_id account_id tracking_number carrier "
    "shipping_name shipping_method cost profile packing_slip warehouse "
    "insurance_amount carrier_account_id source created_date } "
    "line_items{ id quantity sku product_name price } "
    "} } "
    "pageInfo{ hasNextPage endCursor } } } }"
)

ORDERS_QUERY: str = (
    "query Orders($first:Int,$after:String,"
    "$dateFrom:ISODateTime,$dateTo:ISODateTime){ "
    "orders(order_date_from:$dateFrom order_date_to:$dateTo){ "
    "request_id complexity "
    "data(first:$first,after:$after){ "
    "edges{ node{ "
    "id legacy_id order_number shop_name fulfillment_status order_date "
    "total_price subtotal total_tax email profile "
    "line_items{ edges{ node{ id sku product_name quantity price } } } "
    "} } "
    "pageInfo{ hasNextPage endCursor } } } }"
)

PRODUCTS_QUERY: str = (
    "query Products($first:Int,$after:String,"
    "$dateFrom:ISODateTime,$dateTo:ISODateTime){ "
    "products(updated_from:$dateFrom updated_to:$dateTo){ "
    "request_id complexity "
    "data(first:$first,after:$after){ "
    "edges{ node{ "
    "id legacy_id sku name barcode price value "
    "created_at updated_at kit kit_build "
    "warehouse_products{ id warehouse_id on_hand available } "
    "} } "
    "pageInfo{ hasNextPage endCursor } } } }"
)


# Maps a stream name → (query, field_path_to_records, field_path_to_pageinfo).
# field_path_to_records uses "*" to mean "iterate this list element". field_path
# to pageinfo points at the parent `data` block containing both `edges` and
# `pageInfo`.
#
# NOTE: kept here next to the queries — `source.py` only needs the dispatcher;
# splitting it out would force a second import for one constant.
STREAM_QUERIES: dict[str, str] = {
    "shipments": SHIPMENTS_QUERY,
    "orders": ORDERS_QUERY,
    "products": PRODUCTS_QUERY,
}

# Top-level GraphQL field name per stream (the key under "data" in the response).
# Identical to the stream names today; declared explicitly so a future stream
# whose GraphQL field name differs (e.g. a `purchase_orders` stream querying
# `purchaseOrders`) can declare the mapping without code changes elsewhere.
STREAM_GRAPHQL_FIELDS: dict[str, str] = {
    "shipments": "shipments",
    "orders": "orders",
    "products": "products",
}
