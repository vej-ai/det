"""Google Ads source — the ``@stream`` functions the engine discovers.

The connector has ONE extraction surface: GAQL (the Google Ads Query
Language) run against ``GoogleAdsService.searchStream``. Each stream declares
a ``.gaql`` file via its ``gaql:`` block in register.yaml (the GAQL analogue
of stripe's ``sigma:`` block). Every ``@stream`` function here is a thin
wrapper over :func:`_extract_gaql`, which:

  1. reads the GAQL body from ``stream_def.gaql.query`` (the ``stream_def``
     injectable carries the parsed register.yaml entry);
  2. for incremental streams, computes the date window
     ``max(cursor, today - lookback) .. today`` and binds ``{since}`` /
     ``{until}`` into the query body (the lookback re-pulls recent days to
     absorb late conversions / attribution restatements);
  3. fans out over every ``customer_id`` in ``config.customer_ids`` (Google
     Ads is per-customer), submits the query, flattens each nested
     ``GoogleAdsRow`` into the flat snake_case columns the schema declares,
     observes the cursor, and yields batches of 500.

A nested ``GoogleAdsRow`` over REST looks like
``{"campaign": {"id": "1", "name": "X"}, "metrics": {"clicks": "5"},
"segments": {"date": "2026-01-01"}}`` — keys camelCased, leaf metrics as
strings. Flattening the nesting into flat columns is the connector's job;
type-coercing the string leaves to the declared FieldType is the engine's
NORMALIZE step (docs/03; the SDK skill's rule #3). Each stream owns an
explicit GoogleAdsRow-path → column map (``_FIELD_MAP``) so the mapping is
unambiguous rather than inferred from key-mangling.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from dtex import Batch, Config, Cursor, StreamDef, stream

from .client import GoogleAdsClient

_GADS_BATCH_SIZE = 500
"""Records per yielded batch. The engine commits state after each batch
durably lands, so smaller = finer recovery, more destination calls. 500 is a
reasonable load-job batch size."""

# Folder-relative `.gaql` paths in `gaql: {query: ...}` resolve against the
# connector folder root — same convention stripe uses for its `.sql` files.
_CONNECTOR_DIR = Path(__file__).parent

# `{name}` placeholder substitution in GAQL bodies — only `{since}` / `{until}`
# are bound. Braces never collide with GAQL syntax.
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

# Per-stream GoogleAdsRow-path → flat-column map. The path is the dotted
# GoogleAdsRow location with camelCased segments as REST returns them; the
# value is the schema column name. `customer_id` is injected by the connector
# (the row carries it only via the request, not the payload), so it is not in
# these maps.
_FIELD_MAP: dict[str, dict[str, str]] = {
    "campaigns": {
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "campaign.status": "status",
        "campaign.advertisingChannelType": "advertising_channel_type",
        "campaignBudget.amountMicros": "budget_amount_micros",
    },
    "campaign_daily_stats": {
        "segments.date": "date",
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "metrics.impressions": "impressions",
        "metrics.clicks": "clicks",
        "metrics.costMicros": "cost_micros",
        "metrics.conversions": "conversions",
        "metrics.conversionsValue": "conversions_value",
    },
    "ad_group_daily_stats": {
        "segments.date": "date",
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "adGroup.id": "ad_group_id",
        "adGroup.name": "ad_group_name",
        "adGroup.status": "status",
        "metrics.impressions": "impressions",
        "metrics.clicks": "clicks",
        "metrics.costMicros": "cost_micros",
        "metrics.conversions": "conversions",
        "metrics.conversionsValue": "conversions_value",
    },
    "ad_daily_stats": {
        "segments.date": "date",
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "adGroup.id": "ad_group_id",
        "adGroup.name": "ad_group_name",
        "adGroupAd.ad.id": "ad_id",
        "adGroupAd.status": "status",
        "metrics.impressions": "impressions",
        "metrics.clicks": "clicks",
        "metrics.costMicros": "cost_micros",
        "metrics.conversions": "conversions",
        "metrics.conversionsValue": "conversions_value",
    },
    "keyword_daily_stats": {
        "segments.date": "date",
        "campaign.id": "campaign_id",
        "campaign.name": "campaign_name",
        "adGroup.id": "ad_group_id",
        "adGroup.name": "ad_group_name",
        "adGroupCriterion.criterionId": "criterion_id",
        "adGroupCriterion.keyword.text": "keyword_text",
        "adGroupCriterion.keyword.matchType": "keyword_match_type",
        "metrics.impressions": "impressions",
        "metrics.clicks": "clicks",
        "metrics.costMicros": "cost_micros",
        "metrics.conversions": "conversions",
        "metrics.conversionsValue": "conversions_value",
    },
}


def _dig(row: dict[str, Any], dotted_path: str) -> Any:
    """Return the value at a dotted GoogleAdsRow path, or None if absent."""
    node: Any = row
    for part in dotted_path.split("."):
        if not isinstance(node, dict):
            return None
        node = node.get(part)
        if node is None:
            return None
    return node


def _flatten_row(
    row: dict[str, Any], field_map: dict[str, str], customer_id: str
) -> dict[str, Any]:
    """Flatten one nested GoogleAdsRow into the stream's flat column dict.

    Walks each GoogleAdsRow path in ``field_map`` and writes the leaf value
    under the mapped column name. ``customer_id`` is injected (the payload
    doesn't carry it — it's a property of the request, not the row). Leaf
    values are passed through untouched; the engine's NORMALIZE step coerces
    them to the declared FieldType.
    """
    flat: dict[str, Any] = {"customer_id": customer_id}
    for path, column in field_map.items():
        flat[column] = _dig(row, path)
    return flat


def _bind_placeholders(gaql: str, *, since: str, until: str) -> str:
    """Substitute ``{since}`` / ``{until}`` in a GAQL body with date literals.

    Both bind to bare ISO dates (already validated as ``date`` objects
    upstream, so no injection surface). Unknown placeholders raise
    immediately — better than searchStream failing with a parse error after
    the request is in flight.
    """
    bindings = {"since": since, "until": until}

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        try:
            return bindings[name]
        except KeyError as exc:
            raise KeyError(
                f"gads: GAQL placeholder '{{{name}}}' is not bound. "
                f"Supported placeholders: {sorted(bindings)}"
            ) from exc

    return _PLACEHOLDER_RE.sub(replace, gaql)


def _build_client(
    config: Config, log: logging.Logger | logging.LoggerAdapter[Any]
) -> GoogleAdsClient:
    """Construct a :class:`GoogleAdsClient` from ``config`` — single site.

    The four OAuth secrets are resolved by the engine and placed on the
    client; they never appear in a log line. ``login_customer_id`` is an
    ordinary (optional) param, not a secret.

    When auto-discovery is in play (``auto_discover_from_manager`` set) but no
    explicit ``login_customer_id`` is given, the manager id doubles as the
    login-customer-id — child-account data pulls require that header, so
    defaulting it here means an operator only has to name the MCC once.
    """
    login_id = str(config.get("login_customer_id") or "") or str(
        config.get("auto_discover_from_manager") or ""
    )
    return GoogleAdsClient(
        developer_token=config.secrets["developer_token"],
        client_id=config.secrets["client_id"],
        client_secret=config.secrets["client_secret"],
        refresh_token=config.secrets["refresh_token"],
        login_customer_id=login_id or None,
        api_version=str(config.get("api_version", "v24")),
        base_url=str(config.get("base_url", "https://googleads.googleapis.com")),
        token_url=str(
            config.get("token_url", "https://www.googleapis.com/oauth2/v3/token")
        ),
        max_retries=int(config.get("max_retries", 5)),
        retry_backoff_seconds=float(config.get("retry_backoff_seconds", 1.0)),
        log=log,
    )


def _resolve_customer_ids(
    config: Config,
    client: GoogleAdsClient,
    log: logging.Logger | logging.LoggerAdapter[Any],
) -> list[str]:
    """Resolve which customer ids to pull — explicit list OR MCC auto-expand.

    Precedence:

    1. An explicit, non-empty ``customer_ids`` wins outright (no API call).
    2. Otherwise, if ``auto_discover_from_manager`` names an MCC, expand its
       tree via ``customer_client`` and return the enabled, non-manager (leaf)
       children up to ``max_discovery_depth``.
    3. If neither is given, raise — there's nothing to pull.

    Hyphens are stripped from explicit ids (``123-456-7890`` == ``1234567890``).
    """
    raw = str(config.get("customer_ids") or "")
    explicit = [c.strip().replace("-", "") for c in raw.split(",") if c.strip()]
    if explicit:
        return explicit

    manager_id = str(config.get("auto_discover_from_manager") or "").replace("-", "")
    if manager_id:
        max_depth = int(config.get("max_discovery_depth", 1))
        log.info(
            "gads: auto-discovering accounts under manager %s (max_depth=%d)",
            manager_id,
            max_depth,
        )
        discovered = client.list_child_accounts(manager_id, max_depth=max_depth)
        if not discovered:
            raise ValueError(
                f"gads: auto-discovery under manager {manager_id} found no enabled "
                f"non-manager accounts within depth {max_depth}"
            )
        log.info("gads: discovered %d account(s): %s", len(discovered), discovered)
        return discovered

    raise ValueError(
        "gads: provide either `customer_ids` (explicit list) or "
        "`auto_discover_from_manager` (an MCC id to expand)"
    )


def _date_window(config: Config, cursor: Cursor) -> tuple[date, date]:
    """Compute the (since, until) date window for an incremental run.

    ``since = max(cursor.start_value() or initial, today - lookback)``;
    ``until = today``. The lookback re-pulls recent days; the cursor advances
    only past complete days (handled in :func:`_extract_gaql`).
    """
    today = datetime.now(tz=UTC).date()
    lookback = int(config.get("segments_lookback_days", 7))

    cursor_value = cursor.start_value()
    if cursor_value is None:
        cursor_value = date.fromisoformat(
            str(config.get("segments_initial_since_date", "2024-01-01"))
        )
    elif isinstance(cursor_value, str):
        cursor_value = date.fromisoformat(cursor_value)

    since = max(cursor_value, today - timedelta(days=lookback))
    return since, today


def _extract_gaql(
    stream_def: StreamDef,
    config: Config,
    cursor: Cursor | None,
    log: logging.Logger | logging.LoggerAdapter[Any],
) -> Iterator[Batch]:
    """Shared GAQL extract — drives one stream end to end across customers."""
    if stream_def.gaql is None:  # pragma: no cover — defensive guard
        raise RuntimeError(
            f"_extract_gaql called on {stream_def.name!r} which has no `gaql:` "
            f"block in register.yaml"
        )
    field_map = _FIELD_MAP.get(stream_def.name)
    if field_map is None:
        raise RuntimeError(
            f"gads: no _FIELD_MAP entry for stream {stream_def.name!r}"
        )

    gaql_text = (_CONNECTOR_DIR / stream_def.gaql.query).read_text()

    # Incremental streams bind a date window; entity streams (campaigns) have
    # no `{since}`/`{until}` and no cursor.
    if cursor is not None:
        since, until = _date_window(config, cursor)
        if since > until:  # pragma: no cover — defensive
            log.info(
                "gads.%s: since %s > until %s — no work", stream_def.name, since, until
            )
            return
        gaql_text = _bind_placeholders(
            gaql_text, since=since.isoformat(), until=until.isoformat()
        )
        log.info(
            "gads.%s: incremental window %s .. %s", stream_def.name, since, until
        )

    client = _build_client(config, log)
    customer_ids = _resolve_customer_ids(config, client, log)

    # Advance the cursor only past COMPLETE days. Today's metrics are still
    # settling, so we hold the floor at the day before today; the lookback
    # re-pulls today (and recent days) on the next run with corrected values.
    today = datetime.now(tz=UTC).date()
    max_complete: date | None = None

    batch: list[dict[str, Any]] = []
    rows_total = 0
    for cid in customer_ids:
        rows_cid = 0
        for row in client.search_stream(cid, gaql_text):
            flat = _flatten_row(row, field_map, cid)
            rows_cid += 1
            rows_total += 1

            if cursor is not None:
                day = flat.get("date")
                if isinstance(day, str) and day:
                    day_d = date.fromisoformat(day)
                    if day_d < today and (max_complete is None or day_d > max_complete):
                        max_complete = day_d

            batch.append(flat)
            if len(batch) >= _GADS_BATCH_SIZE:
                yield batch
                batch = []
        log.info("gads.%s: customer=%s rows=%d", stream_def.name, cid, rows_cid)

    if batch:
        yield batch

    if cursor is not None and max_complete is not None:
        cursor.observe(max_complete)
        log.info(
            "gads.%s: cursor advanced to %s (last complete day); total rows=%d",
            stream_def.name,
            max_complete,
            rows_total,
        )
    else:
        log.info("gads.%s: total rows=%d", stream_def.name, rows_total)


# --------------------------------------------------------------------------
# @stream functions — one per declared stream, thin wrappers over _extract_gaql.
# The engine routes by name (matched against register.yaml streams[].name).
# --------------------------------------------------------------------------


@stream(name="campaigns")
def campaigns(
    stream_def: StreamDef, config: Config, log: logging.Logger
) -> Iterator[Batch]:
    """Campaign entity list — full replace, non-incremental (no cursor arg)."""
    yield from _extract_gaql(stream_def, config, cursor=None, log=log)


@stream(name="campaign_daily_stats")
def campaign_daily_stats(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Per-campaign daily metrics — incremental on segments.date."""
    yield from _extract_gaql(stream_def, config, cursor, log)


@stream(name="ad_group_daily_stats")
def ad_group_daily_stats(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Per-ad-group daily metrics — incremental on segments.date."""
    yield from _extract_gaql(stream_def, config, cursor, log)


@stream(name="ad_daily_stats")
def ad_daily_stats(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Per-ad daily metrics — incremental on segments.date."""
    yield from _extract_gaql(stream_def, config, cursor, log)


@stream(name="keyword_daily_stats")
def keyword_daily_stats(
    stream_def: StreamDef, config: Config, cursor: Cursor, log: logging.Logger
) -> Iterator[Batch]:
    """Per-keyword daily metrics — incremental on segments.date."""
    yield from _extract_gaql(stream_def, config, cursor, log)
