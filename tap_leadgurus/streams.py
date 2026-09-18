"""Stream type classes for tap-leadgurus.

Schemas are ported verbatim from the field inventories reverse-engineered in
the upstream ``leadgurus`` Python client (Vertex-Service-Partners/shared-libs,
``leadgurus/API_NOTES.md`` + ``models.py``). Money and rate fields arrive as
decimal strings ("63.10", "0.00") and are kept as ``StringType`` to preserve
precision; counts are integers. Nearly every lead field is a string on the
wire — only ``success_post`` is a bool.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from singer_sdk import typing as th

from tap_leadgurus.client import (
    LEADS_MAX_WINDOW_DAYS,
    DateWindowPaginator,
    LeadGurusStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

# ---------------------------------------------------------------------------
# clients
# ---------------------------------------------------------------------------


class ClientsStream(LeadGurusStream):
    """Brands/clients visible to the API key, from GET /clients/.

    Returns a bare JSON list (no pagination, no `results` envelope).
    """

    name = "clients"
    path = "/clients/"
    primary_keys = ("id",)
    replication_key = None  # Small reference list; full sync each run.
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, description="LeadGurus client id"),
        th.Property("name", th.StringType),
        th.Property("dba_name", th.StringType, description="Nullable DBA name"),
        th.Property("slug", th.StringType, description="Client slug (brand key)"),
        th.Property("parent_company_name", th.StringType),
        th.Property("active", th.BooleanType),
        th.Property("tier", th.IntegerType),
        th.Property("status", th.StringType, description='e.g. "active_new", "off"'),
    ).to_dict()


# ---------------------------------------------------------------------------
# leads
# ---------------------------------------------------------------------------


class LeadsStream(LeadGurusStream):
    """Individual leads, from GET /leads/.

    The list endpoint exposes **no per-lead id** and ``/leads/{id}/`` returns
    SPA HTML, so there is no usable surrogate key from the API. ``primary_keys``
    is intentionally empty; dedup downstream in dbt on a composite of
    (``client_slug``, ``phone``, ``date_created``, ``ad_id``).

    Incremental on ``date_created`` via the ``date_after`` filter, which the API
    applies to the created date. Late self-book updates are recovered only
    within the ``lookback_days`` window — see client.py for why.
    """

    name = "leads"
    path = "/leads/"
    primary_keys = ()  # No natural key; never mark a PK required (NOT NULL footgun).
    replication_key = "date_created"
    page_size = 200  # /leads/ caps at 200 regardless of requested size.

    schema = th.PropertiesList(
        # identity / attribution (ad-platform ids, all strings)
        th.Property("ad_id", th.StringType),
        th.Property("ad_set_id", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("client_name", th.StringType),
        th.Property("client_slug", th.StringType, description="Brand discriminator"),
        th.Property("source", th.StringType),
        th.Property("medium", th.StringType),
        # contact details, personally identifiable
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("address", th.StringType),
        th.Property("street", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zip_code", th.StringType),
        # classification
        th.Property("vertical", th.StringType),
        th.Property("project_type", th.StringType),
        th.Property("territory", th.StringType),
        th.Property("credit_score", th.StringType),
        th.Property("success_post", th.BooleanType, description="Only bool field on a lead"),
        # vertical-specific (often empty strings)
        th.Property("flooring_square_feet", th.StringType),
        th.Property("flooring_type", th.StringType),
        th.Property("roof_shading", th.StringType),
        th.Property("windows_count", th.StringType),
        # timestamps (ISO-8601 with offset)
        th.Property("date_created", th.DateTimeType),
        th.Property("date_updated", th.DateTimeType),
        # self-booking (null unless the lead self-booked; format unconfirmed → string)
        th.Property("self_book_appointment_date_time_utc", th.StringType),
        th.Property("self_book_appointment_datetime", th.StringType),
        th.Property("self_book_date_utc", th.StringType),
        th.Property("self_book_notes", th.StringType),
        th.Property("self_book_time_slot", th.StringType),
        th.Property("self_book_time_utc", th.StringType),
    ).to_dict()

    @override
    def get_new_paginator(self) -> DateWindowPaginator:
        # /leads/ caps the date range at 90 days, so page within ≤90-day
        # windows from the bookmark floor up to today. context is None for this
        # unpartitioned stream, so the bookmark lookup needs no partition key.
        floor = self._lookback_floor(self.get_starting_replication_key_value(None))
        start = date.fromisoformat(floor or self.config["start_date"][:10])
        end = datetime.now(timezone.utc).date()
        return DateWindowPaginator(start, end, window_days=LEADS_MAX_WINDOW_DAYS)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        # next_page_token is the window dict (the paginator's start_value seeds
        # the first request), so it's always present here.
        params = dict(next_page_token or {})
        params["page_size"] = self.page_size
        return params


# ---------------------------------------------------------------------------
# summary/* — one shared schema across all six dimension endpoints
# ---------------------------------------------------------------------------

# Common fields are always present; the dimension-specific fields
# (vertical/channel_name/territory_name/dma_name/state/campaign_id) populate
# only on the endpoint that produces them. A single schema covers all six.
SUMMARY_SCHEMA = th.PropertiesList(
    # common
    th.Property("id", th.IntegerType),
    th.Property("date", th.DateType, description="Summary day (YYYY-MM-DD)"),
    th.Property("client_id", th.IntegerType),
    th.Property("client_name", th.StringType),
    th.Property("client_slug", th.StringType),
    th.Property("total_leads", th.IntegerType),
    th.Property("total_spend", th.StringType, description="Decimal string (money)"),
    th.Property("cost_per_lead", th.StringType),
    th.Property("conversion_rate", th.StringType),
    th.Property("accepted_count", th.IntegerType),
    th.Property("accepted_rate", th.StringType),
    th.Property("success_count", th.IntegerType),
    th.Property("self_book_count", th.IntegerType),
    th.Property("self_book_rate", th.StringType),
    th.Property("cost_per_self_book", th.StringType),
    th.Property("cost_per_accepted", th.StringType),
    th.Property("booked", th.IntegerType),
    th.Property("scheduled", th.IntegerType),
    th.Property("issues", th.IntegerType),
    th.Property("demos", th.IntegerType),
    th.Property("gross_sales", th.IntegerType),
    th.Property("net_sales", th.IntegerType),
    th.Property("gross_amount", th.StringType, description="Decimal string (money)"),
    th.Property("net_amount", th.StringType, description="Decimal string (money)"),
    # spend basis — added by LeadGurus 2026-09 (TD-1400), present on every
    # summary row regardless of the `include_fees` setting, so the basis
    # actually received is always explicit in the data.
    th.Property(
        "fee_rate",
        th.StringType,
        description='Client management-fee rate as a decimal string, e.g. "0.1100" (11%)',
    ),
    th.Property(
        "spend_includes_fees",
        th.BooleanType,
        description=(
            "True when total_spend / cost_per_lead / cost_per_accepted / "
            "cost_per_self_book include the management fee (invoice basis)"
        ),
    ),
    # dimension-specific (whichever endpoint produced the row)
    th.Property("vertical", th.StringType),
    th.Property("channel_name", th.StringType),
    th.Property("territory_name", th.StringType),
    th.Property("dma_name", th.StringType),
    th.Property("state", th.StringType),
    th.Property("campaign_id", th.StringType),
).to_dict()


class _SummaryStream(LeadGurusStream):
    """Base for the six daily ``/summary/<dimension>/`` rollups.

    All require ``date_after`` (enforced via ``require_date_after``) and are
    incremental on ``date``. ``page_size`` cap here is 1000 (vs 200 for leads).

    **Spend basis.** By default the API returns ad spend only. With the
    ``include_fees`` config on, the tap adds ``include_fees=true`` and
    ``total_spend`` / ``cost_per_lead`` / ``cost_per_accepted`` /
    ``cost_per_self_book`` come back with the LeadGurus management fee folded
    in (the invoice/dashboard basis). Lead counts, rates and revenue fields are
    the same either way. Each row's ``spend_includes_fees`` says which basis
    was actually served — the key can also be flipped to fee-inclusive on the
    LeadGurus Developer API page, which the tap cannot see.
    """

    primary_keys = ("id",)
    replication_key = "date"
    require_date_after = True
    page_size = 1000
    schema = SUMMARY_SCHEMA

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # First page only: the DRF `next` URL replays every query param
        # (include_fees along with date_after/page_size), so follow-up pages
        # already carry it. Only ever send the param when opted in — the API's
        # documented switch is `include_fees=true`; absence means the key's own
        # spend-basis setting applies, exactly as before this option existed.
        if not next_page_token and self.config.get("include_fees"):
            params["include_fees"] = "true"
        return params


class SummaryClientStream(_SummaryStream):
    """Daily rollup per client — the only source of spend/cost/sales."""

    name = "summary_client"
    path = "/summary/client/"


class SummaryVerticalStream(_SummaryStream):
    """Daily rollup broken out by vertical."""

    name = "summary_vertical"
    path = "/summary/vertical/"


class SummaryChannelStream(_SummaryStream):
    """Daily rollup broken out by channel."""

    name = "summary_channel"
    path = "/summary/channel/"


class SummaryTerritoryStream(_SummaryStream):
    """Daily rollup broken out by territory."""

    name = "summary_territory"
    path = "/summary/territory/"


class SummaryDmaStream(_SummaryStream):
    """Daily rollup broken out by DMA."""

    name = "summary_dma"
    path = "/summary/dma/"


class SummaryStateStream(_SummaryStream):
    """Daily rollup broken out by state."""

    name = "summary_state"
    path = "/summary/state/"
