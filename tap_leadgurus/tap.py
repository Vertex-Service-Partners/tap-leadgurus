"""LeadGurus tap class."""

from __future__ import annotations

import sys

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_leadgurus import streams
from tap_leadgurus.client import DEFAULT_API_URL, DEFAULT_LOOKBACK_DAYS

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class TapLeadGurus(Tap):
    """Singer tap for LeadGurus.

    Extracts marketing leads and the daily spend/cost/conversion summary
    rollups (by client, vertical, channel, territory, DMA, and state) from the
    read-only LeadGurus API. One API key spans every Vertex brand; the brand is
    carried on each record as ``client_slug``.
    """

    name = "tap-leadgurus"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="API Key",
            description="LeadGurus API key, sent as the X-API-Key header.",
        ),
        th.Property(
            "start_date",
            th.DateType(),
            required=True,
            title="Start Date",
            description=(
                "Earliest date to sync (YYYY-MM-DD). Used as the floor for the "
                "`date_after` filter on leads and summaries, and as the required "
                "`date_after` for summary endpoints on a full refresh."
            ),
        ),
        th.Property(
            "api_url",
            th.StringType(nullable=False),
            title="API URL",
            default=DEFAULT_API_URL,
            description="Base URL for the LeadGurus API.",
        ),
        th.Property(
            "lookback_days",
            th.IntegerType(),
            default=DEFAULT_LOOKBACK_DAYS,
            title="Lookback Days",
            description=(
                "Days to re-pull before the saved bookmark, to recover records "
                "that LeadGurus mutates after creation (lead self-book fields, "
                "recomputed summary rows). Dedup downstream in dbt."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list[streams.LeadGurusStream]:
        """Return a list of discovered streams."""
        return [
            streams.ClientsStream(self),
            streams.LeadsStream(self),
            streams.SummaryClientStream(self),
            streams.SummaryVerticalStream(self),
            streams.SummaryChannelStream(self),
            streams.SummaryTerritoryStream(self),
            streams.SummaryDmaStream(self),
            streams.SummaryStateStream(self),
        ]


if __name__ == "__main__":
    TapLeadGurus.cli()
