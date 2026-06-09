"""REST client handling, including LeadGurusStream base class.

Behavior was reverse-engineered from the live LeadGurus API (the vendor
publishes the endpoint list and auth, but not the response shapes). The
surprising bits this base class has to cope with:

* Auth is the ``X-API-Key`` header (not Bearer).
* ``/clients/`` returns a **bare JSON list**; every other list endpoint is
  DRF-paginated (``count``/``next``/``previous``/``results``) and exposes a
  full ``next`` URL for pagination.
* ``/leads/`` caps page size at 200 rows regardless of the requested size, and
  the summary endpoints **require** a ``date_after`` query param.

See README.md and the upstream ``leadgurus`` client's API_NOTES.md for detail.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl

from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, BaseHATEOASPaginator
from singer_sdk.streams import RESTStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from requests import Response
    from singer_sdk.helpers.types import Auth, Context

DEFAULT_API_URL = "https://clients.leadgurus.com/api/v1"
DEFAULT_LOOKBACK_DAYS = 7

#: `/leads/` rejects any date range wider than 90 days (verified against the
#: live API — a gotcha the upstream client's narrow-range probing never hit).
LEADS_MAX_WINDOW_DAYS = 90


class DateWindowPaginator(BaseAPIPaginator):
    """Walk ``date_after``/``date_before`` windows, paging within each.

    The token is a ``{"date_after", "date_before", "page"}`` dict. Within a
    window we follow the DRF ``next`` link by bumping ``page``; when a window is
    exhausted we advance to the next window until ``end`` is reached. Used for
    ``/leads/``, which caps the queryable range at 90 days.
    """

    def __init__(self, start: date, end: date, window_days: int) -> None:
        """Initialize with the first window starting at ``start``, capped at ``end``."""
        self._end = end
        self._window = window_days
        super().__init__(start_value=self._window_from(start))

    def _window_from(self, after: date) -> dict[str, Any]:
        before = min(after + timedelta(days=self._window), self._end)
        return {"date_after": after.isoformat(), "date_before": before.isoformat(), "page": 1}

    @override
    def continue_if_empty(self, response: Response) -> bool:
        # An early window may legitimately have zero leads; keep advancing to
        # later windows instead of letting the SDK stop the stream on the first
        # empty page. get_next still returns None once `end` is reached.
        return True

    @override
    def get_next(self, response: Response) -> dict[str, Any] | None:
        data = response.json()
        current = self.current_value
        if isinstance(data, dict) and data.get("next"):
            return {**current, "page": current["page"] + 1}
        before = date.fromisoformat(current["date_before"])
        if before >= self._end:
            return None
        return self._window_from(before)  # next window (boundary day overlaps; dbt dedups)


class LeadGurusPaginator(BaseHATEOASPaginator):
    """Follow the DRF ``next`` link; stop when it (or the body) isn't paginated.

    ``/clients/`` returns a bare list with no ``next`` key, so a single page is
    emitted and pagination halts immediately.
    """

    @override
    def get_next_url(self, response: Response) -> str | None:
        data = response.json()
        if isinstance(data, dict):
            return data.get("next")
        return None


class LeadGurusStream(RESTStream):
    """Base stream for the LeadGurus API."""

    # Paginated endpoints wrap rows in `results`; ClientsStream overrides this
    # to `$[*]` because /clients/ returns a bare list.
    records_jsonpath = "$.results[*]"

    #: Server-side page-size cap for this stream (200 for leads, 1000 for
    #: summaries — both lower than the docs claim). Overridden per stream.
    page_size = 200

    #: Summary endpoints 400 without `date_after`; those streams set this True
    #: so the floor is always sent even on a full refresh.
    require_date_after = False

    @property
    @override
    def url_base(self) -> str:
        return self.config.get("api_url", DEFAULT_API_URL)

    @cached_property
    @override
    def authenticator(self) -> Auth:
        """LeadGurus authenticates with an ``X-API-Key`` request header."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-API-Key",
            value=self.config["api_key"],
            location="header",
        )

    @override
    def get_new_paginator(self) -> LeadGurusPaginator:
        return LeadGurusPaginator()

    @property
    def lookback_days(self) -> int:
        """Days to re-pull before the bookmark, to catch late record updates.

        LeadGurus mutates records after creation — leads gain self-book fields,
        and summary rows recompute as conversions land — but the only date
        filter is ``date_after`` on the *created*/summary date. Re-pulling a
        small trailing window each run recovers recent updates. dbt dedups.
        """
        return int(self.config.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    def _lookback_floor(self, start_value: str | None) -> str | None:
        """Bookmark (or start_date) minus lookback, clamped to ``start_date``.

        Pure date math — no state access — so it's unit-testable in isolation.
        """
        start_date = self.config.get("start_date")
        if not start_value:
            return str(start_date)[:10] if start_date else None
        floor = date.fromisoformat(str(start_value)[:10]) - timedelta(days=self.lookback_days)
        if start_date:
            floor = max(floor, date.fromisoformat(str(start_date)[:10]))
        return floor.isoformat()

    def _date_after_floor(self, context: Context | None) -> str | None:
        """Compute the ``date_after`` value from the current bookmark."""
        return self._lookback_floor(self.get_starting_replication_key_value(context))

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        # On follow-up pages the DRF `next` URL already carries every param
        # (page, page_size, date_after) — replay it verbatim and add nothing.
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))

        params: dict[str, Any] = {"page_size": self.page_size}
        if self.replication_key:
            floor = self._date_after_floor(context)
            if floor:
                params["date_after"] = floor
        if self.require_date_after and "date_after" not in params:
            start_date = self.config.get("start_date", "")[:10]
            if start_date:
                params["date_after"] = start_date
        return params
