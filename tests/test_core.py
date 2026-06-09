"""Tests for tap-leadgurus.

Unit tests (top of file) need no credentials or network. The SDK standard test
suite at the bottom is gated behind a real ``TAP_LEADGURUS_API_KEY`` and only
runs when one is present.
"""

from __future__ import annotations

import os
from datetime import date
from types import SimpleNamespace

import pytest

from tap_leadgurus.client import DateWindowPaginator, LeadGurusPaginator
from tap_leadgurus.streams import LeadsStream, SummaryClientStream
from tap_leadgurus.tap import TapLeadGurus

STUB_CONFIG = {
    "api_key": "stub-key",
    "start_date": "2026-01-01",
}

EXPECTED_STREAM_NAMES = {
    "clients",
    "leads",
    "summary_client",
    "summary_vertical",
    "summary_channel",
    "summary_territory",
    "summary_dma",
    "summary_state",
}


@pytest.fixture
def tap() -> TapLeadGurus:
    return TapLeadGurus(config=STUB_CONFIG)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_discover_streams_returns_all_known_streams(tap: TapLeadGurus) -> None:
    names = {s.name for s in tap.discover_streams()}
    assert names == EXPECTED_STREAM_NAMES


# ---------------------------------------------------------------------------
# Schema / key invariants
# ---------------------------------------------------------------------------


def test_leads_has_no_primary_key(tap: TapLeadGurus) -> None:
    # /leads/ exposes no per-lead id — keep PK empty and dedup in dbt.
    stream = LeadsStream(tap)
    assert tuple(stream.primary_keys or ()) == ()


def test_no_stream_marks_any_property_required(tap: TapLeadGurus) -> None:
    # required=True propagates to a NOT NULL DDL constraint in target-snowflake
    # and breaks MERGE on null keys. No property should ever be required.
    for stream in tap.discover_streams():
        assert "required" not in stream.schema, stream.name


def test_leads_replicates_on_date_created(tap: TapLeadGurus) -> None:
    stream = LeadsStream(tap)
    assert stream.replication_key == "date_created"
    assert stream.page_size == 200


def test_all_summary_streams_require_date_after(tap: TapLeadGurus) -> None:
    for stream in tap.discover_streams():
        if stream.name.startswith("summary_"):
            assert stream.require_date_after is True, stream.name
            assert stream.replication_key == "date", stream.name


def test_summary_schema_carries_spend_and_dimension_fields(tap: TapLeadGurus) -> None:
    props = SummaryClientStream(tap).schema["properties"]
    # spend lives only on the summaries
    assert "total_spend" in props
    assert "cost_per_lead" in props
    # the union of all six dimension extras is present on the shared schema
    for field in ("vertical", "channel_name", "territory_name", "dma_name", "state"):
        assert field in props


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def test_paginator_follows_drf_next_link() -> None:
    paginator = LeadGurusPaginator()
    response = SimpleNamespace(json=lambda: {"next": "https://x/api/v1/leads/?page=2", "results": []})
    assert paginator.get_next_url(response) == "https://x/api/v1/leads/?page=2"


def test_paginator_stops_on_bare_list() -> None:
    # /clients/ returns a bare list with no `next` key.
    paginator = LeadGurusPaginator()
    response = SimpleNamespace(json=lambda: [{"id": 1}, {"id": 2}])
    assert paginator.get_next_url(response) is None


def test_paginator_stops_when_next_is_null() -> None:
    paginator = LeadGurusPaginator()
    response = SimpleNamespace(json=lambda: {"next": None, "results": []})
    assert paginator.get_next_url(response) is None


# ---------------------------------------------------------------------------
# date_after injection + lookback
# ---------------------------------------------------------------------------


def test_first_page_summary_sends_date_after_floor(tap: TapLeadGurus) -> None:
    stream = SummaryClientStream(tap)
    params = stream.get_url_params(None, None)
    assert params["page_size"] == 1000
    # No bookmark yet → floor is the configured start_date.
    assert params["date_after"] == "2026-01-01"


def test_lookback_clamps_to_start_date(tap: TapLeadGurus) -> None:
    # A bookmark only 2 days after start_date, minus 7-day lookback, must not
    # go earlier than start_date.
    assert LeadsStream(tap)._lookback_floor("2026-01-03") == "2026-01-01"


def test_lookback_subtracts_from_a_later_bookmark(tap: TapLeadGurus) -> None:
    # 7 days before a bookmark well past the start_date floor.
    assert LeadsStream(tap)._lookback_floor("2026-03-20T13:00:00-04:00") == "2026-03-13"


def test_lookback_floor_handles_no_bookmark(tap: TapLeadGurus) -> None:
    assert LeadsStream(tap)._lookback_floor(None) == "2026-01-01"


def test_summary_followup_page_replays_next_url_params(tap: TapLeadGurus) -> None:
    # Summaries use the base HATEOAS paginator: the server `next` URL is replayed.
    from urllib.parse import urlparse  # noqa: PLC0415

    stream = SummaryClientStream(tap)
    token = urlparse("https://x/api/v1/summary/client/?page=3&page_size=1000&date_after=2026-02-01")
    params = stream.get_url_params(None, token)
    assert params == {"page": "3", "page_size": "1000", "date_after": "2026-02-01"}


# ---------------------------------------------------------------------------
# Leads date-window paginator
# ---------------------------------------------------------------------------


def test_leads_first_window_caps_at_90_days(tap: TapLeadGurus) -> None:
    paginator = LeadsStream(tap).get_new_paginator()
    window = paginator.current_value
    span = date.fromisoformat(window["date_before"]) - date.fromisoformat(window["date_after"])
    assert window["page"] == 1
    assert span.days <= 90


def test_window_paginator_pages_then_advances() -> None:
    today = date.fromisoformat("2026-06-09")
    paginator = DateWindowPaginator(date.fromisoformat("2026-01-01"), today, window_days=90)
    first = paginator.current_value
    # A `next` link bumps the page within the same window.
    more = SimpleNamespace(json=lambda: {"next": "u", "results": []})
    nxt = paginator.get_next(more)
    assert nxt == {**first, "page": 2}
    # No `next` advances to the next window, starting at the prior window's end.
    done = SimpleNamespace(json=lambda: {"next": None, "results": []})
    advanced = paginator.get_next(done)
    assert advanced["date_after"] == first["date_before"]
    assert advanced["page"] == 1


def test_window_paginator_stops_at_end() -> None:
    today = date.fromisoformat("2026-02-15")
    paginator = DateWindowPaginator(date.fromisoformat("2026-01-01"), today, window_days=90)
    # Single window already reaches `end`; an exhausted page ends pagination.
    done = SimpleNamespace(json=lambda: {"next": None, "results": []})
    assert paginator.get_next(done) is None


# ---------------------------------------------------------------------------
# SDK standard suite — integration, needs a real key
# ---------------------------------------------------------------------------

if os.environ.get("TAP_LEADGURUS_API_KEY"):
    from singer_sdk.testing import get_tap_test_class

    TestTapLeadGurus = get_tap_test_class(
        tap_class=TapLeadGurus,
        config={
            "api_key": os.environ["TAP_LEADGURUS_API_KEY"],
            "start_date": os.environ.get("TAP_LEADGURUS_START_DATE", "2026-01-01"),
        },
    )
