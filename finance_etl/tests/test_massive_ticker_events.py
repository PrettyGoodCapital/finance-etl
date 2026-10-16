from __future__ import annotations

import gzip
import json
from datetime import date

from ccflow import Flow
from ccflow_etl import WeekdayCalendar
from ccflow_http import HTTPResult

from finance_etl.providers.massive import MassiveTickerEventsExtractModel, TickerEventsModel, diff_ticker_snapshots

PREVIOUS = [
    {"ticker": "AAA", "composite_figi": "BBG000000001", "name": "Steady Co"},
    {"ticker": "OLD", "composite_figi": "BBG000000002", "name": "Renamed Co"},
    {"ticker": "GONE", "composite_figi": "BBG000000003", "name": "Delisted Co"},
    {"ticker": "NOFIGI.WS", "name": "Warrant"},
]
CURRENT = [
    {"ticker": "AAA", "composite_figi": "BBG000000001", "name": "Steady Co"},
    {"ticker": "NEW", "composite_figi": "BBG000000002", "name": "Renamed Co"},
    {"ticker": "IPO", "composite_figi": "BBG000000004", "name": "Listed Co", "type": "CS"},
    {"ticker": "UNIT.U", "name": "Unit"},
]


class MemoryStore:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def artifact_uri(self, key):
        return f"s3://shared/{key}"

    def exists(self, key):
        return key in self.objects

    def read(self, key):
        return self.objects[key]

    def write(self, key, payload, media_type=None, metadata=None):
        self.objects[key] = payload
        return {"status": "written", "object": key}


def _snapshots():
    return {
        "massive/stocks/rest/all-tickers/2025-01-03.json": json.dumps({"results": PREVIOUS}).encode(),
        "massive/stocks/rest/all-tickers/2025-01-06.json": json.dumps({"results": CURRENT}).encode(),
    }


def test_diff_ticker_snapshots_classifies_changes():
    changes = {(change["composite_figi"], change["change"]): change for change in diff_ticker_snapshots(PREVIOUS, CURRENT)}

    assert set(changes) == {
        ("BBG000000002", "ticker_changed"),
        ("BBG000000003", "removed"),
        ("BBG000000004", "added"),
        (None, "added"),
        (None, "removed"),
    }
    assert changes["BBG000000002", "ticker_changed"]["tickers"] == ["NEW"]
    assert changes["BBG000000002", "ticker_changed"]["previous_tickers"] == ["OLD"]
    assert changes["BBG000000004", "added"]["type"] == "CS"
    assert changes[None, "added"]["tickers"] == ["UNIT.U"]
    assert changes[None, "removed"]["previous_tickers"] == ["NOFIGI.WS"]


def test_ticker_events_extract_requests_events_only_for_changed_figis():
    calls = []

    class FakeEventsModel(TickerEventsModel):
        @Flow.call
        def __call__(self, context):
            calls.append(context.identifier)
            if context.identifier == "BBG000000003":
                return HTTPResult(value={"status": "NOT_FOUND"}, status_code=404)
            events = [{"date": "2025-01-06", "type": "ticker_change", "ticker_change": {"ticker": "NEW"}}]
            return HTTPResult(value={"results": {"name": "Renamed Co", "events": events}}, status_code=200)

    store = MemoryStore(_snapshots())
    model = MassiveTickerEventsExtractModel(output=store, calendar=WeekdayCalendar(), events_model=FakeEventsModel())

    payload = model(["2025-01-06"]).value
    key = "massive/stocks/rest/ticker-events/jsonl/2025-01-06.jsonl.gz"
    records = [json.loads(line) for line in gzip.decompress(store.objects[key]).splitlines()]

    assert payload["status"] == "written"
    assert payload["previous_date"] == "2025-01-03"
    assert (payload["added_count"], payload["removed_count"], payload["ticker_changed_count"]) == (2, 2, 1)
    assert payload["requested_count"] == 3
    assert payload["not_found_count"] == 1
    assert sorted(calls) == ["BBG000000002", "BBG000000003", "BBG000000004"]
    by_figi = {record["composite_figi"]: record for record in records if record["composite_figi"]}
    assert by_figi["BBG000000002"]["events"][0]["ticker_change"]["ticker"] == "NEW"
    assert by_figi["BBG000000003"]["events_status"] == "not_found"
    assert all(record["date"] == "2025-01-06" for record in records)
    assert all("events_status" not in record for record in records if record["composite_figi"] is None)


def test_ticker_events_extract_explain_plans_both_snapshots_without_network():
    model = MassiveTickerEventsExtractModel(output=MemoryStore(), calendar=WeekdayCalendar(), explain=True)

    payload = model(["2025-01-06"]).value

    assert payload["status"] == "planned"
    assert payload["will_call_network"] is False
    assert payload["snapshot_keys"] == ["massive/stocks/rest/all-tickers/2025-01-03.json", "massive/stocks/rest/all-tickers/2025-01-06.json"]


def test_ticker_events_extract_skips_existing_output_and_missing_previous_snapshot():
    class FailingEventsModel(TickerEventsModel):
        @Flow.call
        def __call__(self, context):
            raise AssertionError("no events should be requested")

    existing = MemoryStore({**_snapshots(), "massive/stocks/rest/ticker-events/jsonl/2025-01-06.jsonl.gz": b""})
    model = MassiveTickerEventsExtractModel(output=existing, calendar=WeekdayCalendar(), events_model=FailingEventsModel())
    assert model(["2025-01-06"]).value["status"] == "exists"

    first = MemoryStore(_snapshots())
    model = MassiveTickerEventsExtractModel(output=first, calendar=WeekdayCalendar(), events_model=FailingEventsModel())
    payload = model(["2025-01-03"]).value
    assert payload["status"] == "no_previous_snapshot"
    assert payload["output_writes"] == []
    assert model.previous_session(date(2025, 1, 3)) == date(2025, 1, 2)
