from __future__ import annotations

import gzip
import json
from datetime import date

from ccflow import Flow
from ccflow_etl import DatasetDefinition, LocalFileOutput
from ccflow_http import HTTPRequestContext, HTTPResult, safe_request_dump
from pydantic import Field

from finance_etl.providers.fred import FREDCaptureModel, FREDHTTPModel, FREDObservationsModel


def test_fred_http_model_adds_json_format_and_redacted_api_key(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "fred-secret")

    request = FREDHTTPModel().build_request(HTTPRequestContext(path="/fred/series", query={"series_id": "DGS10"}))
    dumped = json.dumps(safe_request_dump(request))

    assert request.params == {"series_id": "DGS10", "file_type": "json", "api_key": "fred-secret"}
    assert "fred-secret" not in dumped


class FakeFRED(FREDHTTPModel):
    pages: dict = Field(default_factory=dict)

    @Flow.call
    def __call__(self, context):
        query = context.query
        if context.path == "/fred/series":
            return HTTPResult(
                value={"seriess": [{"id": query["series_id"], "title": f"{query['series_id']} title", "notes": "Public domain."}]}, status_code=200
            )
        observations = self.pages[(query["series_id"], query["realtime_start"])]
        offset = int(query["offset"])
        page = observations[offset : offset + 2]
        return HTTPResult(value={"count": len(observations), "offset": offset, "observations": page}, status_code=200)


def _observation(day, value, start, end="9999-12-31"):
    return {"date": day, "value": value, "realtime_start": start, "realtime_end": end}


def test_fred_capture_pages_observations_and_keeps_metadata_only_series(tmp_path):
    output = DatasetDefinition(name="fred", key_template="raw/fred/{date}.jsonl.gz", store=LocalFileOutput(path=tmp_path), format="jsonl.gz")
    observations = [
        _observation("2024-01-01", "1.0", "2024-02-01", "2024-02-28"),
        _observation("2024-01-01", "1.1", "2024-02-29"),
        _observation("2024-02-01", ".", "2024-03-01"),
    ]
    model = FREDCaptureModel(
        http_model=FakeFRED(pages={("PAYEMS", "1776-07-04"): observations}),
        series=["PAYEMS"],
        metadata_only=["UMCSENT"],
        observation_start=date(2021, 10, 11),
        output=output,
    )

    result = model(date(2026, 10, 27)).value
    rows = {row["series_id"]: row for row in map(json.loads, gzip.decompress(output.read_bytes(date(2026, 10, 27))).splitlines())}

    assert (result["status"], result["series_count"], result["observation_count"]) == ("written", 2, 3)
    assert rows["PAYEMS"]["observations"] == observations
    assert rows["UMCSENT"]["observations"] is None and rows["UMCSENT"]["metadata"]["notes"] == "Public domain."
    daily = model.model_copy(update={"realtime_lookback_days": 7, "http_model": FakeFRED(pages={("PAYEMS", "2026-10-20"): []})})
    assert daily.model_copy(update={"explain": True})(date(2026, 10, 27)).value["realtime_start"] == "2026-10-20"
    assert model(date(2026, 10, 27)).value["status"] == "exists"


def test_fred_observations_merge_baseline_and_daily_captures_into_vintages(tmp_path):
    store = LocalFileOutput(path=tmp_path)
    baseline = DatasetDefinition(name="base", key_template="raw/base/{date}.jsonl.gz", store=store, format="jsonl.gz")
    daily = DatasetDefinition(name="daily", key_template="raw/daily/{date}.jsonl.gz", store=store, format="jsonl.gz")
    output = DatasetDefinition(name="obs", key_template="curated/fred/latest.parquet", store=store)

    def write(dataset, day, observations):
        row = {"series_id": "PAYEMS", "observations": observations}
        dataset.write_bytes(day, gzip.compress(json.dumps(row).encode() + b"\n"))

    write(baseline, date(2026, 10, 20), [_observation("2026-09-01", "100", "2026-10-03"), _observation("2026-08-01", ".", "2026-09-05")])
    # The October release revises September, closing the vintage the baseline saw as current.
    write(daily, date(2026, 11, 7), [_observation("2026-09-01", "100", "2026-10-03", "2026-11-06"), _observation("2026-09-01", "101", "2026-11-07")])

    result = FREDObservationsModel(baseline=baseline, daily=daily, output=output)(date(2026, 11, 8)).value
    frame = output.read_frame(date(2026, 11, 8))

    assert (result["status"], result["row_count"], result["capture_count"]) == ("written", 3, 2)
    assert frame.select("date", "value", "valid_from", "valid_to").rows() == [
        (date(2026, 8, 1), None, date(2026, 9, 5), None),
        (date(2026, 9, 1), 100.0, date(2026, 10, 3), date(2026, 11, 6)),
        (date(2026, 9, 1), 101.0, date(2026, 11, 7), None),
    ]
    assert FREDObservationsModel(baseline=baseline, output=output, overwrite_output=True)(date(2026, 10, 1)).value["status"] == "no_baseline"
