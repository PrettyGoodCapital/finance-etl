from __future__ import annotations

import gzip
import json

from ccflow import Flow
from ccflow_http import HTTPRequestContext, HTTPResult, RedactionPolicy, safe_request_dump

from finance_etl.providers.openfigi import OpenFIGIMappingCaptureModel, OpenFIGIMappingModel, openfigi_ticker_queries

SNAPSHOT_KEY = "massive/stocks/rest/all-tickers/2025-01-06.json"
RECORDS = [
    {"ticker": "AAPL", "composite_figi": "BBG000B9XRY4", "type": "CS", "name": "Apple Inc."},
    {"ticker": "AACI", "type": "CS", "name": "Armada Acquisition Corp. III"},
    {"ticker": "AAC.WS", "type": "WARRANT", "name": "Ares Acquisition Corporation III Warrants"},
    {"ticker": "ABRpD", "type": "PFD", "name": "Arbor Realty Trust Series D Preferred"},
    {"ticker": "ZZZZ", "type": "CS", "name": "Unknown Corp"},
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


def test_openfigi_mapping_model_sends_api_key_header_and_redacts_it(monkeypatch):
    monkeypatch.setenv("OPENFIGI_API_KEY", "openfigi-secret")

    request = OpenFIGIMappingModel().build_request(HTTPRequestContext(json_body=[{"idType": "TICKER", "idValue": "AAPL"}]))

    assert request.method == "POST"
    assert request.headers["X-OPENFIGI-APIKEY"] == "openfigi-secret"
    assert "openfigi-secret" not in json.dumps(safe_request_dump(request))
    assert OpenFIGIMappingModel().redaction.with_names().is_sensitive("x-openfigi-apikey")
    assert RedactionPolicy(substrings=set()).with_names("X-OPENFIGI-APIKEY").is_sensitive("X-OpenFIGI-ApiKey")


def test_openfigi_ticker_queries_convert_exchange_symbols():
    assert openfigi_ticker_queries("AAC.WS") == ["AAC/WS", "AAC.WS"]
    assert openfigi_ticker_queries("BRK.A") == ["BRK/A", "BRK.A"]
    assert openfigi_ticker_queries("AIIAr") == ["AIIA/R", "AIIAr"]
    assert openfigi_ticker_queries("AACIW") == ["AACIW"]


def test_openfigi_capture_looks_up_records_without_figi_and_tries_fallback_forms():
    requests = []
    known = {"AACI": "BBG01AAAAAA1", "AAC.WS": "BBG01BBBBBB2"}

    class FakeMappingModel(OpenFIGIMappingModel):
        @Flow.call
        def __call__(self, context):
            jobs = context.json_body
            requests.append([job["idValue"] for job in jobs])
            responses = []
            for job in jobs:
                figi = known.get(job["idValue"])
                responses.append({"data": [{"compositeFIGI": figi, "ticker": job["idValue"]}]} if figi else {"warning": "No identifier found."})
            return HTTPResult(value=responses, status_code=200)

    store = MemoryStore({SNAPSHOT_KEY: json.dumps({"results": RECORDS}).encode()})
    model = OpenFIGIMappingCaptureModel(output=store, mapping_model=FakeMappingModel())

    payload = model("2025-01-06").value
    rows = {
        row["record"]["ticker"]: row
        for row in map(json.loads, gzip.decompress(store.objects["openfigi/mapping/jsonl/2025-01-06.jsonl.gz"]).splitlines())
    }

    assert payload["status"] == "written"
    assert (payload["record_count"], payload["matched_count"], payload["no_match_count"], payload["skipped_count"]) == (4, 2, 1, 1)
    assert "AAPL" not in rows
    assert requests == [["AAC/WS", "AACI", "ZZZZ"], ["AAC.WS"]]
    assert rows["AAC.WS"]["query"] == "AAC.WS" and rows["AAC.WS"]["queries"] == ["AAC/WS", "AAC.WS"]
    assert rows["AACI"]["matches"][0]["compositeFIGI"] == "BBG01AAAAAA1"
    assert rows["ABRpD"]["status"] == "skipped" and rows["ABRpD"]["queries"] == []
    assert rows["ZZZZ"]["status"] == "no_match"
    assert all(row["date"] == "2025-01-06" and row["captured_at"] for row in rows.values())


def test_openfigi_capture_batches_jobs_per_request():
    sizes = []

    class FakeMappingModel(OpenFIGIMappingModel):
        @Flow.call
        def __call__(self, context):
            sizes.append(len(context.json_body))
            return HTTPResult(value=[{"warning": "No identifier found."}] * len(context.json_body), status_code=200)

    records = [{"ticker": f"T{index:03d}", "type": "CS"} for index in range(250)]
    store = MemoryStore({SNAPSHOT_KEY: json.dumps({"results": records}).encode()})

    OpenFIGIMappingCaptureModel(output=store, mapping_model=FakeMappingModel())("2025-01-06")

    assert sizes == [100, 100, 50]


def test_openfigi_capture_explain_and_existing_output_skip_network():
    class FailingMappingModel(OpenFIGIMappingModel):
        @Flow.call
        def __call__(self, context):
            raise AssertionError("no mapping requests expected")

    explain = OpenFIGIMappingCaptureModel(output=MemoryStore(), explain=True, mapping_model=FailingMappingModel())("2025-01-06").value
    assert explain["status"] == "planned"
    assert explain["snapshot_key"] == SNAPSHOT_KEY

    existing = MemoryStore({"openfigi/mapping/jsonl/2025-01-06.jsonl.gz": b""})
    assert OpenFIGIMappingCaptureModel(output=existing, mapping_model=FailingMappingModel())("2025-01-06").value["status"] == "exists"
