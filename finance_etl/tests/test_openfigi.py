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


def test_isin_from_cusip_computes_check_digits():
    from finance_etl.providers.openfigi import isin_from_cusip

    assert isin_from_cusip("037833100") == "US0378331005"
    assert isin_from_cusip("G1169T138", "KY") == "KYG1169T1388"


def test_openfigi_cusip_capture_maps_cusips_then_isins_and_waits_for_the_master(tmp_path):
    from datetime import date

    import polars as pl
    from ccflow_etl import DatasetDefinition, LocalFileOutput

    from finance_etl.providers.openfigi import OpenFIGICusipCaptureModel

    day = date(2025, 1, 6)
    store = LocalFileOutput(path=tmp_path)
    snapshot = DatasetDefinition(name="snapshot", key_template="raw/tickers/{date}.json", store=store, format="json")
    master = DatasetDefinition(name="master", key_template="raw/master/{ymd}.parquet", store=store)
    output = DatasetDefinition(name="cusip-fills", key_template="raw/cusip/{date}.jsonl.gz", store=store, format="jsonl.gz")
    snapshot.write_bytes(day, json.dumps({"results": RECORDS}).encode())
    requests = []

    class FakeMappingModel(OpenFIGIMappingModel):
        @Flow.call
        def __call__(self, context):
            requests.append([(job["idType"], job["idValue"]) for job in context.json_body])
            known = {
                "038923876": {"figi": "BBG00PREF001", "name": "ARBOR REALTY TRUST", "securityType2": "Preferred Stock"},
                "KYG0R38M1018": {
                    "compositeFIGI": "BBG01AACI001",
                    "figi": "BBG01AACI001",
                    "name": "ARMADA ACQUISITION CORP III",
                    "securityType2": "Common Stock",
                },
            }
            return HTTPResult(
                value=[
                    {"data": [known[job["idValue"]]]} if job["idValue"] in known else {"warning": "No identifier found."} for job in context.json_body
                ],
                status_code=200,
            )

    model = OpenFIGICusipCaptureModel(mapping_model=FakeMappingModel(), snapshot=snapshot, cusips=master, output=output)
    assert model(day).value["status"] == "waiting"
    assert requests == []

    master.write_frame(day, pl.DataFrame({"CTA_SYMBOL": ["ABRpD", "AACI", "AAPL"], "CUSIP": ["038923876", "G0R38M101", "037833100"]}))
    result = model(day).value
    rows = {row["record"]["ticker"]: row for row in map(json.loads, gzip.decompress(output.read_bytes(day)).splitlines())}

    assert (result["status"], result["matched_count"], result["no_cusip_count"]) == ("written", 2, 2)
    assert rows["ABRpD"]["matches"][0]["compositeFIGI"] == "BBG00PREF001" and rows["ABRpD"]["matches"][0]["figiKind"] == "exchange"
    assert (rows["AACI"]["id_type"], rows["AACI"]["query"]) == ("ID_ISIN", "KYG0R38M1018")
    assert rows["AACI"]["matches"][0]["figiKind"] == "composite"
    assert rows["ZZZZ"]["status"] == "no_cusip" and "AAPL" not in rows
    assert model(day).value["status"] == "exists"


def test_openfigi_figi_capture_maps_only_newly_listed_securities(tmp_path):
    from datetime import date

    import polars as pl
    from ccflow_etl import DatasetDefinition, LocalFileOutput

    from finance_etl.providers.openfigi import OpenFIGIFigiCaptureModel

    store = LocalFileOutput(path=tmp_path)
    listings = DatasetDefinition(name="listings", key_template="curated/listings/{date}.parquet", store=store)
    output = DatasetDefinition(name="figi", key_template="raw/figi/{date}.jsonl.gz", store=store, format="jsonl.gz")
    listings.write_frame(date(2025, 1, 6), pl.DataFrame({"ticker": ["AAPL", "FB", "X"], "security_id": ["BBG1", "BBG2", None]}))
    listings.write_frame(date(2025, 1, 7), pl.DataFrame({"ticker": ["AAPL", "META", "NEW"], "security_id": ["BBG1", "BBG2", "BBG3"]}))
    requests = []

    class FakeMappingModel(OpenFIGIMappingModel):
        @Flow.call
        def __call__(self, context):
            requests.append([(job["idType"], job["idValue"]) for job in context.json_body])
            known = {"BBG1": {"ticker": "AAPL", "exchCode": "US"}, "BBG2": {"ticker": "META", "exchCode": "US"}}
            return HTTPResult(
                value=[
                    {"data": [known[job["idValue"]]]} if job["idValue"] in known else {"warning": "No identifier found."} for job in context.json_body
                ],
                status_code=200,
            )

    model = OpenFIGIFigiCaptureModel(mapping_model=FakeMappingModel(), listings=listings, output=output)

    first = model(date(2025, 1, 6)).value
    second = model(date(2025, 1, 7)).value
    rows = [json.loads(line) for line in gzip.decompress(output.read_bytes(date(2025, 1, 6))).splitlines()]

    assert (first["figi_count"], first["matched_count"], first["previous_date"]) == (2, 2, None)
    assert [(row["figi"], row["matches"][0]["ticker"]) for row in rows] == [("BBG1", "AAPL"), ("BBG2", "META")]
    assert (second["figi_count"], second["matched_count"], second["previous_date"]) == (1, 0, "2025-01-06")
    assert requests == [[("ID_BB_GLOBAL", "BBG1"), ("ID_BB_GLOBAL", "BBG2")], [("ID_BB_GLOBAL", "BBG3")]]
    assert model(date(2025, 1, 7)).value["status"] == "exists"
    assert model(date(2025, 1, 8)).value["status"] == "no_listings"
