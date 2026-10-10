import gzip
import json

import httpx2
from ccflow_etl import LocalFileOutput
from ccflow_http import HTTPConfig

from finance_etl import SECDailyIndexExtractModel, SECDailyIndexModel, SECDateContext, SECFilerRefreshBundleModel, SECHTTPModel, parse_master_index

MASTER = """Description:           Daily Index of EDGAR Dissemination Feed by Company Name
Last Data Received:    October 8, 2026

CIK|Company Name|Form Type|Date Filed|Filename
--------------------------------------------------------------------------------
320193|Apple Inc.|10-Q|20261008|edgar/data/320193/0000320193-26-000101.txt
789019|MICROSOFT CORP|8-K|20261008|edgar/data/789019/0000950170-26-000102.txt
1652044|Alphabet Inc.|10-Q|20261008|edgar/data/1652044/0001652044-26-000103.txt
"""
UA = "Example Research research@example.com"


def _transport(routes, seen):
    def handler(request):
        seen.append(request)
        status, body = routes.get(request.url.path, (404, ""))
        if isinstance(body, (dict, list)):
            return httpx2.Response(status, json=body)
        return httpx2.Response(status, text=body)

    return httpx2.MockTransport(handler)


def _index_extract(store, routes, seen):
    model = SECDailyIndexModel(
        config=HTTPConfig(base_url="https://www.sec.gov", transport=_transport(routes, seen)), user_agent=UA, execution_policy=None
    )
    return SECDailyIndexExtractModel(index_model=model, output=store)


def test_parse_master_index_reads_rows_after_the_separator():
    rows = parse_master_index(MASTER)

    assert [row["cik"] for row in rows] == ["320193", "789019", "1652044"]
    assert rows[0] == {
        "cik": "320193",
        "company_name": "Apple Inc.",
        "form_type": "10-Q",
        "date_filed": "20261008",
        "filename": "edgar/data/320193/0000320193-26-000101.txt",
    }


def test_daily_index_extract_requests_quarter_path_with_user_agent_and_stores_index(tmp_path):
    seen = []
    store = LocalFileOutput(path=tmp_path)
    extract = _index_extract(store, {"/Archives/edgar/daily-index/2026/QTR4/master.20261008.idx": (200, MASTER)}, seen)

    result = extract(SECDateContext(date="2026-10-08")).value
    again = extract(SECDateContext(date="2026-10-08")).value

    assert seen[0].headers["user-agent"] == UA
    assert result["status"] == "written"
    assert result["filing_count"] == 3
    assert store.file_path("sec/edgar/daily-index/2026/10/2026-10-08.idx").read_text() == MASTER
    assert again["status"] == "exists"
    assert len(seen) == 1


def test_daily_index_extract_records_unpublished_days_and_explains_without_io(tmp_path):
    seen = []
    store = LocalFileOutput(path=tmp_path / "store")

    missing = _index_extract(store, {}, seen)(SECDateContext(date="2026-10-10")).value
    planned = _index_extract(store, {}, seen).model_copy(update={"explain": True})(["2026-10-09"]).value

    assert missing["status"] == "not_published"
    assert not (tmp_path / "store").exists()
    assert planned["status"] == "planned"
    assert len(seen) == 1


def _bundle(tmp_path, seen):
    store = LocalFileOutput(path=tmp_path / "store")
    routes = {
        "/Archives/edgar/daily-index/2026/QTR4/master.20261008.idx": (200, MASTER),
        "/api/xbrl/companyfacts/CIK0000320193.json": (200, {"cik": 320193, "facts": {"us-gaap": {}}}),
        "/submissions/CIK0000320193.json": (200, {"cik": "320193", "name": "Apple Inc."}),
        "/submissions/CIK0001652044.json": (200, {"cik": "1652044", "name": "Alphabet Inc."}),
    }
    transport = _transport(routes, seen)

    def http(base):
        return SECHTTPModel(config=HTTPConfig(base_url=base, transport=transport), user_agent=UA, execution_policy=None)

    model = SECFilerRefreshBundleModel(
        index_extract=_index_extract(store, routes, seen),
        facts_model=http("https://data.sec.gov"),
        submissions_model=http("https://data.sec.gov"),
        output=store,
        staging_directory=tmp_path / "staging",
    )
    return model, store


def test_filer_refresh_bundles_financial_filers_from_the_daily_index(tmp_path):
    seen = []
    model, store = _bundle(tmp_path, seen)
    context = SECDateContext(date="2026-10-08")

    deps = model.__deps__(context)
    result = model(context).value
    records = [
        json.loads(line) for line in gzip.decompress(store.file_path("sec/edgar/filer-refresh/jsonl/2026-10-08.jsonl.gz").read_bytes()).splitlines()
    ]

    assert [dep_model for dep_model, _ in deps] == [model.index_extract]
    assert result["status"] == "written"
    assert result["filer_count"] == 2
    assert [(record["cik"], record["companyfacts_status"], record["submissions_status"]) for record in records] == [
        ("320193", 200, 200),
        ("1652044", 404, 200),
    ]
    assert records[0]["filings"] == [{"form_type": "10-Q", "filename": "edgar/data/320193/0000320193-26-000101.txt"}]
    assert all(request.headers["user-agent"] == UA for request in seen)
    assert model.__deps__(context) == []
    assert not list((tmp_path / "staging").iterdir())


def test_filer_refresh_resumes_staged_records(tmp_path):
    seen = []
    model, _ = _bundle(tmp_path, seen)
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "2026-10-08.jsonl.partial").write_text(json.dumps({"cik": "320193", "date": "2026-10-08"}) + "\n" + '{"cik": "16520')

    model(SECDateContext(date="2026-10-08"))

    fetched = [request.url.path for request in seen if "CIK" in request.url.path]
    assert fetched == ["/api/xbrl/companyfacts/CIK0001652044.json", "/submissions/CIK0001652044.json"]
