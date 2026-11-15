from __future__ import annotations

import gzip
import json
from datetime import date

import pytest
from ccflow import Flow
from ccflow_etl import (
    APIKeySecretCredentials,
    DatasetDefinition,
    NoOpArtifactStore,
)
from ccflow_http import HTTPResult
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from finance_etl.providers.massive import (
    ConditionsModel,
    DailyAggregateContext,
    DailyAggregateModel,
    DailyMarketSummaryContext,
    DailyMarketSummaryModel,
    DailyTickerSummaryContext,
    DailyTickerSummaryModel,
    ExchangesModel,
    MarketCalendarContext,
    MarketCalendarModel,
    MarketHolidaysModel,
    MassiveAllTickersContext,
    MassiveAllTickersModel,
    MassiveCredentials,
    MassiveDailyMarketSummaryExtractModel,
    MassiveDatedReferenceContext,
    MassiveDatedReferenceExtractModel,
    MassiveDatedReferenceModel,
    MassiveDatedSymbolUniverseModel,
    MassiveFlatFileTransferModel,
    MassiveReferenceSnapshotContext,
    MassiveReferenceSnapshotExtractModel,
    MassiveTickerOverviewBundleExtractModel,
    TickerOverviewContext,
    TickerOverviewModel,
    TickersContext,
    TickersModel,
    TickerUniversePlanContext,
    TickerUniversePlanModel,
)
from finance_etl.symbols import ExplicitSymbolUniverseModel


class RecordingArtifactOutput:
    def __init__(self):
        self.writes = []

    def artifact_uri(self, key):
        return f"s3://shared/{key}"

    def exists(self, key):
        return False

    def write(self, key, payload, media_type=None, metadata=None):
        self.writes.append({"key": key, "payload": payload, "media_type": media_type, "metadata": metadata})
        return {"status": "written", "object": key}


def _dataset(store, key_template, name="test-dataset", format="json", **kwargs) -> DatasetDefinition:
    return DatasetDefinition(name=name, key_template=key_template, store=store, format=format, **kwargs)


SUMMARY_KEYS = "massive/stocks/rest/daily-market-summary/json/{date}.json"
OVERVIEW_KEYS = "massive/stocks/rest/ticker-overview/jsonl/{date}.jsonl.gz"
ALL_TICKERS_KEYS = "massive/stocks/rest/all-tickers/{date}.json"


def _flat_files(store, flat_file):
    return _dataset(
        store, f"massive/stocks/flat-files/{flat_file}/{{year}}/{{month}}/{{date}}.csv.gz", f"massive-stocks-flat-files-{flat_file}", "csv.gz"
    )


def test_massive_credentials_can_supply_api_key_without_environment(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    request = DailyAggregateModel(credentials=MassiveCredentials(token="configured-secret")).build_request(
        DailyAggregateContext(ticker="AAA", date="2024-01-03", adjusted=True)
    )

    assert request.params["apiKey"] == "configured-secret"


def test_massive_credentials_config_registers_rest_and_flat_file_credentials(tmp_path):
    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - credentials: massive

hydra:
    searchpath:
        - pkg://finance_etl.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    assert isinstance(instantiate(cfg.credentials.massive), MassiveCredentials)
    flat_file_credentials = instantiate(cfg.credentials.massive_flat_files)
    assert isinstance(flat_file_credentials, APIKeySecretCredentials)
    assert flat_file_credentials.api_key_env == "MASSIVE_API_KEY_ID"
    assert flat_file_credentials.secret_key_env == "MASSIVE_API_KEY"

    (tmp_path / "indexed.yaml").write_text(
        """
defaults:
    - _self_
    - credentials: /credentials/providers/massive/rest

hydra:
    searchpath:
        - pkg://finance_etl.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        indexed_cfg = compose(config_name="indexed")

    assert isinstance(instantiate(indexed_cfg.credentials.providers.massive.rest), MassiveCredentials)
    assert indexed_cfg.credentials.providers.massive.rest.token is None
    assert indexed_cfg.credentials.providers.massive.rest.token_env == "MASSIVE_API_KEY"

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        override_cfg = compose(config_name="indexed", overrides=["credentials.providers.massive.rest.token=configured-secret"])

    assert instantiate(override_cfg.credentials.providers.massive.rest).token == "configured-secret"


def test_massive_market_metadata_models_build_expected_requests(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    holidays = MarketHolidaysModel().build_request()
    exchanges = ExchangesModel().build_request()
    conditions = ConditionsModel().build_request()
    tickers = TickersModel().build_request()

    assert holidays.url == "/v1/marketstatus/upcoming"
    assert holidays.params == {"apiKey": "secret"}
    assert exchanges.url == "/v3/reference/exchanges"
    assert exchanges.params == {"asset_class": "stocks", "locale": "us", "apiKey": "secret"}
    assert conditions.url == "/v3/reference/conditions"
    assert conditions.params == {"asset_class": "stocks", "limit": 1000, "apiKey": "secret"}
    assert tickers.url == "/v3/reference/tickers"
    assert tickers.params == {"market": "stocks", "active": True, "limit": 1000, "apiKey": "secret"}


def test_massive_tickers_model_validates_and_builds_supported_filters(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = TickersModel().build_request(
        TickersContext(
            active_date="2025-01-02",
            ticker=" AAPL ",
            ticker_type=" CS ",
            market="stocks",
            exchange="XNYS",
            order="asc",
            sort="ticker",
            limit=500,
        )
    )

    assert request.params == {
        "ticker": "AAPL",
        "type": "CS",
        "market": "stocks",
        "exchange": "XNYS",
        "active": True,
        "order": "asc",
        "limit": 500,
        "sort": "ticker",
        "date": "2025-01-02",
        "apiKey": "secret",
    }
    with pytest.raises(ValueError):
        TickersContext(market="bonds")  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError):
        TickersContext(limit=1001)


def test_massive_daily_aggregate_model_builds_ticker_date_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = DailyAggregateModel().build_request(DailyAggregateContext(ticker="AAA", date="2024-01-03", adjusted=True))

    assert request.url == "/v2/aggs/ticker/AAA/range/1/day/2024-01-03/2024-01-03"
    assert request.params == {"adjusted": True, "sort": "asc", "limit": 50000, "apiKey": "secret"}


def test_massive_ticker_overview_model_builds_ticker_date_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = TickerOverviewModel().build_request(TickerOverviewContext(ticker="AAPL", date="2025-01-02"))

    assert request.url == "/v3/reference/tickers/AAPL"
    assert request.params == {"date": "2025-01-02", "apiKey": "secret"}


def test_massive_daily_market_summary_model_builds_date_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = DailyMarketSummaryModel().build_request(DailyMarketSummaryContext(date="2025-01-02", adjusted=True, include_otc=False))

    assert request.url == "/v2/aggs/grouped/locale/us/market/stocks/2025-01-02"
    assert request.params == {"adjusted": True, "include_otc": False, "apiKey": "secret"}


def test_massive_daily_ticker_summary_model_builds_open_close_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = DailyTickerSummaryModel().build_request(DailyTickerSummaryContext(ticker="AAPL", date="2025-01-02", adjusted=True))

    assert request.url == "/v1/open-close/AAPL/2025-01-02"
    assert request.params == {"adjusted": True, "apiKey": "secret"}


def test_massive_daily_market_summary_extract_explain_plans_output_write(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyMarketSummaryExtractModel(dataset=_dataset(NoOpArtifactStore(), SUMMARY_KEYS), explain=True)(["2025-01-02"]).value

    assert payload["status"] == "planned"
    assert payload["request"]["url"] == "/v2/aggs/grouped/locale/us/market/stocks/2025-01-02"
    assert payload["request"]["params"] == {"adjusted": True, "include_otc": False}
    assert payload["key"] == "massive/stocks/rest/daily-market-summary/json/2025-01-02.json"
    assert payload["uri"] == "noop://artifact/massive/stocks/rest/daily-market-summary/json/2025-01-02.json"


@pytest.mark.parametrize(
    ("model", "context", "partitions"),
    [
        (
            MassiveDailyMarketSummaryExtractModel(dataset=_dataset(NoOpArtifactStore(), SUMMARY_KEYS), explain=True),
            DailyMarketSummaryContext(date="2025-01-02"),
            ["date"],
        ),
    ],
)
def test_massive_extract_explain_schema_includes_dataset_and_base_models(monkeypatch, model, context, partitions):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = model(context).value

    assert payload["dataset_metadata"]["name"] == payload["dataset"]
    assert payload["dataset_metadata"]["partition_keys"] == partitions
    assert payload["base_models"]["http"] == "ccflow_http.HTTPModel"
    assert payload["base_models"]["request_model"].startswith("finance_etl.providers.massive.")
    assert payload["base_models"]["storage"] == ["ccflow_s3.S3ArtifactStore"]


def test_massive_ticker_overview_bundle_explain_plans_one_daily_output(tmp_path):
    output = RecordingArtifactOutput()
    model = MassiveTickerOverviewBundleExtractModel(
        universe_model=ExplicitSymbolUniverseModel(symbols=["AAPL", "MSFT"]),
        dataset=_dataset(output, OVERVIEW_KEYS, format="jsonl.gz"),
        staging_directory=tmp_path,
        explain=True,
    )

    payload = model(["2025-01-02"]).value

    assert payload["status"] == "planned"
    assert payload["will_call_network"] is False
    assert payload["key"] == "massive/stocks/rest/ticker-overview/jsonl/2025-01-02.jsonl.gz"
    assert payload["request_template"]["url"] == "/v3/reference/tickers/{ticker}"
    assert payload["dataset_metadata"]["partition_keys"] == ["date"]
    assert output.writes == []


def test_massive_ticker_overview_bundle_declares_universe_dependency_only_when_fetching(tmp_path):
    class ExistingOutput(RecordingArtifactOutput):
        def exists(self, key):
            return True

    universe = ExplicitSymbolUniverseModel(symbols=["AAPL"])
    context = MassiveAllTickersContext(date="2025-01-02")

    def bundle(store, **kwargs):
        return MassiveTickerOverviewBundleExtractModel(
            universe_model=universe, staging_directory=tmp_path, dataset=_dataset(store, OVERVIEW_KEYS, format="jsonl.gz"), **kwargs
        )

    deps = bundle(RecordingArtifactOutput()).__deps__(context)

    assert [(model, [ctx.date.isoformat() for ctx in contexts]) for model, contexts in deps] == [(universe, ["2025-01-02"])]
    assert bundle(ExistingOutput()).__deps__(context) == []
    assert bundle(ExistingOutput(), overwrite_output=True).__deps__(context) != []
    assert bundle(RecordingArtifactOutput(), explain=True).__deps__(context) == []


def test_massive_ticker_overview_bundle_skips_existing_daily_output(tmp_path):
    class ExistingOutput(RecordingArtifactOutput):
        def exists(self, key):
            return True

    class UnexpectedOverviewModel(TickerOverviewModel):
        @Flow.call
        def __call__(self, context):
            raise AssertionError("existing daily output must skip Massive requests")

    output = ExistingOutput()
    model = MassiveTickerOverviewBundleExtractModel(
        universe_model=ExplicitSymbolUniverseModel(symbols=["AAPL"]),
        overview_model=UnexpectedOverviewModel(),
        dataset=_dataset(output, OVERVIEW_KEYS, format="jsonl.gz"),
        staging_directory=tmp_path,
    )

    payload = model(["2025-01-02"]).value

    assert payload["status"] == "exists"
    assert payload["will_call_network"] is False
    assert payload["will_publish_output"] is False
    assert output.writes == []


def test_massive_ticker_overview_bundle_resumes_staging_and_uploads_once(tmp_path):
    calls = []

    class FakeOverviewModel(TickerOverviewModel):
        @Flow.call
        def __call__(self, context):
            calls.append(context.ticker)
            if context.ticker == "MSFT":
                raise RuntimeError("HTTP GET /v3/reference/tickers/MSFT failed with status 404")
            return HTTPResult(value={"results": {"ticker": context.ticker, "name": f"{context.ticker} Inc."}}, status_code=200)

    class RecordingFileOutput(RecordingArtifactOutput):
        def write_file(self, key, path, media_type=None, metadata=None):
            self.writes.append({"key": key, "payload": path.read_bytes(), "media_type": media_type, "metadata": metadata})
            return {"status": "written", "object": key, "size": path.stat().st_size}

    partial_path = tmp_path / "2025-01-02.jsonl.partial"
    partial_path.write_text(
        json.dumps(
            {
                "date": "2025-01-02",
                "ticker": "AAPL",
                "status": "ok",
                "status_code": 200,
                "attempts": 1,
                "response": {"results": {"ticker": "AAPL", "name": "Apple Inc."}},
            }
        )
        + "\n"
    )
    output = RecordingFileOutput()
    model = MassiveTickerOverviewBundleExtractModel(
        universe_model=ExplicitSymbolUniverseModel(symbols=["MSFT", "AAPL", "NVDA"]),
        overview_model=FakeOverviewModel(),
        dataset=_dataset(output, OVERVIEW_KEYS, format="jsonl.gz"),
        staging_directory=tmp_path,
        max_concurrency=2,
    )

    payload = model(["2025-01-02"]).value
    records = [json.loads(line) for line in gzip.decompress(output.writes[0]["payload"]).splitlines()]

    assert payload["status"] == "written"
    assert payload["ticker_count"] == 3
    assert payload["requested_count"] == 2
    assert payload["resumed_count"] == 1
    assert payload["ok_count"] == 2
    assert payload["not_found_count"] == 1
    assert calls == ["MSFT", "NVDA"]
    assert len(output.writes) == 1
    assert output.writes[0]["key"] == "massive/stocks/rest/ticker-overview/jsonl/2025-01-02.jsonl.gz"
    assert output.writes[0]["media_type"] == "application/gzip"
    assert [(record["ticker"], record["status"]) for record in records] == [
        ("AAPL", "ok"),
        ("MSFT", "not_found"),
        ("NVDA", "ok"),
    ]
    assert not partial_path.exists()
    assert not (tmp_path / "2025-01-02.jsonl.gz").exists()


def test_massive_ticker_overview_bundle_records_accepted_404_result_as_not_found(tmp_path):
    class FakeOverviewModel(TickerOverviewModel):
        @Flow.call
        def __call__(self, context):
            if context.ticker == "MSFT":
                return HTTPResult(value=None, status_code=404, attempts=2)
            return HTTPResult(value={"results": {"ticker": context.ticker}}, status_code=200)

    class RecordingFileOutput(RecordingArtifactOutput):
        def write_file(self, key, path, media_type=None, metadata=None):
            self.writes.append({"key": key, "payload": path.read_bytes(), "media_type": media_type, "metadata": metadata})
            return {"status": "written", "object": key, "size": path.stat().st_size}

    output = RecordingFileOutput()
    model = MassiveTickerOverviewBundleExtractModel(
        universe_model=ExplicitSymbolUniverseModel(symbols=["AAPL", "MSFT"]),
        overview_model=FakeOverviewModel(),
        dataset=_dataset(output, OVERVIEW_KEYS, format="jsonl.gz"),
        staging_directory=tmp_path,
    )

    payload = model(["2025-01-02"]).value
    records = [json.loads(line) for line in gzip.decompress(output.writes[0]["payload"]).splitlines()]

    assert payload["ok_count"] == 1
    assert payload["not_found_count"] == 1
    assert [(record["ticker"], record["status"], record["status_code"], record["attempts"]) for record in records] == [
        ("AAPL", "ok", 200, 1),
        ("MSFT", "not_found", 404, 2),
    ]


def test_massive_daily_market_summary_extract_writes_raw_payload():
    class FakeMarketSummaryModel(DailyMarketSummaryModel):
        @Flow.call
        def __call__(self, context):
            return HTTPResult(value={"status": "OK", "results": [{"T": "AAPL", "c": 42.0}]}, status_code=200)

    output = RecordingArtifactOutput()
    payload = MassiveDailyMarketSummaryExtractModel(market_summary_model=FakeMarketSummaryModel(), dataset=_dataset(output, SUMMARY_KEYS))(
        DailyMarketSummaryContext(date="2025-01-02")
    ).value

    assert payload["status"] == "written"
    assert output.writes[0]["key"] == "massive/stocks/rest/daily-market-summary/json/2025-01-02.json"
    assert output.writes[0]["media_type"] == "application/json"
    assert output.writes[0]["metadata"] == {"date": "2025-01-02", "provider": "massive", "market": "stocks", "adjusted": True, "include_otc": False}
    assert json.loads(output.writes[0]["payload"]) == {"results": [{"T": "AAPL", "c": 42.0}], "status": "OK"}


def test_massive_market_calendar_uses_provider_closed_holidays():
    context = MarketCalendarContext(
        start_date="2024-01-01",
        end_date="2024-01-05",
        exchange="NYSE",
        holidays=[
            {"date": "2024-01-01", "status": "closed", "exchange": "NYSE"},
            {"date": "2024-01-04", "status": "early-close", "exchange": "NYSE"},
            {"date": "2024-01-05", "status": "closed", "exchange": "LSE"},
        ],
    )

    result = MarketCalendarModel()(context)

    assert result.value == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def test_massive_market_calendar_accepts_wrapped_holiday_response():
    context = MarketCalendarContext(
        start_date="2024-01-01",
        end_date="2024-01-03",
        holidays={
            "status": "OK",
            "results": [
                {"date": "2024-01-01", "status": "closed", "exchange": "NYSE"},
            ],
        },
    )

    result = MarketCalendarModel()(context)

    assert result.value == [date(2024, 1, 2), date(2024, 1, 3)]


def test_massive_market_calendar_closes_new_years_day_without_provider_holiday_row():
    context = MarketCalendarContext(
        start_date="2024-12-31",
        end_date="2025-01-02",
        exchange="NYSE",
        holidays=[{"date": "1900-01-01", "status": "closed", "exchange": "NYSE"}],
    )

    result = MarketCalendarModel()(context)

    assert result.value == [date(2024, 12, 31), date(2025, 1, 2)]


def test_massive_market_calendar_filters_unknown_exchange_metadata():
    context = MarketCalendarContext(
        start_date="2024-01-02",
        end_date="2024-01-03",
        exchange="XLON",
        exchanges={"results": [{"mic": "XNYS", "operating_mic": "XNYS", "locale": "us", "asset_class": "stocks"}]},
    )

    result = MarketCalendarModel()(context)

    assert result.value == []


def test_massive_ticker_universe_plan_builds_date_specific_requests(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    context = TickerUniversePlanContext(session_dates=[date(2024, 1, 2), date(2024, 1, 3)])

    result = TickerUniversePlanModel().plan_requests(context)

    assert [request.url for request in result.value] == ["/v3/reference/tickers", "/v3/reference/tickers"]
    assert [request.params for request in result.value] == [
        {"market": "stocks", "active": True, "limit": 1000, "date": "2024-01-02", "apiKey": "secret"},
        {"market": "stocks", "active": True, "limit": 1000, "date": "2024-01-03", "apiKey": "secret"},
    ]


def test_massive_dated_symbol_universe_explain_plans_ticker_request(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDatedSymbolUniverseModel(exchange="XNYS", ticker_type="CS", explain=True)(["2025-01-02"]).value

    assert payload["status"] == "planned"
    assert payload["will_call_network"] is False
    assert payload["request"]["url"] == "/v3/reference/tickers"
    assert payload["request"]["params"] == {
        "market": "stocks",
        "active": True,
        "limit": 1000,
        "type": "CS",
        "exchange": "XNYS",
        "sort": "ticker",
        "date": "2025-01-02",
    }


def test_massive_dated_symbol_universe_fetches_normalized_symbols():
    calls = []

    class FakeTickersModel(TickersModel):
        @Flow.call
        def __call__(self, context):
            calls.append((context, self.max_pages))
            return HTTPResult(
                value={"results": [{"ticker": "msft"}, {"ticker": "AAPL"}, {"ticker": "AAPL"}, {"name": "missing"}]},
                status_code=200,
                pages=2,
            )

    payload = MassiveDatedSymbolUniverseModel(
        tickers_model=FakeTickersModel(),
        exchange="XNYS",
        max_pages=7,
        max_symbols=2,
    )(["2025-01-02"]).value

    assert payload.as_of_date == date(2025, 1, 2)
    assert payload.symbols == ["AAPL", "MSFT"]
    assert payload.source == "massive-stocks-rest-tickers"
    assert payload.metadata["ticker_count"] == 2
    assert payload.metadata["pages"] == 2
    assert calls[0][0].active_date == date(2025, 1, 2)
    assert calls[0][0].exchange == "XNYS"
    assert calls[0][1] == 7


def test_massive_flat_file_transfer_explain_plans_stock_source_and_output():
    payload = MassiveFlatFileTransferModel(flat_file="trades", dataset=_flat_files(NoOpArtifactStore(), "trades"), explain=True)(["2025-11-05"]).value

    assert payload["dataset"] == "massive-stocks-flat-files-trades"
    assert payload["source_key"] == "us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz"
    assert payload["source_uri"] == "s3://flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz"
    assert payload["key"] == "massive/stocks/flat-files/trades/2025/11/2025-11-05.csv.gz"
    assert payload["will_download"] is False
    assert payload["required_env"] == ["MASSIVE_API_KEY_ID", "MASSIVE_API_KEY"]


def test_massive_flat_file_transfer_downloads_and_writes_output(tmp_path):
    class FakeBody:
        def read(self):
            return b"csv-gzip-bytes"

    class FakeSourceClient:
        def __init__(self):
            self.calls = []

        def get_object(self, Bucket, Key):
            self.calls.append({"Bucket": Bucket, "Key": Key})
            return {"Body": FakeBody()}

    class FakeOutput:
        def __init__(self):
            self.writes = []

        def artifact_uri(self, key):
            return f"s3://shared/{key}"

        def exists(self, key):
            return False

        def write_file(self, key, path, media_type=None, metadata=None):
            self.writes.append({"key": key, "body": path.read_bytes(), "media_type": media_type, "metadata": metadata})
            return {"status": "written", "object": key}

    source_client = FakeSourceClient()
    output = FakeOutput()
    model = MassiveFlatFileTransferModel(flat_file="quotes", source_client=source_client, dataset=_flat_files(output, "quotes"), local_dir=tmp_path)

    payload = model(["2025-11-05"]).value

    assert payload["status"] == "written"
    assert source_client.calls == [{"Bucket": "flatfiles", "Key": "us_stocks_sip/quotes_v1/2025/11/2025-11-05.csv.gz"}]
    assert output.writes[0]["key"] == "massive/stocks/flat-files/quotes/2025/11/2025-11-05.csv.gz"
    assert output.writes[0]["body"] == b"csv-gzip-bytes"
    assert output.writes[0]["media_type"] == "application/gzip"
    assert not model.local_path(model.context_type.model_validate(["2025-11-05"])).exists()


def test_massive_flat_file_transfer_records_unavailable_dates_when_asked(tmp_path):
    import pytest

    class Forbidden(Exception):
        def __init__(self):
            super().__init__("Forbidden")
            self.response = {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}}

    class FakeSourceClient:
        def get_object(self, Bucket, Key):
            raise Forbidden()

    class FakeOutput:
        def exists(self, key):
            return False

        def write_file(self, key, path, media_type=None, metadata=None):
            raise AssertionError("unavailable dates must not be written")

    model = MassiveFlatFileTransferModel(
        flat_file="trades", source_client=FakeSourceClient(), dataset=_flat_files(FakeOutput(), "trades"), local_dir=tmp_path
    )

    with pytest.raises(Forbidden):
        model(["2019-11-05"])
    payload = model.model_copy(update={"fail_on_unavailable": False})(["2019-11-05"]).value
    assert (payload["status"], payload["error_code"]) == ("unavailable", "403")
    assert not list(tmp_path.rglob("*.csv.gz"))


def test_massive_all_tickers_explain_plans_single_json_output(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    dataset = _dataset(NoOpArtifactStore(), ALL_TICKERS_KEYS, "massive-stocks-rest-all-tickers")
    payload = MassiveAllTickersModel(dataset=dataset, explain=True)(["2025-01-02"]).value

    assert payload["dataset"] == "massive-stocks-rest-all-tickers"
    assert payload["status"] == "planned"
    assert payload["ticker_request"]["url"] == "/v3/reference/tickers"
    assert payload["ticker_request"]["params"] == {"market": "stocks", "active": True, "limit": 1000, "date": "2025-01-02"}
    assert payload["key"] == "massive/stocks/rest/all-tickers/2025-01-02.json"
    assert payload["uri"] == "noop://artifact/massive/stocks/rest/all-tickers/2025-01-02.json"
    assert payload["provider_metadata"]["retry"]["retry_status_codes"] == [429, 500, 502, 503, 504]


def test_massive_all_tickers_writes_combined_raw_json():
    class FakeTickersModel(TickersModel):
        @Flow.call
        def __call__(self, context):
            return HTTPResult(
                value={"status": "OK", "count": 2, "results": [{"ticker": "AAA"}, {"ticker": "BBB"}]},
                status_code=200,
                url="https://api.massive.com/v3/reference/tickers",
                attempts=3,
                pages=2,
                rate_limit={"x-ratelimit-remaining": "10"},
                retry_events=[{"attempt": 1, "outcome": "retry", "status_code": 429}],
                retry_summary={"attempts": 3, "retried": 1, "failed": 0, "succeeded": 1},
            )

    class FakeOutput:
        def __init__(self):
            self.writes = []

        def artifact_uri(self, key):
            return f"s3://shared/{key}"

        def exists(self, key):
            return False

        def write(self, key, payload, media_type=None, metadata=None):
            self.writes.append({"key": key, "payload": payload, "media_type": media_type, "metadata": metadata})
            return {"status": "written", "object": key}

    output = FakeOutput()
    payload = MassiveAllTickersModel(tickers_model=FakeTickersModel(), dataset=_dataset(output, ALL_TICKERS_KEYS))(
        MassiveAllTickersContext(date="2025-01-02")
    ).value

    assert payload["status"] == "written"
    assert payload["ticker_count"] == 2
    assert payload["page_count"] == 2
    assert payload["retry_summary"] == {"attempts": 3, "retried": 1, "failed": 0, "succeeded": 1}
    assert output.writes[0]["key"] == "massive/stocks/rest/all-tickers/2025-01-02.json"
    assert output.writes[0]["media_type"] == "application/json"
    assert json.loads(output.writes[0]["payload"]) == {"status": "OK", "count": 2, "results": [{"ticker": "AAA"}, {"ticker": "BBB"}]}


def test_massive_all_tickers_preflights_output_before_http_download():
    calls = []

    class FakeTickersModel(TickersModel):
        @Flow.call
        def __call__(self, context):
            calls.append(context)
            return HTTPResult(value={"results": []}, status_code=200)

    class MissingCredentialOutput:
        def artifact_uri(self, key):
            return f"s3://shared/{key}"

        def exists(self, key):
            raise RuntimeError("missing output credentials")

    model = MassiveAllTickersModel(tickers_model=FakeTickersModel(), dataset=_dataset(MissingCredentialOutput(), ALL_TICKERS_KEYS))

    with pytest.raises(RuntimeError, match="missing output credentials"):
        model(["2025-01-02"])

    assert calls == []


def test_massive_all_tickers_skips_http_when_output_exists():
    calls = []

    class FakeTickersModel(TickersModel):
        @Flow.call
        def __call__(self, context):
            calls.append(context)
            return HTTPResult(value={"results": []}, status_code=200)

    class ExistingOutput:
        def artifact_uri(self, key):
            return f"s3://shared/{key}"

        def exists(self, key):
            return True

    payload = MassiveAllTickersModel(tickers_model=FakeTickersModel(), dataset=_dataset(ExistingOutput(), ALL_TICKERS_KEYS))(["2025-01-02"]).value

    assert calls == []
    assert payload["status"] == "exists"
    assert payload["will_call_network"] is False
    assert payload["uri"] == "s3://shared/massive/stocks/rest/all-tickers/2025-01-02.json"


def test_massive_tickers_model_paginates_next_url(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200

        def __init__(self, value, url):
            self.headers = {}
            self._value = value
            self.url = f"https://api.massive.com{url}"

        def json(self):
            return self._value

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def request(self, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return FakeResponse(
                    value={"results": [{"ticker": "AAA"}], "next_url": "/v3/reference/tickers?cursor=page-2"},
                    url=kwargs["url"],
                )
            return FakeResponse(value={"results": [{"ticker": "BBB"}]}, url=kwargs["url"])

    monkeypatch.setenv("MASSIVE_API_KEY", "secret")
    monkeypatch.setattr("ccflow_http.base.httpx2.Client", FakeClient)

    result = TickersModel()(TickersContext(active_date=date(2024, 1, 2)))

    assert [call["url"] for call in calls] == ["/v3/reference/tickers", "/v3/reference/tickers"]
    assert [call["params"].get("apiKey") for call in calls] == ["secret", "secret"]
    assert calls[1]["params"]["cursor"] == "page-2"
    assert result.pages == 2
    assert result.value["results"] == [{"ticker": "AAA"}, {"ticker": "BBB"}]


@pytest.mark.parametrize(
    ("path", "date_field", "sort"),
    [
        ("/stocks/v1/splits", "execution_date", "execution_date.asc,ticker.asc"),
        ("/stocks/v1/dividends", "ex_dividend_date", "ex_dividend_date.asc,ticker.asc"),
    ],
)
def test_massive_dated_reference_model_queries_all_tickers_for_one_date(path, date_field, sort):
    model = MassiveDatedReferenceModel(
        path=path,
        date_field=date_field,
        sort=sort,
        credentials=MassiveCredentials(token="secret"),
    )

    request = model.build_request(MassiveDatedReferenceContext(date="2025-01-02"))

    assert request.url == path
    assert request.params[date_field] == "2025-01-02"
    assert request.params["limit"] == 5000
    assert request.params["sort"] == sort
    assert request.params["apiKey"] == "secret"
    assert "ticker" not in request.params


def test_massive_dated_reference_extract_writes_one_market_wide_daily_artifact(monkeypatch):
    class FakeReferenceModel(MassiveDatedReferenceModel):
        @Flow.call
        def __call__(self, context):
            return HTTPResult(
                value={"status": "OK", "results": [{"ticker": "AAPL", "execution_date": context.date.isoformat()}]},
                status_code=200,
                attempts=1,
                pages=1,
            )

    monkeypatch.setenv("MASSIVE_API_KEY", "secret")
    output = RecordingArtifactOutput()
    model = MassiveDatedReferenceExtractModel(
        reference_model=FakeReferenceModel(path="/stocks/v1/splits", date_field="execution_date"),
        reference_name="splits",
        dataset=_dataset(output, "massive/stocks/rest/splits/json/{year}/{month}/{date}.json", "massive-stocks-rest-splits"),
    )

    payload = model(["2025-01-02"]).value

    assert payload["status"] == "written"
    assert payload["request"]["params"] == {"execution_date": "2025-01-02", "limit": 5000, "apiKey": "***"}
    assert output.writes[0]["key"] == "massive/stocks/rest/splits/json/2025/01/2025-01-02.json"
    assert output.writes[0]["metadata"] == {
        "date": "2025-01-02",
        "provider": "massive",
        "market": "stocks",
        "reference": "splits",
    }
    assert json.loads(output.writes[0]["payload"])["results"] == [{"execution_date": "2025-01-02", "ticker": "AAPL"}]


def test_massive_dated_reference_extract_writes_through_a_dataset_and_skips_dates_before_it(monkeypatch):
    calls = []

    class FakeReferenceModel(MassiveDatedReferenceModel):
        @Flow.call
        def __call__(self, context):
            calls.append(context.date)
            return HTTPResult(value={"status": "OK", "results": []}, status_code=200, attempts=1, pages=1)

    monkeypatch.setenv("MASSIVE_API_KEY", "secret")
    output = RecordingArtifactOutput()
    dataset = DatasetDefinition(
        name="massive-stocks-rest-short-volume",
        key_template="massive/stocks/rest/short-volume/json/{year}/{month}/{date}.json",
        store=output,
        format="json",
        first_date=date(2024, 2, 6),
    )
    model = MassiveDatedReferenceExtractModel(
        reference_model=FakeReferenceModel(path="/stocks/v1/short-volume", date_field="date"), reference_name="short-volume", dataset=dataset
    )

    early = model(["2024-02-05"]).value
    payload = model(["2025-01-02"]).value

    assert early["status"] == "before_first_date"
    assert calls == [date(2025, 1, 2)]
    assert (payload["status"], payload["dataset"]) == ("written", "massive-stocks-rest-short-volume")
    assert output.writes[0]["key"] == "massive/stocks/rest/short-volume/json/2025/01/2025-01-02.json"


@pytest.mark.parametrize(
    ("reference_model", "reference_name"),
    [
        (ExchangesModel(), "exchanges"),
        (ConditionsModel(), "conditions"),
    ],
)
def test_massive_reference_snapshot_plans_one_market_wide_capture(reference_model, reference_name, monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    model = MassiveReferenceSnapshotExtractModel(
        reference_model=reference_model,
        reference_name=reference_name,
        dataset=_dataset(NoOpArtifactStore(), f"massive/stocks/rest/{reference_name}/json/{{year}}/{{month}}/{{date}}.json"),
        explain=True,
    )

    payload = model(MassiveReferenceSnapshotContext(date="2025-01-02")).value

    assert payload["status"] == "planned"
    assert payload["date"] == "2025-01-02"
    assert payload["will_call_network"] is False
    assert payload["request"]["url"] == reference_model.path
    assert "ticker" not in payload["request"]["params"]
    assert payload["key"] == f"massive/stocks/rest/{reference_name}/json/2025/01/2025-01-02.json"
    assert payload["dataset_metadata"]["partition_keys"] == ["capture_date"]


def test_massive_flat_file_transfer_plans_dataset_keys_and_skips_dates_before_it():
    dataset = DatasetDefinition(
        name="raw-trades", key_template="raw/trades/{year}/{date}.csv.gz", store=NoOpArtifactStore(), format="csv.gz", first_date=date(2021, 10, 11)
    )
    model = MassiveFlatFileTransferModel(flat_file="trades", dataset=dataset, explain=True)

    planned = model(["2025-11-05"]).value
    early = model(["2021-10-08"]).value

    assert (planned["status"], planned["dataset"], planned["key"]) == ("planned", "raw-trades", "raw/trades/2025/2025-11-05.csv.gz")
    assert planned["source_key"] == "us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz"
    assert early["status"] == "before_first_date"
