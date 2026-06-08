from __future__ import annotations

import json
from datetime import date

import pyarrow.parquet as pq
import pytest
from ccflow import Flow, GenericResult
from ccflow_etl import (
    APIKeySecretCredentials,
    ArtifactWriteModel,
    LocalFileOutput,
    NoOpArtifactStore,
)
from ccflow_http import HTTPResult
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from finance_etl.providers.massive import (
    DailyAggregateBackfillContext,
    DailyAggregateBackfillModel,
    DailyAggregateContext,
    DailyAggregateModel,
    ExchangesModel,
    MarketCalendarContext,
    MarketCalendarModel,
    MarketHolidaysModel,
    MassiveAllStocksDailySummaryModel,
    MassiveAllTickersContext,
    MassiveAllTickersModel,
    MassiveCredentials,
    MassiveDailyAggregateExtractModel,
    MassiveDailyTickerSummaryContext,
    MassiveDailyTickerSummaryModel,
    MassiveDatedSymbolUniverseModel,
    MassiveFlatFileTransferModel,
    TickersContext,
    TickersModel,
    TickerUniversePlanContext,
    TickerUniversePlanModel,
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
    tickers = TickersModel().build_request()

    assert holidays.url == "/v1/marketstatus/upcoming"
    assert holidays.params == {"apiKey": "secret"}
    assert exchanges.url == "/v3/reference/exchanges"
    assert exchanges.params == {"asset_class": "stocks", "locale": "us", "apiKey": "secret"}
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
        TickersContext(market="bonds")
    with pytest.raises(ValueError):
        TickersContext(limit=1001)


def test_massive_daily_aggregate_model_builds_ticker_date_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = DailyAggregateModel().build_request(DailyAggregateContext(ticker="AAA", date="2024-01-03", adjusted=True))

    assert request.url == "/v2/aggs/ticker/AAA/range/1/day/2024-01-03/2024-01-03"
    assert request.params == {"adjusted": True, "sort": "asc", "limit": 50000, "apiKey": "secret"}


def test_massive_daily_aggregate_extract_explain_plans_output_write(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyAggregateExtractModel(output=NoOpArtifactStore(), explain=True)(
        DailyAggregateContext(ticker="AAPL", date="2025-01-02", adjusted=True)
    ).value

    assert payload["status"] == "planned"
    assert payload["will_call_network"] is False
    assert payload["request"]["url"] == "/v2/aggs/ticker/AAPL/range/1/day/2025-01-02/2025-01-02"
    assert payload["request"]["params"] == {"adjusted": True, "sort": "asc", "limit": 50000}
    assert payload["output_key"] == "massive/stocks/rest/daily-aggs/json/2025-01-02/AAPL.json"
    assert payload["output_uri"] == "noop://artifact/massive/stocks/rest/daily-aggs/json/2025-01-02/AAPL.json"
    assert payload["output_writes"][0]["status"] == "planned"


def test_massive_daily_aggregate_extract_writes_raw_payload():
    class FakeDailyModel(DailyAggregateModel):
        @Flow.call
        def __call__(self, context):
            return HTTPResult(
                value={"ticker": context.ticker, "results": [{"c": 42.0}]},
                status_code=200,
                attempts=2,
                retry_summary={"attempts": 2, "retried": 1, "failed": 0, "succeeded": 1},
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
    payload = MassiveDailyAggregateExtractModel(daily_model=FakeDailyModel(), output=output)(
        DailyAggregateContext(ticker="AAPL", date="2025-01-02")
    ).value

    assert payload["status"] == "written"
    assert payload["will_call_network"] is True
    assert payload["retry_summary"] == {"attempts": 2, "retried": 1, "failed": 0, "succeeded": 1}
    assert output.writes[0]["key"] == "massive/stocks/rest/daily-aggs/json/2025-01-02/AAPL.json"
    assert output.writes[0]["media_type"] == "application/json"
    assert json.loads(output.writes[0]["payload"]) == {"results": [{"c": 42.0}], "ticker": "AAPL"}


def test_massive_daily_aggregate_backfill_builds_business_day_requests(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    context = DailyAggregateBackfillContext(
        ticker="AAA",
        start_datetime="2024-01-05",
        end_datetime="2024-01-09",
    )

    result = DailyAggregateBackfillModel().plan_requests(context)

    assert [request.url for request in result.value] == [
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-05/2024-01-05",
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-08/2024-01-08",
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-09/2024-01-09",
    ]
    assert [request.params["apiKey"] for request in result.value] == ["secret", "secret", "secret"]


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


def test_massive_daily_aggregate_backfill_can_use_market_calendar_session_dates(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    context = DailyAggregateBackfillContext(
        ticker="AAA",
        start_datetime="2024-01-05",
        end_datetime="2024-01-09",
        session_dates=[date(2024, 1, 5), date(2024, 1, 9)],
    )

    result = DailyAggregateBackfillModel().plan_requests(context)

    assert [request.url for request in result.value] == [
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-05/2024-01-05",
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-09/2024-01-09",
    ]


def test_massive_stock_data_plan_builds_symbol_date_requests(monkeypatch):
    from finance_etl.providers.massive import StockDataPlanContext, StockDataPlanModel

    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    context = StockDataPlanContext(session_date=date(2024, 1, 3), symbols=["AAA", "BBB"])

    result = StockDataPlanModel().plan_requests(context)

    assert [request.url for request in result.value] == [
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-03/2024-01-03",
        "/v2/aggs/ticker/BBB/range/1/day/2024-01-03/2024-01-03",
    ]
    assert [request.params for request in result.value] == [
        {"adjusted": True, "sort": "asc", "limit": 50000, "apiKey": "secret"},
        {"adjusted": True, "sort": "asc", "limit": 50000, "apiKey": "secret"},
    ]


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


def test_massive_daily_ticker_summary_explain_includes_metadata_and_requests(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyTickerSummaryModel(tickers=["AAPL"], calendar="/calendars/exchange/NYSE", explain=True)(["2025-01-02"]).value

    assert payload["dataset"] == "massive-daily-ticker-summary"
    assert payload["provider"] == "massive"
    assert payload["will_call_network"] is False
    assert payload["dataset_metadata"]["partition_keys"] == ["date", "ticker"]
    assert payload["provider_metadata"]["retry"]["retry_status_codes"] == [429, 500, 502, 503, 504]
    assert payload["base_models"]["http"] == "ccflow_http.HTTPModel"
    assert "ccflow_s3.S3CacheStore" in payload["base_models"]["storage"]
    assert [request["url"] for request in payload["requests"]] == ["/v2/aggs/ticker/AAPL/range/1/day/2025-01-02/2025-01-02"]
    assert payload["output_keys"] == ["massive/stocks/rest/ticker-summary/json/2025-01-02/AAPL.json"]


def test_massive_daily_ticker_summary_explain_plans_output_writes(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyTickerSummaryModel(
        tickers=["AAPL"],
        explain=True,
        artifact_writer=ArtifactWriteModel(store=NoOpArtifactStore()),
    )(["2025-01-02"]).value

    assert payload["output_writes"][0]["status"] == "planned"
    assert payload["output_writes"][0]["artifact"]["uri"] == "noop://artifact/massive/stocks/rest/ticker-summary/json/2025-01-02/AAPL.json"


def test_massive_daily_ticker_summary_can_plan_parquet_return_type(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyTickerSummaryModel(tickers=["AAPL"], explain=True, return_type="parquet")(["2025-01-02"]).value

    assert payload["return_type"] == "parquet"
    assert payload["output_keys"] == ["massive/stocks/rest/ticker-summary/parquet/2025-01-02/AAPL.parquet"]


def test_massive_daily_ticker_summary_artifact_writer_materializes_parquet_return_type(tmp_path):
    model = MassiveDailyTickerSummaryModel(
        tickers=["AAPL"],
        return_type="parquet",
        artifact_writer=ArtifactWriteModel(store=LocalFileOutput(path=tmp_path)),
    )

    results = model._write_outputs(
        MassiveDailyTickerSummaryContext(date="2025-01-02"),
        [{"ticker": "AAPL", "close": 42}],
    )

    output_path = tmp_path / "massive" / "stocks" / "rest" / "ticker-summary" / "parquet" / "2025-01-02" / "AAPL.parquet"

    assert results[0].status == "written"
    assert pq.read_table(output_path).to_pylist() == [{"ticker": "AAPL", "close": 42}]


def test_massive_flat_file_transfer_explain_plans_stock_source_and_output():
    payload = MassiveFlatFileTransferModel(dataset="trades", output=NoOpArtifactStore(), explain=True)(["2025-11-05"]).value

    assert payload["dataset"] == "massive-stocks-flat-files-trades"
    assert payload["source_key"] == "us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz"
    assert payload["source_uri"] == "s3://flatfiles/us_stocks_sip/trades_v1/2025/11/2025-11-05.csv.gz"
    assert payload["output_key"] == "massive/stocks/flat-files/trades/2025/11/2025-11-05.csv.gz"
    assert payload["will_download"] is False
    assert payload["output_writes"][0]["status"] == "planned"
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
    model = MassiveFlatFileTransferModel(dataset="quotes", source_client=source_client, output=output, local_dir=tmp_path)

    payload = model(["2025-11-05"]).value

    assert payload["status"] == "written"
    assert source_client.calls == [{"Bucket": "flatfiles", "Key": "us_stocks_sip/quotes_v1/2025/11/2025-11-05.csv.gz"}]
    assert output.writes[0]["key"] == "massive/stocks/flat-files/quotes/2025/11/2025-11-05.csv.gz"
    assert output.writes[0]["body"] == b"csv-gzip-bytes"
    assert output.writes[0]["media_type"] == "application/gzip"


def test_massive_all_tickers_explain_plans_single_json_output(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveAllTickersModel(output=NoOpArtifactStore(), explain=True)(["2025-01-02"]).value

    assert payload["dataset"] == "massive-stocks-rest-all-tickers"
    assert payload["status"] == "planned"
    assert payload["ticker_request"]["url"] == "/v3/reference/tickers"
    assert payload["ticker_request"]["params"] == {"market": "stocks", "active": True, "limit": 1000, "date": "2025-01-02"}
    assert payload["output_key"] == "massive/stocks/rest/all-tickers/2025-01-02.json"
    assert payload["output_uri"] == "noop://artifact/massive/stocks/rest/all-tickers/2025-01-02.json"
    assert payload["output_writes"][0]["status"] == "planned"
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
    payload = MassiveAllTickersModel(tickers_model=FakeTickersModel(), output=output)(MassiveAllTickersContext(date="2025-01-02")).value

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

    model = MassiveAllTickersModel(tickers_model=FakeTickersModel(), output=MissingCredentialOutput())

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

    payload = MassiveAllTickersModel(tickers_model=FakeTickersModel(), output=ExistingOutput())(["2025-01-02"]).value

    assert calls == []
    assert payload["status"] == "exists"
    assert payload["will_call_network"] is False
    assert payload["output_writes"][0]["artifact"]["uri"] == "s3://shared/massive/stocks/rest/all-tickers/2025-01-02.json"


def test_massive_all_stocks_daily_summary_explain_plans_ticker_universe_request(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveAllStocksDailySummaryModel(explain=True)(["2025-01-02"]).value

    assert payload["dataset"] == "massive-all-stocks-daily-ticker-summary"
    assert payload["status"] == "planned"
    assert payload["ticker_universe_request"]["url"] == "/v3/reference/tickers"
    assert payload["ticker_universe_request"]["params"] == {"market": "stocks", "active": True, "limit": 1000, "date": "2025-01-02"}
    assert payload["summary_dataset_metadata"]["name"] == "massive-daily-ticker-summary"


def test_massive_all_stocks_daily_summary_composes_ticker_and_daily_models(monkeypatch):
    class FakeTickersModel:
        def build_request(self, context):
            return TickersModel().build_request(context)

        def __call__(self, context):
            return GenericResult(value={"results": [{"ticker": "AAA"}, {"ticker": "BBB"}]})

    class FakeDailyModel(DailyAggregateModel):
        @Flow.call
        def __call__(self, context):
            return {"value": {"ticker": context.ticker, "date": context.date.isoformat()}, "status_code": 200}

    monkeypatch.setenv("MASSIVE_API_KEY", "secret")
    model = MassiveAllStocksDailySummaryModel(
        tickers_model=FakeTickersModel(),
        summary_model=MassiveDailyTickerSummaryModel(daily_model=FakeDailyModel()),
        max_tickers=1,
    )

    payload = model(["2025-01-02"]).value

    assert payload["ticker_count"] == 1
    assert payload["tickers"] == ["AAA"]
    assert payload["summary"]["results"][0]["value"] == {"ticker": "AAA", "date": "2025-01-02"}


def test_massive_tickers_model_paginates_next_url(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200
        headers = {}

        def __init__(self, value, url):
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
    monkeypatch.setattr("ccflow_http.base.httpx.Client", FakeClient)

    result = TickersModel()(TickersContext(active_date=date(2024, 1, 2)))

    assert [call["url"] for call in calls] == ["/v3/reference/tickers", "/v3/reference/tickers"]
    assert [call["params"].get("apiKey") for call in calls] == ["secret", "secret"]
    assert calls[1]["params"]["cursor"] == "page-2"
    assert result.pages == 2
    assert result.value["results"] == [{"ticker": "AAA"}, {"ticker": "BBB"}]


def test_massive_daily_aggregate_backfill_downloads_each_business_day(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 200
        headers = {}

        def __init__(self, url):
            self.url = f"https://api.massive.com{url}"
            self._url = url

        def json(self):
            date = self._url.rsplit("/", 1)[-1]
            return {"ticker": "AAA", "date": date}

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
            return FakeResponse(kwargs["url"])

    monkeypatch.setattr("ccflow_http.base.httpx.Client", FakeClient)

    context = DailyAggregateBackfillContext(
        ticker="AAA",
        start_datetime="2024-01-05",
        end_datetime="2024-01-09",
    )

    result = DailyAggregateBackfillModel()(context)

    assert [call["url"] for call in calls] == [
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-05/2024-01-05",
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-08/2024-01-08",
        "/v2/aggs/ticker/AAA/range/1/day/2024-01-09/2024-01-09",
    ]
    assert [response.value for response in result.value] == [
        {"ticker": "AAA", "date": "2024-01-05"},
        {"ticker": "AAA", "date": "2024-01-08"},
        {"ticker": "AAA", "date": "2024-01-09"},
    ]
