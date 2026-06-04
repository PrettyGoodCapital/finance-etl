from __future__ import annotations

from datetime import date

from ccflow_etl import APIKeySecretCredentials, DatasetDefinition, ProviderDefinition
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
    MassiveCredentials,
    MassiveDailyTickerSummaryModel,
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


def test_massive_catalog_configs_register_dataset_and_provider(tmp_path):
    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - credentials: massive
    - datasets: massive
    - providers: massive

hydra:
    searchpath:
        - pkg://finance_etl.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        cfg = compose(config_name="runner")

    dataset = instantiate(cfg.datasets.massive_daily_ticker_summary)
    provider = instantiate(cfg.providers.massive)

    assert isinstance(dataset, DatasetDefinition)
    assert dataset.name == "massive-daily-ticker-summary"
    assert dataset.partition_keys == ["date", "ticker"]
    assert isinstance(provider, ProviderDefinition)
    assert provider.name == "massive"
    assert provider.dataset_refs == ["/datasets/massive_daily_ticker_summary"]
    assert provider.credentials_ref == "/credentials/massive"


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


def test_massive_daily_aggregate_model_builds_ticker_date_request(monkeypatch):
    monkeypatch.setenv("MASSIVE_API_KEY", "secret")

    request = DailyAggregateModel().build_request(DailyAggregateContext(ticker="AAA", date="2024-01-03", adjusted=True))

    assert request.url == "/v2/aggs/ticker/AAA/range/1/day/2024-01-03/2024-01-03"
    assert request.params == {"adjusted": True, "sort": "asc", "limit": 50000, "apiKey": "secret"}


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


def test_massive_daily_ticker_summary_explain_includes_catalog_and_unit_identity(monkeypatch):
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    payload = MassiveDailyTickerSummaryModel(tickers=["AAPL"], calendar="/calendars/nyse", explain=True, destination="s3")(["2025-01-02"]).value

    assert payload["dataset"] == "massive-daily-ticker-summary"
    assert payload["provider"] == "massive"
    assert payload["will_call_network"] is False
    assert payload["dataset_definition"]["partition_keys"] == ["date", "ticker"]
    assert payload["provider_definition"]["credentials_ref"] == "/credentials/massive"
    assert payload["provider_definition"]["retry"]["retry_status_codes"] == [429, 500, 502, 503, 504]
    assert payload["unit_identities"][0]["partition"] == {"date": "2025-01-02", "ticker": "AAPL"}
    assert payload["unit_identities"][0]["key"].startswith("units/massive/massive-daily-ticker-summary/schema=1/transform=raw/destination=s3/")
    assert payload["base_models"]["http"] == "ccflow_http.HTTPModel"
    assert "ccflow_s3.S3CacheStore" in payload["base_models"]["storage"]
    assert [request["url"] for request in payload["requests"]] == ["/v2/aggs/ticker/AAPL/range/1/day/2025-01-02/2025-01-02"]


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
