import os
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Literal

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from ccflow_etl import (
    APITokenCredentials,
    ArtifactWriteContext,
    ArtifactWriteModel,
    BackfillContext,
    ETLArtifact,
    PayloadCodec,
)
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext, safe_request_dump
from pydantic import Field, field_validator, model_validator

from finance_etl.symbols import SymbolUniverseResult

_US_STOCK_EXCHANGE_ALIASES = {
    "NASDAQ",
    "Nasdaq Stock Market",
    "New York Stock Exchange",
    "NYSE",
    "XNAS",
    "XNYS",
}


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> date:
    first_day = date(year, month, 1)
    days_until_weekday = (weekday - first_day.weekday()) % 7
    return first_day + timedelta(days=days_until_weekday + (occurrence - 1) * 7)


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    if month == 12:
        first_day_next_month = date(year + 1, 1, 1)
    else:
        first_day_next_month = date(year, month + 1, 1)
    last_day = first_day_next_month - timedelta(days=1)
    days_since_weekday = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=days_since_weekday)


def _observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def _easter_date(year: int) -> date:
    century = year // 100
    year_in_century = year % 100
    century_leap_correction = century // 4
    century_remainder = century % 4
    lunar_correction = (century + 8) // 25
    epact_correction = (century - lunar_correction + 1) // 3
    epact = (19 * (year % 19) + century - century_leap_correction - epact_correction + 15) % 30
    year_leap_correction = year_in_century // 4
    year_remainder = year_in_century % 4
    weekday_correction = (32 + 2 * century_remainder + 2 * year_leap_correction - epact - year_remainder) % 7
    paschal_offset = (year % 19 + 11 * epact + 22 * weekday_correction) // 451
    month = (epact + weekday_correction - 7 * paschal_offset + 114) // 31
    day = ((epact + weekday_correction - 7 * paschal_offset + 114) % 31) + 1
    return date(year, month, day)


def _regular_us_stock_market_holidays(start_date: date, end_date: date) -> set[date]:
    closed_dates = set()
    for holiday_year in range(start_date.year, end_date.year + 2):
        closed_dates.update(
            {
                _observed_fixed_holiday(holiday_year, 1, 1),
                _nth_weekday_of_month(holiday_year, 1, 0, 3),
                _nth_weekday_of_month(holiday_year, 2, 0, 3),
                _easter_date(holiday_year) - timedelta(days=2),
                _last_weekday_of_month(holiday_year, 5, 0),
                _observed_fixed_holiday(holiday_year, 7, 4),
                _nth_weekday_of_month(holiday_year, 9, 0, 1),
                _nth_weekday_of_month(holiday_year, 11, 3, 4),
                _observed_fixed_holiday(holiday_year, 12, 25),
            }
        )
        if holiday_year >= 2022:
            closed_dates.add(_observed_fixed_holiday(holiday_year, 6, 19))
    return {closed_date for closed_date in closed_dates if start_date <= closed_date <= end_date}


__all__ = (
    "DailyAggregateBackfillContext",
    "DailyAggregateBackfillModel",
    "DailyAggregateContext",
    "DailyAggregateModel",
    "DailyMarketSummaryContext",
    "DailyMarketSummaryModel",
    "DailyTickerSummaryContext",
    "DailyTickerSummaryModel",
    "ExchangesModel",
    "MarketCalendarContext",
    "MarketCalendarModel",
    "MarketHolidaysModel",
    "MassiveAllStocksDailySummaryModel",
    "MassiveAllTickersContext",
    "MassiveAllTickersModel",
    "MassiveCredentials",
    "MassiveDailyAggregateExtractModel",
    "MassiveDailyMarketSummaryExtractModel",
    "MassiveDailyTickerSummaryContext",
    "MassiveDailyTickerSummaryExtractModel",
    "MassiveDailyTickerSummaryModel",
    "MassiveDatedSymbolUniverseModel",
    "MassiveFlatFileContext",
    "MassiveFlatFileTransferModel",
    "MassiveHTTPModel",
    "MassiveRequestContext",
    "MassiveTickerOverviewExtractModel",
    "StockDataPlanContext",
    "StockDataPlanModel",
    "TickerOverviewContext",
    "TickerOverviewModel",
    "TickerUniversePlanContext",
    "TickerUniversePlanModel",
    "TickersContext",
    "TickersModel",
)

MassiveStockFlatFileDataset = Literal["day-aggs", "minute-aggs", "trades", "quotes"]
MassiveTickerMarket = Literal["crypto", "fx", "indices", "otc", "stocks"]
MassiveTickerOrder = Literal["asc", "desc"]

_MASSIVE_STOCK_FLAT_FILES: dict[MassiveStockFlatFileDataset, dict[str, str]] = {
    "day-aggs": {
        "path": "us_stocks_sip/day_aggs_v1",
        "description": "Daily aggregate OHLCV CSV gzip flat file for all U.S. equities.",
    },
    "minute-aggs": {
        "path": "us_stocks_sip/minute_aggs_v1",
        "description": "Minute aggregate OHLCV CSV gzip flat file for all U.S. equities.",
    },
    "trades": {
        "path": "us_stocks_sip/trades_v1",
        "description": "Tick-level trade CSV gzip flat file for all U.S. equities.",
    },
    "quotes": {
        "path": "us_stocks_sip/quotes_v1",
        "description": "Top-of-book quote CSV gzip flat file for all U.S. equities.",
    },
}


class MassiveCredentials(APITokenCredentials):
    token_env: str | None = "MASSIVE_API_KEY"
    query_param: str = "apiKey"

    def api_key(self) -> str | None:
        return self.resolved_token()


class MassiveRequestContext(HTTPRequestContext):
    api_key: str | None = None
    credentials: MassiveCredentials | None = None


class MarketCalendarContext(MassiveRequestContext):
    start_date: date
    end_date: date
    exchange: str | None = None
    holidays: Any = Field(default_factory=list)
    exchanges: Any = Field(default_factory=list)


class DailyAggregateContext(MassiveRequestContext, DateContext):
    ticker: str
    adjusted: bool = True


class TickerOverviewContext(MassiveRequestContext, DateContext):
    ticker: str


class DailyMarketSummaryContext(MassiveRequestContext, DateContext):
    adjusted: bool = True
    include_otc: bool = False

    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class DailyTickerSummaryContext(MassiveRequestContext, DateContext):
    ticker: str
    adjusted: bool = True


class MassiveDailyTickerSummaryContext(DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class MassiveAllTickersContext(MassiveRequestContext, DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class MassiveFlatFileContext(DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class TickersContext(MassiveRequestContext):
    ticker: str | None = None
    ticker_type: str | None = None
    market: MassiveTickerMarket = "stocks"
    exchange: str | None = None
    cusip: str | None = None
    cik: str | None = None
    search: str | None = None
    active: bool | None = True
    active_date: date | None = None
    order: MassiveTickerOrder | None = None
    sort: str | None = None
    limit: int = Field(default=1000, ge=1, le=1000)

    @field_validator("ticker", "ticker_type", "exchange", "cusip", "cik", "search", "sort", mode="before")
    @classmethod
    def strip_optional_filter(cls, value):
        if value is None:
            return None
        value = str(value).strip()
        return value or None


class TickerUniversePlanContext(MassiveRequestContext):
    session_dates: list[date]
    market: str = "stocks"
    active: bool = True
    limit: int = 1000


class DailyAggregateBackfillContext(BackfillContext[DailyAggregateContext]):
    ticker: str
    adjusted: bool = True
    api_key: str | None = None
    session_dates: list[date] | None = None

    @model_validator(mode="before")
    @classmethod
    def default_daily_context(cls, value):
        if isinstance(value, dict):
            value.setdefault("interval", "1B")
            if "template" not in value:
                value["template"] = {
                    "ticker": value["ticker"],
                    "date": value["start_datetime"],
                    "adjusted": value.get("adjusted", True),
                    "api_key": value.get("api_key"),
                    "credentials": value.get("credentials"),
                }
        return value

    def step_contexts(self) -> list[DailyAggregateContext]:
        if self.session_dates is None:
            return super().step_contexts()

        start_date = self.start_datetime.date()
        end_date = self.end_datetime.date()
        session_dates = [session_date for session_date in self.session_dates if start_date <= session_date <= end_date]
        return [
            self.template.model_copy(
                update={
                    "datetime": datetime.combine(session_date, time()),
                    "dt": datetime.combine(session_date, time()),
                    "date": session_date,
                }
            )
            for session_date in session_dates
        ]


class StockDataPlanContext(MassiveRequestContext):
    session_date: date
    symbols: list[str]
    adjusted: bool = True


class MassiveHTTPModel(HTTPModel):
    base_url: str = "https://api.massive.com"
    credentials: MassiveCredentials = Field(default_factory=MassiveCredentials)
    api_key_env: str = "MASSIVE_API_KEY"
    api_key: str | None = None

    def _api_key(self, context: MassiveRequestContext) -> str | None:
        context_credentials = context.credentials.api_key() if context.credentials else None
        return context.api_key or context_credentials or self.api_key or self.credentials.api_key() or os.environ.get(self.api_key_env)

    def build_request(self, context: MassiveRequestContext | None = None) -> HTTPRequest:
        context = context or MassiveRequestContext()
        api_key = self._api_key(context)
        if api_key:
            context = context.model_copy(update={"query": {**context.query, "apiKey": api_key}})
        return super().build_request(context)


class MarketHolidaysModel(MassiveHTTPModel):
    path: str = "/v1/marketstatus/upcoming"


class MarketCalendarModel(CallableModel):
    holidays_model: MarketHolidaysModel = Field(default_factory=MarketHolidaysModel)

    @property
    def context_type(self):
        return MarketCalendarContext

    @property
    def result_type(self):
        return GenericResult

    def _holiday_items(self, holidays: Any) -> list[dict[str, Any]]:
        if isinstance(holidays, dict):
            return holidays.get("results", [])
        return list(holidays or [])

    def _exchange_items(self, exchanges: Any) -> list[dict[str, Any]]:
        if isinstance(exchanges, dict):
            return exchanges.get("results", [])
        return list(exchanges or [])

    def _exchange_matches(self, exchange_metadata: dict[str, Any], exchange: str) -> bool:
        name = exchange_metadata.get("name")
        exchange_values = {
            exchange_metadata.get("exchange"),
            exchange_metadata.get("mic"),
            exchange_metadata.get("operating_mic"),
            exchange_metadata.get("acronym"),
            name,
        }
        if isinstance(name, str):
            exchange_values.add("".join(word[0] for word in name.split() if word and word[0].isalpha()))
        return exchange.casefold() in {str(value).casefold() for value in exchange_values if value is not None}

    def _exchange_is_known(self, exchanges: Any, exchange: str | None) -> bool:
        if exchange is None:
            return True
        exchange_items = self._exchange_items(exchanges)
        if not exchange_items:
            return True
        return any(self._exchange_matches(exchange_metadata, exchange) for exchange_metadata in exchange_items)

    def _holiday_date(self, holiday: dict[str, Any]) -> date | None:
        holiday_date = holiday.get("date")
        if isinstance(holiday_date, date):
            return holiday_date
        if isinstance(holiday_date, str):
            return date.fromisoformat(holiday_date[:10])
        return None

    def _holiday_matches_exchange(self, holiday: dict[str, Any], exchange: str | None) -> bool:
        if exchange is None:
            return True
        holiday_exchange = holiday.get("exchange") or holiday.get("market")
        if holiday_exchange is None:
            return True
        return str(holiday_exchange).casefold() == exchange.casefold()

    def _closed_holiday_dates(self, holidays: list[dict[str, Any]], exchange: str | None) -> set[date]:
        closed_dates = set()
        for holiday in holidays:
            if str(holiday.get("status", "")).casefold() != "closed":
                continue
            if not self._holiday_matches_exchange(holiday, exchange):
                continue
            holiday_date = self._holiday_date(holiday)
            if holiday_date is not None:
                closed_dates.add(holiday_date)
        return closed_dates

    def _uses_us_stock_calendar(self, exchanges: Any, exchange: str | None) -> bool:
        if exchange is None:
            return True
        if exchange.casefold() in {alias.casefold() for alias in _US_STOCK_EXCHANGE_ALIASES}:
            return True
        for exchange_metadata in self._exchange_items(exchanges):
            if not self._exchange_matches(exchange_metadata, exchange):
                continue
            metadata_values = {
                exchange_metadata.get("mic"),
                exchange_metadata.get("operating_mic"),
                exchange_metadata.get("name"),
                exchange_metadata.get("exchange"),
            }
            if any(str(value).casefold() in {alias.casefold() for alias in _US_STOCK_EXCHANGE_ALIASES} for value in metadata_values if value):
                return True
        return False

    def session_dates(self, context: MarketCalendarContext) -> list[date]:
        if not self._exchange_is_known(context.exchanges, context.exchange):
            return []
        closed_dates = self._closed_holiday_dates(self._holiday_items(context.holidays), context.exchange)
        if self._uses_us_stock_calendar(context.exchanges, context.exchange):
            closed_dates |= _regular_us_stock_market_holidays(context.start_date, context.end_date)
        current_date = context.start_date
        sessions = []
        while current_date <= context.end_date:
            if current_date.weekday() < 5 and current_date not in closed_dates:
                sessions.append(current_date)
            current_date += timedelta(days=1)
        return sessions

    @Flow.call
    def __call__(self, context: MarketCalendarContext) -> GenericResult:
        if not self._exchange_is_known(context.exchanges, context.exchange):
            return GenericResult(value=[])
        holidays = context.holidays
        if not holidays:
            holidays = self.holidays_model(context).value
        return GenericResult(value=self.session_dates(context.model_copy(update={"holidays": holidays})))


class ExchangesModel(MassiveHTTPModel):
    path: str = "/v3/reference/exchanges"
    query: dict = Field(default_factory=lambda: {"asset_class": "stocks", "locale": "us"})


class TickersModel(MassiveHTTPModel):
    path: str = "/v3/reference/tickers"
    query: dict = Field(default_factory=lambda: {"market": "stocks", "active": True})
    paginate: bool = True
    max_pages: int = 1000

    def build_request(self, context: TickersContext | None = None) -> HTTPRequest:
        context = context or TickersContext()
        query = {
            **context.query,
            "market": context.market,
            "active": context.active,
            "limit": context.limit,
        }
        optional_params = {
            "ticker": context.ticker,
            "type": context.ticker_type,
            "exchange": context.exchange,
            "cusip": context.cusip,
            "cik": context.cik,
            "search": context.search,
            "order": context.order,
            "sort": context.sort,
        }
        query.update({key: value for key, value in optional_params.items() if value is not None})
        if context.active_date is not None:
            query["date"] = context.active_date.isoformat()
        return super().build_request(context.model_copy(update={"query": query}))


class MassiveDatedSymbolUniverseModel(CallableModel):
    tickers_model: TickersModel = Field(default_factory=TickersModel)
    explain: bool = False
    ticker: str | None = None
    ticker_type: str | None = None
    market: MassiveTickerMarket = "stocks"
    exchange: str | None = None
    cusip: str | None = None
    cik: str | None = None
    search: str | None = None
    active: bool | None = True
    order: MassiveTickerOrder | None = None
    sort: str | None = "ticker"
    limit: int = Field(default=1000, ge=1, le=1000)
    max_pages: int = Field(default=1000, ge=1)
    max_symbols: int | None = Field(default=None, ge=1)
    source: str = "massive-stocks-rest-tickers"

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def _tickers_context(self, context: MassiveAllTickersContext) -> TickersContext:
        return TickersContext(
            api_key=context.api_key,
            credentials=context.credentials,
            ticker=self.ticker,
            ticker_type=self.ticker_type,
            market=self.market,
            exchange=self.exchange,
            cusip=self.cusip,
            cik=self.cik,
            search=self.search,
            active=self.active,
            active_date=context.date,
            order=self.order,
            sort=self.sort,
            limit=self.limit,
        )

    def _request_model(self) -> TickersModel:
        return self.tickers_model.model_copy(update={"max_pages": self.max_pages})

    def _symbol_values(self, payload: Any) -> list[str]:
        items = payload.get("results", []) if isinstance(payload, dict) else payload
        symbols = [item.get("ticker") for item in items or [] if isinstance(item, dict) and item.get("ticker")]
        symbols = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
        return symbols[: self.max_symbols] if self.max_symbols is not None else symbols

    def _plan(self, context: MassiveAllTickersContext) -> dict[str, Any]:
        request_model = self._request_model()
        tickers_context = self._tickers_context(context)
        return {
            "source": self.source,
            "as_of_date": context.date.isoformat(),
            "request": safe_request_dump(request_model.build_request(tickers_context)),
            "filters": {
                "ticker": self.ticker,
                "type": self.ticker_type,
                "market": self.market,
                "exchange": self.exchange,
                "cusip": self.cusip,
                "cik": self.cik,
                "search": self.search,
                "active": self.active,
                "order": self.order,
                "sort": self.sort,
                "limit": self.limit,
                "max_pages": self.max_pages,
                "max_symbols": self.max_symbols,
            },
        }

    @Flow.call
    def __call__(self, context: MassiveAllTickersContext) -> GenericResult:
        plan = self._plan(context)
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        result = self._request_model()(self._tickers_context(context))
        symbols = self._symbol_values(result.value)
        return GenericResult(
            value=SymbolUniverseResult(
                as_of_date=context.date,
                symbols=symbols,
                source=self.source,
                metadata={
                    "ticker_count": len(symbols),
                    "pages": getattr(result, "pages", None),
                    "filters": plan["filters"],
                    "request": plan["request"],
                },
            )
        )


class MassiveAllTickersModel(CallableModel):
    tickers_model: TickersModel = Field(default_factory=TickersModel)
    output: Any | None = None
    calendar: str = "/calendars/exchange/NYSE"
    explain: bool = False
    ticker: str | None = None
    ticker_type: str | None = None
    market: MassiveTickerMarket = "stocks"
    exchange: str | None = None
    cusip: str | None = None
    cik: str | None = None
    search: str | None = None
    active: bool | None = True
    order: MassiveTickerOrder | None = None
    sort: str | None = None
    limit: int = Field(default=1000, ge=1, le=1000)
    max_pages: int = Field(default=1000, ge=1)
    output_key_prefix: str = "massive/stocks/rest/all-tickers"
    overwrite_output: bool = False
    dataset_name: str = "massive-stocks-rest-all-tickers"
    provider_name: str = "massive"
    provider_type: str = "http"
    provider_capabilities: list[str] = Field(default_factory=lambda: ["pagination", "http_status_retry", "rate_limit_headers"])
    provider_retry: dict[str, Any] = Field(default_factory=lambda: {"retry_status_codes": [429, 500, 502, 503, 504]})
    provider_rate_limit: dict[str, Any] = Field(default_factory=lambda: {"source": "provider_headers"})

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def _date_value(self, context: MassiveAllTickersContext) -> str:
        return context.date.isoformat() if isinstance(context.date, date) else str(context.date)

    def _tickers_context(self, context: MassiveAllTickersContext) -> TickersContext:
        return TickersContext(
            api_key=context.api_key,
            credentials=context.credentials,
            ticker=self.ticker,
            ticker_type=self.ticker_type,
            market=self.market,
            exchange=self.exchange,
            cusip=self.cusip,
            cik=self.cik,
            search=self.search,
            active=self.active,
            active_date=context.date,
            order=self.order,
            sort=self.sort,
            limit=self.limit,
        )

    def _request_model(self) -> TickersModel:
        return self.tickers_model.model_copy(update={"max_pages": self.max_pages})

    def output_key(self, context: MassiveAllTickersContext) -> str:
        return f"{self.output_key_prefix.strip('/')}/{self._date_value(context)}.json"

    def _artifact_uri(self, key: str) -> str:
        if self.output is None:
            return key
        if hasattr(self.output, "artifact_uri"):
            return self.output.artifact_uri(key)
        if hasattr(self.output, "uri"):
            return self.output.uri(key)
        return key

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "description": "Massive ticker reference payload for all matching stock tickers.",
            "endpoint": "/v3/reference/tickers",
            "partition_keys": ["date"],
            "cadence": "1D",
            "media_types": ["application/json"],
            "filters": {
                "ticker": self.ticker,
                "type": self.ticker_type,
                "market": self.market,
                "exchange": self.exchange,
                "cusip": self.cusip,
                "cik": self.cik,
                "search": self.search,
                "active": self.active,
                "order": self.order,
                "sort": self.sort,
                "limit": self.limit,
            },
        }

    def provider_metadata(self) -> dict[str, Any]:
        return {
            "name": self.provider_name,
            "provider_type": self.provider_type,
            "capabilities": list(self.provider_capabilities),
            "rate_limit": dict(self.provider_rate_limit),
            "retry": dict(self.provider_retry),
            "request_templates": {"all_tickers": "/v3/reference/tickers"},
        }

    def _planned_write(self, context: MassiveAllTickersContext) -> list[dict[str, Any]]:
        if self.output is None:
            return []
        writer = ArtifactWriteModel(store=self.output)
        result = writer(
            ArtifactWriteContext(
                key=self.output_key(context),
                payload=b"",
                media_type=PayloadCodec(format="json").media_type,
                dataset=self.dataset_name,
                stage="extract",
                overwrite=self.overwrite_output,
                dry_run=True,
                metadata={"date": self._date_value(context), "provider": self.provider_name},
            )
        )
        return [result.model_dump(mode="json")]

    def _output_exists(self, context: MassiveAllTickersContext) -> bool:
        if self.output is None or self.overwrite_output or not hasattr(self.output, "exists"):
            return False
        return self.output.exists(self.output_key(context))

    def _existing_write(self, context: MassiveAllTickersContext) -> list[dict[str, Any]]:
        if self.output is None:
            return []
        artifact = ETLArtifact(
            key=self.output_key(context),
            stage="extract",
            dataset=self.dataset_name,
            uri=self._artifact_uri(self.output_key(context)),
            media_type=PayloadCodec(format="json").media_type,
            status="exists",
            metadata={"date": self._date_value(context), "provider": self.provider_name, "market": self.market},
        )
        return [
            {
                "key": artifact.key,
                "uri": artifact.uri,
                "status": "exists",
                "artifact": artifact.model_dump(mode="json"),
                "metadata": dict(artifact.metadata),
            }
        ]

    def _write_output(self, context: MassiveAllTickersContext, payload: dict[str, Any]) -> list[dict[str, Any]]:
        if self.output is None:
            raise ValueError("Massive all-tickers task requires output.")
        codec = PayloadCodec(format="json")
        writer = ArtifactWriteModel(store=self.output)
        result = writer(
            ArtifactWriteContext(
                key=self.output_key(context),
                payload=codec.encode(payload),
                media_type=codec.media_type,
                dataset=self.dataset_name,
                stage="extract",
                overwrite=self.overwrite_output,
                metadata={
                    "date": self._date_value(context),
                    "provider": self.provider_name,
                    "market": self.market,
                },
            )
        )
        return [result.model_dump(mode="json")]

    def _plan(self, context: MassiveAllTickersContext) -> dict[str, Any]:
        tickers_context = self._tickers_context(context)
        request_model = self._request_model()
        output_key = self.output_key(context)
        return {
            "dataset": self.dataset_name,
            "provider": self.provider_name,
            "date": self._date_value(context),
            "calendar": self.calendar,
            "return_type": "json",
            "output_key": output_key,
            "output_uri": self._artifact_uri(output_key) if self.output is not None else None,
            "output_writes": self._planned_write(context),
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "will_publish_output": False,
            "ticker_request": safe_request_dump(request_model.build_request(tickers_context)),
            "dataset_metadata": self.dataset_metadata(),
            "provider_metadata": self.provider_metadata(),
            "base_models": {
                "http": "ccflow_http.HTTPModel",
                "request_model": f"{request_model.__class__.__module__}.{request_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3ArtifactStore"],
            },
        }

    @Flow.call
    def __call__(self, context: MassiveAllTickersContext) -> GenericResult:
        payload = self._plan(context)
        if self.explain:
            return GenericResult(value={**payload, "status": "planned"})
        if self.output is None:
            raise ValueError("Massive all-tickers task requires output.")
        if self._output_exists(context):
            return GenericResult(
                value={
                    **payload,
                    "status": "exists",
                    "will_call_network": False,
                    "will_publish_output": False,
                    "ticker_count": None,
                    "page_count": 0,
                    "attempts": 0,
                    "rate_limit": {},
                    "retry_events": [],
                    "retry_summary": {},
                    "output_writes": self._existing_write(context),
                }
            )

        tickers_context = self._tickers_context(context)
        ticker_result = self._request_model()(tickers_context)
        raw_payload = ticker_result.value if isinstance(ticker_result.value, dict) else {"results": ticker_result.value}
        output_writes = self._write_output(context, raw_payload)
        results = raw_payload.get("results", []) if isinstance(raw_payload, dict) else []
        status = output_writes[0]["status"] if output_writes else "written"
        return GenericResult(
            value={
                **payload,
                "status": status,
                "will_call_network": True,
                "will_publish_output": True,
                "ticker_count": len(results) if isinstance(results, list) else None,
                "page_count": ticker_result.pages,
                "attempts": ticker_result.attempts,
                "rate_limit": ticker_result.rate_limit,
                "retry_events": ticker_result.retry_events,
                "retry_summary": ticker_result.retry_summary,
                "output_writes": output_writes,
            }
        )


class TickerUniversePlanModel(CallableModel):
    tickers_model: TickersModel = Field(default_factory=TickersModel)

    @property
    def context_type(self):
        return TickerUniversePlanContext

    @property
    def result_type(self):
        return GenericResult

    def plan_requests(self, context: TickerUniversePlanContext) -> GenericResult:
        return GenericResult(
            value=[
                self.tickers_model.build_request(
                    TickersContext(
                        api_key=context.api_key,
                        market=context.market,
                        active=context.active,
                        active_date=session_date,
                        limit=context.limit,
                    )
                )
                for session_date in context.session_dates
            ]
        )

    @Flow.call
    def __call__(self, context: TickerUniversePlanContext) -> GenericResult:
        return self.plan_requests(context)


class DailyAggregateModel(MassiveHTTPModel):
    path: str = "/v2/aggs/ticker/{{ ticker }}/range/1/day/{{ date }}/{{ date }}"
    query: dict = Field(default_factory=lambda: {"sort": "asc", "limit": 50000})

    @property
    def context_type(self) -> type[ContextType]:
        return DailyAggregateContext

    def build_request(self, context: DailyAggregateContext) -> HTTPRequest:
        context = context.model_copy(update={"query": {**context.query, "adjusted": context.adjusted}})
        return super().build_request(context)


class _MassiveRESTExtractModel(CallableModel):
    output: Any | None = None
    explain: bool = False
    return_type: str = "json"
    output_key_prefix: str
    overwrite_output: bool = False
    dataset_name: str
    provider_name: str = "massive"

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def _request_model(self) -> CallableModel:
        raise NotImplementedError

    def output_key(self, context: ContextType) -> str:
        raise NotImplementedError

    def _metadata(self, context: ContextType) -> dict[str, Any]:
        raise NotImplementedError

    def _plan_fields(self, context: ContextType) -> dict[str, Any]:
        return {}

    def dataset_metadata(self) -> dict[str, Any]:
        raise NotImplementedError

    def _output_required_message(self) -> str:
        return f"{self.dataset_name} extract task requires output."

    def _base_models(self) -> dict[str, Any]:
        request_model = self._request_model()
        return {
            "http": "ccflow_http.HTTPModel",
            "request_model": f"{request_model.__class__.__module__}.{request_model.__class__.__name__}",
            "storage": ["ccflow_s3.S3ArtifactStore"],
        }

    def _planned_write(self, context: ContextType) -> list[dict[str, Any]]:
        if self.output is None:
            return []
        return [
            _artifact_write(
                output=self.output,
                key=self.output_key(context),
                payload={},
                return_type=self.return_type,
                dataset_name=self.dataset_name,
                metadata=self._metadata(context),
                overwrite=self.overwrite_output,
                dry_run=True,
            )
        ]

    def _existing_write(self, context: ContextType) -> list[dict[str, Any]]:
        return [
            _existing_artifact(
                output=self.output,
                key=self.output_key(context),
                return_type=self.return_type,
                dataset_name=self.dataset_name,
                metadata=self._metadata(context),
            )
        ]

    def _output_exists(self, context: ContextType) -> bool:
        if self.output is None or self.overwrite_output or not hasattr(self.output, "exists"):
            return False
        return self.output.exists(self.output_key(context))

    def _write_output(self, context: ContextType, payload: Any) -> list[dict[str, Any]]:
        if self.output is None:
            raise ValueError(self._output_required_message())
        return [
            _artifact_write(
                output=self.output,
                key=self.output_key(context),
                payload=payload,
                return_type=self.return_type,
                dataset_name=self.dataset_name,
                metadata=self._metadata(context),
                overwrite=self.overwrite_output,
            )
        ]

    def _request_error_result(self, context: ContextType, payload: dict[str, Any], exc: RuntimeError) -> GenericResult | None:
        return None

    def _plan(self, context: ContextType) -> dict[str, Any]:
        output_key = self.output_key(context)
        return {
            "dataset": self.dataset_name,
            "provider": self.provider_name,
            **self._plan_fields(context),
            "return_type": self.return_type,
            "output_key": output_key,
            "output_uri": _artifact_uri(self.output, output_key) if self.output is not None else None,
            "output_writes": self._planned_write(context),
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "will_publish_output": False,
            "request": safe_request_dump(self._request_model().build_request(context)),
            "dataset_metadata": self.dataset_metadata(),
            "base_models": self._base_models(),
        }

    @Flow.call
    def __call__(self, context: ContextType) -> GenericResult:
        payload = self._plan(context)
        if self.explain:
            return GenericResult(value={**payload, "status": "planned"})
        if self.output is None:
            raise ValueError(self._output_required_message())
        if self._output_exists(context):
            return GenericResult(
                value={
                    **payload,
                    "status": "exists",
                    "will_call_network": False,
                    "will_publish_output": False,
                    "attempts": 0,
                    "rate_limit": {},
                    "retry_events": [],
                    "retry_summary": {},
                    "output_writes": self._existing_write(context),
                }
            )

        try:
            result = self._request_model()(context)
        except RuntimeError as exc:
            handled_result = self._request_error_result(context, payload, exc)
            if handled_result is not None:
                return handled_result
            raise
        raw_payload = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
        output_writes = self._write_output(context, raw_payload)
        status = output_writes[0]["status"] if output_writes else "written"
        return GenericResult(
            value={
                **payload,
                "status": status,
                "will_call_network": True,
                "will_publish_output": True,
                "status_code": getattr(result, "status_code", None),
                "attempts": getattr(result, "attempts", 1),
                "rate_limit": getattr(result, "rate_limit", {}),
                "retry_events": getattr(result, "retry_events", []),
                "retry_summary": getattr(result, "retry_summary", {}),
                "output_writes": output_writes,
            }
        )


class MassiveDailyAggregateExtractModel(_MassiveRESTExtractModel):
    daily_model: DailyAggregateModel = Field(default_factory=DailyAggregateModel)
    output_key_prefix: str = "massive/stocks/rest/daily-aggs"
    dataset_name: str = "massive-stocks-rest-daily-aggs"

    @property
    def context_type(self) -> type[ContextType]:
        return DailyAggregateContext

    def _request_model(self) -> DailyAggregateModel:
        return self.daily_model

    def _date_value(self, context: DailyAggregateContext) -> str:
        return _date_value(context.date)

    def output_key(self, context: DailyAggregateContext) -> str:
        suffix = PayloadCodec(format=self.return_type).suffix or ".bin"
        return f"{self.output_key_prefix.strip('/')}/{self.return_type}/{self._date_value(context)}/{context.ticker}{suffix}"

    def _metadata(self, context: DailyAggregateContext) -> dict[str, Any]:
        return {"date": self._date_value(context), "ticker": context.ticker, "provider": self.provider_name}

    def _plan_fields(self, context: DailyAggregateContext) -> dict[str, Any]:
        return {"date": self._date_value(context), "ticker": context.ticker, "adjusted": context.adjusted}

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "endpoint": "/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}",
            "partition_keys": ["date", "ticker"],
            "media_types": [PayloadCodec(format=self.return_type).media_type],
        }


class StockDataPlanModel(CallableModel):
    daily_model: DailyAggregateModel = Field(default_factory=DailyAggregateModel)

    @property
    def context_type(self):
        return StockDataPlanContext

    @property
    def result_type(self):
        return GenericResult

    def plan_contexts(self, context: StockDataPlanContext) -> GenericResult:
        return GenericResult(
            value=[
                DailyAggregateContext(
                    ticker=symbol,
                    date=context.session_date,
                    adjusted=context.adjusted,
                    api_key=context.api_key,
                )
                for symbol in context.symbols
            ]
        )

    def plan_requests(self, context: StockDataPlanContext) -> GenericResult:
        return GenericResult(value=[self.daily_model.build_request(step_context) for step_context in self.plan_contexts(context).value])

    @Flow.call
    def __call__(self, context: StockDataPlanContext) -> GenericResult:
        return self.plan_requests(context)


def _date_value(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _artifact_uri(output: Any, key: str) -> str:
    if output is None:
        return key
    if hasattr(output, "artifact_uri"):
        return output.artifact_uri(key)
    if hasattr(output, "uri"):
        return output.uri(key)
    return key


def _artifact_write(
    *,
    output: Any,
    key: str,
    payload: Any,
    return_type: str,
    dataset_name: str,
    metadata: dict[str, Any],
    overwrite: bool,
    dry_run: bool = False,
) -> dict[str, Any]:
    codec = PayloadCodec(format=return_type)
    result = ArtifactWriteModel(store=output)(
        ArtifactWriteContext(
            key=key,
            payload=b"" if dry_run else codec.encode(payload),
            media_type=codec.media_type,
            dataset=dataset_name,
            stage="extract",
            overwrite=overwrite,
            dry_run=dry_run,
            metadata=metadata,
        )
    )
    return result.model_dump(mode="json")


def _existing_artifact(*, output: Any, key: str, return_type: str, dataset_name: str, metadata: dict[str, Any]) -> dict[str, Any]:
    artifact = ETLArtifact(
        key=key,
        stage="extract",
        dataset=dataset_name,
        uri=_artifact_uri(output, key),
        media_type=PayloadCodec(format=return_type).media_type,
        status="exists",
        metadata=metadata,
    )
    return {
        "key": artifact.key,
        "uri": artifact.uri,
        "status": "exists",
        "artifact": artifact.model_dump(mode="json"),
        "metadata": dict(artifact.metadata),
    }


class TickerOverviewModel(MassiveHTTPModel):
    path: str = "/v3/reference/tickers/{{ ticker }}"

    @property
    def context_type(self) -> type[ContextType]:
        return TickerOverviewContext

    def build_request(self, context: TickerOverviewContext) -> HTTPRequest:
        query = {**context.query, "date": _date_value(context.date)}
        return super().build_request(context.model_copy(update={"query": query}))


class DailyMarketSummaryModel(MassiveHTTPModel):
    path: str = "/v2/aggs/grouped/locale/us/market/stocks/{{ date }}"

    @property
    def context_type(self) -> type[ContextType]:
        return DailyMarketSummaryContext

    def build_request(self, context: DailyMarketSummaryContext) -> HTTPRequest:
        query = {**context.query, "adjusted": context.adjusted, "include_otc": context.include_otc}
        return super().build_request(context.model_copy(update={"query": query}))


class DailyTickerSummaryModel(MassiveHTTPModel):
    path: str = "/v1/open-close/{{ ticker }}/{{ date }}"

    @property
    def context_type(self) -> type[ContextType]:
        return DailyTickerSummaryContext

    def build_request(self, context: DailyTickerSummaryContext) -> HTTPRequest:
        query = {**context.query, "adjusted": context.adjusted}
        return super().build_request(context.model_copy(update={"query": query}))


class MassiveTickerOverviewExtractModel(_MassiveRESTExtractModel):
    overview_model: TickerOverviewModel = Field(default_factory=TickerOverviewModel)
    output_key_prefix: str = "massive/stocks/rest/ticker-overview"
    dataset_name: str = "massive-stocks-rest-ticker-overview"

    @property
    def context_type(self) -> type[ContextType]:
        return TickerOverviewContext

    def _request_model(self) -> TickerOverviewModel:
        return self.overview_model

    def output_key(self, context: TickerOverviewContext) -> str:
        suffix = PayloadCodec(format=self.return_type).suffix or ".bin"
        return f"{self.output_key_prefix.strip('/')}/{self.return_type}/{_date_value(context.date)}/{context.ticker}{suffix}"

    def _metadata(self, context: TickerOverviewContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "ticker": context.ticker, "provider": self.provider_name}

    def _plan_fields(self, context: TickerOverviewContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "ticker": context.ticker}

    def _request_error_result(self, context: TickerOverviewContext, payload: dict[str, Any], exc: RuntimeError) -> GenericResult | None:
        if "failed with status 404" not in str(exc):
            return None
        return GenericResult(
            value={
                **payload,
                "status": "skipped",
                "skip_reason": "not_found",
                "will_call_network": True,
                "will_publish_output": False,
                "status_code": 404,
                "attempts": 1,
                "rate_limit": {},
                "retry_events": [],
                "retry_summary": {},
                "output_writes": [],
                "error": str(exc),
            }
        )

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "endpoint": "/v3/reference/tickers/{ticker}",
            "partition_keys": ["date", "ticker"],
            "media_types": [PayloadCodec(format=self.return_type).media_type],
        }


class MassiveDailyMarketSummaryExtractModel(_MassiveRESTExtractModel):
    market_summary_model: DailyMarketSummaryModel = Field(default_factory=DailyMarketSummaryModel)
    output_key_prefix: str = "massive/stocks/rest/daily-market-summary"
    dataset_name: str = "massive-stocks-rest-daily-market-summary"

    @property
    def context_type(self) -> type[ContextType]:
        return DailyMarketSummaryContext

    def _request_model(self) -> DailyMarketSummaryModel:
        return self.market_summary_model

    def output_key(self, context: DailyMarketSummaryContext) -> str:
        suffix = PayloadCodec(format=self.return_type).suffix or ".bin"
        return f"{self.output_key_prefix.strip('/')}/{self.return_type}/{_date_value(context.date)}{suffix}"

    def _metadata(self, context: DailyMarketSummaryContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "provider": self.provider_name, "market": "stocks"}

    def _plan_fields(self, context: DailyMarketSummaryContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "adjusted": context.adjusted, "include_otc": context.include_otc}

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "endpoint": "/v2/aggs/grouped/locale/us/market/stocks/{date}",
            "partition_keys": ["date"],
            "media_types": [PayloadCodec(format=self.return_type).media_type],
        }


class MassiveDailyTickerSummaryExtractModel(_MassiveRESTExtractModel):
    summary_model: DailyTickerSummaryModel = Field(default_factory=DailyTickerSummaryModel)
    output_key_prefix: str = "massive/stocks/rest/daily-ticker-summary"
    dataset_name: str = "massive-stocks-rest-daily-ticker-summary"

    @property
    def context_type(self) -> type[ContextType]:
        return DailyTickerSummaryContext

    def _request_model(self) -> DailyTickerSummaryModel:
        return self.summary_model

    def output_key(self, context: DailyTickerSummaryContext) -> str:
        suffix = PayloadCodec(format=self.return_type).suffix or ".bin"
        return f"{self.output_key_prefix.strip('/')}/{self.return_type}/{_date_value(context.date)}/{context.ticker}{suffix}"

    def _metadata(self, context: DailyTickerSummaryContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "ticker": context.ticker, "provider": self.provider_name}

    def _plan_fields(self, context: DailyTickerSummaryContext) -> dict[str, Any]:
        return {"date": _date_value(context.date), "ticker": context.ticker, "adjusted": context.adjusted}

    def _request_error_result(self, context: DailyTickerSummaryContext, payload: dict[str, Any], exc: RuntimeError) -> GenericResult | None:
        if "failed with status 404" not in str(exc):
            return None
        return GenericResult(
            value={
                **payload,
                "status": "skipped",
                "skip_reason": "not_found",
                "will_call_network": True,
                "will_publish_output": False,
                "status_code": 404,
                "attempts": 1,
                "rate_limit": {},
                "retry_events": [],
                "retry_summary": {},
                "output_writes": [],
                "error": str(exc),
            }
        )

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "endpoint": "/v1/open-close/{stocksTicker}/{date}",
            "partition_keys": ["date", "ticker"],
            "media_types": [PayloadCodec(format=self.return_type).media_type],
        }


class MassiveDailyTickerSummaryModel(CallableModel):
    daily_model: DailyAggregateModel = Field(default_factory=DailyAggregateModel)
    artifact_writer: ArtifactWriteModel | None = None
    tickers: list[str] = Field(default_factory=lambda: ["SPY"])
    calendar: str = "/calendars/exchange/NYSE"
    adjusted: bool = True
    explain: bool = False
    transform_version: str = "raw"
    return_type: str = "json"
    dataset_name: str = "massive-daily-ticker-summary"
    dataset_description: str = "Massive daily aggregate payloads for configured stock tickers."
    schema_name: str = "massive_daily_aggregate_response"
    schema_version: str = "1"
    partition_keys: list[str] = Field(default_factory=lambda: ["date", "ticker"])
    cadence: str = "1D"
    media_types: list[str] = Field(default_factory=lambda: ["application/json"])
    quality_expectations: list[str] = Field(default_factory=lambda: ["one payload per ticker/date", "provider response status is OK"])
    output_hints: dict[str, Any] = Field(
        default_factory=lambda: {"raw_prefix": "massive/stocks/rest/ticker-summary/{return_type}/{date}/{ticker}.{extension}"}
    )
    provider_name: str = "massive"
    provider_type: str = "http"
    provider_capabilities: list[str] = Field(
        default_factory=lambda: ["templated_http_requests", "pagination", "http_status_retry", "rate_limit_headers"]
    )
    provider_rate_limit: dict[str, Any] = Field(default_factory=lambda: {"source": "provider_headers"})
    provider_retry: dict[str, Any] = Field(default_factory=lambda: {"retry_status_codes": [429, 500, 502, 503, 504]})
    provider_request_templates: dict[str, Any] = Field(
        default_factory=lambda: {"daily_aggregate": "/v2/aggs/ticker/{ticker}/range/1/day/{date}/{date}"}
    )

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyTickerSummaryContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def _daily_contexts(self, context: MassiveDailyTickerSummaryContext) -> list[DailyAggregateContext]:
        return [DailyAggregateContext(ticker=ticker, date=context.date, adjusted=self.adjusted) for ticker in self.tickers]

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset_name,
            "description": self.dataset_description,
            "schema_name": self.schema_name,
            "schema_version": self.schema_version,
            "return_type": self.return_type,
            "partition_keys": list(self.partition_keys),
            "cadence": self.cadence,
            "media_types": list(self.media_types),
            "quality_expectations": list(self.quality_expectations),
            "output_hints": dict(self.output_hints),
        }

    def provider_metadata(self) -> dict[str, Any]:
        return {
            "name": self.provider_name,
            "provider_type": self.provider_type,
            "capabilities": list(self.provider_capabilities),
            "rate_limit": dict(self.provider_rate_limit),
            "retry": dict(self.provider_retry),
            "request_templates": dict(self.provider_request_templates),
        }

    def _output_key(self, context: MassiveDailyTickerSummaryContext, ticker: str) -> str:
        date_value = context.date.isoformat() if isinstance(context.date, date) else str(context.date)
        raw_prefix = self.output_hints.get("raw_prefix", "{date}/{ticker}.json")
        extension = "json" if self.return_type == "json" else self.return_type
        return raw_prefix.format(date=date_value, ticker=ticker, return_type=self.return_type, extension=extension)

    def _write_outputs(self, context: MassiveDailyTickerSummaryContext, raw_payloads: list[Any] | None = None, *, dry_run: bool = False) -> list[Any]:
        if self.artifact_writer is None:
            return []
        raw_payloads = raw_payloads or [{} for _ in self.tickers]
        codec = PayloadCodec(format=self.return_type)
        return [
            self.artifact_writer(
                ArtifactWriteContext(
                    key=self._output_key(context, ticker),
                    payload=b"" if dry_run else codec.encode(raw_payload),
                    media_type=codec.media_type,
                    dataset=self.dataset_name,
                    stage="extract",
                    dry_run=dry_run,
                )
            )
            for ticker, raw_payload in zip(self.tickers, raw_payloads)
        ]

    @Flow.call
    def __call__(self, context: MassiveDailyTickerSummaryContext) -> GenericResult:
        daily_contexts = self._daily_contexts(context)
        requests = [safe_request_dump(self.daily_model.build_request(daily_context)) for daily_context in daily_contexts]
        output_results = self._write_outputs(context, dry_run=True) if self.explain else []
        payload = {
            "dataset": self.dataset_name,
            "provider": self.provider_name,
            "date": context.date.isoformat() if isinstance(context.date, date) else str(context.date),
            "calendar": self.calendar,
            "tickers": list(self.tickers),
            "adjusted": self.adjusted,
            "return_type": self.return_type,
            "output_keys": [self._output_key(context, ticker) for ticker in self.tickers],
            "output_writes": [result.model_dump(mode="json") for result in output_results],
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "dataset_metadata": self.dataset_metadata(),
            "provider_metadata": self.provider_metadata(),
            "base_models": {
                "http": "ccflow_http.HTTPModel",
                "request_model": f"{self.daily_model.__class__.__module__}.{self.daily_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3Model", "ccflow_s3.S3CacheStore"],
            },
            "requests": requests,
        }
        if self.explain:
            return GenericResult(value=payload)
        if not os.environ.get("MASSIVE_API_KEY"):
            raise ValueError("massive-daily-ticker-summary requires MASSIVE_API_KEY")
        results = [self.daily_model(daily_context).model_dump(mode="json") for daily_context in daily_contexts]
        output_results = self._write_outputs(context, results, dry_run=False)
        return GenericResult(
            value={
                **payload,
                "will_call_network": True,
                "results": results,
                "output_writes": [result.model_dump(mode="json") for result in output_results],
            }
        )


class MassiveAllStocksDailySummaryModel(CallableModel):
    tickers_model: Any = Field(default_factory=TickersModel)
    summary_model: MassiveDailyTickerSummaryModel = Field(default_factory=MassiveDailyTickerSummaryModel)
    output: Any | None = None
    explain: bool = False
    market: str = "stocks"
    active: bool = True
    limit: int = 1000
    max_tickers: int | None = None

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveDailyTickerSummaryContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def _tickers_context(self, context: MassiveDailyTickerSummaryContext) -> TickersContext:
        return TickersContext(market=self.market, active=self.active, active_date=context.date, limit=self.limit)

    def _ticker_values(self, payload: Any) -> list[str]:
        items = payload.get("results", []) if isinstance(payload, dict) else payload
        tickers = [item.get("ticker") for item in items or [] if isinstance(item, dict) and item.get("ticker")]
        return tickers[: self.max_tickers] if self.max_tickers is not None else tickers

    @Flow.call
    def __call__(self, context: MassiveDailyTickerSummaryContext) -> GenericResult:
        ticker_context = self._tickers_context(context)
        ticker_request = safe_request_dump(self.tickers_model.build_request(ticker_context))
        payload = {
            "dataset": "massive-all-stocks-daily-ticker-summary",
            "provider": "massive",
            "date": context.date.isoformat() if isinstance(context.date, date) else str(context.date),
            "calendar": self.summary_model.calendar,
            "return_type": self.summary_model.return_type,
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "ticker_universe_request": ticker_request,
            "summary_dataset_metadata": self.summary_model.dataset_metadata(),
            "summary_provider_metadata": self.summary_model.provider_metadata(),
            "base_models": {
                "universe": f"{self.tickers_model.__class__.__module__}.{self.tickers_model.__class__.__name__}",
                "summary": f"{self.summary_model.__class__.__module__}.{self.summary_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3ArtifactStore"],
            },
        }
        if self.explain:
            return GenericResult(value={**payload, "status": "planned", "ticker_count": None})
        ticker_result = self.tickers_model(ticker_context)
        tickers = self._ticker_values(ticker_result.value)
        artifact_writer = ArtifactWriteModel(store=self.output) if self.output is not None else self.summary_model.artifact_writer
        summary_model = self.summary_model.model_copy(update={"tickers": tickers, "artifact_writer": artifact_writer, "explain": False})
        summary = summary_model(context).value
        return GenericResult(
            value={
                **payload,
                "status": "written",
                "will_call_network": True,
                "ticker_count": len(tickers),
                "tickers": tickers,
                "summary": summary,
            }
        )


class MassiveFlatFileTransferModel(CallableModel):
    dataset: MassiveStockFlatFileDataset = "day-aggs"
    source_client: Any | None = None
    source_bucket: str = "flatfiles"
    output: Any | None = None
    output_key_prefix: str = "massive/stocks/flat-files"
    local_dir: Path = Path("/tmp/ccflow-massive-flat-files")
    media_type: str = "application/gzip"
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[ContextType]:
        return MassiveFlatFileContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def dataset_metadata(self) -> dict[str, Any]:
        metadata = _MASSIVE_STOCK_FLAT_FILES[self.dataset]
        return {
            "name": f"massive-stocks-flat-files-{self.dataset}",
            "description": metadata["description"],
            "source_path": metadata["path"],
            "partition_keys": ["date"],
            "cadence": "1D",
            "media_types": ["text/csv; charset=utf-8", "application/gzip"],
            "provider_type": "s3",
        }

    def _date_parts(self, context: MassiveFlatFileContext) -> tuple[str, str, str]:
        value = context.date if isinstance(context.date, date) else date.fromisoformat(str(context.date))
        return f"{value:%Y}", f"{value:%m}", value.isoformat()

    def source_key(self, context: MassiveFlatFileContext) -> str:
        year, month, date_value = self._date_parts(context)
        return f"{_MASSIVE_STOCK_FLAT_FILES[self.dataset]['path']}/{year}/{month}/{date_value}.csv.gz"

    def output_key(self, context: MassiveFlatFileContext) -> str:
        year, month, date_value = self._date_parts(context)
        return f"{self.output_key_prefix.strip('/')}/{self.dataset}/{year}/{month}/{date_value}.csv.gz"

    def local_path(self, context: MassiveFlatFileContext) -> Path:
        year, month, date_value = self._date_parts(context)
        return self.local_dir / self.dataset / year / month / f"{date_value}.csv.gz"

    def _artifact_uri(self, key: str) -> str:
        if self.output is None:
            return key
        if hasattr(self.output, "artifact_uri"):
            return self.output.artifact_uri(key)
        if hasattr(self.output, "uri"):
            return self.output.uri(key)
        return key

    def _output_write_record(self, key: str, status: str, metadata: dict | None = None) -> dict:
        metadata = metadata or {}
        artifact = ETLArtifact(
            key=key,
            stage="extract",
            dataset=f"massive-stocks-flat-files-{self.dataset}",
            uri=self._artifact_uri(key),
            media_type=self.media_type,
            status=status,
            metadata=metadata,
        )
        return {"key": key, "uri": artifact.uri, "status": status, "artifact": artifact.model_dump(mode="json"), "metadata": metadata}

    def _output_exists(self, key: str) -> bool:
        if self.output is None or not hasattr(self.output, "exists"):
            return False
        return self.output.exists(key)

    def _source_s3_client(self):
        if self.source_client is None:
            raise ValueError("Massive flat-file transfer requires source_client.")
        return getattr(self.source_client, "client", self.source_client)

    def _download(self, source_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        client = self._source_s3_client()
        if hasattr(client, "download_file"):
            client.download_file(Bucket=self.source_bucket, Key=source_key, Filename=str(local_path))
            return
        response = client.get_object(Bucket=self.source_bucket, Key=source_key)
        local_path.write_bytes(response["Body"].read())

    def _publish_output(self, key: str, local_path: Path, context: MassiveFlatFileContext) -> list[dict]:
        if self.output is None:
            raise ValueError("Massive flat-file transfer requires output.")
        metadata = {
            "dataset": f"massive-stocks-flat-files-{self.dataset}",
            "date": context.date.isoformat() if isinstance(context.date, date) else str(context.date),
            "source_bucket": self.source_bucket,
            "source_key": self.source_key(context),
        }
        if not self.overwrite_output and self._output_exists(key):
            return [self._output_write_record(key, "exists", metadata)]
        if hasattr(self.output, "write_file"):
            response = self.output.write_file(key, local_path, media_type=self.media_type, metadata=metadata)
            response_metadata = response if isinstance(response, dict) else {}
            status = str(response_metadata.get("status", "written"))
            return [self._output_write_record(key, status, {**metadata, **response_metadata})]
        writer = ArtifactWriteModel(store=self.output)
        result = writer(
            ArtifactWriteContext(
                key=key,
                payload=local_path.read_bytes(),
                media_type=self.media_type,
                dataset=f"massive-stocks-flat-files-{self.dataset}",
                stage="extract",
                overwrite=self.overwrite_output,
                metadata=metadata,
            )
        )
        return [result.model_dump(mode="json")]

    def _plan(self, context: MassiveFlatFileContext) -> dict:
        source_key = self.source_key(context)
        output_key = self.output_key(context)
        return {
            "dataset": f"massive-stocks-flat-files-{self.dataset}",
            "provider": "massive",
            "date": context.date.isoformat() if isinstance(context.date, date) else str(context.date),
            "source_bucket": self.source_bucket,
            "source_key": source_key,
            "source_uri": f"s3://{self.source_bucket}/{source_key}",
            "output_key": output_key,
            "output_uri": self._artifact_uri(output_key) if self.output is not None else None,
            "local_path": str(self.local_path(context)),
            "will_download": False,
            "will_publish_output": False,
            "output_writes": [self._output_write_record(output_key, "planned", {})] if self.output is not None else [],
            "required_env": ["MASSIVE_API_KEY_ID", "MASSIVE_API_KEY"],
            "dataset_metadata": self.dataset_metadata(),
            "base_models": {"source": "ccflow_s3.S3Client", "storage": ["ccflow_s3.S3ArtifactStore"]},
        }

    @Flow.call
    def __call__(self, context: MassiveFlatFileContext) -> GenericResult:
        payload = self._plan(context)
        if self.explain:
            return GenericResult(value={**payload, "status": "planned"})
        if self.output is None:
            raise ValueError("Massive flat-file transfer requires output.")
        if not self.overwrite_output and self._output_exists(payload["output_key"]):
            output_writes = [self._output_write_record(payload["output_key"], "exists", {"source_key": payload["source_key"]})]
            return GenericResult(value={**payload, "status": "exists", "output_writes": output_writes})
        local_path = self.local_path(context)
        self._download(payload["source_key"], local_path)
        output_writes = self._publish_output(payload["output_key"], local_path, context)
        status = output_writes[0]["status"] if output_writes else "written"
        return GenericResult(value={**payload, "status": status, "will_download": True, "will_publish_output": True, "output_writes": output_writes})


class DailyAggregateBackfillModel(CallableModel):
    daily_model: DailyAggregateModel = Field(default_factory=DailyAggregateModel)

    @property
    def context_type(self):
        return DailyAggregateBackfillContext

    @property
    def result_type(self):
        return GenericResult

    def plan_requests(self, context: DailyAggregateBackfillContext) -> GenericResult:
        return GenericResult(value=[self.daily_model.build_request(step_context) for step_context in context.step_contexts()])

    @Flow.call
    def __call__(self, context: DailyAggregateBackfillContext) -> GenericResult:
        return GenericResult(value=[self.daily_model(step_context) for step_context in context.step_contexts()])
