import gzip
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, GraphDepList
from ccflow_etl import (
    APITokenCredentials,
    DatasetDefinition,
    PayloadCodec,
    resolve_model,
)
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext, safe_request_dump
from pydantic import Field, field_validator, model_validator

from finance_etl.providers._dataset_output import before_first_date
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
    "ConditionsModel",
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
    "MassiveAllTickersContext",
    "MassiveAllTickersModel",
    "MassiveCredentials",
    "MassiveDailyMarketSummaryExtractModel",
    "MassiveDatedReferenceContext",
    "MassiveDatedReferenceExtractModel",
    "MassiveDatedReferenceModel",
    "MassiveDatedSymbolUniverseModel",
    "MassiveFlatFileContext",
    "MassiveFlatFileTransferModel",
    "MassiveHTTPModel",
    "MassiveReferenceSnapshotContext",
    "MassiveReferenceSnapshotExtractModel",
    "MassiveRequestContext",
    "MassiveTickerEventsExtractModel",
    "MassiveTickerOverviewBundleExtractModel",
    "TickerEventsContext",
    "TickerEventsModel",
    "TickerOverviewContext",
    "TickerOverviewModel",
    "TickerUniversePlanContext",
    "TickerUniversePlanModel",
    "TickersContext",
    "TickersModel",
    "diff_ticker_snapshots",
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


class MassiveAllTickersContext(MassiveRequestContext, DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class MassiveDatedReferenceContext(MassiveRequestContext, DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class MassiveReferenceSnapshotContext(MassiveRequestContext, DateContext):
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
    market: MassiveTickerMarket = "stocks"
    active: bool = True
    limit: int = 1000


class MassiveHTTPModel(HTTPModel):
    base_url: str = "https://api.massive.com"
    credentials: MassiveCredentials = Field(default_factory=MassiveCredentials)
    api_key_env: str = "MASSIVE_API_KEY"
    api_key: str | None = None

    def _api_key(self, context: MassiveRequestContext) -> str | None:
        context_credentials = context.credentials.api_key() if context.credentials else None
        return context.api_key or context_credentials or self.api_key or self.credentials.api_key() or os.environ.get(self.api_key_env)

    def build_request(self, context: MassiveRequestContext | None = None) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
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


class ConditionsModel(MassiveHTTPModel):
    path: str = "/v3/reference/conditions"
    query: dict = Field(default_factory=lambda: {"asset_class": "stocks", "limit": 1000})
    paginate: bool = True
    max_pages: int = 1000


class TickersModel(MassiveHTTPModel):
    path: str = "/v3/reference/tickers"
    query: dict = Field(default_factory=lambda: {"market": "stocks", "active": True})
    paginate: bool = True
    max_pages: int = 1000

    def build_request(self, context: TickersContext | None = None) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
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


class MassiveDatedReferenceModel(MassiveHTTPModel):
    date_field: str
    limit: int | None = Field(default=5000, ge=1, le=5000)
    sort: str | None = None
    paginate: bool = True
    max_pages: int = 1000

    @property
    def context_type(self) -> type[MassiveDatedReferenceContext]:
        return MassiveDatedReferenceContext

    def build_request(self, context: MassiveDatedReferenceContext) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
        query = {**context.query, self.date_field: _date_value(context.date), "limit": self.limit}
        if self.sort is not None:
            query["sort"] = self.sort
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
    def context_type(self) -> type[MassiveAllTickersContext]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[GenericResult]:
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
    dataset: DatasetDefinition = Field(description="Date-partitioned dataset the daily ticker snapshot is written to.")
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
    overwrite_output: bool = False
    provider_name: str = "massive"
    provider_type: str = "http"
    provider_capabilities: list[str] = Field(default_factory=lambda: ["pagination", "http_status_retry", "rate_limit_headers"])
    provider_retry: dict[str, Any] = Field(default_factory=lambda: {"retry_status_codes": [429, 500, 502, 503, 504]})
    provider_rate_limit: dict[str, Any] = Field(default_factory=lambda: {"source": "provider_headers"})

    @property
    def context_type(self) -> type[MassiveAllTickersContext]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[GenericResult]:
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

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset.name,
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

    @Flow.call
    def __call__(self, context: MassiveAllTickersContext) -> GenericResult:
        request_model = self._request_model()
        plan = {
            **self.dataset.plan(context.date),
            "provider": self.provider_name,
            "calendar": self.calendar,
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "will_publish_output": False,
            "ticker_request": safe_request_dump(request_model.build_request(self._tickers_context(context))),
            "dataset_metadata": self.dataset_metadata(),
            "provider_metadata": self.provider_metadata(),
            "base_models": {
                "http": "ccflow_http.HTTPModel",
                "request_model": f"{request_model.__class__.__module__}.{request_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3ArtifactStore"],
            },
        }
        if before_first_date(self.dataset, context.date):
            return GenericResult(value={**plan, "status": "before_first_date"})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.dataset.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})

        ticker_result = request_model(self._tickers_context(context))
        raw_payload = ticker_result.value if isinstance(ticker_result.value, dict) else {"results": ticker_result.value}
        metadata = {"provider": self.provider_name, "market": self.market}
        writes = self.dataset.write_bytes(
            context.date, PayloadCodec(format="json").encode(raw_payload), metadata=metadata, overwrite=self.overwrite_output
        )
        results = raw_payload.get("results", [])
        return GenericResult(
            value={
                **plan,
                "status": writes[-1]["status"],
                "will_call_network": True,
                "will_publish_output": True,
                "ticker_count": len(results) if isinstance(results, list) else None,
                "page_count": ticker_result.pages,
                "attempts": ticker_result.attempts,
                "rate_limit": ticker_result.rate_limit,
                "retry_events": ticker_result.retry_events,
                "retry_summary": ticker_result.retry_summary,
                "output_writes": writes,
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
    def context_type(self) -> type[DailyAggregateContext]:
        return DailyAggregateContext

    def build_request(self, context: DailyAggregateContext) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
        context = context.model_copy(update={"query": {**context.query, "adjusted": context.adjusted}})
        return super().build_request(context)


class _MassiveRESTExtractModel(CallableModel):
    """Fetch one Massive REST payload per date and write it as that date's JSON partition of ``dataset``."""

    dataset: DatasetDefinition = Field(description="Date-partitioned dataset the payloads are written to.")
    explain: bool = False
    overwrite_output: bool = False
    provider_name: str = "massive"

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _request_model(self) -> MassiveHTTPModel:
        raise NotImplementedError

    def _metadata(self, context: ContextType) -> dict[str, Any]:
        """Fields recorded in the plan and in the written artifact's metadata, besides the date."""
        raise NotImplementedError

    def dataset_metadata(self) -> dict[str, Any]:
        raise NotImplementedError

    @Flow.call
    def __call__(self, context: ContextType) -> GenericResult:
        request_model = self._request_model()
        value = cast(DateContext, context).date
        metadata = {"provider": self.provider_name, **self._metadata(context)}
        plan = {
            **self.dataset.plan(value),
            **metadata,
            "required_env": ["MASSIVE_API_KEY"],
            "will_call_network": False,
            "will_publish_output": False,
            "request": safe_request_dump(request_model.build_request(cast(MassiveRequestContext, context))),
            "dataset_metadata": self.dataset_metadata(),
            "base_models": {
                "http": "ccflow_http.HTTPModel",
                "request_model": f"{request_model.__class__.__module__}.{request_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3ArtifactStore"],
            },
        }
        if before_first_date(self.dataset, value):
            return GenericResult(value={**plan, "status": "before_first_date"})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.dataset.exists(value):
            return GenericResult(value={**plan, "status": "exists"})

        result = request_model(context)
        raw_payload = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
        writes = self.dataset.write_bytes(value, PayloadCodec(format="json").encode(raw_payload), metadata=metadata, overwrite=self.overwrite_output)
        return GenericResult(
            value={
                **plan,
                "status": writes[-1]["status"],
                "will_call_network": True,
                "will_publish_output": True,
                "status_code": getattr(result, "status_code", None),
                "attempts": getattr(result, "attempts", 1),
                "rate_limit": getattr(result, "rate_limit", {}),
                "retry_events": getattr(result, "retry_events", []),
                "retry_summary": getattr(result, "retry_summary", {}),
                "output_writes": writes,
            }
        )


class MassiveDatedReferenceExtractModel(_MassiveRESTExtractModel):
    reference_model: MassiveDatedReferenceModel
    reference_name: str

    @property
    def context_type(self) -> type[MassiveDatedReferenceContext]:
        return MassiveDatedReferenceContext

    def _request_model(self) -> MassiveDatedReferenceModel:
        return self.reference_model

    def _metadata(self, context: MassiveDatedReferenceContext) -> dict[str, Any]:
        return {"market": "stocks", "reference": self.reference_name}

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset.name,
            "endpoint": self.reference_model.path,
            "event_date_field": self.reference_model.date_field,
            "partition_keys": ["date"],
            "media_types": [self.dataset.resolved_media_type()],
            "pagination": {"mode": "next_url", "limit": self.reference_model.limit},
        }


class MassiveReferenceSnapshotExtractModel(_MassiveRESTExtractModel):
    reference_model: MassiveHTTPModel
    reference_name: str

    @property
    def context_type(self) -> type[MassiveReferenceSnapshotContext]:
        return MassiveReferenceSnapshotContext

    def _request_model(self) -> MassiveHTTPModel:
        return self.reference_model

    def _metadata(self, context: MassiveReferenceSnapshotContext) -> dict[str, Any]:
        return {"market": "stocks", "reference": self.reference_name}

    def dataset_metadata(self) -> dict[str, Any]:
        metadata = {
            "name": self.dataset.name,
            "endpoint": self.reference_model.path,
            "partition_keys": ["capture_date"],
            "media_types": [self.dataset.resolved_media_type()],
        }
        if self.reference_model.paginate:
            metadata["pagination"] = {"mode": "next_url"}
        return metadata


def _date_value(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


class TickerOverviewModel(MassiveHTTPModel):
    path: str = "/v3/reference/tickers/{{ ticker }}"

    @property
    def context_type(self) -> type[TickerOverviewContext]:
        return TickerOverviewContext

    def build_request(self, context: TickerOverviewContext) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
        query = {**context.query, "date": _date_value(context.date)}
        return super().build_request(context.model_copy(update={"query": query}))


class DailyMarketSummaryModel(MassiveHTTPModel):
    path: str = "/v2/aggs/grouped/locale/us/market/stocks/{{ date }}"

    @property
    def context_type(self) -> type[DailyMarketSummaryContext]:
        return DailyMarketSummaryContext

    def build_request(self, context: DailyMarketSummaryContext) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
        query = {**context.query, "adjusted": context.adjusted, "include_otc": context.include_otc}
        return super().build_request(context.model_copy(update={"query": query}))


class DailyTickerSummaryModel(MassiveHTTPModel):
    path: str = "/v1/open-close/{{ ticker }}/{{ date }}"

    @property
    def context_type(self) -> type[DailyTickerSummaryContext]:
        return DailyTickerSummaryContext

    def build_request(self, context: DailyTickerSummaryContext) -> HTTPRequest:  # ty: ignore[incompatible-method-override]
        query = {**context.query, "adjusted": context.adjusted}
        return super().build_request(context.model_copy(update={"query": query}))


class MassiveTickerOverviewBundleExtractModel(CallableModel):
    universe_model: CallableModel
    overview_model: TickerOverviewModel = Field(default_factory=TickerOverviewModel)
    dataset: DatasetDefinition = Field(description="Date-partitioned dataset each date's compressed JSONL bundle is written to.")
    staging_directory: Path = Path("/tmp/finance-etl/massive-ticker-overview")
    max_concurrency: int = Field(default=8, ge=1)
    max_symbols: int | None = Field(default=None, ge=1)
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[MassiveAllTickersContext]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def partial_path(self, context: MassiveAllTickersContext) -> Path:
        return self.staging_directory / f"{_date_value(context.date)}.jsonl.partial"

    def compressed_path(self, context: MassiveAllTickersContext) -> Path:
        return self.staging_directory / f"{_date_value(context.date)}.jsonl.gz"

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset.name,
            "endpoint": "/v3/reference/tickers/{ticker}",
            "partition_keys": ["date"],
            "media_types": [self.dataset.resolved_media_type()],
            "record_format": "jsonl",
            "compression": "gzip",
        }

    def _universe_context(self, context: MassiveAllTickersContext) -> ContextType:
        return self.universe_model.context_type.model_validate({"date": context.date})

    def _symbols(self, context: MassiveAllTickersContext) -> tuple[list[str], dict[str, Any]]:
        result = self.universe_model(self._universe_context(context))
        universe = result.value if isinstance(result, GenericResult) else result
        if not isinstance(universe, SymbolUniverseResult):
            universe = SymbolUniverseResult.model_validate(universe)
        symbols = universe.symbols[: self.max_symbols] if self.max_symbols is not None else universe.symbols
        return symbols, {
            "source": universe.source,
            "snapshot_uri": universe.snapshot_uri,
            "metadata": universe.metadata,
        }

    def _load_staged_records(self, path: Path) -> tuple[set[str], dict[str, int]]:
        completed = set()
        counts = {"ok": 0, "not_found": 0}
        if not path.exists():
            return completed, counts
        with path.open("rb+") as staged:
            while True:
                line_start = staged.tell()
                line = staged.readline()
                if not line:
                    break
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    if staged.read().strip():
                        raise ValueError(f"Invalid staged ticker overview JSONL at byte {line_start}: {path}") from None
                    staged.seek(line_start)
                    staged.truncate()
                    break
                ticker = record.get("ticker")
                status = record.get("status")
                if ticker and status in counts:
                    completed.add(str(ticker))
                    counts[status] += 1
        return completed, counts

    def _overview_record(self, context: MassiveAllTickersContext, ticker: str) -> dict[str, Any]:
        ticker_context = TickerOverviewContext(
            ticker=ticker,
            date=context.date,
            api_key=context.api_key,
            credentials=context.credentials,
        )
        try:
            result = self.overview_model(ticker_context)
        except RuntimeError as exc:
            if "failed with status 404" not in str(exc):
                raise
            result = None
        if result is None or getattr(result, "status_code", None) == 404:
            return {
                "date": _date_value(context.date),
                "ticker": ticker,
                "status": "not_found",
                "status_code": 404,
                "attempts": getattr(result, "attempts", 1),
                "response": None,
            }
        response = result.value if isinstance(result, GenericResult) else result.model_dump(mode="json")
        return {
            "date": _date_value(context.date),
            "ticker": ticker,
            "status": "ok",
            "status_code": getattr(result, "status_code", None),
            "attempts": getattr(result, "attempts", 1),
            "response": response,
        }

    def _append_records(
        self,
        context: MassiveAllTickersContext,
        path: Path,
        symbols: list[str],
        counts: dict[str, int],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as staged, ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            for offset in range(0, len(symbols), self.max_concurrency):
                batch = symbols[offset : offset + self.max_concurrency]
                for record in executor.map(lambda ticker: self._overview_record(context, ticker), batch):
                    staged.write(json.dumps(record, sort_keys=True, separators=(",", ":")))
                    staged.write("\n")
                    staged.flush()
                    counts[record["status"]] += 1

    def _compress(self, source_path: Path, destination_path: Path) -> None:
        with source_path.open("rb") as source, gzip.open(destination_path, "wb") as destination:
            while chunk := source.read(1024 * 1024):
                destination.write(chunk)

    def _will_fetch(self, context: MassiveAllTickersContext) -> bool:
        if self.explain or before_first_date(self.dataset, context.date):
            return False
        return self.overwrite_output or not self.dataset.exists(context.date)

    @Flow.deps
    def __deps__(self, context: MassiveAllTickersContext) -> GraphDepList:
        return [(self.universe_model, [self._universe_context(context)])] if self._will_fetch(context) else []

    @Flow.call
    def __call__(self, context: MassiveAllTickersContext) -> GenericResult:
        plan = {
            **self.dataset.plan(context.date),
            "provider": "massive",
            "staging_path": str(self.partial_path(context)),
            "max_concurrency": self.max_concurrency,
            "max_symbols": self.max_symbols,
            "will_call_network": False,
            "will_publish_output": False,
            "request_template": safe_request_dump(self.overview_model.build_request(TickerOverviewContext(ticker="{ticker}", date=context.date))),
            "dataset_metadata": self.dataset_metadata(),
            "base_models": {
                "universe": f"{self.universe_model.__class__.__module__}.{self.universe_model.__class__.__name__}",
                "http": f"{self.overview_model.__class__.__module__}.{self.overview_model.__class__.__name__}",
                "storage": ["ccflow_s3.S3ArtifactStore"],
            },
        }
        if before_first_date(self.dataset, context.date):
            return GenericResult(value={**plan, "status": "before_first_date"})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.dataset.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})

        symbols, universe = self._symbols(context)
        partial_path = self.partial_path(context)
        compressed_path = self.compressed_path(context)
        completed, counts = self._load_staged_records(partial_path)
        missing = [symbol for symbol in symbols if symbol not in completed]
        self._append_records(context, partial_path, missing, counts)
        self._compress(partial_path, compressed_path)
        metadata = {
            "provider": "massive",
            "format": "jsonl",
            "compression": "gzip",
            "ticker_count": len(symbols),
            "ok_count": counts["ok"],
            "not_found_count": counts["not_found"],
        }
        writes = self.dataset.write_file(context.date, compressed_path, metadata=metadata, overwrite=self.overwrite_output)
        partial_path.unlink(missing_ok=True)
        compressed_path.unlink(missing_ok=True)
        return GenericResult(
            value={
                **plan,
                "status": writes[-1]["status"],
                "will_call_network": bool(missing),
                "will_publish_output": True,
                "ticker_count": len(symbols),
                "requested_count": len(missing),
                "resumed_count": len(completed),
                "ok_count": counts["ok"],
                "not_found_count": counts["not_found"],
                "universe": universe,
                "output_writes": writes,
            }
        )


class TickerEventsContext(MassiveRequestContext):
    identifier: str = Field(description="Ticker, CUSIP, or composite FIGI.")


class TickerEventsModel(MassiveHTTPModel):
    """Ticker change history for one ticker, CUSIP, or composite FIGI; 404 means no events are known."""

    path: str = "/vX/reference/tickers/{{ identifier }}/events"
    accepted_status_codes: list[int] = Field(default_factory=lambda: [404])

    @property
    def context_type(self) -> type[TickerEventsContext]:
        return TickerEventsContext


def _index_snapshot(records: list[Any]) -> tuple[dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    by_figi: dict[str, list[dict[str, Any]]] = {}
    by_ticker: dict[str, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict) or not record.get("ticker"):
            continue
        if record.get("composite_figi"):
            by_figi.setdefault(record["composite_figi"], []).append(record)
        else:
            by_ticker[record["ticker"]] = record
    return by_figi, by_ticker


def _change_record(change: str, composite_figi: str | None, previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> dict[str, Any]:
    reference = (current or previous)[0]
    return {
        "change": change,
        "composite_figi": composite_figi,
        "tickers": sorted(record["ticker"] for record in current),
        "previous_tickers": sorted(record["ticker"] for record in previous),
        "name": reference.get("name"),
        "type": reference.get("type"),
        "primary_exchange": reference.get("primary_exchange"),
        "cik": reference.get("cik"),
    }


def diff_ticker_snapshots(previous: list[Any], current: list[Any]) -> list[dict[str, Any]]:
    """Diff two all-tickers snapshots on composite FIGI into added, removed, and ticker_changed records.

    Records without a composite FIGI are diffed by ticker, so they can only be added or removed.
    """
    previous_figis, previous_tickers = _index_snapshot(previous)
    current_figis, current_tickers = _index_snapshot(current)
    changes = []
    for figi in sorted(previous_figis.keys() | current_figis.keys()):
        before, after = previous_figis.get(figi, []), current_figis.get(figi, [])
        if {record["ticker"] for record in before} == {record["ticker"] for record in after}:
            continue
        change = "added" if not before else "removed" if not after else "ticker_changed"
        changes.append(_change_record(change, figi, before, after))
    for ticker in sorted(previous_tickers.keys() ^ current_tickers.keys()):
        if ticker in current_tickers:
            changes.append(_change_record("added", None, [], [current_tickers[ticker]]))
        else:
            changes.append(_change_record("removed", None, [previous_tickers[ticker]], []))
    return changes


class MassiveTickerEventsExtractModel(CallableModel):
    """Capture ticker events for securities that changed between consecutive all-tickers snapshots.

    A security is added or removed when its composite FIGI appears in or disappears from the active snapshot, and has
    a ticker change when its FIGI is listed under different tickers. Ticker events are requested only for those FIGIs,
    and one bundle per date holds the diff records with their events. Run over a range, this also fills history,
    because each date's events are requested for the securities that changed on that date.
    """

    dataset: DatasetDefinition = Field(description="Date-partitioned dataset each date's compressed JSONL bundle is written to.")
    snapshot: DatasetDefinition = Field(description="Dataset of daily all-tickers snapshots to diff.")
    snapshot_model: CallableModel | None = Field(
        default=None,
        description="Model that produces the snapshot for a date. It is declared as a dependency for the date and the previous session.",
    )
    calendar: Any = "/calendars/exchange/NYSE"
    events_model: TickerEventsModel = Field(default_factory=TickerEventsModel)
    max_concurrency: int = Field(default=4, ge=1)
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[MassiveAllTickersContext]:
        return MassiveAllTickersContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def previous_session(self, value: date) -> date | None:
        calendar = resolve_model(self.calendar)
        start, end = value - timedelta(days=14), value - timedelta(days=1)
        if hasattr(calendar, "business_days"):
            sessions = list(calendar.business_days(start, end))
        else:
            sessions = [step.date() if isinstance(step, datetime) else step for step in calendar.steps(start, end)]
        return sessions[-1] if sessions else None

    def _will_fetch(self, context: MassiveAllTickersContext) -> bool:
        if self.explain or before_first_date(self.dataset, context.date):
            return False
        return self.overwrite_output or not self.dataset.exists(context.date)

    def _snapshot_contexts(self, context: MassiveAllTickersContext) -> list[ContextType]:
        assert self.snapshot_model is not None
        dates = [value for value in (self.previous_session(context.date), context.date) if value is not None]
        return [self.snapshot_model.context_type.model_validate({"date": value}) for value in dates]

    @Flow.deps
    def __deps__(self, context: MassiveAllTickersContext) -> GraphDepList:
        if self.snapshot_model is None or not self._will_fetch(context):
            return []
        return [(self.snapshot_model, self._snapshot_contexts(context))]

    def _snapshot(self, value: date) -> list[Any] | None:
        if not self.snapshot.exists(value):
            return None
        payload = json.loads(self.snapshot.read_bytes(value))
        records = payload.get("results", []) if isinstance(payload, dict) else payload
        return records if isinstance(records, list) else []

    def _events(self, context: MassiveAllTickersContext, figi: str) -> dict[str, Any]:
        result = self.events_model(TickerEventsContext(identifier=figi, api_key=context.api_key, credentials=context.credentials))
        if result.status_code == 404:
            return {"events_status": "not_found", "events_status_code": 404, "events": []}
        results = result.value.get("results", {}) if isinstance(result.value, dict) else {}
        return {
            "events_status": "ok",
            "events_status_code": result.status_code,
            "events_name": results.get("name"),
            "events": results.get("events", []),
        }

    @Flow.call
    def __call__(self, context: MassiveAllTickersContext) -> GenericResult:
        previous = self.previous_session(context.date)
        plan = {
            **self.dataset.plan(context.date),
            "previous_date": _date_value(previous) if previous else None,
            "snapshot_keys": [self.snapshot.key(value) for value in (previous, context.date) if value is not None],
        }
        if before_first_date(self.dataset, context.date):
            return GenericResult(value={**plan, "status": "before_first_date"})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if not self._will_fetch(context):
            return GenericResult(value={**plan, "status": "exists"})
        if self.snapshot_model is not None:
            for snapshot_context in self._snapshot_contexts(context):
                self.snapshot_model(snapshot_context)
        current = self._snapshot(context.date)
        before = self._snapshot(previous) if previous is not None else None
        if current is None or before is None:
            return GenericResult(value={**plan, "status": "no_previous_snapshot" if current is not None else "no_snapshot"})
        changes = diff_ticker_snapshots(before, current)
        figis = [change["composite_figi"] for change in changes if change["composite_figi"]]
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            events = dict(zip(figis, executor.map(lambda figi: self._events(context, figi), figis)))
        records = [
            {"date": _date_value(context.date), "previous_date": plan["previous_date"], **change, **events.get(change["composite_figi"], {})}
            for change in changes
        ]
        payload = gzip.compress(b"".join(json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n" for record in records))
        counts = {change: sum(record["change"] == change for record in records) for change in ("added", "removed", "ticker_changed")}
        metadata = {
            "previous_date": plan["previous_date"],
            "provider": "massive",
            "record_count": len(records),
            **{f"{change}_count": count for change, count in counts.items()},
        }
        writes = self.dataset.write_bytes(context.date, payload, metadata=metadata, overwrite=self.overwrite_output)
        return GenericResult(
            value={
                **plan,
                "status": writes[-1]["status"],
                "record_count": len(records),
                **{f"{change}_count": count for change, count in counts.items()},
                "requested_count": len(figis),
                "not_found_count": sum(value["events_status"] == "not_found" for value in events.values()),
                "output_writes": writes,
            }
        )


class MassiveDailyMarketSummaryExtractModel(_MassiveRESTExtractModel):
    market_summary_model: DailyMarketSummaryModel = Field(default_factory=DailyMarketSummaryModel)

    @property
    def context_type(self) -> type[DailyMarketSummaryContext]:
        return DailyMarketSummaryContext

    def _request_model(self) -> DailyMarketSummaryModel:
        return self.market_summary_model

    def _metadata(self, context: DailyMarketSummaryContext) -> dict[str, Any]:
        return {"market": "stocks", "adjusted": context.adjusted, "include_otc": context.include_otc}

    def dataset_metadata(self) -> dict[str, Any]:
        return {
            "name": self.dataset.name,
            "endpoint": "/v2/aggs/grouped/locale/us/market/stocks/{date}",
            "partition_keys": ["date"],
            "media_types": [self.dataset.resolved_media_type()],
        }


_UNAVAILABLE_S3_CODES = frozenset({"403", "404", "AccessDenied", "Forbidden", "NoSuchKey", "NotFound"})


def _unavailable_s3_code(exc: Exception) -> str | None:
    """The S3 error code when a source object cannot be read (not entitled, outside the plan window, or unpublished)."""
    response = getattr(exc, "response", None) or {}
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if code in _UNAVAILABLE_S3_CODES or status in (403, 404):
        return code or str(status)
    return None


class MassiveFlatFileTransferModel(CallableModel):
    """Copy one day's Massive flat file to the dataset's stores through a local file, which is removed after publishing."""

    flat_file: MassiveStockFlatFileDataset = "day-aggs"
    source_client: Any | None = None
    source_bucket: str = "flatfiles"
    dataset: DatasetDefinition = Field(description="Date-partitioned dataset the flat files are copied to.")
    local_dir: Path = Path("/tmp/ccflow-massive-flat-files")
    keep_local_file: bool = False
    fail_on_unavailable: bool = Field(
        default=True,
        description="Raise when the source file cannot be read (403 or 404). History runs set this false to record the date as unavailable and continue.",
    )
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[MassiveFlatFileContext]:
        return MassiveFlatFileContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def dataset_metadata(self) -> dict[str, Any]:
        metadata = _MASSIVE_STOCK_FLAT_FILES[self.flat_file]
        return {
            "name": self.dataset.name,
            "description": metadata["description"],
            "source_path": metadata["path"],
            "partition_keys": ["date"],
            "cadence": "1D",
            "media_types": ["text/csv; charset=utf-8", "application/gzip"],
            "provider_type": "s3",
        }

    def source_key(self, context: MassiveFlatFileContext) -> str:
        value = context.date
        return f"{_MASSIVE_STOCK_FLAT_FILES[self.flat_file]['path']}/{value:%Y}/{value:%m}/{value.isoformat()}.csv.gz"

    def local_path(self, context: MassiveFlatFileContext) -> Path:
        value = context.date
        return self.local_dir / self.flat_file / f"{value:%Y}" / f"{value:%m}" / f"{value.isoformat()}.csv.gz"

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

    @Flow.call
    def __call__(self, context: MassiveFlatFileContext) -> GenericResult:
        source_key = self.source_key(context)
        plan = {
            **self.dataset.plan(context.date),
            "provider": "massive",
            "source_bucket": self.source_bucket,
            "source_key": source_key,
            "source_uri": f"s3://{self.source_bucket}/{source_key}",
            "local_path": str(self.local_path(context)),
            "will_download": False,
            "will_publish_output": False,
            "required_env": ["MASSIVE_API_KEY_ID", "MASSIVE_API_KEY"],
            "dataset_metadata": self.dataset_metadata(),
            "base_models": {"source": "ccflow_s3.S3Client", "storage": ["ccflow_s3.S3ArtifactStore"]},
        }
        if before_first_date(self.dataset, context.date):
            return GenericResult(value={**plan, "status": "before_first_date"})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.dataset.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})
        local_path = self.local_path(context)
        try:
            try:
                self._download(source_key, local_path)
            except Exception as exc:
                code = _unavailable_s3_code(exc)
                if code is None or self.fail_on_unavailable:
                    raise
                return GenericResult(value={**plan, "status": "unavailable", "will_download": True, "error_code": code})
            metadata = {"source_bucket": self.source_bucket, "source_key": source_key}
            writes = self.dataset.write_file(context.date, local_path, metadata=metadata, overwrite=self.overwrite_output)
        finally:
            if not self.keep_local_file:
                local_path.unlink(missing_ok=True)
        return GenericResult(
            value={**plan, "status": writes[-1]["status"], "will_download": True, "will_publish_output": True, "output_writes": writes}
        )
