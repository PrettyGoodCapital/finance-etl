import os
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

from ccflow import CallableModel, DateContext, Flow, GenericResult
from ccflow_etl import APITokenCredentials, BackfillContext
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext
from pydantic import Field, model_validator

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
    "MassiveRequestContext",
    "MarketCalendarContext",
    "DailyAggregateContext",
    "DailyAggregateBackfillContext",
    "DailyAggregateBackfillModel",
    "MassiveCredentials",
    "MassiveHTTPModel",
    "MarketCalendarModel",
    "MarketHolidaysModel",
    "ExchangesModel",
    "TickersContext",
    "TickersModel",
    "TickerUniversePlanContext",
    "TickerUniversePlanModel",
    "StockDataPlanContext",
    "StockDataPlanModel",
    "DailyAggregateModel",
)


class MassiveCredentials(APITokenCredentials):
    token_env: Optional[str] = "MASSIVE_API_KEY"
    query_param: str = "apiKey"

    def api_key(self) -> Optional[str]:
        return self.resolved_token()


class MassiveRequestContext(HTTPRequestContext):
    api_key: Optional[str] = None
    credentials: Optional[MassiveCredentials] = None


class MarketCalendarContext(MassiveRequestContext):
    start_date: date
    end_date: date
    exchange: Optional[str] = None
    holidays: Any = Field(default_factory=list)
    exchanges: Any = Field(default_factory=list)


class DailyAggregateContext(MassiveRequestContext, DateContext):
    ticker: str
    adjusted: bool = True


class TickersContext(MassiveRequestContext):
    market: str = "stocks"
    active: bool = True
    active_date: Optional[date] = None
    limit: int = 1000


class TickerUniversePlanContext(MassiveRequestContext):
    session_dates: List[date]
    market: str = "stocks"
    active: bool = True
    limit: int = 1000


class DailyAggregateBackfillContext(BackfillContext[DailyAggregateContext]):
    ticker: str
    adjusted: bool = True
    api_key: Optional[str] = None
    session_dates: Optional[List[date]] = None

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

    def step_contexts(self) -> List[DailyAggregateContext]:
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
    symbols: List[str]
    adjusted: bool = True


class MassiveHTTPModel(HTTPModel):
    base_url: str = "https://api.massive.com"
    credentials: MassiveCredentials = Field(default_factory=MassiveCredentials)
    api_key_env: str = "MASSIVE_API_KEY"
    api_key: Optional[str] = None

    def _api_key(self, context: MassiveRequestContext) -> Optional[str]:
        context_credentials = context.credentials.api_key() if context.credentials else None
        return context.api_key or context_credentials or self.api_key or self.credentials.api_key() or os.environ.get(self.api_key_env)

    def build_request(self, context: Optional[MassiveRequestContext] = None) -> HTTPRequest:
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

    def _holiday_items(self, holidays: Any) -> List[Dict[str, Any]]:
        if isinstance(holidays, dict):
            return holidays.get("results", [])
        return list(holidays or [])

    def _exchange_items(self, exchanges: Any) -> List[Dict[str, Any]]:
        if isinstance(exchanges, dict):
            return exchanges.get("results", [])
        return list(exchanges or [])

    def _exchange_matches(self, exchange_metadata: Dict[str, Any], exchange: str) -> bool:
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

    def _exchange_is_known(self, exchanges: Any, exchange: Optional[str]) -> bool:
        if exchange is None:
            return True
        exchange_items = self._exchange_items(exchanges)
        if not exchange_items:
            return True
        return any(self._exchange_matches(exchange_metadata, exchange) for exchange_metadata in exchange_items)

    def _holiday_date(self, holiday: Dict[str, Any]) -> Optional[date]:
        holiday_date = holiday.get("date")
        if isinstance(holiday_date, date):
            return holiday_date
        if isinstance(holiday_date, str):
            return date.fromisoformat(holiday_date[:10])
        return None

    def _holiday_matches_exchange(self, holiday: Dict[str, Any], exchange: Optional[str]) -> bool:
        if exchange is None:
            return True
        holiday_exchange = holiday.get("exchange") or holiday.get("market")
        if holiday_exchange is None:
            return True
        return str(holiday_exchange).casefold() == exchange.casefold()

    def _closed_holiday_dates(self, holidays: List[Dict[str, Any]], exchange: Optional[str]) -> set[date]:
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

    def _uses_us_stock_calendar(self, exchanges: Any, exchange: Optional[str]) -> bool:
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

    def session_dates(self, context: MarketCalendarContext) -> List[date]:
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
    query: dict = {"asset_class": "stocks", "locale": "us"}


class TickersModel(MassiveHTTPModel):
    path: str = "/v3/reference/tickers"
    query: dict = {"market": "stocks", "active": True}
    paginate: bool = True
    max_pages: int = 1000

    def build_request(self, context: Optional[TickersContext] = None) -> HTTPRequest:
        context = context or TickersContext()
        query = {**context.query, "market": context.market, "active": context.active, "limit": context.limit}
        if context.active_date is not None:
            query["date"] = context.active_date.isoformat()
        return super().build_request(context.model_copy(update={"query": query}))


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
    query: dict = {"sort": "asc", "limit": 50000}

    def build_request(self, context: DailyAggregateContext) -> HTTPRequest:
        context = context.model_copy(update={"query": {**context.query, "adjusted": context.adjusted}})
        return super().build_request(context)


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
