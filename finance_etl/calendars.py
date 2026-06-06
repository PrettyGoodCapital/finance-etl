from __future__ import annotations

from datetime import date, datetime, time, timedelta

from ccflow import ModelRegistry
from ccflow_etl import BaseCalendar
from finance_dates import COUNTRY_CODES, COUNTRY_CODES3, EXCHANGE_CODES, Calendar
from pydantic import Field

__all__ = (
    "FinanceDatesCalendar",
    "ExchangeCalendarRegistry",
    "CountryCalendarRegistry",
    "ExchangeCalendar",
    "ExchangeNonTradingDaysCalendar",
    "ExchangeSessionIntervalCalendar",
    "ExchangeSessionStartCalendar",
    "ExchangeSessionEndCalendar",
    "RegionCalendar",
    "CountryCalendar",
)


_EXCHANGE_CODE_SET = frozenset(EXCHANGE_CODES)
_COUNTRY_CODE_SET = frozenset((*COUNTRY_CODES, *COUNTRY_CODES3))
_COUNTRY_CODES = tuple(dict.fromkeys((*COUNTRY_CODES, *COUNTRY_CODES3)))
_EXCHANGE_ALIASES = {
    "NYSE": "XNYS",
    "NASDAQ": "XNAS",
    "LSE": "XLON",
    "TSE": "XTKS",
    "TOKYO": "XTKS",
    "HKEX": "XHKG",
}

_EXCHANGE_SESSION_VARIANTS = {
    "trading_days": ("trading_days", None),
    "non_trading_days": ("non_trading_days", None),
    "session/starts": ("session_starts", None),
    "session/start": ("session_starts", None),
    "session_starts": ("session_starts", None),
    "trading_session_starts": ("session_starts", None),
    "session/ends": ("session_ends", None),
    "session/end": ("session_ends", None),
    "session_ends": ("session_ends", None),
    "trading_session_ends": ("session_ends", None),
    "session/hours": ("session_interval", 60),
    "session/hour": ("session_interval", 60),
    "session/60_minutes": ("session_interval", 60),
    "session/60m": ("session_interval", 60),
    "trading_session_hours": ("session_interval", 60),
    "session/30_minutes": ("session_interval", 30),
    "session/30m": ("session_interval", 30),
    "trading_session_30_minutes": ("session_interval", 30),
    "session/15_minutes": ("session_interval", 15),
    "session/15m": ("session_interval", 15),
    "trading_session_15_minutes": ("session_interval", 15),
    "session/5_minutes": ("session_interval", 5),
    "session/5m": ("session_interval", 5),
    "trading_session_5_minutes": ("session_interval", 5),
    "session/1_minute": ("session_interval", 1),
    "session/1m": ("session_interval", 1),
    "trading_session_1_minute": ("session_interval", 1),
}


def _normalize_code(value: str) -> str:
    normalized = value.upper().replace("-", "_")
    return _EXCHANGE_ALIASES.get(normalized, normalized)


def _split_registry_item(item: str) -> tuple[str, str | None]:
    code, separator, variant = item.partition("/")
    if not code:
        raise KeyError("Calendar code path cannot be empty")
    return _normalize_code(code), variant if separator else None


def _cache_key(code: str, variant: str | None) -> str:
    if variant is None:
        return code
    return f"{code}__{variant.replace('/', '_')}"


def _validate_exchange_code(code: str) -> None:
    if code in _EXCHANGE_CODE_SET:
        return
    try:
        Calendar.from_exchange(code)
    except ValueError as error:
        raise KeyError(f"No exchange calendar code '{code}'") from error


def _validate_country_code(code: str) -> None:
    if code in _COUNTRY_CODE_SET:
        return
    try:
        Calendar.from_region(code)
    except ValueError as error:
        raise KeyError(f"No country calendar code '{code}'") from error


def _coerce_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.min)


def _coerce_end_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.combine(value, time.max)


def _combine_with_start_time(day: date, start: datetime) -> datetime:
    return datetime.combine(day, start.timetz())


def _within_datetime_range(value: datetime, start: datetime, end: datetime) -> bool:
    if value.tzinfo is not None and start.tzinfo is None:
        start = start.replace(tzinfo=value.tzinfo)
    if value.tzinfo is not None and end.tzinfo is None:
        end = end.replace(tzinfo=value.tzinfo)
    return start <= value <= end


class FinanceDatesCalendar(BaseCalendar):
    code: str

    @classmethod
    def __class_getitem__(cls, code: str):
        return cls(code=_normalize_code(str(code)))

    def calendar(self):
        raise NotImplementedError

    def business_days(self, start: date, end: date) -> list[date]:
        return list(self.calendar().business_days(start, end))

    def days(self, start: date, end: date) -> list[date]:
        return list(self.calendar().days(start, end))

    def session_ranges(self, start: date, end: date) -> list[tuple[datetime, datetime]]:
        return list(self.calendar().sessions(start, end))

    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_datetime(end)
        if start_datetime > end_datetime:
            return []
        steps = []
        for day in self.business_days(start_datetime.date(), end_datetime.date()):
            step = _combine_with_start_time(day, start_datetime)
            if start_datetime <= step <= end_datetime:
                steps.append(step)
        return steps


class ExchangeCalendar(FinanceDatesCalendar):
    code: str = "XNYS"

    @classmethod
    def __class_getitem__(cls, code: str):
        normalized_code = _normalize_code(str(code))
        _validate_exchange_code(normalized_code)
        return cls(code=normalized_code)

    def calendar(self):
        return Calendar.from_exchange(self.code)


class ExchangeNonTradingDaysCalendar(ExchangeCalendar):
    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_end_datetime(end)
        if start_datetime > end_datetime:
            return []
        business_days = set(self.business_days(start_datetime.date(), end_datetime.date()))
        steps = []
        for day in self.days(start_datetime.date(), end_datetime.date()):
            if day in business_days:
                continue
            step = _combine_with_start_time(day, start_datetime)
            if start_datetime <= step <= end_datetime:
                steps.append(step)
        return steps


class ExchangeSessionIntervalCalendar(ExchangeCalendar):
    minutes: int = Field(default=1, gt=0)

    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_end_datetime(end)
        if start_datetime > end_datetime:
            return []
        interval = timedelta(minutes=self.minutes)
        steps = []
        for session_start, session_end in self.session_ranges(start_datetime.date(), end_datetime.date()):
            step = session_start
            while step < session_end:
                if _within_datetime_range(step, start_datetime, end_datetime):
                    steps.append(step)
                step += interval
        return steps


class ExchangeSessionStartCalendar(ExchangeCalendar):
    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_end_datetime(end)
        if start_datetime > end_datetime:
            return []
        return [
            session_start
            for session_start, _ in self.session_ranges(start_datetime.date(), end_datetime.date())
            if _within_datetime_range(session_start, start_datetime, end_datetime)
        ]


class ExchangeSessionEndCalendar(ExchangeCalendar):
    def steps(self, start: date | datetime, end: date | datetime) -> list[datetime]:
        start_datetime = _coerce_datetime(start)
        end_datetime = _coerce_end_datetime(end)
        if start_datetime > end_datetime:
            return []
        return [
            session_end
            for _, session_end in self.session_ranges(start_datetime.date(), end_datetime.date())
            if _within_datetime_range(session_end, start_datetime, end_datetime)
        ]


class ExchangeCalendarRegistry(ModelRegistry):
    name: str = "exchange"

    @property
    def supported_codes(self) -> tuple[str, ...]:
        return tuple(EXCHANGE_CODES)

    def _build_calendar(self, code: str, variant: str | None) -> BaseCalendar:
        _validate_exchange_code(code)
        if variant is None:
            return ExchangeCalendar(code=code)
        try:
            kind, minutes = _EXCHANGE_SESSION_VARIANTS[variant]
        except KeyError as error:
            raise KeyError(f"No exchange calendar variant '{variant}' for code '{code}'") from error
        if kind == "trading_days":
            return ExchangeCalendar(code=code)
        if kind == "non_trading_days":
            return ExchangeNonTradingDaysCalendar(code=code)
        if kind == "session_starts":
            return ExchangeSessionStartCalendar(code=code)
        if kind == "session_ends":
            return ExchangeSessionEndCalendar(code=code)
        return ExchangeSessionIntervalCalendar(code=code, minutes=minutes)

    def __getitem__(self, item) -> BaseCalendar:
        code, variant = _split_registry_item(item)
        cache_key = _cache_key(code, variant)
        if cache_key in self.models:
            return self.models[cache_key]
        return self.add(cache_key, self._build_calendar(code, variant), overwrite=True)

    def __iter__(self):
        return iter(self.supported_codes)

    def __len__(self) -> int:
        return len(self.supported_codes)


class RegionCalendar(FinanceDatesCalendar):
    code: str = "US"

    @classmethod
    def __class_getitem__(cls, code: str):
        normalized_code = _normalize_code(str(code))
        _validate_country_code(normalized_code)
        return cls(code=normalized_code)

    def calendar(self):
        return Calendar.from_region(self.code)


class CountryCalendar(RegionCalendar):
    code: str = "US"


class CountryCalendarRegistry(ModelRegistry):
    name: str = "country"

    @property
    def supported_codes(self) -> tuple[str, ...]:
        return _COUNTRY_CODES

    def __getitem__(self, item) -> CountryCalendar:
        code, variant = _split_registry_item(item)
        if variant is not None:
            raise KeyError(f"No country calendar variant '{variant}' for code '{code}'")
        if code in self.models:
            return self.models[code]
        _validate_country_code(code)
        return self.add(code, CountryCalendar(code=code), overwrite=True)

    def __iter__(self):
        return iter(self.supported_codes)

    def __len__(self) -> int:
        return len(self.supported_codes)
