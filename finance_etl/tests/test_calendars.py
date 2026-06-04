from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Type

import pytest
from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ModelRegistry, ResultType
from ccflow.utils.hydra import cfg_run, load_config as base_load_config
from finance_dates import COUNTRY_CODES, COUNTRY_CODES3, EXCHANGE_CODES
from hydra import compose, initialize_config_dir
from hydra.utils import instantiate

from finance_etl import (
    CountryCalendar,
    CountryCalendarRegistry,
    ExchangeCalendar,
    ExchangeCalendarRegistry,
    ExchangeNonTradingDaysCalendar,
    ExchangeSessionEndCalendar,
    ExchangeSessionIntervalCalendar,
    ExchangeSessionStartCalendar,
    RegionCalendar,
)


class EchoFinanceDateModel(CallableModel):
    @property
    def context_type(self) -> Type[ContextType]:
        return DateContext

    @property
    def result_type(self) -> Type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context):
        return GenericResult(value={"date": context.date.isoformat()})


def _compose_finance_calendars(tmp_path):
    (tmp_path / "runner.yaml").write_text(
        """
defaults:
    - _self_
    - finance_calendars: default

hydra:
    searchpath:
        - pkg://finance_etl.config
""".lstrip()
    )

    with initialize_config_dir(config_dir=str(tmp_path), version_base=None):
        return compose(config_name="runner")


def test_exchange_calendar_uses_finance_dates_business_days():
    calendar = ExchangeCalendar(code="XNYS")

    assert [step.date() for step in calendar.steps(date(2024, 7, 1), date(2024, 7, 5))] == [
        date(2024, 7, 1),
        date(2024, 7, 2),
        date(2024, 7, 3),
        date(2024, 7, 5),
    ]


def test_region_calendar_uses_finance_dates_region_resolver():
    calendar = RegionCalendar(code="US")

    assert [step.date() for step in calendar.steps(date(2024, 7, 1), date(2024, 7, 5))] == [
        date(2024, 7, 1),
        date(2024, 7, 2),
        date(2024, 7, 3),
        date(2024, 7, 5),
    ]


def test_parameterized_exchange_calendar_preserves_start_time():
    calendar = ExchangeCalendar["XNYS"]

    assert [step.isoformat() for step in calendar.steps(date(2024, 7, 1), date(2024, 7, 5))] == [
        "2024-07-01T00:00:00",
        "2024-07-02T00:00:00",
        "2024-07-03T00:00:00",
        "2024-07-05T00:00:00",
    ]


def test_calendar_classes_support_code_parameterization():
    assert ExchangeCalendar["xnas"].code == "XNAS"
    assert RegionCalendar["us"].code == "US"
    assert CountryCalendar["jp"].code == "JP"


def test_exchange_non_trading_day_calendar_includes_holidays_and_weekends():
    calendar = ExchangeNonTradingDaysCalendar(code="XNYS")

    assert [step.date() for step in calendar.steps(date(2024, 7, 1), date(2024, 7, 7))] == [
        date(2024, 7, 4),
        date(2024, 7, 6),
        date(2024, 7, 7),
    ]


def test_exchange_session_interval_calendar_uses_early_close_session_bounds():
    calendar = ExchangeSessionIntervalCalendar(code="XNYS", minutes=60)

    assert [step.isoformat() for step in calendar.steps(date(2024, 7, 3), date(2024, 7, 3))] == [
        "2024-07-03T13:30:00+00:00",
        "2024-07-03T14:30:00+00:00",
        "2024-07-03T15:30:00+00:00",
        "2024-07-03T16:30:00+00:00",
    ]


def test_exchange_session_start_and_end_calendars_include_lunch_break_segments():
    starts = ExchangeSessionStartCalendar(code="XTKS")
    ends = ExchangeSessionEndCalendar(code="XTKS")

    assert [step.isoformat() for step in starts.steps(date(2024, 7, 1), date(2024, 7, 1))] == [
        "2024-07-01T00:00:00+00:00",
        "2024-07-01T03:30:00+00:00",
    ]
    assert [step.isoformat() for step in ends.steps(date(2024, 7, 1), date(2024, 7, 1))] == [
        "2024-07-01T02:30:00+00:00",
        "2024-07-01T06:00:00+00:00",
    ]


def test_finance_calendar_config_registers_exchange_session_presets(tmp_path):
    cfg = _compose_finance_calendars(tmp_path)

    assert isinstance(instantiate(cfg.calendars.trading_days), ExchangeCalendar)
    assert isinstance(instantiate(cfg.calendars.non_trading_days), ExchangeNonTradingDaysCalendar)
    assert instantiate(cfg.calendars.trading_session_hours).minutes == 60
    assert instantiate(cfg.calendars.trading_session_30_minutes).minutes == 30
    assert instantiate(cfg.calendars.trading_session_15_minutes).minutes == 15
    assert instantiate(cfg.calendars.trading_session_5_minutes).minutes == 5
    assert instantiate(cfg.calendars.trading_session_1_minute).minutes == 1
    assert isinstance(instantiate(cfg.calendars.trading_session_starts), ExchangeSessionStartCalendar)
    assert isinstance(instantiate(cfg.calendars.trading_session_ends), ExchangeSessionEndCalendar)


def test_finance_calendar_config_short_aliases_use_parameterized_classes(tmp_path):
    cfg = _compose_finance_calendars(tmp_path)

    assert cfg.calendars.nyse._target_ == "finance_etl.ExchangeCalendar"
    assert cfg.calendars.nyse.code == "XNYS"
    assert cfg.calendars.tokyo._target_ == "finance_etl.ExchangeCalendar"
    assert cfg.calendars.tokyo.code == "XTKS"
    assert cfg.calendars.japan._target_ == "finance_etl.CountryCalendar"
    assert cfg.calendars.japan.code == "JP"

    assert isinstance(instantiate(cfg.calendars.nyse), ExchangeCalendar)
    assert isinstance(instantiate(cfg.calendars.japan), CountryCalendar)


def test_finance_calendar_config_registers_exhaustive_code_namespaces(tmp_path):
    cfg = _compose_finance_calendars(tmp_path)
    root = ModelRegistry.root().clear()
    root.load_config(cfg, overwrite=True)

    exchange_registry = root["/calendars/exchange"]
    country_registry = root["/calendars/country"]
    region_registry = root["/calendars/region"]

    assert isinstance(exchange_registry, ExchangeCalendarRegistry)
    assert isinstance(country_registry, CountryCalendarRegistry)
    assert isinstance(region_registry, CountryCalendarRegistry)
    assert exchange_registry.supported_codes == tuple(EXCHANGE_CODES)
    assert country_registry.supported_codes == tuple(dict.fromkeys((*COUNTRY_CODES, *COUNTRY_CODES3)))
    assert region_registry.supported_codes == tuple(dict.fromkeys((*COUNTRY_CODES, *COUNTRY_CODES3)))

    assert root["/calendars/exchange/XNYS"].code == "XNYS"
    assert root["/calendars/exchange/xlon"].code == "XLON"
    assert root["/calendars/region/us"].code == "US"
    assert root["/calendars/country/JP"].code == "JP"


def test_finance_calendar_code_namespaces_support_exchange_variants(tmp_path):
    cfg = _compose_finance_calendars(tmp_path)
    root = ModelRegistry.root().clear()
    root.load_config(cfg, overwrite=True)

    assert isinstance(root["/calendars/exchange/XNYS/non_trading_days"], ExchangeNonTradingDaysCalendar)
    assert root["/calendars/exchange/XNYS/session/5_minutes"].minutes == 5
    assert root["/calendars/exchange/XNYS/session/5m"].minutes == 5
    assert isinstance(root["/calendars/exchange/XTKS/session/starts"], ExchangeSessionStartCalendar)
    assert isinstance(root["/calendars/exchange/XTKS/session/ends"], ExchangeSessionEndCalendar)

    with pytest.raises(KeyError):
        root["/calendars/exchange/not-a-calendar"]


def test_finance_calendar_config_can_drive_ccflow_backfill(tmp_path):
    config_path = tmp_path / "config"
    config_path.mkdir()
    (config_path / "runner.yaml").write_text(
        """
defaults:
    - /backfill: daily
    - /finance_calendars: default
    - _self_

hydra:
  searchpath:
    - pkg://ccflow_etl.config
    - pkg://finance_etl.config

model:
  _target_: finance_etl.tests.test_calendars.EchoFinanceDateModel

cli:
  model:
    _target_: ccflow.FlowOptions
    evaluator:
      _target_: ccflow.evaluators.MultiEvaluator
      evaluators:
        - _target_: ccflow.evaluators.GraphEvaluator
        - _target_: ccflow.evaluators.MemoryCacheEvaluator
        - _target_: ccflow.evaluators.LoggingEvaluator
    cacheable: true

context:
    start_datetime: 2024-07-01
    end_datetime: 2024-07-05
    calendar: /calendars/exchange/XNYS
    template:
        date: 2024-07-01
""".lstrip()
    )
    result = base_load_config(
        root_config_dir=str(config_path),
        root_config_name="runner",
        config_dir="",
        basepath=str(Path.cwd()),
        debug=False,
    )

    ModelRegistry.root().clear()
    output = cfg_run(result.cfg)

    assert output.value["steps"] == 4
    assert [item["value"]["date"] for item in output.value["outputs"]] == [
        "2024-07-01",
        "2024-07-02",
        "2024-07-03",
        "2024-07-05",
    ]
