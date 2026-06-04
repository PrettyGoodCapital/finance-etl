# API

This page summarizes the public Python surface most callers use directly. Config-driven applications should prefer registry paths and Hydra groups where possible.

## Finance Contract Inventory

`finance-etl` provides typed, provider-neutral record contracts for reusable workflows:

- core market contracts: `InstrumentHandle`, `SecurityRecord`, `ListingRecord`, `UniverseMember`, `SignalRecord`
- portfolio contracts: `OptimizerAllocation`, `TargetPositionRecord`, `PortfolioSnapshot`, `BacktestResultRecord`
- metadata contracts: `SchemaMetadata`, `ReportMetadata`, `ExchangeSessionRecord`, `CorporateActionRecord`

These records are intended to cross package boundaries (`finance-flow`, `pgc-etl`, and downstream apps) without provider-specific assumptions.

## Partition Contexts And Results

Typed partition contexts standardize task and batch boundaries for date-partitioned finance workflows:

- `ExchangePartitionContext`
- `MarketPartitionContext`
- `TickerPartitionContext`
- `SessionDatePartitionContext`
- `TickerDatePartitionContext`
- `PortfolioDatePartitionContext`
- `StrategyDatePartitionContext`
- `ReportDatePartitionContext`

`PartitionComputationResult` captures partition execution output metadata (`partition_key`, `as_of_date`, `row_count`, and optional `SchemaMetadata`).

Task contexts in `finance-flow` can attach these partition objects directly for typed validation of date and scope.

## Calendars

Calendar adapters bridge `finance-dates` calendars into `ccflow-etl` backfills and other callable models.

```python
from datetime import date

from finance_etl import CountryCalendar, ExchangeCalendar, RegionCalendar

nyse = ExchangeCalendar["XNYS"]
nyse.steps(date(2024, 7, 1), date(2024, 7, 5))

us = RegionCalendar["US"]
us.steps(date(2024, 7, 1), date(2024, 7, 5))

japan = CountryCalendar["JP"]
japan.steps(date(2024, 7, 1), date(2024, 7, 5))
```

The subscription form is a convenience constructor; `ExchangeCalendar(code="XNYS")` remains equivalent. `finance-etl` does not define one subclass per exchange or country because supported codes come from `finance-dates`.

Session-aware exchange calendars are available for non-trading days, intraday session intervals, session starts, and session ends. For config-driven use, prefer the registry paths documented in [Calendars](calendars.md).

## Massive Provider

The Massive provider exports callable models and contexts for credentialed HTTP requests, market metadata, ticker metadata, daily aggregates, ticker-universe planning, stock-data planning, and daily aggregate backfill request planning.

```python
from datetime import date

from finance_etl.providers.massive import DailyAggregateContext, DailyAggregateModel

request = DailyAggregateModel().build_request(
    DailyAggregateContext(ticker="AAPL", date=date(2024, 1, 3))
)
```

Provider models are reusable building blocks. Applications decide how to compose them into larger workflows and where to store outputs.
