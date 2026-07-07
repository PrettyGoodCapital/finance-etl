# finance etl

Standard financial ETL workflows

[![Build Status](https://github.com/PrettyGoodCapital/finance-etl/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/PrettyGoodCapital/finance-etl/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/PrettyGoodCapital/finance-etl/branch/main/graph/badge.svg)](https://codecov.io/gh/PrettyGoodCapital/finance-etl)
[![License](https://img.shields.io/github/license/PrettyGoodCapital/finance-etl)](https://github.com/PrettyGoodCapital/finance-etl)
[![PyPI](https://img.shields.io/pypi/v/finance-etl.svg)](https://pypi.python.org/pypi/finance-etl)

## Overview

`finance-etl` provides reusable finance-specific ETL adapters on top of `ccflow` and the generic `ccflow-*` connector packages. It owns public finance data access concerns such as trading calendars, market sessions, provider request planning, credential config shapes, and finance-aware backfill helpers.

Applications compose these adapters into workflows and decide where data is stored. Provider credentials, private paths, and deployment-specific output stores stay outside this package.

## Quick Start

Compose the packaged calendar registry with `ccflow-etl`, then point a context at an explicit calendar path:

```yaml
defaults:
    - /backfills: default
    - /finance_calendars: default
    - _self_

hydra:
    searchpath:
        - pkg://ccflow_etl.config
        - pkg://finance_etl.config

context:
    start_datetime: 2024-07-01
    end_datetime: 2024-07-05
    calendar: /calendars/exchange/XNYS
```

Calendar paths are namespaced by code, for example `/calendars/exchange/XNYS`, `/calendars/exchange/XTKS/session/starts`, `/calendars/country/JP`, and `/calendars/region/us`.

Python callers use the same code-parameterized objects, such as `ExchangeCalendar["XNYS"]`, `RegionCalendar["US"]`, and `CountryCalendar["JP"]`.

Provider credentials are composed through Hydra groups. For Massive REST access:

```yaml
defaults:
    - /credentials: massive
    - _self_
```

The packaged credential config reads secrets from environment variables; it does not store secrets in public package config.

Massive also publishes a reusable dataset config:

```yaml
defaults:
    - /credentials: /credentials/providers/massive/rest
    - dataset: /datasets/massive/stocks/rest/ticker-summary
    - _self_
```

The config selects `MassiveDailyTickerSummaryModel` for the semantic dataset `massive-daily-ticker-summary`. Provider and schema metadata live on that concrete model. Applications can select `dataset=/datasets/massive/stocks/rest/ticker-summary` and run it through generic tasks such as `task=/tasks/extract`.

`MassiveDailyTickerSummaryModel` accepts an optional generic `ccflow-etl` `ArtifactWriteModel`. Explain runs emit output keys and planned writes without provider I/O; live runs can write raw provider payloads through the configured artifact store.

Massive stock REST wrappers also cover ticker overview (`/v3/reference/tickers/{ticker}`), grouped daily market summary (`/v2/aggs/grouped/locale/us/market/stocks/{date}`), and daily ticker open/close (`/v1/open-close/{ticker}/{date}`). Applications should compose per-ticker extract models through `finance_flow.SymbolFanoutModel` and a selected symbol universe rather than embedding ticker loops in provider models.

## Documentation

- [Calendars](docs/src/calendars.md)
- [Providers](docs/src/providers.md)
- [API](docs/src/api.md)
- [Development](docs/src/development.md)

## Dependency Contract

- May depend on `ccflow`, `ccflow-etl`, `ccflow-http`, `ccflow-s3`, `finance-dates`, and `finance-flow`.
- Should keep provider credentials and deployment-specific storage outputs out of package code.
- Must not depend on application-specific packages.

## Test Convention

Default tests should use synthetic market calendars and recorded or generated provider-shaped responses. They should not require live provider credentials, live S3, or live databases.
