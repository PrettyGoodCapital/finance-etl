# Calendars

`finance-etl` publishes finance-aware calendars into the shared `ccflow` `/calendars` registry. These calendars are backed by `finance-dates`, so supported exchange and country codes come from the same source of truth used by the rest of the `finance-*` stack.

## Registry Paths

Use namespaced registry paths when configuring backfills or callable models:

| Path                                            | Meaning                                                                                 |
| ----------------------------------------------- | --------------------------------------------------------------------------------------- |
| `/calendars/exchange/{CODE}`                    | Trading days for an exchange, venue, or market-family code accepted by `finance-dates`. |
| `/calendars/exchange/{CODE}/non_trading_days`   | Holidays and weekends for that exchange calendar.                                       |
| `/calendars/exchange/{CODE}/session/hours`      | Hourly timestamps inside regular trading sessions.                                      |
| `/calendars/exchange/{CODE}/session/30_minutes` | 30-minute timestamps inside regular trading sessions.                                   |
| `/calendars/exchange/{CODE}/session/15_minutes` | 15-minute timestamps inside regular trading sessions.                                   |
| `/calendars/exchange/{CODE}/session/5_minutes`  | 5-minute timestamps inside regular trading sessions.                                    |
| `/calendars/exchange/{CODE}/session/1_minute`   | 1-minute timestamps inside regular trading sessions.                                    |
| `/calendars/exchange/{CODE}/session/starts`     | Session start timestamps.                                                               |
| `/calendars/exchange/{CODE}/session/ends`       | Session end timestamps.                                                                 |
| `/calendars/country/{ISO}`                      | Representative country calendar for a two- or three-letter ISO country code.            |
| `/calendars/region/{ISO}`                       | Alias namespace for representative country calendars.                                   |

Lookup is case-insensitive for calendar codes. For example, `/calendars/region/us` and `/calendars/country/US` resolve to the same representative US calendar.

## Example

Compose the finance calendar registry with `ccflow-etl`, then reference the calendar path from the context that consumes it:

```yaml
defaults:
  - /backfill: daily
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
  template:
    date: 2024-07-01
```

Other examples:

```yaml
context:
  calendar: /calendars/exchange/XTKS/session/starts
```

```yaml
context:
  calendar: /calendars/country/JP
```

```yaml
context:
  calendar: /calendars/region/us
```

## Python Objects

Python code uses the same code-parameterized calendar objects. There are no per-exchange subclasses to keep exhaustive:

```python
from finance_etl import CountryCalendar, ExchangeCalendar, RegionCalendar

nyse = ExchangeCalendar["XNYS"]
us = RegionCalendar["US"]
japan = CountryCalendar["JP"]
```

The subscription form is a convenience constructor. The equivalent explicit constructor is still available when that reads better:

```python
nyse = ExchangeCalendar(code="XNYS")
```

## Code Discovery

Use `finance-dates` when code needs to discover supported identifiers:

```python
from finance_dates import COUNTRY_CODES, COUNTRY_CODES3, EXCHANGE_CODES

"XNYS" in EXCHANGE_CODES
"JP" in COUNTRY_CODES
"JPN" in COUNTRY_CODES3
```

`EXCHANGE_CODES` is the exported exchange-code list. `Calendar.from_exchange()` also accepts selected resolver-only aliases for product families where `finance-dates` supports them. Country and region paths accept the two- and three-letter ISO codes exported by `COUNTRY_CODES` and `COUNTRY_CODES3`.

## Compatibility Aliases

The default config still includes short aliases such as `/calendars/nyse`, `/calendars/nasdaq`, `/calendars/lse`, and XNYS session presets like `/calendars/trading_session_5_minutes`. These aliases are configured instances of `ExchangeCalendar` or `CountryCalendar`, not separate Python subclasses. Prefer the namespaced paths above in new configs because they work for every supported code instead of a fixed subset.
