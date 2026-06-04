# Providers

`finance-etl` owns reusable finance-domain provider adapters. Provider code should turn provider concepts into reusable request plans, calendars, credentials, and results; applications decide where to store outputs and which workflows to schedule.

## Massive

The current public provider surface covers Massive REST workflows for:

- market holidays and exchange metadata
- paginated ticker metadata
- date-specific ticker-universe request planning
- date and symbol daily aggregate request planning
- daily aggregate HTTP requests
- session-date planning for US stock market data
- daily ticker summary explain plans with dataset/provider metadata, unit identities, and redacted HTTP requests

The provider models build on the generic `ccflow-http` request machinery. They do not install package-specific workflow CLIs and do not hard-code storage destinations.

`MassiveDailyTickerSummaryModel` is the first public composition model. It uses `ccflow-etl` dataset/provider contracts, `ccflow-http` request models, and stable unit identities. In explain mode it does not require credentials and does not call Massive; live execution still requires `MASSIVE_API_KEY`.

Downstream normalization belongs in `finance-flow`, which already owns Massive-shaped daily-bar normalization contracts.

## Catalogs

Compose Massive dataset and provider catalogs with:

```yaml
defaults:
  - /credentials: massive
  - /datasets: massive
  - /providers: massive
  - _self_

hydra:
  searchpath:
    - pkg://finance_etl.config
```

The catalog registers:

| Registry Path                            | Purpose                                                                                  |
| ---------------------------------------- | ---------------------------------------------------------------------------------------- |
| `/datasets/massive_daily_ticker_summary` | Dataset definition for the semantic dataset `massive-daily-ticker-summary`.              |
| `/providers/massive`                     | Massive REST provider definition, credential reference, retry hints, and request shapes. |

## Credentials

Compose the packaged Massive credential config when an application needs Massive access:

```yaml
defaults:
  - /credentials: massive
  - _self_

hydra:
  searchpath:
    - pkg://finance_etl.config
```

The config registers:

| Registry Path                     | Purpose                                                                                                  |
| --------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `/credentials/massive`            | REST API token read from `MASSIVE_API_KEY`.                                                              |
| `/credentials/massive_flat_files` | Key/secret credentials for Massive flat-file S3 access using `MASSIVE_API_KEY_ID` and `MASSIVE_API_KEY`. |

Credentials stay in environment variables or deployment-specific overlays. Public package configs describe which credential shape is needed; they do not contain private secrets.

## Testing

Default provider tests use synthetic or recorded provider-shaped payloads. Live provider tests should remain opt-in and credential-gated.
