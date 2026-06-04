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

The provider models build on the generic `ccflow-http` request machinery. They do not install package-specific workflow CLIs and do not hard-code storage destinations.

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
