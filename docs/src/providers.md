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
- daily ticker summary explain plans with dataset/provider metadata, redacted HTTP requests, output keys, and artifact write plans

The provider models build on the generic `ccflow-http` request machinery. They do not install package-specific workflow CLIs and do not hard-code storage outputs.

`MassiveDailyTickerSummaryModel` is the first public composition model. It uses `ccflow-http` request models, `ccflow-etl` artifact IO contracts, and an optional `ccflow-etl` `ArtifactWriteModel`. In explain mode it does not require credentials, does not call Massive, and emits planned artifact writes when a writer is configured. Live execution still requires `MASSIVE_API_KEY`; applications provide the concrete artifact store through config.

Applications should normally select this surface as `dataset=/datasets/massive/stocks/rest/ticker-summary` and run it through a generic task such as `task=/tasks/extract`. Provider, retry, request, and schema metadata live on the dataset model.

Downstream normalization belongs in `finance-flow`, which already owns Massive-shaped daily-bar normalization contracts.

## Dataset Config

Compose Massive REST ticker-summary with:

```yaml
defaults:
  - /credentials: /credentials/providers/massive/rest
  - dataset: /datasets/massive/stocks/rest/ticker-summary
  - _self_

hydra:
  searchpath:
    - pkg://finance_etl.config
```

The dataset config selects `MassiveDailyTickerSummaryModel`, whose metadata methods expose the semantic dataset name, schema name/version, provider name, retry hints, and request shape.

Parquet materialization is selected on the dataset with `+dataset.return_type=parquet`; no separate transform selector is required for this Massive surface.

## Credentials

Compose the packaged Massive REST credential config when an application needs Massive access:

```yaml
defaults:
  - /credentials: /credentials/providers/massive/rest
  - _self_

hydra:
  searchpath:
    - pkg://finance_etl.config
```

The config registers:

| Registry Path | Purpose |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `/credentials/providers/massive/rest` | REST API token read from `MASSIVE_API_KEY`. |
| `/credentials/massive_flat_files` | Key/secret credentials for Massive flat-file S3 access using `MASSIVE_API_KEY_ID` and `MASSIVE_API_KEY`. |

Credentials stay in environment variables or deployment-specific overlays. Public package configs describe which credential shape is needed; they do not contain private secrets.

## Testing

Default provider tests use synthetic or recorded provider-shaped payloads. Live provider tests should remain opt-in and credential-gated.
