# Development

`finance-etl` is the finance-specific ETL layer between generic `ccflow-*` connectors and application packages.

## Package Boundaries

`finance-etl` may depend on:

- `ccflow`
- `ccflow-etl`
- `ccflow-http`
- `ccflow-s3`
- `finance-dates`
- `finance-flow`

It must not depend on application-specific packages, private deployment paths, private credentials, or workflow-specific orchestration code.

## What Belongs Here

Good fits for this package include:

- finance calendars and market-session adapters
- provider-neutral finance context and result models
- provider request planners and reusable provider adapters
- provider credential config groups without secrets
- dataset catalogs and partition planners
- finance-aware validation before extraction or loading

## What Belongs Elsewhere

Generic execution, retry, cache, checkpoint, codec, and handoff behavior belongs in `ccflow-etl` or connector packages. Research workflow composition belongs in `finance-flow`. Private application assembly belongs in downstream packages.

## Test Convention

Default tests should run without live providers, live S3, live databases, or private credentials. Use synthetic market calendars and provider-shaped fixtures for normal coverage. Mark live or end-to-end tests explicitly when they need external services.
