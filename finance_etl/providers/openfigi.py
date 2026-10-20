import gzip
import json
import re
from datetime import UTC, date, datetime
from typing import Any

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, GraphDepList
from ccflow_etl import (
    APITokenCredentials,
    ArtifactReadContext,
    ArtifactReadModel,
    ArtifactWriteContext,
    ArtifactWriteModel,
    ETLArtifact,
    ExecutionPolicy,
)
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext, HTTPRetryPolicy, RedactionPolicy
from ccflow_http.base import HTTPContext, HTTPMethod
from pydantic import Field

__all__ = (
    "OpenFIGICredentials",
    "OpenFIGIMappingCaptureModel",
    "OpenFIGIMappingModel",
    "openfigi_ticker_queries",
)

# Requests with an API key are limited to 25 per 6 seconds, with up to 100 mapping jobs each.
_DEFAULT_OPENFIGI_POLICY = ExecutionPolicy(min_interval_seconds=0.25)
_MAX_JOBS_PER_REQUEST = 100


class OpenFIGICredentials(APITokenCredentials):
    token_env: str | None = "OPENFIGI_API_KEY"
    header_name: str = "X-OPENFIGI-APIKEY"


class OpenFIGIMappingModel(HTTPModel):
    """POST mapping jobs to /v3/mapping; the context's json_body is the list of jobs."""

    base_url: str = "https://api.openfigi.com"
    path: str = "/v3/mapping"
    method: HTTPMethod = "POST"
    credentials: OpenFIGICredentials = Field(default_factory=OpenFIGICredentials)
    execution_policy: ExecutionPolicy | None = Field(default_factory=lambda: _DEFAULT_OPENFIGI_POLICY.model_copy())
    retry_policy: HTTPRetryPolicy | None = Field(
        default_factory=lambda: HTTPRetryPolicy(max_attempts=6, retry_status_codes=[429, 500, 502, 503, 504])
    )
    redaction: RedactionPolicy = Field(default_factory=lambda: RedactionPolicy().with_names(OpenFIGICredentials().header_name))

    def build_request(self, context: HTTPContext | None = None) -> HTTPRequest:
        request = super().build_request(context or HTTPRequestContext())
        token = self.credentials.resolved_token()
        if not token:
            return request
        return request.model_copy(update={"headers": {**request.headers, self.credentials.header_name: token}})


def openfigi_ticker_queries(ticker: str) -> list[str]:
    """Bloomberg-style ticker forms to try for an exchange symbol, most likely first.

    Share classes, units, and warrants use a slash (``BRK.A`` -> ``BRK/A``, ``AAC.WS`` -> ``AAC/WS``), and NYSE
    rights use a lowercase ``r`` suffix (``AIIAr`` -> ``AIIA/R``). Other symbols are tried as they are.
    """
    if "." in ticker:
        return [ticker.replace(".", "/"), ticker]
    match = re.fullmatch(r"([A-Z]+)r", ticker)
    if match:
        return [f"{match.group(1)}/R", ticker]
    return [ticker]


class OpenFIGIMappingCaptureModel(CallableModel):
    """Capture OpenFIGI mappings for the records in a dated listings snapshot that have no composite FIGI.

    Each record is looked up by ticker on the US composite exchange, trying the forms from
    ``openfigi_ticker_queries`` until one matches. One bundle per date holds every looked-up or skipped record with
    the full response, so later steps can validate matches against the listing's name and type.
    """

    mapping_model: OpenFIGIMappingModel = Field(default_factory=OpenFIGIMappingModel)
    snapshot_store: Any | None = Field(default=None, description="Store holding the snapshots; defaults to output.")
    snapshot_key_template: str = "massive/stocks/rest/all-tickers/{date}.json"
    snapshot_model: CallableModel | None = Field(default=None, description="Model that produces the snapshot for a date, declared as a dependency.")
    records_key: str = "results"
    ticker_field: str = "ticker"
    figi_field: str = "composite_figi"
    type_field: str = "type"
    exchange_code: str = "US"
    skip_types: list[str] = Field(
        default_factory=lambda: ["PFD", "SP"],
        description="Types OpenFIGI does not index under exchange symbols (preferreds and exchange-listed notes); recorded as skipped.",
    )
    output: Any | None = None
    output_key_prefix: str = "openfigi/mapping"
    dataset_name: str = "openfigi-mapping"
    explain: bool = False
    overwrite_output: bool = False
    media_type: str = "application/gzip"

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def output_key(self, context: DateContext) -> str:
        return f"{self.output_key_prefix.strip('/')}/jsonl/{context.date.isoformat()}.jsonl.gz"

    def snapshot_key(self, value: date) -> str:
        return self.snapshot_key_template.format(date=value.isoformat())

    def _artifact_uri(self, key: str) -> str:
        store = self.output
        if hasattr(store, "artifact_uri"):
            return store.artifact_uri(key)
        if hasattr(store, "uri"):
            return store.uri(key)
        return key

    def _will_fetch(self, context: DateContext) -> bool:
        if self.explain or self.output is None:
            return False
        return self.overwrite_output or not (hasattr(self.output, "exists") and self.output.exists(self.output_key(context)))

    def _snapshot_context(self, context: DateContext) -> ContextType:
        assert self.snapshot_model is not None
        return self.snapshot_model.context_type.model_validate({"date": context.date})

    @Flow.deps
    def __deps__(self, context: DateContext) -> GraphDepList:
        if self.snapshot_model is None or not self._will_fetch(context):
            return []
        return [(self.snapshot_model, [self._snapshot_context(context)])]

    def _records(self, value: date) -> list[dict[str, Any]] | None:
        store = self.snapshot_store if self.snapshot_store is not None else self.output
        key = self.snapshot_key(value)
        if hasattr(store, "exists") and not store.exists(key):
            return None
        payload = json.loads(ArtifactReadModel(store=store)(ArtifactReadContext(key=key)).payload)
        records = payload.get(self.records_key, []) if isinstance(payload, dict) else payload
        return [record for record in records if isinstance(record, dict) and record.get(self.ticker_field)]

    def _map(self, queries: list[str]) -> list[dict[str, Any]]:
        responses = []
        for start in range(0, len(queries), _MAX_JOBS_PER_REQUEST):
            jobs = [
                {"idType": "TICKER", "idValue": query, "exchCode": self.exchange_code} for query in queries[start : start + _MAX_JOBS_PER_REQUEST]
            ]
            result = self.mapping_model(HTTPRequestContext(json_body=jobs))
            responses.extend(result.value if isinstance(result.value, list) else [])
        return responses

    def _lookup(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        candidates = {ticker: openfigi_ticker_queries(ticker) for ticker in tickers}
        found: dict[str, dict[str, Any]] = {}
        tried: dict[str, list[str]] = {ticker: [] for ticker in tickers}
        level = 0
        while True:
            pending = [ticker for ticker in tickers if ticker not in found and len(candidates[ticker]) > level]
            if not pending:
                break
            queries = [candidates[ticker][level] for ticker in pending]
            for ticker, query, response in zip(pending, queries, self._map(queries)):
                tried[ticker].append(query)
                if response.get("data"):
                    found[ticker] = {"status": "matched", "query": query, "matches": response["data"]}
            level += 1
        return {ticker: found.get(ticker, {"status": "no_match", "query": None, "matches": []}) | {"queries": tried[ticker]} for ticker in tickers}

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        key = self.output_key(context)
        plan = {
            "dataset": self.dataset_name,
            "date": context.date.isoformat(),
            "snapshot_key": self.snapshot_key(context.date),
            "output_key": key,
            "output_uri": self._artifact_uri(key),
        }
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if self.output is None:
            raise ValueError("OpenFIGI mapping capture requires output.")
        metadata = {"date": plan["date"], "provider": "openfigi", "dataset": self.dataset_name}
        if not self._will_fetch(context):
            artifact = ETLArtifact(
                key=key, stage="extract", status="exists", dataset=self.dataset_name, uri=plan["output_uri"], media_type=self.media_type
            )
            return GenericResult(value={**plan, "status": "exists", "output_writes": [artifact.model_dump(mode="json")]})
        if self.snapshot_model is not None:
            self.snapshot_model(self._snapshot_context(context))
        records = self._records(context.date)
        if records is None:
            return GenericResult(value={**plan, "status": "no_snapshot", "output_writes": []})
        missing = [record for record in records if not record.get(self.figi_field)]
        lookup = [record for record in missing if record.get(self.type_field) not in self.skip_types]
        results = self._lookup(sorted({record[self.ticker_field] for record in lookup}))
        captured_at = datetime.now(UTC).isoformat()
        rows = []
        for record in missing:
            ticker = record[self.ticker_field]
            outcome = results.get(ticker, {"status": "skipped", "query": None, "queries": [], "matches": []})
            rows.append({"date": plan["date"], "captured_at": captured_at, "exchange_code": self.exchange_code, "record": record, **outcome})
        counts = {status: sum(row["status"] == status for row in rows) for status in ("matched", "no_match", "skipped")}
        payload = gzip.compress(b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" for row in rows))
        metadata = {**metadata, "record_count": len(rows), **{f"{status}_count": count for status, count in counts.items()}}
        write = ArtifactWriteModel(store=self.output)(
            ArtifactWriteContext(
                key=key,
                payload=payload,
                media_type=self.media_type,
                dataset=self.dataset_name,
                stage="extract",
                overwrite=self.overwrite_output,
                metadata=metadata,
            )
        )
        return GenericResult(
            value={
                **plan,
                "status": write.status,
                "snapshot_record_count": len(records),
                "record_count": len(rows),
                **{f"{status}_count": count for status, count in counts.items()},
                "output_writes": [write.model_dump(mode="json")],
            }
        )
