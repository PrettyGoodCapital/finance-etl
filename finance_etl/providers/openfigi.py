import gzip
import json
import re
from datetime import UTC, date, datetime
from typing import Any

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, GraphDepList, ModelRegistry
from ccflow_etl import (
    APITokenCredentials,
    ArtifactReadContext,
    ArtifactReadModel,
    ArtifactWriteContext,
    ArtifactWriteModel,
    DatasetDefinition,
    ETLArtifact,
    ExecutionPolicy,
)
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext, HTTPRetryPolicy, RedactionPolicy
from ccflow_http.base import HTTPContext, HTTPMethod
from pydantic import Field

from finance_etl.providers._dataset_output import DatasetOutputMixin

__all__ = (
    "OpenFIGICredentials",
    "OpenFIGICusipCaptureModel",
    "OpenFIGIMappingCaptureModel",
    "OpenFIGIMappingModel",
    "isin_from_cusip",
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


class OpenFIGIMappingCaptureModel(DatasetOutputMixin, CallableModel):
    """Capture OpenFIGI mappings for the records in a dated listings snapshot that have no composite FIGI.

    Each record is looked up by ticker on the US composite exchange, trying the forms from
    ``openfigi_ticker_queries`` until one matches. One bundle per date holds every looked-up or skipped record with
    the full response, so later steps can validate matches against the listing's name and type.
    """

    mapping_model: OpenFIGIMappingModel = Field(default_factory=OpenFIGIMappingModel)
    snapshot: DatasetDefinition | None = Field(default=None, description="Snapshot dataset; replaces snapshot_store and snapshot_key_template.")
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
        return self.snapshot.key(value) if self.snapshot is not None else self.snapshot_key_template.format(date=value.isoformat())

    def _snapshot_store(self) -> Any:
        if self.snapshot is not None:
            return ModelRegistry.root()[self.snapshot.store] if isinstance(self.snapshot.store, str) else self.snapshot.store
        return self.snapshot_store if self.snapshot_store is not None else self._store

    def _artifact_uri(self, key: str) -> str:
        store = self._store
        if hasattr(store, "artifact_uri"):
            return store.artifact_uri(key)
        if hasattr(store, "uri"):
            return store.uri(key)
        return key

    def _will_fetch(self, context: DateContext) -> bool:
        if self.explain or self._store is None or self._before_first_date(context):
            return False
        return self.overwrite_output or not (hasattr(self._store, "exists") and self._store.exists(self._key(context)))

    def _snapshot_context(self, context: DateContext) -> ContextType:
        assert self.snapshot_model is not None
        return self.snapshot_model.context_type.model_validate({"date": context.date})

    @Flow.deps
    def __deps__(self, context: DateContext) -> GraphDepList:
        if self.snapshot_model is None or not self._will_fetch(context):
            return []
        return [(self.snapshot_model, [self._snapshot_context(context)])]

    def _records(self, value: date) -> list[dict[str, Any]] | None:
        store = self._snapshot_store()
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
        key = self._key(context)
        plan = {
            "dataset": self._name,
            "date": context.date.isoformat(),
            "snapshot_key": self.snapshot_key(context.date),
            "output_key": key,
            "output_uri": self._artifact_uri(key),
        }
        if self._before_first_date(context):
            return GenericResult(value={**plan, "status": "before_first_date", "output_writes": []})
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if self._store is None:
            raise ValueError("OpenFIGI mapping capture requires output.")
        metadata = {"date": plan["date"], "provider": "openfigi", "dataset": self._name}
        if not self._will_fetch(context):
            artifact = ETLArtifact(key=key, stage="extract", status="exists", dataset=self._name, uri=plan["output_uri"], media_type=self.media_type)
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
        write = ArtifactWriteModel(store=self._store)(
            ArtifactWriteContext(
                key=key,
                payload=payload,
                media_type=self.media_type,
                dataset=self._name,
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


# Country prefixes tried for CINS numbers (CUSIPs starting with a letter, for issuers outside the US).
_CINS_ISIN_PREFIXES = ("KY", "VG", "BM", "US", "MH", "CA", "GB")


def isin_from_cusip(cusip: str, country: str = "US") -> str:
    """ISIN for a CUSIP: country prefix, the CUSIP, and the Luhn check digit over the alphanumeric expansion."""
    body = country + cusip
    digits = "".join(str(int(character, 36)) for character in body)
    total = 0
    for position, digit in enumerate(reversed(digits)):
        value = int(digit) * (2 if position % 2 == 0 else 1)
        total += value // 10 + value % 10
    return body + str((10 - total % 10) % 10)


class OpenFIGICusipCaptureModel(CallableModel):
    """Capture OpenFIGI matches by CUSIP for a session's listings that have no composite FIGI.

    CUSIPs come from a dated symbol master (such as the WRDS NYSE TAQ master) joined on symbol. Each CUSIP is mapped
    on OpenFIGI, and unmatched ones are retried as ISINs. Matches carry the composite FIGI, or the exchange-level FIGI
    for securities that have none (such as exchange-listed preferreds). When the session's master is not available
    yet, nothing is written, so a later run fills the session once the master arrives.
    """

    mapping_model: OpenFIGIMappingModel = Field(default_factory=OpenFIGIMappingModel)
    snapshot: DatasetDefinition = Field(description="Listings snapshot partitions: JSON with records under records_key.")
    cusips: DatasetDefinition = Field(description="Symbol master partitions with symbol and CUSIP columns.")
    output: DatasetDefinition
    records_key: str = "results"
    ticker_field: str = "ticker"
    figi_field: str = "composite_figi"
    symbol_column: str = "CTA_SYMBOL"
    cusip_column: str = "CUSIP"
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _map(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        responses = []
        for start in range(0, len(jobs), _MAX_JOBS_PER_REQUEST):
            result = self.mapping_model(HTTPRequestContext(json_body=jobs[start : start + _MAX_JOBS_PER_REQUEST]))
            responses.extend(result.value if isinstance(result.value, list) else [])
        return responses

    @staticmethod
    def _match(data: list[dict[str, Any]]) -> dict[str, Any]:
        best = min(data, key=lambda item: not item.get("compositeFIGI"))
        return {
            **best,
            "compositeFIGI": best.get("compositeFIGI") or best.get("figi"),
            "figiKind": "composite" if best.get("compositeFIGI") else "exchange",
        }

    def _lookup(self, cusips: list[str]) -> dict[str, dict[str, Any]]:
        found: dict[str, dict[str, Any]] = {}
        for cusip, response in zip(cusips, self._map([{"idType": "ID_CUSIP", "idValue": cusip} for cusip in cusips]), strict=False):
            if response.get("data"):
                found[cusip] = {"query": cusip, "id_type": "ID_CUSIP", "match": self._match(response["data"])}
        retry = [
            (cusip, isin_from_cusip(cusip, country))
            for cusip in cusips
            if cusip not in found
            for country in (("US",) if cusip[0].isdigit() else _CINS_ISIN_PREFIXES)
        ]
        for (cusip, isin), response in zip(retry, self._map([{"idType": "ID_ISIN", "idValue": isin} for _, isin in retry]), strict=False):
            if response.get("data") and cusip not in found:
                found[cusip] = {"query": isin, "id_type": "ID_ISIN", "match": self._match(response["data"])}
        return found

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl

        plan = {**self.output.plan(context.date), "snapshot_key": self.snapshot.key(context.date), "cusips_key": self.cusips.key(context.date)}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if not self.overwrite_output and self.output.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})
        if not self.cusips.exists(context.date):
            return GenericResult(value={**plan, "status": "waiting"})
        if not self.snapshot.exists(context.date):
            return GenericResult(value={**plan, "status": "no_snapshot"})
        payload = json.loads(self.snapshot.read_bytes(context.date))
        records = payload.get(self.records_key, []) if isinstance(payload, dict) else payload
        missing = [record for record in records if isinstance(record, dict) and record.get(self.ticker_field) and not record.get(self.figi_field)]
        master = (
            self.cusips.read_frame(context.date)
            .select(pl.col(self.symbol_column).alias("ticker"), pl.col(self.cusip_column).alias("cusip"))
            .drop_nulls()
        )
        cusip_by_ticker = dict(zip(master["ticker"].to_list(), master["cusip"].to_list(), strict=True))
        found = self._lookup(sorted({cusip_by_ticker[r[self.ticker_field]] for r in missing if r[self.ticker_field] in cusip_by_ticker}))
        captured_at = datetime.now(UTC).isoformat()
        rows = []
        for record in missing:
            cusip = cusip_by_ticker.get(record[self.ticker_field])
            hit = found.get(cusip) if cusip else None
            status = "matched" if hit else ("no_match" if cusip else "no_cusip")
            rows.append(
                {
                    "date": context.date.isoformat(),
                    "captured_at": captured_at,
                    "record": record,
                    "cusip": cusip,
                    "status": status,
                    "query": hit["query"] if hit else None,
                    "id_type": hit["id_type"] if hit else None,
                    "matches": [hit["match"]] if hit else [],
                }
            )
        counts = {status: sum(row["status"] == status for row in rows) for status in ("matched", "no_match", "no_cusip")}
        body = gzip.compress(b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" for row in rows))
        writes = self.output.write_bytes(
            context.date, body, {"record_count": len(rows), **{f"{k}_count": v for k, v in counts.items()}}, overwrite=self.overwrite_output
        )
        return GenericResult(
            value={**plan, "status": "written", "record_count": len(rows), **{f"{k}_count": v for k, v in counts.items()}, "output_writes": writes}
        )
