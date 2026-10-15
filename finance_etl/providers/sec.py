import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

from ccflow import CallableModel, DateContext, Flow, GenericResult, GraphDepList
from ccflow_etl import ArtifactReadContext, ArtifactReadModel, ArtifactWriteContext, ArtifactWriteModel, ETLArtifact, ExecutionPolicy
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext
from ccflow_http.base import ResponseFormat
from pydantic import Field, model_validator

__all__ = (
    "SECDailyIndexContext",
    "SECDailyIndexExtractModel",
    "SECDailyIndexModel",
    "SECDateContext",
    "SECFilerRefreshBundleModel",
    "SECHTTPModel",
    "parse_master_index",
)

# SEC fair access allows at most 10 requests per second across all of a client's traffic.
_DEFAULT_SEC_POLICY = ExecutionPolicy(min_interval_seconds=0.12)
# Forms whose filings carry XBRL financial statements.
DEFAULT_FINANCIAL_FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A", "20-F", "20-F/A", "40-F", "40-F/A", "10-KT", "10-QT")


class SECDateContext(DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if isinstance(value, (tuple, list)) and len(value) == 1:
            value = {"date": value[0]}
        elif not isinstance(value, (cls, dict)) and not isinstance(value, (tuple, list)):
            value = {"date": value}
        return handler(value)


class SECDailyIndexContext(HTTPRequestContext):
    date: date


class SECHTTPModel(HTTPModel):
    """HTTP model with the declared User-Agent SEC requires and request spacing within its fair-access limit."""

    base_url: str = "https://www.sec.gov"
    user_agent: str = Field(description="Declared User-Agent, for example 'Company Name contact@example.com'.")
    execution_policy: ExecutionPolicy | None = Field(default_factory=lambda: _DEFAULT_SEC_POLICY.model_copy())
    accepted_status_codes: list[int] = Field(default_factory=lambda: [404])

    def build_request(self, context=None) -> HTTPRequest:
        request = super().build_request(context)
        return request.model_copy(update={"headers": {**request.headers, "User-Agent": self.user_agent}})


class SECDailyIndexModel(SECHTTPModel):
    """EDGAR master daily index for one date; 404 means no index was published that day."""

    path: str = "/Archives/edgar/daily-index/{{ year }}/QTR{{ quarter }}/master.{{ ymd }}.idx"
    response_format: ResponseFormat = "text"

    @property
    def context_type(self) -> type[SECDailyIndexContext]:
        return SECDailyIndexContext

    def build_request(self, context: SECDailyIndexContext) -> HTTPRequest:  # ty: ignore[invalid-method-override]
        value = context.date
        values = {"year": f"{value.year:04d}", "quarter": (value.month - 1) // 3 + 1, "ymd": value.strftime("%Y%m%d")}
        return super().build_request(context.model_copy(update={"template_values": {**context.template_values, **values}}))


def parse_master_index(text: str) -> list[dict[str, str]]:
    """Parse an EDGAR master index into rows of cik, company_name, form_type, date_filed, and filename."""
    rows = []
    started = False
    for line in text.splitlines():
        if not started:
            started = line.startswith("-----")
            continue
        parts = line.split("|")
        if len(parts) == 5 and parts[0].strip().isdigit():
            rows.append(dict(zip(("cik", "company_name", "form_type", "date_filed", "filename"), (part.strip() for part in parts))))
    return rows


def _artifact_uri(store: Any, key: str) -> str:
    if hasattr(store, "artifact_uri"):
        return store.artifact_uri(key)
    if hasattr(store, "uri"):
        return store.uri(key)
    return key


def _existing_artifact(store: Any, key: str, dataset: str, media_type: str, metadata: dict[str, Any]) -> dict[str, Any]:
    return ETLArtifact(
        key=key, stage="extract", status="exists", dataset=dataset, uri=_artifact_uri(store, key), media_type=media_type, metadata=metadata
    ).model_dump(mode="json")


class SECDailyIndexExtractModel(CallableModel):
    """Store the EDGAR master daily index for a date under a dated key."""

    index_model: SECDailyIndexModel
    output: Any = None
    output_key_prefix: str = "sec/edgar/daily-index"
    dataset_name: str = "sec-edgar-daily-index"
    overwrite_output: bool = False
    explain: bool = False
    media_type: str = "text/plain"

    @property
    def context_type(self) -> type[SECDateContext]:
        return SECDateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def output_key(self, context: SECDateContext) -> str:
        value = context.date
        return f"{self.output_key_prefix.strip('/')}/{value:%Y}/{value:%m}/{value.isoformat()}.idx"

    @Flow.call
    def __call__(self, context: SECDateContext) -> GenericResult:
        key = self.output_key(context)
        metadata = {"date": context.date.isoformat(), "provider": "sec", "dataset": self.dataset_name}
        plan = {"dataset": self.dataset_name, "date": context.date.isoformat(), "output_key": key, "output_uri": _artifact_uri(self.output, key)}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if self.output is None:
            raise ValueError("SEC daily index extract requires output.")
        if not self.overwrite_output and hasattr(self.output, "exists") and self.output.exists(key):
            return GenericResult(
                value={
                    **plan,
                    "status": "exists",
                    "output_writes": [_existing_artifact(self.output, key, self.dataset_name, self.media_type, metadata)],
                }
            )
        result = self.index_model(SECDailyIndexContext(date=context.date))
        if result.status_code == 404:
            return GenericResult(value={**plan, "status": "not_published", "status_code": 404, "output_writes": []})
        rows = parse_master_index(result.value)
        write = ArtifactWriteModel(store=self.output)(
            ArtifactWriteContext(
                key=key,
                payload=result.value.encode(),
                media_type=self.media_type,
                dataset=self.dataset_name,
                stage="extract",
                overwrite=self.overwrite_output,
                metadata={**metadata, "filing_count": len(rows)},
            )
        )
        return GenericResult(value={**plan, "status": write.status, "filing_count": len(rows), "output_writes": [write.model_dump(mode="json")]})


class SECFilerRefreshBundleModel(CallableModel):
    """Fetch current company facts and submissions for every company that filed financial forms on a date.

    Filers come from that date's stored daily index, which is declared as a graph dependency. Records are staged to
    a local JSONL file so an interrupted run resumes, then compressed and stored as one object per date.
    """

    index_extract: SECDailyIndexExtractModel
    facts_model: SECHTTPModel
    submissions_model: SECHTTPModel
    output: Any = None
    staging_directory: Path = Path("/tmp/finance-etl/sec-filer-refresh")
    output_key_prefix: str = "sec/edgar/filer-refresh"
    dataset_name: str = "sec-edgar-filer-refresh"
    form_types: list[str] = Field(default_factory=lambda: list(DEFAULT_FINANCIAL_FORMS))
    max_filers: int | None = Field(default=None, ge=1)
    overwrite_output: bool = False
    explain: bool = False
    media_type: str = "application/gzip"

    @property
    def context_type(self) -> type[SECDateContext]:
        return SECDateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def output_key(self, context: SECDateContext) -> str:
        return f"{self.output_key_prefix.strip('/')}/jsonl/{context.date.isoformat()}.jsonl.gz"

    def _will_fetch(self, context: SECDateContext) -> bool:
        if self.explain or self.output is None:
            return False
        return self.overwrite_output or not (hasattr(self.output, "exists") and self.output.exists(self.output_key(context)))

    @Flow.deps
    def __deps__(self, context: SECDateContext) -> GraphDepList:
        return [(self.index_extract, [context])] if self._will_fetch(context) else []

    def _filers(self, context: SECDateContext) -> list[dict[str, Any]]:
        self.index_extract(context)
        key = self.index_extract.output_key(context)
        if not self.index_extract.output.exists(key):
            return []
        text = ArtifactReadModel(store=self.index_extract.output)(ArtifactReadContext(key=key)).payload.decode()
        wanted = set(self.form_types)
        filers: dict[str, dict[str, Any]] = {}
        for row in parse_master_index(text):
            if row["form_type"] in wanted:
                filer = filers.setdefault(row["cik"], {"cik": row["cik"], "company_name": row["company_name"], "filings": []})
                filer["filings"].append({"form_type": row["form_type"], "filename": row["filename"]})
        selected = sorted(filers.values(), key=lambda filer: int(filer["cik"]))
        return selected[: self.max_filers] if self.max_filers is not None else selected

    def _fetch(self, model: SECHTTPModel, path: str) -> tuple[int, Any]:
        result = model.model_copy(update={"path": path})(HTTPRequestContext())
        return result.status_code, result.value

    def _record(self, context: SECDateContext, filer: dict[str, Any]) -> dict[str, Any]:
        padded = f"CIK{int(filer['cik']):010d}.json"
        facts_status, facts = self._fetch(self.facts_model, f"/api/xbrl/companyfacts/{padded}")
        submissions_status, submissions = self._fetch(self.submissions_model, f"/submissions/{padded}")
        return {
            "date": context.date.isoformat(),
            **filer,
            "companyfacts_status": facts_status,
            "companyfacts": facts,
            "submissions_status": submissions_status,
            "submissions": submissions,
        }

    def _completed(self, path: Path) -> set[str]:
        completed: set[str] = set()
        if not path.exists():
            return completed
        with path.open("rb+") as staged:
            while True:
                start = staged.tell()
                line = staged.readline()
                if not line:
                    break
                try:
                    completed.add(str(json.loads(line)["cik"]))
                except (json.JSONDecodeError, KeyError):
                    staged.seek(start)
                    staged.truncate()
                    break
        return completed

    @Flow.call
    def __call__(self, context: SECDateContext) -> GenericResult:
        key = self.output_key(context)
        metadata = {"date": context.date.isoformat(), "provider": "sec", "dataset": self.dataset_name, "format": "jsonl", "compression": "gzip"}
        plan = {
            "dataset": self.dataset_name,
            "date": context.date.isoformat(),
            "output_key": key,
            "output_uri": _artifact_uri(self.output, key),
            "form_types": self.form_types,
        }
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if self.output is None:
            raise ValueError("SEC filer refresh requires output.")
        if not self._will_fetch(context):
            return GenericResult(
                value={
                    **plan,
                    "status": "exists",
                    "output_writes": [_existing_artifact(self.output, key, self.dataset_name, self.media_type, metadata)],
                }
            )

        filers = self._filers(context)
        self.staging_directory.mkdir(parents=True, exist_ok=True)
        partial = self.staging_directory / f"{context.date.isoformat()}.jsonl.partial"
        compressed = self.staging_directory / f"{context.date.isoformat()}.jsonl.gz"
        completed = self._completed(partial)
        with partial.open("a", encoding="utf-8") as staged:
            for filer in filers:
                if filer["cik"] in completed:
                    continue
                staged.write(json.dumps(self._record(context, filer), sort_keys=True, separators=(",", ":")) + "\n")
                staged.flush()
        with partial.open("rb") as source, gzip.open(compressed, "wb") as destination:
            for block in iter(lambda: source.read(1 << 20), b""):
                destination.write(block)
        metadata = {**metadata, "filer_count": len(filers)}
        response = self.output.write_file(key, compressed, media_type=self.media_type, metadata=metadata)
        status = str(response.get("status", "written")) if isinstance(response, dict) else "written"
        partial.unlink(missing_ok=True)
        compressed.unlink(missing_ok=True)
        artifact = ETLArtifact(
            key=key,
            stage="extract",
            status=status,
            dataset=self.dataset_name,
            uri=_artifact_uri(self.output, key),
            media_type=self.media_type,
            metadata=metadata,
        )
        return GenericResult(value={**plan, "status": status, "filer_count": len(filers), "output_writes": [artifact.model_dump(mode="json")]})
