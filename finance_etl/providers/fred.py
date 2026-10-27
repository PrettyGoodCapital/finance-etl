import gzip
import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ccflow import CallableModel, DateContext, Flow, GenericResult
from ccflow_etl import APITokenCredentials, DatasetDefinition, ExecutionPolicy
from ccflow_http import HTTPModel, HTTPRequest, HTTPRequestContext, HTTPRetryPolicy, RedactionPolicy
from ccflow_http.base import HTTPContext
from pydantic import Field

__all__ = (
    "FREDCaptureModel",
    "FREDCredentials",
    "FREDHTTPModel",
    "FREDObservationsModel",
)

# FRED allows 120 requests a minute per API key.
_DEFAULT_FRED_POLICY = ExecutionPolicy(min_interval_seconds=0.6)
# The earliest and latest real-time dates FRED accepts; together they request every vintage.
_REALTIME_START = date(1776, 7, 4)
_REALTIME_END = date(9999, 12, 31)
_PAGE_LIMIT = 100000


class FREDCredentials(APITokenCredentials):
    token_env: str | None = "FRED_API_KEY"


class FREDHTTPModel(HTTPModel):
    """GET a FRED API path with the API key and ``file_type=json`` added to the query."""

    base_url: str = "https://api.stlouisfed.org"
    path: str = "/fred/series"
    credentials: FREDCredentials = Field(default_factory=FREDCredentials)
    execution_policy: ExecutionPolicy | None = Field(default_factory=lambda: _DEFAULT_FRED_POLICY.model_copy())
    retry_policy: HTTPRetryPolicy | None = Field(
        default_factory=lambda: HTTPRetryPolicy(max_attempts=6, retry_status_codes=[429, 500, 502, 503, 504])
    )
    redaction: RedactionPolicy = Field(default_factory=lambda: RedactionPolicy().with_names("api_key"))

    def build_request(self, context: HTTPContext | None = None) -> HTTPRequest:
        request = super().build_request(context or HTTPRequestContext())
        query = {**request.params, "file_type": "json"}
        token = self.credentials.resolved_token()
        if token:
            query["api_key"] = token
        return request.model_copy(update={"params": query})


class FREDCaptureModel(CallableModel):
    """Capture FRED series metadata and observation vintages for a capture date, one JSONL bundle per date.

    Each row holds one series: its ``/fred/series`` metadata (including notes, where copyright terms appear) and, for
    series in ``series``, every observation whose vintage began on or after the requested real-time start. With
    ``realtime_lookback_days`` unset the capture requests every vintage (a baseline); otherwise it requests vintages
    that began within that many days of the capture date, which a daily run uses to pick up new releases and
    revisions. Series in ``metadata_only`` get metadata alone, for reviewing their terms before capture.
    """

    http_model: FREDHTTPModel = Field(default_factory=FREDHTTPModel)
    series: list[str] = Field(default_factory=list, description="Series to capture with observations.")
    metadata_only: list[str] = Field(default_factory=list, description="Series to capture metadata for, without observations.")
    observation_start: date = Field(description="Earliest observation date to request.")
    realtime_lookback_days: int | None = Field(
        default=None, ge=1, description="Vintages that began this many days before the capture date; every vintage when unset."
    )
    output: DatasetDefinition
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _get(self, path: str, query: dict[str, Any]) -> Any:
        http_model = self.http_model.model_copy(update={"accepted_status_codes": [400]})
        result = http_model(HTTPRequestContext(path=path, query={key: str(value) for key, value in query.items()}))
        if result.status_code == 400:
            # FRED explains a rejected request in error_message; surface it with the series.
            message = result.value.get("error_message") if isinstance(result.value, dict) else result.value
            raise RuntimeError(f"FRED rejected {path} for {query.get('series_id')}: {message}")
        return result.value

    def _metadata(self, series_id: str) -> dict[str, Any]:
        payload = self._get("/fred/series", {"series_id": series_id})
        series = payload.get("seriess") if isinstance(payload, dict) else None
        return series[0] if series else {}

    def _observations(self, series_id: str, realtime_start: date) -> list[dict[str, Any]]:
        observations: list[dict[str, Any]] = []
        offset = 0
        while True:
            payload = self._get(
                "/fred/series/observations",
                {
                    "series_id": series_id,
                    "observation_start": self.observation_start.isoformat(),
                    "realtime_start": realtime_start.isoformat(),
                    "realtime_end": _REALTIME_END.isoformat(),
                    "limit": _PAGE_LIMIT,
                    "offset": offset,
                },
            )
            page = payload.get("observations", []) if isinstance(payload, dict) else []
            observations.extend(page)
            offset += len(page)
            if not page or offset >= int(payload.get("count", 0)):
                return observations

    def _realtime_start(self, value: date) -> date:
        return value - timedelta(days=self.realtime_lookback_days) if self.realtime_lookback_days else _REALTIME_START

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        realtime_start = self._realtime_start(context.date)
        plan = {
            **self.output.plan(context.date),
            "series_count": len(self.series),
            "metadata_only_count": len(self.metadata_only),
            "realtime_start": realtime_start.isoformat(),
            "observation_start": self.observation_start.isoformat(),
        }
        if self.explain:
            return GenericResult(value={**plan, "status": "planned", "will_call_network": False})
        if not self.overwrite_output and self.output.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})
        captured_at = datetime.now(UTC).isoformat()
        rows = []
        for series_id in [*self.series, *self.metadata_only]:
            observations = self._observations(series_id, realtime_start) if series_id in self.series else None
            rows.append(
                {
                    "series_id": series_id,
                    "captured_at": captured_at,
                    "realtime_start": realtime_start.isoformat(),
                    "metadata": self._metadata(series_id),
                    "observations": observations,
                }
            )
        payload = gzip.compress(b"".join(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n" for row in rows))
        observation_count = sum(len(row["observations"] or []) for row in rows)
        metadata = {"series_count": len(rows), "observation_count": observation_count, "realtime_start": realtime_start.isoformat()}
        writes = self.output.write_bytes(context.date, payload, metadata=metadata, overwrite=self.overwrite_output)
        return GenericResult(value={**plan, "status": "written", **metadata, "output_writes": writes})


class FREDObservationsModel(CallableModel):
    """Build one point-in-time table of FRED observations from a baseline capture and the daily captures after it.

    Each row is a value of a series for an observation date during one vintage: ``valid_from`` and ``valid_to`` are
    FRED's real-time start and end (the dates the value was the latest published), with ``valid_to`` open for current
    values. Rows seen in several captures are kept once, preferring the latest capture, so a value whose vintage
    closed after the baseline gets its end date.
    """

    baseline: DatasetDefinition = Field(description="Full-history captures, partitioned by capture date.")
    daily: DatasetDefinition | None = Field(default=None, description="Daily captures of recent vintages, partitioned by capture date.")
    output: DatasetDefinition = Field(description="One current table (a key without a date).")
    lookback_days: int = Field(default=3660, ge=1, description="How far back to look for a baseline capture on or before the date.")
    explain: bool = False
    overwrite_output: bool = False

    @property
    def context_type(self) -> type[DateContext]:
        return DateContext

    @property
    def result_type(self) -> type[GenericResult]:
        return GenericResult

    def _baseline(self, value: date) -> date | None:
        dates = self.baseline.partitions(value - timedelta(days=self.lookback_days), value)
        return next((day for day in reversed(dates) if self.baseline.exists(day)), None)

    @staticmethod
    def _rows(dataset: DatasetDefinition, value: date) -> list[dict[str, Any]]:
        rows = []
        for line in gzip.decompress(dataset.read_bytes(value)).splitlines():
            capture = json.loads(line)
            for observation in capture.get("observations") or []:
                rows.append(
                    {
                        "series_id": capture["series_id"],
                        "date": observation["date"],
                        "valid_from": observation["realtime_start"],
                        "valid_to": observation["realtime_end"],
                        "raw_value": observation["value"],
                        "captured": value.isoformat(),
                    }
                )
        return rows

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        import polars as pl

        plan = {**self.output.plan(context.date), "baseline": self.baseline.name}
        if self.explain:
            return GenericResult(value={**plan, "status": "planned"})
        if not self.overwrite_output and self.output.exists(context.date):
            return GenericResult(value={**plan, "status": "exists"})
        baseline = self._baseline(context.date)
        if baseline is None:
            return GenericResult(value={**plan, "status": "no_baseline"})
        rows = self._rows(self.baseline, baseline)
        captures = [baseline.isoformat()]
        if self.daily is not None:
            for day in self.daily.partitions(baseline + timedelta(days=1), context.date):
                if self.daily.exists(day):
                    rows.extend(self._rows(self.daily, day))
                    captures.append(day.isoformat())
        schema = {
            "series_id": pl.String,
            "date": pl.String,
            "valid_from": pl.String,
            "valid_to": pl.String,
            "raw_value": pl.String,
            "captured": pl.String,
        }
        end = pl.col("valid_to").str.to_date()
        frame = (
            pl.DataFrame(rows, schema=schema)
            .sort("captured")
            .unique(["series_id", "date", "valid_from"], keep="last", maintain_order=True)
            .select(
                "series_id",
                pl.col("date").str.to_date(),
                # FRED marks missing values with ".".
                pl.col("raw_value").replace(".", None).cast(pl.Float64).alias("value"),
                pl.col("valid_from").str.to_date(),
                pl.when(end == _REALTIME_END).then(None).otherwise(end).alias("valid_to"),
            )
            .sort("series_id", "date", "valid_from")
        )
        lineage = {
            "sources": {"baseline": self.baseline.uri(baseline), "daily": captures[1:]},
            "quality": {"row_count": frame.height, "series_count": frame["series_id"].n_unique(), "capture_count": len(captures)},
        }
        writes = self.output.write_frame(context.date, frame, lineage=lineage, overwrite=self.overwrite_output)
        return GenericResult(
            value={**plan, "status": "written", "baseline_date": baseline.isoformat(), **lineage["quality"], "output_writes": writes}
        )
