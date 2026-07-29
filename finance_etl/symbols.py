import json
from datetime import date
from typing import Any

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from ccflow_etl import ArtifactReadContext, ArtifactReadModel
from pydantic import BaseModel, Field, model_validator

__all__ = (
    "ArtifactSymbolUniverseModel",
    "ExplicitSymbolUniverseModel",
    "SymbolUniverseContext",
    "SymbolUniverseResult",
)


class SymbolUniverseContext(DateContext):
    @model_validator(mode="wrap")
    @classmethod
    def validate_date_only_context(cls, value, handler, info):
        if not isinstance(value, (cls, dict)):
            if isinstance(value, (tuple, list)) and len(value) == 1:
                value = value[0]
            value = {"date": value}
        return handler(value)


class SymbolUniverseResult(BaseModel):
    as_of_date: date
    symbols: list[str] = Field(default_factory=list)
    source: str = "explicit"
    snapshot_uri: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_symbols(self):
        self.symbols = sorted({str(symbol).strip().upper() for symbol in self.symbols if str(symbol).strip()})
        return self


class ExplicitSymbolUniverseModel(CallableModel):
    symbols: list[str] = Field(default_factory=list)
    source: str = "explicit"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def context_type(self) -> type[ContextType]:
        return SymbolUniverseContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        return GenericResult(
            value=SymbolUniverseResult(
                as_of_date=context.date,
                symbols=self.symbols,
                source=self.source,
                metadata=dict(self.metadata),
            )
        )


class ArtifactSymbolUniverseModel(CallableModel):
    store: Any
    key_template: str
    records_key: str | None = "results"
    symbol_field: str = "ticker"
    source: str = "artifact"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def context_type(self) -> type[ContextType]:
        return SymbolUniverseContext

    @property
    def result_type(self) -> type[ResultType]:
        return GenericResult

    def artifact_key(self, context: DateContext) -> str:
        return self.key_template.format(date=context.date.isoformat())

    def _artifact_uri(self, key: str) -> str:
        if hasattr(self.store, "artifact_uri"):
            return self.store.artifact_uri(key)
        if hasattr(self.store, "uri"):
            return self.store.uri(key)
        return key

    def _payload(self, key: str) -> Any:
        result = ArtifactReadModel(store=self.store)(ArtifactReadContext(key=key))
        return json.loads(result.payload)

    def _records(self, payload: Any) -> list[Any]:
        if self.records_key is None:
            return payload if isinstance(payload, list) else []
        if isinstance(payload, dict):
            records = payload.get(self.records_key, [])
            return records if isinstance(records, list) else []
        return []

    def _symbols(self, payload: Any) -> list[str]:
        symbols = []
        for record in self._records(payload):
            if isinstance(record, dict):
                symbol = record.get(self.symbol_field)
                if symbol is not None:
                    symbols.append(str(symbol))
        return symbols

    def _normalized_symbols(self, payload: Any) -> list[str]:
        return sorted({symbol.strip().upper() for symbol in self._symbols(payload) if symbol.strip()})

    @Flow.call
    def __call__(self, context: DateContext) -> GenericResult:
        key = self.artifact_key(context)
        payload = self._payload(key)
        records = self._records(payload)
        symbols = self._normalized_symbols(payload)
        return GenericResult(
            value=SymbolUniverseResult(
                as_of_date=context.date,
                symbols=symbols,
                source=self.source,
                snapshot_uri=self._artifact_uri(key),
                metadata={
                    **self.metadata,
                    "key": key,
                    "record_count": len(records),
                    "ticker_count": len(symbols),
                },
            )
        )
