from datetime import date
from typing import Any, Dict, List, Optional, Type

from ccflow import CallableModel, ContextType, DateContext, Flow, GenericResult, ResultType
from pydantic import BaseModel, Field, model_validator

__all__ = (
    "SymbolUniverseContext",
    "SymbolUniverseResult",
    "ExplicitSymbolUniverseModel",
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
    symbols: List[str] = Field(default_factory=list)
    source: str = "explicit"
    snapshot_uri: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_symbols(self):
        self.symbols = sorted({str(symbol).strip().upper() for symbol in self.symbols if str(symbol).strip()})
        return self


class ExplicitSymbolUniverseModel(CallableModel):
    symbols: List[str] = Field(default_factory=list)
    source: str = "explicit"
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @property
    def context_type(self) -> Type[ContextType]:
        return SymbolUniverseContext

    @property
    def result_type(self) -> Type[ResultType]:
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
