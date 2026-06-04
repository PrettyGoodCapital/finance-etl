from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Dict, Optional

from pydantic import BaseModel, Field, model_validator

__all__ = (
    "SchemaMetadata",
    "PartitionComputationResult",
    "ExchangePartitionContext",
    "MarketPartitionContext",
    "TickerPartitionContext",
    "SessionDatePartitionContext",
    "TickerDatePartitionContext",
    "PortfolioDatePartitionContext",
    "StrategyDatePartitionContext",
    "ReportDatePartitionContext",
    "InstrumentHandle",
    "SecurityRecord",
    "ListingRecord",
    "ExchangeSessionRecord",
    "CorporateActionRecord",
    "PortfolioSnapshot",
    "BacktestResultRecord",
    "ReportMetadata",
    "UniverseMember",
    "SignalRecord",
    "OptimizerAllocation",
    "TargetPositionRecord",
)


class SchemaMetadata(BaseModel):
    schema_name: str
    schema_version: int = 1
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def is_compatible_with(self, expected_name: str, min_version: int, max_version: int) -> bool:
        if self.schema_name != expected_name:
            return False
        return min_version <= self.schema_version <= max_version

    def assert_compatible(self, expected_name: str, min_version: int, max_version: int) -> None:
        if not self.is_compatible_with(expected_name=expected_name, min_version=min_version, max_version=max_version):
            raise ValueError(
                f"Schema metadata mismatch: expected {expected_name} v{min_version}-{max_version}, "
                f"received {self.schema_name} v{self.schema_version}."
            )


class PartitionComputationResult(BaseModel):
    partition_key: str
    as_of_date: date
    row_count: int = 0
    schema_metadata: Optional[SchemaMetadata] = None


class ExchangePartitionContext(BaseModel):
    exchange: str
    as_of_date: date


class MarketPartitionContext(BaseModel):
    market: str = "equities"
    exchange: Optional[str] = None
    as_of_date: date


class TickerPartitionContext(BaseModel):
    ticker: str
    exchange: str = "XNYS"
    market: str = "equities"
    instrument_id: Optional[str] = None

    @model_validator(mode="after")
    def _default_instrument_id(self):
        if not self.instrument_id:
            self.instrument_id = f"{self.exchange}:{self.ticker}"
        return self


class SessionDatePartitionContext(BaseModel):
    session_date: date
    exchange: str = "XNYS"


class TickerDatePartitionContext(BaseModel):
    ticker: str
    exchange: str = "XNYS"
    as_of_date: date
    instrument_id: Optional[str] = None

    @model_validator(mode="after")
    def _default_instrument_id(self):
        if not self.instrument_id:
            self.instrument_id = f"{self.exchange}:{self.ticker}"
        return self


class PortfolioDatePartitionContext(BaseModel):
    portfolio_id: str
    as_of_date: date


class StrategyDatePartitionContext(BaseModel):
    strategy_id: str
    as_of_date: date


class ReportDatePartitionContext(BaseModel):
    report_id: str
    report_type: str
    as_of_date: date


class InstrumentHandle(BaseModel):
    symbol: str
    exchange: str = "XNYS"
    instrument_id: Optional[str] = None
    vendor_id: Optional[str] = None
    listing_state: str = "active"
    effective_time: Optional[datetime] = None
    knowledge_time: Optional[datetime] = None
    identifiers: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _default_instrument_id(self):
        if not self.instrument_id:
            self.instrument_id = f"{self.exchange}:{self.symbol}"
        return self


class SecurityRecord(BaseModel):
    instrument: InstrumentHandle
    security_type: str = "equity"
    currency: str = "USD"
    issuer_name: Optional[str] = None


class ListingRecord(BaseModel):
    instrument_id: str
    exchange: str
    listed_at: Optional[datetime] = None
    delisted_at: Optional[datetime] = None
    active: bool = True


class ExchangeSessionRecord(BaseModel):
    exchange: str
    session_date: date
    open_time: datetime
    close_time: datetime


class CorporateActionRecord(BaseModel):
    instrument_id: str
    effective_date: date
    action_type: str
    ratio: Optional[float] = None
    amount: Optional[float] = None


class PortfolioSnapshot(BaseModel):
    as_of_date: date
    portfolio_id: str
    instrument_id: str
    quantity: float
    market_value: float
    cash_value: Optional[float] = None


class BacktestResultRecord(BaseModel):
    strategy_id: str
    as_of_date: date
    return_pct: float
    turnover: Optional[float] = None
    drawdown: Optional[float] = None


class ReportMetadata(BaseModel):
    report_id: str
    report_type: str
    as_of_date: date
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    schema_metadata: SchemaMetadata


class UniverseMember(BaseModel):
    as_of_date: date
    symbol: str
    exchange: str = "XNYS"
    instrument_id: Optional[str] = None
    close: Optional[float] = None
    currency: str = "USD"

    @model_validator(mode="after")
    def _default_instrument_id(self):
        if not self.instrument_id:
            self.instrument_id = f"{self.exchange}:{self.symbol}"
        return self


class SignalRecord(BaseModel):
    as_of_date: date
    instrument_id: str
    signal_name: str = "alpha"
    horizon_days: int = 1
    value: float


class OptimizerAllocation(BaseModel):
    as_of_date: date
    instrument_id: str
    weight: float
    score: Optional[float] = None


class TargetPositionRecord(BaseModel):
    as_of_date: date
    instrument_id: str
    target_weight: float
    target_notional: float
    target_quantity: Optional[float] = None
    currency: str = "USD"
