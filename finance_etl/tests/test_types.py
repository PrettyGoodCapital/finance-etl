import pytest

from finance_etl import (
    BacktestResultRecord,
    CorporateActionRecord,
    ExchangePartitionContext,
    ExchangeSessionRecord,
    InstrumentHandle,
    ListingRecord,
    OptimizerAllocation,
    PartitionComputationResult,
    PortfolioDatePartitionContext,
    PortfolioSnapshot,
    ReportDatePartitionContext,
    ReportMetadata,
    SchemaMetadata,
    SecurityRecord,
    SessionDatePartitionContext,
    SignalRecord,
    StrategyDatePartitionContext,
    TargetPositionRecord,
    TickerDatePartitionContext,
    TickerPartitionContext,
    UniverseMember,
)


def test_instrument_handle_defaults_are_generic_and_stable():
    handle = InstrumentHandle(symbol="AAPL", exchange="XNYS")

    assert handle.instrument_id == "XNYS:AAPL"
    assert handle.listing_state == "active"
    assert handle.identifiers == {}


def test_universe_member_and_signal_types_round_trip_dates():
    member = UniverseMember(symbol="AAPL", exchange="XNYS", as_of_date="2024-01-03", close=184.95)

    signal = SignalRecord(as_of_date="2024-01-03", instrument_id=member.instrument_id, value=0.12)

    assert member.instrument_id == "XNYS:AAPL"
    assert signal.instrument_id == "XNYS:AAPL"
    assert signal.as_of_date.isoformat() == "2024-01-03"


def test_optimizer_and_target_position_types_capture_allocations():
    allocation = OptimizerAllocation(as_of_date="2024-01-03", instrument_id="XNYS:AAPL", weight=0.35, score=1.1)

    target = TargetPositionRecord(
        as_of_date="2024-01-03",
        instrument_id=allocation.instrument_id,
        target_weight=allocation.weight,
        target_notional=350000.0,
        target_quantity=1892.4,
    )

    assert target.instrument_id == "XNYS:AAPL"
    assert target.target_weight == 0.35
    assert target.target_notional == 350000.0


def test_expanded_generic_record_types_are_constructible():
    instrument = InstrumentHandle(symbol="AAPL", exchange="XNYS")
    security = SecurityRecord(instrument=instrument, security_type="equity", currency="USD", issuer_name="Apple Inc")
    listing = ListingRecord(instrument_id=instrument.instrument_id, exchange="XNYS", active=True)

    session = ExchangeSessionRecord(
        exchange="XNYS",
        session_date="2024-01-03",
        open_time="2024-01-03T14:30:00+00:00",
        close_time="2024-01-03T21:00:00+00:00",
    )
    action = CorporateActionRecord(instrument_id=instrument.instrument_id, effective_date="2024-01-15", action_type="split", ratio=2.0)
    snapshot = PortfolioSnapshot(
        as_of_date="2024-01-03",
        portfolio_id="core",
        instrument_id=instrument.instrument_id,
        quantity=100.0,
        market_value=18495.0,
    )
    backtest = BacktestResultRecord(strategy_id="mean-reversion", as_of_date="2024-01-03", return_pct=0.012, turnover=0.14)
    report = ReportMetadata(
        report_id="risk-2024-01-03",
        report_type="risk",
        as_of_date="2024-01-03",
        schema_metadata=SchemaMetadata(schema_name="risk-report", schema_version=1),
    )

    assert security.instrument.instrument_id == "XNYS:AAPL"
    assert listing.exchange == "XNYS"
    assert session.exchange == "XNYS"
    assert action.action_type == "split"
    assert snapshot.market_value == 18495.0
    assert backtest.strategy_id == "mean-reversion"
    assert report.schema_metadata.schema_name == "risk-report"


def test_schema_metadata_compatibility_checks_fail_cleanly():
    schema = SchemaMetadata(schema_name="signals", schema_version=2)

    assert schema.is_compatible_with(expected_name="signals", min_version=1, max_version=3)
    assert not schema.is_compatible_with(expected_name="signals", min_version=3, max_version=4)
    assert not schema.is_compatible_with(expected_name="portfolio", min_version=1, max_version=3)

    with pytest.raises(ValueError, match="Schema metadata mismatch"):
        schema.assert_compatible(expected_name="signals", min_version=3, max_version=4)


def test_partition_context_and_result_types_are_constructible():
    exchange = ExchangePartitionContext(exchange="XNYS", as_of_date="2024-01-03")
    session = SessionDatePartitionContext(session_date="2024-01-03", exchange="XNYS")
    ticker_only = TickerPartitionContext(ticker="AAPL", exchange="XNYS")
    ticker = TickerDatePartitionContext(ticker="AAPL", exchange="XNYS", as_of_date="2024-01-03")
    portfolio = PortfolioDatePartitionContext(portfolio_id="core", as_of_date="2024-01-03")
    strategy = StrategyDatePartitionContext(strategy_id="mean-reversion", as_of_date="2024-01-03")
    report = ReportDatePartitionContext(report_id="risk-2024-01-03", report_type="risk", as_of_date="2024-01-03")
    result = PartitionComputationResult(partition_key=ticker.instrument_id, as_of_date="2024-01-03", row_count=12)

    assert exchange.exchange == "XNYS"
    assert session.session_date.isoformat() == "2024-01-03"
    assert ticker_only.instrument_id == "XNYS:AAPL"
    assert ticker.instrument_id == "XNYS:AAPL"
    assert portfolio.portfolio_id == "core"
    assert strategy.strategy_id == "mean-reversion"
    assert report.report_type == "risk"
    assert result.row_count == 12
