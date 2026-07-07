from __future__ import annotations

from finance_etl import ArtifactSymbolUniverseModel


class FakeArtifactStore:
    def __init__(self, payload: bytes):
        self.payload = payload

    def artifact_uri(self, key: str) -> str:
        return f"fake://{key}"

    def read(self, key: str) -> bytes:
        return self.payload


def test_artifact_symbol_universe_reads_massive_all_tickers_payload():
    store = FakeArtifactStore(b'{"results":[{"ticker":"msft"},{"ticker":"AAPL"},{"ticker":"AAPL"},{"ticker":""},{"name":"missing"}]}')
    model = ArtifactSymbolUniverseModel(
        store=store,
        key_template="massive/stocks/rest/all-tickers/{date}.json",
        source="massive-stocks-rest-all-tickers-artifact",
    )

    result = model(["2025-01-02"]).value

    assert result.as_of_date.isoformat() == "2025-01-02"
    assert result.symbols == ["AAPL", "MSFT"]
    assert result.source == "massive-stocks-rest-all-tickers-artifact"
    assert result.snapshot_uri == "fake://massive/stocks/rest/all-tickers/2025-01-02.json"
    assert result.metadata == {
        "key": "massive/stocks/rest/all-tickers/2025-01-02.json",
        "record_count": 5,
        "ticker_count": 2,
    }


def test_artifact_symbol_universe_can_read_root_list_payload():
    store = FakeArtifactStore(b'[{"symbol":"spy"},{"symbol":"QQQ"}]')
    model = ArtifactSymbolUniverseModel(
        store=store,
        key_template="symbols/{date}.json",
        records_key=None,
        symbol_field="symbol",
    )

    result = model(["2025-01-02"]).value

    assert result.symbols == ["QQQ", "SPY"]
    assert result.metadata["record_count"] == 2
    assert result.metadata["ticker_count"] == 2
