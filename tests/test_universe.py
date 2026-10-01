from datetime import UTC, date, datetime
from io import BytesIO
from xml.sax.saxutils import escape
from zipfile import ZipFile

import pytest

import sweepflow.universe as universe
from sweepflow.universe import (
    SPY_HOLDINGS_URL,
    fetch_sp500_universe,
    load_symbols,
    normalize_symbol,
    parse_spy_holdings,
    save_universe,
)


def workbook(rows, *, shared=False):
    cells, strings = [], []
    for index, values in enumerate(rows, start=1):
        values_xml = []
        for column, value in values.items():
            if shared:
                strings.append(value)
                values_xml.append(f'<c r="{column}{index}" t="s"><v>{len(strings) - 1}</v></c>')
            else:
                values_xml.append(
                    f'<c r="{column}{index}" t="inlineStr"><is><t>{escape(value)}</t></is></c>'
                )
        cells.append(f'<row r="{index}">{"".join(values_xml)}</row>')
    stream = BytesIO()
    namespace = 'xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"'
    with ZipFile(stream, "w") as archive:
        archive.writestr(
            "xl/worksheets/sheet1.xml",
            f"<worksheet {namespace}><sheetData>{''.join(cells)}</sheetData></worksheet>",
        )
        if shared:
            values = "".join(f"<si><t>{escape(text)}</t></si>" for text in strings)
            archive.writestr("xl/sharedStrings.xml", f"<sst {namespace}>{values}</sst>")
    return stream.getvalue()


def holdings(*rows, as_of="25-Sep-2026", shared=False):
    return workbook(
        [
            {"A": "Holdings:", "B": f"As of {as_of}"},
            {"A": "Name", "B": "Ticker", "D": "Sector"},
            *({"A": name, "B": ticker} for name, ticker in rows),
        ],
        shared=shared,
    )


@pytest.mark.parametrize("shared", [False, True])
def test_state_street_holdings_shape_and_non_equities(shared):
    payload = holdings(
        ("APPLE INC", "AAPL"),
        ("BERKSHIRE HATHAWAY", "BRK.B"),
        ("Cash", "USD"),
        ("Corporate action rights", "2602335D"),
        shared=shared,
    )
    snapshot = parse_spy_holdings(payload, retrieved_at=datetime(2026, 9, 28, tzinfo=UTC))
    assert snapshot.symbols == ("AAPL", "BRK.B")
    assert snapshot.as_of == date(2026, 9, 25)
    assert snapshot.source == SPY_HOLDINGS_URL
    assert (
        snapshot.to_dict()["description"] == "Equity symbols in State Street's daily SPY holdings"
    )


def test_snapshot_roundtrip(tmp_path):
    snapshot = parse_spy_holdings(
        holdings(("APPLE", "AAPL")),
        retrieved_at=datetime(2026, 9, 28, tzinfo=UTC),
    )
    path = tmp_path / "constituents.json"
    save_universe(snapshot, path)
    assert load_symbols(path) == ("AAPL",)


def test_missing_invalid_or_future_date_rejected():
    rows = [{"A": "Name", "B": "Ticker"}, {"A": "Apple", "B": "AAPL"}]
    with pytest.raises(ValueError, match="missing"):
        parse_spy_holdings(workbook(rows))
    with pytest.raises(ValueError, match="Unrecognized"):
        parse_spy_holdings(holdings(("Apple", "AAPL"), as_of="bad-date"))
    with pytest.raises(ValueError, match="future"):
        parse_spy_holdings(
            holdings(("Apple", "AAPL"), as_of="25-Sep-2099"),
            retrieved_at=datetime(2026, 9, 28, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="timezone"):
        parse_spy_holdings(
            holdings(("Apple", "AAPL")),
            retrieved_at=datetime(2026, 9, 28),
        )


def test_duplicate_equity_symbols_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        parse_spy_holdings(holdings(("Apple", "AAPL"), ("Apple", "AAPL")))


def test_fetch_uses_official_state_street_spy_holdings(monkeypatch):
    def ticker(index: int) -> str:
        first, second = divmod(index, 26)
        return f"T{chr(65 + first)}{chr(65 + second)}"

    payload = holdings(*[(f"Company {index}", ticker(index)) for index in range(450)])

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self, size):
            assert size == 1_000_001
            return payload

    def fake_urlopen(request, *, timeout):
        assert request.full_url == SPY_HOLDINGS_URL
        assert request.headers["User-agent"] == "sweepflow-research/0.1"
        assert timeout == 12
        return Response()

    monkeypatch.setattr(universe, "urlopen", fake_urlopen)
    snapshot = fetch_sp500_universe(timeout=12)
    assert len(snapshot.symbols) == 450
    assert snapshot.as_of == date(2026, 9, 25)


@pytest.mark.parametrize(
    "text,expected",
    [
        ("# watched constituents\nAAPL\nBRK-B\nAAPL", ("AAPL", "BRK.B")),
        ("company,symbol\nApple,AAPL\nMicrosoft,msft", ("AAPL", "MSFT")),
        ('["SPY", "VOO"]', ("SPY", "VOO")),
        ("SPY,VOO,IVV", ("SPY", "VOO", "IVV")),
    ],
)
def test_symbol_file_formats(tmp_path, text, expected):
    path = tmp_path / "symbols.txt"
    path.write_text(text)
    assert load_symbols(path) == expected


@pytest.mark.parametrize(
    "text", ["", "# empty", '{"source":"no symbols"}', "SPY,$$$", "name,ticker\nApple"]
)
def test_bad_universe_files_rejected(tmp_path, text):
    path = tmp_path / "symbols.txt"
    path.write_text(text)
    with pytest.raises(ValueError):
        load_symbols(path)


@pytest.mark.parametrize("symbol", ["", "../../bad", "BTC-USD", "ES=F", "2602335D", None])
def test_invalid_equity_symbols_rejected(symbol):
    with pytest.raises(ValueError):
        normalize_symbol(symbol)
