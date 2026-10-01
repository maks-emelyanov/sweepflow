"""S&P 500 investable universe from State Street's daily SPY holdings.

The holdings are a current fund snapshot rather than point-in-time index
membership history. Use dated input universes for historical studies to avoid
survivorship bias.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from io import BytesIO, StringIO
from pathlib import Path
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from zipfile import BadZipFile, ZipFile

SP500_ETFS = ("SPY", "VOO", "IVV")
SPY_HOLDINGS_URL = (
    "https://www.ssga.com/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"
)
_SYMBOL = re.compile(r"[A-Z]{1,5}(?:\.[A-Z])?\Z")
_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def normalize_symbol(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("symbol must be text")
    value = value.strip().upper().replace("-", ".")
    if not _SYMBOL.fullmatch(value):
        raise ValueError(f"Invalid US equity symbol: {value!r}")
    return value


@dataclass(frozen=True)
class UniverseSnapshot:
    symbols: tuple[str, ...]
    as_of: date
    retrieved_at: datetime
    source: str = SPY_HOLDINGS_URL

    def to_dict(self) -> dict[str, object]:
        return {
            "symbols": list(self.symbols),
            "as_of": self.as_of.isoformat(),
            "retrieved_at": self.retrieved_at.isoformat(),
            "source": self.source,
            "description": "Equity symbols in State Street's daily SPY holdings",
        }


def load_symbols(path: str | Path) -> tuple[str, ...]:
    """Read JSON snapshots/lists or text/CSV ticker lists, without network access."""
    text = Path(path).read_text(encoding="utf-8-sig")
    if text.lstrip().startswith(("[", "{")):
        payload = json.loads(text)
        values = payload.get("symbols") if isinstance(payload, dict) else payload
        if not isinstance(values, list):
            raise ValueError("Universe JSON must be a list or contain a symbols list")
    else:
        lines = [
            line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")
        ]
        rows = list(csv.reader(StringIO("\n".join(lines))))
        if not rows:
            raise ValueError("Universe is empty")
        header = [column.strip().lower() for column in rows[0]]
        if "symbol" in header or "ticker" in header:
            field = header.index("symbol" if "symbol" in header else "ticker")
            if any(len(row) <= field for row in rows[1:]):
                raise ValueError("Malformed universe CSV row")
            values = [row[field] for row in rows[1:]]
        else:
            values = [value for row in rows for value in row]
    symbols = tuple(dict.fromkeys(normalize_symbol(value) for value in values))
    if not symbols:
        raise ValueError("Universe is empty")
    return symbols


def save_universe(snapshot: UniverseSnapshot, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot.to_dict(), indent=2) + "\n", encoding="utf-8")


def _spreadsheet_rows(content: bytes) -> list[dict[str, str]]:
    """Read State Street's small XLSX worksheet without an Excel dependency."""
    try:
        with ZipFile(BytesIO(content)) as archive:
            if sum(info.file_size for info in archive.infolist()) > 20_000_000:
                raise ValueError("SPY holdings spreadsheet is unexpectedly large")
            strings: list[str] = []
            if "xl/sharedStrings.xml" in archive.namelist():
                shared = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
                strings = ["".join(node.itertext()) for node in shared]
            sheet = ElementTree.fromstring(archive.read("xl/worksheets/sheet1.xml"))
            rows: list[dict[str, str]] = []
            for row in sheet.findall(".//s:row", _NS):
                values: dict[str, str] = {}
                for cell in row.findall("s:c", _NS):
                    column = re.sub(r"\d", "", cell.attrib["r"])
                    value = cell.findtext("s:v", default="", namespaces=_NS)
                    if cell.get("t") == "s":
                        value = strings[int(value)]
                    elif cell.get("t") == "inlineStr":
                        value = "".join(cell.find("s:is", _NS).itertext())
                    values[column] = value.strip()
                rows.append(values)
            return rows
    except (BadZipFile, KeyError, IndexError, ElementTree.ParseError, AttributeError) as exc:
        raise ValueError("Invalid State Street SPY holdings document") from exc


def parse_spy_holdings(content: bytes, *, retrieved_at: datetime | None = None) -> UniverseSnapshot:
    rows = _spreadsheet_rows(content)
    as_of: date | None = None
    header: dict[str, str] | None = None
    symbols: list[str] = []
    for row in rows:
        if row.get("A") == "Holdings:":
            value = row.get("B", "").removeprefix("As of ")
            try:
                as_of = datetime.strptime(value, "%d-%b-%Y").date()
            except ValueError as exc:
                raise ValueError(f"Unrecognized SPY holdings date: {value!r}") from exc
        if "Ticker" in row.values() and "Name" in row.values():
            header = {name: column for column, name in row.items()}
            continue
        if header is None:
            continue
        ticker = row.get(header["Ticker"], "")
        name = row.get(header["Name"], "")
        if not ticker or ticker in {"-", "USD", "CASH"} or "CASH" in name.upper():
            continue
        try:
            symbols.append(normalize_symbol(ticker))
        except ValueError:
            # State Street can include non-equity corporate-action identifiers.
            continue
    if as_of is None or header is None or not symbols:
        raise ValueError("SPY holdings document is missing its date, ticker header, or equities")
    if len(symbols) != len(set(symbols)):
        raise ValueError("SPY holdings document contains duplicate equity symbols")
    retrieved_at = retrieved_at or datetime.now(UTC)
    if retrieved_at.tzinfo is None or retrieved_at.utcoffset() is None:
        raise ValueError("retrieved_at must include a timezone")
    if as_of > retrieved_at.date():
        raise ValueError("SPY holdings document has a future date")
    return UniverseSnapshot(tuple(sorted(symbols)), as_of, retrieved_at)


def fetch_sp500_universe(*, timeout: float = 30.0) -> UniverseSnapshot:
    """Download and validate State Street's current daily SPY holdings."""
    request = Request(SPY_HOLDINGS_URL, headers={"User-Agent": "sweepflow-research/0.1"})
    with urlopen(request, timeout=timeout) as response:
        content = response.read(1_000_001)
    if len(content) > 1_000_000:
        raise ValueError("SPY holdings download is unexpectedly large")
    snapshot = parse_spy_holdings(content)
    if not 450 <= len(snapshot.symbols) <= 550:
        raise ValueError(f"Unexpected SPY equity universe size: {len(snapshot.symbols)}")
    return snapshot


def fetch_sp500_symbols(*, timeout: float = 30.0) -> tuple[str, ...]:
    return fetch_sp500_universe(timeout=timeout).symbols
