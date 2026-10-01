"""Core workflows must work in an interpreter without the optional data wrapper."""

import subprocess
import sys
from pathlib import Path
from textwrap import dedent

from examples.make_demo import demo_bars
from sweepflow.data import FiveMinuteAggregator, write_csv

ROOT = Path(__file__).resolve().parents[1]
BLOCK_WRAPPER = """
import importlib.abc
import sys

class WithoutWrapper(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"robinhood_mcp", "mcp_types"}:
            raise ModuleNotFoundError("Optional integration unavailable", name=fullname)

sys.meta_path.insert(0, WithoutWrapper())
"""


def without_wrapper(source: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, "-c", BLOCK_WRAPPER + dedent(source), *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def test_cli_core_commands_work_without_wrapper(tmp_path):
    minutes = tmp_path / "minutes.csv"
    candles = tmp_path / "candles.csv"
    db = tmp_path / "audit.sqlite"
    universe_path = tmp_path / "universe.json"
    bars = demo_bars()
    aggregator = FiveMinuteAggregator()
    write_csv(minutes, bars)
    write_csv(candles, [out for bar in bars if (out := aggregator.on_bar(bar)) is not None])

    result = without_wrapper(
        """
        import json
        from datetime import UTC, date, datetime
        from pathlib import Path
        from unittest.mock import patch

        from sweepflow.cli import main
        from sweepflow.universe import UniverseSnapshot

        minutes, candles, db, universe_path = sys.argv[1:]
        try:
            main(["--help"])
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise AssertionError("Help did not exit successfully")
        assert main(["replay", minutes, "--db", db]) == 0
        assert main(["signals", candles]) == 0
        assert main(["audit", db]) == 0
        assert main(["live"]) == 1
        snapshot = UniverseSnapshot(
            ("SPY",), date(2026, 9, 22), datetime(2026, 9, 22, tzinfo=UTC)
        )
        with patch("sweepflow.cli.fetch_sp500_universe", return_value=snapshot):
            assert main(["universe", "--output", universe_path]) == 0
        assert json.loads(Path(universe_path).read_text())["symbols"] == ["SPY"]
        assert "sweepflow.robinhood" not in sys.modules
        """,
        str(minutes),
        str(candles),
        str(db),
        str(universe_path),
    )
    assert '"pnl": "792' in result.stdout
    assert '"event": "run_started"' in result.stdout
    assert "atomic entry/stop/target" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_network_commands_explain_missing_wrapper_before_fetching_universe():
    result = without_wrapper(
        """
        from unittest.mock import patch

        from sweepflow.cli import main

        with patch("sweepflow.cli.fetch_sp500_universe") as fetch:
            assert main(["discover"]) == 1
            assert main(["scan"]) == 1
            fetch.assert_not_called()
        """
    )
    assert result.stderr.count("optional robinhood-mcp-wrapper integration") == 2
    assert "uv pip install -e /path/to/robinhood" in result.stderr
    assert "Traceback" not in result.stderr


def test_fake_monitor_and_closed_market_paper_sync_work_without_wrapper():
    without_wrapper(
        """
        import asyncio
        from datetime import UTC, datetime

        from examples.make_demo import demo_bars
        from sweepflow.config import AppConfig
        from sweepflow.data import FiveMinuteAggregator
        from sweepflow.monitor import scan
        from sweepflow.paper import run_paper
        from sweepflow.storage import Journal

        aggregator = FiveMinuteAggregator()
        bars = [
            out for bar in demo_bars()
            if (out := aggregator.on_bar(bar)) is not None
        ]
        now = datetime(2026, 9, 22, 13, 45, 30, tzinfo=UTC)

        class FakeSource:
            async def get_bars(self, symbols, start, end, *, now):
                return {
                    symbol: [
                        bar for bar in bars
                        if bar.symbol == symbol and start <= bar.start and bar.end <= now
                    ]
                    for symbol in symbols
                }

        class ClosedBroker:
            syncs = 0

            def sync(self):
                self.syncs += 1
                return {"market_open": False}

            def invalidate_pending(self, active_ids):
                raise AssertionError("Closed-market sync should finish normally")

        async def run():
            with Journal(":memory:") as journal:
                result = await scan(
                    FakeSource(), ["SPY"], AppConfig(), journal, clock=lambda: now
                )
                assert len(result.new_signals) == 1
            broker = ClosedBroker()
            with Journal(":memory:") as journal:
                await run_paper(
                    broker, ["SPY"], AppConfig(), journal, once=True,
                    emit=lambda _: None, clock=lambda: now
                )
            assert broker.syncs == 1

        asyncio.run(run())
        assert "sweepflow.robinhood" not in sys.modules
        """
    )
