"""Exercise actual rebuild and broker reconciliation together across corrections."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

from test_alpaca_execution import FakeAlpaca
from test_monitor import FakeSource, scenario

from sweepflow.alpaca_execution import AlpacaPaperBroker
from sweepflow.paper import run_paper
from sweepflow.storage import Journal


def test_paper_workflow_applies_and_reverts_revisions_without_duplicate_orders():
    async def check():
        bars, config, session = scenario()
        now = session.open + timedelta(minutes=30, seconds=30)
        volume_only = [replace(bar, volume=bar.volume + 1) for bar in bars]
        repriced = list(volume_only)
        repriced[-1] = replace(repriced[-1], low=Decimal("100.6"))
        versions = [bars, volume_only, repriced, bars]
        stage = 0
        done = asyncio.Event()
        outcomes = []
        client = FakeAlpaca()
        client.clock["next_close"] = session.close.isoformat()

        class Source(FakeSource):
            def __init__(self):
                super().__init__(versions[stage])

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

        class TimedBroker(AlpacaPaperBroker):
            def sync(self, at=None):
                return super().sync(at or now)

            def submit(self, signal, at=None):
                return super().submit(signal, at or now)

            def invalidate_pending(self, active_ids, at=None):
                return super().invalidate_pending(active_ids, at or now)

        def emit(event):
            nonlocal stage
            if event.get("event") != "paper_scan":
                return
            outcomes.append(
                {
                    "submissions": len(client.submitted),
                    "active": [
                        (order["client_order_id"], order["limit_price"])
                        for order in client.get_open_orders()
                    ],
                    "scan": event["scan"],
                }
            )
            stage += 1
            if stage == len(versions):
                done.set()

        with Journal(":memory:") as journal:
            broker = TimedBroker(client, config, journal)
            task = asyncio.create_task(
                run_paper(
                    broker,
                    ["AAPL"],
                    config,
                    journal,
                    source_factory=Source,
                    emit=emit,
                    clock=lambda: now,
                    poll_seconds=0.001,
                    scan_seconds=0.001,
                )
            )
            try:
                await asyncio.wait_for(done.wait(), timeout=5)
                assert [item["submissions"] for item in outcomes] == [1, 1, 2, 3]
                assert [len(item["active"]) for item in outcomes] == [1, 1, 1, 1]
                assert [Decimal(item["active"][0][1]) for item in outcomes] == [
                    Decimal("100.50"),
                    Decimal("100.50"),
                    Decimal("100.60"),
                    Decimal("100.50"),
                ]
                assert outcomes[0]["active"][0][0] != outcomes[-1]["active"][0][0]
                assert all(item["scan"].quarantined == () for item in outcomes)
                assert outcomes[1]["scan"].new_signals == ()
                assert outcomes[-1]["scan"].new_signals == ()
                assert len({order["client_order_id"] for order in client.submitted}) == 3
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(check())
