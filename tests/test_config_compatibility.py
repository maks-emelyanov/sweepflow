from dataclasses import replace
from decimal import Decimal

import pytest

from sweepflow.config import AppConfig
from sweepflow.monitor import bind_monitor
from sweepflow.storage import Journal

# Persisted identity of the original default Alpaca paper configuration.
PAPER_BINDING = "b4fbbb6e13a9ca46527f47e00787f0696349ae0496e91427afb4bd09ce3bdca3"


def test_paper_reopens_existing_binding_and_preserves_owned_orders(tmp_path):
    path = tmp_path / "paper.sqlite"
    state = {"intents": {"signal-id": {"client_order_id": "liq-entry-existing"}}}
    with Journal(path) as journal:
        journal.connection.execute("INSERT INTO metadata VALUES ('config', ?)", (PAPER_BINDING,))
        journal.save_alpaca_state("test-account", state)

    config = AppConfig()
    config = replace(
        config,
        symbols=("MSFT",),
        universe="custom",
        execution=replace(config.execution, allow_shorts=True),
    )
    with Journal(path) as journal:
        bind_monitor(journal, config, ["MSFT"], "alpaca-paper")
        assert (
            journal.connection.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[
                0
            ]
            == PAPER_BINDING
        )
        assert journal.get_alpaca_state("test-account") == state
        with pytest.raises(ValueError, match="different Alpaca paper account"):
            journal.bind_alpaca_account("other-account")


def test_paper_changed_strategy_rejects_existing_binding(tmp_path):
    path = tmp_path / "paper.sqlite"
    with Journal(path) as journal:
        journal.connection.execute("INSERT INTO metadata VALUES ('config', ?)", (PAPER_BINDING,))
        journal.connection.commit()
    config = AppConfig()
    changed = replace(config, strategy=replace(config.strategy, minimum_rr=Decimal("3")))
    with Journal(path) as journal:
        with pytest.raises(ValueError, match="different configuration"):
            bind_monitor(journal, changed, ["AAPL"], "alpaca-paper")
        assert (
            journal.connection.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[
                0
            ]
            == PAPER_BINDING
        )
