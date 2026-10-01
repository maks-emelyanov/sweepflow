"""Optional market-data integration loading and core execution capability checks."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sweepflow.robinhood import RobinhoodDataSource


class UnsupportedExecutionError(RuntimeError):
    """The broker cannot provide the strategy's required execution semantics."""


def require_live_execution() -> None:
    raise UnsupportedExecutionError(
        "Live execution is unavailable: the strategy requires an atomic entry/stop/target "
        "bracket and short-equity support that this Robinhood integration cannot provide. "
        "Use shadow scanning, Alpaca paper trading, or offline paper replay."
    )


def load_robinhood_data_source() -> type[RobinhoodDataSource]:
    """Load the optional wrapper only when Robinhood data is requested."""
    try:
        from sweepflow.robinhood import RobinhoodDataSource
    except ModuleNotFoundError as exc:
        if exc.name not in {"robinhood_mcp", "mcp_types"}:
            raise
        raise ValueError(
            "Robinhood market data requires the optional robinhood-mcp-wrapper integration. "
            "Install your wrapper checkout with `uv pip install -e /path/to/robinhood` "
            "and configure it as described in README.md."
        ) from exc
    return RobinhoodDataSource


def create_robinhood_data_source() -> RobinhoodDataSource:
    """Construct a source lazily so paper reconciliation can run without the wrapper."""
    return load_robinhood_data_source()()
