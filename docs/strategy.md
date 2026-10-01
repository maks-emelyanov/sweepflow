# Strategy and replay

## Strategy rules

Each symbol has independent state. Only a complete immediately preceding **regular exchange session** supplies PDH/PDL. The XNYS calendar handles weekends, holidays, daylight saving, and early closes. Data before/after the regular session is not used.

1. A wick strictly beyond PDL starts a long candidate; strictly beyond PDH starts a short candidate. A bar breaking both levels is skipped.
2. Freeze the latest confirmed swing high/low that existed **before** the sweep. Pivots are strict against the configured left candles and non-strict against right candles. A pivot confirmed by the sweep candle itself is ineligible.
3. A subsequent candle must **close** through that level within the next three five-minute candles, including the third. The sweep candle cannot also be its own BOS.
4. BOS must be the middle candle B of A/B/C. The immediately following C must close with `C.low > A.high` for a long, or `C.high < A.low` for a short.
5. Choose the first-touch edge, midpoint, or deep edge. Set the stop beyond the complete sweep-through-C extreme plus the tick buffer. Target the opposite prior-session extreme. Apply the minimum 2.5R filter **before** submitting a paper limit entry.
6. Invalidate on a missed BOS deadline, failed FVG, opposite target touch before entry, expired setup window, or missing data. Pending signals also invalidate on a stop touch or close through the far FVG edge. An existing position prevents new setups.

Setup windows use the **completed candle's timestamp**, with an exclusive end. At 10:30 a new setup cannot be confirmed, and unfilled entries expire. An existing position retains its exits past the setup window. Replay flattens remaining positions at the regular-session close, using the final minute's close. One setup attempt per symbol per session is the default; even a rejected single-direction sweep consumes an attempt. Ambiguous dual sweeps do not.

Entries round to a configured price tick in the favorable direction, stops away from price, and targets toward entry. The engine recomputes reward/risk after rounding. The tick size is configurable rather than inferred from Robinhood.

## Replay execution and model limits

Paper execution uses whole shares and the minimum of fixed account risk, remaining daily risk, symbol/portfolio exposure caps, and available buying power. Pending orders reserve slots, notional, and risk. The daily breaker includes marked unrealized P&L and, when triggered, cancels entries and flattens positions at synchronized minute-close marks. It remains active until the next session.

The fill model is deliberately conservative:

- New orders cannot fill on their FVG-confirmation candle; the first eligible execution minute starts at or after confirmation.
- Entry and stop touched in one minute means a fill and a stop loss. Existing positions touching both stop and target exit at the stop. Stop gaps fill at the worse opening price.
- Entry limits receive no favorable gap improvement. Ambiguous target touches on the entry minute receive no profit credit unless the opening price already filled the entry.
- A target reached before entry cancels the pending order when its ordering is known. Expiry is checked before a subsequent minute can fill.

Minute OHLC still cannot establish every intrabar path. Fees, spread/queue effects, market impact, partial fills, borrowing, settlement constraints, and extra slippage are not modeled. Replay results are research outputs, not evidence of live profitability.

For CSV requirements and commands, see [Usage and data](usage.md). Actual Alpaca paper execution has separate broker semantics described in [Paper trading](paper-trading.md).
