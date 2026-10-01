# Contributing to SweepFlow

Use Python 3.14 or newer and uv. From the repository root:

```bash
uv sync --locked --inexact --group dev
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv build
```

The core and most tests work without broker accounts, credentials, or sibling repositories. To exercise the Robinhood adapter tests, install its independent wrapper as described in [README.md](README.md#optional-integrations), then run the same test command. Those tests use fake clients; they do not call a broker. The adapter test module is skipped when `robinhood_mcp_wrapper` is unavailable; check the pytest summary to confirm it ran when reviewing integration changes.

Before submitting a pull request, describe the behavior that changes and the checks you ran. Add focused regression coverage for changes to strategy rules, data validation, risk sizing, execution, persistence, or restart behavior. Use synthetic fixtures or sanitized reproductions. Keep the documentation and command examples consistent with the CLI.

Pull requests from external contributors require a maintainer to approve GitHub Actions before CI runs.

Strategy changes must preserve causal candle ordering, timezone-aware timestamps, decimal price/risk arithmetic, and complete prior-session validation. Execution changes must retain paper-only endpoints, durable order identity, reconciliation after ambiguous submissions, and ownership checks before canceling or closing exposure. Existing `liquidity:` UUID seeds and `liq-` client order prefixes are persisted compatibility identifiers.

When dependencies change, regenerate `uv.lock` with `uv lock` and verify `uv sync --locked`. Core package metadata must not depend on a developer's local filesystem. The dashboard and Robinhood wrapper remain separately installed integrations until a deliberate distribution strategy is adopted.

Do not commit `.env`, OAuth tokens, API keys, account journals, raw broker responses, or generated runtime data. Report potential vulnerabilities using the process in [SECURITY.md](SECURITY.md).

Contributions are covered by the repository's [MIT license](LICENSE).
