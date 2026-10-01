# Security

## Reporting a vulnerability

Use GitHub's [Report a vulnerability](https://github.com/maks-emelyanov/sweepflow/security/advisories/new) to submit a private report. Include the affected version or commit, a sanitized reproduction, impact, and any suggested fix. Do not publish exploit details or credentials in a public issue.

This is an alpha project. Reports against the current development version are welcome; no response-time or supported-release guarantee is provided.

## Credentials and account data

Copy [.env.example](.env.example) to a local `.env` for Alpaca paper credentials. SweepFlow sends credentials only to `https://paper-api.alpaca.markets` and rejects live endpoints and redirects. Robinhood OAuth credentials are managed by the separate wrapper's per-user credential store.

The `.gitignore` excludes environment overrides, local credential files, data, SQLite journals, logs, and local tool state. Do not force-add these files. Sanitize account IDs, orders, and balances before attaching logs or audit output to an issue. If a credential is exposed, revoke or rotate it at the provider; deleting the file from the latest commit does not remove it from Git history.

Use a dedicated Alpaca paper account for this strategy. The paper journal records order ownership and submission intents; preserve it across restarts. The [paper trading guide](docs/paper-trading.md) explains reconciliation and process-outage behavior.
