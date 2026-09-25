# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2Book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `nLevels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

## Modified local implementation

This branch is pinned to upstream commit `8b4f237904f683aca2dba21a07d87e831ead2a97`.
It adds controlled in-process recovery, safe trade reconstruction, fixed market filtering,
configurable data/snapshot paths, optional integrity checkpoints, and guarded streamed
block-info support. Full snapshots are requested at startup/recovery; scheduled full
snapshots are **disabled by default**.

See [DEPLOYMENT.md](DEPLOYMENT.md) for architecture, build/test commands, Docker/Compose,
node flags, LAN API behavior, migration, actual-server checks, and remaining limitations.
Streamed mode remains experimental until the node's output continuity is verified.

See [INVESTIGATION.md](INVESTIGATION.md) for the September 25 LAN measurements,
wallet API limitations, stage timing instrumentation, and host diagnostics commands.
`/version`, `/capabilities`, and `/diagnostics` describe the running build and its
bounded timing samples. [WebSocket Info posts](WS_INFO.md) support bounded read-only
queries such as `openOrders` and local `l2Book`. Wallet subscriptions remain unimplemented.

L2 aggregation follows active market/rounding subscriptions while all configured
books continue reconstructing and validating. See [PERFORMANCE_FOLLOWUP.md](PERFORMANCE_FOLLOWUP.md)
for parity tests, the bounded opt-in node profiler, and the unsent upstream report.

```bash
cargo fmt --check
cargo test --locked
cargo build --locked --release --bin websocket_server
python3 scripts/mock_e2e.py
python3 scripts/mock_e2e.py --stream
```

The mock tests are entirely local and never contact your Hyperliquid node.

See [LOCAL_WALLETS.md](LOCAL_WALLETS.md) for opt-in fully local `userFills`,
`orderUpdates`, reconciled `openOrders`, complete account subscriptions and local
`orderStatus` lookups, bounded durable history,
gap handling, performance limits, and deployment. No public wallet relay is used.
