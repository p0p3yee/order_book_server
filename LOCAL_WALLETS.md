# Fully local wallet streams

This service can expose `userFills`, `orderUpdates`, and `openOrders` through the
existing `/ws` endpoint. It uses only configured node files and the configured
local HTTP Info endpoint. No public API fallback, private key, node binary change,
or replay from genesis is required. Wallet ingestion is disabled until an explicit
allowlist is configured. No historical fills before retained observations are inferred.

## Enable

Use `--wallets 0xADDRESS,0xSECOND_ADDRESS` or `WS_WALLETS` (maximum 16 addresses).
Addresses are normalized to lowercase. Use the actual wallet/subaccount addresses
whose events matter; an agent/signing address does not implicitly include its
parent trading account. Each account must appear in the allowlist.

The node flags already used in this deployment supply the data:

```
--write-fills --write-order-statuses --write-raw-book-diffs
--batch-by-block --disable-output-file-buffering --serve-info
```

Keep these flags for the first comparison. `--stream-with-block-info` is also
supported by the wallet worker when the WS process has its matching flag. Wallet
events are delivered per record, without waiting for a subsequent block. Existing
book reconstruction still waits for complete blocks. Streaming mode cannot prove
that a fragment inside a block was not silently omitted; file errors and regressions
are detected, and sparse heights alone are not treated as proof of a wallet gap.
A stream that does not emit records during inactivity is conservatively marked
stale after the configured stale timeout; this may require node-version-specific
heartbeat support before using streamed mode in production.

Optional settings:

| CLI | Environment | Default |
|---|---|---|
| `--wallet-journal-path` | `WS_WALLET_JOURNAL_PATH` | `NODE_DATA_DIR/ws-wallet-journal.json` |
| `--wallet-poll-interval-ms` | `WS_WALLET_POLL_INTERVAL_MS` | 1000 ms; minimum 250 ms |

The journal path must be writable, persistent, and dedicated to this one WS
process. Do not point it at a node-owned file or share it between running replicas.
The Compose service supplies `/node-data/ws-wallet-journal.json`; set `WS_WALLETS`
in its environment or `.env`. An empty value disables wallet ingestion.

## API

```json
{"method":"subscribe","subscription":{"type":"userFills","user":"0x0000000000000000000000000000000000000001"}}
{"method":"subscribe","subscription":{"type":"orderUpdates","user":"0x0000000000000000000000000000000000000001"}}
{"method":"subscribe","subscription":{"type":"openOrders","user":"0x0000000000000000000000000000000000000001","dex":"xyz"}}
```

For native perpetuals use `dex:""` or omit it. Use `dex:"xyz"` for the xyz DEX.
Unsubscribe with the same subscription and `method:"unsubscribe"`.

- `userFills`: standard `{user,isSnapshot,fills}` data. Initial/reset snapshots
  contain retained local fills, then live individual fills. `aggregateByTime:true`
  is explicitly rejected; false/omitted is supported. Fees and extra node fill
  fields are preserved. A missing counterparty does not suppress a wallet's fill.
  Identity includes wallet, coin, time, tid, oid and side, retaining both sides of
  a self-trade while deduplicating replayed observations. Conflicting duplicates
  raise a gap rather than silently replacing an event.
- `orderUpdates`: standard array of `{order,status,statusTimestamp}`. `origSz`
  comes from the node, never from a guess using remaining `sz`. Missing required
  fields cause an explicit wallet gap. This is a live stream, not an initial list
  of open orders. Use `openOrders` for reconciliation. Only one orderUpdates wallet
  per WS connection is allowed because the official response has no user field.
- `openOrders`: `{user,dex,orders}` snapshots queried from local
  `frontendOpenOrders`, sent initially and on changes. These are **polled
  authoritative snapshots**, not per-event public-server-equivalent updates.
  The default interval is one second plus query/scheduling time. Intermediate
  open/cancel transitions can be absent; use `orderUpdates` for those. Polls are
  shared per wallet/DEX across clients, single-flight, with bounded HTTP size,
  concurrency and timeouts. No older file events are merged onto a newer HTTP
  result without a common authoritative block-height boundary.

Wallet events include all markets (including spot and HIP-3) for allowed wallets,
independent of `--markets`, which continues to control reconstructed books/trades.
Eight wallet subscriptions per connection and 32 distinct openOrders wallet/DEX
cache keys per process are supported. The cache-key cap lasts until process restart.

## Required gap and freshness handling

A separate `walletStatus` channel accompanies wallet subscriptions:

```json
{"channel":"walletStatus","data":{"source":"localNode","user":"0x...","state":"Ready","generation":0,"historyComplete":false,"resetRequired":true}}
```

The real message also carries its subscription, reason, retained sequence bounds,
last observed source heights/times, gap count and journal error. `Ready` means both
wallet input streams are progressing within the configured stale timeout. It does
not promise subsecond freshness or full historical coverage. Your existing book
`status` channel remains separate.

Bot behavior:

1. On `Stale`, stop treating the affected wallet stream/state as current. On
   `scope:"openOrders"`, invalidate that subscription's last snapshot until a
   successful authoritative refresh. Other wallet streams may remain live.
2. On a generation change or `resetRequired:true`, invalidate incremental wallet
   state. A userFills reset snapshot can replay already observed fills; deduplicate
   using wallet/coin/time/tid/oid/side. Refresh current open orders. Missing fills
   from a real input gap cannot be recreated from current open orders.
3. Treat `historyComplete:false` literally. An empty fills snapshot means no
   retained observations, not proof of no previous trades. New subscriptions,
   worker overflow, lost retained history, process restarts and input discontinuity
   are explicitly distinguished from uninterrupted delivery.
4. Apply the bot's own event-age limits. A stale market book should not trigger a
   full L4 snapshot merely to chase freshness.

A bounded journal preserves up to 2000 combined fill/order events with a 2 MiB
accounted payload/key budget across configured wallets. Actual JSON size includes
encoding overhead (bounded load limit 6 MiB); atomic replacement can temporarily
hold the previous file and its replacement. This is a small local history, not an
archive. A busy wallet can evict another wallet's older events; lagging cursors
receive reset notifications. Snapshots include the available retained fills only.

Changed history is checkpointed approximately once per second with atomic rename
and file sync on the worker. A crash or slow disk can lose uncheckpointed events;
no exactly-once or gapless-restart claim is made. Every restart begins a new live
boundary and reports historical incompleteness. The service starts tailing near
current output and does not scan old node files to backfill downtime. Journal
write failure is exposed as `journalError`; live delivery can continue without
pretending persistence succeeded. A malformed/oversized journal is an explicit
startup error; preserve it for diagnosis before choosing a fresh journal path.

## Performance and isolation

The book listener enqueues raw records before market filtering. With no allowlist
there is no extra wallet parse/journal work. With wallets enabled, a dedicated
thread parses envelopes, skips unselected payloads using borrowed raw JSON, and
stores only selected events. Input is capped at 64 records and 16 MiB; one wallet
event is capped at 64 KiB. The hot enqueue uses atomics and a nonblocking bounded
queue. Overflow marks a wallet gap instead of waiting behind the worker. Journal
I/O runs outside shared state locks and outside the book listener. There is still
additional copying, parsing CPU and bounded journal I/O when enabled; production
cost must be measured with realistic node traffic.

No full-book snapshot is requested by wallet initialization, polling or journal
persistence. Book recovery may conservatively invalidate wallet continuity, but
wallet-only worker errors do not trigger book snapshots or close the WS connection.
Slow Info requests are asynchronous; disconnection cancels connection-owned jobs.
The shared cache prevents duplicate clients multiplying successful polling rates.

`/diagnostics.wallet` reports configured wallet count, retained/queued bytes,
source heights/times and bounded queue-wait/decode/persist/open-orders-query timing samples.
Raw balances/history are not returned by that diagnostics endpoint.

## Build, deployment and verification

Build locally or on the node host from this checkout:

```sh
cargo fmt --check
cargo test --locked
cargo build --locked --release --bin websocket_server
python3 scripts/mock_wallet_e2e.py
python3 scripts/mock_wallet_e2e.py --stream
python3 scripts/mock_e2e.py
python3 scripts/mock_e2e.py --stream
cargo test --locked --release wallet_parser_cost -- --ignored --nocapture
```

For Compose, set `WS_WALLETS` and rebuild with an explicit source revision:

```sh
export WS_WALLETS=0x0000000000000000000000000000000000000001
export SOURCE_REVISION=$(git rev-parse HEAD)
docker compose build websocket
```

Do not start this Compose service while the earlier standalone container occupies
port 8000. Choose one deployment method. `Dockerfile.ws-low-latency` is the
standalone GitHub-cloning alternative; pass `--build-arg SOURCE_REVISION=<full SHA>`.
Build before stopping the current container. When recreating it, preserve the same
network, volume and market flags, and add `-e WS_WALLETS=<addresses>` to `docker run`.
The journal persists on the existing data mount; no node restart is required.

After deployment verify `/version`, `/capabilities`, and `/diagnostics.wallet`.
Subscribe using the examples above. Verify actual wallet order/fill schemas with
real activity before enabling trading decisions. Do not submit real orders merely
to test this service. Existing five-level market comparisons should remain clean.
Run the normal host collector and latency probe during real wallet subscriptions,
then compare book timings and `wallet.metrics` against the wallet-disabled baseline.
Synthetic tests cannot establish mainnet performance or complete official API parity.

References: [official subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions),
[node schemas](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/nodes/l1-data-schemas),
[local Info limits](https://github.com/hyperliquid-dex/node#evm-and-info-servers).


Local parser timing fixture (macOS release build, 100 repetitions of a 345099-byte
batch containing 1000 unselected orders): worker envelope filtering took 38.12 ms
total, about 0.381 ms per batch. The existing typed book decoder in that fixture
took 63.49 ms total. Wallet filtering is **additional** work when enabled, not a
replacement or claimed mainnet speedup. Actual node traffic and selected-event
rates can differ substantially; use the added worker metrics after deployment.

A convenience script, `scripts/deploy_local_wallets.sh`, implements this exact
standalone replacement for the documented host paths. Inspect/download it at the
same full commit you intend to build, then run:

```sh
WS_WALLETS=0xYOUR_TRADING_WALLET bash scripts/deploy_local_wallets.sh FULL_COMMIT_SHA
```

It builds first, stops/removes only `hyperliquid-ws-low-latency`, and starts the
replacement with the existing market/snapshot/network settings. It does not change
`hyperliquid-node`. Replacement disconnects current clients and starts a normal
startup snapshot. This script has been syntax-checked locally, not run against the
production host. It does not automatically roll back a runtime startup failure;
previous image tags remain available. There is no automatic container replacement
merely from pulling the repository.
