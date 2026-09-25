# Fully local wallet streams

This service exposes `userFills`, `orderUpdates`, `openOrders`,
`allDexsClearinghouseState`, `spotState`, and local `orderStatus` Info posts through the
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
raw fills and order events are delivered per record, without waiting for a subsequent block. Aggregated fills wait until the next fill-source block confirms completion. Existing
book reconstruction still waits for complete blocks. Streaming mode cannot prove
that a fragment inside a block was not silently omitted; file errors and regressions
are detected, and sparse heights alone are not treated as proof of a wallet gap.
A stream that does not emit records during inactivity is conservatively marked
stale after the configured stale timeout; this may require node-version-specific
heartbeat support before using streamed mode in production.

Optional settings:

| CLI | Environment | Default |
|---|---|---|
| `--wallet-journal-path` | `WS_WALLET_JOURNAL_PATH` | `NODE_DATA_DIR/ws-wallet-journal.sqlite` (legacy `.json` path migrates automatically) |
| `--wallet-poll-interval-ms` | `WS_WALLET_POLL_INTERVAL_MS` | 30000 ms reconciliation; minimum 250 ms |
| `--wallet-account-interval-ms` | `WS_WALLET_ACCOUNT_INTERVAL_MS` | 1000 ms periodic account sampling; range 250–5000 ms |
| `--wallet-event-interval-ms` | `WS_WALLET_EVENT_INTERVAL_MS` | 100 ms minimum interval between event-triggered queries |
| `--wallet-history-events` | `WS_WALLET_HISTORY_EVENTS` | 100000 combined disk events; range 2000–1000000 |
| `--wallet-history-days` | `WS_WALLET_HISTORY_DAYS` | 7 days; range 1–3650 |

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
  combines partial fills as described below; false/omitted preserves raw fills. Fees and extra node fill
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
  `frontendOpenOrders`, sent initially and when the result changes. Relevant fills/order
  events trigger a refresh, coalesced to at most one query per 100 ms per wallet/DEX.
  A 30-second reconciliation timer catches state changes absent from observed events.
  These remain authoritative state snapshots. Intermediate
  open/cancel transitions can be absent; use `orderUpdates` for those. Polls are
  shared per wallet/DEX across clients, single-flight, with bounded HTTP size,
  concurrency and timeouts. No older file events are merged onto a newer HTTP
  result without a common authoritative block-height boundary. Emitted snapshots
  have a walletStatus companion with the original host query timestamps; a cached
  delivery does not renew sample time. Cache reuse on a new request is capped at
  five seconds.

Wallet events include all markets (including spot and HIP-3) for allowed wallets,
independent of `--markets`, which continues to control reconstructed books/trades.
128 wallet subscriptions per connection and 256 distinct openOrders wallet/DEX
cache keys per process are supported. The cache-key cap lasts until process restart.

## Account snapshots and order metadata (wallet contract version 2)

```json
{"method":"subscribe","subscription":{"type":"allDexsClearinghouseState","user":"0xYOUR_ACCOUNT"}}
{"method":"subscribe","subscription":{"type":"spotState","user":"0xYOUR_ACCOUNT"}}
{"method":"post","id":71,"request":{"type":"info","payload":{"type":"orderStatus","user":"0xYOUR_ACCOUNT","oid":123456789}}}
```

`allDexsClearinghouseState` returns `{user,clearinghouseStates:[[dex,state],...]}`.
The native DEX is `""`; the complete DEX catalog comes from local `perpDexs`, cached
for 60 seconds. Each venue is sampled through local `clearinghouseState`. One failed,
stale, or malformed venue suppresses the **whole** sample. The assembled response is
bounded to 2 MiB/64 venues. Financial strings and integer timestamps are unchanged;
empty positions mean an empty complete venue, never a fabricated missing venue.

`spotState` returns `{user,spotState:{balances:[...]}}` from the full local
`spotClearinghouseState` result, without filtering tokens or inventing balances.
Its acknowledgement includes `ignorePortfolioMargin:false`, matching the observed
public acknowledgement when omitted. `true` is explicitly unsupported because the
local Info semantics are not documented; it is not silently ignored.

Account sampling is shared per wallet/subscription, single-flight, with at most four
concurrent account HTTP requests process-wide (inside the existing 16-request Info
limit). Only active subscriptions initiate sampling. One-second periodic sampling
covers transfers, funding and liquidation even without fill/order events. It is
separate from the event-triggered openOrders cache. Additional clients share samples.
For three wallets/11 venues, budgeting approximately 39 account queries/second plus
catalog refresh is a useful upper estimate at 1 Hz; actual scheduling/HTTP time can
reduce cadence. Measure node impact before enabling all accounts in production.

Every newly sampled account message is preceded by `walletStatus` containing its
`scope`, `subscription`, wallet `generation`, `subscriptionGeneration`, server
`sessionStartedAt`, `sampleStartedAt`, `sampleCompletedAt`, `upstreamTimes`, and
`atomic:false`. Sample span must be at most 1000 ms; upstream age must be at most
5000 ms, with 1000 ms future tolerance. These are independent HTTP reads, **not an
atomic multi-venue snapshot**. A cached resend retains its original timestamps.
Unchanged spot balances are re-emitted only after fresh authoritative sampling.

For perps, `upstreamTimes` maps DEX names to the original state's `time`. Spot has
no native snapshot timestamp: `upstreamTimes.observedNodeTime` is a separate local
`exchangeStatus` observation after the balance query. It is **not** a spot fill
replay watermark. Host sampling bounds do not prove a causal snapshot/fill boundary.
A consumer needs explicit reconciliation before using these as inventory baselines.

`orderStatus` supports numeric exchange OIDs (up to signed 64-bit storage range)
and 16-byte hexadecimal cloids. It looks up the most recent retained local order
record, preserves full node metadata and exact `statusTimestamp`, and uses the
standard correlated `post` Info response envelope. It does not manufacture absent
optional node fields. Required classification fields must be present. Basic
`orderUpdates` messages retain their existing public-compatible projection.
Open records become unavailable after a history gap or a later observed fill until
new order evidence exists; the service never guesses remaining size from history.
Historical lookups use indexed SQLite reads; recent metadata uses the bounded hot
cache. Neither requests an L4 snapshot nor queries the public API.

Missing/expired history, disabled wallets, stale ingestion or incomplete metadata
returns a correlated error beginning `LOCAL_HISTORY_UNAVAILABLE:`. This implementation
never emits `unknownOid`: its bounded local history cannot certify a complete negative.
Malformed requests and resource limits can return other correlated errors. Consumers
must preserve uncertainty and route fallback deliberately, never infer rejection.

Enable every source **and** the follower address explicitly in `WS_WALLETS`; adding
a source does not automatically authorize the follower. The provided deployment
script accepts the full comma-separated list. An image update does not change an
existing container's allowlist.

This is an account/metadata compatibility extension, not full public-WS parity.
`bbo`, `allMids`, `activeAssetData` subscriptions and unconfigured/spot market books
remain on the bot's public connection. The local bot adapter must handle walletStatus,
per-channel freshness and explicit fallback before any endpoint switch.

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
   lost retained history, file replay, process restarts and input discontinuity
   are explicitly distinguished from uninterrupted delivery.
4. Apply the bot's own event-age limits. A stale market book should not trigger a
   full L4 snapshot merely to chase freshness.

The hot cache holds at most 2000 combined events and 2 MiB of accounted payload/key
bytes across wallets. The separate SQLite history defaults to 100000 events or seven
days, whichever removes an event first. A busy wallet can evict another wallet's
older events. Hot-cache overflow causes a subscription reset; it does **not** erase
events still retained on disk. Disk retention is an event count/time limit, not a
fixed byte limit: payloads can be up to 64 KiB. SQLite reuses freed pages but does
not automatically shrink its file. Include the database, `-wal`, and `-shm` in
capacity planning; use SQLite's backup API for a consistent live backup.

The worker commits selected events and both input-file positions in one SQLite WAL
transaction approximately once per second (or after 256 selected events), with
FULL synchronization. After restart it resumes those positions and catches up
before marking the wallet feed Ready. This also recovers uncommitted records if
the node files still exist. No genesis replay is needed. Preserve node output for
at least the longest expected WS outage; a pruned/replaced/truncated file produces
an explicit persisted gap and a fresh live boundary. An absent historical input
cannot be reconstructed from an account's current state.

Persistence failure pauses wallet ingestion/publication and retries once per
second. Book processing and existing sockets continue. Recovery then reads retained
input. Live messages can precede the next disk commit; clients must deduplicate
replayed fills after a restart. This is not an exactly-once delivery protocol.

Existing `.json` journal paths automatically use a sibling `.sqlite` file and
import legacy retained events on first initialization. The old JSON is preserved.
Legacy data has no file cursors, so migration records an explicit coverage gap.
Reuse the same persistent volume and path on upgrades. Do not run two instances
against one journal. Storage corruption at startup is an error, not a silent reset.

### Retrieve disk history

This custom, local-only WebSocket Info request returns raw fills and order events:

```json
{"method":"post","id":10,"request":{"type":"info","payload":{"type":"localWalletHistory","user":"0x0000000000000000000000000000000000000001","afterSequence":0,"limit":100}}}
```

The response contains `events` with `sequence`, `channel`, `data`, and `height`;
`nextSequence`, `hasMore`, `oldestRetainedSequence`, and recent `gaps`. Continue
using `afterSequence:nextSequence`. Limits are 1–1000 events and 1 MiB of event
JSON per page. Only configured wallets can be queried. Results include committed
history, which can trail live delivery by the checkpoint interval. Sequence numbers
are global across wallets, so jumps alone do not prove missing wallet events.
`historyComplete:false` still applies to the full account lifetime, especially
before collection, after actual lost input, or beyond retention.

### Aggregated fills

Set `aggregateByTime:true` on a `userFills` subscription. Crossing fills are grouped
by wallet, coin, order ID, side, fee token and execution hash. Resting-order fills
are grouped within a block. Size, fee, closed PnL and optional builder/deployer fees
are summed with exact decimal arithmetic; price is the size-weighted mean rounded
half away from zero to 18 decimal places. First-constituent identifiers, timestamp,
start position and direction are retained. Raw fills remain available separately.

The [official grouping rules](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
do not fully specify metadata tie-breaking or price rounding. These local choices
are explicit; exact public-server field parity has **not** been empirically verified.
Use separate connections for raw and aggregated subscriptions for the same wallet,
since their response channel does not identify the aggregation mode.

Streamed mode waits for the next fill-source height before releasing aggregates;
a quiet source can therefore delay an aggregate. Batch mode emits complete batches
without that extra block wait. If the hot cache evicts part of a block, aggregates
from that boundary block are conservatively omitted; raw constituents may still
be retrieved from disk. Retained snapshots remain partial history, never a claim
of full historical coverage.

## Performance and isolation

Wallets use a dedicated file-reader thread, independent of the book listener. With
no allowlist, no wallet files/database are opened. With wallets enabled, the worker
parses borrowed raw envelopes, skips unselected payloads, and stores only selected
events. Each pass reads at most 128 records or approximately 1 MiB per source
(a single record can be up to 16 MiB), then yields for the configured poll interval.
File discovery runs once per second; rotation can add up to that discovery delay.
Disk writes run outside shared state locks. Wallet recovery does not request full
book snapshots or invalidate an otherwise healthy book.

This adds node-file reads, parsing CPU and SQLite I/O. Measure the production cost;
local mock tests do not establish a production latency improvement. Event-triggered
HTTP work runs asynchronously, uses shared single-flight caches, and only occurs
for active openOrders subscriptions. Additional clients do not multiply successful
query rates. Stale/error queries are retried with the configured minimum interval.

`/diagnostics.wallet` reports source heights/times, coverage start, replay state,
gaps, journal errors, hot-cache size and decode/persist/open-order/account sample timings.
Raw wallet history is not exposed by diagnostics. Existing market-data diagnostics
remain available for before/after comparisons.

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
batch containing 1000 unselected orders): worker envelope filtering took 38.73 ms
total, about 0.387 ms per batch. The existing typed book decoder in that fixture
took 63.13 ms total. Wallet filtering is **additional** work when enabled, not a
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

See [BOT_HANDOFF.md](BOT_HANDOFF.md) for the independently reviewed bot contract,
producer epoch semantics, and staged integration/deployment gates.
