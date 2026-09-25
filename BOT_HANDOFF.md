# Bot integration handoff — wallet contract 2

This handoff is **local compatibility-ready**, not a production endpoint switch.
The implementation was exercised against synthetic localhost node files and HTTP;
no orders were submitted and the production node/container was not changed.
The bot agent independently reviewed the emitted transcript with its offline checker.

## Implemented surface

* Complete `allDexsClearinghouseState` and `spotState` subscriptions, in the captured
  public envelope. Decimal strings/integer times and token IDs remain unchanged.
* Shared bounded account sampling (default 1 second), dynamic local DEX discovery,
  complete-coverage validation, failed-venue suppression, and actual sampling provenance.
* Local `orderStatus` Info posts by numeric OID or hexadecimal cloid. Retained full
  node classification fields/status timestamps are preserved. Missing evidence is
  `LOCAL_HISTORY_UNAVAILABLE` in a correlated post error; it is never inferred unknown.
* 128 wallet subscriptions per connection and 256 shared wallet/DEX order caches.
  The synthetic test exercises 15 subscriptions per wallet across three wallets
  and eleven DEXes, without the previous eight-subscription/32-cache failures.
* Existing durable raw fill/order history, replay cursors, aggregation, and book
  recovery remain. Wallet indexing is independent of the market-book allowlist.

All source addresses and the follower address must appear explicitly in the
container's `WS_WALLETS` list. Rebuilding an image does not change an existing
container's environment. See LOCAL_WALLETS.md and scripts/deploy_local_wallets.sh.

## Consumer contract

Use `(sessionStartedAt, generation)` for the producer's stream epoch; the generation
counter alone can repeat after a process restart. Keep the bot connection generation
separately. Ignore results from previous connections/subscription tokens.

An initial `resetRequired:true` is a per-subscription baseline boundary. Collect the
required fresh baseline for that scope; do not erase unrelated initial baseline
components repeatedly. A global producer epoch change invalidates the wallet's
incremental state. `gaps` is a cumulative persisted history-gap count, not a boolean
that becomes zero after recovery. Resuming after a newly observed gap requires the
bot's explicit authoritative reconciliation. `historyComplete:false` describes the
bounded observation history; it is not a substitute for readiness/freshness checks.

Account messages have an immediately preceding `walletStatus` companion. It carries
scope, subscription, producer epoch, subscription generation, host sampling start/end,
and upstream timestamps. Enforce the <=1000 ms sample span and <=5000 ms sample age.
Perp upstream times are the actual venue timestamps. Spot `observedNodeTime` comes
from a separate exchangeStatus request: **it is not an exact spot snapshot time or a
fill replay watermark**. `atomic:false` is intentional. These samples cannot by
themselves establish a causally aligned spot inventory/fill baseline.

Emitted `openOrders` snapshots also carry a preceding companion with the original
host query start/end and `sampleTimeSource:"hostQueryWindow"`. Cached delivery does
not renew that time. There is no invented exchange timestamp for openOrders. The
steady reconciliation interval remains 30 seconds plus event-triggered refreshes;
new cache requests reuse a successful response for at most five seconds. A bot's
stricter metadata lease can reject an older cached observation.

An Info result is an accelerator only. Require the same wallet, producer/connection
generation, OID/cloid, complete classification fields, exact source revision and
status timestamp, and the bot's request-age bound. Later fills or source revisions
invalidate an earlier lookup. Do not classify an entry from a stale open record.
A local error must preserve uncertainty and activate a deliberate public fallback;
never release reservations, infer cancellation, or resubmit from unavailable data.

## First bot implementation

Keep the existing public source/follower sockets, account/fill baselines, signing,
order execution and reconciliation authoritative. Add local orderStatus/metadata
acceleration behind a feature flag, default off. Do not replace entire sockets:
`bbo`, `allMids`, `activeAssetData` subscriptions and unconfigured/spot market books
are not supplied by this WS implementation. Status/heartbeat messages must not
renew account freshness or source-entry evidence.

Local account streams can first run in observation mode. Promoting them to inventory
or margin authority requires separately tested non-fill reconciliation and causal
snapshot/fill recovery. Good transport tests do not resolve that consumer issue.

## Evidence and deployment gates

Run from this repository:

```sh
cargo fmt --check
cargo test --locked
cargo build --locked --release --bin websocket_server
python3 scripts/mock_bot_compat_e2e.py --out /tmp/ws-bot-handoff.jsonl
python3 scripts/mock_wallet_e2e.py
python3 scripts/mock_wallet_e2e.py --stream
python3 scripts/mock_e2e.py
python3 scripts/mock_e2e.py --stream
```

The compatibility test writes synthetic JSONL rows with direction, message,
received_ms, producer generation, connection identity and scenario, plus a manifest.
It covers full baselines, numeric/cloid open/canceled/filled lookups, unavailable
history, later-fill invalidation and partial/stale/slow account suppression. No real
account balances or captured private bot source are copied into this repository.

The bot-side independent checker lives in its own workspace:
`reports/node_checks/ws_handoff_acceptance.py`; its review is in
`reports/node_checks/ws_agent_coordination.md`. The captured public compatibility
packet remains in that workspace. Check TEST_RESULTS.md for local results.

Before enabling live local routing: manually deploy the pinned revision with the
complete allowlist, inspect version/capabilities/diagnostics, run a paired passive
public/local observation containing actual naturally occurring fills, compare
missing/conflicting/duplicate/reordered events and latency tails, and measure host
CPU/disk/HTTP load. No production latency gain or full public API parity is claimed.
