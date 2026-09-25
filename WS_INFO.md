# WebSocket Info support and wallet limitations

This implementation now accepts read-only `method:"post"` Info requests on the
existing `/ws` endpoint. Opt-in fully local wallet subscriptions are documented
separately in [LOCAL_WALLETS.md](LOCAL_WALLETS.md). Neither feature claims full
public-API compatibility.

## Requests and responses

```json
{"method":"post","id":1,"request":{"type":"info","payload":{"type":"openOrders","user":"0x0000000000000000000000000000000000000001","dex":"xyz"}}}
{"method":"post","id":2,"request":{"type":"info","payload":{"type":"l2Book","coin":"BTC","nLevels":5}}}
{"method":"post","id":3,"request":{"type":"info","payload":{"type":"exchangeStatus"}}}
```

Successful open-orders response shape:

```json
{"channel":"post","data":{"id":1,"response":{"type":"info","payload":{"type":"openOrders","data":[]}}}}
```

`data` contains the actual node response; the empty array above is only an example.
Errors keep the request ID and use `response.type:"error"` with a string payload.
IDs must be unsigned integers. Malformed envelopes without a usable ID receive
the ordinary `error` channel. Responses can arrive out of order; match them by ID.
The envelope follows the [official WS post schema](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests).

`l2Book` is answered from the validated reconstructed book, with the same market
allowlist, rounding, and depth validation as L2 subscriptions. A non-ready book
returns an error, not a suspect snapshot. `orderStatus` and `localWalletHistory`
are served by the local wallet journal. Other allowlisted read-only requests
use the configured `--info-url` directly, without environment HTTP proxies or
redirect following. Unsupported node queries remain errors; no public fallback
is used. `/capabilities` lists the exact allowed request types.

## Resource and connection behavior

* Four pending Info requests per connection; sixteen active operations globally.
* Two-second async timeout; configured HTTP connect timeout 500 ms.
* Request frames limited to 64 KiB; forwarded HTTP responses limited to 2 MiB.
* Pending futures are polled alongside book delivery; slow HTTP requests do not
  await inside the subscription receive handler. They are dropped on disconnect.
* Timeouts, HTTP errors, invalid upstream JSON, and limit violations return errors
  while leaving the WebSocket connection open.
* Signed actions and `fileSnapshot` are rejected. This endpoint cannot trigger
  the expensive node snapshot operation. Recovery remains its internal owner.

Limits apply to the adapter. Cooperative async timeouts cannot preempt synchronous
book computation or an already executing upstream operation. Slow socket writes
remain a per-client transport constraint as before.

## Wallet subscriptions

`orderUpdates`, `userFills`, `openOrders`, `allDexsClearinghouseState`, and
`spotState` subscriptions are available with
`--wallets` / `WS_WALLETS`. See [LOCAL_WALLETS.md](LOCAL_WALLETS.md) for bounded
history, required walletStatus gap handling, event-triggered openOrders, and
periodic authoritative account sampling. Local `orderStatus` posts support retained
full records by numeric OID or cloid; unavailable evidence returns a correlated
`LOCAL_HISTORY_UNAVAILABLE` error, never an invented `unknownOid`.
The Info adapter itself remains request/response; it does not synthesize historical
HTTP `userFills` support that the local node lacks. No public wallet relay is used.

Latency also has two distinct measures: matching-event arrival delay and freshness
of the latest available book. More frequent local updates can improve the latter
without improving the former. Info transport support does not reduce node
hash/checkpoint latency or claim to fix the reported 70/96 ms arrival disadvantage.

## Tests

Unit tests cover query preservation, rejection of actions/snapshots/oversized
payloads, correlated errors, and the global concurrency bound. Both mock file
modes exercise Info success, slow responses with continued L2 delivery, HTTP 503,
timeout, invalid JSON, oversized responses, per-client overload, and local L2 Info.
Unconfigured wallets return explicit allowlist errors without disconnecting.
The separate `mock_wallet_e2e.py` tests wallet delivery and recovery in both file
modes. `mock_bot_compat_e2e.py` covers three wallets across eleven DEXes, account
provenance and local order lookups. These mocks do not substitute for verification on the actual node.
