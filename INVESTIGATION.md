# LAN feed investigation — September 25, 2026 UTC

Follow-up: [instrumented five-minute capture analysis](CAPTURE_ANALYSIS_20260925.md)
contains host evidence, measured stage costs, and a correction to process discovery
in the host collector. That correction does not require a WS container rebuild.

Protocol follow-up: [WS_INFO.md](WS_INFO.md) describes newly implemented read-only
WebSocket Info posts. The unsupported-post findings below describe the earlier
deployed version. Wallet subscriptions remain unsupported.

## Findings and evidence

The service at `NODE_LAN_IP:8000` has the behavior of this fork's market-data-only
server. It reports the fork's Ready status and rejects wallet subscriptions and
WebSocket Info posts. The previously supplied Dockerfile pinned `9abbb78`, but the
running binary has no version endpoint: `/version`, `/capabilities`, and `/diagnostics`
returned 404. **The exact deployed commit cannot be established from these HTTP/WS
responses.** Check the deployed Dockerfile/build record or image provenance; the
new instrumentation build supplies `/version` for subsequent deployments.

These are unsupported methods, not a subscription configuration error. Current
`ClientMessage` and `Subscription` enums implement subscribe/unsubscribe for
`l2Book`, `trades`, and `l4Book`. A Ready greeting describes the reconstructed book;
it does not promise the full public Hyperliquid WebSocket API.

Read-only live checks made from the development machine:

* Node HTTP `openOrders` for the supplied wallet returned HTTP 200 and six records.
  Order contents are deliberately omitted from this report.
* Node HTTP `userFills` returned HTTP 422 (request-type deserialization failure).
* The wallet subscriptions and WS Info post were rejected as reported.
* `/health` was Ready, generation 0, with zero reported resyncs/validation failures.

The user's earlier 01:08:28–01:13:29 test reported 2,770 comparable matching books,
no disconnect, book arrival delays of 69/296 ms median/p95, trade delays of 77/246 ms,
and occasional books over one second old. Those historical tail events cannot be
attributed to a particular stage from the existing logs.

A separate 60-second BTC probe ran at **01:36:05–01:37:05 UTC**:

| Measurement | Median | p95 | Maximum |
|---|---:|---:|---:|
| Matching book arrival, local minus public (110 matches) | 43.4 ms | 211.5 ms | 503.3 ms |
| Matching trade arrival, local minus public (150 matches) | 34.7 ms | 109.4 ms | 556.1 ms |
| Local book age at this observer (844 messages) | 362.6 ms | 562.5 ms | 865.8 ms |
| Public book age at this observer (111 messages) | 314.4 ms | 447.5 ms | 643.4 ms |
| Local HTTP RTT | 14.7 ms | 40.9 ms | 50.7 ms |
| Public HTTP RTT | 178.2 ms | 218.7 ms | 249.2 ms |

There were zero comparable book/trade mismatches, zero ambiguous book timestamps,
no transport errors, and no local book age above one second in this shorter window.
Public subscription requested `fast:true`; local requested `nLevels:5`. Both books
were normalized to five levels per side, comparing price, size, and order count at
equal timestamps. Trades were matched by `(time, coin, tid)` and checked for price,
size, and side. Different publication frequencies and unmatched messages are not
classified as mismatches. This sample is not proof of correctness for every market.

Matched WS arrival differences use the same observer's monotonic clock. Absolute
book age requires a synchronized observer clock. HTTP RTT includes fresh connection
setup/TLS. Concurrent HTTP public-minus-local chain timestamps had median 277.5 ms,
p95 597 ms, and max 812 ms, but different request latencies mean these are **not
simultaneous state samples or a direct node-ingestion measurement**. Concurrent
local Info time minus reconstructed health time was median 0 ms and max 137 ms in
60 polls: no sustained sampled reconstruction backlog, but spikes between polls
remain possible. No node/container CPU, disk, cgroup, or packet-capture evidence was
available from the remotely exposed endpoints.

## Bot endpoints and wallet support

Current local market feed: `ws://NODE_LAN_IP:8000/ws`:

```json
{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC","nLevels":5}}
```

Current local wallet state: **HTTP POST** `http://NODE_LAN_IP:3001/info`:

```json
{"type":"openOrders","user":"0x0000000000000000000000000000000000000001"}
```

For a builder DEX, include its `dex` name (for example `"dex":"xyz"`) and verify
that query on your node. Do not infer that a default-DEX query covers all venues.
Polling open orders is state reconciliation, not an order/fill event stream.

For standard wallet streams today, use `wss://api.hyperliquid.xyz/ws`:

```json
{"method":"subscribe","subscription":{"type":"orderUpdates","user":"0x0000000000000000000000000000000000000001"}}
{"method":"subscribe","subscription":{"type":"userFills","user":"0x0000000000000000000000000000000000000001","aggregateByTime":false}}
{"method":"subscribe","subscription":{"type":"openOrders","user":"0x0000000000000000000000000000000000000001","dex":""}}
```

Public WS Info schema:

```json
{"method":"post","id":1,"request":{"type":"info","payload":{"type":"openOrders","user":"0x0000000000000000000000000000000000000001"}}}
```

Handle responses by channel; the `post` response contains `data.id` and
`data.response`. `orderUpdates` returns order/status records; `userFills` distinguishes
the initial snapshot from subsequent fills; `openOrders` carries user, DEX, and orders.
See the official [subscription schemas](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
and [WS post schema](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests).

The node's `--write-fills` and `--write-order-statuses` outputs can supply a future
local wallet dispatcher. This is a real protocol extension, not an environment
variable. Route raw records by wallet **before market filtering**, preserve original
size and optional fill fields, track its own continuity and deduplication, and
reconcile open orders through Info. Trigger orders, spot, and excluded markets make
the filtered L4 book unsuitable as a wallet's complete open-order inventory. Historical
fill snapshots and recovery after a gap need an explicit replay/history strategy;
the observed local Info endpoint cannot supply `userFills`. Never fabricate an empty
complete history snapshot. Keep wallet continuity independent of book resyncs.
The subsequent [Info-only WS post adapter](WS_INFO.md) adds bounded requests,
timeouts, and the public response envelope. Wallet event streaming remains unimplemented.

## Changes prepared locally

* Added `/version`, `/capabilities`, and `/diagnostics`, without modifying book/trade
  wire payloads. Unsupported wallet/post requests now explain the limitation.
* Added bounded stage measurements (last 512 samples per fixed metric, lifetime
  sample counts). Durations use monotonic time, are reported in microseconds, and
  do not generate per-event info logs. Percentile windows differ by stage rate;
  they are not five-minute histograms. Scrape periodically to retain spike evidence.
* Added optional sampled timestamp traces using `RUST_LOG=info,latency=debug`.
  One block height in 100 is eligible; it may generate multiple lines across
  messages/clients. Trace fields include block, node-local, file-read start, apply,
  publish, and socket-send begin/end. Fill traces have no book-apply stage.
* Replaced replay budget recount serialization with retained input-length accounting.
  Recounts can no longer serialize all queued events merely to measure their size.
  Original bytes are conservative after filtering; this is not process RSS.
* L2 depth truncation clones only requested levels rather than the complete vector.
* Replaced misleading `replay_bytes` logging with `budget_counter_bytes`,
  `retained_input_bytes`, and queued block counts. The old counter's sawtooth did
  not establish actual replay backlog or a memory leak.
* Added reproducible equal-depth LAN/public probe and read-only Linux host collector.

No running node or container was modified or restarted. These changes therefore
cannot explain or have improved the measured live latency yet. Instrumentation has
bounded memory and fixed metric names, but its production CPU overhead still needs
measurement. Validation, recovery gating, subscription retention, and the default
disabled integrity-snapshot interval remain intact.

## Reading the stage measurements

| Measurements from `/diagnostics` | Interpretation / next check |
|---|---|
| `*_block_to_node_local_us` | Block timestamp to the node's output `local_time`; a proxy for upstream processing, not a measured gossip receive timestamp. |
| `*_node_local_to_read_us` | Node-local timestamp to file-read start; may include output buffering, scheduling, and reader delay. Signed and clock-sensitive. |
| `*_file_read_us`, `*_unread_bytes` | Read duration and bytes not consumed at the last read; correlate with disk/IO pressure and reader throughput. |
| `*_json_parse_us` | Per-line JSON decoding, before market filtering. |
| `book_stream_arrival_skew_us`, `book_replay_wait_us` | Arrival skew between status/diff streams, and wait after the later recorded parse before applying. Stream fragments retain the first fragment's trace; completion-watermark waiting is included. |
| `book_apply_us`, `l2_aggregate_us` | Reconstruction/validation and L2 aggregation costs. |
| `listener_tick_lateness_us`, `listener_lock_wait_us`, `listener_lock_hold_us` | Listener scheduling/processing delays and contention. Tick lateness is not an isolated event-loop benchmark. |
| `book_read_to_publish_us`, `fills_read_to_publish_us` | End-to-end reader/reconstruction/publication duration. |
| `ws_dispatch_queue_us`, `client_lagged_messages` | Broadcast dispatch delay and dropped internal broadcast messages. |
| `trade_reconstruct_us`, `ws_serialize_us`, `ws_socket_send_us` | Per-client trade reconstruction, serialization, and awaited socket write. |
| `read_to_socket_send_complete_us`, `event_age_at_send_us` | Total server path since reading, and block age when starting a send. |

These distributions are diagnostic, not additive percentiles. Socket write completion
does not establish client receipt or isolate kernel/NIC/LAN/proxy delays. Actual
network ingestion inside hl-node cannot be instrumented by this Rust service.
The trace uses the node record's existing timestamps; negative wall-clock differences
must not be clamped away. Reads can overlap newly written records.

## Verification and next decisions

On the bot PC, capture another five minutes with the same tool/market before and
after deploying instrumentation:

```sh
python3 scripts/probe_latency.py --coin BTC --seconds 300 --out /tmp/hl-feed-probe.json
```

On the Linux node host, from the updated repository, run this read-only collector
while that probe runs (Docker/proc permissions are needed for full coverage):

```sh
python3 scripts/collect_host_diagnostics.py --seconds 300
```

Add `--node-container YOUR_NODE_CONTAINER` to collect node process/cgroup data as
well as WS data. It reports its output directory under `/tmp`. It collects available
vmstat/iostat/pidstat, CPU/IO/memory pressure, throttling counters, process IO, listener
ownership, clock sync, container restart metadata, HTTP diagnostics, and bounded WS
logs. Missing utilities are reported; it does not install packages, request a
snapshot, or change containers. Review logs before sharing. It works against the old
server too, recording unavailable `/diagnostics` rather than treating 404 as a crash.
The Linux collector has been syntax-checked locally, not exercised on this host.

After building/deploying the instrumented version, verify:

```sh
curl -s http://127.0.0.1:8000/version
curl -s http://127.0.0.1:8000/capabilities
curl -s http://127.0.0.1:8000/diagnostics
```

For repository Docker builds, embed the revision:

```sh
docker build --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" -t hyperliquid-ws:low-latency .
```

For the earlier standalone Dockerfile that clones GitHub, pin the reviewed new commit
in its `git checkout`; the native build script can read that checkout's Git metadata.
Building an image does not replace the running container. Preserve existing mounts,
host networking, market list, and snapshot paths when choosing to replace it.

Use evidence to choose the next change:

1. If node-local-to-read dominates, verify actual node flags, especially
   `--disable-output-file-buffering`, and correlate with IO/scheduler pressure.
   Preserve `--batch-by-block` while testing instrumentation. Stream mode requires
   switching both node output and reader configuration and validating empty-block
   behavior; it deliberately waits for the next block watermark to avoid incomplete
   books and is not automatically lower latency. See [node output flags](https://github.com/hyperliquid-dex/node#flags).
2. If apply/aggregate or tick/lock times spike, identify CPU throttling and expensive
   subscribed aggregation variants. Moving synchronous parse/reconstruction work off
   Tokio workers or computing only requested variants is a subsequent targeted change.
3. If socket dispatch/write dominates, inspect slow clients and TCP queues. The
   supplied Docker deployment uses host networking and no reverse proxy, but deployed
   ownership/configuration must be verified; the endpoint alone cannot prove this.
4. If books are already old at node output while reconstruction remains current,
   changing WebSocket serialization cannot recover the upstream delay.

Retain the bot's one-second age rejection and synchronized clocks. Ready means the
book satisfies the server's validation/staleness settings, not that every received
message satisfies a one-second bot threshold. Do not respond to every brief age
excursion with another full-book snapshot; that could recreate the original node load.
