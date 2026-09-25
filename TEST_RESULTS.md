# Local verification — 2026-09-24

Implementation commit: `aa6b933` (based on upstream
`8b4f237904f683aca2dba21a07d87e831ead2a97`).
Environment: macOS development workspace, Rust 1.94.0. No Hyperliquid node was accessed.

| Check | Result |
|---|---|
| `cargo fmt --check` | PASS |
| `cargo test` | PASS — 35 tests, 0 failed |
| `cargo build --release --bin websocket_server` | PASS — native development-machine target |
| `git diff --check` | PASS |
| `docker compose config --quiet` | PASS |
| `python3 -m py_compile scripts/*.py` | PASS |
| `python3 scripts/mock_e2e.py` | PASS |
| `python3 scripts/mock_e2e.py --stream` | PASS |
| Docker image build | NOT VERIFIED — Docker Hub base-image metadata fetch timed out (`DeadlineExceeded`) |
| Actual-node parity, latency, HIP-3 coverage | NOT RUN — intentionally left to deployment canary |

The two process tests started the compiled release executable, an HTTP mock for
`fileSnapshot`, and temporary JSONL files. They retained a single WebSocket connection
through recovery, including L4 replacement snapshots. Each passed an 11-second healthy
period with no additional snapshot request, malformed fill JSON, a skipped book block,
hourly rotation, stale input, snapshot HTTP 503 failures, and resumed updates.

Unit coverage includes valid/incomplete/same-side/interleaved/duplicate/inconsistent
trade groups; snapshot list length/order/market mismatch; missing blocks and replay;
stream fragment accumulation and two-stream watermarks; price/original-size mismatch;
filtering; stale block timestamps; duplicate/regressing batches; bounded replay;
partial lines, startup fragments, file replacement/truncation, numeric hourly discovery,
invalid UTF-8 recovery; and exact decimal parsing. Original linked-list, L2 aggregation,
priority ALO insertion, snapshot deserialization, and subscription tests are retained.
The obsolete notification-only directory-reader test was replaced alongside its
production reader by the new tail tests and actual-process rotation test.

These tests establish the exercised behavior, not mainnet book parity or a measured
latency reduction. Stream fixtures include contiguous empty-block envelopes; the real
node's streamed empty-block behavior remains a deployment qualification requirement.

## Decimal compatibility follow-up

The first real-node startup report failed with `invalid fixed point decimal` after
snapshot generation and JSON reading succeeded. The original diagnostic did not expose
the value, so its precise format/market cannot be determined from that report.

The follow-up supports scientific notation using exact integer conversion, filters
excluded snapshot markets before fixed-point conversion, and adds coin/order/field/value
context to conversion errors. Regression tests cover exact scientific prices/sizes,
exponent limits, overflow, rejection of lossy precision, and exclusion of a spot market
with an unrepresentable price. `cargo test --locked`: 38 passed. Formatting and the
release build passed. Both batch and streamed local process tests also passed again.
Subsequent user logs showed successful startup and continued Ready operation.

## Latency investigation follow-up — September 25 UTC

Read-only LAN/public probes were authorized for this follow-up. See
[INVESTIGATION.md](INVESTIGATION.md) for measurements and sampling limitations;
these do not retroactively change the original local-only test scope above.

* `cargo test --locked`: 40 passed, including bounded metric windows and retained
  fragment byte accounting/clear-on-recovery with no internal fields in serialized batches.
* `cargo fmt --check`, `git diff --check`, Python syntax checks: passed.
* Native `cargo build --locked --release --bin websocket_server`: passed.
* Batch and streamed actual-process mock tests: passed; now also check version,
  capabilities, stage measurements, and continued L2 delivery after rejected WS post.
* Read-only 60-second live BTC probe: 110 comparable five-level books matched,
  150 comparable trades matched, zero transport errors; no book over one second old.
* Instrumented build has not been deployed to the node. No host CPU/disk attribution
  or production instrumentation overhead measurement is claimed.
* Linux host collector: syntax checked only. Docker Linux image build remains unverified.

## Subscription-aware L2 follow-up

* `cargo test --locked`: 42 passed, 1 ignored manual timing test. The latter was
  separately run in release mode and passed (see PERFORMANCE_FOLLOWUP.md).
* Reference-parity fixture: five markets, every supported rounding variant,
  depth 1/5/20/100/full; demanded outputs equal the old all-variant outputs.
* Shared demand reference-count test: duplicate updates, unsubscribe, and Drop cleanup pass.
* Both mock process modes pass recovery and added multi-client subscription replacement.
* Formatting, diff checks, Python syntax checks and native release build pass.
* Four collector tests pass, including mocked opt-in profiling selecting the actual
  hl-node child PID. Actual Linux perf execution and production L2 speedup are unverified.
* No running node/WS configuration changed; no upstream issue submitted.

## WebSocket Info follow-up

* `cargo test --locked`: 45 passed, 1 ignored manual aggregation benchmark.
* Formatting, native release build, Python syntax and diff checks passed.
* Both batch/stream process tests passed Info success, slow HTTP with continued L2,
  timeout, HTTP 503, invalid JSON, response-size limits, per-client overload,
  rejection of fileSnapshot, local L2 Info, and refusal to answer L2 Info while Stale.
* Unit tests cover global concurrency rejection and permit release without contacting
  a node, payload preservation and rejection, and correlated error envelopes.
* The production container was not changed. Wallet subscriptions are not implemented
  by this increment; no public wallet feed is contacted or relayed.

## Fully local wallets follow-up

* `cargo fmt --check`, `git diff --check`, and native release build passed.
* `cargo test --locked`: 55 passed, 2 ignored manual timing fixtures.
* New tests cover individual fills without a counterparty, extra fee preservation,
  market-independent wallet filtering, streamed fragments, self-trade identity,
  duplicate/conflicting fills, order original size/time, missing fields, batch
  gaps, journal retention/cursor resets, queue overflow, journal restart/corruption,
  invalid open-order responses and unsubscribe/in-flight token safety.
* `mock_wallet_e2e.py` passed in batch and stream modes: actual WS/file/HTTP process
  delivery, wallet isolation, reconnect snapshots, slow HTTP with continued L2,
  shared query caching across clients, failed-query recovery, missed blocks/stale
  inputs on the same socket, allowlist/aggregation errors, journal write failures
  with continued book delivery, and retained fills across process restart.
* Existing `mock_e2e.py` passed both modes, preserving book validation, recovery,
  no periodic full snapshot loop, Info behavior and stable socket tests.
* Four host-collector unit tests passed. Python syntax and deployment script shell
  syntax checks passed. The deployment script was not run against a real server.
* Release-mode wallet parser fixture ran separately: 100 x 1000-order unselected
  batches, 345099 bytes each, wallet filtering 38.12 ms total versus existing typed
  book parsing 63.49 ms. Filtering is additional worker work, not a mainnet speedup.
* No Linux Docker image build or production wallet performance/schema validation
  is claimed. The daemon running on the user's node was not modified or restarted.
* History remains bounded/partial with explicit gaps; openOrders is authoritative
  polling, not a per-event replica of the public subscription implementation.


## Durable local wallet history, event refresh and aggregation

* `cargo fmt --check`, `git diff --check`, `cargo test --locked` and the native
  `cargo build --locked --release --bin websocket_server` passed: 60 unit tests,
  two manual performance fixtures ignored by the normal test run.
* SQLite tests cover transactional event/cursor rollback, conflicting retained
  identities, event retention, paginated history, partial records, resume anchors,
  file rotation, truncation/missing files, legacy import and corrupt startup data.
* Aggregation tests cover taker/maker grouping boundaries, exact summed amounts,
  weighted prices, optional fees, scientific decimals, malformed amounts, streamed
  completion/cursor handling and omission of evicted partial boundary groups.
* Both batch and streamed wallet process tests passed with actual local WS sockets:
  event-triggered authoritative openOrders refresh before reconciliation, shared
  query caching, slow/failed HTTP without blocking books, aggregated fills, disk
  write-lock failure/recovery, offline fills across restart/file rotation, and
  localWalletHistory queries. No production node or public wallet API was used.
* Both existing market-book process tests passed: malformed fills, missed blocks,
  stale output, rotation, HTTP failures, validated resync on the same socket/process,
  L4 reset and absence of recurring healthy-state full snapshots.
* Parser fixture: 100 x 345099-byte / 1000-unselected-order batches took 38.73 ms
  on the wallet worker versus 63.13 ms for the existing typed book decoder. This
  measures parser CPU only; SQLite, filesystem replay and real traffic need a
  production comparison. Wallet work is additional CPU/I/O, not a claimed speedup.
* Docker/Compose defaults and deployment-script tuning were updated; shell syntax
  checked. A Linux Docker build and deployment have not been performed here.
* Exact public aggregate metadata/rounding parity remains unverified. Missing or
  pruned node output cannot be recreated locally; history reports that limitation.
