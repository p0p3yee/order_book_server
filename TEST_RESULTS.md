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


## Bot account and metadata compatibility — wallet contract 2

* Rust tests: 65 passed, 2 ignored manual fixtures. Formatting, release build,
  shell syntax and whitespace checks passed.
* Added full account-type/freshness validation, cached-sample lease tests, numeric
  OID/cloid indexed-history lookup with later-fill evidence, and complete metadata
  validation that fails closed on gaps or incomplete classification fields.
* `mock_bot_compat_e2e.py` passed with three synthetic wallets and eleven venues:
  15 subscriptions per connection, full baselines, original scalar types, spot
  default acknowledgement, sampling companions, numeric/cloid open/canceled/filled
  lookups, unavailable history, later-fill invalidation and whole-sample suppression
  after venue failures, stale upstream times and overlong non-atomic sampling.
* Both existing wallet process modes passed, including disk-write failure,
  offline replay/rotation, aggregation and continued book delivery. Both book process
  modes passed after updating the expected capability list for the new subscriptions.
* The bot agent independently checked the synthetic wire transcript against its
  acceptance harness and found no local compatibility blocker. The harness lives
  in the bot workspace; it is not copied into this repository. Initial scoped resets,
  normalized spot acknowledgements, producer restart identity and lifetime gap
  counters were explicitly reviewed between agents.
* No real wallet balances/captured bot source were published as fixtures. No live
  node/bot was modified or restarted. No live orders were submitted. A Linux image
  build, deployed-schema verification and paired real-fill observation remain
  production gates, not claims established by the localhost tests.
* See BOT_HANDOFF.md: first bot integration is default-off local metadata
  acceleration with public fallback; non-atomic spot account samples cannot by
  themselves establish a fill-replay baseline.

## Mainnet-sized wallet record recovery regression

Live diagnostics on 362053a showed wallet generation advancing 86 times in about
20 seconds while the gap counter stayed unchanged. A synthetic >1 MiB order
record reproduced 68 epoch changes in three seconds: the per-poll byte limit was
mistaken for evidence of replay backlog even after the last record was consumed.

The worker now checks the consumed cursor against the current file length and
known rotation state at budget boundaries. Unread/prefetched records, partial
records, rotation, and truncation cannot pass this check. Work budgets and stale
checks remain in effect. This avoids artificial recovery boundaries; it does not
promise no resets during real backlog or upstream stalls.

Regression command: `python3 scripts/mock_wallet_e2e.py --large-records`.
Rust tests: 66 passed, 2 ignored; release websocket_server build passed.
The large-record process regression passes after the fix, as do the batch and
streamed wallet process tests and the three-wallet bot compatibility process test.
`cargo fmt --check` and `git diff --check` pass. No production latency benefit is
claimed until the corrected image is manually deployed and observed.

## Separate wallet readiness from continuity

A passive 180-second mainnet capture after 53f1ccb recorded 29 temporary readiness
cycles with unchanged gaps and no persistence errors. The Ready-to-Stale branch
still advanced the continuity epoch for genuine but recoverable read backlog.

Temporary backlog/upstream-age pauses now gate publication without changing the
epoch. Retained delivery cursors resume incrementally when Ready. Actual gaps,
retention-overflow reset handling, and conservative persistence-error invalidation
remain. Consumers must invalidate pending proofs on Stale, independently of epoch.

Validation: 67 Rust tests passed, two ignored; release build, formatting and diff
checks passed. Large-record, batch wallet, streamed wallet, and bot compatibility
process tests passed. The unit regression verifies withholding a pending fill
while Stale, incremental delivery after Ready, and reset on an actual gap. Both
wallet process modes exercise stale upstream recovery on the same connection
without an invented generation/gap. The large-record test now waits for independent
wallet startup rather than incorrectly equating book health with wallet readiness.
Production pause frequency and event delivery still require a post-deploy capture.

## Bounded wallet catch-up and stale book replay

* Old but contiguous book updates are validated and applied while publication is
  gated. They no longer discard an authoritative snapshot solely for timestamp
  age. A Ready-to-Stale transition invalidates queued book frames; a fresh block
  resumes with a reset/snapshot. Actual block gaps and validation errors still
  recover. Batch and streamed process tests assert no L2/L4 frames during stale
  replay and no additional snapshot while it catches up.
* An already-ready wallet tolerates less than 100 ms of read backlog only while
  both sources remain fresh and persistence is healthy. Startup, gaps, persistence
  failure and stale timestamps bypass this grace. Backlog polls sleep at most 1 ms;
  byte/record budgets remain bounded. `wallet.metrics.backlog_age_us` measures it.
* Source age is checked before each batch notification. Unit tests cover the
  99/100 ms boundary and each fail-closed condition; existing process tests cover
  same-connection stale recovery, gaps, persistence failure and incremental replay.

Validation: 69 Rust tests passed, two ignored; release build and formatting passed.
All six process scenarios passed: book batch/stream, wallet large-record/batch/stream,
and bot compatibility. The account-isolation test's 1.2-second spot-refresh window
failed once and passed on unchanged retry; it now allows the one-second sampling
period plus the two-second HTTP work window, retaining strict partial-perp rejection.
These synthetic tests do not establish production latency gains or remove node
output delays. Passive deployment verification remains required.

## Strict order-history gate following backlog grace

The backlog-grace checkpoint must not be deployed without this correction:
publication readiness alone could authorize an open-order answer ahead of pending
fills. A separate `historyCurrent` gate now requires fresh, aligned source tips.
Both hot and indexed SQLite lookups reject known backlog and incomplete streamed
blocks. Tests cover a pending fill within 99 ms of grace, later-fill invalidation,
observed terminal state, mismatched heights, streamed block completion, and the
same checks through SQLite after evicting the hot record. The exact history-gate
error is asserted so an unrelated allowlist failure cannot satisfy the regression.
Rust tests: 71 passed, two ignored; release build passed.
Final process checks passed: bot compatibility and wallet large-record, batch, and
streamed modes. Book batch/stream checks passed with the unchanged book replay
implementation in the preceding checkpoint. Formatting and privacy checks passed.

The strict-history correction also covers retention before commit: lookups search
pending plus retained records by sequence, refuse SQLite fallback with pending
history, and gate the interval while the writer owns pending records. A real SQLite
regression exceeds the hot-cache retention bound, verifies that an evicted pending
fill cannot expose an older open order, verifies the commit-in-progress gate, and
uses the newer evicted pending terminal record. Rust tests: 72 passed, two ignored;
release build passed. Publication readiness does not imply durable history.
