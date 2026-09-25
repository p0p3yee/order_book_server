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
Actual-node confirmation remains pending.
