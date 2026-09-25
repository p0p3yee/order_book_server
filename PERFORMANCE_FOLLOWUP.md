# Targeted performance follow-up

## Implemented WS optimization

L2 aggregation now follows active market/rounding subscriptions. A BTC raw L2
subscriber no longer causes HYPE/AERO/xyz markets and every rounded variant to be
aggregated. All configured L4 books still reconstruct and validate; this changes
derived L2 work, not the source of truth or recovery rules.

Shared subscriptions are reference-counted across clients. Unsubscribe, disconnect,
and task cancellation remove only that client's demand. Initial and recovery
snapshots compute the specific subscription directly, so an already-published
height does not prevent a newly added subscription from receiving its initial book.
Different depths share an aggregation and retain per-client truncation. Full depth
is still computed for a requested variant; depth-aware aggregation is not claimed.

Rounded variants use the exact previous dependency chain. Intermediate variants
needed for rounding are computed but not published. In particular, mantissa 5
must derive from the default five-significant-figure result, not mantissa 2.

`/diagnostics` now includes `l2_demand.markets` and `l2_demand.variants`.
For one raw BTC subscription, both are 1; after the last L2 client leaves both
are 0. Existing L2 payloads, height/generation gating and L4/trades APIs are unchanged.

Local release-mode timing fixture (macOS, two Rayon workers, five markets,
250 orders per side, 1,000 iterations): reference all-market/all-variant work
57.71 ms total; one requested BTC raw variant 4.39 ms total. This synthetic
comparison is not a production speedup estimate or end-to-end feed benchmark.

## Node investigation

[NODE_PERFORMANCE_REPORT.md](NODE_PERFORMANCE_REPORT.md) is a draft upstream
report linking observed app hashing, memory faults, CPU and event age. It has
not been submitted. The node's actual binary version remains unknown; public
README flags do not establish a supported way to bypass the expensive operations.

The host collector now records executable SHA-256, CPU topology/kernel and bounded
node logs as well as WS logs when `--node-container` is supplied. Fingerprinting
reads the executable and is done before the sample loop. It does not execute or
replace a node binary.

Optional targeted profiling on the node host, using the updated collector:

```sh
sudo python3 scripts/collect_host_diagnostics.py \
  --seconds 180 --node-container hyperliquid-node --profile-node
```

This records node CPU cycles at a requested 49 Hz with frame-pointer call stacks
through the existing `perf` utility, alongside resource/stage samples and node logs.
It neither restarts the node nor requests snapshots nor changes node flags/sysctls.
It does add profiling overhead; compare with the existing unprofiled capture and
do not treat its timings as an untouched baseline. Keep a BTC subscriber active
if WS timing correlation is needed. A three-minute window will often span the
observed roughly 140-second hash interval, but that cadence is not guaranteed.

No packages are installed. If perf is missing, unavailable to the kernel, or
permission-denied, `perf.log` records the failure and ordinary metrics still collect.
The useful outputs are `metadata.json`, `node.log`, `samples.jsonl`, `perf.log`,
`perf-report.txt` and optionally the raw `node.perf.data`. Inspect logs before sharing.
Raw addresses/paths may appear. Frame-pointer stacks and symbols can be incomplete
in the shipped binary; missing symbols are not evidence that a function was absent.
CPU samples do not directly measure off-CPU lock waits or network ingestion.
See [perf record documentation](https://man7.org/linux/man-pages/man1/perf-record.1.html).

The opt-in profiling path is prepared and locally tested with mocked commands, but
has not been executed on the user's Linux host. No profiling result is claimed.

## Verification

```sh
cargo fmt --check
cargo test --locked
cargo build --locked --release --bin websocket_server
python3 -m unittest discover -s scripts -p 'test_collect_host_diagnostics.py'
python3 scripts/mock_e2e.py
python3 scripts/mock_e2e.py --stream
RAYON_NUM_THREADS=2 cargo test --locked --release measure_requested_l2_work -- --ignored --nocapture
```

The parity fixture checks all supported rounding variants, five markets, and
depths 1/5/20/100/full against the unchanged reference aggregation. Multi-client
tests cover shared demand, duplicate updates, unsubscribe and drop. Process tests
cover live subscription replacement in addition to the existing recovery cases.
Deploy only after the normal build process; these commits do not update a running
container. Keep the current node flags and one-second bot age gate.
