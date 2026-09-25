# Draft upstream report: app-hash work coincides with stale non-validator market output

Prepared locally; not submitted to GitHub or sent to anyone.

## Problem

A mainnet non-validator continues applying blocks, but exported market events
occasionally exceed one second of age. During a September 25, 2026 capture,
block-to-output age reached about 1.20 seconds while app-hash work coincided
with a CPU/minor-page-fault burst. We seek supported ways to reduce this tail
latency without bypassing validation or compromising state correctness.

## Environment

* Linux 6.8.0-111-generic, x86_64; sysstat reports 32 logical CPUs.
* Docker host networking, no CPU quota or memory limit on the node or WS container.
* Node supervised by hl-visor; no restarts or OOMs reported in capture.
* Exact deployed node binary build/version not yet established. The next collector
  records the executable SHA-256; a node image ID alone is not a binary version
  when the supervisor can update the executable.
* Flags:

```text
run-non-validator --write-fills --write-order-statuses --write-raw-book-diffs
--batch-by-block --disable-output-file-buffering
--replica-cmds-style recent-actions --serve-info --serve-eth-rpc
```

The separate WS daemon tails files and reconstructs selected markets. It requests
a full L4 snapshot at startup/recovery only; periodic integrity snapshots are
disabled. There was no WS resync/snapshot request during the incident capture.

## Evidence

At block 1160140000:

* 03:45:17.700 UTC: node logs the block applied.
* During the interval ending 03:45:18 UTC: hl-node CPU 814% (100%=one core),
  including 229% system CPU; 604,376 minor faults/sec; zero major faults;
  resident memory up roughly 755 MiB from the preceding one-second report.
* 03:45:18.898 UTC diagnostic scrape: newly observed maximum block-to-output
  age 1,202–1,203 ms and WS send age 1,225 ms. The reader-to-send window maximum
  remains 49 ms. No sampled replay backlog at those instants.
* 03:45:21.316 UTC: computed app_hash, logged total_latency 3.596725405 seconds.
* 03:45:31.942 UTC: serialized ABCI state, elapsed 10.566232081 seconds; shortly
  afterward linked a periodic state at this height.
* 03:45:34.910–03:45:42.145 UTC: greeting-state serialization; elapsed
  7.231660412 seconds; serialized state length 1,740,073,399 bytes, with EVM checkpoint.

At block 1160142000:

* 03:47:38.855 UTC: node logs the block applied.
* 03:47:39–40 UTC: another large minor-fault/CPU burst.
* 03:47:42.556 UTC: computed app_hash, total_latency 3.686372998 seconds.

Neither container was CPU-quota throttled. Overall CPU had ample idle capacity,
although several cores became briefly busy. Node IO-pressure totals were very
small; no major-fault burst or sustained disk pressure coincided with the first
spike. We do not claim operation durations are whole-node pauses: block progress
continues during the broader sequence.

The five-minute BTC client test had 553 matching comparable top-five books, no
book mismatch, and five of 4,222 local book messages older than one second (max
1,205.56 ms). The corresponding client summary lacks individual stale-event
timestamps, so exact pairing to the logged operations is not yet established.
All 892 comparable trades matched; 30 additional public trade IDs are not yet
classified as capture-boundary effects versus internal gaps.

## Interpretation and questions

App-hash work is a strong repeated correlate of the CPU/memory bursts; the first
coincides with stale output before WS reconstruction. This is observational evidence,
not proof of a particular lock, allocator path, or peer behavior. Later serialization
cannot explain an earlier delay simply by being expensive.

1. Does app-hash preparation copy or hold application state synchronously with
   block application/output writers on a non-validator?
2. Are there supported controls for worker concurrency, priority, or scheduling
   of hashing and periodic/greeting serialization that preserve validation?
3. Are these timings expected for this state size, or is there a known regression
   associated with the deployed binary (fingerprint to be supplied)?
4. Which profiling symbols/build identifiers would make a bounded perf capture
   useful? The distributed executable may be stripped or omit frame pointers.

Public documentation: [node flags and snapshots](https://github.com/hyperliquid-dex/node).
No documented hash/checkpoint bypass was found in that README. No such flag has
been applied or recommended. The unrelated midnight file-writer issue found during
research does not match this continuously advancing, short-duration incident.
