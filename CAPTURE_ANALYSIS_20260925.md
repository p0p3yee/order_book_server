# Instrumented BTC capture — September 25, 03:25:12–03:30:13 UTC

## Conclusion

The capture supports correct comparable BTC books and stable operation, with most
absolute event age already present at the node's output timestamp. It does not
show sustained reader backlog, heavy socket-write delay, WS CPU quota throttling,
or sustained host disk pressure. The specific 411 ms matched-trade p95 cannot be
assigned to a cause from aggregate statistics without paired per-event timestamps.

Do not restart or change flags on the basis of these results alone. Next inspect
the actual node output flags and obtain node process/per-core measurements. A
targeted WS optimization is demand-driven L2 aggregation; it cannot remove delay
that exists before node output.

## Inputs and limits

Analyzed user-provided `metadata.json`, `samples.jsonl`, `vmstat.txt`, `iostat.txt`,
`pidstat.txt`, and `ws.log`, together with the pasted client summary. Raw host files
remain outside this repository and are not published. The capture contains 301
one-second diagnostic samples at 03:25:12.792683 through 03:30:13.093554 UTC. The
server was revision `adb25a0`, batch mode, polling every 5 ms, with integrity
snapshots disabled. One BTC L2/trades client was logged during the test.

Diagnostic percentiles are rolling windows of at most 512 observations per metric,
with different window durations by stage. The table below reports the **median of
the sampled rolling p50/p95 values**, not reconstructed five-minute percentiles.
Maxima are the largest retained maxima observed; initial windows may include some
pre-test history, and polling can miss rapidly overwritten samples. Socket metrics
mix subscribed book and trade messages; stage percentiles must not be added.

## Client results

552 comparable five-level books matched, with no mismatch or ambiguous timestamps.
639 comparable trades matched. The public count was 669; unmatched identities and
their positions within the shared capture window were not included in the supplied
summary, so a gap cannot be proved or dismissed. Local received 4,140 book messages,
with none over one second old (median 367.6 ms, p95 495.9 ms, maximum 879.5 ms).
Matching local-minus-public arrivals: books 45.7/137.5 ms median/p95; trades
73.6/411.0 ms. No transport errors were reported. Every sampled health was Ready,
and logs show the client closing at the planned end of the probe.

## Server stage measurements (milliseconds)

| Stage | Typical rolling p50 | Typical rolling p95 | Largest observed window maximum |
|---|---:|---:|---:|
| Block to node-local output time, diffs | 381.24 | 507.65 | 893.55 |
| Node-local output to diff read start | 3.96 | 9.61 | 31.47 |
| Node-local output to order-status read start | 5.93 | 11.95 | 32.97 |
| Node-local output to fill read start | 4.42 | 16.44 | 32.53 |
| Order-status JSON parsing | 2.37 | 6.64 | 25.94 |
| Book application/validation | 0.36 | 1.14 | 4.52 |
| L2 aggregation | 7.38 | 10.21 | 14.48 |
| Book read to publish | 15.61 | 27.70 | 68.06 |
| Fill read to publish | 0.074 | 0.201 | 1.10 |
| WebSocket dispatch queue | 0.29 | 9.24 | 22.59 |
| Trade reconstruction | 0.0012 | 0.0155 | 0.187 |
| Serialization | 0.0055 | 0.0155 | 0.142 |
| Socket send | 0.040 | 0.053 | 0.087 |
| Read to socket-send completion, mixed messages | 15.55 | 28.06 | 71.17 |

Node `local_time` is an existing output timestamp, not an instrumented network
ingestion timestamp. Block-to-output age includes upstream processing and any
clock offset; it is not the local-minus-public arrival difference. Client and host
absolute ages also use different clocks and populations. Host reports NTP sync,
but this does not measure its offset against the bot or the block-time clock.

The largest observed read-to-send maximum first appeared in the 03:28:10.971 UTC
scrape alongside maxima of 25.94 ms status parsing, 10.13 ms status-file reading,
37.89 ms listener lock hold, and 47.43 ms tick lateness. This bounds the appearance
to an interval, not an exact per-event trace or proof that the maxima share an event.
Host IO/memory PSI did not show a corresponding sustained pressure rise. Around
03:28:51, the output-age maximum approached 894 ms. These are different observations;
do not equate them with the unknown timestamp of the client's worst trade delay.

Reader unread-byte metrics were zero throughout observed windows. At sampled
instants, each book queue held at most one block, and retained input was at most
311,474 bytes (median/p95 zero). There were no client-lagged-message metrics.
This supports small transient stream alignment waits rather than accumulated replay.

## Host evidence

* WS container CPU consumed 91.48 CPU-seconds over 300.30 wall seconds: about
  **0.305 CPU cores on average**, not 30.5% of the entire host. No quota was set;
  `nr_throttled` and `throttled_usec` stayed zero. Its recorded cgroup CPU-pressure
  total increased by about 0.327 seconds over the window.
* Container memory.current peaked at 229,527,552 bytes (218.9 MiB). This includes
  cgroup-accounted memory and is not process RSS. No memory high/max/OOM events.
* Excluding the initial since-boot iostat report, roughly overlapping one-second
  host samples showed median CPU idle 86.29%, minimum 70.18%. Iowait median 0.03%,
  maximum 0.35%; steal was zero. Aggregate idle does not exclude one busy core.
* Host CPU PSI avg10 peaked at 1.68%; IO PSI avg10 stayed at 0.00%; memory PSI
  avg10 briefly reached 0.18%. There was no sustained swapping (swap-out zero;
  tiny swap-in bursts). Existing allocated swap is not evidence of current thrashing.
* nvme0n1 had up to 17.9% utilization, read-await maximum 0.11 ms and write-await
  maximum 0.5 ms; nvme1n1 had up to 13.9% utilization and a write-throughput burst
  near 347 MiB/s. nvme2n1 had occasional 8.5 ms await at low request volume and
  at most 2.4% utilization. The data-volume-to-device mapping was not captured.
  These averages/pressure measurements do not establish storage saturation or
  eliminate every individual IO stall.
* Docker metadata confirms host networking. The listening PID at port 8000 was
  `websocket_serve`, supporting a direct WS listener rather than a local proxy.
  This does not measure downstream NIC/LAN/client receive scheduling.

Pressure interpretation follows the [Linux PSI documentation](https://docs.kernel.org/accounting/psi.html).

## Collector correction and next actions

The old collector used Docker's initial PID. With `--init`, that was **docker-init
1836829**, while the real server was **1836843**. Thus `pidstat.txt` cannot be used
as the server's per-process CPU/RSS measurement. The cgroup statistics remain useful
because init and server belong to the same container cgroup. No node-container
process was requested in this capture.

The collector now discovers workload PIDs through
[`docker top`](https://docs.docker.com/reference/cli/docker/container/top/), includes
all discovered processes, records discovery failures, adds per-core mpstat if
available, and uses UTC/C-locale subprocess output. PID discovery is at capture
start; rerun after a restart. Shared cgroup counters must not be summed over PIDs.
Three regression tests cover wrapped/unwrapped processes and discovery failure.
No Rust server behavior changed; this collector fix needs no image rebuild.

Prioritized follow-up:

1. Inspect actual hl-node flags, especially `--disable-output-file-buffering`,
   while retaining batch mode until evidence supports a tested format change.
   The small observed output-to-read delays do not prove buffering is the bottleneck.
2. Include the hl-node container in the corrected collector and collect per-core CPU.
   Current host averages cannot reveal a saturated node thread or peer/ingestion delay.
3. Capture per-event identities and arrival timestamps if diagnosing the trade tail
   or the 30 unmatched trades; the current summary alone is insufficient.
4. Optimize L2 aggregation for actually subscribed markets/rounding variants. Source
   currently computes full L2 and six rounded variants for every configured market
   when any L2 client exists. That explains avoidable work even for one BTC top-five
   subscriber; preserving subscription refresh and cache invalidation needs tests.
   This is proposed, not changed in this analysis commit.

No live server/container was changed, and no new snapshot was requested for this analysis.
