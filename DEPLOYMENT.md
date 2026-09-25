# Local low-latency WebSocket server

Based on Hyperliquid's reference `order_book_server` at
`8b4f237904f683aca2dba21a07d87e831ead2a97`, on branch `low-latency-ws`.
Fork: https://github.com/p0p3yee/order_book_server.git.
No node access is required to build or run the mock tests. Use the `low-latency-ws` branch of the fork.

## What changed

The existing order book, L2 aggregation, subscription shapes, priority ALO insertion,
and L4 snapshot/update wire formats are retained. The listener now owns all book
mutations and sends ordered messages directly. Its states are `Initializing`,
`Ready`, `Resyncing`, and `Stale`.

On a gap, malformed book record, price/size inconsistency, crossed completed book,
truncation, or stale stream, it drops the suspect book, increments a generation,
and gates book publication. One snapshot request may be in flight at a time.
Incoming blocks are buffered with a configurable byte budget. A fresh snapshot
is round-trip validated, older buffered records are discarded, and newer complete
blocks are replayed in order. READY requires a fresh contiguous post-snapshot block.
A second problem during snapshot generation invalidates that attempt. Failures retry
at a limited rate without restarting the daemon or closing healthy client sockets.

There is **no periodic full snapshot by default**. Startup and recovery still require
one full snapshot; market filtering cannot reduce the file produced by hl-node's
currently documented `l4Snapshots` request. Optional `--integrity-interval-secs 3600`
performs a gated checkpoint comparison/rebuild. It clones a checkpoint, replays it
to the fetched height where possible, compares full selected-market order lists,
logs comparison failures, and installs the authoritative snapshot. Book publication
pauses during this check. Set `0` (the default) to disable scheduled checks.

Correctness checks remain active on every block: continuity and block metadata,
order existence, duplicate inserts, insert-before anchors, new-order status/diff
price agreement, existing-order price, update `origSz`, and an uncrossed final book.
Snapshot comparison checks list lengths as well as each order; an equal prefix no
longer hides missing orders. Decimal input/output uses exact eight-decimal fixed
point conversion rather than floating point. Unsupported precision is rejected.
The raw-diff path inserts orders without locally matching orders a second time.

Fills are grouped by `(coin, tid)`. Exactly one consistent Ask/Bid pair is required;
incomplete, same-side, duplicated, or inconsistent pairs are skipped and counted in
warnings. Trades are only constructed in socket tasks that have trade subscriptions.
Malformed fill JSON does not invalidate the order book. Streamed fills accumulate
until the next fill block, so an inactive market's last trade batch can be delayed.

`--markets` filters reconstruction, replay events, and L2 computation while preserving
all block envelopes. It is a fixed deployment allowlist, not a dynamic subscription
filter; changing it requires a restart. An empty allowlist means all non-spot markets.
The full input JSON is still parsed. L2 aggregation is skipped when there are no L2
subscriptions. The existing supported aggregation variants are computed for retained
markets when L2 is needed.

The previous filesystem-notification reader was replaced with bounded JSONL tailing:
partial lines wait for a newline, consumed lines are not replayed after a partial write,
truncation/replacement causes recovery, and hourly rotation drains the old file before
reading the new one. Polling defaults to 5 ms and is configurable. Discovery runs once
per second; rotation can therefore add up to about one second of latency. The old
notification-only reader and its platform-dependent test were removed; tailing and
real rotation are covered by the replacement unit and process tests.

## Build on the node machine

Clone the modified branch from your fork:

```bash
mkdir -p /path/to/project
cd /path/to/project
git clone --branch low-latency-ws --single-branch \
  https://github.com/p0p3yee/order_book_server.git order_book_server
cd order_book_server
```

The clone destination must be absent/empty. If an existing deployment uses that directory,
clone into a separate directory rather than overwriting it. Build a fresh Linux binary
or Docker image on your node machine; the development-machine binary is not a Linux build.
A Git bundle is also available for offline transfer, but is not needed for a GitHub clone.

With Rust 1.94.0 and the normal Linux build prerequisites (C compiler, pkg-config,
OpenSSL headers):

```bash
cd /path/to/project/order_book_server
cargo fmt --check
cargo test --locked
cargo build --locked --release --bin websocket_server
python3 scripts/mock_e2e.py
python3 scripts/mock_e2e.py --stream
```

Both Python tests only launch loopback mock servers and temporary files. They never
contact or modify a Hyperliquid node. They verify a >10-second healthy interval without
another snapshot, fill errors, block gaps, rotation, L4 replacement snapshots, stale
input, and HTTP failure recovery on the same WebSocket connection and process.

Direct host invocation, **assuming the supplied host path is the directory directly
containing `node_*_by_block`**:

```bash
RUST_LOG=info RAYON_NUM_THREADS=2 TOKIO_WORKER_THREADS=2 \
./target/release/websocket_server \
  --address 0.0.0.0 --port 8000 \
  --node-data-dir /path/to/node-data \
  --snapshot-path /path/to/node-data/ws-snapshot.json \
  --snapshot-node-path /path/to/node-data/ws-snapshot.json \
  --info-url http://127.0.0.1:3001/info \
  --markets BTC,HYPE,AERO,xyz:NVDA,xyz:DRAM \
  --websocket-compression-level 0
```

If hl-node is containerized, `--snapshot-node-path` must instead be the path it sees
inside its container, even when the WS binary runs on the host. Do not set it to the
host path unless hl-node can actually resolve that path.

## Docker and path mapping

`Dockerfile` is a multi-stage Rust 1.94.0 / Debian Bookworm build. `compose.yaml` defines
only the `websocket` component; it does not start or modify your node. Linux host
networking preserves access to `http://127.0.0.1:3001/info` and exposes port 8000.
The healthcheck reports NOT READY without asking Docker to restart an unhealthy book.
`restart: unless-stopped` covers actual process exits only.

```bash
cd /path/to/project/order_book_server
cp .env.example .env
# Edit .env to match your EXISTING node's volume layout and owner UID/GID.
docker compose config
# Build while the existing WS service is still running; no port conflict at build time.
docker compose build websocket
```

Resolve these three paths before starting:

| Setting | Meaning | Supplied default/example |
|---|---|---|
| `HL_DATA_DIR` | Host directory containing output directories | `/path/to/node-data` |
| WS `--node-data-dir` | That directory mounted in the WS container | `/node-data` |
| `HL_NODE_SNAPSHOT_PATH` | File path interpreted by the existing hl-node process | `/path/in/node/data/ws-snapshot.json` |

The `/path/in/node/data` example is **not an assumption about your current node container**.
Inspect its mounts locally on your server. If `/path/to/node-data` is a node home
or `hl` directory, set `HL_DATA_DIR` to its actual `hl/data` or `data` subdirectory.
Both snapshot paths must refer to the same underlying file. Actual requests add a
unique process/time suffix before `.json`, and successful reads remove those files.
Timeouts can leave orphan files if hl-node completes a request after the HTTP timeout;
inspect `ws-snapshot.*.json` during maintenance. Never run a broad cleanup against node data.

The WS UID/GID needs read access to event files and read/delete access to generated
snapshot files. hl-node needs write access to the snapshot's parent directory. Use
`stat -c '%u:%g %a %n' "$HL_DATA_DIR"` to inspect ownership; do not recursively change
ownership of an active node volume as part of this migration.

## Migration

1. Keep the node and its existing output flags running. Back up the existing WS Compose
   configuration/image tag so it can be restored. Record baseline node lag, CPU, disk
   I/O, and restart counts using your existing measurement procedure.
2. Copy/build the modified source; configure paths and UID/GID as above. Run the local
   mock tests first. Keep batch mode for the first deployment.
3. Stop **only the old WS service/container** using its existing Compose project/service
   name. Do not run `docker compose down` against the node stack. This deliberate cutover
   disconnects the old clients once; internal recovery after migration does not.
4. Start this project's component:

   ```bash
   cd /path/to/project/order_book_server
   docker compose up -d websocket
   docker compose logs -f --tail=100 websocket
   ```

5. Wait for `resync end` and `/health` READY. Subscribe from the bot PC, confirm book
   freshness and selected markets, and repeat the baseline lag measurement under the
   same workload. Do not infer a numerical latency improvement from the local mock tests.
6. If paths or schemas are wrong, the component will remain NOT READY and log the reason.
   Fix configuration and recreate **only websocket**. To roll back, stop this component
   and restore/start the old WS service. Leave node state/volumes untouched.

## Node flags and streamed mode

Recommended first deployment, appended to your existing non-validator invocation:

```text
--serve-info
--write-order-statuses
--write-raw-book-diffs
--write-fills
--batch-by-block
--disable-output-file-buffering
```

`--write-fills` is needed for trades; L2 reconstruction only depends on order statuses
and raw book diffs. Keep your existing chain and unrelated node settings.

[The official node README](https://github.com/hyperliquid-dex/node#flags) documents
that `--stream-with-block-info` shares the `{local_time, block_time, block_number,
events}` envelope but emits events during processing. Equal-height lines are fragments,
not duplicate complete batches. This implementation aggregates them and closes a book
block only when **both** book streams have advanced beyond that height. It preserves
record order within each stream and validates the completed block before publication.

For a future canary, replace `--batch-by-block` with `--stream-with-block-info` on the
node and add `--stream-with-block-info` to the WS command. Keep
`--disable-output-file-buffering`. Do not combine the two batching flags.

**Streamed mode is experimental until checked against your node build.** The public
schema does not specify empty-block markers or a separate end-of-block signal. Missing
heights cannot safely be distinguished from dropped output. This implementation
therefore requires contiguous heights in both book streams and resyncs on gaps instead
of synthesizing empty blocks. If your streamed outputs omit empty blocks, stay in batch
mode; reliable block-completion metadata is needed before relaxing that rule. The
one-block watermark wait also means this is not a sub-block L2 feed.

Default directories retain the upstream `_by_block` names. If the node build uses
other names in stream mode, provide `--order-status-dir`, `--book-diff-dir`, and
`--fills-dir` explicitly. Do not point a metadata reader at legacy bare-event files.
Use the read-only preflight on the actual server after checking output names:

```bash
python3 scripts/check_stream.py --data-dir /path/to/node-data --mode batch
# On a streamed canary, use --mode stream and explicit directory overrides if necessary.
```

A passing sample checks recent continuity, not a guarantee about future empty blocks.
[The official L1 schemas](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/nodes/l1-data-schemas)
remain the source for individual order-status and raw-diff fields.

## LAN API and bot behavior

Endpoint remains `ws://NODE_LAN_IP:8000/ws`:

```json
{"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}
```

`trades` and `l4Book` subscriptions keep their existing shapes. L2 subscribers receive
an immediate snapshot when READY. During startup/recovery subscriptions can be accepted
and remain registered, with book data delayed until READY. Existing clients remain
connected and subscriptions survive recovery. A new additive status channel carries:

```json
{"channel":"status","data":{"state":"Resyncing","generation":2,"height":12345,"upstream_time":1790000000000,"reason":"skipped block ...","resyncs":2,"validation_failures":0}}
```

The bot should gate trading on `state == "Ready"`, enforce its own age timeout, and
replace an L4 book whenever it receives `L4Book::Snapshot`, including mid-connection.
L2 messages are complete snapshots. Queued messages from invalid generations are
suppressed, and per-subscription height floors prevent old queued updates from being
sent after a newer immediate/reset snapshot. A slow client's broadcast overflow resets
its books instead of dropping TCP; trades are live-only and can have gaps during overflow.
Status transitions and book snapshots are not a transaction across the network: the bot
must wait for fresh book data after READY. Normal network/peer failures can still close
connections. There is no durable trade replay, authentication, or TLS in this LAN service.

## Actual-server verification

Run these yourself; development did not access your server:

```bash
cd /path/to/project/order_book_server
curl -sS -i http://127.0.0.1:8000/health
# Read-only node freshness check; does not request a snapshot:
curl -sS http://127.0.0.1:3001/info -H 'Content-Type: application/json' \
  --data '{"type":"exchangeStatus"}'
docker compose logs --since=10m websocket
# Expect one startup snapshot; healthy operation with interval=0 must show no periodic requests.
docker compose logs --since=10m websocket | rg 'snapshot_generation_ms|resync|recovery|validation|skipped'
docker inspect --format '{{.State.Pid}} {{.RestartCount}} {{.State.Health.Status}}' \
  "$(docker compose ps -q websocket)"
docker stats --no-stream "$(docker compose ps -q websocket)"
```

From the bot PC (copy `scripts/watch_feed.py` and `scripts/mock_e2e.py` together):

```bash
python3 scripts/watch_feed.py ws://NODE_LAN_IP:8000/ws --coin BTC --seconds 60
```

The report measures arrival age from the block timestamp, **not isolated WS processing
latency**. Synchronize both machines' clocks before interpreting it. Repeat for HYPE,
AERO, xyz:NVDA, and xyz:DRAM and confirm that each is present in the node snapshot.
Compare node lag with your prior 1552/2484 ms baseline and 278/566 ms WS-stopped baseline,
using the same collection method. Check CPU, disk I/O, snapshot request counts, and
resync reasons as well as feed age.

Test fault recovery with `scripts/mock_e2e.py` first. Do not delete/truncate live node
output or stop the production node solely to test this service. During a naturally
occurring stall, verify that `/health` becomes non-200, book updates stop, PID and restart
count stay constant, and the same LAN client resumes after READY.

For a deliberate full integrity comparison, temporarily set
`WS_INTEGRITY_INTERVAL_SECS=3600` and recreate only the WS component. This adds a full
snapshot each hour and intentionally pauses book publication for the check. Inspect
`integrity comparison passed/failed` and recovery logs. It is not enabled by default.

## Configuration and limitations

| CLI option | Default |
|---|---|
| `--node-data-dir` | `$HOME/hl/data` |
| `--snapshot-path` | `<node-data-dir>/ws-snapshot.json` |
| `--snapshot-node-path` | Same as snapshot-path |
| `--info-url` | `http://127.0.0.1:3001/info` |
| `--markets` | All non-spot markets |
| `--stream-with-block-info` | Off (complete batch mode) |
| `--stale-after-secs` | 5 |
| `--snapshot-timeout-secs` | 120 (HTTP generation timeout) |
| `--retry-interval-secs` | 30 after attempt completion |
| `--integrity-interval-secs` | 0 (disabled) |
| `--max-buffer-mib` | 256 replay accounting budget |
| `--poll-interval-ms` | 5 |

Snapshot parsing runs on a blocking worker; installation and replay use the book lock
and may pause health/API handling during large work. Snapshot memory is additional to
the replay budget; budget accounting covers serialized bytes, not allocator overhead
or the whole process RSS. There is no claim of constant memory at mainnet scale.
Retries can still impose node load during prolonged failure, so investigate repeated
resyncs rather than decreasing the retry interval in production.

Continuous invariants cannot prove equality with every untouched order in upstream
state. An error that preserves all checked invariants may remain undetected until an
optional full integrity checkpoint. This is the explicit tradeoff when eliminating
mandatory full-book comparisons; validation has not been removed. Full production
book parity and latency require your real-node canary measurements.

Spot and untriggered trigger orders remain unsupported. Eight-decimal fixed point
precision is inherited from the reference representation. Historical output replay,
node version schema changes, sparse streamed blocks, and all HIP-3 instruments have
not been validated against captured production fixtures. Filtering cannot fix a node
that is already producing stale events. There is no Prometheus endpoint; `/health`,
status messages, and structured key/value logs provide the current observability.

## Changed files

Relative to the pinned upstream commit (including the removed legacy reader):

- `.dockerignore`
- `.env.example`
- `.gitignore`
- `Cargo.lock`
- `DEPLOYMENT.md`
- `Dockerfile`
- `README.md`
- `TEST_RESULTS.md`
- `binaries/src/bin/websocket_server.rs`
- `compose.yaml`
- `rustfmt.toml`
- `scripts/check_stream.py`
- `scripts/mock_e2e.py`
- `scripts/watch_feed.py`
- `server/Cargo.toml`
- `server/src/config.rs`
- `server/src/lib.rs`
- `server/src/listeners/directory.rs`
- `server/src/listeners/mod.rs`
- `server/src/listeners/order_book/mod.rs`
- `server/src/listeners/order_book/state.rs`
- `server/src/listeners/order_book/tail.rs`
- `server/src/listeners/order_book/utils.rs`
- `server/src/order_book/mod.rs`
- `server/src/order_book/multi_book.rs`
- `server/src/order_book/types.rs`
- `server/src/servers/websocket_server.rs`
- `server/src/types/mod.rs`
- `server/src/types/node_data.rs`
- `server/src/types/subscription.rs`
