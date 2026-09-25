use crate::{
    ServerConfig,
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_str_filtered},
    },
    prelude::*,
    telemetry::{Metrics, Trace, unix_us},
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::Subscription,
    },
};
use alloy::primitives::Address;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use state::OrderBookState;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tail::{Tail, latest_file};
use tokio::sync::{Mutex, broadcast::Sender};
use utils::{process_rmp_file, validate_snapshot_consistency};
mod state;
mod tail;
mod utils;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Health {
    Initializing,
    Ready,
    Resyncing,
    Stale,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FeedStatus {
    pub state: Health,
    pub generation: u64,
    pub height: Option<u64>,
    pub upstream_time: Option<u64>,
    pub reason: String,
    pub resyncs: u64,
    pub validation_failures: u64,
}

/// One owner applies book mutations and publishes them synchronously in block order.
pub(crate) struct OrderBookListener {
    config: ServerConfig,
    state: Option<OrderBookState>,
    checkpoint: Option<OrderBookState>,
    pub(crate) status: FeedStatus,
    orders: BTreeMap<u64, Batch<NodeDataOrderStatus>>,
    diffs: BTreeMap<u64, Batch<NodeDataOrderDiff>>,
    fills: Option<Batch<NodeDataFill>>,
    order_watermark: Option<u64>,
    diff_watermark: Option<u64>,
    last_fill: Option<u64>,
    buffered_bytes: usize,
    last_progress: Instant,
    latest_trace: Option<Trace>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) wallet: Option<crate::wallet::WalletHub>,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    pub(crate) l2_subscriptions: Arc<std::sync::Mutex<HashMap<Subscription, usize>>>,
}
impl OrderBookListener {
    pub(crate) fn new(tx: Sender<Arc<InternalMessage>>, config: ServerConfig) -> Self {
        Self {
            config,
            state: None,
            checkpoint: None,
            status: FeedStatus {
                state: Health::Initializing,
                generation: 0,
                height: None,
                upstream_time: None,
                reason: "startup".into(),
                resyncs: 0,
                validation_failures: 0,
            },
            orders: BTreeMap::new(),
            diffs: BTreeMap::new(),
            fills: None,
            order_watermark: None,
            diff_watermark: None,
            last_fill: None,
            buffered_bytes: 0,
            last_progress: Instant::now(),
            latest_trace: None,
            metrics: Arc::new(Metrics::default()),
            wallet: None,
            internal_message_tx: tx,
            l2_subscriptions: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }
    pub(crate) fn is_ready(&self) -> bool {
        self.status.state == Health::Ready
    }
    pub(crate) fn accepts(&self, coin: &str) -> bool {
        self.config.includes(coin)
    }
    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }
    fn send(&self, message: InternalMessage) {
        let _unused = self.internal_message_tx.send(Arc::new(message));
    }
    fn status_message(&self) {
        self.send(InternalMessage::Status(self.status.clone()));
    }
    fn recover(&mut self, reason: impl ToString, stale: bool) {
        let reason = reason.to_string();
        let repeated = self.state.is_none() && self.status.reason == reason;
        if !repeated {
            warn!("book recovery height={:?} reason={reason}", self.status.height);
        }
        self.status.generation += 1;
        self.status.resyncs += 1;
        self.status.state = if stale { Health::Stale } else { Health::Resyncing };
        self.status.reason = reason;
        self.state = None;
        self.latest_trace = None;
        self.checkpoint = None;
        self.orders.clear();
        self.diffs.clear();
        self.fills = None;
        self.order_watermark = None;
        self.diff_watermark = None;
        self.buffered_bytes = 0;
        if !repeated {
            self.status_message();
        }
    }
    pub(crate) fn compute_snapshot(&self) -> Option<TimedSnapshots> {
        if self.is_ready() { self.state.as_ref().map(OrderBookState::compute_snapshot) } else { None }
    }
    pub(crate) fn current_l2(&mut self, subscription: &Subscription) -> Option<(u64, L2Snapshots)> {
        let requested = l2_requests(std::iter::once(subscription));
        if self.is_ready() { self.state.as_mut().and_then(|s| s.l2_snapshots(false, &requested)) } else { None }
    }
    fn selected_snapshot(&self, snapshot: Snapshots<InnerL4Order>) -> Snapshots<InnerL4Order> {
        Snapshots::new(snapshot.value().into_iter().filter(|(c, _)| self.config.includes(&c.value())).collect())
    }
    fn install(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) -> Result<()> {
        let snapshot = self.selected_snapshot(snapshot);
        if let Some(mut checkpoint) = self.checkpoint.take() {
            let validation = (|| -> Result<()> {
                if checkpoint.height() > height {
                    return Err("integrity snapshot behind checkpoint".into());
                }
                while checkpoint.height() < height {
                    let next = checkpoint.height() + 1;
                    if self.config.stream_with_block_info
                        && !(self.order_watermark.is_some_and(|h| h > next)
                            && self.diff_watermark.is_some_and(|h| h > next))
                    {
                        return Err("integrity snapshot ahead of complete stream; retry checkpoint later".into());
                    }
                    let orders = self.orders.get(&next).ok_or("integrity replay missing order block")?.clone();
                    let diffs = self.diffs.get(&next).ok_or("integrity replay missing diff block")?.clone();
                    checkpoint.apply_updates(orders, diffs)?;
                }
                validate_snapshot_consistency(&checkpoint.compute_snapshot().snapshot, snapshot.clone(), true)
            })();
            match validation {
                Ok(()) => info!("integrity comparison passed height={height}"),
                Err(err) => {
                    self.status.validation_failures += 1;
                    warn!("integrity comparison failed height={height}: {err}; rebuilding from authoritative snapshot");
                }
            }
        }
        // Round-trip validation catches duplicate IDs, crossed snapshots or representation loss.
        let state = OrderBookState::from_snapshot(snapshot.clone(), height, 0, true, true, self.config.markets.clone());
        validate_snapshot_consistency(&state.compute_snapshot().snapshot, snapshot, true)?;
        self.state = Some(state);
        self.last_progress = Instant::now();
        self.orders.retain(|h, _| *h > height);
        self.diffs.retain(|h, _| *h > height);
        self.status.height = Some(height);
        // Remain gated until a fresh, complete block after the snapshot has been validated.
        self.status.state = Health::Resyncing;
        self.status.reason = "snapshot installed; waiting for contiguous live block".into();
        self.status_message();
        self.drain()
    }
    #[cfg(test)]
    fn ingest(&mut self, source: usize, line: &str) -> Result<()> {
        self.ingest_observed(source, line, Instant::now(), unix_us())
    }
    pub(crate) fn diagnostics(&self) -> serde_json::Value {
        let requested = {
            let demand = self.l2_subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            l2_requests(demand.keys())
        };
        serde_json::json!({"status":self.status,"server":crate::telemetry::version(),
            "config":{"poll_interval_ms":self.config.poll_interval.as_millis(),
                "stale_after_ms":self.config.stale_after.as_millis(),
                "stream_with_block_info":self.config.stream_with_block_info,
                "integrity_interval_secs":self.config.integrity_interval.as_secs()},
            "wallet":self.wallet.as_ref().map(|w|w.status()),
            "l2_demand":{"markets":requested.len(),"variants":requested.values().map(HashSet::len).sum::<usize>()},
            "backlog":{"order_blocks":self.orders.len(),"diff_blocks":self.diffs.len(),
                "retained_input_bytes":self.retained_input_bytes(),
                "budget_accounting_counter_bytes":self.buffered_bytes,
                "note":"retained_input_bytes counts original record bytes for retained blocks, not process RSS"},
            "metrics":self.metrics.snapshot(),
            "clock_note":"duration metrics use monotonic time; node/output timestamps need synchronized clocks; socket send completion is not client receipt"})
    }
    fn retained_input_bytes(&self) -> usize {
        self.orders.values().map(|b| b.input_bytes).sum::<usize>()
            + self.diffs.values().map(|b| b.input_bytes).sum::<usize>()
            + self.fills.as_ref().map_or(0, |b| b.input_bytes)
    }
    fn ingest_observed(&mut self, source: usize, line: &str, first_read: Instant, read_us: i64) -> Result<()> {
        let ingest_start = Instant::now();
        self.buffered_bytes = self.buffered_bytes.saturating_add(line.len());
        if self.buffered_bytes > self.config.max_buffer_bytes {
            // Recount retained input lengths; never serialize queues just to measure them.
            self.buffered_bytes = self.retained_input_bytes().saturating_add(line.len());
            if self.buffered_bytes > self.config.max_buffer_bytes {
                return Err("replay buffer limit exceeded".into());
            }
        }
        match source {
            0 => {
                let mut batch: Batch<NodeDataOrderStatus> = serde_json::from_str(line)?;
                batch.input_bytes = line.len();
                batch.trace = Some(Trace::new(
                    batch.block_number(),
                    batch.block_time.and_utc().timestamp_micros(),
                    batch.local_time.and_utc().timestamp_micros(),
                    first_read,
                    read_us,
                ));
                record_decode(&self.metrics, source, &batch, ingest_start);
                batch.events.retain(|e| self.config.includes(&e.order.coin));
                insert_batch(&mut self.orders, &mut self.order_watermark, batch, self.config.stream_with_block_info)?;
            }
            1 => {
                let mut batch: Batch<NodeDataOrderDiff> = serde_json::from_str(line)?;
                batch.input_bytes = line.len();
                batch.trace = Some(Trace::new(
                    batch.block_number(),
                    batch.block_time.and_utc().timestamp_micros(),
                    batch.local_time.and_utc().timestamp_micros(),
                    first_read,
                    read_us,
                ));
                record_decode(&self.metrics, source, &batch, ingest_start);
                batch.events.retain(|e| self.config.includes(&e.coin().value()));
                insert_batch(&mut self.diffs, &mut self.diff_watermark, batch, self.config.stream_with_block_info)?;
            }
            _ => {
                let mut batch: Batch<NodeDataFill> = serde_json::from_str(line)?;
                batch.input_bytes = line.len();
                batch.trace = Some(Trace::new(
                    batch.block_number(),
                    batch.block_time.and_utc().timestamp_micros(),
                    batch.local_time.and_utc().timestamp_micros(),
                    first_read,
                    read_us,
                ));
                record_decode(&self.metrics, source, &batch, ingest_start);
                batch.events.retain(|e| self.config.includes(&e.1.coin));
                if self.config.stream_with_block_info {
                    if let Some(mut previous) = self.fills.take() {
                        if previous.block_number() == batch.block_number() {
                            previous.events.extend(batch.events);
                            previous.input_bytes = previous.input_bytes.saturating_add(batch.input_bytes);
                            self.fills = Some(previous);
                            return Ok(());
                        }
                        if previous.block_number() > batch.block_number() {
                            return Err("fill height regression".into());
                        }
                        self.publish_fills(previous);
                    }
                    self.fills = Some(batch);
                } else {
                    self.publish_fills(batch);
                }
            }
        }
        self.drain()
    }
    fn publish_fills(&mut self, batch: Batch<NodeDataFill>) {
        if self.last_fill.is_none_or(|h| h < batch.block_number()) {
            self.last_fill = Some(batch.block_number());
            let trace = batch.trace.map(Trace::publish);
            if let Some(t) = trace {
                self.metrics.elapsed("fills_read_to_publish_us", t.first_read);
            }
            self.send(InternalMessage::Fills { batch, trace });
        }
    }
    fn drain(&mut self) -> Result<()> {
        while let Some(state) = self.state.as_mut() {
            let next = state.height() + 1;
            self.orders.retain(|h, _| *h >= next);
            self.diffs.retain(|h, _| *h >= next);
            // A later block proves all fragments at `next` are complete in stream mode.
            if self.config.stream_with_block_info
                && !(self.order_watermark.is_some_and(|h| h > next) && self.diff_watermark.is_some_and(|h| h > next))
            {
                break;
            }
            let (Some((&oh, _)), Some((&dh, _))) = (self.orders.first_key_value(), self.diffs.first_key_value()) else {
                break;
            };
            if oh != next || dh != next {
                return Err(format!("skipped block: expecting {next}, orders={oh}, diffs={dh}").into());
            }
            let orders = self.orders.remove(&next).ok_or("missing orders")?;
            let diffs = self.diffs.remove(&next).ok_or("missing diffs")?;
            let time = orders.block_time();
            let mut trace = diffs.trace.or(orders.trace);
            if let (Some(o), Some(d)) = (orders.trace, diffs.trace) {
                self.metrics.observe(
                    "book_stream_arrival_skew_us",
                    o.first_read.max(d.first_read).duration_since(o.first_read.min(d.first_read)).as_secs_f64() * 1e6,
                );
                self.metrics.elapsed("book_replay_wait_us", o.parsed.max(d.parsed));
                if let Some(t) = &mut trace {
                    t.first_read = o.first_read.min(d.first_read);
                    t.read_us = o.read_us.min(d.read_us);
                }
            }
            let apply_start = Instant::now();
            state.apply_updates(orders.clone(), diffs.clone())?;
            self.metrics.elapsed("book_apply_us", apply_start);
            if let Some(t) = &mut trace {
                t.applied_us = unix_us();
            }
            self.latest_trace = trace;
            self.status.height = Some(next);
            self.status.upstream_time = Some(time);
            self.last_progress = Instant::now();
            let age = chrono::Utc::now().timestamp_millis().max(0) as u64;
            if age.saturating_sub(time) > self.config.stale_after.as_millis() as u64 {
                return Err(
                    format!("upstream event time stale: block={next} age_ms={}", age.saturating_sub(time)).into()
                );
            }
            if self.status.state != Health::Ready {
                self.status.state = Health::Ready;
                self.status.reason = "contiguous replay validated".into();
                info!("resync end generation={} height={next} upstream_time={time}", self.status.generation);
                self.status_message();
                self.send(InternalMessage::Reset { generation: self.status.generation });
            } else {
                self.send(InternalMessage::L4BookUpdates {
                    generation: self.status.generation,
                    diff_batch: diffs,
                    status_batch: orders,
                    trace: trace.map(Trace::publish),
                });
            }
        }
        let requested = {
            let demand = self.l2_subscriptions.lock().unwrap_or_else(|e| e.into_inner());
            l2_requests(demand.keys())
        };
        if self.is_ready() && !requested.is_empty() {
            let l2_start = Instant::now();
            if let Some((time, l2_snapshots)) = self.state.as_mut().and_then(|s| s.l2_snapshots(true, &requested)) {
                self.metrics.elapsed("l2_aggregate_us", l2_start);
                let trace = self.latest_trace.map(Trace::publish);
                if let Some(t) = trace {
                    self.metrics.elapsed("book_read_to_publish_us", t.first_read);
                }
                self.send(InternalMessage::Snapshot {
                    generation: self.status.generation,
                    height: self.status.height.unwrap_or_default(),
                    l2_snapshots,
                    time,
                    trace,
                });
            }
        }
        Ok(())
    }
}

fn record_decode<E>(metrics: &Metrics, source: usize, batch: &Batch<E>, start: Instant) {
    metrics.elapsed(["orders_json_parse_us", "diffs_json_parse_us", "fills_json_parse_us"][source], start);
    if let Some(trace) = batch.trace {
        metrics.observe(
            ["orders_block_to_node_local_us", "diffs_block_to_node_local_us", "fills_block_to_node_local_us"][source],
            (trace.node_local_us - trace.block_us) as f64,
        );
        metrics.observe(
            ["orders_node_local_to_read_us", "diffs_node_local_to_read_us", "fills_node_local_to_read_us"][source],
            (trace.read_us - trace.node_local_us) as f64,
        );
    }
}
impl InternalMessage {
    pub(crate) fn trace(&self) -> Option<Trace> {
        match self {
            Self::Snapshot { trace, .. } | Self::Fills { trace, .. } | Self::L4BookUpdates { trace, .. } => *trace,
            _ => None,
        }
    }
}

fn insert_batch<E>(
    queue: &mut BTreeMap<u64, Batch<E>>,
    watermark: &mut Option<u64>,
    batch: Batch<E>,
    streamed: bool,
) -> Result<()> {
    let height = batch.block_number();
    if watermark.is_some_and(|h| height < h) {
        return Err("upstream block height regressed".into());
    }
    if watermark.is_some_and(|h| height == h) && !streamed {
        return Err("duplicate batch height".into());
    }
    *watermark = Some(height);
    if let Some(previous) = queue.get_mut(&height) {
        if previous.block_time != batch.block_time {
            return Err("inconsistent block timestamps".into());
        }
        previous.events.extend(batch.events);
        previous.input_bytes = previous.input_bytes.saturating_add(batch.input_bytes);
    } else {
        queue.insert(height, batch);
    }
    Ok(())
}

pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, config: ServerConfig) -> Result<()> {
    let names = ["node_order_statuses_by_block", "node_raw_book_diffs_by_block", "node_fills_by_block"];
    // Both metadata modes use the _by_block envelope. Explicit per-stream directory overrides
    // are supported by symlinking these directories if a node build uses different names.
    let mut dirs = names.map(|n| config.data_dir.join(n));
    for (i, custom) in [&config.order_status_dir, &config.book_diff_dir, &config.fills_dir].into_iter().enumerate() {
        if let Some(path) = custom {
            dirs[i] = path.clone();
        }
    }
    let mut tails: [Option<Tail>; 3] = [None, None, None];
    for (i, dir) in dirs.iter().enumerate() {
        if let Some(path) = latest_file(dir)? {
            tails[i] = Some(Tail::open(path, true)?);
        }
    }
    let metrics = listener.lock().await.metrics.clone();
    let mut ticker = tokio::time::interval(config.poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut discovery = Instant::now();
    let mut next_attempt = Instant::now();
    let mut last_integrity = Instant::now();
    let mut snapshot_task = None;
    let mut snapshot_generation = 0;
    let mut diagnostic = Instant::now();
    loop {
        let scheduled = ticker.tick().await;
        metrics.observe(
            "listener_tick_lateness_us",
            tokio::time::Instant::now().saturating_duration_since(scheduled).as_secs_f64() * 1e6,
        );
        for i in 0..3 {
            let result: Result<()> = (|| {
                if discovery.elapsed() >= Duration::from_secs(1) || tails[i].is_none() {
                    if let Some(path) = latest_file(&dirs[i])? {
                        if tails[i].as_ref().is_none_or(|t| t.path() != path) {
                            if let Some(tail) = &mut tails[i] {
                                if !tail.drained()? {
                                    return Ok(());
                                }
                                if tail.has_fragment() {
                                    return Err("partial record at file rotation".into());
                                }
                            }
                            tails[i] = Some(Tail::open(path, false)?);
                        }
                    }
                }
                Ok(())
            })();
            if let Err(err) = result {
                listener.lock().await.recover(err, false);
                tails[i] = None;
            }
            if let Some(tail) = &mut tails[i] {
                let read_start = Instant::now();
                let read_us = unix_us();
                let result = tail.read(config.max_buffer_bytes);
                metrics.elapsed(["orders_file_read_us", "diffs_file_read_us", "fills_file_read_us"][i], read_start);
                metrics.observe(
                    ["orders_unread_bytes", "diffs_unread_bytes", "fills_unread_bytes"][i],
                    tail.unread_bytes as f64,
                );
                match result {
                    Ok(lines) => {
                        if lines.is_empty() {
                            continue;
                        }
                        let lock_start = Instant::now();
                        let mut book = listener.lock().await;
                        metrics.elapsed("listener_lock_wait_us", lock_start);
                        let hold_start = Instant::now();
                        for line in lines {
                            if let Err(err) = book.ingest_observed(i, &line, read_start, read_us) {
                                if i == 2 {
                                    warn!("skipping malformed fill batch: {err}");
                                    book.fills = None;
                                } else {
                                    book.status.validation_failures += 1;
                                    book.recover(err, false);
                                }
                                break;
                            }
                        }
                        metrics.elapsed("listener_lock_hold_us", hold_start);
                    }
                    Err(err) => {
                        listener.lock().await.recover(err, false);
                    }
                }
            }
        }
        if discovery.elapsed() >= Duration::from_secs(1) {
            discovery = Instant::now();
        }
        if let Some(task) = snapshot_task.as_mut() {
            let task: &mut tokio::task::JoinHandle<Result<(u64, Snapshots<InnerL4Order>)>> = task;
            if task.is_finished() {
                let result = task.await;
                snapshot_task = None;
                let mut book = listener.lock().await;
                if book.status.generation == snapshot_generation {
                    let result = match result {
                        Ok(result) => result,
                        Err(err) => Err(err.into()),
                    };
                    match result.and_then(|(height, snapshot)| book.install(snapshot, height)) {
                        Ok(()) => {}
                        Err(err) => {
                            book.status.validation_failures += 1;
                            book.recover(err, false);
                        }
                    }
                }
                next_attempt = Instant::now() + config.retry_interval;
            }
        }
        let mut book = listener.lock().await;
        if book.state.is_some() && book.last_progress.elapsed() > config.stale_after {
            book.recover("stream has fallen behind (no complete book blocks)", true);
        }
        if !config.integrity_interval.is_zero()
            && last_integrity.elapsed() >= config.integrity_interval
            && book.is_ready()
        {
            // A scheduled checkpoint is a gated rebuild, never an unchecked replacement.
            let checkpoint = book.state.take();
            book.recover("scheduled integrity checkpoint", false);
            book.checkpoint = checkpoint;
            next_attempt = Instant::now();
            last_integrity = Instant::now();
        }
        if snapshot_task.is_none() && book.state.is_none() && Instant::now() >= next_attempt {
            snapshot_generation = book.status.generation;
            book.status.state = Health::Resyncing;
            book.status_message();
            info!("resync start generation={} reason={}", snapshot_generation, book.status.reason);
            let config = config.clone();
            snapshot_task = Some(tokio::spawn(async move {
                let path = process_rmp_file(&config).await?;
                let parse_start = Instant::now();
                let read_path = path.clone();
                let parsed = tokio::task::spawn_blocking(move || {
                    let json = fs::read_to_string(read_path)?;
                    load_snapshots_from_str_filtered::<InnerL4Order, (Address, L4Order)>(&json, |coin| {
                        config.includes(coin)
                    })
                })
                .await?;
                if let Err(err) = tokio::fs::remove_file(&path).await {
                    warn!("snapshot cleanup failed path={} error={err}", path.display());
                }
                info!("snapshot_parse_ms={}", parse_start.elapsed().as_millis());
                parsed
            }));
        }
        if diagnostic.elapsed() >= Duration::from_secs(30) {
            info!(
                "feed state={:?} height={:?} upstream_time={:?} clients={} budget_counter_bytes={} retained_input_bytes={} order_blocks={} diff_blocks={}",
                book.status.state,
                book.status.height,
                book.status.upstream_time,
                book.internal_message_tx.receiver_count(),
                book.buffered_bytes,
                book.retained_input_bytes(),
                book.orders.len(),
                book.diffs.len()
            );
            diagnostic = Instant::now();
        }
    }
}

pub(crate) struct L2Snapshots(HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>);
impl L2Snapshots {
    pub(crate) const fn as_ref(&self) -> &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>> {
        &self.0
    }
}
pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}
pub(crate) enum InternalMessage {
    Status(FeedStatus),
    Reset {
        generation: u64,
    },
    Snapshot {
        generation: u64,
        height: u64,
        l2_snapshots: L2Snapshots,
        time: u64,
        trace: Option<Trace>,
    },
    Fills {
        batch: Batch<NodeDataFill>,
        trace: Option<Trace>,
    },
    L4BookUpdates {
        generation: u64,
        diff_batch: Batch<NodeDataOrderDiff>,
        status_batch: Batch<NodeDataOrderStatus>,
        trace: Option<Trace>,
    },
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

pub(super) type L2Requests = HashMap<Coin, HashSet<L2SnapshotParams>>;
fn l2_requests<'a>(subscriptions: impl Iterator<Item = &'a Subscription> + 'a) -> L2Requests {
    let mut requested = L2Requests::new();
    for sub in subscriptions {
        if let Subscription::L2Book { coin, n_sig_figs, mantissa, .. } = sub {
            requested.entry(Coin::new(coin)).or_default().insert(L2SnapshotParams::new(*n_sig_figs, *mantissa));
        }
    }
    requested
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    fn listener(stream: bool) -> OrderBookListener {
        let (tx, _) = tokio::sync::broadcast::channel(100);
        let config = ServerConfig { stream_with_block_info: stream, ..ServerConfig::default() };
        OrderBookListener::new(tx, config)
    }
    fn snapshot() -> Snapshots<InnerL4Order> {
        Snapshots::new(HashMap::from([(Coin::new("BTC"), Snapshot::new([vec![], vec![]]))]))
    }
    fn batch(height: u64, events: Vec<Value>) -> String {
        static TIME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        let time = TIME.get_or_init(|| chrono::Utc::now().naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string());
        json!({"local_time":time,"block_time":time,"block_number":height,"events":events}).to_string()
    }
    fn status(oid: u64, px: &str) -> Value {
        json!({"time":"2026-01-01T00:00:00","user":"0x0000000000000000000000000000000000000001",
            "status":"open","order":{"coin":"BTC","side":"B","limitPx":px,"sz":"1","oid":oid,
            "timestamp":1,"triggerCondition":"N/A","isTrigger":false,"triggerPx":"0",
            "isPositionTpsl":false,"reduceOnly":false,"orderType":"Limit","tif":"Gtc","cloid":null}})
    }
    fn diff(oid: u64, px: &str, change: Value) -> Value {
        json!({"user":"0x0000000000000000000000000000000000000001","oid":oid,"coin":"BTC","px":px,"raw_book_diff":change})
    }
    fn empty_block(book: &mut OrderBookListener, h: u64) -> Result<()> {
        let line = batch(h, vec![]);
        book.ingest(0, &line)?;
        book.ingest(1, &line)
    }
    #[test]
    fn missed_block_gates_then_recovers_without_replacing_channel() {
        let mut book = listener(false);
        let mut rx = book.internal_message_tx.subscribe();
        book.install(snapshot(), 100).unwrap();
        assert!(!book.is_ready());
        empty_block(&mut book, 101).unwrap();
        assert!(book.is_ready());
        let err = empty_block(&mut book, 103).unwrap_err();
        book.recover(err, false);
        assert!(!book.is_ready());
        assert!(book.compute_snapshot().is_none());
        book.install(snapshot(), 103).unwrap();
        empty_block(&mut book, 104).unwrap();
        assert!(book.is_ready());
        assert_eq!(book.status.height, Some(104));
        let mut reset = 0;
        while let Ok(msg) = rx.try_recv() {
            if matches!(*msg, InternalMessage::Reset { .. }) {
                reset += 1;
            }
        }
        assert_eq!(reset, 2);
    }
    #[test]
    fn stream_fragments_wait_for_both_watermarks() {
        let mut book = listener(true);
        book.install(snapshot(), 100).unwrap();
        book.ingest(0, &batch(101, vec![status(1, "100")])).unwrap();
        book.ingest(1, &batch(101, vec![diff(1, "100", json!({"new":{"sz":"1"}}))])).unwrap();
        book.ingest(0, &batch(101, vec![status(2, "99")])).unwrap();
        book.ingest(1, &batch(101, vec![diff(2, "99", json!({"new":{"sz":"1"}}))])).unwrap();
        assert!(!book.is_ready());
        book.ingest(0, &batch(102, vec![])).unwrap();
        assert!(!book.is_ready());
        book.ingest(1, &batch(102, vec![])).unwrap();
        assert!(book.is_ready());
        let snap = book.compute_snapshot().unwrap();
        assert_eq!(snap.height, 101);
        assert_eq!(snap.snapshot.as_ref()[&Coin::new("BTC")].as_ref()[0].len(), 2);
    }
    #[test]
    fn duplicate_batch_and_regression_rejected() {
        let mut book = listener(false);
        book.ingest(0, &batch(10, vec![])).unwrap();
        assert!(book.ingest(0, &batch(10, vec![])).is_err());
        assert!(book.ingest(0, &batch(9, vec![])).is_err());
    }
    #[test]
    fn price_divergence_and_original_size_detected() {
        for (px, size) in [("101", "1"), ("100", "2")] {
            let mut book = listener(false);
            book.install(snapshot(), 100).unwrap();
            book.ingest(0, &batch(101, vec![status(1, "100")])).unwrap();
            book.ingest(1, &batch(101, vec![diff(1, "100", json!({"new":{"sz":"1"}}))])).unwrap();
            book.ingest(0, &batch(102, vec![])).unwrap();
            assert!(
                book.ingest(1, &batch(102, vec![diff(1, px, json!({"update":{"origSz":size,"newSz":"0.5"}}))]))
                    .is_err()
            );
        }
    }
    #[test]
    fn new_order_price_mismatch_rejected() {
        let mut book = listener(false);
        book.install(snapshot(), 100).unwrap();
        book.ingest(0, &batch(101, vec![status(1, "100")])).unwrap();
        assert!(book.ingest(1, &batch(101, vec![diff(1, "100.00000001", json!({"new":{"sz":"1"}}))])).is_err());
    }
    #[test]
    fn market_filter_ignores_unselected_orders_but_preserves_heights() {
        let mut book = listener(false);
        book.config.markets.insert("HYPE".into());
        book.install(snapshot(), 100).unwrap();
        book.ingest(0, &batch(101, vec![status(1, "100")])).unwrap();
        book.ingest(1, &batch(101, vec![diff(1, "100", json!({"new":{"sz":"1"}}))])).unwrap();
        assert!(book.is_ready());
        assert!(book.universe().is_empty());
    }
    #[test]
    fn bounded_cache_and_malformed_input() {
        let mut book = listener(false);
        book.config.max_buffer_bytes = 32;
        assert!(book.ingest(0, &batch(1, vec![])).is_err());
        assert!(book.ingest(1, "{").is_err());
    }
    #[test]
    fn retained_bytes_track_fragments_and_clear_after_recovery() {
        let mut book = listener(true);
        let line = batch(101, vec![]);
        book.ingest(0, &line).unwrap();
        book.ingest(0, &line).unwrap();
        book.ingest(1, &line).unwrap();
        book.ingest(2, &line).unwrap();
        assert_eq!(book.retained_input_bytes(), 4 * line.len());
        // Internal accounting and timing must not leak into the node schema.
        let encoded = serde_json::to_value(book.orders.get(&101).unwrap()).unwrap();
        assert!(encoded.get("trace").is_none());
        assert!(encoded.get("input_bytes").is_none());
        book.recover("test recovery", false);
        assert_eq!(book.retained_input_bytes(), 0);
    }
    #[test]
    fn stale_event_timestamp_not_published() {
        let mut book = listener(false);
        book.install(snapshot(), 100).unwrap();
        let mut line: Value = serde_json::from_str(&batch(101, vec![])).unwrap();
        line["block_time"] = json!("2020-01-01T00:00:00");
        book.ingest(0, &line.to_string()).unwrap();
        let err = book.ingest(1, &line.to_string()).unwrap_err();
        book.recover(err, true);
        assert_eq!(book.status.state, Health::Stale);
        assert!(
            book.current_l2(&Subscription::L2Book {
                coin: "BTC".into(),
                n_sig_figs: None,
                mantissa: None,
                n_levels: None
            })
            .is_none()
        );
    }
    #[test]
    fn replay_ignores_blocks_at_or_before_snapshot() {
        let mut book = listener(false);
        empty_block(&mut book, 99).unwrap();
        empty_block(&mut book, 100).unwrap();
        empty_block(&mut book, 101).unwrap();
        book.install(snapshot(), 100).unwrap();
        assert_eq!(book.status.height, Some(101));
        assert!(book.is_ready());
    }
    #[test]
    fn integrity_mismatch_rebuilds_and_counts_failure() {
        let mut book = listener(false);
        book.install(snapshot(), 100).unwrap();
        book.ingest(0, &batch(101, vec![status(1, "100")])).unwrap();
        book.ingest(1, &batch(101, vec![diff(1, "100", json!({"new":{"sz":"1"}}))])).unwrap();
        book.checkpoint = book.state.take();
        book.install(snapshot(), 101).unwrap();
        assert_eq!(book.status.validation_failures, 1);
        assert!(!book.is_ready());
        empty_block(&mut book, 102).unwrap();
        assert!(book.is_ready());
    }
}
