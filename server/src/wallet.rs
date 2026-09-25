//! Local-only wallet journal with independent, resumable node-file readers.
//! Parsing and persistence run on a dedicated worker, never under the book lock.
mod accounts;
pub(crate) use accounts::Sample as AccountSample;
mod aggregate;
mod gate;
mod orders;
use gate::{Gate, Reason as GateReason};
mod storage;
use crate::ServerConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::watch;

const MAX_BYTES: usize = 2 * 1024 * 1024;
const MAX_EVENTS: usize = 2000;
const INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 64 * 1024;

pub(crate) fn valid_address(s: &str) -> bool {
    s.len() == 42 && s.starts_with("0x") && s[2..].bytes().all(|c| c.is_ascii_hexdigit())
}
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum WalletSubscription {
    #[serde(rename_all = "camelCase")]
    UserFills {
        user: String,
        #[serde(default)]
        aggregate_by_time: bool,
    },
    OrderUpdates {
        user: String,
    },
    AllDexsClearinghouseState {
        user: String,
    },
    #[serde(rename_all = "camelCase")]
    SpotState {
        user: String,
        #[serde(default)]
        ignore_portfolio_margin: bool,
    },
    OpenOrders {
        user: String,
        #[serde(default)]
        dex: String,
    },
}
impl WalletSubscription {
    pub(crate) fn user(&self) -> &str {
        match self {
            Self::UserFills { user, .. }
            | Self::OrderUpdates { user }
            | Self::OpenOrders { user, .. }
            | Self::AllDexsClearinghouseState { user }
            | Self::SpotState { user, .. } => user,
        }
    }
    pub(crate) fn normalize(&mut self) {
        match self {
            Self::UserFills { user, .. }
            | Self::OrderUpdates { user }
            | Self::OpenOrders { user, .. }
            | Self::AllDexsClearinghouseState { user }
            | Self::SpotState { user, .. } => user.make_ascii_lowercase(),
        }
    }
    pub(crate) fn account(&self) -> bool {
        matches!(self, Self::AllDexsClearinghouseState { .. } | Self::SpotState { .. })
    }
    pub(crate) fn channel(&self) -> &'static str {
        match self {
            Self::UserFills { .. } => "userFills",
            Self::OrderUpdates { .. } => "orderUpdates",
            Self::OpenOrders { .. } => "openOrders",
            Self::AllDexsClearinghouseState { .. } => "allDexsClearinghouseState",
            Self::SpotState { .. } => "spotState",
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Event {
    seq: u64,
    user: String,
    channel: String,
    data: Value,
    key: String,
    bytes: usize,
    #[serde(default)]
    height: u64,
}
#[derive(Default)]
struct Source {
    height: Option<u64>,
    block_time: Option<i64>,
    seen: Option<Instant>,
}
struct State {
    session_started_at: i64,
    epoch: u64,
    seq: u64,
    removed_through: u64,
    events: VecDeque<Event>,
    keys: HashSet<String>,
    bytes: usize,
    sources: [Source; 2],
    ready: bool,
    history_current: bool,
    history_gate: Gate,
    reason: String,
    journal_error: Option<String>,
    gaps: u64,
    recovering_gap: bool,
    pending: Vec<Event>,
    coverage_start: i64,
    replaying: bool,
    dirty: HashMap<(String, String), u64>,
    source_height: u64,
    aggregation_floor: HashMap<String, u64>,
}
#[derive(Serialize, Deserialize)]
struct Journal {
    schema: u32,
    seq: u64,
    events: VecDeque<Event>,
}
impl State {
    fn new() -> Self {
        Self {
            session_started_at: chrono::Utc::now().timestamp_millis(),
            epoch: 0,
            seq: 0,
            removed_through: 0,
            events: VecDeque::new(),
            keys: HashSet::new(),
            bytes: 0,
            sources: [Source::default(), Source::default()],
            ready: false,
            history_current: false,
            history_gate: Gate::new(Instant::now()),
            reason: "startup: loading retained history and resuming input cursors".into(),
            journal_error: None,
            gaps: 0,
            recovering_gap: false,
            pending: Vec::new(),
            coverage_start: chrono::Utc::now().timestamp_millis(),
            replaying: true,
            dirty: HashMap::new(),
            source_height: 0,
            aggregation_floor: HashMap::new(),
        }
    }
    // Readiness gates publication; epoch identifies continuity, not scheduling.
    // Catching up retained input or waiting for fresh upstream data preserves
    // cursors. Actual gaps and persistence failures invalidate continuity below.
    fn set_history_gate(&mut self, reason: GateReason) {
        self.history_current = reason == GateReason::Current;
        self.history_gate.set(reason, Instant::now());
    }
    // The persisted gap counter describes history, not the cause of this wait.
    fn missing_source_reason(&self) -> GateReason {
        if self.recovering_gap { GateReason::Gap } else { GateReason::Initializing }
    }
    fn sources_fresh(&self, stale_after: Duration) -> bool {
        let now = chrono::Utc::now().timestamp_millis();
        self.sources
            .iter()
            .all(|source| source.block_time.is_some_and(|t| now.saturating_sub(t) <= stale_after.as_millis() as i64))
    }
    fn can_publish(&self, at_tip: bool, backlog_age: Duration, fresh: bool, persistence_failed: bool) -> bool {
        !persistence_failed && fresh && (at_tip || (self.ready && backlog_age < Duration::from_millis(100)))
    }
    fn set_ready(&mut self, ready: bool) -> bool {
        if ready {
            self.recovering_gap = false;
        }
        if ready == self.ready && !(ready && self.replaying) {
            return false;
        }
        self.ready = ready;
        if !ready && self.history_current {
            self.set_history_gate(GateReason::StaleInput);
        }
        self.replaying = !ready;
        self.reason = if ready {
            "retained source replay caught up; live local streams ready"
        } else {
            "wallet replay or stale upstream; publishing paused"
        }
        .into();
        true
    }
    fn gap(&mut self, reason: String) {
        self.epoch += 1;
        self.gaps += 1;
        self.recovering_gap = true;
        self.ready = false;
        self.set_history_gate(GateReason::Gap);
        self.sources = [Source::default(), Source::default()];
        self.reason = reason;
    }
    fn push(&mut self, user: String, channel: &str, mut data: Value, key: String) {
        if !self.keys.insert(key.clone()) {
            return;
        }
        self.seq += 1;
        if channel == "orderUpdates" {
            data["_localGapCount"] = json!(self.gaps);
        }
        let bytes = data.to_string().len() + key.len() + user.len() + 128;
        self.bytes += bytes;
        let coin = data["coin"].as_str().or_else(|| data["order"]["coin"].as_str()).unwrap_or("");
        let dex = coin.split_once(':').map_or("", |x| x.0).to_string();
        self.dirty.insert((user.clone(), dex), self.seq);
        let event =
            Event { seq: self.seq, user, channel: channel.into(), data, key, bytes, height: self.source_height };
        self.pending.push(event.clone());
        self.events.push_back(event);
        while self.events.len() > MAX_EVENTS || self.bytes > MAX_BYTES {
            if let Some(e) = self.events.pop_front() {
                if e.channel == "userFills" {
                    self.aggregation_floor
                        .entry(e.user.clone())
                        .and_modify(|h| *h = (*h).max(e.height))
                        .or_insert(e.height);
                }
                self.bytes -= e.bytes;
                self.keys.remove(&e.key);
                self.removed_through = e.seq;
            }
        }
    }
    fn status(&self) -> Value {
        json!({"source":"localNode","state":if self.ready {"Ready"} else {"Stale"},
            "generation":self.epoch,"reason":self.reason,"gaps":self.gaps,"sessionStartedAt":self.session_started_at,
            "historyComplete":false,"historyScope":"retained local observations; see localWalletHistory for disk history and gaps",
            "coverageStartTime":self.coverage_start,"replaying":self.replaying,"historyCurrent":self.history_current,
            "oldestRetainedSequence":self.events.front().map(|e|e.seq),"latestSequence":self.seq,
            "retainedEvents":self.events.len(),"retainedBytes":self.bytes,"journalError":self.journal_error,
            "orderHeight":self.sources[0].height,"fillHeight":self.sources[1].height,
            "orderTime":self.sources[0].block_time,"fillTime":self.sources[1].block_time})
    }
}
#[derive(Default)]
struct OrdersCache {
    epoch: u64,
    fetched: Option<Instant>,
    response: Value,
    dirty: u64,
}
type OrderCaches = Arc<Mutex<HashMap<(String, String), Arc<tokio::sync::Mutex<OrdersCache>>>>>;
#[derive(Clone)]
pub(crate) struct WalletHub {
    state: Arc<Mutex<State>>,
    allowed: Arc<HashSet<String>>,
    running: Arc<std::sync::atomic::AtomicBool>,
    db_path: PathBuf,
    streamed: bool,
    metrics: Arc<crate::telemetry::Metrics>,
    order_caches: OrderCaches,
    signal: watch::Sender<u64>,
    pub(crate) accounts: Arc<accounts::Accounts>,
    pub(crate) poll_interval: Duration,
    pub(crate) event_interval: Duration,
}
impl WalletHub {
    pub(crate) fn new(config: &ServerConfig) -> crate::Result<Self> {
        let allowed: HashSet<_> = config.wallets.iter().map(|w| w.to_ascii_lowercase()).collect();
        let legacy =
            config.wallet_journal_path.clone().unwrap_or_else(|| config.data_dir.join("ws-wallet-journal.json"));
        let path = if legacy.extension().is_some_and(|s| s == "json") {
            legacy.with_extension("sqlite")
        } else {
            legacy.clone()
        };
        if path == config.snapshot_path {
            return Err("wallet database and book snapshot paths must differ".into());
        }
        let mut state = State::new();
        let mut store = None;
        let mut checkpoints: [storage::Checkpoint; 2] = Default::default();
        let mut gaps = Vec::new();
        if !allowed.is_empty() {
            std::fs::create_dir_all(path.parent().ok_or("wallet database needs parent directory")?)?;
            let db = storage::Store::open(&path)?;
            checkpoints = db.meta("checkpoints")?.unwrap_or_default();
            let saved_seq = db.meta("seq")?;
            state.seq = saved_seq.unwrap_or_default();
            if saved_seq.is_some() && checkpoints.iter().any(|c| c.cursor.is_none()) {
                gaps.push("restart without a persisted source-file cursor; resumed at a fresh live boundary".into());
            }
            state.gaps = db.meta("gap_count")?.unwrap_or_default();
            state.coverage_start = db.meta("coverage")?.unwrap_or(state.coverage_start);
            if let Some(old) = db.meta::<HashSet<String>>("wallets")? {
                if old != allowed {
                    gaps.push("wallet allowlist changed; older events for new wallets were not indexed".into());
                }
            }
            for event in db.recent()? {
                if allowed.contains(&event.user) {
                    state.bytes += event.bytes;
                    state.keys.insert(event.key.clone());
                    state.events.push_back(event);
                }
            }
            while state.bytes > MAX_BYTES {
                if let Some(e) = state.events.pop_front() {
                    state.bytes -= e.bytes;
                    state.keys.remove(&e.key);
                } else {
                    break;
                }
            }
            state.removed_through = state.events.front().map_or(state.seq, |e| e.seq.saturating_sub(1));
            if state.removed_through > 0 {
                for e in &state.events {
                    if e.channel == "userFills" {
                        state.aggregation_floor.entry(e.user.clone()).or_insert(e.height);
                    }
                }
            }
            if state.seq == 0 && legacy != path && legacy.exists() {
                if std::fs::metadata(&legacy)?.len() > 6 * 1024 * 1024 {
                    return Err("legacy wallet journal exceeds limit".into());
                }
                let journal: Journal = serde_json::from_slice(&std::fs::read(&legacy)?)?;
                if journal.schema != 1 {
                    return Err("unsupported legacy wallet journal".into());
                }
                for e in journal.events {
                    if allowed.contains(&e.user) {
                        state.push(e.user, &e.channel, e.data, e.key);
                    }
                }
                gaps.push("imported legacy wallet journal; pre-migration source cursors unavailable".into());
            }
            for (source, checkpoint) in state.sources.iter_mut().zip(&checkpoints) {
                source.height = checkpoint.height;
                source.block_time = checkpoint.time;
            }
            store = Some(db);
        }
        let (signal, _) = watch::channel(0);
        let state = Arc::new(Mutex::new(state));
        let allowed = Arc::new(allowed);
        let metrics = Arc::new(crate::telemetry::Metrics::default());
        let running = Arc::new(std::sync::atomic::AtomicBool::new(!allowed.is_empty()));
        let hub = Self {
            state: state.clone(),
            allowed: allowed.clone(),
            running: running.clone(),
            db_path: path,
            streamed: config.stream_with_block_info,
            metrics: metrics.clone(),
            order_caches: Arc::new(Mutex::new(HashMap::new())),
            signal: signal.clone(),
            accounts: Arc::new(accounts::Accounts::new(config.wallet_account_interval)),
            poll_interval: config.wallet_poll_interval,
            event_interval: config.wallet_event_interval,
        };
        if let Some(store) = store {
            let config = config.clone();
            let weak = Arc::downgrade(&running);
            std::thread::Builder::new().name("wallet-reader".into()).spawn(move || {
                run_worker(config, state, allowed, metrics, signal, weak, store, checkpoints, gaps);
            })?;
        }
        Ok(hub)
    }
    /// Single-flight, shared per-wallet/dex cache: extra clients do not multiply node queries.
    pub(crate) async fn open_orders(
        &self,
        bridge: crate::servers::info::InfoBridge,
        user: String,
        dex: String,
        epoch: u64,
    ) -> Value {
        let key = (user.clone(), dex.clone());
        let cache = {
            let mut caches = self.order_caches.lock().unwrap_or_else(|e| e.into_inner());
            if !caches.contains_key(&key) && caches.len() >= 256 {
                return json!({"type":"error","payload":"429: maximum 256 distinct wallet/dex query keys per process"});
            }
            caches.entry(key).or_default().clone()
        };
        let mut cache = cache.lock().await;
        if cache.response["type"] == "info"
            && cache.epoch == epoch
            && cache.dirty == self.dirty_version(&user, &dex)
            && cache.fetched.is_some_and(|t| t.elapsed() < self.poll_interval.min(Duration::from_secs(5)))
        {
            return cache.response.clone();
        }
        if let Some(t) = cache.fetched {
            tokio::time::sleep(self.event_interval.saturating_sub(t.elapsed())).await;
        }
        let dirty = self.dirty_version(&user, &dex);
        let start = Instant::now();
        let sampled_at = chrono::Utc::now().timestamp_millis();
        let mut response = bridge
            .execute(crate::servers::info::PostRequest {
                id: 0,
                request: json!({"type":"info",
            "payload":{"type":"frontendOpenOrders","user":user,"dex":dex}}),
            })
            .await
            .response;
        response["_localSampleStartedAt"] = json!(sampled_at);
        response["_localSampleCompletedAt"] = json!(chrono::Utc::now().timestamp_millis());
        self.metrics.elapsed("open_orders_query_us", start);
        cache.dirty = dirty;
        cache.epoch = epoch;
        cache.fetched = Some(Instant::now());
        cache.response = response.clone();
        response
    }
    pub(crate) async fn sample_account(
        &self,
        bridge: crate::servers::info::InfoBridge,
        sub: WalletSubscription,
        epoch: u64,
    ) -> Result<AccountSample, String> {
        let started = Instant::now();
        let result = self.accounts.sample(bridge, sub, epoch).await;
        self.metrics.elapsed("account_sample_us", started);
        result
    }
    pub(crate) fn dirty_version(&self, user: &str, dex: &str) -> u64 {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()).dirty.get(&(user.into(), dex.into())).unwrap_or(&0)
    }
    pub(crate) async fn history(&self, payload: Value) -> Result<Value, String> {
        let user = payload["user"].as_str().ok_or("missing user")?.to_ascii_lowercase();
        if !self.allowed.contains(&user) {
            return Err("wallet not configured".into());
        }
        let after = payload.get("afterSequence").map_or(Ok(0), |v| v.as_u64().ok_or("invalid afterSequence"))?;
        let limit = payload.get("limit").map_or(Ok(100), |v| v.as_u64().ok_or("invalid limit"))?;
        if limit == 0 || limit > 1000 || after > i64::MAX as u64 {
            return Err("limit must be 1..1000; sequence must fit signed 64 bits".into());
        }
        let path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            storage::history(&path, &user, after, limit as usize).map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())?
    }
    pub(crate) fn enabled(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
    pub(crate) fn subscribe_signal(&self) -> watch::Receiver<u64> {
        self.signal.subscribe()
    }
    pub(crate) fn validate(&self, sub: &WalletSubscription) -> Result<(), String> {
        if !self.allowed.contains(sub.user()) {
            return Err("wallet not enabled; configure --wallets with this address".into());
        }
        if matches!(sub, WalletSubscription::SpotState { ignore_portfolio_margin: true, .. }) {
            return Err("ignorePortfolioMargin:true is not supported by the local account adapter".into());
        }
        if let WalletSubscription::OpenOrders { dex, .. } = sub {
            if dex.len() > 64 || !dex.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-') {
                return Err("invalid dex name".into());
            }
        }
        Ok(())
    }
    pub(crate) fn status(&self) -> Value {
        self.status_inner(false)
    }
    pub(crate) fn diagnostics(&self) -> Value {
        self.status_inner(true)
    }
    fn status_inner(&self, include_gate: bool) -> Value {
        let mut status = {
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            let mut status = state.status();
            if include_gate {
                status["historyGate"] = state.history_gate.snapshot(Instant::now());
            }
            status
        };
        status["enabled"] = json!(self.enabled());
        status["configuredWallets"] = json!(self.allowed.len());
        status["storage"] = json!("SQLite WAL with transactional source cursors");
        status["metrics"] = self.metrics.snapshot();
        status
    }
    // Snapshot and cursor are captured under one lock to avoid subscribe/live races.
    pub(crate) fn read(
        &self,
        sub: &WalletSubscription,
        cursor: Option<(u64, u64)>,
    ) -> (Value, Vec<Value>, (u64, u64), bool) {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let reset = cursor.is_none_or(|(epoch, seq)| epoch != s.epoch || seq < s.removed_through);
        let mut status = s.status();
        status["user"] = json!(sub.user());
        status["subscription"] = serde_json::to_value(sub).unwrap_or_default();
        status["resetRequired"] = json!(reset);
        let aggregated = matches!(sub, WalletSubscription::UserFills { aggregate_by_time: true, .. });
        let complete = s.sources[1].height.unwrap_or(0).saturating_sub(u64::from(self.streamed));
        let next_seq = if aggregated {
            s.events
                .iter()
                .find(|e| e.user == sub.user() && e.channel == "userFills" && e.height > complete)
                .map_or(s.seq, |e| e.seq.saturating_sub(1))
        } else {
            s.seq
        };
        let messages = if !s.ready {
            Vec::new()
        } else if sub.channel() == "userFills" {
            let events: Vec<_> = s
                .events
                .iter()
                .filter(|e| {
                    e.user == sub.user()
                        && e.channel == "userFills"
                        && (reset || cursor.is_some_and(|(_, seq)| e.seq > seq))
                })
                .map(|e| e.data.clone())
                .collect();
            let events = if aggregated
                && !reset
                && !s.events.iter().any(|e| {
                    e.user == sub.user()
                        && e.channel == "userFills"
                        && e.height <= complete
                        && cursor.is_some_and(|(_, seq)| e.seq > seq)
                }) {
                vec![]
            } else if aggregated {
                match aggregate::fills(
                    s.events.iter().filter(|e| {
                        e.user == sub.user()
                            && e.channel == "userFills"
                            && s.aggregation_floor.get(&e.user).is_none_or(|floor| e.height > *floor)
                    }),
                    if reset { None } else { cursor.map(|(_, seq)| seq) },
                    complete,
                ) {
                    Ok(events) => events,
                    Err(reason) => {
                        status["state"] = json!("Stale");
                        status["reason"] = json!(reason);
                        return (status, vec![], cursor.unwrap_or((s.epoch, 0)), true);
                    }
                }
            } else {
                events
            };
            if reset || !events.is_empty() {
                vec![json!({"channel":"userFills","data":{"user":sub.user(),"isSnapshot":reset,"fills":events}})]
            } else {
                vec![]
            }
        } else if sub.channel() == "orderUpdates" && !reset {
            let events: Vec<_> = s
                .events
                .iter()
                .filter(|e| {
                    e.user == sub.user() && e.channel == "orderUpdates" && cursor.is_some_and(|(_, seq)| e.seq > seq)
                })
                .map(|e| orders::basic_update(&e.data))
                .collect();
            if events.is_empty() { vec![] } else { vec![json!({"channel":"orderUpdates","data":events})] }
        } else {
            vec![]
        };
        (status, messages, (s.epoch, next_seq), reset)
    }
}
fn source_read_order(orders: Option<u64>, fills: Option<u64>) -> [usize; 2] {
    if orders.zip(fills).is_some_and(|(o, f)| o > f) { [1, 0] } else { [0, 1] }
}
fn reader_delay(poll: Duration, caught_up: bool, aligned: bool, persistence_failed: bool) -> Duration {
    if !persistence_failed && (!caught_up || !aligned) { poll.min(Duration::from_millis(1)) } else { poll }
}
fn run_worker(
    config: ServerConfig,
    state: Arc<Mutex<State>>,
    allowed: Arc<HashSet<String>>,
    metrics: Arc<crate::telemetry::Metrics>,
    signal: watch::Sender<u64>,
    running: std::sync::Weak<std::sync::atomic::AtomicBool>,
    mut store: storage::Store,
    mut checkpoints: [storage::Checkpoint; 2],
    mut gaps: Vec<String>,
) {
    let dirs = [
        config.order_status_dir.clone().unwrap_or_else(|| config.data_dir.join("node_order_statuses_by_block")),
        config.fills_dir.clone().unwrap_or_else(|| config.data_dir.join("node_fills_by_block")),
    ];
    let mut readers: [Option<storage::Reader>; 2] = [None, None];
    for i in 0..2 {
        match storage::Reader::open(dirs[i].clone(), checkpoints[i].cursor.clone()) {
            Ok(reader) => {
                checkpoints[i].cursor = reader.cursor.clone();
                readers[i] = Some(reader)
            }
            Err(e) => {
                gaps.push(format!("wallet source {i} cannot resume retained file: {e}"));
                checkpoints[i] = Default::default();
                state.lock().unwrap_or_else(|e| e.into_inner()).sources[i] = Source::default();
                readers[i] = storage::Reader::open(dirs[i].clone(), None).ok();
            }
        }
    }
    if !gaps.is_empty() {
        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
        s.gaps += gaps.len() as u64;
        s.recovering_gap = true;
        s.reason = gaps.join("; ");
    }
    // Establish the initial EOF anchors before publishing any live record. A crash
    // before the next checkpoint can then replay those records from these anchors.
    let (pending, seq, coverage) = {
        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
        s.set_history_gate(GateReason::Commit);
        (std::mem::take(&mut s.pending), s.seq, s.coverage_start)
    };
    let initial = store.commit(&pending, &checkpoints, seq, &gaps, &config, coverage);
    let mut persistence_failed = initial.is_err();
    match initial {
        Ok(()) => {
            gaps.clear();
            state.lock().unwrap_or_else(|e| e.into_inner()).set_history_gate(GateReason::AwaitingRecheck);
        }
        Err(error) => {
            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
            s.pending = pending;
            s.journal_error = Some(error.to_string());
            s.set_history_gate(GateReason::Persistence);
            s.reason = "initial wallet checkpoint blocked; retrying before ingestion".into();
        }
    }
    let mut last_commit = Instant::now();
    let mut last_discovery = Instant::now();
    let mut at_tip = [false; 2];
    let mut backlog_since: Option<Instant> = None;
    loop {
        if running.upgrade().is_none() {
            break;
        }
        // Publication may tolerate brief backlog, but history-derived answers
        // must never use an open record ahead of the fill reader in this pass.
        let read_order = {
            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
            s.set_history_gate(if persistence_failed { GateReason::Persistence } else { GateReason::InputPass });
            source_read_order(s.sources[0].height, s.sources[1].height)
        };
        if !persistence_failed {
            for i in read_order {
                if readers[i].is_none() && last_discovery.elapsed() >= Duration::from_secs(1) {
                    readers[i] = storage::Reader::open(dirs[i].clone(), None).ok();
                }
                let Some(reader) = readers[i].as_mut() else {
                    at_tip[i] = false;
                    continue;
                };
                let mut bytes = 0;
                at_tip[i] = false;
                for _ in 0..128 {
                    let line = match reader.next() {
                        Ok(Some(line)) => line,
                        Ok(None) => {
                            checkpoints[i].cursor = reader.cursor.clone();
                            at_tip[i] = reader.at_tip().unwrap_or(false);
                            break;
                        }
                        Err(error) => {
                            let reason = format!("wallet source {i} file continuity lost: {error}");
                            log::warn!("{reason}");
                            gaps.push(reason.clone());
                            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
                            s.gap(reason);
                            s.replaying = true;
                            signal.send_modify(|n| *n += 1);
                            drop(s);
                            readers[i] = None;
                            checkpoints[i] = Default::default();
                            break;
                        }
                    };
                    bytes += line.len();
                    checkpoints[i].cursor = reader.cursor.clone();
                    if line.trim().is_empty() {
                        continue;
                    }
                    let start = Instant::now();
                    let decoded = decode(i, &line, &allowed);
                    metrics.elapsed("decode_us", start);
                    let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
                    let previous_height = s.sources[i].height;
                    // Historical records are valid replay inputs. Readiness is decided only after
                    // both readers reach current output, not by the age of each replayed record.
                    let was_ready = s.ready;
                    let was_replaying = s.replaying;
                    s.replaying = true;
                    match decoded.and_then(|d| apply(&mut s, i, d, config.stream_with_block_info, config.stale_after)) {
                        Ok(changed) => {
                            s.ready = was_ready;
                            s.replaying = was_replaying;
                            // Enforce age before notifying subscribers about each batch,
                            // not only after the whole bounded read pass completes.
                            if !s.sources_fresh(config.stale_after) {
                                let reason = if s.sources.iter().any(|source| source.height.is_none()) {
                                    s.missing_source_reason()
                                } else {
                                    GateReason::StaleInput
                                };
                                s.set_history_gate(reason);
                                if s.set_ready(false) {
                                    signal.send_modify(|n| *n += 1);
                                }
                            }
                            checkpoints[i].height = s.sources[i].height;
                            checkpoints[i].time = s.sources[i].block_time;
                            if changed || (i == 1 && previous_height != s.sources[i].height) {
                                signal.send_modify(|n| *n += 1);
                            }
                        }
                        Err(reason) => {
                            log::warn!("wallet gap: {reason}");
                            gaps.push(reason.clone());
                            s.gap(reason);
                            s.replaying = true;
                            // This record cannot be reconstructed reliably. Its cursor and the gap
                            // are committed together; never get stuck replaying it forever.
                            checkpoints[i].height = None;
                            checkpoints[i].time = None;
                            signal.send_modify(|n| *n += 1);
                        }
                    }
                    if bytes >= 1024 * 1024 {
                        break;
                    }
                }
                // Hitting a byte/record budget does not imply unread input. Large
                // complete records routinely consume the entire budget at EOF.
                // Keep genuine backlog/partial records gated; errors are handled
                // by the reader's continuity path on the next iteration.
                if !at_tip[i] {
                    if let Some(reader) = readers[i].as_mut() {
                        at_tip[i] = reader.at_tip().unwrap_or(false);
                    }
                }
            }
        }
        if last_discovery.elapsed() >= Duration::from_secs(1) {
            last_discovery = Instant::now();
        }
        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
        let caught_up = at_tip.iter().all(|v| *v);
        let backlog_age = if caught_up {
            backlog_since = None;
            Duration::ZERO
        } else {
            backlog_since.get_or_insert_with(Instant::now).elapsed()
        };
        metrics.observe("backlog_age_us", backlog_age.as_secs_f64() * 1e6);
        let fresh = s.sources_fresh(config.stale_after);
        let ready = s.can_publish(caught_up, backlog_age, fresh, persistence_failed);
        let aligned = s.sources[0].height.is_some() && s.sources[0].height == s.sources[1].height;
        let gate = if persistence_failed {
            GateReason::Persistence
        } else if s.sources.iter().any(|source| source.height.is_none()) {
            s.missing_source_reason()
        } else if !fresh {
            GateReason::StaleInput
        } else if !caught_up {
            GateReason::Backlog
        } else if !aligned {
            GateReason::HeightSkew
        } else if ready {
            GateReason::Current
        } else {
            GateReason::Backlog
        };
        s.set_history_gate(gate);
        if s.set_ready(ready) {
            signal.send_modify(|n| *n += 1);
        }
        if last_commit.elapsed() >= Duration::from_secs(1) || (!persistence_failed && s.pending.len() >= 256) {
            // pending leaves State while SQLite commits. Do not allow a lookup
            // to mistake that temporary absence for fully persisted history.
            s.set_history_gate(GateReason::Commit);
            let pending = std::mem::take(&mut s.pending);
            let seq = s.seq;
            let coverage = s.coverage_start;
            drop(s);
            let start = Instant::now();
            let result = store.commit(&pending, &checkpoints, seq, &gaps, &config, coverage);
            metrics.elapsed("persist_us", start);
            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
            match result {
                Ok(()) => {
                    if s.journal_error.take().is_some() {
                        signal.send_modify(|n| *n += 1);
                    }
                    persistence_failed = false;
                    gaps.clear();
                    s.set_history_gate(GateReason::AwaitingRecheck);
                }
                Err(error) => {
                    let reason = error.to_string();
                    if s.journal_error.as_ref() != Some(&reason) {
                        log::warn!("wallet transaction failed: {reason}");
                        signal.send_modify(|n| *n += 1);
                    }
                    s.pending = pending;
                    s.journal_error = Some(reason);
                    if s.ready {
                        s.epoch += 1;
                    }
                    s.ready = false;
                    s.set_history_gate(GateReason::Persistence);
                    s.reason = "wallet persistence blocked; source cursors retained for retry".into();
                    persistence_failed = true;
                }
            }
            last_commit = Instant::now();
        } else {
            drop(s);
        }
        // Catch-up remains byte/record bounded, but avoid adding the idle polling
        // delay on every backlog chunk. This worker is independent of the book loop.
        let delay = reader_delay(config.poll_interval, caught_up, aligned, persistence_failed);
        std::thread::sleep(delay);
    }
    // Graceful worker teardown when used by tests/embedders. Abrupt process exits
    // recover uncommitted records from the last transactional cursor instead.
}

#[cfg(test)]
fn save(path: &PathBuf, journal: &Journal) -> crate::Result<()> {
    use std::io::Write;
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = std::fs::File::create(&temp)?;
    serde_json::to_writer(&mut file, journal)?;
    file.flush()?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;
    Ok(())
}
#[derive(Deserialize)]
struct RawBatch<E> {
    block_number: u64,
    block_time: chrono::NaiveDateTime,
    events: Vec<E>,
}
struct Decoded {
    height: u64,
    time: i64,
    events: Vec<(String, &'static str, Value, String)>,
}
fn selected_user(user: &str, allowed: &HashSet<String>) -> Option<String> {
    if allowed.contains(user) {
        return Some(user.into());
    }
    if user.bytes().any(|b| b.is_ascii_uppercase()) {
        let lower = user.to_ascii_lowercase();
        if allowed.contains(&lower) {
            return Some(lower);
        }
    }
    None
}
fn decode(source: usize, line: &str, allowed: &HashSet<String>) -> Result<Decoded, String> {
    let mut selected = Vec::new();
    let (height, time) = if source == 1 {
        let batch: RawBatch<(&str, &RawValue)> =
            serde_json::from_str(line).map_err(|e| format!("malformed fill batch: {e}"))?;
        for (user, payload) in batch.events {
            let Some(user) = selected_user(user, allowed) else { continue };
            if payload.get().len() > MAX_EVENT_BYTES {
                return Err("wallet event too large".into());
            }
            let value: Value = serde_json::from_str(payload.get()).map_err(|_| "invalid fill event")?;
            selected.push((user, value));
        }
        (batch.block_number, batch.block_time.and_utc().timestamp_millis())
    } else {
        #[derive(Deserialize)]
        struct Envelope<'a> {
            user: &'a str,
            time: &'a str,
            status: &'a str,
            #[serde(borrow)]
            order: &'a RawValue,
        }
        let batch: RawBatch<Envelope<'_>> =
            serde_json::from_str(line).map_err(|e| format!("malformed order batch: {e}"))?;
        for e in batch.events {
            let Some(user) = selected_user(e.user, allowed) else { continue };
            if e.order.get().len() + e.time.len() + e.status.len() > MAX_EVENT_BYTES {
                return Err("wallet event too large".into());
            }
            let order: Value = serde_json::from_str(e.order.get()).map_err(|_| "invalid order event")?;
            selected.push((user, json!({"order":order,"status":e.status,"time":e.time})));
        }
        (batch.block_number, batch.block_time.and_utc().timestamp_millis())
    };
    let mut events = Vec::new();
    for (user, value) in selected {
        let (channel, data, key) = if source == 1 {
            for field in ["coin", "px", "sz", "side", "startPosition", "dir", "closedPnl", "hash", "fee", "feeToken"] {
                if !value[field].is_string() {
                    return Err(format!("fill missing {field}"));
                }
            }
            for field in ["oid", "tid", "time"] {
                if !value[field].is_u64() {
                    return Err(format!("fill missing {field}"));
                }
            }
            if !value["crossed"].is_boolean() || !matches!(value["side"].as_str(), Some("A" | "B")) {
                return Err("invalid fill side/crossed".into());
            }
            let key = format!(
                "f:{user}:{}:{}:{}:{}:{}",
                value["coin"], value["time"], value["tid"], value["oid"], value["side"]
            );
            ("userFills", value, key)
        } else {
            let order = &value["order"];
            for field in ["coin", "side", "limitPx", "sz", "origSz"] {
                if !order[field].is_string() {
                    return Err(format!("order missing {field}; no guessed original size"));
                }
            }
            for field in ["oid", "timestamp"] {
                if !order[field].is_u64() {
                    return Err(format!("order missing {field}"));
                }
            }
            if !value["status"].is_string() {
                return Err("order status missing".into());
            }
            let time: chrono::NaiveDateTime =
                serde_json::from_value(value["time"].clone()).map_err(|_| "invalid order time")?;
            let data =
                json!({"order":order,"status":value["status"],"statusTimestamp":time.and_utc().timestamp_millis()});
            let key = format!("o:{user}:{data}");
            ("orderUpdates", data, key)
        };
        events.push((user, channel, data, key));
    }
    Ok(Decoded { height, time, events })
}
fn apply(s: &mut State, source: usize, batch: Decoded, streamed: bool, stale: Duration) -> Result<bool, String> {
    let previous = &s.sources[source];
    if let Some(h) = previous.height {
        if batch.height < h
            || (!streamed && h.checked_add(1) != Some(batch.height))
            || (batch.height == h && previous.block_time != Some(batch.time))
        {
            return Err(format!("wallet source {source} discontinuity: previous {h}, received {}", batch.height));
        }
    }
    if !s.replaying
        && chrono::Utc::now().timestamp_millis().saturating_sub(batch.time)
            > stale.as_millis().min(i64::MAX as u128) as i64
    {
        return Err("wallet upstream event time stale".into());
    }
    let mut batch_keys = HashMap::new();
    for (_, _, data, key) in &batch.events {
        if batch_keys.insert(key, data).is_some_and(|old| old != data)
            || (s.keys.contains(key) && s.events.iter().any(|e| &e.key == key && !orders::same_record(&e.data, data)))
        {
            return Err("conflicting duplicate wallet event".into());
        }
    }
    s.sources[source] = Source { height: Some(batch.height), block_time: Some(batch.time), seen: Some(Instant::now()) };
    let before = s.seq;
    s.source_height = batch.height;
    for (user, channel, data, key) in batch.events {
        s.push(user, channel, data, key);
    }
    let was_ready = s.ready;
    s.ready = s.sources.iter().all(|x| x.seen.is_some_and(|t| t.elapsed() <= stale));
    if !was_ready && s.ready {
        s.reason = "live local observations resumed; historical gaps remain unavailable".into();
    }
    Ok(before != s.seq || was_ready != s.ready)
}

#[cfg(test)]
mod tests {
    use super::*;
    const USER: &str = "0x0000000000000000000000000000000000000001";
    fn batch(height: u64, events: Value) -> String {
        json!({"block_number":height,"block_time":chrono::Utc::now().naive_utc(),"events":events}).to_string()
    }
    fn fill() -> Value {
        json!({"coin":"xyz:NVDA","side":"B","px":"100","sz":"1","time":123,"startPosition":"0","dir":"Open Long",
            "closedPnl":"0","hash":"0xabc","oid":1,"crossed":true,"fee":"0.1","tid":7,"feeToken":"USDC","builderFee":"0.01","deployerFee":"0.02"})
    }
    pub(super) fn hub(state: State) -> WalletHub {
        let (signal, _) = watch::channel(0);
        WalletHub {
            state: Arc::new(Mutex::new(state)),
            allowed: Arc::new(HashSet::from([USER.into()])),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            db_path: PathBuf::new(),
            streamed: false,
            metrics: Arc::new(crate::telemetry::Metrics::default()),
            order_caches: Arc::new(Mutex::new(HashMap::new())),
            signal,
            accounts: Arc::new(accounts::Accounts::new(Duration::from_secs(1))),
            poll_interval: Duration::from_secs(1),
            event_interval: Duration::from_millis(100),
        }
    }
    #[test]
    fn fills_preserve_fees_and_do_not_need_a_trade_pair() {
        let f = fill();
        let allowed = HashSet::from([USER.into()]);
        let decoded = decode(1, &batch(5, json!([[USER, f]])), &allowed).unwrap();
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(decoded.events[0].2, fill());
        let mut s = State::new();
        apply(&mut s, 1, decoded, false, Duration::from_secs(5)).unwrap();
        apply(&mut s, 0, decode(0, &batch(5, json!([])), &allowed).unwrap(), false, Duration::from_secs(5)).unwrap();
        let hub = hub(s);
        let sub = WalletSubscription::UserFills { user: USER.into(), aggregate_by_time: false };
        let (status, messages, cursor, reset) = hub.read(&sub, None);
        assert!(reset);
        assert_eq!(status["historyComplete"], false);
        assert_eq!(messages[0]["data"]["isSnapshot"], true);
        assert_eq!(messages[0]["data"]["fills"][0]["deployerFee"], "0.02");
        assert!(hub.read(&sub, Some(cursor)).1.is_empty());
    }
    #[test]
    fn stream_fragments_deduplicate_and_distinguish_self_trade_sides() {
        let allowed = HashSet::from([USER.into()]);
        let mut s = State::new();
        let f = fill();
        let line = batch(5, json!([[USER, f], [USER, f]]));
        let decoded = decode(1, &line, &allowed).unwrap();
        let time = decoded.time;
        apply(&mut s, 1, decoded, true, Duration::from_secs(5)).unwrap();
        assert_eq!(s.events.len(), 1);
        let mut other = fill();
        other["side"] = json!("A");
        other["oid"] = json!(2);
        let mut d = decode(1, &batch(5, json!([[USER, other]])), &allowed).unwrap();
        d.time = time;
        apply(&mut s, 1, d, true, Duration::from_secs(5)).unwrap();
        assert_eq!(s.events.len(), 2);
        assert!(
            apply(&mut s, 1, decode(1, &batch(4, json!([])), &allowed).unwrap(), true, Duration::from_secs(5)).is_err()
        );
    }
    #[test]
    fn batch_gap_and_malformed_pair_are_explicit() {
        let allowed = HashSet::from([USER.into()]);
        let mut s = State::new();
        apply(&mut s, 1, decode(1, &batch(1, json!([])), &allowed).unwrap(), false, Duration::from_secs(5)).unwrap();
        assert!(
            apply(&mut s, 1, decode(1, &batch(3, json!([])), &allowed).unwrap(), false, Duration::from_secs(5))
                .is_err()
        );
        let mut f = fill();
        f.as_object_mut().unwrap().remove("tid");
        assert!(decode(1, &batch(4, json!([[USER, f]])), &allowed).is_err());
        assert!(decode(1, "{broken", &allowed).is_err());
        assert!(decode(1, &batch(4, json!([["not-our-wallet", {}]])), &allowed).unwrap().events.is_empty());
    }
    #[test]
    fn conflicting_duplicate_fill_does_not_silently_overwrite() {
        let allowed = HashSet::from([USER.into()]);
        let mut s = State::new();
        apply(
            &mut s,
            1,
            decode(1, &batch(1, json!([[USER, fill()]])), &allowed).unwrap(),
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        let mut changed = fill();
        changed["fee"] = json!("99");
        let error = apply(
            &mut s,
            1,
            decode(1, &batch(2, json!([[USER, changed]])), &allowed).unwrap(),
            false,
            Duration::from_secs(5),
        )
        .unwrap_err();
        assert!(error.contains("conflicting duplicate"));
        assert_eq!(s.events[0].data["fee"], "0.1");
    }
    #[test]
    fn order_schema_preserves_original_size_and_status_timestamp() {
        let event = json!({"user":USER,"time":"2026-09-25T04:00:00.123","status":"canceled",
            "order":{"coin":"@1","side":"A","limitPx":"1","sz":"2","origSz":"5","oid":99,"timestamp":100,"cloid":"0x123"}});
        let allowed = HashSet::from([USER.into()]);
        let d = decode(0, &batch(1, json!([event])), &allowed).unwrap();
        assert_eq!(d.events[0].2["order"]["origSz"], "5");
        assert_eq!(d.events[0].2["statusTimestamp"], 1790308800123_i64);
        let mut bad = event;
        bad["order"].as_object_mut().unwrap().remove("origSz");
        assert!(decode(0, &batch(1, json!([bad])), &allowed).is_err());
    }
    #[test]
    fn historical_gaps_do_not_label_normal_initialization_as_gap_recovery() {
        let mut s = State::new();
        // Simulate a persisted historical gap loaded on an ordinary restart.
        s.gaps = 1;
        assert_eq!(s.missing_source_reason(), GateReason::Initializing);
        s.gap("new input discontinuity".into());
        assert_eq!(s.missing_source_reason(), GateReason::Gap);
        assert_eq!(s.gaps, 2);
        s.set_ready(true);
        assert!(!s.recovering_gap);
        assert_eq!(s.gaps, 2); // Recovery does not erase historical evidence.
        s.set_ready(false);
        assert_eq!(s.missing_source_reason(), GateReason::Initializing);
        s.gap("another discontinuity".into());
        assert_eq!(s.missing_source_reason(), GateReason::Gap);
    }
    #[test]
    fn lagging_reader_is_prioritized_with_positive_bounded_sleep() {
        assert_eq!(source_read_order(Some(10), Some(9)), [1, 0]);
        assert_eq!(source_read_order(Some(9), Some(10)), [0, 1]);
        assert_eq!(source_read_order(Some(10), Some(10)), [0, 1]);
        let poll = Duration::from_millis(5);
        assert_eq!(reader_delay(poll, true, false, false), Duration::from_millis(1));
        assert_eq!(reader_delay(poll, false, false, false), Duration::from_millis(1));
        assert_eq!(reader_delay(poll, true, true, false), poll);
        assert_eq!(reader_delay(poll, true, false, true), poll);
        for caught_up in [false, true] {
            for aligned in [false, true] {
                for failed in [false, true] {
                    assert!(!reader_delay(poll, caught_up, aligned, failed).is_zero());
                }
            }
        }
    }
    #[test]
    fn backlog_grace_never_masks_stale_input_startup_or_journal_failure() {
        let mut s = State::new();
        let brief = Duration::from_millis(99);
        assert!(!s.can_publish(false, brief, true, false)); // startup must catch up
        s.set_ready(true);
        assert!(s.can_publish(false, brief, true, false));
        assert!(!s.can_publish(false, Duration::from_millis(100), true, false));
        assert!(!s.can_publish(false, brief, false, false)); // age > stale threshold
        assert!(!s.can_publish(true, Duration::ZERO, false, false));
        assert!(!s.can_publish(true, Duration::ZERO, true, true));
        s.gap("missing block".into());
        assert!(!s.can_publish(false, brief, true, false));
        assert!(s.can_publish(true, Duration::ZERO, true, false));
    }
    #[test]
    fn temporary_pause_preserves_continuity_and_replays_undelivered_events() {
        let mut s = State::new();
        assert!(s.set_ready(true));
        let hub = hub(s);
        let sub = WalletSubscription::UserFills { user: USER.into(), aggregate_by_time: false };
        let cursor = hub.read(&sub, None).2;
        {
            let mut s = hub.state.lock().unwrap();
            assert!(s.set_ready(false));
            assert!(!s.set_ready(false));
            s.push(USER.into(), "userFills", fill(), "pending".into());
        }
        let (status, messages, _, reset) = hub.read(&sub, Some(cursor));
        assert_eq!(status["state"], "Stale");
        assert!(!reset);
        assert!(messages.is_empty());
        assert!(hub.state.lock().unwrap().set_ready(true));
        let (status, messages, next, reset) = hub.read(&sub, Some(cursor));
        assert_eq!(status["generation"], cursor.0);
        assert_eq!(status["gaps"], 0);
        assert!(!reset);
        assert_eq!(messages[0]["data"]["isSnapshot"], false);
        assert_eq!(messages[0]["data"]["fills"], json!([fill()]));
        assert!(next.1 > cursor.1);
        hub.state.lock().unwrap().gap("actual missing input".into());
        assert!(hub.read(&sub, Some(next)).3);
    }
    #[test]
    fn retention_overflow_and_epoch_changes_require_reset() {
        let mut s = State::new();
        s.ready = true;
        for n in 0..MAX_EVENTS + 1 {
            s.push(USER.into(), "userFills", json!({"n":n}), n.to_string());
        }
        assert_eq!(s.events.len(), MAX_EVENTS);
        assert_eq!(s.removed_through, 1);
        let hub = hub(s);
        let sub = WalletSubscription::UserFills { user: USER.into(), aggregate_by_time: false };
        assert!(hub.read(&sub, Some((0, 0))).3);
        let old = hub.read(&sub, None).2;
        hub.state.lock().unwrap().gap("test gap".into());
        let (status, messages, _, reset) = hub.read(&sub, Some(old));
        assert!(reset);
        assert!(messages.is_empty());
        assert_eq!(status["state"], "Stale");
    }
    #[test]
    fn aggregate_cursor_waits_for_complete_stream_block() {
        let mut s = State::new();
        s.ready = true;
        s.source_height = 10;
        s.sources[1].height = Some(10);
        s.push(USER.into(), "userFills", fill(), "a".into());
        let mut hub = hub(s);
        hub.streamed = true;
        let sub = WalletSubscription::UserFills { user: USER.into(), aggregate_by_time: true };
        let (_, messages, cursor, _) = hub.read(&sub, None);
        assert_eq!(messages[0]["data"]["fills"], json!([]));
        assert_eq!(cursor.1, 0);
        {
            let mut s = hub.state.lock().unwrap();
            let mut f = fill();
            f["tid"] = json!(8);
            s.push(USER.into(), "userFills", f, "b".into());
            s.sources[1].height = Some(11);
        }
        let (_, messages, cursor, _) = hub.read(&sub, Some(cursor));
        assert_eq!(messages[0]["data"]["fills"].as_array().unwrap().len(), 1);
        assert_eq!(messages[0]["data"]["fills"][0]["sz"], "2");
        assert!(hub.read(&sub, Some(cursor)).1.is_empty());
    }
    #[test]
    fn aggregate_omits_evicted_boundary_and_bad_decimal_fails_closed() {
        let mut s = State::new();
        s.ready = true;
        s.source_height = 10;
        s.sources[1].height = Some(10);
        for n in 0..MAX_EVENTS + 1 {
            s.push(USER.into(), "userFills", fill(), n.to_string());
        }
        let hub = hub(s);
        let sub = WalletSubscription::UserFills { user: USER.into(), aggregate_by_time: true };
        assert_eq!(hub.read(&sub, None).1[0]["data"]["fills"], json!([]));
        {
            let mut s = hub.state.lock().unwrap();
            s.source_height = 11;
            s.sources[1].height = Some(11);
            let mut f = fill();
            f["px"] = json!("NaN");
            s.push(USER.into(), "userFills", f, "bad".into());
        }
        let (status, messages, _, _) = hub.read(&sub, None);
        assert_eq!(status["state"], "Stale");
        assert!(messages.is_empty());
        assert_eq!(hub.status()["state"], "Ready"); // raw wallet/book state unaffected
    }
    #[test]
    fn journal_roundtrip_restart_and_corruption() {
        let root = std::env::temp_dir().join(format!("wallet-journal-test-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("journal.json");
        let mut s = State::new();
        s.push(USER.into(), "userFills", fill(), "key".into());
        save(&path, &Journal { schema: 1, seq: s.seq, events: s.events }).unwrap();
        let config = ServerConfig {
            data_dir: root.clone(),
            wallets: HashSet::from([USER.into()]),
            wallet_journal_path: Some(path.clone()),
            ..ServerConfig::default()
        };
        let hub = WalletHub::new(&config).unwrap();
        assert_eq!(hub.status()["retainedEvents"], 1);
        assert_eq!(hub.status()["state"], "Stale");
        let deadline = Instant::now() + Duration::from_secs(2);
        while storage::Store::open(&path.with_extension("sqlite")).unwrap().meta::<u64>("seq").unwrap() != Some(1) {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(hub);
        std::thread::sleep(Duration::from_millis(30));
        // Migration is once only: the completed SQLite store supersedes legacy JSON.
        std::fs::write(&path, "broken").unwrap();
        let hub = WalletHub::new(&config).unwrap();
        drop(hub);
        std::thread::sleep(Duration::from_millis(30));
        std::fs::write(path.with_extension("sqlite"), "broken database").unwrap();
        assert!(WalletHub::new(&config).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    #[test]
    #[ignore = "manual release-mode parser cost measurement; not a latency guarantee"]
    fn wallet_parser_cost() {
        let user = "0x0000000000000000000000000000000000000001";
        let event = json!({"user":user,"time":"2026-09-25T04:00:00","status":"canceled","order":{
            "coin":"BTC","side":"A","limitPx":"100","sz":"1","origSz":"5","oid":1,"timestamp":123,
            "triggerCondition":"N/A","isTrigger":false,"triggerPx":"0","isPositionTpsl":false,
            "reduceOnly":false,"orderType":"Limit","tif":"Gtc","cloid":null}});
        let line = json!({"local_time":"2026-09-25T04:00:00","block_time":"2026-09-25T04:00:00","block_number":1,
            "events":vec![event;1000]})
        .to_string();
        let allowed = HashSet::from(["0x0000000000000000000000000000000000000002".into()]);
        let start = Instant::now();
        for _ in 0..100 {
            std::hint::black_box(decode(0, &line, &allowed).unwrap());
        }
        let wallet = start.elapsed();
        let start = Instant::now();
        for _ in 0..100 {
            std::hint::black_box(
                serde_json::from_str::<crate::types::node_data::Batch<crate::types::node_data::NodeDataOrderStatus>>(
                    &line,
                )
                .unwrap(),
            );
        }
        eprintln!(
            "100 x {} byte / 1000-order batches: wallet worker filtering {:?}; existing typed book decode {:?}",
            line.len(),
            wallet,
            start.elapsed()
        );
    }
}
