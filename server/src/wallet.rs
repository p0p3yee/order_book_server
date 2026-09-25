//! Local-only wallet journal. Raw node records are tapped before market filtering.
//! Parsing and persistence run on a dedicated worker, never under the book lock.
use crate::ServerConfig;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json, value::RawValue};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
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
    OpenOrders {
        user: String,
        #[serde(default)]
        dex: String,
    },
}
impl WalletSubscription {
    pub(crate) fn user(&self) -> &str {
        match self {
            Self::UserFills { user, .. } | Self::OrderUpdates { user } | Self::OpenOrders { user, .. } => user,
        }
    }
    pub(crate) fn normalize(&mut self) {
        match self {
            Self::UserFills { user, .. } | Self::OrderUpdates { user } | Self::OpenOrders { user, .. } => {
                user.make_ascii_lowercase()
            }
        }
    }
    pub(crate) fn channel(&self) -> &'static str {
        match self {
            Self::UserFills { .. } => "userFills",
            Self::OrderUpdates { .. } => "orderUpdates",
            Self::OpenOrders { .. } => "openOrders",
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
    reason: String,
    journal_error: Option<String>,
    gaps: u64,
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
            reason: "startup: history before collection and downtime are unavailable".into(),
            journal_error: None,
            gaps: 0,
        }
    }
    fn gap(&mut self, reason: String) {
        self.epoch += 1;
        self.gaps += 1;
        self.ready = false;
        self.sources = [Source::default(), Source::default()];
        self.reason = reason;
    }
    fn push(&mut self, user: String, channel: &str, data: Value, key: String) {
        if !self.keys.insert(key.clone()) {
            return;
        }
        self.seq += 1;
        let bytes = data.to_string().len() + key.len() + user.len() + 128;
        self.bytes += bytes;
        self.events.push_back(Event { seq: self.seq, user, channel: channel.into(), data, key, bytes });
        while self.events.len() > MAX_EVENTS || self.bytes > MAX_BYTES {
            if let Some(e) = self.events.pop_front() {
                self.bytes -= e.bytes;
                self.keys.remove(&e.key);
                self.removed_through = e.seq;
            }
        }
    }
    fn status(&self) -> Value {
        json!({"source":"localNode","state":if self.ready {"Ready"} else {"Stale"},
            "generation":self.epoch,"reason":self.reason,"gaps":self.gaps,"sessionStartedAt":self.session_started_at,
            "historyComplete":false,"historyScope":"bounded local observations; pre-collection and downtime events unavailable",
            "oldestRetainedSequence":self.events.front().map(|e|e.seq),"latestSequence":self.seq,
            "retainedEvents":self.events.len(),"retainedBytes":self.bytes,"journalError":self.journal_error,
            "orderHeight":self.sources[0].height,"fillHeight":self.sources[1].height,
            "orderTime":self.sources[0].block_time,"fillTime":self.sources[1].block_time})
    }
}
struct Input {
    enqueued: Instant,
    source: usize,
    line: String,
    epoch: u64,
}
#[derive(Default)]
struct OrdersCache {
    epoch: u64,
    fetched: Option<Instant>,
    response: Value,
}
type OrderCaches = Arc<Mutex<HashMap<(String, String), Arc<tokio::sync::Mutex<OrdersCache>>>>>;
#[derive(Clone)]
pub(crate) struct WalletHub {
    state: Arc<Mutex<State>>,
    allowed: Arc<HashSet<String>>,
    tx: Option<mpsc::SyncSender<Input>>,
    queued: Arc<AtomicUsize>,
    epoch: Arc<AtomicU64>,
    metrics: Arc<crate::telemetry::Metrics>,
    order_caches: OrderCaches,
    signal: watch::Sender<u64>,
    pub(crate) poll_interval: Duration,
}
impl WalletHub {
    pub(crate) fn new(config: &ServerConfig) -> crate::Result<Self> {
        let allowed: HashSet<_> = config.wallets.iter().map(|w| w.to_ascii_lowercase()).collect();
        let path = config.wallet_journal_path.clone().unwrap_or_else(|| config.data_dir.join("ws-wallet-journal.json"));
        let mut state = State::new();
        if !allowed.is_empty() && path.exists() {
            if std::fs::metadata(&path)?.len() > (MAX_BYTES * 3) as u64 {
                return Err("wallet journal exceeds size limit".into());
            }
            let journal: Journal = serde_json::from_slice(&std::fs::read(&path)?)?;
            if journal.schema != 1 {
                return Err("unsupported wallet journal schema".into());
            }
            let mut previous = 0;
            for e in &journal.events {
                if e.seq <= previous
                    || e.seq > journal.seq
                    || !valid_address(&e.user)
                    || !["userFills", "orderUpdates"].contains(&e.channel.as_str())
                    || e.data.to_string().len() > MAX_EVENT_BYTES
                {
                    return Err("invalid wallet journal".into());
                }
                previous = e.seq;
            }
            // Re-sequence retained observations; every restart explicitly opens a new gap.
            for e in journal.events {
                if allowed.contains(&e.user) {
                    state.push(e.user, &e.channel, e.data, e.key);
                }
            }
        }
        let (signal, _) = watch::channel(0);
        let state = Arc::new(Mutex::new(state));
        let queued = Arc::new(AtomicUsize::new(0));
        let allowed = Arc::new(allowed);
        let epoch = Arc::new(AtomicU64::new(0));
        let metrics = Arc::new(crate::telemetry::Metrics::default());
        let mut hub = Self {
            state: state.clone(),
            allowed: allowed.clone(),
            tx: None,
            queued: queued.clone(),
            epoch: epoch.clone(),
            metrics: metrics.clone(),
            order_caches: Arc::new(Mutex::new(HashMap::new())),
            signal: signal.clone(),
            poll_interval: config.wallet_poll_interval,
        };
        if !allowed.is_empty() {
            let parent = path.parent().ok_or("wallet journal needs a parent directory")?;
            std::fs::create_dir_all(parent)?;
            // Keep the journal separate from the large node snapshot.
            if path == config.snapshot_path {
                return Err("wallet journal and snapshot paths must differ".into());
            }
            let (tx, rx) = mpsc::sync_channel::<Input>(64);
            hub.tx = Some(tx);
            let streamed = config.stream_with_block_info;
            let stale = config.stale_after;
            std::thread::Builder::new().name("wallet-journal".into()).spawn(move || {
                let mut persisted = 0;
                let mut last_save = Instant::now();
                loop {
                    match rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(input) => {
                            queued.fetch_sub(input.line.len(), Ordering::Relaxed);
                            metrics.elapsed("queue_wait_us", input.enqueued);
                            let decode_start = Instant::now();
                            let decoded = decode(input.source, &input.line, &allowed);
                            metrics.elapsed("decode_us", decode_start);
                            let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
                            if s.epoch == input.epoch {
                                match decoded.and_then(|batch| apply(&mut s, input.source, batch, streamed, stale)) {
                                    Ok(changed) => {
                                        if changed {
                                            signal.send_modify(|n| *n += 1);
                                        }
                                    }
                                    Err(reason) => {
                                        log::warn!("wallet gap: {reason}");
                                        s.gap(reason);
                                        epoch.store(s.epoch, Ordering::Release);
                                        signal.send_modify(|n| *n += 1);
                                    }
                                }
                            }
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                    let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
                    if s.ready && s.sources.iter().any(|x| x.seen.is_none_or(|t| t.elapsed() > stale)) {
                        s.gap("upstream wallet stream stopped progressing".into());
                        epoch.store(s.epoch, Ordering::Release);
                        signal.send_modify(|n| *n += 1);
                    }
                    if last_save.elapsed() >= Duration::from_secs(1) && s.seq != persisted {
                        let seq = s.seq;
                        let journal = Journal { schema: 1, seq, events: s.events.clone() };
                        drop(s);
                        let persist_start = Instant::now();
                        let result = save(&path, &journal);
                        metrics.elapsed("persist_us", persist_start);
                        let mut s = state.lock().unwrap_or_else(|e| e.into_inner());
                        let error = result.err().map(|e| e.to_string());
                        if error != s.journal_error {
                            if error.is_some() {
                                log::warn!("wallet journal write failed: {error:?}");
                            }
                            s.journal_error = error.clone();
                            signal.send_modify(|n| *n += 1);
                        }
                        if error.is_none() {
                            persisted = seq;
                        }
                        last_save = Instant::now();
                    }
                }
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
            if !caches.contains_key(&key) && caches.len() >= 32 {
                return json!({"type":"error","payload":"429: maximum 32 distinct wallet/dex query keys per process"});
            }
            caches.entry(key).or_default().clone()
        };
        let mut cache = cache.lock().await;
        if cache.epoch == epoch && cache.fetched.is_some_and(|t| t.elapsed() < self.poll_interval) {
            return cache.response.clone();
        }
        let start = Instant::now();
        let response = bridge
            .execute(crate::servers::info::PostRequest {
                id: 0,
                request: json!({"type":"info",
            "payload":{"type":"frontendOpenOrders","user":user,"dex":dex}}),
            })
            .await
            .response;
        self.metrics.elapsed("open_orders_query_us", start);
        cache.epoch = epoch;
        cache.fetched = Some(Instant::now());
        cache.response = response.clone();
        response
    }
    pub(crate) fn enabled(&self) -> bool {
        self.tx.is_some()
    }
    pub(crate) fn subscribe_signal(&self) -> watch::Receiver<u64> {
        self.signal.subscribe()
    }
    pub(crate) fn validate(&self, sub: &WalletSubscription) -> Result<(), String> {
        if !self.allowed.contains(sub.user()) {
            return Err("wallet not enabled; configure --wallets with this address".into());
        }
        if matches!(sub, WalletSubscription::UserFills { aggregate_by_time: true, .. }) {
            return Err("aggregateByTime=true is not supported locally; use false or omit it".into());
        }
        if let WalletSubscription::OpenOrders { dex, .. } = sub {
            if dex.len() > 64 || !dex.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-') {
                return Err("invalid dex name".into());
            }
        }
        Ok(())
    }
    pub(crate) fn tap(&self, source: usize, line: &str) {
        let Some(tx) = &self.tx else { return };
        let source = match source {
            0 => 0,
            2 => 1,
            _ => return,
        };
        let old = self.queued.fetch_add(line.len(), Ordering::Relaxed);
        if old.saturating_add(line.len()) > INPUT_BYTES {
            self.queued.fetch_sub(line.len(), Ordering::Relaxed);
            self.gap("wallet input byte limit exceeded");
            return;
        }
        let epoch = self.epoch.load(Ordering::Acquire);
        if let Err(e) = tx.try_send(Input { enqueued: Instant::now(), source, line: line.into(), epoch }) {
            let input = match e {
                mpsc::TrySendError::Full(i) | mpsc::TrySendError::Disconnected(i) => i,
            };
            self.queued.fetch_sub(input.line.len(), Ordering::Relaxed);
            self.gap("wallet worker queue unavailable");
        }
    }
    pub(crate) fn gap(&self, reason: &str) {
        if !self.enabled() {
            return;
        }
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !s.ready && s.reason == reason {
            return;
        }
        s.gap(reason.into());
        self.epoch.store(s.epoch, Ordering::Release);
        self.signal.send_modify(|n| *n += 1);
    }
    pub(crate) fn status(&self) -> Value {
        let mut status = self.state.lock().unwrap_or_else(|e| e.into_inner()).status();
        status["enabled"] = json!(self.enabled());
        status["configuredWallets"] = json!(self.allowed.len());
        status["queuedBytes"] = json!(self.queued.load(Ordering::Relaxed));
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
                .map(|e| e.data.clone())
                .collect();
            if events.is_empty() { vec![] } else { vec![json!({"channel":"orderUpdates","data":events})] }
        } else {
            vec![]
        };
        (status, messages, (s.epoch, s.seq), reset)
    }
}
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
            let mut basic = serde_json::Map::new();
            for field in ["coin", "side", "limitPx", "sz", "origSz", "oid", "timestamp", "cloid"] {
                if let Some(v) = order.get(field) {
                    basic.insert(field.into(), v.clone());
                }
            }
            let data =
                json!({"order":basic,"status":value["status"],"statusTimestamp":time.and_utc().timestamp_millis()});
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
            || (!streamed && batch.height != h + 1)
            || (batch.height == h && previous.block_time != Some(batch.time))
        {
            return Err(format!("wallet source {source} discontinuity: previous {h}, received {}", batch.height));
        }
    }
    if chrono::Utc::now().timestamp_millis().saturating_sub(batch.time) > stale.as_millis() as i64 {
        return Err("wallet upstream event time stale".into());
    }
    s.sources[source] = Source { height: Some(batch.height), block_time: Some(batch.time), seen: Some(Instant::now()) };
    let before = s.seq;
    for (user, channel, data, key) in batch.events {
        if s.keys.contains(&key) && s.events.iter().any(|e| e.key == key && e.data != data) {
            return Err("conflicting duplicate wallet event".into());
        }
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
    fn hub(state: State) -> WalletHub {
        let (signal, _) = watch::channel(0);
        WalletHub {
            state: Arc::new(Mutex::new(state)),
            allowed: Arc::new(HashSet::from([USER.into()])),
            tx: None,
            queued: Arc::new(AtomicUsize::new(0)),
            epoch: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(crate::telemetry::Metrics::default()),
            order_caches: Arc::new(Mutex::new(HashMap::new())),
            signal,
            poll_interval: Duration::from_secs(1),
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
        drop(hub);
        std::fs::write(&path, "broken").unwrap();
        assert!(WalletHub::new(&config).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn queue_overflow_never_blocks_and_signals_gap() {
        let mut hub = hub(State::new());
        let (tx, _rx) = mpsc::sync_channel(1);
        hub.tx = Some(tx);
        hub.tap(0, "first");
        hub.tap(0, "second");
        assert_eq!(hub.status()["gaps"], 1);
        assert_eq!(hub.queued.load(Ordering::Relaxed), 5);
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
