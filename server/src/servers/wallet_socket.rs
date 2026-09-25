//! Per-connection wallet cursors and asynchronous authoritative open-order snapshots.
use super::info::InfoBridge;
use crate::wallet::{WalletHub, WalletSubscription};
use futures_util::{FutureExt, future::BoxFuture, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::{collections::HashMap, time::Instant};
use tokio::sync::watch;

struct Entry {
    cursor: Option<(u64, u64)>,
    status_key: String,
    token: u64,
    pending: bool,
    last_poll: Option<Instant>,
    last_orders: Option<Value>,
    query_failed: bool,
    dirty: u64,
}
pub(crate) struct Completed {
    sub: WalletSubscription,
    token: u64,
    epoch: u64,
    response: Value,
}
pub(crate) struct WalletSession {
    pub(crate) hub: WalletHub,
    pub(crate) signal: watch::Receiver<u64>,
    pub(crate) jobs: FuturesUnordered<BoxFuture<'static, Completed>>,
    entries: HashMap<WalletSubscription, Entry>,
    token: u64,
}
pub(crate) fn is_wallet_request(v: &Value) -> bool {
    matches!(v["subscription"]["type"].as_str(), Some("userFills" | "orderUpdates" | "openOrders"))
}
fn error(message: impl ToString) -> Value {
    json!({"channel":"error","data":message.to_string()})
}
impl WalletSession {
    pub(crate) fn new(hub: WalletHub) -> Self {
        Self { signal: hub.subscribe_signal(), hub, jobs: FuturesUnordered::new(), entries: HashMap::new(), token: 0 }
    }
    pub(crate) fn active(&self) -> bool {
        !self.entries.is_empty()
    }
    pub(crate) fn polls(&self) -> bool {
        self.entries.keys().any(|s| matches!(s, WalletSubscription::OpenOrders { .. }))
    }
    pub(crate) fn request(&mut self, value: Value) -> Vec<Value> {
        let method = value["method"].as_str().unwrap_or("");
        if method != "subscribe" && method != "unsubscribe" {
            return vec![error("Expected subscribe or unsubscribe")];
        }
        let mut sub: WalletSubscription = match serde_json::from_value(value["subscription"].clone()) {
            Ok(s) => s,
            Err(e) => return vec![error(format!("Invalid wallet subscription: {e}"))],
        };
        sub.normalize();
        if method == "unsubscribe" {
            return if self.entries.remove(&sub).is_some() {
                vec![json!({"channel":"subscriptionResponse","data":value})]
            } else {
                vec![error("Wallet subscription not active")]
            };
        }
        if let Err(e) = self.hub.validate(&sub) {
            return vec![error(e)];
        }
        if self.entries.contains_key(&sub) {
            return vec![error("Wallet subscription already active")];
        }
        if matches!(sub, WalletSubscription::UserFills { .. })
            && self
                .entries
                .keys()
                .any(|other| matches!(other, WalletSubscription::UserFills { .. }) && other.user() == sub.user())
        {
            return vec![error("Use separate connections for raw and aggregated fills for the same wallet")];
        }
        if self.entries.len() >= 8 {
            return vec![error("Maximum eight wallet subscriptions per connection")];
        }
        if matches!(&sub, WalletSubscription::OrderUpdates { .. })
            && self.entries.keys().any(|s| matches!(s, WalletSubscription::OrderUpdates { .. }))
        {
            return vec![error(
                "Use a separate connection per orderUpdates wallet; its official payload has no user field",
            )];
        }
        self.token += 1;
        self.entries.insert(
            sub,
            Entry {
                cursor: None,
                status_key: String::new(),
                token: self.token,
                pending: false,
                last_poll: None,
                last_orders: None,
                query_failed: false,
                dirty: 0,
            },
        );
        let mut messages = vec![json!({"channel":"subscriptionResponse","data":value})];
        messages.extend(self.updates());
        messages
    }
    pub(crate) fn updates(&mut self) -> Vec<Value> {
        let mut messages = Vec::new();
        for (sub, entry) in &mut self.entries {
            let (status, events, cursor, reset) = self.hub.read(sub, entry.cursor);
            let ready = status["state"] == "Ready";
            let key = json!([status["state"], status["generation"], status["journalError"]]).to_string();
            if entry.status_key != key || (reset && entry.cursor.is_some() && ready) {
                messages.push(json!({"channel":"walletStatus","data":status}));
                entry.status_key = key;
            }
            if ready {
                entry.cursor = Some(cursor);
                if reset {
                    entry.last_orders = None;
                    entry.last_poll = None;
                }
                messages.extend(events);
            }
        }
        messages
    }
    pub(crate) fn schedule(&mut self, bridge: InfoBridge) {
        if !self.polls() {
            return;
        }
        let status = self.hub.status();
        if status["state"] != "Ready" {
            return;
        }
        let epoch = status["generation"].as_u64().unwrap_or_default();
        for (sub, entry) in &mut self.entries {
            let WalletSubscription::OpenOrders { user, dex } = sub else { continue };
            let dirty = self.hub.dirty_version(user, dex);
            let interval = if dirty != entry.dirty || entry.query_failed {
                self.hub.event_interval
            } else {
                self.hub.poll_interval
            };
            if self.jobs.len() >= 4 || entry.pending || entry.last_poll.is_some_and(|t| t.elapsed() < interval) {
                continue;
            }
            let bridge = bridge.clone();
            let token = entry.token;
            let sub = sub.clone();
            let user = user.clone();
            let dex = dex.clone();
            let hub = self.hub.clone();
            entry.dirty = dirty;
            entry.pending = true;
            entry.last_poll = Some(Instant::now());
            self.jobs.push(
                async move {
                    let response = hub.open_orders(bridge, user, dex, epoch).await;
                    Completed { sub, token, epoch, response }
                }
                .boxed(),
            );
        }
    }
    pub(crate) fn complete(&mut self, result: Completed) -> Vec<Value> {
        let Some(entry) = self.entries.get_mut(&result.sub) else { return vec![] };
        if entry.token != result.token {
            return vec![];
        }
        entry.pending = false;
        let state = self.hub.status();
        if state["state"] != "Ready" || state["generation"] != result.epoch {
            return vec![];
        }
        let WalletSubscription::OpenOrders { user, dex } = &result.sub else { return vec![] };
        let mut orders = result.response["payload"]["data"].clone();
        // Never infer current orders from incomplete deltas. Each response is an authoritative
        // point-in-time query; do not replay possibly older events over a newer HTTP snapshot.
        if result.response["type"] != "info" || !valid_orders(&orders) {
            if entry.query_failed {
                return vec![];
            }
            entry.query_failed = true;
            entry.last_orders = None;
            return vec![json!({"channel":"walletStatus","data":{"user":user,"subscription":result.sub,
                "state":"Stale","scope":"openOrders","reason":"local Info query failed or returned invalid orders",
                "detail":result.response,"source":"localNode","resetRequired":true}})];
        }
        if let Some(a) = orders.as_array_mut() {
            a.sort_by_key(|o| o["oid"].as_u64());
        }
        let mut messages = Vec::new();
        if entry.query_failed {
            messages.push(json!({"channel":"walletStatus","data":{"user":user,"subscription":result.sub,
                "state":"Ready","scope":"openOrders","reason":"authoritative local snapshot refreshed","source":"localNode"}}));
            entry.query_failed = false;
        }
        if entry.last_orders.as_ref() != Some(&orders) {
            entry.last_orders = Some(orders.clone());
            messages.push(json!({"channel":"openOrders","data":{"user":user,"dex":dex,"orders":orders}}));
        }
        messages
    }
}
fn valid_orders(v: &Value) -> bool {
    v.as_array().is_some_and(|a| {
        a.iter().all(|o| {
            ["coin", "side", "limitPx", "sz"].iter().all(|k| o[k].is_string())
                && o["oid"].is_u64()
                && o["timestamp"].is_u64()
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_orders_are_not_published_as_an_empty_snapshot() {
        assert!(valid_orders(&json!([])));
        assert!(!valid_orders(&json!({"error":"not available"})));
        assert!(!valid_orders(&json!([{"coin":"BTC"}])));
    }
    #[test]
    fn normalization_unsubscribe_and_inflight_token_guard() {
        let config = crate::ServerConfig::default();
        let hub = WalletHub::new(&config).unwrap();
        let mut session = WalletSession::new(hub);
        let sub = WalletSubscription::OpenOrders {
            user: "0x0000000000000000000000000000000000000001".into(),
            dex: "xyz".into(),
        };
        session.entries.insert(
            sub.clone(),
            Entry {
                cursor: None,
                status_key: String::new(),
                token: 2,
                pending: true,
                last_poll: None,
                last_orders: None,
                query_failed: false,
                dirty: 0,
            },
        );
        let result =
            Completed { sub: sub.clone(), token: 1, epoch: 0, response: json!({"type":"info","payload":{"data":[]}}) };
        assert!(session.complete(result).is_empty());
        assert!(session.entries[&sub].pending);
        let messages = session.request(json!({"method":"unsubscribe","subscription":sub}));
        assert_eq!(messages[0]["channel"], "subscriptionResponse");
        assert!(session.entries.is_empty());
        assert!(session.complete(Completed { sub, token: 2, epoch: 0, response: json!({}) }).is_empty());
    }
}
