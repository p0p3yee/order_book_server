//! Bounded diagnostics. No unbounded event history and no changes to market-data wire payloads.
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

const WINDOW: usize = 512;
#[derive(Default)]
struct Samples {
    count: u64,
    recent: VecDeque<f64>,
}
#[derive(Default)]
pub(crate) struct Metrics {
    samples: Mutex<BTreeMap<&'static str, Samples>>,
}
impl Metrics {
    pub(crate) fn observe(&self, name: &'static str, value: f64) {
        let mut values = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        let s = values.entry(name).or_default();
        s.count += 1;
        if s.recent.len() == WINDOW {
            s.recent.pop_front();
        }
        s.recent.push_back(value);
    }
    pub(crate) fn elapsed(&self, name: &'static str, start: Instant) {
        self.observe(name, start.elapsed().as_secs_f64() * 1e6);
    }
    pub(crate) fn snapshot(&self) -> Value {
        let copied: Vec<_> = self
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(name, s)| (*name, s.count, s.recent.iter().copied().collect::<Vec<_>>()))
            .collect();
        let mut result = serde_json::Map::new();
        for (name, count, mut values) in copied {
            values.sort_by(f64::total_cmp);
            let n = values.len();
            if n > 0 {
                result.insert(
                    name.into(),
                    json!({"count":count,"window_samples":n,
                "p50":values[n/2],"p95":values[(n*95/100).min(n-1)],"max":values[n-1],"min":values[0]}),
                );
            }
        }
        Value::Object(result)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Trace {
    pub height: u64,
    pub block_us: i64,
    pub node_local_us: i64,
    pub read_us: i64,
    pub first_read: Instant,
    pub parsed: Instant,
    pub applied_us: i64,
    pub published_us: i64,
    pub published: Instant,
}
impl Trace {
    pub(crate) fn new(height: u64, block_us: i64, node_local_us: i64, first_read: Instant, read_us: i64) -> Self {
        Self {
            height,
            block_us,
            node_local_us,
            read_us,
            first_read,
            parsed: Instant::now(),
            applied_us: 0,
            published_us: 0,
            published: Instant::now(),
        }
    }
    pub(crate) fn publish(mut self) -> Self {
        self.published = Instant::now();
        self.published_us = unix_us();
        self
    }
}
pub(crate) fn unix_us() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

pub(crate) struct SocketTelemetry {
    pub metrics: Arc<Metrics>,
    pub trace: RefCell<Option<Trace>>,
}
tokio::task_local! { pub(crate) static SOCKET_TELEMETRY: SocketTelemetry; }
pub(crate) fn dispatch(trace: Option<Trace>) {
    let _unused = SOCKET_TELEMETRY.try_with(|ctx| {
        if let Some(trace) = trace {
            ctx.metrics.elapsed("ws_dispatch_queue_us", trace.published);
        }
        *ctx.trace.borrow_mut() = trace;
    });
}
pub(crate) fn socket_metrics() -> Option<Arc<Metrics>> {
    SOCKET_TELEMETRY.try_with(|ctx| ctx.metrics.clone()).ok()
}
pub(crate) fn socket_trace() -> Option<Trace> {
    SOCKET_TELEMETRY.try_with(|ctx| *ctx.trace.borrow()).ok().flatten()
}
pub(crate) fn version() -> Value {
    json!({"implementation":"hyperliquid-order-book-server/low-latency-ws",
        "package_version":env!("CARGO_PKG_VERSION"),"revision":env!("WS_SOURCE_REVISION"),
        "diagnostics_schema":1})
}
pub(crate) fn capabilities() -> Value {
    json!({"server":version(),"subscriptions":["l2Book","trades","l4Book"],
        "methods":["subscribe","unsubscribe","post"],"wallet_subscriptions":false,"websocket_info_post":true,
        "websocket_info_queries":crate::servers::info::QUERIES,"websocket_actions":false,
        "info_post_limits":{"per_connection":4,"global":16,"timeout_ms":2000,"response_bytes":2097152},
        "l2_default_depth":20,"l2_depth_parameter":"nLevels","l2_max_depth":100,
        "wallet_info_transport":"Node HTTP Info or WebSocket post/info for supported read-only queries; wallet subscriptions are not implemented"})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_are_bounded_and_preserve_negative_clock_deltas() {
        let metrics = Metrics::default();
        for n in 0..1000 {
            metrics.observe("sample_us", n as f64);
        }
        metrics.observe("clock_us", -5.0);
        let value = metrics.snapshot();
        assert_eq!(value["sample_us"]["count"], 1000);
        assert_eq!(value["sample_us"]["window_samples"], 512);
        assert_eq!(value["sample_us"]["min"], 488.0);
        assert_eq!(value["clock_us"]["min"], -5.0);
    }
}
