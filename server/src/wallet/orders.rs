//! Observed order records, never inferred unknown orders or fabricated metadata.
use super::*;
const UNAVAILABLE: &str = "LOCAL_HISTORY_UNAVAILABLE: ";

pub(super) fn basic_update(data: &Value) -> Value {
    let mut order = serde_json::Map::new();
    for k in ["coin", "side", "limitPx", "sz", "origSz", "oid", "timestamp", "cloid"] {
        if let Some(v) = data["order"].get(k) {
            order.insert(k.into(), v.clone());
        }
    }
    json!({"order":order,"status":data["status"],"statusTimestamp":data["statusTimestamp"]})
}
pub(super) fn same_record(a: &Value, b: &Value) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    if let Some(o) = a.as_object_mut() {
        o.remove("_localGapCount");
    }
    if let Some(o) = b.as_object_mut() {
        o.remove("_localGapCount");
    }
    a == b
}
pub(super) fn identity_matches(order: &Value, oid: &Value) -> bool {
    if oid.is_u64() {
        order["oid"] == *oid
    } else {
        order["cloid"].as_str().zip(oid.as_str()).is_some_and(|(a, b)| a.eq_ignore_ascii_case(b))
    }
}
fn complete(data: &Value) -> bool {
    let o = &data["order"];
    ["coin", "side", "limitPx", "sz", "origSz", "triggerCondition", "triggerPx", "orderType"]
        .iter()
        .all(|k| o[*k].is_string())
        && ["isTrigger", "isPositionTpsl", "reduceOnly"].iter().all(|k| o[*k].is_boolean())
        && ["oid", "timestamp"].iter().all(|k| o[*k].is_u64())
        && o.get("tif").is_some_and(|v| v.is_string() || v.is_null())
        && o.get("cloid").is_some_and(|v| v.is_string() || v.is_null())
        && data["status"].is_string()
        && data["statusTimestamp"].is_u64()
}
fn reply(data: Value, gaps: u64, last_fill_seq: u64, seq: u64) -> Result<Value, String> {
    if !complete(&data) {
        return Err(format!("{UNAVAILABLE}full classification fields were not retained"));
    }
    let status = data["status"].as_str().unwrap_or("");
    let terminal = status == "filled"
        || status == "canceled"
        || status == "rejected"
        || status.ends_with("Canceled")
        || status.ends_with("Rejected");
    if !terminal && (data["_localGapCount"].as_u64() != Some(gaps) || last_fill_seq > seq) {
        return Err(format!("{UNAVAILABLE}open record cannot be verified after a gap or subsequent fill"));
    }
    Ok(
        json!({"status":"order","order":{"order":data["order"],"status":data["status"],"statusTimestamp":data["statusTimestamp"]}}),
    )
}
fn complete_height(s: &State, streamed: bool) -> Option<u64> {
    let orders = s.sources[0].height?;
    let fills = s.sources[1].height?;
    if orders != fills {
        return None;
    }
    orders.checked_sub(u64::from(streamed))
}
impl WalletHub {
    pub(crate) async fn order_status(&self, payload: Value) -> Result<Value, String> {
        let user = payload["user"].as_str().ok_or("400: missing user")?.to_ascii_lowercase();
        if !self.allowed.contains(&user) {
            return Err(format!("{UNAVAILABLE}wallet not enabled"));
        }
        let oid = payload["oid"].clone();
        if !oid.as_u64().is_some_and(|n| n <= i64::MAX as u64)
            && !oid
                .as_str()
                .is_some_and(|s| s.len() == 34 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err("400: oid must be a nonnegative integer or 16-byte hexadecimal cloid".into());
        }
        let (epoch, seq, hot) = {
            let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if !s.ready || !s.history_current || s.journal_error.is_some() {
                return Err(format!("{UNAVAILABLE}wallet history is not current (replay, backlog or stale input)"));
            }
            let hot = s
                .pending
                .iter()
                .chain(s.events.iter())
                .filter(|e| e.user == user && e.channel == "orderUpdates" && identity_matches(&e.data["order"], &oid))
                .max_by_key(|e| e.seq)
                .cloned();
            if let Some(e) = &hot {
                if complete_height(&s, self.streamed).is_none_or(|height| e.height > height) {
                    return Err(format!("{UNAVAILABLE}order block is not complete in both sources"));
                }
                let fill = s
                    .pending
                    .iter()
                    .chain(s.events.iter())
                    .filter(|f| f.user == user && f.channel == "userFills" && f.data["oid"] == e.data["order"]["oid"])
                    .max_by_key(|f| f.seq)
                    .map_or(0, |f| f.seq);
                return reply(e.data.clone(), s.gaps, fill, e.seq);
            }
            if !s.pending.is_empty() {
                return Err(format!("{UNAVAILABLE}uncommitted history prevents database fallback"));
            }
            (s.epoch, s.seq, hot)
        };
        let _hot = hot;
        let path = self.db_path.clone();
        let u = user.clone();
        let identity = oid.clone();
        let record =
            tokio::task::spawn_blocking(move || storage::order_record(&path, &u, &identity).map_err(|e| e.to_string()))
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| format!("{UNAVAILABLE}{e}"))?;
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !s.ready || !s.history_current || s.epoch != epoch || s.seq != seq || s.journal_error.is_some() {
            return Err(format!("{UNAVAILABLE}wallet changed during historical lookup; retry"));
        }
        let Some((seq, data, fill, height)) = record else {
            return Err(format!("{UNAVAILABLE}no retained authoritative record for this identity"));
        };
        if complete_height(&s, self.streamed).is_none_or(|complete| height > complete) {
            return Err(format!("{UNAVAILABLE}historical order block is not complete in both sources"));
        }
        reply(data, s.gaps, fill, seq)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> Value {
        json!({"order":{"coin":"BTC","side":"B","limitPx":"1","sz":"1","origSz":"1","oid":7,"timestamp":1,"triggerCondition":"N/A","triggerPx":"0","orderType":"Limit","isTrigger":false,"isPositionTpsl":false,"reduceOnly":false,"tif":"Gtc","cloid":null},"status":"open","statusTimestamp":1,"_localGapCount":0})
    }
    #[test]
    fn missing_metadata_gaps_and_newer_fills_are_not_unknown_orders() {
        assert!(reply(record(), 0, 0, 1).is_ok());
        assert!(reply(record(), 1, 0, 1).unwrap_err().starts_with(UNAVAILABLE));
        assert!(reply(record(), 0, 2, 1).is_err());
        let mut data = record();
        data["order"].as_object_mut().unwrap().remove("reduceOnly");
        assert!(reply(data, 0, 0, 1).is_err());
        let mut data = record();
        data["status"] = json!("filled");
        assert!(reply(data, 1, 2, 1).is_ok());
    }
    #[tokio::test]
    async fn publication_grace_never_authorizes_history_ahead_of_fills() {
        let user = "0x0000000000000000000000000000000000000001";
        let mut s = State::new();
        s.set_ready(true);
        for source in &mut s.sources {
            source.height = Some(10);
        }
        s.source_height = 10;
        s.push(user.into(), "orderUpdates", record(), "open".into());
        assert!(s.can_publish(false, Duration::from_millis(99), true, false));
        let hub = super::super::tests::hub(s);
        let request = json!({"user":user,"oid":7});
        assert_eq!(hub.status()["historyCurrent"], false);
        assert_eq!(
            hub.order_status(request.clone()).await.unwrap_err(),
            format!("{UNAVAILABLE}wallet history is not current (replay, backlog or stale input)")
        );
        {
            let mut s = hub.state.lock().unwrap();
            s.push(user.into(), "userFills", json!({"oid":7}), "fill".into());
            s.history_current = true;
        }
        assert!(hub.order_status(request.clone()).await.unwrap_err().contains("subsequent fill"));
        {
            let mut s = hub.state.lock().unwrap();
            let mut terminal = record();
            terminal["status"] = json!("filled");
            s.push(user.into(), "orderUpdates", terminal, "terminal".into());
        }
        assert_eq!(hub.order_status(request.clone()).await.unwrap()["order"]["status"], "filled");
        // Even an otherwise valid terminal/historical response is gated by backlog.
        hub.state.lock().unwrap().history_current = false;
        assert!(hub.order_status(request).await.unwrap_err().starts_with(UNAVAILABLE));
    }
    #[tokio::test]
    async fn current_stream_fragments_and_misaligned_sources_cannot_answer_orders() {
        let user = "0x0000000000000000000000000000000000000001";
        let mut s = State::new();
        s.set_ready(true);
        s.history_current = true;
        s.source_height = 10;
        s.sources[0].height = Some(10);
        s.sources[1].height = Some(9);
        s.push(user.into(), "orderUpdates", record(), "open".into());
        let mut hub = super::super::tests::hub(s);
        let request = json!({"user":user,"oid":7});
        assert!(hub.order_status(request.clone()).await.is_err());
        hub.state.lock().unwrap().sources[1].height = Some(10);
        assert!(hub.order_status(request.clone()).await.is_ok());
        hub.streamed = true;
        assert!(hub.order_status(request.clone()).await.is_err());
        for source in &mut hub.state.lock().unwrap().sources {
            source.height = Some(11);
        }
        assert!(hub.order_status(request.clone()).await.is_ok());
        let root = std::env::temp_dir().join(format!(
            "ws-order-query-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        hub.db_path = root.join("history.sqlite");
        {
            let mut s = hub.state.lock().unwrap();
            let now = chrono::Utc::now().timestamp_millis();
            s.pending[0].data["statusTimestamp"] = json!(now);
            let mut store = storage::Store::open(&hub.db_path).unwrap();
            store.commit(&s.pending, &Default::default(), s.seq, &[], &ServerConfig::default(), now).unwrap();
            s.pending.clear();
            s.events.clear(); // force the indexed historical path
            for source in &mut s.sources {
                source.height = Some(10);
            }
        }
        assert!(hub.order_status(request.clone()).await.unwrap_err().contains("historical order block"));
        for source in &mut hub.state.lock().unwrap().sources {
            source.height = Some(11);
        }
        assert!(hub.order_status(request.clone()).await.is_ok());
        hub.state.lock().unwrap().history_current = false;
        assert!(hub.order_status(request).await.unwrap_err().contains("history is not current"));
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn evicted_uncommitted_records_never_fall_back_to_older_sqlite_state() {
        let user = "0x0000000000000000000000000000000000000001";
        let mut s = State::new();
        s.set_ready(true);
        s.history_current = true;
        s.source_height = 10;
        for source in &mut s.sources {
            source.height = Some(10);
        }
        let mut old = record();
        let now = chrono::Utc::now().timestamp_millis();
        old["statusTimestamp"] = json!(now);
        s.push(user.into(), "orderUpdates", old, "persisted-open".into());
        let root = std::env::temp_dir().join(format!(
            "ws-pending-query-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("history.sqlite");
        {
            let mut store = storage::Store::open(&path).unwrap();
            store.commit(&s.pending, &Default::default(), s.seq, &[], &ServerConfig::default(), now).unwrap();
        }
        s.pending.clear();
        s.events.clear();
        s.push(user.into(), "userFills", json!({"oid":7}), "pending-fill".into());
        for n in 0..=MAX_EVENTS {
            s.push(user.into(), "userFills", json!({"oid":8}), format!("other-fill-{n}"));
        }
        assert!(!s.events.iter().any(|e| e.data["oid"] == 7));
        let mut hub = super::super::tests::hub(s);
        hub.db_path = path;
        let request = json!({"user":user,"oid":7});
        assert!(hub.order_status(request.clone()).await.unwrap_err().contains("uncommitted history"));
        let committing = {
            let mut s = hub.state.lock().unwrap();
            s.history_current = false;
            std::mem::take(&mut s.pending)
        };
        // The writer owns pending now; an empty pending vector must not expose old SQLite.
        assert!(hub.order_status(request.clone()).await.unwrap_err().contains("history is not current"));
        {
            let mut s = hub.state.lock().unwrap();
            s.pending = committing;
            s.history_current = true;
            let mut latest = record();
            latest["status"] = json!("canceled");
            s.push(user.into(), "orderUpdates", latest, "pending-cancel".into());
            for n in 0..=MAX_EVENTS {
                s.push(user.into(), "userFills", json!({"oid":8}), format!("more-fills-{n}"));
            }
            assert!(!s.events.iter().any(|e| e.channel == "orderUpdates"));
        }
        assert_eq!(hub.order_status(request).await.unwrap()["order"]["status"], "canceled");
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn wire_updates_exclude_internal_fields_and_keep_basic_types() {
        let data = basic_update(&record());
        assert!(data.get("_localGapCount").is_none());
        assert!(data["order"].get("reduceOnly").is_none());
        assert_eq!(data["order"]["sz"], "1");
    }
}
