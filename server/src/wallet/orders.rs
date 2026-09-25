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
            if !s.ready || s.journal_error.is_some() {
                return Err(format!("{UNAVAILABLE}wallet stream is not ready"));
            }
            let hot = s
                .events
                .iter()
                .rev()
                .find(|e| e.user == user && e.channel == "orderUpdates" && identity_matches(&e.data["order"], &oid))
                .cloned();
            if let Some(e) = &hot {
                let fill = s
                    .events
                    .iter()
                    .rev()
                    .find(|f| f.user == user && f.channel == "userFills" && f.data["oid"] == e.data["order"]["oid"])
                    .map_or(0, |f| f.seq);
                return reply(e.data.clone(), s.gaps, fill, e.seq);
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
        if !s.ready || s.epoch != epoch || s.seq != seq || s.journal_error.is_some() {
            return Err(format!("{UNAVAILABLE}wallet changed during historical lookup; retry"));
        }
        let Some((seq, data, fill)) = record else {
            return Err(format!("{UNAVAILABLE}no retained authoritative record for this identity"));
        };
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
    #[test]
    fn wire_updates_exclude_internal_fields_and_keep_basic_types() {
        let data = basic_update(&record());
        assert!(data.get("_localGapCount").is_none());
        assert!(data["order"].get("reduceOnly").is_none());
        assert_eq!(data["order"]["sz"], "1");
    }
}
