//! Shared authoritative account sampling. No public fallback and no balance synthesis.
use super::*;
use crate::servers::info::{InfoBridge, PostRequest};
use futures_util::{StreamExt, stream};
use tokio::sync::{Mutex as AsyncMutex, Semaphore};

#[derive(Clone)]
pub(crate) struct Sample {
    pub data: Value,
    pub started: i64,
    pub completed: i64,
    pub upstream: Value,
    pub epoch: u64,
}
impl Sample {
    pub(crate) fn usable(&self) -> bool {
        let now = chrono::Utc::now().timestamp_millis();
        now.saturating_sub(self.started) <= 5000
            && self.completed >= self.started
            && self.completed - self.started <= 1000
            && self
                .upstream
                .as_object()
                .is_some_and(|times| times.values().all(|t| t.as_i64().is_some_and(|t| fresh(t).is_ok())))
    }
}
#[derive(Default)]
struct Cache {
    fetched: Option<Instant>,
    sample: Option<Sample>,
}
#[derive(Default)]
struct Catalog {
    fetched: Option<Instant>,
    names: Vec<String>,
}
type Caches = Mutex<HashMap<WalletSubscription, Arc<AsyncMutex<Cache>>>>;
pub(crate) struct Accounts {
    caches: Caches,
    catalog: AsyncMutex<Catalog>,
    slots: Semaphore,
    pub interval: Duration,
}
impl Accounts {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            caches: Mutex::new(HashMap::new()),
            catalog: AsyncMutex::new(Catalog::default()),
            slots: Semaphore::new(4),
            interval,
        }
    }
    async fn query(&self, bridge: &InfoBridge, payload: Value) -> Result<Value, String> {
        let _permit = self.slots.acquire().await.map_err(|e| e.to_string())?;
        let response =
            bridge.execute(PostRequest { id: 0, request: json!({"type":"info","payload":payload}) }).await.response;
        if response["type"] != "info" {
            return Err(response["payload"].to_string());
        }
        Ok(response["payload"]["data"].clone())
    }
    async fn dexes(&self, bridge: &InfoBridge) -> Result<Vec<String>, String> {
        let mut catalog = self.catalog.lock().await;
        if catalog.fetched.is_some_and(|t| t.elapsed() < Duration::from_secs(60)) {
            return Ok(catalog.names.clone());
        }
        let value = self.query(bridge, json!({"type":"perpDexs"})).await?;
        let mut names = vec![String::new()];
        for entry in value.as_array().ok_or("invalid perpDexs array")? {
            if entry.is_null() {
                continue;
            }
            let name = entry["name"].as_str().ok_or("invalid perpDexs entry")?;
            if name.is_empty()
                || name.len() > 64
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || names.iter().any(|s| s == name)
            {
                return Err("invalid or duplicate DEX name".into());
            }
            names.push(name.to_string());
        }
        if names.len() > 64 {
            return Err("DEX catalog exceeds 64 venues; no partial account snapshot published".into());
        }
        catalog.names = names.clone();
        catalog.fetched = Some(Instant::now());
        Ok(names)
    }
    pub(crate) async fn sample(
        &self,
        bridge: InfoBridge,
        sub: WalletSubscription,
        epoch: u64,
    ) -> Result<Sample, String> {
        let cache = {
            let mut caches = self.caches.lock().unwrap_or_else(|e| e.into_inner());
            caches.entry(sub.clone()).or_default().clone()
        };
        let mut cache = cache.lock().await;
        if cache.fetched.is_some_and(|t| t.elapsed() < self.interval) {
            if let Some(sample) = &cache.sample {
                if sample.epoch == epoch && sample.usable() {
                    return Ok(sample.clone());
                }
            }
        }
        let result = tokio::time::timeout(Duration::from_secs(2), self.fetch(&bridge, &sub, epoch))
            .await
            .map_err(|_| "account sampling exceeded 2 seconds")?;
        // Failed refreshes never extend a previous sample's lease.
        match result {
            Ok(sample) => {
                cache.fetched = Some(Instant::now());
                cache.sample = Some(sample.clone());
                Ok(sample)
            }
            Err(e) => {
                cache.sample = None;
                Err(e)
            }
        }
    }
    async fn fetch(&self, bridge: &InfoBridge, sub: &WalletSubscription, epoch: u64) -> Result<Sample, String> {
        let started = chrono::Utc::now().timestamp_millis();
        let user = sub.user();
        let (data, upstream) = match sub {
            WalletSubscription::AllDexsClearinghouseState { .. } => {
                let dexes = self.dexes(bridge).await?;
                let mut states = stream::iter(dexes.into_iter().map(|dex| async move {
                    let data = self.query(bridge, json!({"type":"clearinghouseState","user":user,"dex":dex})).await?;
                    validate_perp(&data)?;
                    Ok::<_, String>((dex, data))
                }))
                .buffered(4);
                let mut pairs = Vec::new();
                let mut times = serde_json::Map::new();
                let mut bytes = 0;
                while let Some(result) = states.next().await {
                    let (dex, state) = result?;
                    bytes += dex.len() + state.to_string().len();
                    if bytes > 2 * 1024 * 1024 {
                        return Err("complete account sample exceeds 2 MiB; no partial state published".into());
                    }
                    times.insert(dex.clone(), state["time"].clone());
                    pairs.push(json!([dex, state]));
                }
                (json!({"user":user,"clearinghouseStates":pairs}), Value::Object(times))
            }
            WalletSubscription::SpotState { .. } => {
                let data = self.query(bridge, json!({"type":"spotClearinghouseState","user":user})).await?;
                validate_spot(&data)?;
                // The spot payload has no exchange timestamp. Report an independently
                // observed node clock, never mislabel it as the spot snapshot timestamp.
                let head = self.query(bridge, json!({"type":"exchangeStatus"})).await?;
                fresh(head["time"].as_i64().ok_or("exchangeStatus missing integer time")?)?;
                (json!({"user":user,"spotState":data}), json!({"observedNodeTime":head["time"]}))
            }
            _ => return Err("not an account subscription".into()),
        };
        let sample = Sample { data, started, completed: chrono::Utc::now().timestamp_millis(), upstream, epoch };
        if !sample.usable() {
            return Err("account sampling span exceeds 1000ms or upstream freshness limit".into());
        }
        Ok(sample)
    }
}
fn fresh(time: i64) -> Result<(), String> {
    let age = chrono::Utc::now().timestamp_millis().saturating_sub(time);
    if !(-1000..=5000).contains(&age) {
        return Err("account upstream timestamp is stale or in the future".into());
    }
    Ok(())
}
fn amount(value: &Value) -> bool {
    value.as_str().is_some_and(|s| s.len() <= 128 && s.parse::<f64>().is_ok_and(f64::is_finite))
}
pub(super) fn validate_perp(v: &Value) -> Result<(), String> {
    for key in ["marginSummary", "crossMarginSummary"] {
        for field in ["accountValue", "totalNtlPos", "totalRawUsd", "totalMarginUsed"] {
            if !amount(&v[key][field]) {
                return Err(format!("invalid {key}.{field}"));
            }
        }
    }
    if !amount(&v["withdrawable"]) || !amount(&v["crossMaintenanceMarginUsed"]) {
        return Err("invalid perp margin values".into());
    }
    for entry in v["assetPositions"].as_array().ok_or("missing assetPositions")? {
        let p = &entry["position"];
        if !p["coin"].is_string() || !amount(&p["szi"]) || !amount(&p["positionValue"]) {
            return Err("invalid perp position".into());
        }
        if !p["entryPx"].is_null() && !amount(&p["entryPx"]) {
            return Err("invalid position entry price".into());
        }
    }
    fresh(v["time"].as_i64().ok_or("missing perp timestamp")?)
}
pub(super) fn validate_spot(v: &Value) -> Result<(), String> {
    let mut seen = HashSet::new();
    for row in v["balances"].as_array().ok_or("missing spot balances")? {
        let token = row["token"].as_u64().ok_or("invalid spot token id")?;
        if !seen.insert(token)
            || !row["coin"].is_string()
            || !["total", "hold", "entryNtl"].iter().all(|k| amount(&row[k]))
        {
            return Err("invalid or duplicate spot balance".into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn account_types_and_coverage_fail_closed() {
        let mut spot = json!({"balances":[{"coin":"TEST","token":734,"total":"2.0","hold":"0","entryNtl":"1.0"}]});
        assert!(validate_spot(&spot).is_ok());
        spot["balances"][0]["total"] = json!(2.0);
        assert!(validate_spot(&spot).is_err());
        assert!(validate_spot(&json!({})).is_err());
        let summary = json!({"accountValue":"0","totalNtlPos":"0","totalRawUsd":"0","totalMarginUsed":"0"});
        let mut perp = json!({"marginSummary":summary,"crossMarginSummary":summary,"crossMaintenanceMarginUsed":"0","withdrawable":"0","assetPositions":[],"time":chrono::Utc::now().timestamp_millis()});
        assert!(validate_perp(&perp).is_ok());
        perp["time"] = json!(0);
        assert!(validate_perp(&perp).is_err());
    }
    #[test]
    fn cached_sample_lease_does_not_renew_on_delivery() {
        let now = chrono::Utc::now().timestamp_millis();
        let mut s = Sample { data: json!({}), started: now - 20, completed: now, upstream: json!({"":now}), epoch: 0 };
        assert!(s.usable());
        s.started = now - 1001;
        assert!(!s.usable());
        s.started = now - 6000;
        s.completed = now - 5900;
        assert!(!s.usable());
        s.started = now - 20;
        s.completed = now;
        s.upstream = json!({"":now-6000});
        assert!(!s.usable());
    }
}
