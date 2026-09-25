use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

pub(crate) const MAX_PENDING: usize = 4;
const MAX_RESPONSE: usize = 2 * 1024 * 1024;
// Explicit read-only node queries. fileSnapshot is intentionally absent: callers
// must not be able to reintroduce the expensive snapshot loop via this adapter.
pub(crate) const QUERIES: &[&str] = &[
    "l2Book",
    "meta",
    "spotMeta",
    "clearinghouseState",
    "spotClearinghouseState",
    "openOrders",
    "exchangeStatus",
    "frontendOpenOrders",
    "activeAssetData",
    "maxMarketOrderNtls",
    "vaultSummaries",
    "userVaultEquities",
    "leadingVaults",
    "extraAgents",
    "subAccounts",
    "userFees",
    "userRateLimit",
    "spotDeployState",
    "perpDeployAuctionStatus",
    "delegations",
    "delegatorSummary",
    "maxBuilderFee",
    "userToMultiSigSigners",
    "userRole",
    "perpsAtOpenInterestCap",
    "validatorL1Votes",
    "marginTable",
    "perpDexs",
    "webData2",
];

#[derive(Debug, Deserialize)]
pub(crate) struct PostRequest {
    pub id: u64,
    pub request: Value,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PostResponse {
    pub id: u64,
    pub response: Value,
}
impl PostResponse {
    pub(crate) fn error(id: u64, message: impl ToString) -> Self {
        Self { id, response: json!({"type":"error","payload":message.to_string()}) }
    }
}

#[derive(Clone)]
pub(crate) struct InfoBridge {
    client: reqwest::Client,
    url: String,
    slots: Arc<Semaphore>,
}
impl InfoBridge {
    pub(crate) fn acquire(&self) -> Result<tokio::sync::OwnedSemaphorePermit, String> {
        self.slots.clone().try_acquire_owned().map_err(|_| "429: node Info concurrency limit reached".to_string())
    }
    pub(crate) fn new(url: String) -> crate::Result<Self> {
        Ok(Self {
            url,
            slots: Arc::new(Semaphore::new(16)),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .connect_timeout(Duration::from_millis(500))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
    pub(crate) async fn execute(&self, post: PostRequest) -> PostResponse {
        match self.query(&post.request).await {
            Ok(response) => PostResponse { id: post.id, response },
            Err(message) => PostResponse::error(post.id, message),
        }
    }
    async fn query(&self, request: &Value) -> Result<Value, String> {
        let (kind, body) = validated_payload(request)?;
        let _permit = self.acquire()?;
        let mut response =
            self.client.post(&self.url).header("Content-Type", "application/json").body(body).send().await.map_err(
                |e| {
                    if e.is_timeout() { "504: node Info request timed out" } else { "502: node Info request failed" }
                        .to_string()
                },
            )?;
        let status = response.status();
        if response.content_length().is_some_and(|n| n > MAX_RESPONSE as u64) {
            return Err("502: node Info response exceeds 2 MiB limit".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) =
            response.chunk().await.map_err(|_| "502: failed to read node Info response".to_string())?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err("502: node Info response exceeds 2 MiB limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            return Err(format!(
                "{}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes).chars().take(512).collect::<String>()
            ));
        }
        let data: Value =
            serde_json::from_slice(&bytes).map_err(|_| "502: node Info returned invalid JSON".to_string())?;
        Ok(json!({"type":"info","payload":{"type":kind,"data":data}}))
    }
}
fn validated_payload(request: &Value) -> Result<(String, String), String> {
    if request["type"] != "info" {
        return Err("400: only read-only Info posts are supported; signed actions are not supported".into());
    }
    let payload = &request["payload"];
    let kind = payload["type"].as_str().ok_or("400: Info payload requires a type")?;
    if !QUERIES.contains(&kind) {
        return Err(format!("400: unsupported node Info query {kind}; see /capabilities"));
    }
    let body = payload.to_string();
    if body.len() > 64 * 1024 {
        return Err("413: Info payload exceeds 64 KiB limit".into());
    }
    Ok((kind.to_string(), body))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn global_limit_rejects_without_contacting_node_and_releases_slots() {
        let bridge = InfoBridge::new("http://127.0.0.1:1/info".into()).unwrap();
        let permits: Vec<_> = (0..16).map(|_| bridge.acquire().unwrap()).collect();
        let response = bridge
            .execute(PostRequest {
                id: 7,
                request: json!({"type":"info","payload":{"type":"openOrders","user":"unused"}}),
            })
            .await;
        assert_eq!(response.id, 7);
        assert!(response.response["payload"].as_str().unwrap().starts_with("429:"));
        drop(permits);
        assert!(bridge.acquire().is_ok());
    }
    #[test]
    fn readonly_payload_preserves_wallet_and_dex_but_rejects_snapshots_and_actions() {
        let p = json!({"type":"openOrders","user":"0x1234","dex":"xyz"});
        let (kind, body) = validated_payload(&json!({"type":"info","payload":p})).unwrap();
        assert_eq!(kind, "openOrders");
        assert_eq!(serde_json::from_str::<Value>(&body).unwrap(), p);
        for r in [
            json!({"type":"info","payload":{"type":"fileSnapshot"}}),
            json!({"type":"action","payload":{"type":"order"}}),
            json!({"type":"info"}),
        ] {
            assert!(validated_payload(&r).is_err());
        }
        assert!(
            validated_payload(&json!({"type":"info","payload":{"type":"openOrders","user":"x".repeat(65536)}}))
                .is_err()
        );
    }
    #[test]
    fn errors_keep_request_id() {
        let value = serde_json::to_value(PostResponse::error(123, "504: timeout")).unwrap();
        assert_eq!(value, json!({"id":123,"response":{"type":"error","payload":"504: timeout"}}));
    }
}
