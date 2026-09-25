use std::{collections::HashSet, path::PathBuf, time::Duration};

/// All paths are explicit. `snapshot_node_path` is interpreted by hl-node, not this process.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub data_dir: PathBuf,
    pub snapshot_path: PathBuf,
    pub snapshot_node_path: PathBuf,
    pub info_url: String,
    pub order_status_dir: Option<PathBuf>,
    pub book_diff_dir: Option<PathBuf>,
    pub fills_dir: Option<PathBuf>,
    pub markets: HashSet<String>,
    /// Explicit wallet allowlist; empty disables wallet ingestion.
    pub wallets: HashSet<String>,
    pub wallet_journal_path: Option<PathBuf>,
    pub wallet_poll_interval: Duration,
    pub wallet_event_interval: Duration,
    pub wallet_account_interval: Duration,
    pub wallet_history_events: usize,
    pub wallet_history_days: u32,
    pub stream_with_block_info: bool,
    pub stale_after: Duration,
    pub poll_interval: Duration,
    pub snapshot_timeout: Duration,
    pub retry_interval: Duration,
    /// Zero disables scheduled full-book comparisons.
    pub integrity_interval: Duration,
    pub max_buffer_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let data_dir = std::env::home_dir().unwrap_or_default().join("hl/data");
        Self {
            snapshot_path: data_dir.join("ws-snapshot.json"),
            snapshot_node_path: data_dir.join("ws-snapshot.json"),
            data_dir,
            info_url: "http://127.0.0.1:3001/info".into(),
            order_status_dir: None,
            book_diff_dir: None,
            fills_dir: None,
            markets: HashSet::new(),
            wallets: HashSet::new(),
            wallet_journal_path: None,
            wallet_poll_interval: Duration::from_secs(30),
            wallet_event_interval: Duration::from_millis(100),
            wallet_account_interval: Duration::from_secs(1),
            wallet_history_events: 100_000,
            wallet_history_days: 7,
            stream_with_block_info: false,
            poll_interval: Duration::from_millis(5),
            stale_after: Duration::from_secs(5),
            snapshot_timeout: Duration::from_secs(120),
            retry_interval: Duration::from_secs(30),
            integrity_interval: Duration::ZERO,
            max_buffer_bytes: 256 * 1024 * 1024,
        }
    }
}
impl ServerConfig {
    pub(crate) fn includes(&self, coin: &str) -> bool {
        !coin.starts_with('@') && (self.markets.is_empty() || self.markets.contains(coin))
    }
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.wallets.len() > 16 || self.wallets.iter().any(|w| !crate::wallet::valid_address(w)) {
            return Err("wallets must contain at most 16 valid 0x addresses".into());
        }
        if self.wallet_poll_interval < Duration::from_millis(250) {
            return Err("wallet polling interval must be at least 250 ms".into());
        }
        if self.wallet_event_interval < Duration::from_millis(10)
            || self.wallet_account_interval < Duration::from_millis(250)
            || self.wallet_account_interval > Duration::from_secs(5)
            || self.wallet_history_events < 2000
            || self.wallet_history_events > 1_000_000
            || self.wallet_history_days == 0
            || self.wallet_history_days > 3650
        {
            return Err("invalid wallet event interval or retention limits".into());
        }
        if !self.data_dir.is_dir() {
            return Err("node data directory does not exist".into());
        }
        if self.poll_interval.is_zero()
            || self.stale_after.is_zero()
            || self.retry_interval.is_zero()
            || self.snapshot_timeout.is_zero()
            || self.max_buffer_bytes < 1024
        {
            return Err("timeouts and buffer size must be positive".into());
        }
        if self.snapshot_path == self.data_dir
            || !self.snapshot_path.is_absolute()
            || !self.snapshot_node_path.is_absolute()
        {
            return Err("snapshot paths must be absolute file paths".into());
        }
        Ok(())
    }
}
