#![allow(unused_crate_dependencies)]
use std::net::Ipv4Addr;

use clap::Parser;
use server::{Result, run_websocket_server};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    /// Directory containing node_*_by_block output directories.
    #[arg(long)]
    node_data_dir: Option<std::path::PathBuf>,
    #[arg(long)]
    order_status_dir: Option<std::path::PathBuf>,
    #[arg(long)]
    book_diff_dir: Option<std::path::PathBuf>,
    #[arg(long)]
    fills_dir: Option<std::path::PathBuf>,
    /// Snapshot path as seen by this server.
    #[arg(long)]
    snapshot_path: Option<std::path::PathBuf>,
    /// Snapshot path as seen by hl-node (defaults to snapshot-path).
    #[arg(long)]
    snapshot_node_path: Option<std::path::PathBuf>,
    #[arg(long, default_value = "http://127.0.0.1:3001/info")]
    info_url: String,
    #[arg(long, value_delimiter = ',')]
    markets: Vec<String>,
    #[arg(long)]
    stream_with_block_info: bool,
    /// Wallets to index locally (no public API fallback).
    #[arg(long, env = "WS_WALLETS", value_delimiter = ',')]
    wallets: Vec<String>,
    /// Persistent SQLite wallet journal (legacy .json paths use a sibling .sqlite and import once).
    #[arg(long, env = "WS_WALLET_JOURNAL_PATH")]
    wallet_journal_path: Option<std::path::PathBuf>,
    #[arg(long, env = "WS_WALLET_POLL_INTERVAL_MS", default_value_t = 30000)]
    wallet_poll_interval_ms: u64,
    #[arg(long, env = "WS_WALLET_EVENT_INTERVAL_MS", default_value_t = 100)]
    wallet_event_interval_ms: u64,
    #[arg(long, env = "WS_WALLET_HISTORY_EVENTS", default_value_t = 100000)]
    wallet_history_events: usize,
    #[arg(long, env = "WS_WALLET_HISTORY_DAYS", default_value_t = 7)]
    wallet_history_days: u32,
    #[arg(long, default_value_t = 5)]
    poll_interval_ms: u64,
    #[arg(long, default_value_t = 5)]
    stale_after_secs: u64,
    #[arg(long, default_value_t = 120)]
    snapshot_timeout_secs: u64,
    #[arg(long, default_value_t = 30)]
    retry_interval_secs: u64,
    /// Zero disables periodic full snapshots.
    #[arg(long, default_value_t = 0)]
    integrity_interval_secs: u64,
    #[arg(long, default_value_t = 256)]
    max_buffer_mib: usize,
    /// Server address (e.g., 0.0.0.0)
    #[arg(long)]
    address: Ipv4Addr,

    /// Server port (e.g., 8000)
    #[arg(long)]
    port: u16,

    /// Compression level for WebSocket connections.
    /// Accepts values in the range `0..=9`.
    /// * `0` – compression disabled.
    /// * `1` – fastest compression, low compression ratio (default).
    /// * `9` – slowest compression, highest compression ratio.
    ///
    /// The level is passed to `flate2::Compression::new(level)`; see the
    /// documentation for <https://docs.rs/flate2/1.1.2/flate2/struct.Compression.html#method.new> for more info.
    #[arg(long)]
    websocket_compression_level: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let full_address = format!("{}:{}", args.address, args.port);
    println!("Running websocket server on {full_address}");

    let compression_level = args.websocket_compression_level.unwrap_or(/* Some compression */ 1);
    if compression_level > 9 {
        return Err("compression level must be 0..9".into());
    }
    let mut config = server::ServerConfig::default();
    if let Some(dir) = args.node_data_dir {
        config.data_dir = dir;
    }
    config.snapshot_path = args.snapshot_path.unwrap_or_else(|| config.data_dir.join("ws-snapshot.json"));
    config.snapshot_node_path = args.snapshot_node_path.unwrap_or_else(|| config.snapshot_path.clone());
    config.info_url = args.info_url;
    config.order_status_dir = args.order_status_dir;
    config.book_diff_dir = args.book_diff_dir;
    config.fills_dir = args.fills_dir;
    config.markets = args.markets.into_iter().collect();
    config.stream_with_block_info = args.stream_with_block_info;
    config.wallets =
        args.wallets.into_iter().map(|w| w.trim().to_ascii_lowercase()).filter(|w| !w.is_empty()).collect();
    config.wallet_journal_path = args.wallet_journal_path;
    config.wallet_poll_interval = std::time::Duration::from_millis(args.wallet_poll_interval_ms);
    config.wallet_event_interval = std::time::Duration::from_millis(args.wallet_event_interval_ms);
    config.wallet_history_events = args.wallet_history_events;
    config.wallet_history_days = args.wallet_history_days;
    config.poll_interval = std::time::Duration::from_millis(args.poll_interval_ms);
    config.stale_after = std::time::Duration::from_secs(args.stale_after_secs);
    config.snapshot_timeout = std::time::Duration::from_secs(args.snapshot_timeout_secs);
    config.retry_interval = std::time::Duration::from_secs(args.retry_interval_secs);
    config.integrity_interval = std::time::Duration::from_secs(args.integrity_interval_secs);
    config.max_buffer_bytes = args.max_buffer_mib.checked_mul(1024 * 1024).ok_or("buffer size overflow")?;
    run_websocket_server(&full_address, true, compression_level, config).await?;

    Ok(())
}
