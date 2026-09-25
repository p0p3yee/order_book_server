use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::inner::InnerLevel,
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::{collections::HashMap, path::PathBuf};

pub(super) async fn process_rmp_file(config: &crate::ServerConfig) -> Result<PathBuf> {
    // Unique names prevent a timed-out request from racing with a subsequent attempt.
    let id = format!("{}.{}", std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default());
    let output_path = config.snapshot_path.with_extension(format!("{id}.json"));
    let node_path = config.snapshot_node_path.with_extension(format!("{id}.json"));
    let payload = json!({
        "type": "fileSnapshot",
        "request": { "type": "l4Snapshots", "includeUsers": true, "includeTriggerOrders": false },
        "outPath": node_path, "includeHeightInOutput": true
    });
    let start = std::time::Instant::now();
    Client::builder()
        .timeout(config.snapshot_timeout)
        .build()?
        .post(&config.info_url)
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    log::info!("snapshot_generation_ms={}", start.elapsed().as_millis());
    Ok(output_path)
}

pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
    let mut snapshot_map: HashMap<_, _> =
        expected.value().into_iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                if orders1.len() != orders2.len() {
                    return Err(format!("Order count mismatch for {}", coin.value()).into());
                }
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
            .map(|(coin, order_book)| {
                let mut entries = Vec::new();
                let snapshot = order_book.to_l2_snapshot(None, None, None);
                entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
                let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                    if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                        let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                        entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                    }
                };
                for n_sig_figs in (2..=5).rev() {
                    if n_sig_figs == 5 {
                        for mantissa in [None, Some(2), Some(5)] {
                            if mantissa == Some(5) {
                                // Some(2) is NOT a superset of this info!
                                add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                            } else {
                                add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                            }
                        }
                    } else {
                        add_new_snapshot(Some(n_sig_figs), None, 1);
                    }
                }
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::Coin;
    fn snapshot(orders: Vec<u64>) -> Snapshots<u64> {
        Snapshots::new(HashMap::from([(Coin::new("BTC"), Snapshot::new([orders, vec![]]))]))
    }
    #[test]
    fn detects_extra_orders_even_when_prefix_matches() {
        assert!(validate_snapshot_consistency(&snapshot(vec![1]), snapshot(vec![1, 2]), false).is_err());
        assert!(validate_snapshot_consistency(&snapshot(vec![1, 2]), snapshot(vec![1]), false).is_err());
    }
    #[test]
    fn detects_order_value_and_market_changes() {
        assert!(validate_snapshot_consistency(&snapshot(vec![1]), snapshot(vec![2]), false).is_err());
        let other = Snapshots::new(HashMap::from([(Coin::new("HYPE"), Snapshot::new([vec![1], vec![]]))]));
        assert!(validate_snapshot_consistency(&snapshot(vec![1]), other, false).is_err());
    }
}
