use crate::{
    listeners::order_book::{L2Requests, L2SnapshotParams, L2Snapshots},
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

// Preserve the reference rounding chain: 5/mantissa=5 comes from 5/default,
// and 4 significant figures comes from 5/mantissa=5. Only compute ancestors
// needed by requested variants, and never publish those intermediate variants.
fn rounding_parent(params: L2SnapshotParams) -> Option<L2SnapshotParams> {
    match (params.n_sig_figs, params.mantissa) {
        (Some(5), None) => Some(L2SnapshotParams::new(None, None)),
        (Some(5), Some(2 | 5)) => Some(L2SnapshotParams::new(Some(5), None)),
        (Some(4), None) => Some(L2SnapshotParams::new(Some(5), Some(5))),
        (Some(n @ 2..=3), None) => Some(L2SnapshotParams::new(Some(n + 1), None)),
        _ => None,
    }
}

pub(super) fn compute_requested_l2_snapshots<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    requested: &L2Requests,
) -> L2Snapshots {
    L2Snapshots(
        requested
            .par_iter()
            .filter_map(|(coin, wanted)| {
                let book = order_books.as_ref().get(coin)?;
                let mut needed = wanted.clone();
                for params in wanted {
                    let mut parent = rounding_parent(*params);
                    while let Some(p) = parent {
                        needed.insert(p);
                        parent = rounding_parent(p);
                    }
                }
                let mut computed = HashMap::<L2SnapshotParams, Snapshot<InnerLevel>>::new();
                for (figs, mantissa) in [
                    (None, None),
                    (Some(5), None),
                    (Some(5), Some(2)),
                    (Some(5), Some(5)),
                    (Some(4), None),
                    (Some(3), None),
                    (Some(2), None),
                ] {
                    let key = L2SnapshotParams::new(figs, mantissa);
                    if !needed.contains(&key) {
                        continue;
                    }
                    let snapshot = if let Some(parent) = rounding_parent(key) {
                        computed.get(&parent).map(|s| s.to_l2_snapshot(None, figs, mantissa))
                    } else {
                        Some(book.to_l2_snapshot(None, None, None))
                    };
                    if let Some(snapshot) = snapshot {
                        computed.insert(key, snapshot);
                    }
                }
                computed.retain(|p, _| wanted.contains(p));
                Some((coin.clone(), computed))
            })
            .collect(),
    )
}

#[cfg(test)]
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
    fn populated_books() -> OrderBooks<crate::types::inner::InnerL4Order> {
        use crate::order_book::{Px, Side, Sz};
        use crate::types::inner::InnerL4Order;
        let mut snapshots = HashMap::new();
        for (market, base) in [
            ("BTC", 10_000_000_000_000_u64),
            ("HYPE", 4_000_000_000),
            ("AERO", 3_000_000),
            ("xyz:NVDA", 18_000_000_000),
            ("xyz:DRAM", 25_000_000_000),
        ] {
            let mut sides = [Vec::new(), Vec::new()];
            for (i, side) in [Side::Bid, Side::Ask].into_iter().enumerate() {
                for n in 0..250_u64 {
                    let px = if i == 0 { base - n * (base / 100_000) } else { base + (n + 1) * (base / 100_000) };
                    sides[i].push(InnerL4Order {
                        user: alloy::primitives::Address::ZERO,
                        coin: Coin::new(market),
                        side,
                        limit_px: Px::new(px),
                        sz: Sz::new((n + 1) * 100),
                        oid: n + i as u64 * 1000,
                        timestamp: 0,
                        trigger_condition: String::new(),
                        is_trigger: false,
                        trigger_px: String::new(),
                        is_position_tpsl: false,
                        reduce_only: false,
                        order_type: String::new(),
                        tif: None,
                        cloid: None,
                    });
                }
            }
            snapshots.insert(Coin::new(market), Snapshot::new(sides));
        }
        OrderBooks::from_snapshots(Snapshots::new(snapshots), false)
    }
    #[test]
    fn demanded_variants_match_reference_at_every_depth() {
        let books = populated_books();
        let reference = compute_l2_snapshots(&books);
        for (coin, variants) in reference.as_ref() {
            for (params, expected) in variants {
                let requested = L2Requests::from([(coin.clone(), std::collections::HashSet::from([*params]))]);
                let selected = compute_requested_l2_snapshots(&books, &requested);
                assert_eq!(selected.as_ref().len(), 1);
                let actual = &selected.as_ref()[coin];
                assert_eq!(actual.len(), 1, "intermediate variants must not leak");
                for depth in [1, 5, 20, 100, 1000] {
                    assert_eq!(
                        serde_json::to_value(actual[params].truncate(depth).export_inner_snapshot()).unwrap(),
                        serde_json::to_value(expected.truncate(depth).export_inner_snapshot()).unwrap()
                    );
                }
            }
        }
        assert!(compute_requested_l2_snapshots(&books, &L2Requests::new()).as_ref().is_empty());
        let all = reference.as_ref().iter().map(|(c, v)| (c.clone(), v.keys().copied().collect())).collect();
        let actual = compute_requested_l2_snapshots(&books, &all);
        for (coin, variants) in reference.as_ref() {
            for (params, expected) in variants {
                assert_eq!(
                    serde_json::to_value(actual.as_ref()[coin][params].clone().export_inner_snapshot()).unwrap(),
                    serde_json::to_value(expected.clone().export_inner_snapshot()).unwrap()
                );
            }
        }
    }
    #[test]
    #[ignore = "manual release-mode timing comparison, not a performance assertion"]
    fn measure_requested_l2_work() {
        let books = populated_books();
        let requested = L2Requests::from([(
            Coin::new("BTC"),
            std::collections::HashSet::from([L2SnapshotParams::new(None, None)]),
        )]);
        // Warm the worker pool and allocator for both paths before timing.
        std::hint::black_box(compute_l2_snapshots(&books));
        std::hint::black_box(compute_requested_l2_snapshots(&books, &requested));
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            std::hint::black_box(compute_l2_snapshots(&books));
        }
        let reference = start.elapsed();
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            std::hint::black_box(compute_requested_l2_snapshots(&books, &requested));
        }
        eprintln!(
            "1000 iterations; five markets, 250 orders/side: all variants={reference:?}, one BTC unrounded={:?}",
            start.elapsed()
        );
    }
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
