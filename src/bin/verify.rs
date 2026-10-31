//! Read an aligned capture without credentials; validate records and report counts.
use anyhow::{Context, Result, ensure};
use engineering_challenge::{
    Side,
    output::CHUNK,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use std::collections::{BTreeMap, HashMap};
fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: verify CAPTURE.sbe")?;
    ensure!(
        std::fs::metadata(&path)?.len() % CHUNK as u64 == 0,
        "capture has a truncated chunk"
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut books = HashMap::<String, (BTreeMap<i64, i64>, BTreeMap<i64, i64>)>::new();
    let (mut snapshots, mut updates) = (0u64, 0u64);
    let (mut replay_matches, mut replacements) = (0u64, 0u64);
    let mut previous_snapshot = HashMap::<String, chrono::DateTime<chrono::Utc>>::new();
    let mut gaps_ms = GapStats::default();
    let mut last_exchange = HashMap::new();
    let mut replacement_examples = Vec::new();
    let mut same_time_replacements = 0;
    let mut first_time = None;
    let mut last_time = None;
    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s)?;
                let mut bids = BTreeMap::new();
                let mut asks = BTreeMap::new();
                for (p, q) in s.bids.iter().map(|l| (l.price_level, l.quantity)) {
                    ensure!((0..=1_000_000).contains(&p) && q > 0, "invalid bid");
                    ensure!(bids.insert(p, q).is_none(), "duplicate bid");
                }
                for (p, q) in s.asks.iter().map(|l| (l.price_level, l.quantity)) {
                    ensure!((0..=1_000_000).contains(&p) && q > 0, "invalid ask");
                    ensure!(asks.insert(p, q).is_none(), "duplicate ask");
                }
                if let Some(previous) = books.get(&s.symbol) {
                    if previous.0 == bids && previous.1 == asks {
                        replay_matches += 1;
                    } else {
                        // Recovery snapshots can legitimately replace state after a gap.
                        replacements += 1;
                        if last_exchange.get(&s.symbol) == Some(&s.exchange_timestamp) {
                            same_time_replacements += 1;
                        }
                        if replacement_examples.len() < 5 {
                            replacement_examples.push(serde_json::json!({
                                "symbol":s.symbol, "received":s.received_at.to_rfc3339(),
                                "previous_exchange":last_exchange.get(&s.symbol).map(|t: &chrono::DateTime<chrono::Utc>| t.to_rfc3339()),
                                "snapshot_exchange":s.exchange_timestamp.to_rfc3339(),
                                "previous_snapshot":previous_snapshot.get(&s.symbol).map(|t| t.to_rfc3339())
                            }));
                        }
                    }
                }
                if let Some(previous) = previous_snapshot.insert(s.symbol.clone(), s.received_at) {
                    gaps_ms.record((s.received_at - previous).num_milliseconds());
                }
                first_time = Some(
                    first_time.map_or(s.received_at, |t: chrono::DateTime<chrono::Utc>| {
                        t.min(s.received_at)
                    }),
                );
                last_time = Some(
                    last_time.map_or(s.received_at, |t: chrono::DateTime<chrono::Utc>| {
                        t.max(s.received_at)
                    }),
                );
                last_exchange.insert(s.symbol.clone(), s.exchange_timestamp);
                books.insert(s.symbol, (bids, asks));
                snapshots += 1;
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u)?;
                ensure!(
                    (0..=1_000_000).contains(&u.price_level) && u.quantity >= 0,
                    "invalid update"
                );
                let (bids, asks) = books
                    .get_mut(&u.symbol)
                    .context("update before initial snapshot")?;
                let side = if u.side == Side::Buy { bids } else { asks };
                if u.quantity == 0 {
                    side.remove(&u.price_level);
                } else {
                    side.insert(u.price_level, u.quantity);
                }
                last_exchange.insert(u.symbol, u.exchange_timestamp);
                updates += 1;
            }
            _ => anyhow::bail!("unknown SBE message"),
        }
    }
    println!(
        "{}",
        serde_json::json!({
            "symbols":books.len(), "snapshots":snapshots, "updates":updates,
            "snapshot_replay_matches":replay_matches, "snapshot_state_replacements":replacements,
            "first_snapshot":first_time.map(|t| t.to_rfc3339()), "last_snapshot":last_time.map(|t| t.to_rfc3339()),
            "snapshot_gap_ms_min":gaps_ms.at_rank(0),
            "snapshot_gap_ms_median":gaps_ms.at_rank(gaps_ms.count / 2),
            "snapshot_gap_ms_p99":gaps_ms.at_rank(((u128::from(gaps_ms.count.saturating_sub(1)) * 99) / 100) as u64),
            "snapshot_gap_ms_max":gaps_ms.at_rank(gaps_ms.count.saturating_sub(1)),
            "state_replacements_with_same_exchange_time":same_time_replacements,
            "replacement_examples":replacement_examples
        })
    );
    Ok(())
}

/// Exact millisecond quantiles without keeping one allocation entry per snapshot.
#[derive(Default)]
struct GapStats {
    buckets: BTreeMap<i64, u64>,
    count: u64,
}
impl GapStats {
    fn record(&mut self, gap_ms: i64) {
        *self.buckets.entry(gap_ms).or_default() += 1;
        self.count += 1;
    }
    fn at_rank(&self, rank: u64) -> Option<i64> {
        let mut cumulative = 0;
        for (&gap_ms, &count) in &self.buckets {
            cumulative += count;
            if rank < cumulative {
                return Some(gap_ms);
            }
        }
        None
    }
}

#[cfg(test)]
#[path = "verify/tests.rs"]
mod tests;
