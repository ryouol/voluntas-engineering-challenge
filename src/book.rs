//! Canonical YES book. All prices and quantities are exact millionths.
use crate::{
    Side,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use std::collections::{BTreeMap, BTreeSet};

pub const SCALE: i64 = 1_000_000;
pub fn fixed(s: &str) -> Result<i64> {
    let (negative, s) = match s.strip_prefix('-') {
        Some(s) => (true, s),
        None => (false, s),
    };
    let (whole, fraction) = s.split_once('.').unwrap_or((s, ""));
    ensure!(
        !whole.is_empty() && whole.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal"
    );
    ensure!(
        fraction.len() <= 6 && fraction.bytes().all(|c| c.is_ascii_digit()),
        "invalid decimal precision"
    );
    ensure!(!s.ends_with('.'), "empty fractional part");
    let whole: i64 = whole.parse().context("decimal overflow")?;
    let fractional_places = fraction.len() as u32;
    let fraction: i64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse()?
    };
    let value = whole
        .checked_mul(SCALE)
        .and_then(|n| n.checked_add(fraction * 10_i64.pow(6 - fractional_places)))
        .context("decimal overflow")?;
    Ok(if negative { -value } else { value })
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Book {
    pub bids: BTreeMap<i64, i64>,
    pub asks: BTreeMap<i64, i64>,
    pub exchange_time: DateTime<Utc>,
}
fn price(s: &str) -> Result<i64> {
    let p = fixed(s)?;
    ensure!((0..=SCALE).contains(&p), "price out of range");
    Ok(p)
}
impl Book {
    pub fn from_snapshot(
        yes: &[(String, String)],
        no: &[(String, String)],
        exchange_time: DateTime<Utc>,
    ) -> Result<Self> {
        let mut book = Self {
            exchange_time,
            ..Default::default()
        };
        for (levels, side) in [(yes, Side::Buy), (no, Side::Sell)] {
            let mut seen = BTreeSet::new();
            for (p, q) in levels {
                let p = price(p)?;
                let p = if side == Side::Buy { p } else { SCALE - p };
                let q = fixed(q)?;
                ensure!(q >= 0, "negative snapshot quantity");
                let map = if side == Side::Buy {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                ensure!(seen.insert(p), "duplicate snapshot price");
                if q > 0 {
                    map.insert(p, q);
                }
            }
        }
        Ok(book)
    }
    pub fn update(
        &mut self,
        symbol: &str,
        side: Side,
        p: &str,
        delta: &str,
        received: DateTime<Utc>,
        exchange: DateTime<Utc>,
    ) -> Result<PriceLevelUpdateDomain> {
        let p = price(p)?;
        let p = if side == Side::Buy { p } else { SCALE - p };
        let delta = fixed(delta)?;
        let map = if side == Side::Buy {
            &mut self.bids
        } else {
            &mut self.asks
        };
        let q = map
            .get(&p)
            .copied()
            .unwrap_or(0)
            .checked_add(delta)
            .context("quantity overflow")?;
        ensure!(q >= 0, "delta produces negative quantity");
        if q == 0 {
            map.remove(&p);
        } else {
            map.insert(p, q);
        }
        self.exchange_time = exchange;
        Ok(PriceLevelUpdateDomain {
            symbol: symbol.into(),
            side,
            price_level: p,
            quantity: q,
            backtest_timestamp: received,
            exchange_timestamp: exchange,
        })
    }
    pub fn snapshot(&self, symbol: &str, received: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            symbol: symbol.into(),
            received_at: received,
            exchange_timestamp: self.exchange_time,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&p, &q)| SnapshotBidsEntryDomain {
                    price_level: p,
                    quantity: q,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&p, &q)| SnapshotAsksEntryDomain {
                    price_level: p,
                    quantity: q,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
#[path = "book_tests.rs"]
mod tests;
