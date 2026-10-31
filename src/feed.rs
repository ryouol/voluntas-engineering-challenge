use crate::{Side, book::Book, output::Record};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use tokio::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: Option<u64>,
    pub sid: Option<u64>,
    pub seq: Option<u64>,
    pub sending_ts_ms: Option<i64>,
    #[serde(default)]
    pub msg: Value,
}
#[derive(Deserialize)]
struct Snapshot {
    market_ticker: String,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}
#[derive(Deserialize)]
struct Delta {
    market_ticker: String,
    price_dollars: String,
    delta_fp: String,
    side: WireSide,
    ts_ms: Option<i64>,
}
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum WireSide {
    Yes,
    No,
}
fn timestamp(ms: Option<i64>) -> Result<DateTime<Utc>> {
    match ms {
        Some(ms) => {
            ensure!(ms >= 0, "negative exchange timestamp");
            DateTime::from_timestamp_millis(ms).context("invalid exchange timestamp")
        }
        None => Ok(DateTime::UNIX_EPOCH),
    }
}
struct Pending {
    id: u64,
    tickers: Vec<String>,
    add: bool,
    started: Instant,
    initial: bool,
}
/// Owned by exactly one socket task; discarded in its entirety on reconnect.
#[derive(Default)]
pub struct Feed {
    pub desired: BTreeSet<String>,
    pub acknowledged: BTreeSet<String>,
    pub books: HashMap<String, Book>,
    sid: Option<u64>,
    seq: Option<u64>,
    next_id: u64,
    pending: Option<Pending>,
    pending_removals: BTreeSet<String>,
    awaiting: HashMap<String, Instant>,
}
impl Feed {
    pub fn set_desired(&mut self, desired: BTreeSet<String>) {
        // Once a removed stream is ignored, finish deleting it before reactivation.
        // Otherwise its acknowledgment could survive without a valid book.
        for symbol in self.desired.difference(&desired) {
            if self.acknowledged.contains(symbol)
                || self
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.add && p.tickers.contains(symbol))
            {
                self.pending_removals.insert(symbol.clone());
            }
        }
        self.books.retain(|s, _| desired.contains(s));
        self.awaiting.retain(|s, _| desired.contains(s));
        self.desired = desired;
    }
    pub fn command(&mut self) -> Result<Option<Value>> {
        if let Some(p) = &self.pending {
            ensure!(
                p.started.elapsed() < Duration::from_secs(20),
                "subscription acknowledgment timeout"
            );
            return Ok(None);
        }
        for t in self.awaiting.values() {
            ensure!(
                t.elapsed() < Duration::from_secs(30),
                "initial snapshot timeout"
            );
        }
        let remove: Vec<_> = self
            .acknowledged
            .iter()
            .filter(|symbol| {
                !self.desired.contains(*symbol) || self.pending_removals.contains(*symbol)
            })
            .take(100)
            .cloned()
            .collect();
        let (tickers, add) = if remove.is_empty() {
            (
                self.desired
                    .difference(&self.acknowledged)
                    .take(100)
                    .cloned()
                    .collect::<Vec<_>>(),
                true,
            )
        } else {
            (remove, false)
        };
        if tickers.is_empty() {
            return Ok(None);
        }
        self.next_id += 1;
        let initial = self.sid.is_none();
        let command = if let Some(sid) = self.sid {
            json!({
                "id": self.next_id,
                "cmd": "update_subscription",
                "params": {
                    "sid": sid,
                    "market_tickers": tickers,
                    "action": if add { "add_markets" } else { "delete_markets" }
                }
            })
        } else {
            json!({
                "id": self.next_id,
                "cmd": "subscribe",
                "params": { "channels": ["orderbook_delta"], "market_tickers": tickers }
            })
        };
        if add {
            for s in &tickers {
                self.awaiting.insert(s.clone(), Instant::now());
            }
        }
        self.pending = Some(Pending {
            id: self.next_id,
            tickers,
            add,
            started: Instant::now(),
            initial,
        });
        Ok(Some(command))
    }
    pub fn receive(&mut self, env: Envelope, received: DateTime<Utc>) -> Result<Option<Record>> {
        if env.kind == "error" {
            bail!("exchange rejected subscription command (id {:?})", env.id);
        }
        if let Some(sid) = env.sid {
            ensure!(self.sid == Some(sid), "unexpected subscription ID");
            if let Some(seq) = env.seq {
                if let Some(previous) = self.seq {
                    ensure!(
                        previous.checked_add(1) == Some(seq),
                        "subscription sequence discontinuity"
                    );
                }
                self.seq = Some(seq);
            }
        }
        match env.kind.as_str() {
            "subscribed" | "ok" => {
                let p = self
                    .pending
                    .as_ref()
                    .context("unexpected subscription acknowledgment")?;
                ensure!(env.id == Some(p.id), "acknowledgment command mismatch");
                ensure!(
                    (env.kind == "subscribed") == p.initial,
                    "unexpected acknowledgment type"
                );
                if p.initial {
                    ensure!(
                        env.msg["channel"] == "orderbook_delta",
                        "wrong channel acknowledgment"
                    );
                    self.sid = Some(env.msg["sid"].as_u64().context("missing subscription ID")?);
                }
                let p = self.pending.take().unwrap();
                for s in p.tickers {
                    if p.add {
                        self.acknowledged.insert(s);
                    } else {
                        self.acknowledged.remove(&s);
                        self.pending_removals.remove(&s);
                    }
                }
                Ok(None)
            }
            "orderbook_snapshot" => {
                ensure!(
                    env.sid.is_some() && env.seq.is_some(),
                    "snapshot missing sequence"
                );
                // Kalshi omits both side fields for a genuinely empty book.
                // Reject obsolete numeric fields rather than silently losing their levels.
                ensure!(
                    env.msg.get("yes").is_none() && env.msg.get("no").is_none(),
                    "unexpected legacy snapshot fields"
                );
                let s: Snapshot = serde_json::from_value(env.msg)?;
                if !self.desired.contains(&s.market_ticker)
                    || self.pending_removals.contains(&s.market_ticker)
                {
                    return Ok(None);
                }
                let book = Book::from_snapshot(
                    &s.yes_dollars_fp,
                    &s.no_dollars_fp,
                    timestamp(env.sending_ts_ms)?,
                )?;
                let snapshot = book.snapshot(&s.market_ticker, received);
                self.awaiting.remove(&s.market_ticker);
                self.books.insert(s.market_ticker, book);
                Ok(Some(Record::Snapshot(snapshot)))
            }
            "orderbook_delta" => {
                ensure!(
                    env.sid.is_some() && env.seq.is_some(),
                    "delta missing sequence"
                );
                let d: Delta = serde_json::from_value(env.msg)?;
                if !self.desired.contains(&d.market_ticker)
                    || self.pending_removals.contains(&d.market_ticker)
                {
                    return Ok(None);
                }
                let book = self
                    .books
                    .get_mut(&d.market_ticker)
                    .context("delta before snapshot")?;
                let side = match d.side {
                    WireSide::Yes => Side::Buy,
                    WireSide::No => Side::Sell,
                };
                Ok(Some(Record::Update(book.update(
                    &d.market_ticker,
                    side,
                    &d.price_dollars,
                    &d.delta_fp,
                    received,
                    timestamp(d.ts_ms.or(env.sending_ts_ms))?,
                )?)))
            }
            "unsubscribed" => bail!("unexpected unsubscribe"),
            _ => Ok(None),
        }
    }
    pub fn snapshots(&self, received: DateTime<Utc>) -> Vec<Record> {
        let mut symbols: Vec<_> = self.books.keys().collect();
        symbols.sort();
        symbols
            .into_iter()
            .map(|s| Record::Snapshot(self.books[s].snapshot(s, received)))
            .collect()
    }
}

#[cfg(test)]
#[path = "feed_tests.rs"]
mod tests;
