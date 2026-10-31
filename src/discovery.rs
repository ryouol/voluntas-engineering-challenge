use crate::config::Config;
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
#[derive(Debug, Deserialize)]
pub struct Market {
    pub ticker: String,
    pub status: String,
    #[serde(default)]
    pub mve_collection_ticker: Option<String>,
    #[serde(default)]
    pub mve_selected_legs: Option<Vec<serde_json::Value>>,
}
impl Market {
    pub fn eligible(&self) -> bool {
        self.status == "active"
            && self
                .mve_collection_ticker
                .as_deref()
                .unwrap_or("")
                .is_empty()
            && self.mve_selected_legs.as_ref().is_none_or(Vec::is_empty)
    }
}
#[derive(Deserialize)]
struct Page {
    markets: Vec<Market>,
    cursor: Option<String>,
}
#[derive(Clone)]
pub struct Discovery {
    client: reqwest::Client,
    base: String,
    filter: BTreeSet<String>,
    next_request: Arc<tokio::sync::Mutex<tokio::time::Instant>>,
}
impl Discovery {
    pub fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            base: config.rest_url.trim_end_matches('/').into(),
            filter: config.symbols.iter().cloned().collect(),
            next_request: Arc::new(tokio::sync::Mutex::new(tokio::time::Instant::now())),
        })
    }
    fn selected(&self, m: &Market) -> bool {
        m.eligible() && (self.filter.is_empty() || self.filter.contains(&m.ticker))
    }
    async fn get<T: serde::de::DeserializeOwned>(&self, url: reqwest::Url) -> Result<T> {
        let mut delay = Duration::from_millis(500);
        for attempt in 0..4 {
            // One shared ceiling for discovery and lifecycle lookups, including retries.
            {
                let mut next = self.next_request.lock().await;
                tokio::time::sleep_until(*next).await;
                *next = tokio::time::Instant::now() + Duration::from_millis(100);
            }
            match self.client.get(url.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    match response.json().await {
                        Ok(page) => return Ok(page),
                        Err(error) if attempt == 3 => {
                            return Err(error).context("decode market response");
                        }
                        Err(_) => {} // Retry interrupted/truncated response bodies on the same page.
                    }
                }
                Ok(response) => {
                    let status = response.status();
                    if !(status == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || status.is_server_error())
                        || attempt == 3
                    {
                        anyhow::bail!("market request failed: {status}");
                    }
                    if let Some(seconds) = response
                        .headers()
                        .get("retry-after")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        delay = Duration::from_secs(seconds.min(30));
                    }
                }
                Err(error) => {
                    if attempt == 3 {
                        return Err(error).context("market request failed");
                    }
                }
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
        unreachable!()
    }
    pub async fn all(&self) -> Result<BTreeSet<String>> {
        let mut result = BTreeSet::new();
        let mut cursor = String::new();
        let mut seen = HashSet::new();
        let mut pages = 0usize;
        loop {
            let mut url = reqwest::Url::parse(&format!("{}/markets", self.base))?;
            url.query_pairs_mut().extend_pairs([
                ("status", "open"),
                ("mve_filter", "exclude"),
                ("limit", "1000"),
            ]);
            if !self.filter.is_empty() {
                url.query_pairs_mut().append_pair(
                    "tickers",
                    &self.filter.iter().cloned().collect::<Vec<_>>().join(","),
                );
            }
            if !cursor.is_empty() {
                url.query_pairs_mut().append_pair("cursor", &cursor);
            }
            let page: Page = self.get(url).await?;
            pages += 1;
            if pages.is_multiple_of(25) {
                eprintln!("discovery scanning pages={pages} selected={}", result.len());
            }
            for m in page.markets {
                if self.selected(&m) {
                    result.insert(m.ticker);
                }
            }
            cursor = page.cursor.unwrap_or_default();
            if cursor.is_empty() {
                break;
            }
            ensure!(seen.insert(cursor.clone()), "repeated pagination cursor");
        }
        Ok(result)
    }
    async fn lookup(&self, tickers: &BTreeSet<String>) -> Result<BTreeSet<String>> {
        let selected: Vec<_> = tickers
            .iter()
            .filter(|ticker| self.filter.is_empty() || self.filter.contains(*ticker))
            .cloned()
            .collect();
        if selected.is_empty() {
            return Ok(BTreeSet::new());
        }
        // A small batch bounds both request size and the work before cancellation.
        let mut url = reqwest::Url::parse(&format!("{}/markets", self.base))?;
        url.query_pairs_mut().extend_pairs([
            ("status", "open"),
            ("mve_filter", "exclude"),
            ("tickers", &selected.join(",")),
            ("limit", "1000"),
        ]);
        ensure!(
            url.as_str().len() <= 8192,
            "lifecycle lookup URL exceeds 8 KiB"
        );
        let page: Page = self.get(url).await?;
        ensure!(
            page.cursor.as_deref().unwrap_or("").is_empty(),
            "unexpected paginated lifecycle lookup"
        );
        Ok(page
            .markets
            .into_iter()
            .filter(|market| tickers.contains(&market.ticker) && self.selected(market))
            .map(|market| market.ticker)
            .collect())
    }
}
#[derive(Debug)]
pub enum Notice {
    Market(String),
    Reconcile,
}
/// Full scans run concurrently with lifecycle lookups. Completed scans are patched
/// with newer lifecycle observations before publication, so stale scans cannot
/// resurrect a closed market or erase a newly activated one.
pub async fn run(
    discovery: Discovery,
    seconds: u64,
    mut notices: mpsc::Receiver<Notice>,
    universe: watch::Sender<BTreeSet<String>>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut current = BTreeSet::new();
    let mut initialized = false;
    let mut overrides = std::collections::BTreeMap::new();
    let mut scan: Option<tokio::task::JoinHandle<Result<BTreeSet<String>>>> = None;
    let mut next_scan = tokio::time::Instant::now();
    let mut rescan = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                if let Some(task) = scan.take() { task.abort(); let _ = task.await; }
                return Ok(());
            }
            result = async { scan.as_mut().unwrap().await }, if scan.is_some() => {
                scan.take();
                match result.context("discovery task panicked").and_then(|r| r) {
                    Ok(mut next) => {
                        for (symbol, eligible) in std::mem::take(&mut overrides) {
                            if eligible { next.insert(symbol); } else { next.remove(&symbol); }
                        }
                        let first_scan = !initialized;
                        initialized = true;
                        if first_scan || current != next {
                            current = next;
                            eprintln!("discovery desired={}", current.len());
                            universe.send_replace(current.clone());
                        }
                    }
                    Err(error) => {
                        eprintln!("discovery failed; retaining {} symbols: {error:#}", current.len());
                        next_scan = tokio::time::Instant::now() + Duration::from_secs(2);
                    }
                }
                if rescan { next_scan = tokio::time::Instant::now(); rescan = false; }
            }
            _ = tokio::time::sleep_until(next_scan), if scan.is_none() => {
                overrides.clear();
                // Schedule from scan start, not completion. Slow scans never overlap;
                // when a scan takes longer than the interval, start the next immediately.
                next_scan = tokio::time::Instant::now() + Duration::from_secs(seconds);
                let source = discovery.clone();
                scan = Some(tokio::spawn(async move { source.all().await }));
            }
            notice = notices.recv() => {
                let Some(first) = notice else { cancel.cancel(); continue; };
                let (tickers, reconcile) = notice_batch(first, &mut notices);
                if reconcile {
                    if scan.is_some() { rescan = true; }
                    else { next_scan = tokio::time::Instant::now(); }
                }
                if tickers.is_empty() { continue; }
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => continue,
                    result = discovery.lookup(&tickers) => result,
                };
                match result {
                    Ok(eligible) => {
                        let mut changed = false;
                        for symbol in tickers {
                            let is_live = eligible.contains(&symbol);
                            if scan.is_some() { overrides.insert(symbol.clone(), is_live); }
                            changed |= if is_live { current.insert(symbol) } else { current.remove(&symbol) };
                        }
                        if initialized && changed { universe.send_replace(current.clone()); }
                    }
                    Err(error) => {
                        eprintln!("lifecycle lookup failed; retaining membership: {error:#}");
                        if scan.is_some() { rescan = true; }
                        else { next_scan = tokio::time::Instant::now(); }
                    }
                }
            }
        }
    }
}
fn notice_batch(first: Notice, notices: &mut mpsc::Receiver<Notice>) -> (BTreeSet<String>, bool) {
    let mut tickers = BTreeSet::new();
    let mut reconcile = false;
    let mut next = Some(first);
    for index in 0..32 {
        match next.take() {
            Some(Notice::Market(ticker)) => {
                tickers.insert(ticker);
            }
            Some(Notice::Reconcile) => reconcile = true,
            None => break,
        }
        if index < 31 {
            next = notices.try_recv().ok();
        }
    }
    // Do not receive a 33rd item: every consumed notice must be included.
    (tickers, reconcile)
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
