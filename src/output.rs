use crate::util::{
    AlignedSbeWriter,
    dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain},
};
use anyhow::{Context, Result};
use std::{io::Write, sync::Arc};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;
pub const CHUNK: usize = 64 << 20;
pub const QUEUE_CAPACITY: usize = 8192;
#[derive(Debug, Clone)]
pub enum Record {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
    Flush,
}
#[derive(Clone)]
pub struct Output {
    tx: mpsc::Sender<Record>,
    failed: CancellationToken,
    pub written: Arc<std::sync::atomic::AtomicU64>,
}
impl Output {
    pub async fn send(&self, record: Record) -> Result<()> {
        let result = timeout(Duration::from_secs(10), self.tx.send(record))
            .await
            .context("output queue stalled for 10 seconds")
            .and_then(|r| r.context("output writer stopped"));
        if result.is_err() {
            self.failed.cancel();
        }
        result
    }
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }
}
pub fn start<W: Write + Send + 'static>(
    sink: W,
    cancel: CancellationToken,
) -> (Output, tokio::task::JoinHandle<Result<()>>) {
    let (tx, mut rx) = mpsc::channel(QUEUE_CAPACITY);
    let written = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let count = written.clone();
    let failed = cancel.clone();
    let task = tokio::task::spawn_blocking(move || {
        let result = (|| {
            let mut writer = AlignedSbeWriter::from_writer(sink, CHUNK)?;
            while let Some(record) = rx.blocking_recv() {
                match record {
                    Record::Snapshot(s) => writer.write_msg(&s)?,
                    Record::Update(u) => writer.write_msg(&u)?,
                    Record::Flush => {
                        writer.flush()?;
                        continue;
                    }
                }
                count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            writer.flush()
        })();
        if result.is_err() {
            cancel.cancel();
        }
        result
    });
    (
        Output {
            tx,
            failed,
            written,
        },
        task,
    )
}

#[cfg(test)]
#[path = "output_tests.rs"]
mod tests;
