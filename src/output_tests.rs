use super::*;
use std::io;
struct Broken;
impl Write for Broken {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "test"))
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn broken_pipe_is_fatal_and_reported() {
    let cancel = CancellationToken::new();
    let (out, task) = start(Broken, cancel.clone());
    out.send(Record::Snapshot(
        crate::book::Book::default().snapshot("A", chrono::DateTime::UNIX_EPOCH),
    ))
    .await
    .unwrap();
    out.send(Record::Flush).await.unwrap();
    drop(out);
    assert!(task.await.unwrap().is_err());
    assert!(cancel.is_cancelled());
}
#[tokio::test(start_paused = true)]
async fn bounded_queue_stall_cancels_collector() {
    let (tx, _rx) = mpsc::channel(1);
    let failed = CancellationToken::new();
    let out = Output {
        tx,
        failed: failed.clone(),
        written: Arc::new(std::sync::atomic::AtomicU64::new(0)),
    };
    out.send(Record::Flush).await.unwrap();
    assert!(out.send(Record::Flush).await.is_err());
    assert!(failed.is_cancelled());
}
