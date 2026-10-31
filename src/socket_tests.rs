use super::*;
#[tokio::test(start_paused = true)]
async fn checkpoint_timer_waits_one_minute_and_skips_missed_ticks() {
    let mut interval = checkpoint_timer();
    tokio::time::advance(Duration::from_secs(59)).await;
    assert!(
        timeout(Duration::from_millis(1), interval.tick())
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    interval.tick().await;
    tokio::time::advance(Duration::from_secs(180)).await;
    interval.tick().await;
    assert!(
        timeout(Duration::from_millis(1), interval.tick())
            .await
            .is_err()
    );
    assert!(Feed::default().snapshots(chrono::Utc::now()).is_empty());
}

#[tokio::test]
async fn session_does_not_release_ownership_while_old_output_is_blocked() {
    use crate::{
        book::Book,
        output::{self, Record},
        util::{
            AlignedSbeReader, OrderbookSchema,
            dtos::challenge_orderbook::{AnyMessage, SnapshotDomain},
        },
    };
    use std::io::Write;
    use std::sync::{Condvar, Mutex};
    struct BlockingSink {
        file: std::fs::File,
        gate: Arc<(Mutex<bool>, Condvar)>,
    }
    impl Write for BlockingSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.file.write(bytes)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            let (lock, wake) = &*self.gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
            self.file.flush()
        }
    }
    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("handoff.sbe");
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release = ReleaseOnDrop(gate.clone());
    let cancel = CancellationToken::new();
    let (out, writer) = output::start(
        BlockingSink {
            file: std::fs::File::create(&path).unwrap(),
            gate,
        },
        cancel.clone(),
    );
    out.send(Record::Flush).await.unwrap();
    for _ in 0..output::QUEUE_CAPACITY {
        out.send(Record::Flush).await.unwrap();
    }
    assert_eq!(out.capacity(), 0);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let connect = tokio::spawn(async move { connect_async(url).await.unwrap().0 });
    let (stream, _) = listener.accept().await.unwrap();
    let mut server = tokio_tungstenite::accept_async(stream).await.unwrap();
    let mut client = connect.await.unwrap();
    let (desired, mut desired_rx) = watch::channel(["A".into()].into());
    let (applied, mut applied_rx) = watch::channel(BTreeSet::new());
    let stats = Arc::new(Stats::default());
    let session_stats = stats.clone();
    let session_out = out.clone();
    let task = tokio::spawn(async move {
        session(
            &mut client,
            &mut desired_rx,
            &applied,
            &session_out,
            &session_stats,
            &cancel,
        )
        .await
    });
    server.next().await.unwrap().unwrap(); // Subscribe command.
    for message in [
        serde_json::json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}),
        serde_json::json!({"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"A","yes_dollars_fp":[["0.2","2"]]}}),
    ] {
        server
            .send(Message::Text(message.to_string().into()))
            .await
            .unwrap();
    }
    timeout(Duration::from_secs(2), async {
        while stats.snapshots.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(applied_rx.borrow_and_update().contains("A"));
    desired.send_replace(BTreeSet::new());
    assert!(
        timeout(Duration::from_millis(50), applied_rx.changed())
            .await
            .is_err(),
        "source released ownership before its pending output was accepted"
    );
    drop(release);
    timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(applied_rx.borrow().is_empty());
    let time = chrono::DateTime::UNIX_EPOCH;
    let replacement = Book::from_snapshot(&[("0.2".into(), "7".into())], &[], time)
        .unwrap()
        .snapshot("A", time);
    out.send(Record::Snapshot(replacement)).await.unwrap();
    drop(out);
    writer.await.unwrap().unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut quantities = Vec::new();
    while let Some(message) = reader.read_msg().unwrap() {
        if let AnyMessage::Snapshot(snapshot) = message {
            quantities.push(SnapshotDomain::try_from_decoder(snapshot).unwrap().bids[0].quantity);
        }
    }
    assert_eq!(quantities, [2_000_000, 7_000_000]);
}
