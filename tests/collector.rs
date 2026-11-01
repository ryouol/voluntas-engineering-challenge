mod common;
use common::{ChildGuard, setup, wait_until};
use engineering_challenge::{
    discovery::Discovery,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use std::{collections::BTreeSet, sync::atomic::Ordering, time::Duration};
#[tokio::test]
async fn pagination_and_failure_are_not_partial_success() {
    let (s, c, _temp, server) = setup().await;
    let d = Discovery::new(&c).unwrap();
    s.throttled_once.store(true, Ordering::Relaxed);
    s.bad_json_once.store(true, Ordering::Relaxed);
    assert_eq!(d.all().await.unwrap(), ["A".into(), "B".into()].into());
    s.bad_page.store(true, Ordering::Relaxed);
    assert!(d.all().await.is_err());
    server.abort();
}
#[tokio::test]
async fn live_mock_recovery_lifecycle_and_independent_replay() {
    let (s, c, _temp, server) = setup().await;
    let output = c.output.clone().unwrap();
    let started = chrono::Utc::now();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(engineering_challenge::collector::run(c, cancel.clone()));
    wait_until(|| s.snapshots.lock().unwrap().len() == 2).await;
    assert!(s.connections.load(Ordering::Relaxed) >= 3);
    s.gap.store(true, Ordering::Relaxed);
    wait_until(|| s.snapshots.lock().unwrap().values().sum::<usize>() >= 3).await;
    assert!(
        s.connections.load(Ordering::Relaxed) > 3,
        "gap should rebuild a data socket"
    );
    {
        let mut symbols = s.symbols.lock().unwrap();
        symbols.remove("A");
        symbols.insert("C".into());
    }
    s.events.send("A".into()).unwrap();
    s.events.send("C".into()).unwrap();
    wait_until(|| s.snapshots.lock().unwrap().contains_key("C")).await;
    let previous_a = s.snapshots.lock().unwrap()["A"];
    // Reactivation must start with another snapshot.
    s.symbols.lock().unwrap().insert("A".into());
    s.events.send("A".into()).unwrap();
    wait_until(|| s.snapshots.lock().unwrap()["A"] > previous_a).await;
    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(s.closes.load(Ordering::Relaxed) >= 3);
    let finished = chrono::Utc::now();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(output).unwrap();
    let mut quantity = std::collections::HashMap::new();
    let mut snapshots = std::collections::HashMap::<String, usize>::new();
    let mut updates = 0;
    while let Some(m) = reader.read_msg().unwrap() {
        match m {
            AnyMessage::Snapshot(m) => {
                let m = SnapshotDomain::try_from_decoder(m).unwrap();
                assert_eq!(m.exchange_timestamp.timestamp_millis(), 1_700_000_000_000);
                assert!((started..=finished).contains(&m.received_at));
                assert_eq!(m.asks[0].price_level, 630000);
                assert_eq!(m.asks[0].quantity, 12500000);
                quantity.insert(m.symbol.clone(), m.bids[0].quantity);
                *snapshots.entry(m.symbol).or_default() += 1;
            }
            AnyMessage::PriceLevelUpdate(m) => {
                let m = PriceLevelUpdateDomain::try_from_decoder(m).unwrap();
                // The fixture's sending time is one millisecond later than its event time.
                assert_eq!(m.exchange_timestamp.timestamp_millis(), 1_700_000_000_000);
                assert!((started..=finished).contains(&m.backtest_timestamp));
                let previous = quantity
                    .get_mut(&m.symbol)
                    .expect("snapshot must precede update");
                assert_eq!(m.quantity, *previous + 1000000);
                *previous = m.quantity;
                updates += 1;
            }
            _ => panic!(),
        }
    }
    assert!(snapshots.contains_key("C"));
    assert!(snapshots["A"] >= 2);
    assert!(updates > 10);
    server.abort();
}
#[tokio::test]
async fn subprocess_stdout_and_sigint_are_decodable() {
    let (s, c, temp, server) = setup().await;
    let path = temp.path().join("stdout.sbe");
    let stderr = temp.path().join("stderr.log");
    let mut child = ChildGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .current_dir(temp.path())
            .args([
                "--key-id",
                "mock",
                "--private-key",
                c.private_key.to_str().unwrap(),
                "--rest-url",
                &c.rest_url,
                "--ws-url",
                &c.ws_url,
            ])
            .stdout(std::fs::File::create(&path).unwrap())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap(),
    );
    wait_until(|| s.snapshots.lock().unwrap().len() == 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        std::process::Command::new("kill")
            .args(["-INT", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = child.wait().await;
    assert!(
        status.success(),
        "{}",
        std::fs::read_to_string(stderr).unwrap()
    );
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path).unwrap();
    assert!(reader.read_msg().unwrap().is_some());
    assert!(s.closes.load(Ordering::Relaxed) >= 3);
    let verify = std::process::Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(path)
        .output()
        .unwrap();
    assert!(verify.status.success());
    server.abort();
}

#[tokio::test]
#[ignore = "65-second real-clock checkpoint and replay soak"]
async fn minute_checkpoint_soak() {
    let (_s, c, _temp, server) = setup().await;
    let path = c.output.clone().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(engineering_challenge::collector::run(c, cancel.clone()));
    tokio::time::sleep(Duration::from_secs(65)).await;
    cancel.cancel();
    task.await.unwrap().unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut quantities = std::collections::HashMap::new();
    let mut timestamps =
        std::collections::HashMap::<String, Vec<chrono::DateTime<chrono::Utc>>>::new();
    while let Some(m) = reader.read_msg().unwrap() {
        match m {
            AnyMessage::Snapshot(m) => {
                let m = SnapshotDomain::try_from_decoder(m).unwrap();
                if let Some(expected) = quantities.get(&m.symbol) {
                    assert_eq!(
                        *expected, m.bids[0].quantity,
                        "minute checkpoint disagrees with independent replay"
                    );
                }
                quantities.insert(m.symbol.clone(), m.bids[0].quantity);
                timestamps.entry(m.symbol).or_default().push(m.received_at);
            }
            AnyMessage::PriceLevelUpdate(m) => {
                let m = PriceLevelUpdateDomain::try_from_decoder(m).unwrap();
                quantities.insert(m.symbol, m.quantity);
            }
            _ => panic!("unexpected message"),
        }
    }
    assert_eq!(timestamps.len(), 2);
    for times in timestamps.values() {
        assert_eq!(times.len(), 2);
        let elapsed = (times[1] - times[0]).num_seconds();
        assert!((59..=62).contains(&elapsed), "bad cadence: {elapsed}");
    }
    server.abort();
}

#[tokio::test]
async fn lifecycle_overrides_inflight_scan_and_failed_refresh_preserves_membership() {
    let (s, c, _temp, server) = setup().await;
    let d = Discovery::new(&c).unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let (tx, rx) = tokio::sync::mpsc::channel(32);
    let (universe, mut observed) = tokio::sync::watch::channel(BTreeSet::new());
    let task = tokio::spawn(engineering_challenge::discovery::run(
        d,
        60,
        rx,
        universe,
        cancel.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), observed.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        *observed.borrow_and_update(),
        ["A".into(), "B".into()].into()
    );
    s.page_delay_ms.store(800, Ordering::Relaxed);
    let before = s.page_count.load(Ordering::Relaxed);
    tx.send(engineering_challenge::discovery::Notice::Reconcile)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while s.page_count.load(Ordering::Relaxed) == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    {
        let mut symbols = s.symbols.lock().unwrap();
        symbols.remove("B");
        symbols.insert("C".into());
    }
    for symbol in ["B", "C"] {
        tx.send(engineering_challenge::discovery::Notice::Market(
            symbol.into(),
        ))
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while *observed.borrow_and_update() != ["A".into(), "C".into()].into() {
            observed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        *observed.borrow(),
        ["A".into(), "C".into()].into(),
        "stale page resurrected closed market"
    );
    s.bad_page.store(true, Ordering::Relaxed);
    tx.send(engineering_challenge::discovery::Notice::Reconcile)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(*observed.borrow(), ["A".into(), "C".into()].into());
    cancel.cancel();
    task.await.unwrap().unwrap();
    server.abort();
}

#[path = "collector/discovery_tests.rs"]
mod discovery_tests;
#[path = "collector/scale_tests.rs"]
mod scale_tests;
