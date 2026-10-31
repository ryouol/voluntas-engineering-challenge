use super::*;
use engineering_challenge::discovery::{self, Notice};

#[tokio::test]
async fn lifecycle_bursts_coalesce_and_keep_notices_received_during_lookup() {
    let (mock, config, _temp, server) = setup().await;
    let (notices, receiver) = tokio::sync::mpsc::channel(128);
    let (universe, mut observed) = tokio::sync::watch::channel(BTreeSet::new());
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(discovery::run(
        Discovery::new(&config).unwrap(),
        3600,
        receiver,
        universe,
        cancel.clone(),
    ));
    observed.changed().await.unwrap();
    observed.borrow_and_update();
    let mut expected: BTreeSet<String> = (0..50).map(|n| format!("C{n:03}")).collect();
    expected.insert("B".into());
    *mock.symbols.lock().unwrap() = expected.clone();
    mock.lookup_delay_ms.store(200, Ordering::Relaxed);
    for symbol in expected.iter().chain(std::iter::once(&"A".to_string())) {
        notices.send(Notice::Market(symbol.clone())).await.unwrap();
    }
    for _ in 0..20 {
        notices.send(Notice::Market("C000".into())).await.unwrap();
    }
    wait_until(|| mock.lookup_requests.load(Ordering::Relaxed) > 0).await;
    expected.remove("C000");
    expected.insert("Z".into());
    *mock.symbols.lock().unwrap() = expected.clone();
    notices.send(Notice::Market("C000".into())).await.unwrap();
    notices.send(Notice::Market("Z".into())).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if *observed.borrow_and_update() == expected {
                break;
            }
            observed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        mock.lookup_requests.load(Ordering::Relaxed) <= 5,
        "burst was not batched"
    );
    cancel.cancel();
    task.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn failed_lifecycle_batch_retains_membership_then_reconciles() {
    let (mock, config, _temp, server) = setup().await;
    let (notices, receiver) = tokio::sync::mpsc::channel(32);
    let (universe, mut observed) = tokio::sync::watch::channel(BTreeSet::new());
    let cancel = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(discovery::run(
        Discovery::new(&config).unwrap(),
        3600,
        receiver,
        universe,
        cancel.clone(),
    ));
    observed.changed().await.unwrap();
    assert_eq!(
        *observed.borrow_and_update(),
        ["A".into(), "B".into()].into()
    );
    *mock.symbols.lock().unwrap() = ["B".into(), "C".into()].into();
    mock.bad_page.store(true, Ordering::Relaxed);
    mock.fail_lookup_once.store(true, Ordering::Relaxed);
    notices.send(Notice::Market("A".into())).await.unwrap();
    notices.send(Notice::Market("C".into())).await.unwrap();
    wait_until(|| !mock.fail_lookup_once.load(Ordering::Relaxed)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), observed.changed())
            .await
            .is_err()
    );
    assert_eq!(*observed.borrow(), ["A".into(), "B".into()].into());
    mock.bad_page.store(false, Ordering::Relaxed);
    notices.send(Notice::Reconcile).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), observed.changed())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*observed.borrow(), ["B".into(), "C".into()].into());
    cancel.cancel();
    task.await.unwrap().unwrap();
    server.abort();
}
