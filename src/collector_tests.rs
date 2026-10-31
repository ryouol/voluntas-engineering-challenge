use super::*;
#[test]
fn stable_balanced_assignments_cover_universe_once() {
    let all: BTreeSet<String> = (0..251).map(|n| format!("M{n:03}")).collect();
    let mut shards = vec![];
    assign(&mut shards, &all, 100);
    assert_eq!(shards.len(), 3);
    assert!(shards.iter().all(|s| s.len() <= 100));
    assert_eq!(shards.iter().map(BTreeSet::len).sum::<usize>(), 251);
    let old = shards.clone();
    let mut next = all.clone();
    next.remove("M000");
    next.insert("NEW".into());
    assign(&mut shards, &next, 100);
    for (a, b) in old.iter().zip(&shards) {
        for s in a {
            if s != "M000" {
                assert!(b.contains(s));
            }
        }
    }
}
#[test]
fn two_symbols_use_two_sockets() {
    let mut s = vec![];
    assign(&mut s, &["A".into(), "B".into()].into(), 100);
    assert_eq!(s.iter().map(BTreeSet::len).collect::<Vec<_>>(), [1, 1]);
}

#[tokio::test]
async fn ownership_handoff_waits_until_source_records_are_queued() {
    let (applied, mut receiver) = watch::channel(["A".into()].into());
    let (records, mut output) = mpsc::channel(1);
    records.send("old checkpoint").await.unwrap();
    let source = records.clone();
    let source_task = tokio::spawn(async move {
        source.send("last old update").await.unwrap();
        applied.send_replace(BTreeSet::new());
    });
    let destination = tokio::spawn(async move {
        wait_for_releases(std::slice::from_mut(&mut receiver), &[BTreeSet::new()])
            .await
            .unwrap();
        records.send("new snapshot").await.unwrap();
    });
    tokio::task::yield_now().await;
    assert!(!destination.is_finished());
    assert_eq!(output.recv().await, Some("old checkpoint"));
    assert_eq!(output.recv().await, Some("last old update"));
    assert_eq!(output.recv().await, Some("new snapshot"));
    source_task.await.unwrap();
    destination.await.unwrap();
}

#[tokio::test]
async fn ordinary_reactivation_waits_for_the_previous_owner() {
    let (source, receiver) = watch::channel(["A".into(), "C".into(), "E".into()].into());
    let (_, destination) = watch::channel(["F".into()].into());
    let mut applied = [receiver, destination];
    let mut shards = vec![["C".into(), "E".into()].into(), ["F".into()].into()];
    assign(
        &mut shards,
        &["A".into(), "C".into(), "E".into(), "F".into()].into(),
        100,
    );
    assert!(shards[1].contains("A"));
    assert!(
        timeout(
            Duration::from_millis(10),
            wait_for_releases(&mut applied, &shards)
        )
        .await
        .is_err()
    );
    source.send_replace(["C".into(), "E".into()].into());
    wait_for_releases(&mut applied, &shards).await.unwrap();
}
