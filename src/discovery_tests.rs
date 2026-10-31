use super::*;
#[test]
fn excludes_mve_and_closed() {
    let mut m = Market {
        ticker: "A".into(),
        status: "active".into(),
        mve_collection_ticker: None,
        mve_selected_legs: None,
    };
    assert!(m.eligible());
    m.mve_collection_ticker = Some("combo".into());
    assert!(!m.eligible());
    m.mve_collection_ticker = None;
    m.status = "closed".into();
    assert!(!m.eligible());
}

#[test]
fn notice_batch_preserves_the_first_item_after_its_limit() {
    let (sender, mut receiver) = mpsc::channel(64);
    for n in 1..=33 {
        sender.try_send(Notice::Market(format!("M{n}"))).unwrap();
    }
    let (tickers, reconcile) = notice_batch(Notice::Reconcile, &mut receiver);
    assert!(reconcile);
    assert_eq!(tickers.len(), 31);
    assert!(matches!(receiver.try_recv().unwrap(), Notice::Market(t) if t == "M32"));
}
