use super::*;
#[test]
fn decimals_are_exact() {
    for (s, n) in [
        ("0.000001", 1),
        ("12.50", 12_500_000),
        ("-2.50", -2_500_000),
        ("0", 0),
        ("0.123456", 123456),
    ] {
        assert_eq!(fixed(s).unwrap(), n);
    }
    for s in [
        "1e3",
        "NaN",
        "1.1234567",
        "1.",
        ".1",
        "+1",
        "",
        "-",
        "9223372036854775807",
        "1.2.3",
    ] {
        assert!(fixed(s).is_err(), "{s}");
    }
}
#[test]
fn conversion_updates_deletes_and_atomic_failure() {
    let t = DateTime::UNIX_EPOCH;
    let mut b = Book::from_snapshot(
        &[("0.40".into(), "1".into())],
        &[("0.37".into(), "12.50".into())],
        t,
    )
    .unwrap();
    assert_eq!(b.asks[&630000], 12500000);
    let u = b.update("A", Side::Sell, "0.37", "-2.50", t, t).unwrap();
    assert_eq!((u.price_level, u.quantity), (630000, 10000000));
    b.update("A", Side::Sell, "0.37", "-10", t, t).unwrap();
    assert!(b.asks.is_empty());
    let before = b.clone();
    assert!(b.update("A", Side::Buy, "0.4", "-2", t, t).is_err());
    assert_eq!(before, b);
    b.update("A", Side::Buy, "0.4", "2", t, t).unwrap();
    assert_eq!(b.bids[&400000], 3000000);
}
#[test]
fn quantity_overflow_and_duplicate_zero_levels_fail_atomically() {
    let t = DateTime::UNIX_EPOCH;
    let mut b = Book::default();
    b.bids.insert(200000, i64::MAX);
    let before = b.clone();
    assert!(b.update("A", Side::Buy, "0.2", "0.000001", t, t).is_err());
    assert_eq!(b, before);
    assert!(
        Book::from_snapshot(
            &[("0.2".into(), "0".into()), ("0.20".into(), "1".into())],
            &[],
            t
        )
        .is_err()
    );
}
#[test]
fn snapshot_validation_and_ordering() {
    let levels = vec![("0.2".into(), "1".into()), ("0.6".into(), "2".into())];
    let b = Book::from_snapshot(&levels, &levels, DateTime::UNIX_EPOCH).unwrap();
    let s = b.snapshot("A", DateTime::UNIX_EPOCH);
    assert_eq!(s.bids[0].price_level, 600000);
    assert_eq!(s.asks[0].price_level, 400000);
    assert!(Book::from_snapshot(&[("2".into(), "1".into())], &[], DateTime::UNIX_EPOCH).is_err());
    assert!(
        Book::from_snapshot(&[("0.2".into(), "-1".into())], &[], DateTime::UNIX_EPOCH).is_err()
    );
}
