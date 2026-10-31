use chrono::{DateTime, Duration, Utc};
use engineering_challenge::{
    Side,
    book::Book,
    output::CHUNK,
    util::{
        AlignedSbeWriter,
        dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain},
    },
};
use serde_json::Value;
use std::{fs::File, path::Path, process::Output};

fn run_verify(path: &Path) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(path)
        .output()
        .unwrap()
}
fn snapshot(quantity: &str, second: i64) -> SnapshotDomain {
    let time = DateTime::<Utc>::UNIX_EPOCH + Duration::seconds(second);
    Book::from_snapshot(&[("0.2".into(), quantity.into())], &[], time)
        .unwrap()
        .snapshot("A", time)
}
fn update() -> PriceLevelUpdateDomain {
    PriceLevelUpdateDomain {
        symbol: "A".into(),
        side: Side::Buy,
        price_level: 200000,
        quantity: 3000000,
        backtest_timestamp: DateTime::UNIX_EPOCH + Duration::seconds(1),
        exchange_timestamp: DateTime::UNIX_EPOCH + Duration::seconds(1),
    }
}
fn capture(path: &Path, write: impl FnOnce(&mut AlignedSbeWriter<File>)) {
    let mut writer = AlignedSbeWriter::create(path, CHUNK).unwrap();
    write(&mut writer);
    writer.flush().unwrap();
}

#[test]
fn verifier_reports_exact_counts_replay_matches_and_gap_ranks() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("known.sbe");
    capture(&path, |writer| {
        writer.write_msg(&snapshot("2", 0)).unwrap();
        writer.write_msg(&update()).unwrap();
        writer.write_msg(&snapshot("3", 60)).unwrap();
        writer.write_msg(&snapshot("7", 70)).unwrap();
        writer.write_msg(&snapshot("7", 130)).unwrap();
    });
    let result = run_verify(&path);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["symbols"], 1);
    assert_eq!(report["snapshots"], 4);
    assert_eq!(report["updates"], 1);
    assert_eq!(report["snapshot_replay_matches"], 2);
    assert_eq!(report["snapshot_state_replacements"], 1);
    assert_eq!(report["snapshot_gap_ms_min"], 10000);
    for field in [
        "snapshot_gap_ms_median",
        "snapshot_gap_ms_p99",
        "snapshot_gap_ms_max",
    ] {
        assert_eq!(report[field], 60000);
    }
    let empty = temp.path().join("empty.sbe");
    File::create(&empty).unwrap();
    let report: Value = serde_json::from_slice(&run_verify(&empty).stdout).unwrap();
    assert_eq!(report["snapshots"], 0);
    assert!(report["snapshot_gap_ms_median"].is_null());
}

#[test]
fn verifier_rejects_invalid_books_updates_and_truncated_chunks() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("invalid.sbe");
    capture(&path, |writer| {
        writer.write_msg(&update()).unwrap();
    });
    assert!(
        !run_verify(&path).status.success(),
        "update before snapshot accepted"
    );
    capture(&path, |writer| {
        let mut duplicate = snapshot("2", 0);
        duplicate.bids.push(duplicate.bids[0].clone());
        writer.write_msg(&duplicate).unwrap();
    });
    assert!(
        !run_verify(&path).status.success(),
        "duplicate price accepted"
    );
    capture(&path, |writer| {
        writer.write_msg(&snapshot("2", 0)).unwrap();
        let mut invalid = update();
        invalid.quantity = -1;
        writer.write_msg(&invalid).unwrap();
    });
    assert!(
        !run_verify(&path).status.success(),
        "negative quantity accepted"
    );
    std::fs::write(&path, [0; 20]).unwrap();
    assert!(
        !run_verify(&path).status.success(),
        "truncated chunk accepted"
    );
}
