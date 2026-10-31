use super::*;
fn env(v: Value) -> Envelope {
    serde_json::from_value(v).unwrap()
}
fn ready() -> Feed {
    let mut f = Feed::default();
    f.set_desired(["A".into(), "B".into()].into());
    f.command().unwrap();
    f.receive(
        env(json!({"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":5}})),
        DateTime::UNIX_EPOCH,
    )
    .unwrap();
    f
}
fn snap(s: &str, seq: u64) -> Envelope {
    env(
        json!({"type":"orderbook_snapshot","sid":5,"seq":seq,"msg":{"market_ticker":s,"yes_dollars_fp":[["0.2","2"]],"no_dollars_fp":[]}}),
    )
}
#[test]
fn shared_sequence_replacement_and_removal() {
    let mut f = ready();
    f.receive(snap("A", 3), DateTime::UNIX_EPOCH).unwrap();
    f.receive(snap("B", 4), DateTime::UNIX_EPOCH).unwrap();
    let s = env(
        json!({"type":"orderbook_snapshot","sid":5,"seq":5,"msg":{"market_ticker":"A","yes_dollars_fp":[],"no_dollars_fp":[]}}),
    );
    f.receive(s, DateTime::UNIX_EPOCH).unwrap();
    assert!(f.books["A"].bids.is_empty());
    f.set_desired(["B".into()].into());
    assert_eq!(f.snapshots(DateTime::UNIX_EPOCH).len(), 1);
    assert_eq!(
        f.command().unwrap().unwrap()["params"]["action"],
        "delete_markets"
    );
    f.receive(
        env(json!({"id":2,"type":"ok","sid":5,"seq":6})),
        DateTime::UNIX_EPOCH,
    )
    .unwrap();
    assert!(!f.acknowledged.contains("A"));
    assert!(f.receive(snap("B", 8), DateTime::UNIX_EPOCH).is_err());
}
#[test]
fn no_delta_before_snapshot_or_false_ack() {
    let mut f = ready();
    assert!(f.snapshots(DateTime::UNIX_EPOCH).is_empty());
    assert!(f.receive(env(json!({"type":"orderbook_delta","sid":5,"seq":1,"msg":{"market_ticker":"A","price_dollars":"0.2","delta_fp":"1","side":"yes"}})),DateTime::UNIX_EPOCH).is_err());
    let mut f = Feed::default();
    f.set_desired(["A".into()].into());
    f.command().unwrap();
    assert!(
        f.receive(
            env(json!({"id":9,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":5}})),
            DateTime::UNIX_EPOCH
        )
        .is_err()
    );
}
#[test]
fn omitted_sides_are_an_empty_snapshot_then_accept_deltas() {
    let mut f = ready();
    let empty = env(json!({"type":"orderbook_snapshot","sid":5,"seq":1,
        "msg":{"market_ticker":"A","market_id":"public-id","clock":123}}));
    assert!(matches!(
        f.receive(empty, DateTime::UNIX_EPOCH).unwrap(),
        Some(Record::Snapshot(_))
    ));
    assert!(f.books["A"].bids.is_empty() && f.books["A"].asks.is_empty());
    let delta = env(json!({"type":"orderbook_delta","sid":5,"seq":2,
        "msg":{"market_ticker":"A","price_dollars":"0.2","delta_fp":"1.25","side":"yes"}}));
    assert!(matches!(
        f.receive(delta, DateTime::UNIX_EPOCH).unwrap(),
        Some(Record::Update(_))
    ));
    assert_eq!(f.books["A"].bids[&200000], 1250000);
}
#[tokio::test(start_paused = true)]
async fn missing_ack_times_out() {
    let mut f = Feed::default();
    f.set_desired(["A".into()].into());
    f.command().unwrap();
    tokio::time::advance(Duration::from_secs(21)).await;
    assert!(f.command().is_err());
}
#[test]
fn timestamps_do_not_invent_exchange_time() {
    assert_eq!(timestamp(None).unwrap(), DateTime::UNIX_EPOCH);
    assert!(timestamp(Some(-1)).is_err());
}

#[test]
fn rapid_reactivation_finishes_delete_and_rebuilds_before_updates() {
    for restore_before_delete in [true, false] {
        let mut feed = ready();
        feed.receive(snap("A", 1), DateTime::UNIX_EPOCH).unwrap();
        feed.receive(snap("B", 2), DateTime::UNIX_EPOCH).unwrap();
        feed.set_desired(["B".into()].into());
        if restore_before_delete {
            feed.set_desired(["A".into(), "B".into()].into());
        }
        let delete = feed.command().unwrap().expect("removal must not disappear");
        assert_eq!(delete["params"]["action"], "delete_markets");
        if !restore_before_delete {
            feed.set_desired(["A".into(), "B".into()].into());
        }
        let stale = env(json!({"type":"orderbook_delta","sid":5,"seq":3,"msg":{
            "market_ticker":"A","price_dollars":"0.2","delta_fp":"9","side":"yes"
        }}));
        assert!(feed.receive(stale, DateTime::UNIX_EPOCH).unwrap().is_none());
        assert!(!feed.books.contains_key("A"));
        feed.receive(
            env(json!({"id":2,"type":"ok","sid":5,"seq":4})),
            DateTime::UNIX_EPOCH,
        )
        .unwrap();
        let add = feed.command().unwrap().unwrap();
        assert_eq!(add["params"]["action"], "add_markets");
        feed.receive(
            env(json!({"id":3,"type":"ok","sid":5,"seq":5})),
            DateTime::UNIX_EPOCH,
        )
        .unwrap();
        assert!(matches!(
            feed.receive(snap("A", 6), DateTime::UNIX_EPOCH).unwrap(),
            Some(Record::Snapshot(_))
        ));
        let delta = env(json!({"type":"orderbook_delta","sid":5,"seq":7,"msg":{
            "market_ticker":"A","price_dollars":"0.2","delta_fp":"1","side":"yes"
        }}));
        feed.receive(delta, DateTime::UNIX_EPOCH).unwrap();
        assert_eq!(feed.books["A"].bids[&200000], 3000000);
    }
}

#[tokio::test(start_paused = true)]
async fn acknowledged_market_without_snapshot_times_out() {
    let mut feed = ready();
    feed.receive(snap("A", 1), DateTime::UNIX_EPOCH).unwrap();
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(
        feed.command()
            .unwrap_err()
            .to_string()
            .contains("initial snapshot timeout")
    );
}
