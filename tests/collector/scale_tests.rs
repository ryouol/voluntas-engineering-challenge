use super::*;

#[tokio::test]
async fn capped_sockets_cover_multiple_subscription_batches() {
    let (mock, mut config, _temp, server) = setup().await;
    *mock.symbols.lock().unwrap() = (0..203).map(|n| format!("M{n:03}")).collect();
    config.markets_per_socket = 1;
    config.max_data_sockets = 2;
    let path = config.output.clone().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let collector = tokio::spawn(engineering_challenge::collector::run(
        config,
        cancel.clone(),
    ));
    wait_until(|| mock.snapshots.lock().unwrap().len() == 203).await;
    assert_eq!(mock.connections.load(Ordering::Relaxed), 3);
    let commands = mock.commands.lock().unwrap().clone();
    assert!(commands.iter().any(|v| v["cmd"] == "update_subscription"));
    for command in commands {
        if let Some(tickers) = command["params"]["market_tickers"].as_array() {
            assert!(tickers.len() <= 100);
        }
    }
    cancel.cancel();
    collector.await.unwrap().unwrap();
    let report = std::process::Command::new(env!("CARGO_BIN_EXE_verify"))
        .arg(path)
        .output()
        .unwrap();
    assert!(report.status.success());
    let report: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(report["symbols"], 203);
    server.abort();
}

#[tokio::test]
async fn shrinking_universe_hands_off_one_market_with_fresh_snapshot() {
    let (mock, mut config, _temp, server) = setup().await;
    *mock.symbols.lock().unwrap() = ["A".into(), "B".into(), "C".into(), "D".into()].into();
    config.markets_per_socket = 2;
    mock.delete_delay_ms.store(250, Ordering::Relaxed);
    let path = config.output.clone().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let collector = tokio::spawn(engineering_challenge::collector::run(
        config,
        cancel.clone(),
    ));
    wait_until(|| mock.snapshots.lock().unwrap().len() == 4).await;
    // Stable initial assignment puts A/C together. B/D closing forces C's handoff.
    *mock.symbols.lock().unwrap() = ["A".into(), "C".into()].into();
    for symbol in ["B", "D"] {
        mock.events.send(symbol.into()).unwrap();
    }
    wait_until(|| {
        mock.snapshots
            .lock()
            .unwrap()
            .get("C")
            .copied()
            .unwrap_or(0)
            == 2
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    cancel.cancel();
    collector.await.unwrap().unwrap();
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut quantities = std::collections::HashMap::new();
    while let Some(message) = reader.read_msg().unwrap() {
        match message {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s).unwrap();
                quantities.insert(s.symbol, s.bids[0].quantity);
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
                let previous = quantities.get_mut(&u.symbol).unwrap();
                assert_eq!(
                    u.quantity,
                    *previous + 1_000_000,
                    "interleaved owners for {}",
                    u.symbol
                );
                *previous = u.quantity;
            }
            _ => panic!("unexpected message"),
        }
    }
    server.abort();
}

#[tokio::test]
async fn lifecycle_reconnect_reconciles_events_missed_while_disconnected() {
    let (mock, mut config, _temp, server) = setup().await;
    config.discovery_seconds = 3600;
    let cancel = tokio_util::sync::CancellationToken::new();
    let collector = tokio::spawn(engineering_challenge::collector::run(
        config,
        cancel.clone(),
    ));
    wait_until(|| mock.snapshots.lock().unwrap().len() == 2).await;
    mock.symbols.lock().unwrap().insert("C".into());
    mock.disconnect_control.store(true, Ordering::Relaxed);
    wait_until(|| mock.snapshots.lock().unwrap().contains_key("C")).await;
    cancel.cancel();
    collector.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn closed_stdout_pipe_exits_nonzero_after_sigint() {
    let (mock, config, temp, server) = setup().await;
    let mut child = ChildGuard(
        std::process::Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
            .current_dir(temp.path())
            .args([
                "--key-id",
                "mock",
                "--private-key",
                config.private_key.to_str().unwrap(),
                "--rest-url",
                &config.rest_url,
                "--ws-url",
                &config.ws_url,
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    drop(child.0.stdout.take());
    wait_until(|| mock.snapshots.lock().unwrap().len() == 2).await;
    assert!(
        std::process::Command::new("kill")
            .args(["-INT", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    assert!(!child.wait().await.success());
    server.abort();
}
