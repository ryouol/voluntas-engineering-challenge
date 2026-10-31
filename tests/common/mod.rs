use axum::{
    Json, Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signature, SigningKey, Verifier, pkcs8::EncodePrivateKey};
use engineering_challenge::config::Config;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct Mock {
    pub symbols: Arc<Mutex<BTreeSet<String>>>,
    pub bad_page: Arc<AtomicBool>,
    pub bad_json_once: Arc<AtomicBool>,
    pub throttled_once: Arc<AtomicBool>,
    pub page_delay_ms: Arc<AtomicU64>,
    pub page_count: Arc<AtomicUsize>,
    pub gap: Arc<AtomicBool>,
    pub connections: Arc<AtomicUsize>,
    pub snapshots: Arc<Mutex<std::collections::HashMap<String, usize>>>,
    pub closes: Arc<AtomicUsize>,
    pub events: broadcast::Sender<String>,
    pub public: ed25519_dalek::VerifyingKey,
    pub commands: Arc<Mutex<Vec<Value>>>,
    pub lookup_requests: Arc<AtomicUsize>,
    pub lookup_delay_ms: Arc<AtomicU64>,
    pub fail_lookup_once: Arc<AtomicBool>,
    pub disconnect_control: Arc<AtomicBool>,
    pub delete_delay_ms: Arc<AtomicU64>,
}
async fn markets(
    State(s): State<Mock>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    assert_eq!(q.get("status").map(String::as_str), Some("open"));
    assert_eq!(q.get("mve_filter").map(String::as_str), Some("exclude"));
    if s.throttled_once.swap(false, Ordering::Relaxed) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"rate limit"})),
        );
    }
    if s.bad_json_once.swap(false, Ordering::Relaxed) {
        return (StatusCode::OK, Json(json!("truncated or invalid page")));
    }
    if q.contains_key("cursor") && s.bad_page.load(Ordering::Relaxed) {
        return (StatusCode::BAD_REQUEST, Json(json!({})));
    }
    let symbols: Vec<_> = s.symbols.lock().unwrap().iter().cloned().collect();
    if let Some(tickers) = q.get("tickers") {
        s.lookup_requests.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(
            s.lookup_delay_ms.load(Ordering::Relaxed),
        ))
        .await;
        if s.fail_lookup_once.swap(false, Ordering::Relaxed) {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"deliberate batch failure"})),
            );
        }
        let selected: Vec<_> = symbols
            .iter()
            .filter(|s| tickers.split(',').any(|t| t == *s))
            .map(|s| json!({"ticker":s,"status":"active"}))
            .collect();
        return (
            StatusCode::OK,
            Json(json!({"markets":selected,"cursor":""})),
        );
    }
    let second = q.contains_key("cursor");
    if second {
        s.page_count.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(
            s.page_delay_ms.load(Ordering::Relaxed),
        ))
        .await;
    }
    let slice = if second {
        &symbols[symbols.len().min(1)..]
    } else {
        &symbols[..symbols.len().min(1)]
    };
    let mut markets: Vec<_> = slice
        .iter()
        .map(|s| json!({"ticker":s,"status":"active"}))
        .collect();
    if second {
        markets.push(json!({"ticker":"COMBO","status":"active","mve_collection_ticker":"MVE"}));
        markets.push(json!({"ticker":"CLOSED","status":"closed"}));
    }
    (
        StatusCode::OK,
        Json(json!({"markets":markets,"cursor":if second {""}else{"page2"}})),
    )
}
async fn upgrade(
    State(s): State<Mock>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let valid = (|| {
        let timestamp = headers.get("KALSHI-ACCESS-TIMESTAMP")?.to_str().ok()?;
        let sig = STANDARD
            .decode(headers.get("KALSHI-ACCESS-SIGNATURE")?)
            .ok()?;
        s.public
            .verify(
                format!("{timestamp}GET/trade-api/ws/v2").as_bytes(),
                &Signature::from_slice(&sig).ok()?,
            )
            .ok()?;
        Some(())
    })()
    .is_some();
    if !valid {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.on_upgrade(move |ws| socket(ws, s))
}
async fn socket(mut ws: WebSocket, s: Mock) {
    s.connections.fetch_add(1, Ordering::Relaxed);
    let mut seq = 0u64;
    let mut symbols = BTreeSet::<String>::new();
    let mut control = false;
    let mut events = s.events.subscribe();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut steps = 0u64;
    loop {
        tokio::select! {
            message = ws.next() => {
                let Some(Ok(message)) = message else { break; };
                match message {
                    Message::Text(text) => {
                        let command: Value = serde_json::from_str(&text).unwrap();
                        s.commands.lock().unwrap().push(command.clone());
                        let id = command["id"].as_u64().unwrap();
                        let initial = command["cmd"] == "subscribe";
                        if initial {
                            control = command["params"]["channels"][0] == "market_lifecycle_v2";
                            let channel = if control { "market_lifecycle_v2" } else { "orderbook_delta" };
                            let ack = json!({"id":id,"type":"subscribed","msg":{"channel":channel,"sid":1}});
                            if ws.send(Message::Text(ack.to_string().into())).await.is_err() { break; }
                        }
                        let list: Vec<String> = command["params"]["market_tickers"].as_array()
                            .map(|v| v.iter().map(|s| s.as_str().unwrap().into()).collect())
                            .unwrap_or_default();
                        let deleting = command["params"]["action"] == "delete_markets";
                        if deleting {
                            tokio::time::sleep(Duration::from_millis(s.delete_delay_ms.load(Ordering::Relaxed))).await;
                            for ticker in &list { symbols.remove(ticker); }
                        } else {
                            symbols.extend(list.iter().cloned());
                        }
                        if !initial {
                            seq += 1;
                            let ack = json!({"id":id,"type":"ok","sid":1,"seq":seq,"msg":{"market_tickers":symbols}});
                            if ws.send(Message::Text(ack.to_string().into())).await.is_err() { break; }
                        }
                        if !deleting {
                            for ticker in list {
                                seq += 1;
                                let snapshot = json!({"type":"orderbook_snapshot","sid":1,"seq":seq,
                                    "sending_ts_ms":1700000000000i64,"msg":{"market_ticker":ticker,
                                    "yes_dollars_fp":[["0.20","2"]],"no_dollars_fp":[["0.37","12.50"]]}});
                                if ws.send(Message::Text(snapshot.to_string().into())).await.is_err() { return; }
                                *s.snapshots.lock().unwrap().entry(ticker).or_default() += 1;
                            }
                        }
                    }
                    Message::Close(_) => {
                        s.closes.fetch_add(1, Ordering::Relaxed);
                        let _ = ws.send(Message::Close(None)).await;
                        break;
                    }
                    _ => {}
                }
            }
            _ = tick.tick() => {
                if control && s.disconnect_control.swap(false, Ordering::Relaxed) {
                    let _ = ws.send(Message::Close(None)).await;
                    break;
                }
                steps += 1;
                if steps.is_multiple_of(50) && ws.send(Message::Ping(vec![1].into())).await.is_err() { break; }
                if !control {
                    for symbol in &symbols {
                        seq += 1;
                        if s.gap.swap(false, Ordering::Relaxed) { seq += 1; }
                        let delta = json!({"type":"orderbook_delta","sid":1,"seq":seq,
                            "sending_ts_ms":1700000000001i64,"msg":{"market_ticker":symbol,
                            "price_dollars":"0.2","delta_fp":"1","side":"yes","ts_ms":1700000000000i64}});
                        if ws.send(Message::Text(delta.to_string().into())).await.is_err() { return; }
                    }
                }
            }
            Ok(ticker) = events.recv(), if control => {
                let notice = json!({"type":"market_lifecycle_v2","msg":{"market_ticker":ticker,"event_type":"activated"}});
                if ws.send(Message::Text(notice.to_string().into())).await.is_err() { break; }
            }
        }
    }
}
pub async fn setup() -> (Mock, Config, tempfile::TempDir, tokio::task::JoinHandle<()>) {
    let temp = tempfile::tempdir().unwrap();
    let key = SigningKey::from_bytes(&[42; 32]);
    std::fs::write(
        temp.path().join("key.pem"),
        key.to_pkcs8_pem(Default::default()).unwrap().as_str(),
    )
    .unwrap();
    let (events, _) = broadcast::channel(64);
    let s = Mock {
        symbols: Arc::new(Mutex::new(["A".into(), "B".into()].into())),
        bad_page: Arc::new(AtomicBool::new(false)),
        bad_json_once: Arc::new(AtomicBool::new(false)),
        throttled_once: Arc::new(AtomicBool::new(false)),
        page_delay_ms: Arc::new(AtomicU64::new(0)),
        page_count: Arc::new(AtomicUsize::new(0)),
        gap: Arc::new(AtomicBool::new(false)),
        connections: Arc::new(AtomicUsize::new(0)),
        snapshots: Arc::new(Mutex::new(std::collections::HashMap::new())),
        closes: Arc::new(AtomicUsize::new(0)),
        events,
        public: key.verifying_key(),
        commands: Arc::new(Mutex::new(Vec::new())),
        lookup_requests: Arc::new(AtomicUsize::new(0)),
        lookup_delay_ms: Arc::new(AtomicU64::new(0)),
        fail_lookup_once: Arc::new(AtomicBool::new(false)),
        disconnect_control: Arc::new(AtomicBool::new(false)),
        delete_delay_ms: Arc::new(AtomicU64::new(0)),
    };
    let app = Router::new()
        .route("/trade-api/v2/markets", get(markets))
        .route("/trade-api/ws/v2", get(upgrade))
        .with_state(s.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let config = Config {
        key_id: "mock".into(),
        private_key: temp.path().join("key.pem"),
        rest_url: format!("http://{address}/trade-api/v2"),
        ws_url: format!("ws://{address}/trade-api/ws/v2"),
        output: Some(temp.path().join("capture.sbe")),
        markets_per_socket: 100,
        max_data_sockets: 199,
        discovery_seconds: 60,
        symbols: vec![],
        run_seconds: None,
    };
    (s, config, temp, server)
}
pub async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mock exchange did not reach expected state");
}

/// Failed assertions must not leave a collector child alive after the test ends.
pub struct ChildGuard(pub std::process::Child);
impl ChildGuard {
    pub async fn wait(&mut self) -> std::process::ExitStatus {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("collector did not stop within 20 seconds")
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
