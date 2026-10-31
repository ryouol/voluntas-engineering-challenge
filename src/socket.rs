use crate::{
    auth::Auth,
    discovery::Notice,
    feed::{Envelope, Feed},
    output::Output,
};
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time::{Duration, Instant, timeout},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
#[derive(Default)]
pub struct Stats {
    pub acknowledged: AtomicUsize,
    pub valid: AtomicUsize,
    pub reconnects: AtomicU64,
    pub snapshots: AtomicU64,
    pub deltas: AtomicU64,
}
async fn connect(auth: &Auth, url: &str) -> Result<Socket> {
    auth.wait_for_connection_slot().await;
    Ok(
        timeout(Duration::from_secs(15), connect_async(auth.request(url)?))
            .await
            .context("WebSocket connection timed out")??
            .0,
    )
}
async fn send(ws: &mut Socket, value: serde_json::Value) -> Result<()> {
    timeout(
        Duration::from_secs(5),
        ws.send(Message::Text(value.to_string().into())),
    )
    .await
    .context("socket write timeout")??;
    Ok(())
}
async fn close(ws: &mut Socket) {
    let _ = timeout(Duration::from_secs(3), async {
        ws.close(None).await?;
        while let Some(msg) = ws.next().await {
            match msg? {
                Message::Close(_) => break,
                _ => continue,
            }
        }
        Ok::<_, tokio_tungstenite::tungstenite::Error>(())
    })
    .await;
}
fn record_stats(stats: &Stats, record: &crate::output::Record) {
    match record {
        crate::output::Record::Snapshot(_) => {
            stats.snapshots.fetch_add(1, Ordering::Relaxed);
        }
        crate::output::Record::Update(_) => {
            stats.deltas.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}
/// The applied set acknowledges that old records have finished entering the output queue.
pub struct SubscriptionControl {
    pub desired: watch::Receiver<BTreeSet<String>>,
    pub applied: watch::Sender<BTreeSet<String>>,
}

pub async fn shard(
    id: usize,
    auth: Arc<Auth>,
    url: String,
    mut control: SubscriptionControl,
    out: Output,
    stats: Arc<Stats>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut failures = 0u32;
    let desired = &mut control.desired;
    loop {
        while desired.borrow().is_empty() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                changed = desired.changed() => { changed.context("coordinator stopped")?; }
            }
        }
        let started = Instant::now();
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = connect(&auth, &url) => result,
        };
        let result = match result {
            Ok(mut ws) => {
                let result =
                    session(&mut ws, desired, &control.applied, &out, &stats, &cancel).await;
                close(&mut ws).await;
                result
            }
            Err(error) => Err(error),
        };
        control.applied.send_replace(BTreeSet::new());
        stats.acknowledged.store(0, Ordering::Relaxed);
        stats.valid.store(0, Ordering::Relaxed);
        if cancel.is_cancelled() {
            return result;
        }
        if result.is_ok() {
            continue;
        } // An empty assignment is an intentional stop.
        if let Err(error) = result {
            eprintln!("shard={id} rebuilding after error: {error:#}");
        }
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        if started.elapsed() > Duration::from_secs(60) {
            failures = 0;
        }
        let delay = reconnect_delay(failures);
        failures = failures.saturating_add(1);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}
async fn session(
    ws: &mut Socket,
    desired: &mut watch::Receiver<BTreeSet<String>>,
    applied: &watch::Sender<BTreeSet<String>>,
    out: &Output,
    stats: &Stats,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut feed = Feed::default();
    feed.set_desired(desired.borrow_and_update().clone());
    applied.send_replace(feed.desired.clone());
    let mut command_timer = tokio::time::interval(Duration::from_millis(100));
    command_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut snapshots = checkpoint_timer();
    let mut last_frame = Instant::now();
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            changed = desired.changed() => {
                changed?;
                feed.set_desired(desired.borrow_and_update().clone());
                applied.send_replace(feed.desired.clone());
                if feed.desired.is_empty() { return Ok(()); }
            }
            _ = command_timer.tick() => {
                ensure!(last_frame.elapsed() < Duration::from_secs(45), "connection heartbeat timeout");
                if let Some(command) = feed.command()? { send(ws, command).await?; }
            }
            _ = snapshots.tick() => {
                for record in feed.snapshots(chrono::Utc::now()) {
                    record_stats(stats, &record);
                    out.send(record).await?;
                }
            }
            frame = ws.next() => {
                let received = chrono::Utc::now();
                last_frame = Instant::now();
                match frame.context("WebSocket ended")?? {
                    Message::Text(text) => {
                        let env: Envelope = serde_json::from_str(&text).context("invalid feed JSON")?;
                        if let Some(record) = feed.receive(env, received)? {
                            record_stats(stats, &record);
                            out.send(record).await?;
                        }
                    }
                    Message::Ping(data) => {
                        timeout(Duration::from_secs(5), ws.send(Message::Pong(data))).await??;
                    }
                    Message::Close(_) => anyhow::bail!("exchange closed WebSocket"),
                    Message::Binary(_) => anyhow::bail!("unexpected binary exchange frame"),
                    _ => {}
                }
            }
        }
        stats
            .acknowledged
            .store(feed.acknowledged.len(), Ordering::Relaxed);
        stats.valid.store(feed.books.len(), Ordering::Relaxed);
    }
}
/// A separate control socket subscribes before discovery and forces reconciliation on reconnect.
pub async fn lifecycle(
    auth: Arc<Auth>,
    url: String,
    notices: mpsc::Sender<Notice>,
    ready: tokio::sync::oneshot::Sender<()>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut ready = Some(ready);
    let mut failures = 0u32;
    loop {
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = connect(&auth, &url) => result,
        };
        let result = match result {
            Ok(mut ws) => {
                let result =
                    async {
                        send(&mut ws, serde_json::json!({
                        "id":1, "cmd":"subscribe", "params":{"channels":["market_lifecycle_v2"]}
                    })).await?;
                        let mut subscribed = false;
                        let deadline = Instant::now() + Duration::from_secs(20);
                        loop {
                            let frame = tokio::select! {
                                biased;
                                _ = cancel.cancelled() => return Ok(()),
                                _ = tokio::time::sleep_until(deadline), if !subscribed => {
                                    anyhow::bail!("lifecycle acknowledgment timeout");
                                }
                                frame = timeout(Duration::from_secs(45), ws.next()) => {
                                    frame.context("lifecycle heartbeat timeout")?
                                }
                            };
                            match frame.context("lifecycle socket ended")?? {
                                Message::Text(text) => {
                                    let env: Envelope = serde_json::from_str(&text)?;
                                    match env.kind.as_str() {
                                        "subscribed" => {
                                            ensure!(
                                                env.id == Some(1)
                                                    && env.msg["channel"] == "market_lifecycle_v2",
                                                "unexpected lifecycle acknowledgment"
                                            );
                                            subscribed = true;
                                            failures = 0;
                                            if let Some(ready) = ready.take() {
                                                let _ = ready.send(());
                                            } else {
                                                notices.send(Notice::Reconcile).await?;
                                            }
                                        }
                                        "market_lifecycle_v2" => {
                                            let event = env.msg["event_type"]
                                                .as_str()
                                                .context("lifecycle missing event type")?;
                                            if matches!(
                                                event,
                                                "created"
                                                    | "activated"
                                                    | "deactivated"
                                                    | "determined"
                                                    | "settled"
                                                    | "close_date_updated"
                                            ) {
                                                let ticker = env.msg["market_ticker"]
                                                    .as_str()
                                                    .context("lifecycle missing ticker")?;
                                                notices.send(Notice::Market(ticker.into())).await?;
                                            }
                                        }
                                        "error" => anyhow::bail!("lifecycle subscription rejected"),
                                        _ => {}
                                    }
                                }
                                Message::Ping(data) => {
                                    timeout(Duration::from_secs(5), ws.send(Message::Pong(data)))
                                        .await??;
                                }
                                Message::Close(_) => anyhow::bail!("lifecycle socket closed"),
                                _ => {}
                            }
                        }
                    }
                    .await;
                close(&mut ws).await;
                result
            }
            Err(error) => Err(error),
        };
        if cancel.is_cancelled() {
            return Ok(());
        }
        if let Err(error) = result {
            eprintln!("lifecycle reconnect: {error:#}");
        }
        let delay = reconnect_delay(failures);
        failures = failures.saturating_add(1);
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

fn reconnect_delay(failures: u32) -> Duration {
    Duration::from_millis(
        (500u64.saturating_mul(1 << failures.min(6))).min(30_000) + rand::random_range(0..250),
    )
}

fn checkpoint_timer() -> tokio::time::Interval {
    let mut timer = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer
}

#[cfg(test)]
#[path = "socket_tests.rs"]
mod tests;
