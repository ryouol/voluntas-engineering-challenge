use crate::{
    auth::Auth,
    config::Config,
    discovery::{self, Discovery},
    output::{self, Record},
    socket::{self, Stats},
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeSet,
    io::{IsTerminal, Write},
    sync::{Arc, atomic::Ordering},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
    time::{Duration, Instant, timeout},
};
use tokio_util::sync::CancellationToken;

/// Stable ownership avoids resubscribing unaffected markets during reconciliation.
pub fn assign(shards: &mut Vec<BTreeSet<String>>, desired: &BTreeSet<String>, capacity: usize) {
    for shard in shards.iter_mut() {
        shard.retain(|s| desired.contains(s));
    }
    let required = desired.len().div_ceil(capacity).max(desired.len().min(2));
    while shards.len() < required {
        shards.push(BTreeSet::new());
    }
    let assigned: BTreeSet<_> = shards.iter().flatten().cloned().collect();
    for symbol in desired.difference(&assigned) {
        let id = shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.len() < capacity)
            .min_by_key(|(i, s)| (s.len(), *i))
            .map(|(i, _)| i)
            .expect("capacity allocated");
        shards[id].insert(symbol.clone());
    }
    // A universe can shrink until all remaining symbols share one old shard.
    // Move just one symbol to preserve the two-socket baseline in that case.
    if desired.len() > 1 && shards.iter().filter(|s| !s.is_empty()).count() == 1 {
        let source = shards.iter().position(|s| !s.is_empty()).unwrap();
        let destination = shards.iter().position(BTreeSet::is_empty).unwrap();
        let symbol = shards[source].pop_last().unwrap();
        shards[destination].insert(symbol);
    }
}
pub async fn run(config: Config, cancel: CancellationToken) -> Result<()> {
    config.validate()?;
    let auth = Arc::new(Auth::load(config.key_id.clone(), &config.private_key)?);
    let discovery = Discovery::new(&config)?;
    let sink: Box<dyn Write + Send> = if let Some(path) = &config.output {
        Box::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .context("create output (path must not already exist)")?,
        )
    } else {
        ensure!(
            !std::io::stdout().is_terminal(),
            "binary output requires stdout redirection or --output"
        );
        Box::new(std::io::stdout())
    };
    let (out, writer) = output::start(sink, cancel.clone());
    let mut tasks = JoinSet::new();
    let result = async {
        let (notices_tx, notices_rx) = mpsc::channel(4096);
        let (ready_tx, ready_rx) = oneshot::channel();
        tasks.spawn(socket::lifecycle(
            auth.clone(), config.ws_url.clone(), notices_tx, ready_tx, cancel.clone(),
        ));
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            result = timeout(Duration::from_secs(45), ready_rx) => {
                result.context("could not establish authenticated lifecycle subscription in 45 seconds")??;
            }
        }
        let (universe_tx, mut universe_rx) = watch::channel(BTreeSet::new());
        tasks.spawn(discovery::run(
            discovery, config.discovery_seconds, notices_rx, universe_tx, cancel.clone(),
        ));
        let mut assignments: Vec<BTreeSet<String>> = Vec::new();
        let mut controls: Vec<watch::Sender<BTreeSet<String>>> = Vec::new();
        let mut applied: Vec<watch::Receiver<BTreeSet<String>>> = Vec::new();
        let mut stats: Vec<Arc<Stats>> = Vec::new();
        let mut status = tokio::time::interval(Duration::from_secs(10));
        status.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut flush = tokio::time::interval_at(
            Instant::now() + Duration::from_secs(60), Duration::from_secs(60),
        );
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                joined = tasks.join_next() => {
                    let task = joined.context("service tasks stopped")?;
                    let outcome = task.context("service task panicked")?;
                    outcome?;
                    if cancel.is_cancelled() { return Ok(()); }
                    anyhow::bail!("service stopped unexpectedly");
                }
                changed = universe_rx.changed() => {
                    changed?;
                    let desired = universe_rx.borrow_and_update().clone();
                    let capacity = config.markets_per_socket
                        .max(desired.len().div_ceil(config.max_data_sockets));
                    assign(&mut assignments, &desired, capacity);
                    // Apply removals everywhere before any destination receives additions.
                    // This also covers reactivation on a different least-loaded socket.
                    for (id, control) in controls.iter().enumerate() {
                        let retained = control.borrow().intersection(&assignments[id]).cloned().collect();
                        control.send_if_modified(|current| {
                            if *current == retained { false } else { *current = retained; true }
                        });
                    }
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return Ok(()),
                        result = timeout(Duration::from_secs(15), wait_for_releases(&mut applied, &assignments)) => {
                            result.context("market ownership handoff timed out")??;
                        }
                    }
                    for (id, symbols) in assignments.iter().enumerate() {
                        if id == controls.len() {
                            let (tx, rx) = watch::channel(symbols.clone());
                            let (applied_tx, applied_rx) = watch::channel(BTreeSet::new());
                            let control = socket::SubscriptionControl { desired: rx, applied: applied_tx };
                            let st = Arc::new(Stats::default());
                            tasks.spawn(socket::shard(
                                id, auth.clone(), config.ws_url.clone(), control,
                                out.clone(), st.clone(), cancel.clone(),
                            ));
                            controls.push(tx);
                            applied.push(applied_rx);
                            stats.push(st);
                        } else if *controls[id].borrow() != *symbols {
                            controls[id].send_replace(symbols.clone());
                        }
                    }
                }
                _ = flush.tick() => out.send(Record::Flush).await?,
                _ = status.tick() => report(&stats, &assignments, universe_rx.borrow().len(), &out),
            }
        }
    }.await;
    cancel.cancel();
    let drained = timeout(Duration::from_secs(15), async {
        let mut failure = None;
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result.context("service task panicked").and_then(|r| r) {
                failure = Some(e);
            }
        }
        match failure {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })
    .await;
    drop(out);
    let written = timeout(Duration::from_secs(20), writer)
        .await
        .context("writer drain timed out; output may be incomplete")
        .and_then(|r| r.context("writer panicked"))
        .and_then(|r| r);
    result?;
    drained.context("socket shutdown timed out")??;
    written?;
    eprintln!("shutdown complete: sockets closed and output flushed");
    Ok(())
}
async fn wait_for_releases(
    applied: &mut [watch::Receiver<BTreeSet<String>>],
    assignments: &[BTreeSet<String>],
) -> Result<()> {
    for (source, retained) in applied.iter_mut().zip(assignments) {
        while !source.borrow_and_update().is_subset(retained) {
            source
                .changed()
                .await
                .context("source shard stopped during handoff")?;
        }
    }
    Ok(())
}

fn report(
    stats: &[Arc<Stats>],
    assignments: &[BTreeSet<String>],
    desired: usize,
    out: &output::Output,
) {
    let sum = |get: fn(&Stats) -> u64| stats.iter().map(|s| get(s)).sum::<u64>();
    eprintln!(
        "status desired={} acknowledged={} valid={} sockets={} reconnects={} snapshots={} deltas={} written={} queue={}",
        desired,
        sum(|s| s.acknowledged.load(Ordering::Relaxed) as u64),
        sum(|s| s.valid.load(Ordering::Relaxed) as u64),
        assignments.iter().filter(|s| !s.is_empty()).count(),
        sum(|s| s.reconnects.load(Ordering::Relaxed)),
        sum(|s| s.snapshots.load(Ordering::Relaxed)),
        sum(|s| s.deltas.load(Ordering::Relaxed)),
        out.written.load(Ordering::Relaxed),
        output::QUEUE_CAPACITY - out.capacity(),
    );
}
#[cfg(test)]
#[path = "collector_tests.rs"]
mod tests;
