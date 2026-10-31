use anyhow::Result;
use clap::Parser;
use engineering_challenge::{collector, config::Config};
use tokio_util::sync::CancellationToken;
fn main() -> Result<()> {
    // Load only this checkout's .env; do not search parent directories.
    match dotenvy::from_path(".env") {
        Ok(_) => {}
        Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let config = Config::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                signal.cancel();
            }
        });
        if let Some(seconds) = config.run_seconds {
            let stop = cancel.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
                stop.cancel();
            });
        }
        collector::run(config, cancel).await
    });
    // A permanently blocked pipe must not hold the process alive after drain timeout.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}
