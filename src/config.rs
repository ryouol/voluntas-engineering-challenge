use anyhow::{Result, ensure};
use clap::Parser;
use std::path::PathBuf;
#[derive(Parser, Debug, Clone)]
#[command(about = "Collect Kalshi order books into the Voluntas SBE format")]
pub struct Config {
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    pub key_id: String,
    #[arg(
        long,
        env = "KALSHI_PRIVATE_KEY_PATH",
        default_value = ".secrets/kalshi.pem"
    )]
    pub private_key: PathBuf,
    #[arg(long, default_value = "https://external-api.kalshi.com/trade-api/v2")]
    pub rest_url: String,
    #[arg(
        long,
        default_value = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    )]
    pub ws_url: String,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long, default_value_t = 100)]
    pub markets_per_socket: usize,
    /// Leave one connection for lifecycle notifications within the Basic tier's 200.
    #[arg(long, default_value_t = 199)]
    pub max_data_sockets: usize,
    #[arg(long, default_value_t = 60)]
    pub discovery_seconds: u64,
    /// Limit to explicitly selected live tickers for smoke tests only.
    #[arg(long, value_delimiter = ',')]
    pub symbols: Vec<String>,
    /// Stop gracefully after this many seconds; omitted means run until SIGINT.
    #[arg(long)]
    pub run_seconds: Option<u64>,
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.key_id.trim().is_empty(), "empty Kalshi key ID");
        ensure!(
            self.markets_per_socket > 0 && self.discovery_seconds > 0 && self.max_data_sockets >= 2,
            "capacity and discovery interval must be positive"
        );
        for (url, secure, local) in [
            (&self.rest_url, "https", "http"),
            (&self.ws_url, "wss", "ws"),
        ] {
            let u = reqwest::Url::parse(url)?;
            ensure!(
                u.username().is_empty() && u.password().is_none(),
                "URL must not contain credentials"
            );
            ensure!(
                u.scheme() == secure
                    || (u.scheme() == local
                        && matches!(u.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))),
                "TLS required except on loopback"
            );
        }
        Ok(())
    }
}
