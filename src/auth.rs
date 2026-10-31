use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer, SigningKey, pkcs8::DecodePrivateKey};
use std::path::Path;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, handshake::client::Request};
pub struct Auth {
    id: String,
    key: SigningKey,
    next_connection: tokio::sync::Mutex<tokio::time::Instant>,
}
impl Auth {
    pub fn load(id: String, path: &Path) -> Result<Self> {
        let pem = std::fs::read_to_string(path).context("read local signing key")?;
        let key = SigningKey::from_pkcs8_pem(&pem).context("expected Ed25519 PKCS#8 PEM key")?;
        Ok(Self {
            id,
            key,
            next_connection: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        })
    }
    /// Smooth connection startup/retries across shards (20 handshakes/second).
    pub async fn wait_for_connection_slot(&self) {
        let mut next = self.next_connection.lock().await;
        tokio::time::sleep_until(*next).await;
        *next = tokio::time::Instant::now() + std::time::Duration::from_millis(50);
    }
    pub fn request(&self, url: &str) -> Result<Request> {
        let mut request = url.into_client_request()?;
        let timestamp = chrono::Utc::now().timestamp_millis().to_string();
        let message = format!("{}GET{}", timestamp, request.uri().path());
        let signature = STANDARD.encode(self.key.sign(message.as_bytes()).to_bytes());
        let headers = request.headers_mut();
        headers.insert("KALSHI-ACCESS-KEY", self.id.parse()?);
        headers.insert("KALSHI-ACCESS-TIMESTAMP", timestamp.parse()?);
        headers.insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
        Ok(request)
    }
}
#[cfg(test)]
#[path = "auth_tests.rs"]
mod tests;
