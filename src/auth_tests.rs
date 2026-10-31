use super::*;
use ed25519_dalek::{Signature, Verifier};
#[test]
fn signs_path_without_query() {
    let key = SigningKey::from_bytes(&[7; 32]);
    let public = key.verifying_key();
    let auth = Auth {
        id: "test".into(),
        key,
        next_connection: tokio::sync::Mutex::new(tokio::time::Instant::now()),
    };
    let req = auth
        .request("wss://example.com/trade-api/ws/v2?x=1")
        .unwrap();
    let timestamp = req.headers()["KALSHI-ACCESS-TIMESTAMP"].to_str().unwrap();
    let bytes = STANDARD
        .decode(&req.headers()["KALSHI-ACCESS-SIGNATURE"])
        .unwrap();
    public
        .verify(
            format!("{timestamp}GET/trade-api/ws/v2").as_bytes(),
            &Signature::from_slice(&bytes).unwrap(),
        )
        .unwrap();
}
