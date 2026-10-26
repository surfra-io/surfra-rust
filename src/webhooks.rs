//! Verify the callbacks Surfra sends to a `callback_url`.
//!
//! Each callback carries an `X-Surfra-Signature` header:
//!
//! ```text
//! X-Surfra-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>
//! ```
//!
//! While a signing secret is being rotated the header holds one `v1=` per secret; a callback is
//! genuine when any of them matches any of your secrets. Callbacks signed more than [`TOLERANCE`]
//! away from your clock are rejected, so a captured callback cannot be replayed later.
//!
//! Verify the raw body bytes exactly as received, before parsing them. With axum:
//!
//! ```ignore
//! use axum::{body::Bytes, http::{HeaderMap, StatusCode}};
//!
//! async fn callback(headers: HeaderMap, body: Bytes) -> StatusCode {
//!     let secret = std::env::var("SURFRA_CALLBACK_SECRET").unwrap();
//!     match surfra::webhooks::verify_headers(&body, &headers, &[&secret]) {
//!         Ok(event) => {
//!             println!("{} seq {}", event["type"], event["seq"]);
//!             StatusCode::OK
//!         }
//!         Err(_) => StatusCode::BAD_REQUEST,
//!     }
//! }
//! ```
//!
//! The functions do no I/O, so `surfra::blocking::webhooks` is this same module.
//!
//! ```
//! use surfra::webhooks::{self, WebhookError};
//!
//! let header = "t=1761415391,v1=0000000000000000000000000000000000000000000000000000000000000000";
//! let result = webhooks::verify_at(b"{}", header, &["whsec_example"], 1761415391, webhooks::TOLERANCE);
//! assert!(matches!(result, Err(WebhookError::Mismatch)));
//! ```

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, KeyInit, Mac};
use reqwest::header::HeaderMap;
use serde_json::Value;
use sha2::Sha256;

/// How far the signature's timestamp may be from the verifier's clock, either way.
pub const TOLERANCE: Duration = Duration::from_secs(300);

/// The header Surfra signs callbacks with.
pub const HEADER: &str = "X-Surfra-Signature";

/// Why a callback was rejected.
#[derive(Debug)]
pub enum WebhookError {
    /// No `X-Surfra-Signature` header, or an empty one.
    Missing,
    /// The header is not `t=<seconds>,v1=<hex>[,v1=<hex>...]`.
    Malformed,
    /// The timestamp is more than the tolerance away from now.
    Expired,
    /// No signature matches any of the secrets.
    Mismatch,
    /// The signature matches but the body is not JSON.
    Body(serde_json::Error),
}

impl fmt::Display for WebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebhookError::Missing => write!(f, "the callback has no {HEADER} header"),
            WebhookError::Malformed => write!(f, "the {HEADER} header is malformed"),
            WebhookError::Expired => write!(f, "the callback's timestamp is outside the tolerance"),
            WebhookError::Mismatch => write!(f, "the callback's signature does not match"),
            WebhookError::Body(error) => write!(f, "the callback's body is not JSON: {error}"),
        }
    }
}

impl std::error::Error for WebhookError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WebhookError::Body(error) => Some(error),
            _ => None,
        }
    }
}

/// Verify a callback against the current clock and [`TOLERANCE`], and return its parsed body.
pub fn verify(body: &[u8], signature_header: &str, secrets: &[&str]) -> Result<Value, WebhookError> {
    let now = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_secs() as i64,
        Err(before) => -(before.duration().as_secs() as i64),
    };
    verify_at(body, signature_header, secrets, now, TOLERANCE)
}

/// Verify a callback as of `now_unix` (seconds), and return its parsed body.
pub fn verify_at(body: &[u8], signature_header: &str, secrets: &[&str], now_unix: i64, tolerance: Duration) -> Result<Value, WebhookError> {
    let header = signature_header.trim();
    if header.is_empty() {
        return Err(WebhookError::Missing);
    }
    let mut timestamp: Option<&str> = None;
    let mut signatures: Vec<Vec<u8>> = Vec::new();
    for part in header.split(',') {
        let (key, value) = part.trim().split_once('=').ok_or(WebhookError::Malformed)?;
        match key.trim() {
            "t" if timestamp.is_none() => timestamp = Some(value.trim()),
            "t" => return Err(WebhookError::Malformed),
            "v1" => signatures.push(decode_hex(value.trim()).ok_or(WebhookError::Malformed)?),
            _ => {} // Other schemes are ignored, so new ones can be added without breaking this one.
        }
    }
    let timestamp = timestamp.ok_or(WebhookError::Malformed)?;
    if signatures.is_empty() || timestamp.is_empty() || !timestamp.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(WebhookError::Malformed);
    }
    let t: i64 = timestamp.parse().map_err(|_| WebhookError::Malformed)?;
    if now_unix.abs_diff(t) > tolerance.as_secs() {
        return Err(WebhookError::Expired);
    }
    let matched = secrets.iter().any(|secret| {
        signatures.iter().any(|signature| {
            let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).expect("HMAC takes a key of any length");
            mac.update(timestamp.as_bytes());
            mac.update(b".");
            mac.update(body);
            mac.verify_slice(signature).is_ok()
        })
    });
    if !matched {
        return Err(WebhookError::Mismatch);
    }
    serde_json::from_slice(body).map_err(WebhookError::Body)
}

/// Verify a callback by its `X-Surfra-Signature` header, against the current clock and [`TOLERANCE`].
pub fn verify_headers(body: &[u8], headers: &HeaderMap, secrets: &[&str]) -> Result<Value, WebhookError> {
    let header = headers.get(HEADER).ok_or(WebhookError::Missing)?;
    verify(body, header.to_str().map_err(|_| WebhookError::Malformed)?, secrets)
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = text.as_bytes();
    if bytes.is_empty() || bytes.len() % 2 != 0 {
        return None;
    }
    bytes.chunks(2).map(|pair| Some(nibble(pair[0])? << 4 | nibble(pair[1])?)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, t: i64, body: &[u8]) -> String {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(format!("{t}.").as_bytes());
        mac.update(body);
        mac.finalize().into_bytes().iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn errors_name_the_reason() {
        let body = br#"{"seq":7}"#;
        let good = sign("s", 100, body);
        let check = |header: &str| verify_at(body, header, &["s"], 100, TOLERANCE);
        assert_eq!(check(&format!("t=100,v1={good}")).unwrap()["seq"], 7);
        assert_eq!(check(&format!(" t=100 , v1={} ", good.to_uppercase())).unwrap()["seq"], 7);
        assert!(matches!(check(""), Err(WebhookError::Missing)));
        assert!(matches!(check("garbage"), Err(WebhookError::Malformed)));
        assert!(matches!(check("t=100"), Err(WebhookError::Malformed)));
        assert!(matches!(check(&format!("v1={good}")), Err(WebhookError::Malformed)));
        assert!(matches!(check(&format!("t=-100,v1={good}")), Err(WebhookError::Malformed)));
        assert!(matches!(check(&format!("t=100,t=100,v1={good}")), Err(WebhookError::Malformed)));
        assert!(matches!(check("t=100,v1=zz"), Err(WebhookError::Malformed)));
        assert!(matches!(check(&format!("t=100,v1={}", &good[..62])), Err(WebhookError::Mismatch)));
        assert!(matches!(
            verify_at(body, &format!("t=100,v1={good}"), &["s"], 401, TOLERANCE),
            Err(WebhookError::Expired)
        ));
        let not_json = sign("s", 100, b"nope");
        assert!(matches!(
            verify_at(b"nope", &format!("t=100,v1={not_json}"), &["s"], 100, TOLERANCE),
            Err(WebhookError::Body(_))
        ));
    }

    #[test]
    fn headers_are_read_by_name() {
        let body = br#"{"seq":7}"#;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let mut headers = HeaderMap::new();
        assert!(matches!(verify_headers(body, &headers, &["s"]), Err(WebhookError::Missing)));
        headers.insert("x-surfra-signature", format!("t={now},v1={}", sign("s", now, body)).parse().unwrap());
        assert_eq!(verify_headers(body, &headers, &["old", "s"]).unwrap()["seq"], 7);
    }
}
