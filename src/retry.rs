//! One retry loop for every control-plane and volumes request.
//!
//! Classification follows the shared SDK contract (surfra-sdk-tests/CASES.md, "Retries"):
//!
//! | Answer | Retry? |
//! | --- | --- |
//! | any `429`; `503 QUEUE_FULL`; `503 SERVER_BUSY` (any typed `RATE_LIMITED`/`QUEUE_FULL`/`SERVER_BUSY`) | every method |
//! | `APP_PAUSED`, `DEFCON_ACTIVE`, `402 HTTP_CALLS_LIMIT` | never |
//! | `502`/`503`/`504` without a Surfra error body | `GET`/`HEAD` only |
//! | `504` or a dropped connection after a submission was sent | never: the outcome is unknown |
//! | a connection that never reached the server | every method |
//! | a connection dropped after the request was sent | `GET`/`HEAD` only |
//! | any other answer | never |

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use reqwest::header::HeaderMap;
use reqwest::{Method, RequestBuilder, Response};
use serde_json::Value;

use crate::error::{Error, Result};

/// A function told about each retry before its wait.
pub type RetryHook = Arc<dyn Fn(&RetryEvent) + Send + Sync>;

/// How many times, and for how long, a request is retried. See the crate README, "Errors and retries".
///
/// ```
/// use std::time::Duration;
/// use surfra::RetryPolicy;
///
/// let patient = RetryPolicy {
///     max_retries: 10,
///     deadline: Duration::from_secs(600),
///     ..RetryPolicy::default()
/// }
/// .on_retry(|event| eprintln!("surfra: {event}"));
/// let never = RetryPolicy::none();
/// ```
#[derive(Clone)]
pub struct RetryPolicy {
    /// Retries after the first attempt. Default 7, so at most 8 attempts. `0` turns retries off.
    pub max_retries: u32,
    /// How long, from the first attempt, retries may go on. A wait that would end past it is not taken:
    /// the last error is returned at once. Default 3 minutes.
    pub deadline: Duration,
    /// Called before each wait, with the attempt that failed, why, and how long the wait is.
    /// Nothing is reported when this is `None` (the default).
    pub on_retry: Option<RetryHook>,
}

impl RetryPolicy {
    /// The default attempts and deadline.
    pub const DEFAULT_MAX_RETRIES: u32 = 7;
    pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(180);

    /// No retries: every request is tried once.
    pub fn none() -> Self {
        RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        }
    }

    /// The same policy, reporting each retry to `hook`.
    pub fn on_retry(mut self, hook: impl Fn(&RetryEvent) + Send + Sync + 'static) -> Self {
        self.on_retry = Some(Arc::new(hook));
        self
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_retries: Self::DEFAULT_MAX_RETRIES,
            deadline: Self::DEFAULT_DEADLINE,
            on_retry: None,
        }
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_retries", &self.max_retries)
            .field("deadline", &self.deadline)
            .field("on_retry", &self.on_retry.as_ref().map(|_| "Fn(&RetryEvent)"))
            .finish()
    }
}

/// One retry about to happen, as given to [`RetryPolicy::on_retry`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct RetryEvent {
    pub method: Method,
    pub url: String,
    /// The attempt that failed, from 1.
    pub attempt: u32,
    /// Why it is retried, such as `503 SERVER_BUSY` or `connection failed: ...`.
    pub reason: String,
    /// The HTTP status, when Surfra answered.
    pub status: Option<u16>,
    /// The `error.type` Surfra sent, if any.
    pub error_type: Option<String>,
    /// How long the SDK waits before the next attempt.
    pub wait: Duration,
}

impl fmt::Display for RetryEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} attempt {} failed ({}); retrying in {:.2}s",
            self.method,
            self.url,
            self.attempt,
            self.reason,
            self.wait.as_secs_f64()
        )
    }
}

/// The error types Surfra marks as safe to retry for every method.
const RETRYABLE: [&str; 3] = ["RATE_LIMITED", "QUEUE_FULL", "SERVER_BUSY"];

/// What a request is, for classifying its failures.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// Anything that is not a submission.
    Plain,
    /// A `POST` that starts work (a transaction or a job): a 504 or a dropped connection after it
    /// was sent leaves the outcome unknown.
    Submission,
}

/// A successful answer, read in full inside the loop so a body cut short is retried like any other
/// dropped connection.
pub(crate) struct Reply {
    pub body: Vec<u8>,
}

impl Reply {
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).map_err(Error::Decode)
    }
}

/// A Surfra error answer: status, `error.type`, message and `Retry-After`.
pub(crate) fn api_error(status: u16, reason: &str, headers: &HeaderMap, text: &str, attempts: u32) -> Error {
    let body = serde_json::from_str::<Value>(text).ok();
    let error = body.as_ref().and_then(|body| body.get("error"));
    let error_type = error.and_then(|error| error.get("type")).and_then(Value::as_str).map(str::to_string);
    let detail = error
        .and_then(|error| error.get("message").or(Some(error)))
        .and_then(Value::as_str)
        .or_else(|| body.as_ref().and_then(|body| body.get("detail")).and_then(Value::as_str))
        .map(str::to_string)
        .unwrap_or_else(|| if text.trim().is_empty() { reason.to_string() } else { text.to_string() });
    let retry_after = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<f64>().ok())
        .or_else(|| error.and_then(|error| error.get("retry_after")).and_then(Value::as_f64))
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
        .map(Duration::from_secs_f64);
    Error::Api {
        status,
        detail,
        error_type,
        retry_after,
        attempts,
    }
}

fn safe(method: &Method) -> bool {
    *method == Method::GET || *method == Method::HEAD
}

enum Verdict {
    Retry,
    Stop,
    Unknown,
}

fn classify_answer(method: &Method, kind: Kind, error: &Error) -> Verdict {
    let Error::Api { status, error_type, .. } = error else { return Verdict::Stop };
    match error_type.as_deref() {
        Some(kind) if RETRYABLE.contains(&kind) => Verdict::Retry,
        Some(_) if *status == 429 => Verdict::Retry,
        Some(_) => Verdict::Stop,
        None if *status == 429 => Verdict::Retry,
        None if matches!(status, 502..=504) && safe(method) => Verdict::Retry,
        None if *status == 504 && kind == Kind::Submission => Verdict::Unknown,
        None => Verdict::Stop,
    }
}

fn classify_transport(method: &Method, kind: Kind, error: &reqwest::Error) -> Verdict {
    if error.is_connect() {
        // Never reached the server: nothing can have happened.
        Verdict::Retry
    } else if safe(method) {
        Verdict::Retry
    } else if kind == Kind::Submission {
        Verdict::Unknown
    } else {
        Verdict::Stop
    }
}

const UNKNOWN: &str = "the outcome is unknown: the submission was sent, so the work may be running. \
     Check it before submitting again, or for long work use enqueue and poll its result, or a callback_url";

/// Full-jitter exponential backoff: a random wait in `[0, min(30 s, 0.5 s * 2^retry))`.
fn backoff(retry: u32) -> Duration {
    let cap = (0.5 * 2f64.powi(retry.min(16) as i32)).min(30.0);
    Duration::from_secs_f64(cap * random())
}

/// A uniform number in `[0, 1)`. Backoff jitter needs no cryptographic quality, so this is a xorshift64*
/// generator shared by the process, seeded from the clock, rather than a dependency on `rand`.
fn random() -> f64 {
    static STATE: AtomicU64 = AtomicU64::new(0);
    let mut x = STATE.load(Ordering::Relaxed);
    if x == 0 {
        x = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15)
            | 1;
    }
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    STATE.store(x, Ordering::Relaxed);
    (x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
}

/// Send a request until it succeeds or the policy says stop. `build` makes a fresh request each attempt
/// (and may fail, such as for a missing credential, which is returned at once). `finish` turns a
/// successful response into the result; a transport error there is classified like one from `send`.
pub(crate) async fn run<T, B, F, Fut>(policy: &RetryPolicy, method: Method, url: &str, kind: Kind, build: B, finish: F) -> Result<T>
where
    B: Fn() -> Result<RequestBuilder>,
    F: Fn(Response) -> Fut,
    Fut: Future<Output = std::result::Result<T, reqwest::Error>>,
{
    let started = Instant::now();
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let outcome = match build()?.send().await {
            Ok(response) if response.status().is_success() => match finish(response).await {
                Ok(value) => return Ok(value),
                Err(error) => Err(error),
            },
            Ok(response) => {
                let status = response.status();
                let headers = response.headers().clone();
                match response.text().await {
                    Ok(text) => Ok(api_error(status.as_u16(), status.canonical_reason().unwrap_or(""), &headers, &text, attempt)),
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        };
        let (verdict, error) = match outcome {
            Ok(answer) => (classify_answer(&method, kind, &answer), answer),
            Err(source) => {
                let verdict = classify_transport(&method, kind, &source);
                (verdict, Error::Http { source, attempts: attempt })
            }
        };
        match verdict {
            Verdict::Stop => return Err(error),
            Verdict::Unknown => {
                let (status, source) = match error {
                    Error::Api { status, .. } => (Some(status), None),
                    Error::Http { source, .. } => (None, Some(source)),
                    _ => (None, None),
                };
                let what = status.map_or_else(|| "the connection was lost".to_string(), |code| format!("{code} from {url}"));
                return Err(Error::OutcomeUnknown {
                    status,
                    message: format!("{what}: {UNKNOWN}"),
                    attempts: attempt,
                    source,
                });
            }
            Verdict::Retry => {}
        }
        if attempt > policy.max_retries {
            return Err(error);
        }
        let wait = error.retry_after().unwrap_or_else(|| backoff(attempt - 1));
        if started.elapsed() + wait > policy.deadline {
            return Err(error);
        }
        if let Some(hook) = &policy.on_retry {
            let reason = match &error {
                Error::Api { status, error_type, .. } => match error_type {
                    Some(kind) => format!("{status} {kind}"),
                    None => status.to_string(),
                },
                other => other.to_string(),
            };
            hook(&RetryEvent {
                method: method.clone(),
                url: url.to_string(),
                attempt,
                reason,
                status: error.status(),
                error_type: error.error_type().map(str::to_string),
                wait,
            });
        }
        tokio::time::sleep(wait).await;
    }
}

/// [`run`], reading a successful body in full.
pub(crate) async fn buffered<B>(policy: &RetryPolicy, method: Method, url: &str, kind: Kind, build: B) -> Result<Reply>
where
    B: Fn() -> Result<RequestBuilder>,
{
    run(policy, method, url, kind, build, |response| async move {
        let body: Vec<u8> = response.bytes().await?.into();
        Ok(Reply { body })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_inside_the_cap() {
        for retry in 0..12 {
            let cap = (0.5 * 2f64.powi(retry)).min(30.0);
            for _ in 0..200 {
                let wait = backoff(retry as u32).as_secs_f64();
                assert!((0.0..cap).contains(&wait), "{wait} for retry {retry}");
            }
        }
    }

    #[test]
    fn errors_parse_type_message_and_retry_after() {
        let mut headers = HeaderMap::new();
        let text = r#"{"detail":"busy","error":{"type":"SERVER_BUSY","message":"short of connections","retry_after":2}}"#;
        let error = api_error(503, "Service Unavailable", &headers, text, 1);
        assert_eq!(error.error_type(), Some("SERVER_BUSY"));
        assert_eq!(error.message(), "short of connections");
        assert_eq!(error.retry_after(), Some(Duration::from_secs(2)));
        headers.insert(reqwest::header::RETRY_AFTER, "0".parse().unwrap());
        assert_eq!(api_error(503, "", &headers, text, 1).retry_after(), Some(Duration::ZERO));
        let plain = api_error(502, "Bad Gateway", &HeaderMap::new(), "", 1);
        assert_eq!((plain.error_type(), plain.message()), (None, "Bad Gateway".to_string()));
        let volumes = api_error(404, "Not Found", &HeaderMap::new(), r#"{"error":"no such file"}"#, 1);
        assert_eq!((volumes.error_type(), volumes.message()), (None, "no such file".to_string()));
    }
}
