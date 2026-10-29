use std::fmt;
use std::time::Duration;

/// Everything that can go wrong talking to Surfra.
///
/// The accessors ([`Error::status`], [`Error::error_type`], [`Error::message`], [`Error::attempts`],
/// [`Error::retry_after`], [`Error::outcome_unknown`]) read the same facts from every variant.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Surfra refused the request, after `attempts` tries.
    Api {
        /// The HTTP status.
        status: u16,
        /// The reason Surfra gave (`error.message`, `error`, `detail`, or the status text).
        detail: String,
        /// Surfra's `error.type`, such as `SERVER_BUSY` or `APP_PAUSED`, when the answer had one.
        error_type: Option<String>,
        /// How long Surfra asked to wait (`Retry-After`, else `error.retry_after`).
        retry_after: Option<Duration>,
        /// How many requests were sent, the retries included.
        attempts: u32,
    },
    /// No credential was found. The message says what to set.
    Credential(String),
    /// The request did not complete: connection, TLS or timeout, after `attempts` tries.
    Http { source: reqwest::Error, attempts: u32 },
    /// A submission (a transaction or a job) was sent, then got a `504` or lost its connection. It may
    /// be running. It is never retried, so it is not started twice; check it before submitting again.
    OutcomeUnknown {
        /// `504` when the platform answered, `None` when the connection was lost.
        status: Option<u16>,
        message: String,
        attempts: u32,
        source: Option<reqwest::Error>,
    },
    /// Surfra's answer was not the JSON expected.
    Decode(serde_json::Error),
    /// Reading or writing local files.
    Io(std::io::Error),
    /// A call or job did not finish in time, or finished without succeeding.
    Failed(String),
}

impl Error {
    /// The HTTP status, when Surfra (or the platform in front of it) answered.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } => Some(*status),
            Error::OutcomeUnknown { status, .. } => *status,
            Error::Http { source, .. } => source.status().map(|status| status.as_u16()),
            _ => None,
        }
    }

    /// Surfra's `error.type`, such as `RATE_LIMITED`, `SERVER_BUSY`, `APP_PAUSED` or `HTTP_CALLS_LIMIT`.
    pub fn error_type(&self) -> Option<&str> {
        match self {
            Error::Api { error_type, .. } => error_type.as_deref(),
            _ => None,
        }
    }

    /// The server's message for an answer, else this error's description.
    pub fn message(&self) -> String {
        match self {
            Error::Api { detail, .. } => detail.clone(),
            Error::OutcomeUnknown { message, .. } => message.clone(),
            other => other.to_string(),
        }
    }

    /// How many requests were sent before this error, the retries included. `0` when none was sent.
    pub fn attempts(&self) -> u32 {
        match self {
            Error::Api { attempts, .. } | Error::Http { attempts, .. } | Error::OutcomeUnknown { attempts, .. } => *attempts,
            _ => 0,
        }
    }

    /// How long Surfra asked to wait before trying again, if it said.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// True when a submission may have started although this error came back. See [`Error::OutcomeUnknown`].
    pub fn outcome_unknown(&self) -> bool {
        matches!(self, Error::OutcomeUnknown { .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let tries = |attempts: u32| {
            if attempts > 1 {
                format!(" (after {attempts} attempts)")
            } else {
                String::new()
            }
        };
        match self {
            Error::Api {
                status,
                detail,
                error_type,
                attempts,
                ..
            } => match error_type {
                Some(kind) => write!(f, "{status} {kind}: {detail}{}", tries(*attempts)),
                None => write!(f, "{status}: {detail}{}", tries(*attempts)),
            },
            Error::Credential(message) | Error::Failed(message) => f.write_str(message),
            Error::Http { source, attempts } => write!(f, "request failed: {source}{}", tries(*attempts)),
            Error::OutcomeUnknown { message, .. } => f.write_str(message),
            Error::Decode(error) => write!(f, "unexpected answer: {error}"),
            Error::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http { source, .. } => Some(source),
            Error::OutcomeUnknown { source: Some(source), .. } => Some(source),
            Error::Decode(error) => Some(error),
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(source: reqwest::Error) -> Self {
        Error::Http { source, attempts: 1 }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
