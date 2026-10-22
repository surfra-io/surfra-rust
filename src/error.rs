use std::fmt;

/// Everything that can go wrong talking to Surfra.
#[derive(Debug)]
pub enum Error {
    /// Surfra refused the request. `status` is the HTTP code, `detail` the reason it gave.
    Api { status: u16, detail: String },
    /// No credential was found. The message says what to set.
    Credential(String),
    /// The request did not complete: connection, TLS or timeout.
    Http(reqwest::Error),
    /// Reading or writing local files.
    Io(std::io::Error),
    /// A call or job did not finish in time, or finished without succeeding.
    Failed(String),
}

impl Error {
    /// The HTTP status, when Surfra answered with an error.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } => Some(*status),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Api { status, detail } => write!(f, "{status}: {detail}"),
            Error::Credential(message) | Error::Failed(message) => f.write_str(message),
            Error::Http(error) => write!(f, "request failed: {error}"),
            Error::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Http(error) => Some(error),
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Error::Http(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Turn a non-success response into `Error::Api`, reading `{"error": ...}` or `{"detail": ...}`.
pub(crate) async fn checked(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status().as_u16();
    let reason = response.status().canonical_reason().unwrap_or("").to_string();
    let text = response.text().await.unwrap_or_default();
    let detail = serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|body| {
            body.get("error")
                .or_else(|| body.get("detail"))
                .and_then(|value| value.as_str().map(str::to_string))
        })
        .unwrap_or(if text.is_empty() { reason } else { text });
    Err(Error::Api { status, detail })
}
