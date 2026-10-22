use std::path::PathBuf;

use crate::error::{Error, Result};

const DEFAULT_MASTER: &str = "https://api.surfra.io";
const DEFAULT_VOLUMES: &str = "https://volumes.surfra.io";
const PART_BYTES: u64 = 64 * 1024 * 1024;

/// Where Surfra is and which credential to send. [`Settings::from_env`] reads the same variables as
/// every Surfra SDK; set fields directly to override them.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Sent before anything else when set.
    pub api_key: Option<String>,
    /// The control plane. `SURFRA_MASTER`, default `https://api.surfra.io`.
    pub master: String,
    /// The volumes service. `SURFRA_VOLUMES_URL`, default `https://volumes.surfra.io`.
    /// Inside a Surfra app or job this is a local proxy that adds the credential itself.
    pub volumes_url: String,
    /// Where `local()` keeps synced files. `SURFRA_VOLUME_CACHE`, default `<temp dir>/surfra`.
    pub cache: PathBuf,
    /// The largest single upload; larger files go in parts. `SURFRA_VOLUMES_PART_BYTES`, default 64 MiB.
    pub part_bytes: u64,
    /// `SURFRA_VOLUMES_TOKEN_FILE`: a volumes-only token Surfra keeps fresh, read on every request.
    pub token_file: Option<PathBuf>,
    /// `SURFRA_API_KEY`.
    pub env_api_key: Option<String>,
    /// `SURFRA_REPLICA_TOKEN`.
    pub replica_token: Option<String>,
    /// The session `surfra login` saves, `~/.surfra/config.json`.
    pub login_file: Option<PathBuf>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|value| value.trim().to_string()).filter(|value| !value.is_empty())
}

impl Settings {
    /// Read `SURFRA_*` variables, falling back to Surfra's defaults.
    pub fn from_env() -> Self {
        Settings {
            api_key: None,
            master: env("SURFRA_MASTER").unwrap_or_else(|| DEFAULT_MASTER.into()).trim_end_matches('/').into(),
            volumes_url: env("SURFRA_VOLUMES_URL").unwrap_or_else(|| DEFAULT_VOLUMES.into()).trim_end_matches('/').into(),
            cache: env("SURFRA_VOLUME_CACHE")
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("surfra")),
            part_bytes: env("SURFRA_VOLUMES_PART_BYTES").and_then(|value| value.parse().ok()).unwrap_or(PART_BYTES),
            token_file: env("SURFRA_VOLUMES_TOKEN_FILE").map(PathBuf::from),
            env_api_key: env("SURFRA_API_KEY"),
            replica_token: env("SURFRA_REPLICA_TOKEN"),
            login_file: std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".surfra").join("config.json")),
        }
    }

    /// The same settings with an explicit API key, which wins over every other credential.
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// The credential to send, in Surfra's order: an explicit key, the token file, `SURFRA_API_KEY`,
    /// `SURFRA_REPLICA_TOKEN`, then the saved login.
    pub fn credential(&self) -> Result<String> {
        if let Some(key) = &self.api_key {
            return Ok(key.clone());
        }
        if let Some(path) = &self.token_file {
            if let Ok(token) = std::fs::read_to_string(path) {
                let token = token.trim();
                if !token.is_empty() {
                    return Ok(token.to_string());
                }
            }
        }
        if let Some(key) = self.env_api_key.as_ref().or(self.replica_token.as_ref()) {
            return Ok(key.clone());
        }
        if let Some(path) = &self.login_file {
            if let Ok(text) = std::fs::read_to_string(path) {
                if let Some(token) = serde_json::from_str::<serde_json::Value>(&text)
                    .ok()
                    .and_then(|saved| saved.get("token").and_then(|token| token.as_str().map(str::to_string)))
                {
                    return Ok(token);
                }
            }
        }
        Err(Error::Credential("no credential: set SURFRA_API_KEY or run `surfra login`".into()))
    }
}

impl Default for Settings {
    fn default() -> Self {
        Settings::from_env()
    }
}
