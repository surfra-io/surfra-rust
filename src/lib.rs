//! The Surfra SDK for Rust: volumes, apps and jobs.
//!
//! ```no_run
//! # async fn demo() -> surfra::Result<()> {
//! use surfra::{Client, Volume};
//!
//! // A folder of a volume, synced to local disk.
//! let weights = Volume::new("weights").path("minilm/").local().await?;
//!
//! // Call a Serverless app and wait for its output.
//! let output = Client::new().run("embed", serde_json::json!({ "texts": ["hello"] }), None).await?;
//! # Ok(()) }
//! ```
//!
//! Credentials come from the environment, in Surfra's order: the volumes token Surfra gives apps and
//! jobs (`SURFRA_VOLUMES_TOKEN_FILE`), `SURFRA_API_KEY`, `SURFRA_REPLICA_TOKEN`, then the session saved
//! by `surfra login`. See [`Settings`] to set them in code.
//!
//! Without async, enable the `blocking` feature: `surfra::blocking` has the same names, minus `.await`.
//!
//! To check that a callback really came from Surfra, see [`webhooks`].

mod client;
mod error;
mod retry;
mod settings;
pub mod volumes;
pub mod webhooks;

#[cfg(feature = "blocking")]
pub mod blocking;

pub use client::{App, AppSpec, Call, Client, Job, JobSpec, Workflow};
pub use error::{Error, Result};
pub use retry::{RetryEvent, RetryPolicy};
pub use settings::Settings;
pub use volumes::{Committed, Entry, Mode, Upload, Volume, VolumePath};

/// Surfra-Client: surfra-rust/<crate version>, sent on every request so the control plane can trace errors to a release.
pub const CLIENT: &str = concat!("surfra-rust/", env!("CARGO_PKG_VERSION"));

pub(crate) fn client_headers() -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("surfra-client", reqwest::header::HeaderValue::from_static(CLIENT));
    headers
}

#[cfg(test)]
mod client_header_tests {
    #[test]
    fn the_client_header_names_this_crate_and_its_version() {
        assert_eq!(super::CLIENT, format!("surfra-rust/{}", env!("CARGO_PKG_VERSION")));
        assert_eq!(super::client_headers()["surfra-client"], super::CLIENT);
    }
}
