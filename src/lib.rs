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

mod client;
mod error;
mod settings;
pub mod volumes;

#[cfg(feature = "blocking")]
pub mod blocking;

pub use client::{Call, Client, Job, JobSpec};
pub use error::{Error, Result};
pub use settings::Settings;
pub use volumes::{Committed, Entry, Mode, Upload, Volume, VolumePath};
