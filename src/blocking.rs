//! The same API without async, for code that has no runtime. Enable the `blocking` feature.
//!
//! ```no_run
//! # fn demo() -> surfra::Result<()> {
//! let path = surfra::blocking::Volume::new("weights").path("minilm/").local()?;
//! # Ok(()) }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::runtime::Runtime;

use crate::{Committed, Entry, JobSpec, Mode, Result, Settings};

fn runtime() -> Arc<Runtime> {
    Arc::new(tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a Tokio runtime"))
}

/// A volume by name. See [`crate::Volume`].
pub struct Volume {
    inner: crate::Volume,
    rt: Arc<Runtime>,
}

/// A file or folder inside a volume.
pub struct VolumePath<'a> {
    volume: &'a Volume,
    path: String,
}

impl Volume {
    pub fn new(name: impl Into<String>) -> Self {
        Volume::with_settings(name, Settings::from_env())
    }

    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        Volume {
            inner: crate::Volume::with_settings(name, settings),
            rt: runtime(),
        }
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    pub fn grant(&self, mode: Mode) -> String {
        self.inner.grant(mode)
    }

    pub fn path(&self, path: impl Into<String>) -> VolumePath<'_> {
        VolumePath {
            volume: self,
            path: path.into(),
        }
    }

    pub fn create(&self) -> Result<Value> {
        self.rt.block_on(self.inner.create())
    }

    pub fn info(&self) -> Result<Value> {
        self.rt.block_on(self.inner.info())
    }

    pub fn delete(&self) -> Result<Value> {
        self.rt.block_on(self.inner.delete())
    }

    pub fn ls(&self, prefix: &str, recursive: bool) -> Result<Vec<Entry>> {
        self.rt.block_on(self.inner.ls(prefix, recursive))
    }

    pub fn rm(&self, path: &str) -> Result<()> {
        self.rt.block_on(self.inner.rm(path))
    }

    pub fn rm_recursive(&self, folder: &str) -> Result<usize> {
        self.rt.block_on(self.inner.rm_recursive(folder))
    }

    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.rt.block_on(self.inner.read(path))
    }

    pub fn write(&self, remote: &str, data: impl Into<Vec<u8>>) -> Result<Entry> {
        self.rt.block_on(self.inner.write(remote, data))
    }

    pub fn put_file(&self, local: impl AsRef<Path>, remote: &str) -> Result<Entry> {
        self.rt.block_on(self.inner.put_file(local, remote))
    }

    pub fn put_directory(&self, local: impl AsRef<Path>, remote: &str) -> Result<usize> {
        self.rt.block_on(self.inner.put_directory(local, remote))
    }

    pub fn commit(&self) -> Result<Committed> {
        self.rt.block_on(self.inner.commit())
    }
}

impl VolumePath<'_> {
    pub fn local(&self) -> Result<PathBuf> {
        self.volume.rt.block_on(self.volume.inner.path(self.path.clone()).local())
    }

    pub fn local_with(&self, mode: Mode, refresh: bool) -> Result<PathBuf> {
        self.volume.rt.block_on(self.volume.inner.path(self.path.clone()).local_with(mode, refresh))
    }

    pub fn download(&self, dest: impl AsRef<Path>) -> Result<PathBuf> {
        self.volume.rt.block_on(self.volume.inner.path(self.path.clone()).download(dest))
    }
}

/// The control plane. See [`crate::Client`].
pub struct Client {
    inner: crate::Client,
    rt: Arc<Runtime>,
}

impl Client {
    pub fn new() -> Self {
        Client::with_settings(Settings::from_env())
    }

    pub fn with_settings(settings: Settings) -> Self {
        Client {
            inner: crate::Client::with_settings(settings),
            rt: runtime(),
        }
    }

    /// Call an app and wait for its output.
    pub fn run(&self, app: &str, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.rt.block_on(self.inner.run(app, input, timeout))
    }

    /// Run a command once on one machine, and wait for it to end.
    pub fn run_job(&self, job: &JobSpec, interval: Duration, timeout: Option<Duration>) -> Result<Value> {
        self.rt.block_on(async {
            let mut job = self.inner.submit_job(job).await?;
            Ok(job.wait(interval, timeout).await?.clone())
        })
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new()
    }
}

/// A deployed app by name. See [`crate::App`].
pub struct App {
    inner: crate::App,
    rt: Arc<Runtime>,
}

impl App {
    pub fn new(name: impl Into<String>) -> Self {
        App::with_settings(name, Settings::from_env())
    }

    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        App {
            inner: crate::App::with_client(name, crate::Client::with_settings(settings)),
            rt: runtime(),
        }
    }

    pub fn deploy(&self, spec: &crate::AppSpec) -> Result<Value> {
        self.rt.block_on(self.inner.deploy(spec))
    }

    pub fn info(&self) -> Result<Value> {
        self.rt.block_on(self.inner.info())
    }

    pub fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.rt.block_on(self.inner.run(input, timeout))
    }

    pub fn volumes(&self) -> Result<Value> {
        self.rt.block_on(self.inner.volumes())
    }

    pub fn set_volumes(&self, volumes: &[&str]) -> Result<Value> {
        self.rt.block_on(self.inner.set_volumes(volumes))
    }
}

/// A saved workflow by name. See [`crate::Workflow`].
pub struct Workflow {
    inner: crate::Workflow,
    rt: Arc<Runtime>,
}

impl Workflow {
    pub fn new(name: impl Into<String>) -> Self {
        Workflow::with_settings(name, Settings::from_env())
    }

    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        Workflow {
            inner: crate::Workflow::with_client(name, crate::Client::with_settings(settings)),
            rt: runtime(),
        }
    }

    pub fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        self.rt.block_on(self.inner.run(input, timeout))
    }
}
