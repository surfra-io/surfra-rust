//! The same API without async: the same type and method names as the crate root, each call waiting for
//! its result. Enable the `blocking` feature.
//!
//! ```no_run
//! # fn demo() -> surfra::Result<()> {
//! use surfra::blocking::{Client, Volume};
//!
//! let path = Volume::new("weights").path("minilm/").local()?;
//! let output = Client::new().run("embed", serde_json::json!({ "texts": ["hello"] }), None)?;
//! # Ok(()) }
//! ```
//!
//! Every type here wraps its async counterpart, so behavior is the same in both modes. All of them
//! share one multi-thread Tokio runtime, started on first use. Calling from inside a Tokio runtime
//! works too: the call runs on a helper thread and the calling thread waits for it. That still blocks
//! the calling thread, so async code should use the async API.
//!
//! The fields and plain methods of the async types (`job.id`, `call.view`, `job.status()`, `app.name`)
//! are reachable through `Deref`.

use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::time::Duration;

use serde_json::Value;
use tokio::runtime::Runtime;

/// Callback signature verification; it does no I/O, so it is the same module as [`crate::webhooks`].
pub use crate::webhooks;
pub use crate::{AppSpec, Committed, Entry, Error, JobSpec, Mode, Result, Settings};

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("surfra-blocking")
        .enable_all()
        .build()
        .expect("a Tokio runtime for surfra::blocking")
});

/// Run a future on the shared runtime and wait for it on this thread.
///
/// Outside a runtime the calling thread drives the future with `block_on`. Inside one, `block_on` would
/// panic, so a scoped helper thread drives it instead, and this thread waits for that thread to finish.
fn wait<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_err() {
        return RUNTIME.block_on(future);
    }
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("surfra-blocking-call".into())
            .spawn_scoped(scope, || RUNTIME.block_on(future))
            .expect("a thread for a surfra::blocking call")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

/// The control plane. See [`crate::Client`]. Clones share one connection pool.
#[derive(Clone)]
pub struct Client {
    inner: crate::Client,
}

impl Client {
    /// A client using credentials and URLs from the environment.
    pub fn new() -> Self {
        Client::with_settings(Settings::from_env())
    }

    pub fn with_settings(settings: Settings) -> Self {
        Client {
            inner: crate::Client::with_settings(settings),
        }
    }

    /// The async client this wraps.
    pub fn as_async(&self) -> &crate::Client {
        &self.inner
    }

    /// Start a transaction. See [`crate::Client::submit`].
    pub fn submit(&self, stages: Value, metadata: Value) -> Result<Value> {
        wait(self.inner.submit(stages, metadata))
    }

    /// Start a transaction with a `callback_url`. Surfra POSTs the task's latest state there each time it changes.
    pub fn submit_with_callback(&self, stages: Value, metadata: Value, callback_url: &str) -> Result<Value> {
        wait(self.inner.submit_with_callback(stages, metadata, callback_url))
    }

    /// Queue one call to an app and return it before the app finishes.
    pub fn enqueue(&self, app: &str, input: Value) -> Result<Call> {
        wait(self.inner.enqueue(app, input)).map(Call::from)
    }

    /// [`Client::enqueue`] with a `callback_url`. Surfra POSTs the task's latest state there each time it changes.
    pub fn enqueue_with_callback(&self, app: &str, input: Value, callback_url: &str) -> Result<Call> {
        wait(self.inner.enqueue_with_callback(app, input, callback_url)).map(Call::from)
    }

    /// Call an app and wait for its output.
    pub fn run(&self, app: &str, input: Value, timeout: Option<Duration>) -> Result<Value> {
        wait(self.inner.run(app, input, timeout))
    }

    /// Run a command once on one machine.
    pub fn submit_job(&self, job: &JobSpec) -> Result<Job> {
        wait(self.inner.submit_job(job)).map(Job::from)
    }

    /// Run a command once on one machine, and wait for it to end. Returns the job's final view.
    pub fn run_job(&self, job: &JobSpec, interval: Duration, timeout: Option<Duration>) -> Result<Value> {
        wait(self.inner.run_job(job, interval, timeout))
    }

    /// A job by id.
    pub fn job(&self, id: &str) -> Result<Job> {
        wait(self.inner.job(id)).map(Job::from)
    }

    /// A deployed app by name, sharing this client.
    pub fn app(&self, name: impl Into<String>) -> App {
        App::with_client(name, self.clone())
    }

    /// A saved workflow by name, sharing this client.
    pub fn workflow(&self, name: impl Into<String>) -> Workflow {
        Workflow::with_client(name, self.clone())
    }
}

impl Default for Client {
    fn default() -> Self {
        Client::new()
    }
}

impl From<crate::Client> for Client {
    fn from(inner: crate::Client) -> Self {
        Client { inner }
    }
}

/// A deployed app by name. See [`crate::App`].
#[derive(Clone)]
pub struct App {
    inner: crate::App,
}

impl App {
    pub fn new(name: impl Into<String>) -> Self {
        App::with_client(name, Client::new())
    }

    pub fn with_client(name: impl Into<String>, client: Client) -> Self {
        App {
            inner: crate::App::with_client(name, client.inner),
        }
    }

    /// The app with its own client from `settings`.
    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        App::with_client(name, Client::with_settings(settings))
    }

    /// Create the app, or update the app with this name.
    pub fn deploy(&self, spec: &AppSpec) -> Result<Value> {
        wait(self.inner.deploy(spec))
    }

    /// The saved app: image, entrypoint, runtime and autoscale.
    pub fn info(&self) -> Result<Value> {
        wait(self.inner.info())
    }

    /// Queue one request and return it before the app finishes.
    pub fn enqueue(&self, input: Value) -> Result<Call> {
        wait(self.inner.enqueue(input)).map(Call::from)
    }

    /// Call the app and wait for its output.
    pub fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        wait(self.inner.run(input, timeout))
    }

    /// The volumes this app's replicas may reach.
    pub fn volumes(&self) -> Result<Value> {
        wait(self.inner.volumes())
    }

    /// Replace the app's volumes, such as `["weights:r", "outputs:rw"]`.
    pub fn set_volumes(&self, volumes: &[&str]) -> Result<Value> {
        wait(self.inner.set_volumes(volumes))
    }
}

impl Deref for App {
    type Target = crate::App;

    fn deref(&self) -> &crate::App {
        &self.inner
    }
}

/// A saved workflow by name. See [`crate::Workflow`].
#[derive(Clone)]
pub struct Workflow {
    inner: crate::Workflow,
}

impl Workflow {
    pub fn new(name: impl Into<String>) -> Self {
        Workflow::with_client(name, Client::new())
    }

    pub fn with_client(name: impl Into<String>, client: Client) -> Self {
        Workflow {
            inner: crate::Workflow::with_client(name, client.inner),
        }
    }

    /// The workflow with its own client from `settings`.
    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        Workflow::with_client(name, Client::with_settings(settings))
    }

    /// Save the stages. See [`crate::Workflow::deploy`].
    pub fn deploy(&self, stages: Value) -> Result<Value> {
        wait(self.inner.deploy(stages))
    }

    /// Start one run. Stages with no dependencies receive `input`.
    pub fn enqueue(&self, input: Value) -> Result<Call> {
        wait(self.inner.enqueue(input)).map(Call::from)
    }

    /// Start one run and wait. Returns `{stage name: output}`, or the output for a single stage.
    pub fn run(&self, input: Value, timeout: Option<Duration>) -> Result<Value> {
        wait(self.inner.run(input, timeout))
    }
}

impl Deref for Workflow {
    type Target = crate::Workflow;

    fn deref(&self) -> &crate::Workflow {
        &self.inner
    }
}

/// One call to an app. See [`crate::Call`]; `id` and `view` are reachable through `Deref`.
pub struct Call {
    inner: crate::Call,
}

impl Call {
    /// Track a call from the view [`Client::submit`] returned.
    pub fn new(client: Client, view: Value) -> Self {
        Call {
            inner: crate::Call::new(client.inner, view),
        }
    }

    /// Refresh and return the call's current view.
    pub fn get(&mut self) -> Result<&Value> {
        wait(self.inner.get())
    }

    /// Wait for the call to finish and return its output. A failed call returns each stage's error.
    pub fn result(self, timeout: Option<Duration>) -> Result<Value> {
        wait(self.inner.result(timeout))
    }
}

impl From<crate::Call> for Call {
    fn from(inner: crate::Call) -> Self {
        Call { inner }
    }
}

impl Deref for Call {
    type Target = crate::Call;

    fn deref(&self) -> &crate::Call {
        &self.inner
    }
}

/// One job. See [`crate::Job`]; `id`, `view` and `status()` are reachable through `Deref`.
pub struct Job {
    inner: crate::Job,
}

impl Job {
    /// Refresh and return the job's current view.
    pub fn get(&mut self) -> Result<&Value> {
        wait(self.inner.get())
    }

    /// Stop the job.
    pub fn stop(&mut self) -> Result<&Value> {
        wait(self.inner.stop())
    }

    /// The job's log lines after `after`, oldest first.
    pub fn logs(&self, after: Option<&str>) -> Result<Vec<Value>> {
        wait(self.inner.logs(after))
    }

    /// Wait until the job ends: succeeded, failed, stopped or lost.
    pub fn wait(&mut self, interval: Duration, timeout: Option<Duration>) -> Result<&Value> {
        wait(self.inner.wait(interval, timeout))
    }
}

impl From<crate::Job> for Job {
    fn from(inner: crate::Job) -> Self {
        Job { inner }
    }
}

impl Deref for Job {
    type Target = crate::Job;

    fn deref(&self) -> &crate::Job {
        &self.inner
    }
}

/// A volume by name. See [`crate::Volume`]. Cloning shares the sync record.
#[derive(Clone)]
pub struct Volume {
    inner: crate::Volume,
}

impl Volume {
    /// A volume, using credentials and URLs from the environment.
    pub fn new(name: impl Into<String>) -> Self {
        Volume::with_settings(name, Settings::from_env())
    }

    /// A volume with explicit settings.
    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        Volume {
            inner: crate::Volume::with_settings(name, settings),
        }
    }

    /// The async volume this wraps. It shares this volume's sync record.
    pub fn as_async(&self) -> &crate::Volume {
        &self.inner
    }

    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// A grant such as `weights:r`, for jobs and apps.
    pub fn grant(&self, mode: Mode) -> String {
        self.inner.grant(mode)
    }

    /// A file or folder inside the volume. A trailing slash means a folder.
    pub fn path(&self, path: impl Into<String>) -> VolumePath<'_> {
        VolumePath { inner: self.inner.path(path) }
    }

    /// Create the volume. Writing to a volume that does not exist yet does this for you.
    pub fn create(&self) -> Result<Value> {
        wait(self.inner.create())
    }

    /// The volume's size, file count and what uses it.
    pub fn info(&self) -> Result<Value> {
        wait(self.inner.info())
    }

    /// Delete the whole volume and every file in it. Needs an admin.
    pub fn delete(&self) -> Result<Value> {
        wait(self.inner.delete())
    }

    /// Every volume in the organization.
    pub fn list(settings: Settings) -> Result<Vec<Value>> {
        wait(crate::Volume::list(settings))
    }

    /// Files under `prefix`. Without `recursive`, folders show as entries ending in `/`.
    pub fn ls(&self, prefix: &str, recursive: bool) -> Result<Vec<Entry>> {
        wait(self.inner.ls(prefix, recursive))
    }

    /// Delete one file.
    pub fn rm(&self, path: &str) -> Result<()> {
        wait(self.inner.rm(path))
    }

    /// Delete everything under a folder. Returns how many files were deleted.
    pub fn rm_recursive(&self, folder: &str) -> Result<usize> {
        wait(self.inner.rm_recursive(folder))
    }

    /// Download one file into memory.
    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        wait(self.inner.read(path))
    }

    /// Upload bytes as one file. More than the part size goes up in parts.
    pub fn write(&self, remote: &str, data: impl Into<Vec<u8>>) -> Result<Entry> {
        let data: Vec<u8> = data.into();
        wait(self.inner.write(remote, data))
    }

    /// A file in the volume to write in pieces. Nothing is uploaded until [`Upload::finish`].
    pub fn writer(&self, remote: impl Into<String>) -> Upload<'_> {
        Upload {
            inner: self.inner.writer(remote),
        }
    }

    /// Upload one local file. Files over the part size go up in parts.
    pub fn put_file(&self, local: impl AsRef<Path>, remote: &str) -> Result<Entry> {
        wait(self.inner.put_file(local.as_ref(), remote))
    }

    /// Upload a folder's files under `remote`. Returns how many were uploaded.
    pub fn put_directory(&self, local: impl AsRef<Path>, remote: &str) -> Result<usize> {
        wait(self.inner.put_directory(local.as_ref(), remote))
    }

    /// Upload what changed in every folder this volume synced with [`Mode::Write`] or [`Mode::ReadWrite`].
    pub fn commit(&self) -> Result<Committed> {
        wait(self.inner.commit())
    }
}

/// A file or folder inside a volume. See [`crate::VolumePath`].
pub struct VolumePath<'a> {
    inner: crate::VolumePath<'a>,
}

impl VolumePath<'_> {
    /// Download this piece into the local cache, read-only, and return its real path.
    pub fn local(&self) -> Result<PathBuf> {
        wait(self.inner.local())
    }

    /// Sync with a mode, and with `refresh` look again even if this volume already synced it.
    pub fn local_with(&self, mode: Mode, refresh: bool) -> Result<PathBuf> {
        wait(self.inner.local_with(mode, refresh))
    }

    /// A plain copy into `dest`, outside the sync cache.
    pub fn download(&self, dest: impl AsRef<Path>) -> Result<PathBuf> {
        wait(self.inner.download(dest.as_ref()))
    }
}

/// A file being written. [`Upload::finish`] uploads it; dropping it without finishing uploads nothing.
pub struct Upload<'a> {
    inner: crate::Upload<'a>,
}

impl Upload<'_> {
    pub fn write_all(&mut self, data: &[u8]) {
        self.inner.write_all(data);
    }

    pub fn finish(self) -> Result<Entry> {
        wait(self.inner.finish())
    }
}

impl std::io::Write for Upload<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(&mut self.inner, data)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
