//! Volumes: named storage your apps, jobs and machines share, kept in R2 and served by volumes.surfra.io.
//!
//! There is no mount. Code names a piece of a volume and asks for it:
//!
//! ```no_run
//! # async fn demo() -> surfra::Result<()> {
//! let weights = surfra::Volume::new("weights");
//! let path = weights.path("minilm/").local().await?; // downloads that folder, returns a real path
//! # Ok(()) }
//! ```
//!
//! `local` downloads only files that changed since the last sync, once per `Volume` unless asked to
//! refresh. A folder synced with [`Mode::ReadWrite`] is uploaded by [`Volume::commit`]. The sync cache
//! layout is shared with every Surfra SDK, so Rust and Python code in one container agree on it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt, stream};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::error::{Error, Result};
use crate::retry::{self, Kind, RetryPolicy};
use crate::settings::Settings;

const DOWNLOADS: usize = 8;
const UPLOADS: usize = 4;
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// What code may do with a volume, or how a folder is synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mode {
    /// List and download.
    Read,
    /// Upload and delete, without reading.
    Write,
    /// Both.
    ReadWrite,
}

impl Mode {
    /// The short form used in grants: `r`, `w` or `rw`.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Read => "r",
            Mode::Write => "w",
            Mode::ReadWrite => "rw",
        }
    }

    fn wider(self, other: Mode) -> Mode {
        if self == other { self } else { Mode::ReadWrite }
    }
}

/// One file in a volume. Folders from a non-recursive listing end in `/` and have size 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub path: String,
    pub size: u64,
    pub etag: String,
    pub modified: String,
}

/// What [`Volume::commit`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Committed {
    pub uploaded: usize,
    pub deleted: usize,
}

#[derive(Deserialize)]
struct Listing {
    files: Vec<ListedFile>,
    #[serde(default)]
    folders: Vec<String>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct ListedFile {
    path: String,
    size: u64,
    etag: String,
    modified: String,
}

#[derive(Deserialize)]
struct Stored {
    path: String,
    size: u64,
    etag: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Known {
    etag: String,
    size: u64,
    #[serde(default)]
    mtime_ns: u64,
}

type State = HashMap<String, Known>;

/// A volume by name. No request is made until it is used. Cloning shares the sync record.
#[derive(Clone)]
pub struct Volume {
    name: String,
    settings: Arc<Settings>,
    http: reqwest::Client,
    synced: Arc<Mutex<HashMap<String, Mode>>>,
    retry: RetryPolicy,
}

/// A file or folder inside a volume. A trailing slash means a folder.
pub struct VolumePath<'a> {
    volume: &'a Volume,
    path: String,
}

fn encode(path: &str) -> String {
    path.split('/')
        .map(|part| utf8_percent_encode(part, SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn is_folder(path: &str) -> bool {
    path.is_empty() || path.ends_with('/')
}

/// The file's modification time in integer nanoseconds, as every Surfra SDK records it.
fn mtime_ns(meta: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        (meta.mtime() as i128 * 1_000_000_000 + meta.mtime_nsec() as i128).max(0) as u64
    }
    #[cfg(not(unix))]
    {
        meta.modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0)
    }
}

impl Volume {
    /// A volume, using credentials and URLs from the environment.
    pub fn new(name: impl Into<String>) -> Self {
        Volume::with_settings(name, Settings::from_env())
    }

    /// A volume with explicit settings.
    pub fn with_settings(name: impl Into<String>, settings: Settings) -> Self {
        Volume {
            name: name.into(),
            settings: Arc::new(settings),
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(30))
                .read_timeout(std::time::Duration::from_secs(300))
                .default_headers(crate::client_headers())
                .build()
                .expect("an HTTP client"),
            synced: Arc::new(Mutex::new(HashMap::new())),
            retry: RetryPolicy::default(),
        }
    }

    /// The same volume (sharing its connections and sync record) with another retry policy.
    /// See [`crate::Client::with_retry`].
    pub fn with_retry(&self, policy: RetryPolicy) -> Volume {
        Volume { retry: policy, ..self.clone() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// A grant such as `weights:r`, for jobs and apps.
    pub fn grant(&self, mode: Mode) -> String {
        format!("{}:{}", self.name, mode.as_str())
    }

    /// A file or folder inside the volume. A trailing slash means a folder.
    pub fn path(&self, path: impl Into<String>) -> VolumePath<'_> {
        VolumePath {
            volume: self,
            path: path.into().trim_start_matches('/').to_string(),
        }
    }

    fn files_url(&self, route: &str) -> String {
        format!("{}/v1/{}/{}", self.settings.volumes_url, utf8_percent_encode(&self.name, SEGMENT), route)
    }

    /// One request to the volumes service, retried as [`RetryPolicy`] says. An error answer comes back as
    /// [`Error::Api`]; a success comes back unread, so downloads stream.
    async fn send(&self, method: Method, url: &str, body: Option<Vec<u8>>, json_body: Option<&Value>, query: &[(&str, &str)]) -> Result<reqwest::Response> {
        let build = || {
            let mut request = self.http.request(method.clone(), url).bearer_auth(self.settings.credential()?).query(query);
            if let Some(data) = &body {
                request = request.header(reqwest::header::CONTENT_LENGTH, data.len()).body(data.clone());
            }
            if let Some(value) = json_body {
                request = request.json(value);
            }
            Ok(request)
        };
        retry::run(&self.retry, method.clone(), url, Kind::Plain, build, |response| async move { Ok(response) }).await
    }

    /// A request to the volumes service. Writes to a volume that does not exist yet create it first.
    async fn request(
        &self,
        method: Method,
        route: &str,
        body: Option<Vec<u8>>,
        json_body: Option<Value>,
        query: &[(&str, &str)],
        create: bool,
    ) -> Result<reqwest::Response> {
        let url = self.files_url(route);
        match self.send(method.clone(), &url, body.clone(), json_body.as_ref(), query).await {
            Err(Error::Api { status: 404, ref detail, .. }) if create && detail.contains("no volume") => {
                self.create().await?;
                self.send(method, &url, body, json_body.as_ref(), query).await
            }
            other => other,
        }
    }

    async fn control(&self, method: Method, route: &str, body: Option<Value>) -> Result<Value> {
        let url = format!("{}{}", self.settings.master, route);
        let build = || {
            let mut request = self.http.request(method.clone(), &url).bearer_auth(self.settings.control_credential()?);
            if let Some(value) = &body {
                request = request.json(value);
            }
            Ok(request)
        };
        retry::buffered(&self.retry, method.clone(), &url, Kind::Plain, build).await?.json()
    }

    /// Create the volume. Writing to a volume that does not exist yet does this for you.
    pub async fn create(&self) -> Result<Value> {
        self.control(Method::POST, "/v1/volumes", Some(json!({ "name": self.name }))).await
    }

    /// The volume's size, file count and what uses it.
    pub async fn info(&self) -> Result<Value> {
        self.control(Method::GET, &format!("/v1/volumes/{}", utf8_percent_encode(&self.name, SEGMENT)), None)
            .await
    }

    /// Delete the whole volume and every file in it. Needs an admin.
    pub async fn delete(&self) -> Result<Value> {
        self.control(Method::DELETE, &format!("/v1/volumes/{}", utf8_percent_encode(&self.name, SEGMENT)), None)
            .await
    }

    /// Every volume in the organization.
    pub async fn list(settings: Settings) -> Result<Vec<Value>> {
        let body = Volume::with_settings("_", settings).control(Method::GET, "/v1/volumes", None).await?;
        Ok(body.get("volumes").and_then(Value::as_array).cloned().unwrap_or_default())
    }

    /// Files under `prefix`. Without `recursive`, folders show as entries ending in `/`.
    pub async fn ls(&self, prefix: &str, recursive: bool) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut query = vec![("prefix", prefix), ("recursive", if recursive { "1" } else { "0" })];
            if let Some(next) = cursor.as_deref() {
                query.push(("cursor", next));
            }
            let page: Listing = self.request(Method::GET, "list", None, None, &query, false).await?.json().await?;
            out.extend(page.files.into_iter().map(|item| Entry {
                path: item.path,
                size: item.size,
                etag: item.etag,
                modified: item.modified,
            }));
            if !recursive {
                out.extend(page.folders.into_iter().map(|folder| Entry {
                    path: folder,
                    size: 0,
                    etag: String::new(),
                    modified: String::new(),
                }));
            }
            match page.cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(out),
            }
        }
    }

    /// Delete one file.
    pub async fn rm(&self, path: &str) -> Result<()> {
        self.request(Method::DELETE, &format!("files/{}", encode(path)), None, None, &[], false).await?;
        Ok(())
    }

    /// Delete everything under a folder. Returns how many files were deleted.
    pub async fn rm_recursive(&self, folder: &str) -> Result<usize> {
        let prefix = if folder.ends_with('/') { folder.to_string() } else { format!("{folder}/") };
        let mut total = 0;
        loop {
            let result: Value = self
                .request(Method::POST, "delete", None, Some(json!({ "prefix": prefix })), &[], false)
                .await?
                .json()
                .await?;
            total += result.get("deleted").and_then(Value::as_u64).unwrap_or(0) as usize;
            if !result.get("more").and_then(Value::as_bool).unwrap_or(false) {
                return Ok(total);
            }
        }
    }

    /// Download one file into memory.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self
            .request(Method::GET, &format!("files/{}", encode(path)), None, None, &[], false)
            .await?
            .bytes()
            .await?
            .to_vec())
    }

    /// Upload bytes as one file. More than the part size goes up in parts.
    pub async fn write(&self, remote: &str, data: impl Into<Vec<u8>>) -> Result<Entry> {
        let data = data.into();
        if data.len() as u64 > self.settings.part_bytes {
            let temp = std::env::temp_dir().join(format!(".surfra-upload-{}-{}", std::process::id(), unique()));
            tokio::fs::write(&temp, &data).await?;
            let result = self.multipart(&temp, remote, data.len() as u64).await;
            let _ = tokio::fs::remove_file(&temp).await;
            return result;
        }
        self.put_small(remote, data).await
    }

    async fn put_small(&self, remote: &str, data: Vec<u8>) -> Result<Entry> {
        let stored: Stored = self
            .request(Method::PUT, &format!("files/{}", encode(remote)), Some(data), None, &[], true)
            .await?
            .json()
            .await?;
        Ok(Entry {
            path: stored.path,
            size: stored.size,
            etag: stored.etag,
            modified: String::new(),
        })
    }

    /// A file in the volume to write in pieces. Nothing is uploaded until [`Upload::finish`]; dropping it uploads nothing.
    pub fn writer(&self, remote: impl Into<String>) -> Upload<'_> {
        Upload {
            volume: self,
            path: remote.into(),
            buffer: Vec::new(),
        }
    }

    /// Upload one local file. Files over the part size go up in parts.
    pub async fn put_file(&self, local: impl AsRef<Path>, remote: &str) -> Result<Entry> {
        let local = local.as_ref();
        let size = tokio::fs::metadata(local).await?.len();
        if size <= self.settings.part_bytes {
            return self.put_small(remote, tokio::fs::read(local).await?).await;
        }
        self.multipart(local, remote, size).await
    }

    async fn multipart(&self, local: &Path, remote: &str, size: u64) -> Result<Entry> {
        let started: Value = self
            .request(Method::POST, "uploads", None, Some(json!({ "path": remote, "size": size })), &[], true)
            .await?
            .json()
            .await?;
        let upload = started.get("upload_id").and_then(Value::as_str).unwrap_or_default().to_string();
        let part_size = started.get("part_size").and_then(Value::as_u64).unwrap_or(self.settings.part_bytes).max(1);
        let route = format!("uploads/{}", utf8_percent_encode(&upload, SEGMENT));
        let count = size.div_ceil(part_size);
        let sent = stream::iter(1..=count)
            .map(|number| {
                let route = route.clone();
                async move {
                    let mut file = tokio::fs::File::open(local).await?;
                    file.seek(std::io::SeekFrom::Start((number - 1) * part_size)).await?;
                    let mut chunk = Vec::with_capacity(part_size as usize);
                    file.take(part_size).read_to_end(&mut chunk).await?;
                    let part: Value = self
                        .request(Method::PUT, &format!("{route}/{number}"), Some(chunk), None, &[("path", remote)], false)
                        .await?
                        .json()
                        .await?;
                    Ok::<_, Error>(json!({ "part": number, "etag": part.get("etag").cloned().unwrap_or(Value::Null) }))
                }
            })
            .buffer_unordered(UPLOADS)
            .try_collect::<Vec<_>>()
            .await;
        let mut parts = match sent {
            Ok(parts) => parts,
            Err(error) => {
                let _ = self.request(Method::DELETE, &route, None, None, &[("path", remote)], false).await;
                return Err(error);
            }
        };
        parts.sort_by_key(|part| part.get("part").and_then(Value::as_u64).unwrap_or(0));
        let stored: Stored = self
            .request(
                Method::POST,
                &format!("{route}/complete"),
                None,
                Some(json!({ "path": remote, "parts": parts })),
                &[],
                false,
            )
            .await?
            .json()
            .await?;
        Ok(Entry {
            path: stored.path,
            size: stored.size,
            etag: stored.etag,
            modified: String::new(),
        })
    }

    /// Upload a folder's files under `remote`. Returns how many were uploaded.
    pub async fn put_directory(&self, local: impl AsRef<Path>, remote: &str) -> Result<usize> {
        let root = local.as_ref();
        let prefix = if remote.is_empty() || remote.ends_with('/') {
            remote.to_string()
        } else {
            format!("{remote}/")
        };
        let mut files = Vec::new();
        walk(root, &mut files)?;
        files.sort();
        let count = files.len();
        stream::iter(files)
            .map(|file| {
                let relative = file.strip_prefix(root).unwrap_or(&file).to_string_lossy().replace('\\', "/");
                let target = format!("{prefix}{relative}");
                async move { self.put_file(&file, &target).await }
            })
            .buffer_unordered(UPLOADS)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(count)
    }

    async fn download_to(&self, remote: &str, target: &Path) -> Result<()> {
        let parent = target.parent().unwrap_or(Path::new("."));
        tokio::fs::create_dir_all(parent).await?;
        let temp = parent.join(format!(".surfra-{}-{}", std::process::id(), unique()));
        let result = async {
            let response = self.request(Method::GET, &format!("files/{}", encode(remote)), None, None, &[], false).await?;
            let mut file = tokio::fs::File::create(&temp).await?;
            let mut body = response.bytes_stream();
            while let Some(chunk) = body.next().await {
                file.write_all(&chunk?).await?;
            }
            file.flush().await?;
            drop(file);
            tokio::fs::rename(&temp, target).await?;
            Ok::<_, Error>(())
        }
        .await;
        if result.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        result
    }

    async fn head(&self, path: &str) -> Result<Vec<Entry>> {
        let response = match self
            .send(Method::HEAD, &self.files_url(&format!("files/{}", encode(path))), None, None, &[])
            .await
        {
            Err(Error::Api { status: 404, .. }) => return Ok(Vec::new()),
            other => other?,
        };
        let header = |name: &str| response.headers().get(name).and_then(|value| value.to_str().ok()).unwrap_or("").to_string();
        Ok(vec![Entry {
            path: path.to_string(),
            size: header("content-length").parse().unwrap_or(0),
            etag: header("etag"),
            modified: header("last-modified"),
        }])
    }

    fn root(&self) -> PathBuf {
        self.settings.cache.join(format!("vol-{}", self.name))
    }

    fn state_path(&self) -> PathBuf {
        self.settings.cache.join(".state").join(format!("vol-{}.json", self.name))
    }

    fn load_state(&self) -> State {
        std::fs::read_to_string(self.state_path())
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }

    fn save_state(&self, state: &State) -> Result<()> {
        let path = self.state_path();
        std::fs::create_dir_all(path.parent().unwrap())?;
        let temp = path.with_extension("tmp");
        std::fs::write(&temp, serde_json::to_vec(state).expect("state is JSON"))?;
        std::fs::rename(temp, path)?;
        Ok(())
    }

    async fn sync(&self, path: &str, mode: Mode, refresh: bool) -> Result<PathBuf> {
        let root = self.root();
        let target = root.join(path.trim_end_matches('/'));
        let mut synced = self.synced.lock().await;
        if let Some(previous) = synced.get(path).copied() {
            if !refresh {
                synced.insert(path.to_string(), previous.wider(mode));
                return Ok(target);
            }
        }
        let folder = is_folder(path);
        if mode == Mode::Write {
            // Write-only: nothing to download, only a place to write.
            tokio::fs::create_dir_all(if folder { &target } else { target.parent().unwrap() }).await?;
            synced.insert(path.to_string(), mode);
            return Ok(target);
        }
        let mut state = self.load_state();
        let remote: HashMap<String, Entry> = if folder { self.ls(path, true).await? } else { self.head(path).await? }
            .into_iter()
            .map(|entry| (entry.path.clone(), entry))
            .collect();
        let changed: Vec<Entry> = remote
            .values()
            .filter(|entry| stale(&root.join(&entry.path), state.get(&entry.path), entry))
            .cloned()
            .collect();
        // Owned paths, so the future stays `Send` (a borrowed item trips rustc's higher-ranked inference).
        stream::iter(changed.iter().map(|entry| entry.path.clone()).collect::<Vec<_>>())
            .map(|remote| {
                let target = root.join(&remote);
                async move { self.download_to(&remote, &target).await }
            })
            .buffer_unordered(DOWNLOADS)
            .try_collect::<Vec<_>>()
            .await?;
        for entry in &changed {
            let meta = std::fs::metadata(root.join(&entry.path))?;
            state.insert(
                entry.path.clone(),
                Known {
                    etag: entry.etag.clone(),
                    size: entry.size,
                    mtime_ns: mtime_ns(&meta),
                },
            );
        }
        let known: Vec<String> = state.keys().cloned().collect();
        for key in known {
            let inside = if folder { key.starts_with(path) } else { key == path };
            if inside && !remote.contains_key(&key) {
                let _ = std::fs::remove_file(root.join(&key));
                state.remove(&key);
            }
        }
        self.save_state(&state)?;
        if folder {
            tokio::fs::create_dir_all(&target).await?;
        }
        synced.insert(path.to_string(), mode);
        Ok(target)
    }

    /// Upload what changed in every folder this volume synced with [`Mode::Write`] or [`Mode::ReadWrite`].
    pub async fn commit(&self) -> Result<Committed> {
        let synced = self.synced.lock().await.clone();
        let mut state = self.load_state();
        let root = self.root();
        let mut done = Committed::default();
        for (path, mode) in synced {
            if mode == Mode::Read {
                continue;
            }
            let folder = is_folder(&path);
            let base = root.join(&path);
            let mut found = Vec::new();
            if folder {
                if base.is_dir() {
                    walk(&base, &mut found)?;
                }
            } else if base.is_file() {
                found.push(base.clone());
            }
            let mut seen = std::collections::HashSet::new();
            for file in found {
                if file.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.starts_with(".surfra-")) {
                    continue;
                }
                let remote = file.strip_prefix(&root).unwrap_or(&file).to_string_lossy().replace('\\', "/");
                seen.insert(remote.clone());
                let meta = std::fs::metadata(&file)?;
                if let Some(known) = state.get(&remote) {
                    if known.size == meta.len() && known.mtime_ns == mtime_ns(&meta) {
                        continue;
                    }
                }
                let entry = self.put_file(&file, &remote).await?;
                state.insert(
                    remote,
                    Known {
                        etag: entry.etag,
                        size: entry.size,
                        mtime_ns: mtime_ns(&meta),
                    },
                );
                done.uploaded += 1;
            }
            if mode == Mode::ReadWrite {
                let gone: Vec<String> = state
                    .keys()
                    .filter(|key| if folder { key.starts_with(&path) } else { **key == path })
                    .filter(|key| !seen.contains(*key))
                    .cloned()
                    .collect();
                for key in gone {
                    match self.rm(&key).await {
                        Ok(()) | Err(Error::Api { status: 404, .. }) => {}
                        Err(error) => return Err(error),
                    }
                    state.remove(&key);
                    done.deleted += 1;
                }
            }
        }
        self.save_state(&state)?;
        Ok(done)
    }
}

impl VolumePath<'_> {
    /// Download this piece into the local cache, read-only, and return its real path.
    pub async fn local(&self) -> Result<PathBuf> {
        self.volume.sync(&self.path, Mode::Read, false).await
    }

    /// Sync with a mode, and with `refresh` look again even if this volume already synced it.
    pub async fn local_with(&self, mode: Mode, refresh: bool) -> Result<PathBuf> {
        self.volume.sync(&self.path, mode, refresh).await
    }

    /// A plain copy into `dest`, outside the sync cache.
    pub async fn download(&self, dest: impl AsRef<Path>) -> Result<PathBuf> {
        let dest = dest.as_ref().to_path_buf();
        if is_folder(&self.path) {
            for entry in self.volume.ls(&self.path, true).await? {
                self.volume.download_to(&entry.path, &dest.join(&entry.path[self.path.len()..])).await?;
            }
        } else {
            let target = if dest.is_dir() {
                dest.join(Path::new(&self.path).file_name().unwrap())
            } else {
                dest.clone()
            };
            self.volume.download_to(&self.path, &target).await?;
        }
        Ok(dest)
    }
}

/// A file being written. [`Upload::finish`] uploads it; dropping it without finishing uploads nothing.
pub struct Upload<'a> {
    volume: &'a Volume,
    path: String,
    buffer: Vec<u8>,
}

impl Upload<'_> {
    pub fn write_all(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub async fn finish(self) -> Result<Entry> {
        self.volume.write(&self.path, self.buffer).await
    }
}

impl std::io::Write for Upload<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn stale(local: &Path, known: Option<&Known>, entry: &Entry) -> bool {
    let Some(known) = known else { return true };
    if known.etag != entry.etag {
        return true;
    }
    match std::fs::metadata(local) {
        Ok(meta) if meta.is_file() => meta.len() != entry.size || mtime_ns(&meta) != known.mtime_ns,
        _ => true,
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            walk(&path, out)?;
        } else if path.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ NEXT.fetch_add(1, Ordering::Relaxed)
}
