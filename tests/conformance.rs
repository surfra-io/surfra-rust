//! The shared SDK cases (surfra-sdk-tests/CASES.md), each against its own copy of the fake server.
//!
//! Set SURFRA_SDK_TESTS to the suite's folder; a sibling checkout is found on its own. Skipped when absent.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{Value, json};
use surfra::{App, AppSpec, Call, Client, Error, JobSpec, Mode, Settings, Volume, Workflow};

const PART: u64 = 1024;

struct Fake {
    url: String,
    cache: PathBuf,
    child: Child,
    http: reqwest::Client,
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.cache);
    }
}

fn suite() -> Option<PathBuf> {
    let dir = std::env::var_os("SURFRA_SDK_TESTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../surfra-sdk-tests"));
    dir.join("server.py").is_file().then_some(dir)
}

fn start() -> Option<Fake> {
    let Some(dir) = suite() else {
        eprintln!("surfra-sdk-tests is not checked out; skipping");
        return None;
    };
    let python = std::env::var("SURFRA_SDK_PYTHON").unwrap_or_else(|_| "python3".into());
    let mut child = Command::new(python)
        .arg(dir.join("server.py"))
        .args(["--port", "0"])
        .env("SURFRA_VOLUMES_PART_BYTES", PART.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("python3 runs the fake server");
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
    let url = line.trim().trim_start_matches("listening on ").to_string();
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let cache = std::env::temp_dir().join(format!(
        "surfra-rust-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    Some(Fake {
        url,
        cache,
        child,
        http: reqwest::Client::new(),
    })
}

impl Fake {
    fn settings(&self) -> Settings {
        Settings {
            api_key: None,
            master: self.url.clone(),
            volumes_url: self.url.clone(),
            cache: self.cache.clone(),
            part_bytes: PART,
            token_file: None,
            env_api_key: Some("sfk_test".into()),
            replica_token: None,
            login_file: None,
        }
    }

    async fn volume(&self, name: &str) -> Volume {
        self.http
            .post(format!("{}/v1/volumes", self.url))
            .bearer_auth("sfk_test")
            .json(&json!({ "name": name }))
            .send()
            .await
            .unwrap();
        Volume::with_settings(name, self.settings())
    }

    async fn seed(&self, volume: &str, path: &str, body: &str) {
        self.http
            .post(format!("{}/_test/files", self.url))
            .json(&json!({ "volume": volume, "path": path, "body": body }))
            .send()
            .await
            .unwrap();
    }

    async fn state(&self) -> Value {
        self.http.get(format!("{}/_test/state", self.url)).send().await.unwrap().json().await.unwrap()
    }

    async fn requests(&self) -> Vec<Value> {
        self.state().await["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| !item["path"].as_str().unwrap().starts_with("/_test/"))
            .cloned()
            .collect()
    }
}

fn temp(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("surfra-rust-src-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn v01_small_upload_round_trips() {
    let Some(fake) = start() else { return };
    let weights = fake.volume("weights").await;
    weights.write("a.txt", "hello").await.unwrap();
    assert_eq!(weights.read("a.txt").await.unwrap(), b"hello");
    assert!(!fake.requests().await.iter().any(|item| item["path"].as_str().unwrap().ends_with("/uploads")));
}

#[tokio::test]
async fn v02_large_upload_goes_in_parts() {
    let Some(fake) = start() else { return };
    let dir = temp("v02");
    let data: Vec<u8> = (0..(PART * 3 + 17)).map(|n| (n * 7 % 251) as u8).collect();
    std::fs::write(dir.join("big.bin"), &data).unwrap();
    let weights = fake.volume("weights").await;
    weights.put_file(dir.join("big.bin"), "big.bin").await.unwrap();
    assert_eq!(weights.read("big.bin").await.unwrap(), data);
    assert!(fake.requests().await.iter().any(|item| item["path"].as_str().unwrap().ends_with("/uploads")));
}

#[tokio::test]
async fn v03_put_directory_keeps_relative_paths() {
    let Some(fake) = start() else { return };
    let dir = temp("v03");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("a"), "a").unwrap();
    std::fs::write(dir.join("sub/b"), "b").unwrap();
    assert_eq!(fake.volume("weights").await.put_directory(&dir, "minilm/").await.unwrap(), 2);
    let files = fake.state().await["files"]["weights"].as_object().unwrap().keys().cloned().collect::<Vec<_>>();
    assert_eq!(files, vec!["minilm/a", "minilm/sub/b"]);
}

#[tokio::test]
async fn v04_ls_lists_one_level_or_everything() {
    let Some(fake) = start() else { return };
    let weights = fake.volume("weights").await;
    for path in ["m/a", "m/sub/b", "top"] {
        fake.seed("weights", path, "x").await;
    }
    let top: Vec<String> = weights.ls("m/", false).await.unwrap().into_iter().map(|entry| entry.path).collect();
    assert_eq!(top, vec!["m/a", "m/sub/"]);
    let mut all: Vec<String> = weights.ls("", true).await.unwrap().into_iter().map(|entry| entry.path).collect();
    all.sort();
    assert_eq!(all, vec!["m/a", "m/sub/b", "top"]);
}

#[tokio::test]
async fn v05_local_downloads_once() {
    let Some(fake) = start() else { return };
    let weights = fake.volume("weights").await;
    fake.seed("weights", "m/a", "aaa").await;
    fake.seed("weights", "m/b", "bbb").await;
    let path = weights.path("m/").local().await.unwrap();
    assert_eq!(std::fs::read_to_string(path.join("a")).unwrap(), "aaa");
    assert_eq!(std::fs::read_to_string(path.join("b")).unwrap(), "bbb");
    let before = fake.requests().await.len();
    weights.path("m/").local().await.unwrap();
    assert_eq!(fake.requests().await.len(), before);
}

#[tokio::test]
async fn v06_a_new_object_fetches_only_changes() {
    let Some(fake) = start() else { return };
    fake.volume("weights").await;
    fake.seed("weights", "m/a", "aaa").await;
    fake.seed("weights", "m/b", "bbb").await;
    Volume::with_settings("weights", fake.settings()).path("m/").local().await.unwrap();
    fake.seed("weights", "m/a", "AAAA").await;
    fake.http
        .post(format!("{}/v1/weights/delete", fake.url))
        .bearer_auth("sfk_test")
        .json(&json!({ "paths": ["m/b"] }))
        .send()
        .await
        .unwrap();
    let downloads = fake.state().await["downloads"].as_u64().unwrap();
    let path = Volume::with_settings("weights", fake.settings()).path("m/").local().await.unwrap();
    assert_eq!(fake.state().await["downloads"].as_u64().unwrap(), downloads + 1);
    assert_eq!(std::fs::read_to_string(path.join("a")).unwrap(), "AAAA");
    assert!(!path.join("b").exists());
}

#[tokio::test]
async fn v07_commit_uploads_changes_and_deletes() {
    let Some(fake) = start() else { return };
    let ckpts = fake.volume("ckpts").await;
    for path in ["r/a", "r/b", "r/c"] {
        fake.seed("ckpts", path, path).await;
    }
    let folder = ckpts.path("r/").local_with(Mode::ReadWrite, false).await.unwrap();
    std::fs::write(folder.join("a"), "changed!").unwrap();
    std::fs::write(folder.join("d"), "new").unwrap();
    std::fs::remove_file(folder.join("c")).unwrap();
    let done = ckpts.commit().await.unwrap();
    assert_eq!((done.uploaded, done.deleted), (2, 1));
    let files = fake.state().await["files"]["ckpts"].clone();
    assert_eq!(files.as_object().unwrap().keys().cloned().collect::<Vec<_>>(), vec!["r/a", "r/b", "r/d"]);
    assert_eq!(files["r/a"]["body"], "changed!");
    let again = ckpts.commit().await.unwrap();
    assert_eq!((again.uploaded, again.deleted), (0, 0));
}

#[tokio::test]
async fn v08_open_write_uploads_on_close_only() {
    let Some(fake) = start() else { return };
    let results = fake.volume("results").await;
    let mut done = results.writer("runs/1.jsonl");
    done.write_all(b"one\n");
    done.finish().await.unwrap();
    {
        let mut abandoned = results.writer("runs/2.jsonl");
        abandoned.write_all(b"partial");
        // Dropped without finish: nothing is uploaded.
    }
    let files = fake.state().await["files"]["results"].as_object().unwrap().keys().cloned().collect::<Vec<_>>();
    assert_eq!(files, vec!["runs/1.jsonl"]);
}

#[tokio::test]
async fn v09_rm_file_and_folder() {
    let Some(fake) = start() else { return };
    let weights = fake.volume("weights").await;
    for path in ["a", "old/x", "old/y"] {
        fake.seed("weights", path, "x").await;
    }
    weights.rm("a").await.unwrap();
    assert_eq!(weights.rm_recursive("old/").await.unwrap(), 2);
    assert!(fake.state().await["files"]["weights"].as_object().unwrap().is_empty());
}

#[tokio::test]
async fn v10_missing_file_raises_404() {
    let Some(fake) = start() else { return };
    match fake.volume("weights").await.read("nope").await {
        Err(Error::Api { status: 404, detail }) => assert!(detail.contains("no such file")),
        other => panic!("expected a 404, got {other:?}"),
    }
}

#[tokio::test]
async fn v11_token_file_wins_over_the_api_key() {
    let Some(fake) = start() else { return };
    fake.volume("weights").await;
    let dir = temp("v11");
    std::fs::write(dir.join("token"), "aaa.bbb.ccc\n").unwrap();
    let settings = Settings {
        token_file: Some(dir.join("token")),
        ..fake.settings()
    };
    Volume::with_settings("weights", settings).ls("", false).await.unwrap();
    assert_eq!(fake.requests().await.last().unwrap()["auth"], "Bearer aaa.bbb.ccc");
}

#[tokio::test]
async fn v12_no_credential_says_what_to_set() {
    let Some(fake) = start() else { return };
    let settings = Settings {
        env_api_key: None,
        ..fake.settings()
    };
    match Volume::with_settings("weights", settings).ls("", false).await {
        Err(error @ Error::Credential(_)) => assert!(error.to_string().contains("SURFRA_API_KEY")),
        other => panic!("expected a credential error, got {other:?}"),
    }
}

#[tokio::test]
async fn v13_writing_to_a_new_volume_creates_it() {
    let Some(fake) = start() else { return };
    Volume::with_settings("fresh", fake.settings()).write("a.txt", "x").await.unwrap();
    assert_eq!(fake.state().await["files"]["fresh"]["a.txt"]["body"], "x");
}

#[tokio::test]
async fn v14_another_sdks_state_file_is_honored() {
    let Some(fake) = start() else { return };
    fake.volume("weights").await;
    fake.seed("weights", "m/a", "aaa").await;
    let local = fake.cache.join("vol-weights/m/a");
    std::fs::create_dir_all(local.parent().unwrap()).unwrap();
    std::fs::write(&local, "aaa").unwrap();
    let meta = std::fs::metadata(&local).unwrap();
    let mtime_ns = {
        use std::os::unix::fs::MetadataExt;
        meta.mtime() as u64 * 1_000_000_000 + meta.mtime_nsec() as u64
    };
    let etag = fake.state().await["files"]["weights"]["m/a"]["etag"].clone();
    std::fs::create_dir_all(fake.cache.join(".state")).unwrap();
    std::fs::write(
        fake.cache.join(".state/vol-weights.json"),
        json!({ "m/a": { "etag": etag, "size": 3, "mtime_ns": mtime_ns } }).to_string(),
    )
    .unwrap();
    Volume::with_settings("weights", fake.settings()).path("m/").local().await.unwrap();
    assert_eq!(fake.state().await["downloads"], 0);
}

#[tokio::test]
async fn a01_run_returns_the_output() {
    let Some(fake) = start() else { return };
    let output = Client::with_settings(fake.settings())
        .run("embed", json!({ "texts": ["hi"] }), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(output, json!({ "app": "embed", "echo": { "texts": ["hi"] } }));
}

#[tokio::test]
async fn a06_submit_a_graph_and_wait() {
    let Some(fake) = start() else { return };
    let client = Client::with_settings(fake.settings());
    let view = client
        .submit(
            json!([
                { "name": "a", "app_name": "embed", "depends_on": [], "input": { "n": 1 } },
                { "name": "b", "app_name": "rerank", "depends_on": ["a"], "input": {} }
            ]),
            json!({}),
        )
        .await
        .unwrap();
    let out = Call::new(client, view).result(Some(Duration::from_secs(30))).await.unwrap();
    assert_eq!(out, json!({ "a": { "app": "embed", "echo": { "n": 1 } }, "b": { "app": "rerank", "echo": {} } }));
}

#[tokio::test]
async fn a02_enqueue_then_result() {
    let Some(fake) = start() else { return };
    let client = Client::with_settings(fake.settings());
    let call = client.enqueue("embed", json!({ "n": 1 })).await.unwrap();
    assert_eq!(
        call.result(Some(Duration::from_secs(30))).await.unwrap(),
        json!({ "app": "embed", "echo": { "n": 1 } })
    );
    let failed = client.enqueue("fails", json!({})).await.unwrap().result(Some(Duration::from_secs(30))).await;
    assert!(matches!(failed, Err(Error::Failed(reason)) if reason.contains("ValueError")));
}

#[tokio::test]
async fn j01_job_with_volumes_finishes() {
    let Some(fake) = start() else { return };
    let spec = JobSpec {
        entrypoint: Some("python train.py".into()),
        image: Some("img".into()),
        providers: vec![json!({ "provider": "local" })],
        volumes: vec![Volume::new("weights").grant(Mode::Read)],
        ..Default::default()
    };
    let mut job = Client::with_settings(fake.settings()).submit_job(&spec).await.unwrap();
    assert_eq!(fake.state().await["jobs"][&job.id]["volumes"], json!(["weights:r"]));
    let view = job.wait(Duration::from_millis(50), Some(Duration::from_secs(30))).await.unwrap().clone();
    assert_eq!(view["status"], "succeeded");
    assert_eq!(view["exit_code"], 0);
}

#[tokio::test]
async fn v15_write_and_read_helpers() {
    let Some(fake) = start() else { return };
    let weights = fake.volume("weights").await;
    weights.write("bytes.bin", vec![0u8, 1, b'r', b'a', b'w']).await.unwrap();
    assert_eq!(weights.read("bytes.bin").await.unwrap(), vec![0u8, 1, b'r', b'a', b'w']);
}

#[tokio::test]
async fn c01_control_plane_calls_skip_the_volumes_token() {
    let Some(fake) = start() else { return };
    let dir = temp("c01");
    std::fs::write(dir.join("token"), "aaa.bbb.ccc").unwrap();
    let settings = Settings {
        token_file: Some(dir.join("token")),
        ..fake.settings()
    };
    let weights = Volume::with_settings("weights", settings.clone());
    weights.create().await.unwrap();
    weights.ls("", false).await.unwrap();
    Client::with_settings(settings)
        .run("embed", json!({}), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    let sent: std::collections::HashMap<String, String> = fake
        .requests()
        .await
        .into_iter()
        .map(|item| (item["path"].as_str().unwrap().to_string(), item["auth"].as_str().unwrap_or("").to_string()))
        .collect();
    assert_eq!(sent["/v1/volumes"], "Bearer sfk_test");
    assert_eq!(sent["/v1/transactions"], "Bearer sfk_test");
    assert_eq!(sent["/v1/weights/list"], "Bearer aaa.bbb.ccc");
}

#[tokio::test]
async fn a03_app_deploy_sends_the_spec() {
    let Some(fake) = start() else { return };
    let app = App::with_client("embed", Client::with_settings(fake.settings()));
    let spec = AppSpec {
        image: "ghcr.io/acme/embed:1".into(),
        entrypoint: "app.py".into(),
        autoscale: Some(json!({ "max_replicas": 2 })),
        runtime: Some(json!({ "engine": "python" })),
        ..Default::default()
    };
    let saved = app.deploy(&spec).await.unwrap();
    assert_eq!(saved["name"], "embed");
    assert!(saved["id"].as_str().unwrap().starts_with("sfa_"));
    assert_eq!(saved["image"], "ghcr.io/acme/embed:1");
    assert_eq!(saved["autoscale"], json!({ "max_replicas": 2 }));
    assert!(
        fake.requests()
            .await
            .iter()
            .any(|item| item["method"] == "PUT" && item["path"] == "/v1/apps/embed")
    );
}

#[tokio::test]
async fn a04_app_run_and_volumes() {
    let Some(fake) = start() else { return };
    let app = App::with_client("embed", Client::with_settings(fake.settings()));
    assert_eq!(
        app.run(json!({ "x": 1 }), Some(Duration::from_secs(30))).await.unwrap(),
        json!({ "app": "embed", "echo": { "x": 1 } })
    );
    app.set_volumes(&["weights:r"]).await.unwrap();
    assert_eq!(app.volumes().await.unwrap(), json!([{ "volume": "weights", "mode": "r" }]));
}

#[tokio::test]
async fn a05_a_rate_limited_submit_waits_and_succeeds() {
    let Some(fake) = start() else { return };
    let output = Client::with_settings(fake.settings())
        .run("busy", json!({ "n": 1 }), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(output, json!({ "app": "busy", "echo": { "n": 1 } }));
    assert_eq!(fake.requests().await.iter().filter(|item| item["path"] == "/v1/transactions").count(), 2);
}

#[tokio::test]
async fn w01_workflow_run_returns_each_stage() {
    let Some(fake) = start() else { return };
    let out = Workflow::with_client("pipeline", Client::with_settings(fake.settings()))
        .run(json!({ "q": "hi" }), Some(Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(
        out,
        json!({ "a": { "app": "embed", "echo": { "q": "hi" } }, "b": { "app": "rerank", "echo": {} } })
    );
}

#[tokio::test]
async fn w02_workflow_deploy_saves_stages() {
    let Some(fake) = start() else { return };
    let flow = Workflow::with_client("solo", Client::with_settings(fake.settings()));
    flow.deploy(json!([{ "name": "only", "app": "embed", "depends": [] }])).await.unwrap();
    let out = flow.run(json!({ "q": "hi" }), Some(Duration::from_secs(30))).await.unwrap();
    assert_eq!(out, json!({ "app": "embed", "echo": { "q": "hi" } }));
}

#[tokio::test]
async fn j02_job_logs_oldest_first() {
    let Some(fake) = start() else { return };
    let spec = JobSpec {
        entrypoint: Some("echo hi".into()),
        image: Some("img".into()),
        providers: vec![json!({ "provider": "local" })],
        ..Default::default()
    };
    let job = Client::with_settings(fake.settings()).submit_job(&spec).await.unwrap();
    let lines: Vec<String> = job
        .logs(None)
        .await
        .unwrap()
        .into_iter()
        .map(|line| line["m"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(lines, vec!["line 0", "line 1", "line 2"]);
}
