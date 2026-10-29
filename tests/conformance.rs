//! The shared SDK cases (surfra-sdk-tests/CASES.md), each against its own copy of the fake server.
//!
//! Set SURFRA_SDK_TESTS to the suite's folder; a sibling checkout is found on its own. Skipped when absent.
//!
//! Every case is written once, in `cases!`, and expanded per mode, so the async and blocking APIs run
//! the same cases by construction:
//!
//! - `async_cases`: `surfra::*` under `#[tokio::test]`;
//! - `blocking_cases`: `surfra::blocking::*` under `#[test]` (feature `blocking`);
//! - `blocking_in_current_thread` / `blocking_in_multi_thread`: the blocking API called from inside a
//!   Tokio runtime of each flavor, which must not panic (feature `blocking`).
//!
//! Inside a case, `call!(expr)` is an SDK call: `expr.await` in async mode, `expr` in blocking mode.
//! The fake server's own helpers are synchronous in every mode.

use std::future::Future;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use serde_json::{Value, json};

const PART: u64 = 1024;

struct Fake {
    url: String,
    cache: PathBuf,
    child: Child,
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
    Some(Fake { url, cache, child })
}

/// Run a test-side request to completion from any thread, inside a runtime or not.
fn side<T: Send>(future: impl Future<Output = T> + Send) -> T {
    std::thread::scope(|scope| {
        scope
            .spawn(|| tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(future))
            .join()
            .unwrap()
    })
}

impl Fake {
    fn settings(&self) -> surfra::Settings {
        surfra::Settings {
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

    fn post(&self, route: &str, auth: Option<&str>, body: Value) {
        side(async {
            let mut request = reqwest::Client::new().post(format!("{}{route}", self.url)).json(&body);
            if let Some(token) = auth {
                request = request.bearer_auth(token);
            }
            request.send().await.unwrap();
        })
    }

    /// Create a volume on the fake, as the server, and return nothing; cases build their own handle.
    fn make_volume(&self, name: &str) {
        self.post("/v1/volumes", Some("sfk_test"), json!({ "name": name }));
    }

    fn seed(&self, volume: &str, path: &str, body: &str) {
        self.post("/_test/files", None, json!({ "volume": volume, "path": path, "body": body }));
    }

    fn state(&self) -> Value {
        side(async {
            reqwest::Client::new()
                .get(format!("{}/_test/state", self.url))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap()
        })
    }

    /// Answer the next matching requests with a fault (see server.py, POST /_test/faults).
    fn faults(&self, items: Value) {
        self.post("/_test/faults", None, json!({ "faults": items }));
    }

    fn count(&self, method: &str, prefix: &str) -> usize {
        self.requests()
            .iter()
            .filter(|item| item["method"] == method && item["path"].as_str().unwrap().starts_with(prefix))
            .count()
    }

    fn requests(&self) -> Vec<Value> {
        self.state()["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| !item["path"].as_str().unwrap().starts_with("/_test/"))
            .cloned()
            .collect()
    }
}

/// A typed refusal like the control plane's: {"detail", "error": {"type", "message", "retry_after"}} and Retry-After.
fn refusal(method: &str, path: &str, status: u16, kind: &str, retry_after: Option<u64>) -> Value {
    let mut fault = json!({
        "method": method, "path": path, "status": status,
        "body": { "detail": kind.to_lowercase(), "error": { "type": kind, "message": kind.to_lowercase() } },
    });
    if let Some(seconds) = retry_after {
        fault["body"]["error"]["retry_after"] = json!(seconds);
        fault["headers"] = json!({ "Retry-After": seconds.to_string() });
    }
    fault
}

fn temp(name: &str) -> PathBuf {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "surfra-rust-src-{}-{name}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Every case, once. `$case` declares one test in the mode's style; `$call` marks an SDK call.
macro_rules! cases {
    ($case:ident, $call:ident) => {
        $case!(v01_small_upload_round_trips, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            $call!(weights.write("a.txt", "hello")).unwrap();
            assert_eq!($call!(weights.read("a.txt")).unwrap(), b"hello");
            assert!(!fake.requests().iter().any(|item| item["path"].as_str().unwrap().ends_with("/uploads")));
        });

        $case!(v02_large_upload_goes_in_parts, {
            let Some(fake) = start() else { return };
            let dir = temp("v02");
            let data: Vec<u8> = (0..(PART * 3 + 17)).map(|n| (n * 7 % 251) as u8).collect();
            std::fs::write(dir.join("big.bin"), &data).unwrap();
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            $call!(weights.put_file(dir.join("big.bin"), "big.bin")).unwrap();
            assert_eq!($call!(weights.read("big.bin")).unwrap(), data);
            assert!(fake.requests().iter().any(|item| item["path"].as_str().unwrap().ends_with("/uploads")));
        });

        $case!(v03_put_directory_keeps_relative_paths, {
            let Some(fake) = start() else { return };
            let dir = temp("v03");
            std::fs::create_dir_all(dir.join("sub")).unwrap();
            std::fs::write(dir.join("a"), "a").unwrap();
            std::fs::write(dir.join("sub/b"), "b").unwrap();
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            assert_eq!($call!(weights.put_directory(&dir, "minilm/")).unwrap(), 2);
            let files = fake.state()["files"]["weights"].as_object().unwrap().keys().cloned().collect::<Vec<_>>();
            assert_eq!(files, vec!["minilm/a", "minilm/sub/b"]);
        });

        $case!(v04_ls_lists_one_level_or_everything, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            for path in ["m/a", "m/sub/b", "top"] {
                fake.seed("weights", path, "x");
            }
            let top: Vec<String> = $call!(weights.ls("m/", false)).unwrap().into_iter().map(|entry| entry.path).collect();
            assert_eq!(top, vec!["m/a", "m/sub/"]);
            let mut all: Vec<String> = $call!(weights.ls("", true)).unwrap().into_iter().map(|entry| entry.path).collect();
            all.sort();
            assert_eq!(all, vec!["m/a", "m/sub/b", "top"]);
        });

        $case!(v05_local_downloads_once, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            fake.seed("weights", "m/a", "aaa");
            fake.seed("weights", "m/b", "bbb");
            let path = $call!(weights.path("m/").local()).unwrap();
            assert_eq!(std::fs::read_to_string(path.join("a")).unwrap(), "aaa");
            assert_eq!(std::fs::read_to_string(path.join("b")).unwrap(), "bbb");
            let before = fake.requests().len();
            $call!(weights.path("m/").local()).unwrap();
            assert_eq!(fake.requests().len(), before);
        });

        $case!(v06_a_new_object_fetches_only_changes, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            fake.seed("weights", "m/a", "aaa");
            fake.seed("weights", "m/b", "bbb");
            $call!(Volume::with_settings("weights", fake.settings()).path("m/").local()).unwrap();
            fake.seed("weights", "m/a", "AAAA");
            fake.post("/v1/weights/delete", Some("sfk_test"), json!({ "paths": ["m/b"] }));
            let downloads = fake.state()["downloads"].as_u64().unwrap();
            let path = $call!(Volume::with_settings("weights", fake.settings()).path("m/").local()).unwrap();
            assert_eq!(fake.state()["downloads"].as_u64().unwrap(), downloads + 1);
            assert_eq!(std::fs::read_to_string(path.join("a")).unwrap(), "AAAA");
            assert!(!path.join("b").exists());
        });

        $case!(v07_commit_uploads_changes_and_deletes, {
            let Some(fake) = start() else { return };
            fake.make_volume("ckpts");
            let ckpts = Volume::with_settings("ckpts", fake.settings());
            for path in ["r/a", "r/b", "r/c"] {
                fake.seed("ckpts", path, path);
            }
            let folder = $call!(ckpts.path("r/").local_with(Mode::ReadWrite, false)).unwrap();
            std::fs::write(folder.join("a"), "changed!").unwrap();
            std::fs::write(folder.join("d"), "new").unwrap();
            std::fs::remove_file(folder.join("c")).unwrap();
            let done = $call!(ckpts.commit()).unwrap();
            assert_eq!((done.uploaded, done.deleted), (2, 1));
            let files = fake.state()["files"]["ckpts"].clone();
            assert_eq!(files.as_object().unwrap().keys().cloned().collect::<Vec<_>>(), vec!["r/a", "r/b", "r/d"]);
            assert_eq!(files["r/a"]["body"], "changed!");
            let again = $call!(ckpts.commit()).unwrap();
            assert_eq!((again.uploaded, again.deleted), (0, 0));
        });

        $case!(v08_open_write_uploads_on_close_only, {
            let Some(fake) = start() else { return };
            fake.make_volume("results");
            let results = Volume::with_settings("results", fake.settings());
            let mut done = results.writer("runs/1.jsonl");
            done.write_all(b"one\n");
            $call!(done.finish()).unwrap();
            {
                let mut abandoned = results.writer("runs/2.jsonl");
                abandoned.write_all(b"partial");
                // Dropped without finish: nothing is uploaded.
            }
            let files = fake.state()["files"]["results"].as_object().unwrap().keys().cloned().collect::<Vec<_>>();
            assert_eq!(files, vec!["runs/1.jsonl"]);
        });

        $case!(v09_rm_file_and_folder, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            for path in ["a", "old/x", "old/y"] {
                fake.seed("weights", path, "x");
            }
            $call!(weights.rm("a")).unwrap();
            assert_eq!($call!(weights.rm_recursive("old/")).unwrap(), 2);
            assert!(fake.state()["files"]["weights"].as_object().unwrap().is_empty());
        });

        $case!(v10_missing_file_raises_404, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            match $call!(Volume::with_settings("weights", fake.settings()).read("nope")) {
                Err(Error::Api { status: 404, detail, .. }) => assert!(detail.contains("no such file")),
                other => panic!("expected a 404, got {other:?}"),
            }
        });

        $case!(v11_token_file_wins_over_the_api_key, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let dir = temp("v11");
            std::fs::write(dir.join("token"), "aaa.bbb.ccc\n").unwrap();
            let settings = Settings {
                token_file: Some(dir.join("token")),
                ..fake.settings()
            };
            $call!(Volume::with_settings("weights", settings).ls("", false)).unwrap();
            assert_eq!(fake.requests().last().unwrap()["auth"], "Bearer aaa.bbb.ccc");
        });

        $case!(v12_no_credential_says_what_to_set, {
            let Some(fake) = start() else { return };
            let settings = Settings {
                env_api_key: None,
                ..fake.settings()
            };
            match $call!(Volume::with_settings("weights", settings).ls("", false)) {
                Err(error @ Error::Credential(_)) => assert!(error.to_string().contains("SURFRA_API_KEY")),
                other => panic!("expected a credential error, got {other:?}"),
            }
        });

        $case!(v13_writing_to_a_new_volume_creates_it, {
            let Some(fake) = start() else { return };
            $call!(Volume::with_settings("fresh", fake.settings()).write("a.txt", "x")).unwrap();
            assert_eq!(fake.state()["files"]["fresh"]["a.txt"]["body"], "x");
        });

        $case!(v14_another_sdks_state_file_is_honored, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            fake.seed("weights", "m/a", "aaa");
            let local = fake.cache.join("vol-weights/m/a");
            std::fs::create_dir_all(local.parent().unwrap()).unwrap();
            std::fs::write(&local, "aaa").unwrap();
            let meta = std::fs::metadata(&local).unwrap();
            let mtime_ns = {
                use std::os::unix::fs::MetadataExt;
                meta.mtime() as u64 * 1_000_000_000 + meta.mtime_nsec() as u64
            };
            let etag = fake.state()["files"]["weights"]["m/a"]["etag"].clone();
            std::fs::create_dir_all(fake.cache.join(".state")).unwrap();
            std::fs::write(
                fake.cache.join(".state/vol-weights.json"),
                json!({ "m/a": { "etag": etag, "size": 3, "mtime_ns": mtime_ns } }).to_string(),
            )
            .unwrap();
            $call!(Volume::with_settings("weights", fake.settings()).path("m/").local()).unwrap();
            assert_eq!(fake.state()["downloads"], 0);
        });

        $case!(v15_write_and_read_helpers, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            let weights = Volume::with_settings("weights", fake.settings());
            $call!(weights.write("bytes.bin", vec![0u8, 1, b'r', b'a', b'w'])).unwrap();
            assert_eq!($call!(weights.read("bytes.bin")).unwrap(), vec![0u8, 1, b'r', b'a', b'w']);
        });

        $case!(c01_control_plane_calls_skip_the_volumes_token, {
            let Some(fake) = start() else { return };
            let dir = temp("c01");
            std::fs::write(dir.join("token"), "aaa.bbb.ccc").unwrap();
            let settings = Settings {
                token_file: Some(dir.join("token")),
                ..fake.settings()
            };
            let weights = Volume::with_settings("weights", settings.clone());
            $call!(weights.create()).unwrap();
            $call!(weights.ls("", false)).unwrap();
            $call!(Client::with_settings(settings).run("embed", json!({}), Some(Duration::from_secs(30)))).unwrap();
            let sent: std::collections::HashMap<String, String> = fake
                .requests()
                .into_iter()
                .map(|item| (item["path"].as_str().unwrap().to_string(), item["auth"].as_str().unwrap_or("").to_string()))
                .collect();
            assert_eq!(sent["/v1/volumes"], "Bearer sfk_test");
            assert_eq!(sent["/v1/transactions"], "Bearer sfk_test");
            assert_eq!(sent["/v1/weights/list"], "Bearer aaa.bbb.ccc");
        });

        $case!(a01_run_returns_the_output, {
            let Some(fake) = start() else { return };
            let output = $call!(Client::with_settings(fake.settings()).run("embed", json!({ "texts": ["hi"] }), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(output, json!({ "app": "embed", "echo": { "texts": ["hi"] } }));
        });

        $case!(a02_enqueue_then_result, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let call = $call!(client.enqueue("embed", json!({ "n": 1 }))).unwrap();
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": { "n": 1 } }));
            let failed = $call!($call!(client.enqueue("fails", json!({}))).unwrap().result(Some(Duration::from_secs(30))));
            assert!(matches!(failed, Err(Error::Failed(reason)) if reason.contains("ValueError")));
        });

        $case!(a03_app_deploy_sends_the_spec, {
            let Some(fake) = start() else { return };
            let app = App::with_client("embed", Client::with_settings(fake.settings()));
            let spec = AppSpec {
                image: "ghcr.io/acme/embed:1".into(),
                entrypoint: "app.py".into(),
                autoscale: Some(json!({ "max_replicas": 2 })),
                runtime: Some(json!({ "engine": "python" })),
                ..Default::default()
            };
            let saved = $call!(app.deploy(&spec)).unwrap();
            assert_eq!(saved["name"], "embed");
            assert!(saved["id"].as_str().unwrap().starts_with("sfa_"));
            assert_eq!(saved["image"], "ghcr.io/acme/embed:1");
            assert_eq!(saved["autoscale"], json!({ "max_replicas": 2 }));
            assert!(fake.requests().iter().any(|item| item["method"] == "PUT" && item["path"] == "/v1/apps/embed"));
        });

        $case!(a04_app_run_and_volumes, {
            let Some(fake) = start() else { return };
            let app = App::with_client("embed", Client::with_settings(fake.settings()));
            assert_eq!($call!(app.run(json!({ "x": 1 }), Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": { "x": 1 } }));
            $call!(app.set_volumes(&["weights:r"])).unwrap();
            assert_eq!($call!(app.volumes()).unwrap(), json!([{ "volume": "weights", "mode": "r" }]));
        });

        $case!(a05_a_rate_limited_submit_waits_and_succeeds, {
            let Some(fake) = start() else { return };
            let output = $call!(Client::with_settings(fake.settings()).run("busy", json!({ "n": 1 }), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(output, json!({ "app": "busy", "echo": { "n": 1 } }));
            assert_eq!(fake.requests().iter().filter(|item| item["path"] == "/v1/transactions").count(), 2);
        });

        $case!(a06_submit_a_graph_and_wait, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let view = $call!(client.submit(
                json!([
                    { "name": "a", "app_name": "embed", "depends_on": [], "input": { "n": 1 } },
                    { "name": "b", "app_name": "rerank", "depends_on": ["a"], "input": {} }
                ]),
                json!({}),
            ))
            .unwrap();
            let out = $call!(Call::new(client, view).result(Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(out, json!({ "a": { "app": "embed", "echo": { "n": 1 } }, "b": { "app": "rerank", "echo": {} } }));
        });

        $case!(w01_workflow_run_returns_each_stage, {
            let Some(fake) = start() else { return };
            let out = $call!(Workflow::with_client("pipeline", Client::with_settings(fake.settings())).run(json!({ "q": "hi" }), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(out, json!({ "a": { "app": "embed", "echo": { "q": "hi" } }, "b": { "app": "rerank", "echo": {} } }));
        });

        $case!(w02_workflow_deploy_saves_stages, {
            let Some(fake) = start() else { return };
            let flow = Workflow::with_client("solo", Client::with_settings(fake.settings()));
            $call!(flow.deploy(json!([{ "name": "only", "app": "embed", "depends": [] }]))).unwrap();
            let out = $call!(flow.run(json!({ "q": "hi" }), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(out, json!({ "app": "embed", "echo": { "q": "hi" } }));
        });

        $case!(j01_job_with_volumes_finishes, {
            let Some(fake) = start() else { return };
            let spec = JobSpec {
                entrypoint: Some("python train.py".into()),
                image: Some("img".into()),
                providers: vec![json!({ "provider": "local" })],
                volumes: vec![Volume::new("weights").grant(Mode::Read)],
                ..Default::default()
            };
            let mut job = $call!(Client::with_settings(fake.settings()).submit_job(&spec)).unwrap();
            assert_eq!(fake.state()["jobs"][&job.id]["volumes"], json!(["weights:r"]));
            let view = $call!(job.wait(Duration::from_millis(50), Some(Duration::from_secs(30)))).unwrap().clone();
            assert_eq!(view["status"], "succeeded");
            assert_eq!(view["exit_code"], 0);
        });

        $case!(j02_job_logs_oldest_first, {
            let Some(fake) = start() else { return };
            let spec = JobSpec {
                entrypoint: Some("echo hi".into()),
                image: Some("img".into()),
                providers: vec![json!({ "provider": "local" })],
                ..Default::default()
            };
            let job = $call!(Client::with_settings(fake.settings()).submit_job(&spec)).unwrap();
            let lines: Vec<String> = $call!(job.logs(None))
                .unwrap()
                .into_iter()
                .map(|line| line["m"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(lines, vec!["line 0", "line 1", "line 2"]);
        });

        $case!(h01_callback_signatures_match_the_vectors, {
            let Some(dir) = suite() else { return };
            let vectors: Value = serde_json::from_slice(&std::fs::read(dir.join("webhooks.json")).unwrap()).unwrap();
            let tolerance = Duration::from_secs(vectors["tolerance_seconds"].as_u64().unwrap());
            assert_eq!(tolerance, webhooks::TOLERANCE);
            let cases = vectors["cases"].as_array().unwrap();
            assert!(!cases.is_empty());
            for case in cases {
                let name = case["name"].as_str().unwrap();
                let result = webhooks::verify_at(
                    case["body"].as_str().unwrap().as_bytes(),
                    case["signature"].as_str().unwrap(),
                    &[case["secret"].as_str().unwrap()],
                    case["now"].as_i64().unwrap(),
                    tolerance,
                );
                if case["ok"].as_bool().unwrap() {
                    assert_eq!(result.unwrap_or_else(|error| panic!("{name}: {error}"))["seq"], 7, "{name}");
                } else {
                    assert!(result.is_err(), "{name}: expected a rejection, got {result:?}");
                }
            }
        });

        // Surface beyond CASES.md: the rest of the API both modes share.

        $case!(surface_client_app_and_workflow_share_the_client, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let app = client.app("embed");
            assert_eq!(app.name, "embed");
            let mut call = $call!(app.enqueue(json!({ "n": 2 }))).unwrap();
            assert!(!call.id.is_empty());
            let id = call.id.clone();
            assert_eq!($call!(call.get()).unwrap()["id"], json!(id));
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": { "n": 2 } }));
            let flow = client.workflow("pipeline");
            let call = $call!(flow.enqueue(json!({ "q": "x" }))).unwrap();
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap()["a"]["echo"], json!({ "q": "x" }));
        });

        $case!(surface_enqueue_with_callback_still_runs, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let call = $call!(client.enqueue_with_callback("embed", json!({ "n": 3 }), "https://example.test/hook")).unwrap();
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": { "n": 3 } }));
        });

        $case!(surface_job_by_id_and_run_job, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let spec = JobSpec {
                entrypoint: Some("echo hi".into()),
                image: Some("img".into()),
                providers: vec![json!({ "provider": "local" })],
                ..Default::default()
            };
            let submitted = $call!(client.submit_job(&spec)).unwrap();
            let mut again = $call!(client.job(&submitted.id)).unwrap();
            assert_eq!(again.id, submitted.id);
            assert!(!again.status().is_empty());
            $call!(again.get()).unwrap();
            let done = $call!(client.run_job(&spec, Duration::from_millis(50), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(done["status"], "succeeded");
        });

        $case!(surface_volume_list_info_and_download, {
            let Some(fake) = start() else { return };
            fake.make_volume("weights");
            fake.seed("weights", "m/a", "aaa");
            fake.seed("weights", "m/sub/b", "bbb");
            let names: Vec<Value> = $call!(Volume::list(fake.settings())).unwrap().into_iter().map(|row| row["name"].clone()).collect();
            assert_eq!(names, vec![json!("weights")]);
            let weights = Volume::with_settings("weights", fake.settings());
            assert_eq!($call!(weights.info()).unwrap()["name"], "weights");
            let dest = temp("download");
            $call!(weights.path("m/").download(&dest)).unwrap();
            assert_eq!(std::fs::read_to_string(dest.join("a")).unwrap(), "aaa");
            assert_eq!(std::fs::read_to_string(dest.join("sub/b")).unwrap(), "bbb");
        });

        $case!(r01_rate_limited_waits_retry_after_then_succeeds, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 429, "RATE_LIMITED", Some(1))]));
            let started = std::time::Instant::now();
            let output = $call!(Client::with_settings(fake.settings()).run("embed", json!({ "n": 1 }), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(output, json!({ "app": "embed", "echo": { "n": 1 } }));
            assert!(started.elapsed() >= Duration::from_millis(900));
            assert_eq!(fake.count("POST", "/v1/transactions"), 2);
        });

        $case!(r02_queue_full_is_retried, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 503, "QUEUE_FULL", Some(0))]));
            let output = $call!(Client::with_settings(fake.settings()).run("embed", json!({}), Some(Duration::from_secs(30)))).unwrap();
            assert_eq!(output, json!({ "app": "embed", "echo": {} }));
            assert_eq!(fake.count("POST", "/v1/transactions"), 2);
        });

        $case!(r03_server_busy_on_a_get_is_retried, {
            let Some(fake) = start() else { return };
            let call = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({ "n": 3 }))).unwrap();
            fake.faults(json!([refusal("GET", "/v1/transactions/", 503, "SERVER_BUSY", Some(0))]));
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": { "n": 3 } }));
        });

        $case!(r04_app_paused_is_not_retried, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 503, "APP_PAUSED", None)]));
            let error = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!((error.status(), error.error_type(), error.attempts()), (Some(503), Some("APP_PAUSED"), 1));
            assert_eq!(fake.count("POST", "/v1/transactions"), 1);
        });

        $case!(r05_defcon_active_is_not_retried, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 503, "DEFCON_ACTIVE", None)]));
            let error = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!(error.error_type(), Some("DEFCON_ACTIVE"));
            assert_eq!(fake.count("POST", "/v1/transactions"), 1);
        });

        $case!(r13_http_calls_limit_is_not_retried, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 402, "HTTP_CALLS_LIMIT", None)]));
            let error = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!((error.status(), error.error_type(), error.attempts()), (Some(402), Some("HTTP_CALLS_LIMIT"), 1));
            assert!(!error.message().is_empty());
            assert_eq!(fake.count("POST", "/v1/transactions"), 1);
        });

        $case!(r06_a_plain_502_is_retried_on_get_only, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let call = $call!(client.enqueue("embed", json!({}))).unwrap();
            fake.faults(json!([{ "method": "GET", "path": "/v1/transactions/", "status": 502, "raw": "<html>bad gateway</html>" }]));
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": {} }));
            fake.faults(json!([{ "method": "POST", "path": "/v1/transactions", "status": 502, "raw": "<html>bad gateway</html>" }]));
            let error = $call!(client.enqueue("embed", json!({}))).err().unwrap();
            assert_eq!((error.status(), error.error_type(), error.outcome_unknown()), (Some(502), None, false));
            assert_eq!(fake.count("POST", "/v1/transactions"), 2);
        });

        $case!(r07_a_plain_504_on_a_submit_is_an_unknown_outcome, {
            let Some(fake) = start() else { return };
            fake.faults(json!([{ "method": "POST", "path": "/v1/transactions", "status": 504, "raw": "upstream timed out" }]));
            let error = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({}))).err().unwrap();
            assert!(error.outcome_unknown());
            assert_eq!(error.status(), Some(504));
            assert!(error.to_string().contains("may be running"), "{error}");
            assert_eq!(fake.count("POST", "/v1/transactions"), 1);
        });

        $case!(r08_a_dropped_connection_is_retried_on_get_only, {
            let Some(fake) = start() else { return };
            let client = Client::with_settings(fake.settings());
            let call = $call!(client.enqueue("embed", json!({}))).unwrap();
            fake.faults(json!([{ "method": "GET", "path": "/v1/transactions/", "drop": true }]));
            assert_eq!($call!(call.result(Some(Duration::from_secs(30)))).unwrap(), json!({ "app": "embed", "echo": {} }));
            fake.faults(json!([{ "method": "POST", "path": "/v1/transactions", "drop": true }]));
            let error = $call!(client.enqueue("embed", json!({}))).err().unwrap();
            assert!(error.outcome_unknown(), "{error}");
            assert!(error.to_string().contains("may be running"), "{error}");
            assert_eq!(fake.count("POST", "/v1/transactions"), 2);
        });

        $case!(r09_a_400_is_not_retried, {
            let Some(fake) = start() else { return };
            fake.faults(json!([{ "method": "POST", "path": "/v1/transactions", "status": 400, "body": { "detail": "stages must be a list" } }]));
            let error = $call!(Client::with_settings(fake.settings()).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!((error.status(), error.attempts()), (Some(400), 1));
            assert_eq!(fake.count("POST", "/v1/transactions"), 1);
        });

        $case!(r10_a_refused_connection_is_retried, {
            let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
            let settings = Settings {
                master: format!("http://127.0.0.1:{port}"),
                env_api_key: Some("sfk_test".into()),
                api_key: None,
                token_file: None,
                login_file: None,
                replica_token: None,
                ..Settings::from_env()
            };
            let attempts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = attempts.clone();
            let policy = RetryPolicy { max_retries: 2, ..RetryPolicy::default() }.on_retry(move |event| seen.lock().unwrap().push(event.attempt));
            let error = $call!(Client::with_settings(settings).with_retry(policy).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!(error.attempts(), 3, "{error}");
            assert!(!error.outcome_unknown());
            assert_eq!(*attempts.lock().unwrap(), vec![1, 2]);
        });

        $case!(r11_retries_stop_after_max_retries, {
            let Some(fake) = start() else { return };
            let mut fault = refusal("POST", "/v1/transactions", 503, "SERVER_BUSY", Some(0));
            fault["times"] = json!(10);
            fake.faults(json!([fault]));
            let policy = RetryPolicy { max_retries: 2, ..RetryPolicy::default() };
            let error = $call!(Client::with_settings(fake.settings()).with_retry(policy).enqueue("embed", json!({}))).err().unwrap();
            assert_eq!((error.error_type(), error.attempts()), (Some("SERVER_BUSY"), 3));
            assert_eq!(fake.count("POST", "/v1/transactions"), 3);
        });

        $case!(r12_a_wait_past_the_deadline_is_not_taken, {
            let Some(fake) = start() else { return };
            fake.faults(json!([refusal("POST", "/v1/transactions", 429, "RATE_LIMITED", Some(30))]));
            let policy = RetryPolicy { deadline: Duration::from_secs(2), ..RetryPolicy::default() };
            let started = std::time::Instant::now();
            let error = $call!(Client::with_settings(fake.settings()).with_retry(policy).enqueue("embed", json!({}))).err().unwrap();
            assert!(started.elapsed() < Duration::from_millis(1500));
            assert_eq!((error.error_type(), error.retry_after(), error.attempts()), (Some("RATE_LIMITED"), Some(Duration::from_secs(30)), 1));
        });
    };
}

mod async_cases {
    use super::*;
    use std::time::Duration;
    use surfra::{App, AppSpec, Call, Client, Error, JobSpec, Mode, RetryPolicy, Settings, Volume, Workflow, webhooks};

    macro_rules! call {
        ($e:expr) => {
            $e.await
        };
    }

    macro_rules! case {
        ($name:ident, $body:block) => {
            #[tokio::test]
            async fn $name() $body
        };
    }

    cases!(case, call);
}

/// The blocking API, with no runtime around it.
#[cfg(feature = "blocking")]
mod blocking_cases {
    use super::*;
    use std::time::Duration;
    use surfra::blocking::{App, AppSpec, Call, Client, Error, JobSpec, Mode, RetryPolicy, Settings, Volume, Workflow, webhooks};

    macro_rules! call {
        ($e:expr) => {
            $e
        };
    }

    macro_rules! case {
        ($name:ident, $body:block) => {
            #[test]
            fn $name() $body
        };
    }

    cases!(case, call);
}

/// The blocking API called from inside a current-thread Tokio runtime: it must not panic.
#[cfg(feature = "blocking")]
mod blocking_in_current_thread {
    use super::*;
    use std::time::Duration;
    use surfra::blocking::{App, AppSpec, Call, Client, Error, JobSpec, Mode, RetryPolicy, Settings, Volume, Workflow, webhooks};

    macro_rules! call {
        ($e:expr) => {
            $e
        };
    }

    macro_rules! case {
        ($name:ident, $body:block) => {
            #[tokio::test(flavor = "current_thread")]
            async fn $name() $body
        };
    }

    cases!(case, call);
}

/// The blocking API called from inside a multi-thread Tokio runtime: it must not panic.
#[cfg(feature = "blocking")]
mod blocking_in_multi_thread {
    use super::*;
    use std::time::Duration;
    use surfra::blocking::{App, AppSpec, Call, Client, Error, JobSpec, Mode, RetryPolicy, Settings, Volume, Workflow, webhooks};

    macro_rules! call {
        ($e:expr) => {
            $e
        };
    }

    macro_rules! case {
        ($name:ident, $body:block) => {
            #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $name() $body
        };
    }

    cases!(case, call);
}
