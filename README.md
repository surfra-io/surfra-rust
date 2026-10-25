# surfra

The Surfra SDK for Rust: volumes, apps and jobs on serverless GPUs across your own clouds.

```toml
[dependencies]
surfra = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

## Volumes

```rust
use surfra::{Mode, Volume};

#[tokio::main]
async fn main() -> surfra::Result<()> {
    let weights = Volume::new("weights");

    // A folder of the volume, synced to local disk. Only changed files download.
    let model_dir = weights.path("minilm/").local().await?;

    // One file in, one file out.
    weights.write("notes/run-1.txt", "done").await?;
    let bytes = weights.read("notes/run-1.txt").await?;

    // Change a folder in place, then upload what changed.
    let ckpts = Volume::new("ckpts");
    let dir = ckpts.path("run-7/").local_with(Mode::ReadWrite, false).await?;
    std::fs::write(dir.join("step-100.bin"), &bytes)?;
    ckpts.commit().await?;
    Ok(())
}
```

## Without async

Enable the `blocking` feature and use `surfra::blocking`. It has the same types and methods as the async
API (`Client`, `App`, `Workflow`, `Call`, `Job`, `Volume`, `VolumePath`, `Upload`), minus the `.await`:

```toml
surfra = { version = "0.2", features = ["blocking"] }
```

```rust
use std::time::Duration;
use surfra::blocking::{Client, Volume};

fn main() -> surfra::Result<()> {
    let model_dir = Volume::new("weights").path("minilm/").local()?;

    let client = Client::new();
    let call = client.app("embed").enqueue(serde_json::json!({ "texts": ["hello"] }))?;
    println!("queued {}", call.id);
    let output = call.result(Some(Duration::from_secs(600)))?;
    Ok(())
}
```

Every blocking call runs on one shared multi-thread Tokio runtime, started on first use. Calling it from
inside a Tokio runtime does not panic (the call runs on a helper thread while the caller waits), but it
still blocks that thread, so async code should use the async API.

## Apps and jobs

```rust
use std::time::Duration;
use surfra::{Client, JobSpec};

async fn train() -> surfra::Result<()> {
    let client = Client::new();
    let output = client.run("embed", serde_json::json!({ "texts": ["hello"] }), Some(Duration::from_secs(600))).await?;

    let mut job = client
        .submit_job(&JobSpec {
            entrypoint: Some("./train --epochs 3".into()),
            image: Some("ghcr.io/acme/train:3".into()),
            providers: vec![serde_json::json!({ "provider": "runpod", "gpu": "rtx4090" })],
            volumes: vec!["weights:r".into(), "ckpts:rw".into()],
            ..Default::default()
        })
        .await?;
    job.wait(Duration::from_secs(10), None).await?;
    Ok(())
}
```

## Credentials

Inside a Surfra app or job nothing needs setting: `SURFRA_VOLUMES_URL` is a local proxy that adds the
credential. Elsewhere, set `SURFRA_API_KEY`, or pass one with `Settings::from_env().with_api_key(...)`.

The order is the same in every Surfra SDK: an explicit key, `SURFRA_VOLUMES_TOKEN_FILE`, `SURFRA_API_KEY`,
`SURFRA_REPLICA_TOKEN`, then the session `surfra login` saved.

## Tests

The tests are the shared [Surfra SDK cases](https://github.com/surfra-io/surfra-sdk-tests), run against its
fake server. Check it out next to this repository, or set `SURFRA_SDK_TESTS`, then `cargo test`.
