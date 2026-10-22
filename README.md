# surfra

The Surfra SDK for Rust: volumes, apps and jobs on serverless GPUs across your own clouds.

```toml
[dependencies]
surfra = "0.1"
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

Without an async runtime, enable the `blocking` feature and use `surfra::blocking::Volume`, which has the same methods.

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
