# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows [Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-10-25

### Added
- `surfra::blocking` (feature `blocking`) now mirrors the whole async API with the same names: `Client`
  (`submit`, `enqueue`, `run`, `submit_job`, `run_job`, `job`, `app`, `workflow`), `App`, `Workflow`,
  `Call` (`get`, `result`), `Job` (`get`, `stop`, `logs`, `wait`), `Volume` (including `list`, `writer`
  and `Upload`) and `VolumePath` (`local`, `local_with`, `download`). Fields such as `job.id` and
  `call.view` are reachable through `Deref`.
- `Client::app` and `Client::workflow`, sharing the client's connection pool.
- `Client::run_job` in the async API (it was blocking-only).
- `Client::submit_with_callback` and `Client::enqueue_with_callback`. Surfra POSTs the task's latest
  state to the `callback_url` each time it changes.
- `surfra::webhooks` (also `surfra::blocking::webhooks`): `verify`, `verify_at` and `verify_headers` check a
  callback's `X-Surfra-Signature` (HMAC-SHA256, several `v1=` during secret rotation, 300 s tolerance)
  and return the parsed body.

### Changed
- The blocking API runs on one shared multi-thread Tokio runtime instead of one runtime per object, and
  no longer panics when called from inside a Tokio runtime.
- The `blocking` feature now enables `tokio/rt-multi-thread`.
- `VolumePath::local` and `local_with` futures are now `Send`.

### Fixed
- Job and transaction ids are percent-encoded in request paths.

## [0.1.1]

### Added
- `Workflow::deploy` and `Call::new`.

## [0.1.0]

- First release: volumes, apps, workflows and jobs, async with a partial `blocking` facade.

[0.2.0]: https://github.com/surfra-io/surfra-rust/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/surfra-io/surfra-rust/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/surfra-io/surfra-rust/releases/tag/v0.1.0
