# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Every request sends `Surfra-Client: surfra-rust/<version>`, so the control plane can trace errors to a release.
- Overload retries for every request, async and blocking:
  - **Retried for every request:** 429 `RATE_LIMITED`, 503 `QUEUE_FULL` and 503 `SERVER_BUSY`, after `Retry-After`, plus connections that never got through.
  - **Retried only for `GET`/`HEAD`:** 502/503/504 without a Surfra error body, and connections lost after sending.
  - **Never retried:** `APP_PAUSED`, `DEFCON_ACTIVE`, other 4xx, and submissions that got a 504 or lost their connection after sending.
- `RetryPolicy` (`max_retries` 7, `deadline` 3 min, `on_retry`) and `with_retry` on `Client`, `App`, `Workflow` and `Volume`, in both modes.
- `Error::error_type()`, `message()`, `attempts()`, `retry_after()` and `outcome_unknown()`.
- 402 `HTTP_CALLS_LIMIT` (the organization used its included HTTP calls this month and has no card on file) is named in the docs and never retried; conformance case r13.

### Changed (breaking)
- `Error::Api` gains `error_type`, `retry_after` and `attempts`. `Error::Http` is now `{ source, attempts }`. A new `Error::OutcomeUnknown` and `Error::Decode`, and `Error` is `#[non_exhaustive]`. Code matching `Error::Api { status, detail }` needs `..`.
- The submit-only 429 retry (5 tries, 1 to 60 s) is replaced by the policy above.

## [0.2.1] - 2026-10-26

### Added
- `surfra::webhooks` (also `surfra::blocking::webhooks`): `verify`, `verify_at` and `verify_headers` check a
  callback's `X-Surfra-Signature` (HMAC-SHA256, several `v1=` during secret rotation, 300 s tolerance)
  and return the parsed body.

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
