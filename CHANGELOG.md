# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.3] - 2026-07-09

### Changed

- **Unroutable responses now dead-letter the command instead of vanishing.**
  Responses (and status updates) are published with the AMQP *mandatory*
  flag. Previously, if your `reply_to` exchange existed but had **no queue
  bound** for the `response` routing key, the publish "succeeded", the
  response was silently dropped by the broker, and the command was ACKed as
  if delivered. Now the broker returns the message, `send_response()` reports
  the failure, and the command is NACKed to the dead-letter queue — the same
  path as a missing exchange, with one clean ERROR line and no traceback
  noise. Status updates remain best-effort (a single WARNING, processing
  continues).

  **Action required only if you relied on the old silent behavior:** bind a
  queue to your reply exchange (declare it `durable=True, auto_delete=False`
  — see `examples/hello_world_service/scripts/send_hello.py`), or set
  `reply_to` to `""` for fire-and-forget. Correctly wired reply
  infrastructure is unaffected.

### Added

- **`reply_unroutable_failures` counter** on the transport, bumped alongside
  `reply_publish_failures` when a reply failed specifically because the
  exchange existed but had no bound queue — so operators can tell "reply
  exchange missing" apart from "reply queue missing/unbound".

### Fixed

- **The per-message retry counter is now size-bounded** (oldest-entry
  eviction at 10,000 tracked messages). In long-running multi-replica
  deployments, a requeued message whose redelivery landed on another replica
  used to leave an orphaned counter slot forever — a slow, unbounded memory
  growth. Eviction only resets that message's local retry budget.

## [0.3.2] - 2026-07-01

First public release of the KeborMed LEDSAS SDK. The SDK connects directly to
RabbitMQ and Azure Blob Storage, and provides the handler-based programming model
for building LEDSAS data-processing services.

### Added
- `ServiceApp` with the `@app.handler(...)` decorator for registering async handlers.
- Azure Blob helpers: `download_bytes`/`text`/`json`/`stream` and
  `upload_bytes`/`text`/`json`, including memory-bounded streaming for large files.
- Typed Pydantic v2 models for blob references and message envelopes.
- Built-in retries and a per-handler timeout, with `Retryable`, `Permanent`, and
  `DeadlineExceeded` error classes.
- Health & readiness HTTP endpoints (`/health`, `/ready`, `/livez`, `/readyz`) with
  pluggable liveness/readiness checks and optional `deployment_id` reporting.
- Structured JSON logging, exposed via `json_log_formatter()`.
- Full type hints (`py.typed`); requires Python 3.11+.

[Unreleased]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.3...HEAD
[0.3.3]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/releases/tag/v0.3.2
