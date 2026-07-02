# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.2...HEAD
[0.3.2]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/releases/tag/v0.3.2
