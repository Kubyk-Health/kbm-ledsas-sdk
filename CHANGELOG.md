# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.6] - 2026-10-21

Security, dependency and reliability release, upgrading from 0.3.3 (0.3.4 and 0.3.5 were not
released: numbers skipped). No public method or parameter was removed or renamed, but some behaviour
changes and input that was never valid is now refused: read the upgrade notes and
[UPGRADING.md](UPGRADING.md). The [API reference](docs/SDK_API_REFERENCE.md) has the full detail.

### Upgrade notes

- **One reply per command.** An attempt that will be retried no longer replies; the one reply is the
  result, a non-retryable error, or the last attempt's error. **Action:** stop at the first reply,
  and allow for the retries in the caller's timeout (the backoff alone is 7 to 10 s with the default
  3 retries).
- **Stricter envelopes.** `sent_at` / `deadline` must be ISO 8601 text, `priority` an integer,
  `schema_version` / `message_version` 1 to 4 ASCII digits on each side of the dot, and `reply_to`
  may not name a `cmd.`, `dlq.` or `amq.` exchange; such a command is dead-lettered before its
  handler runs. `"reply_to": null` now means fire-and-forget. **Action:** fix the sender; a caller
  that used `amq.topic` as its `reply_to` declares a reply exchange of its own.
- **Handlers are checked at registration** (when your module is imported): a name not matching
  `^[A-Za-z][A-Za-z0-9_]*$` (at most 64 characters), a version not like `1.0`, or a function that is
  not `async def` taking `(ctx, payload)` raises `ValueError` / `TypeError`. **Action:** rename the
  command (callers send the new name) or fix the handler.
- **Message models and `BlobRef` are read-only.** Assigning a field of `Envelope`, `Command`,
  `Response`, `Status` or `BlobRef` raises `ValidationError` ("Instance is frozen"; in a handler,
  the command is replied `Permanent`, not retried, with an ERROR naming `model_copy(update=...)`);
  `Command` /
  `Response` / `Status` refuse an envelope of another `type`; `BlobRef(uri=..., version_id=...)`
  raises (`BlobRef` takes only `uri`). **Action:** use `model_copy(update={...})`, and put a version
  in the URI as `?versionId=<version>`.
- **Handler results.** Any returned dict is a successful reply (a top-level `error` key no longer
  fails the command) and `None` replies `{}`. A result with no JSON form, with `bytes` anywhere, or
  larger than `KBM_LEDSAS_MAX_PAYLOAD_BYTES` is replied `Permanent`, not retried. **Action:** raise
  to fail a command; return text (`data.decode()`) or a blob reference.
- **Blob arguments are checked before any I/O.** Malformed blob URIs (a container name storage
  refuses, user info, a port, a query other than `versionId`, a fragment, a backslash, control
  characters, a name over 1024 characters) and invalid upload paths raise `ValueError`; `overwrite`
  must be `True` or `False`, `upload_bytes` data and `upload_stream` chunks bytes-like, and
  `emit_status` `stage` / `note` text (a `str`-valued `Enum` still works). **Action:** parse URIs
  with `BlobRef.from_uri()` and pass text to `upload_text`.
- **`download_json` / `upload_json` take only a JSON object or array.** A scalar root raises
  `ValueError` on download (0.3.3 returned it) and `TypeError` on upload. **Action:** use
  `download_text` / `upload_text` for other JSON values.
- **Configuration is checked at start.** The broker URL must be `amqp://` or `amqps://` with a host
  and percent-encoded credentials; `no_verify_ssl` set to any value except `0` is refused unless
  `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1`; `?heartbeat=` must be a whole number from 0 to 600; boolean
  settings must be booleans; the storage connection string needs a usable credential; a health port
  already in use stops the start. **Action:** fix the setting the one-line `Configuration error`
  names (for a private certificate authority use `?cafile=<path>`), and give each service on a host
  its own `KBM_LEDSAS_HEALTH_PORT`.
- **A redelivery after a lost connection counts as an attempt.** It counts toward
  `KBM_LEDSAS_MAX_RETRIES`, and the command is dead-lettered with one reply once they are spent. A
  handler that blocks the event loop longer than about two heartbeat intervals loses the connection.
  **Action:** run blocking work with `await asyncio.to_thread(...)`.
- **Log changes.** A retried failure logs one WARNING per attempt (a traceback only on the last);
  JSON and text timestamps are UTC ISO 8601 with milliseconds and a `Z`; the text format adds
  `key=value` fields. **Action:** alert on the `Max retries (N) exceeded` ERROR instead of `Uncaught
  exception in handler`, and update log parsers.
- **`/ready` follows the broker.** It answers `503` during a broker outage, after the broker cancels
  the consumer, and while the service stops. **Action:** none, unless a probe expected `200`
  throughout.
- **Long handlers.** RabbitMQ takes back a command unacknowledged for longer than its
  `consumer_timeout` (30 minutes by default). **Action:** have it raised for longer handlers.
- **Deep bodies.** A command body nested more than 256 levels deep is dead-lettered. **Action:**
  flatten it, or send a blob reference.
- **Dependency floors.** A pin of `cryptography` below 50.0.0, `aiohttp` below 3.14.4 or
  `azure-core` below 1.38.0 no longer resolves. **Action:** raise the pin.
- **Numeric container user.** The example `Dockerfile` ends with the numeric `USER 1000:1000` (in
  0.3.3 it ended with the named `USER appuser`), so a pod with `runAsNonRoot: true` and no
  `runAsUser` is admitted. **Action:** end your own `Dockerfile` the same way, or set `runAsUser:
  1000` in the pod.

### Added

- **Container base image** `ghcr.io/kebormed/kbm-ledsas-base-direct:<version>`: Python 3.11 on
  Alpine Linux (`linux/amd64`, `linux/arm64`) with that SDK version installed from PyPI and pinned
  (`PIP_CONSTRAINT`), user `kbmuser` (uid 1000), working directory `/app`. See
  [COMPATIBILITY.md](COMPATIBILITY.md#container-base-image).
- **`KBM_LEDSAS_ALLOWED_CONTAINERS`** (optional): the containers `ctx.blob` may use; any other
  raises `errors.ContainerNotAllowed` (a `PermissionError`, replied `Permanent`) before storage is
  called. Unset, nothing is restricted.
- **`KBM_LEDSAS_HEARTBEAT`** (default 60 s, `0` to `600`, `0` off): the AMQP heartbeat the SDK asks
  for. 0.3.3 set none, so the AMQP client library's default of 60 s applied. A dead broker is
  noticed after `3 × (interval + 1)` seconds plus up to half an interval: 183 s at the default,
  plus up to 30 s. A valid `?heartbeat=` in the URL wins.
- **`KBM_LEDSAS_CONNECT_TIMEOUT`** (default 15 s): one broker connection attempt fails the start
  within it, with one ERROR line.
- **`from kbm_ledsas_sdk import BlobOperations`**, the type of `ctx.blob`.
- **WARNINGs** for a misspelled `KBM_LEDSAS_*` variable, `KBM_LEDSAS_PREFETCH` below
  `KBM_LEDSAS_CONCURRENCY`, no handler registered, a handler timeout above the broker's default
  `consumer_timeout`, a too-small memory limit, cleartext storage on another host, an overriding
  `KBM_LEDSAS_SERVICE_NAME`, and an event loop blocked for 1 s or more.
- **More record fields:** every per-command record carries `message_id`, `command_name`, `trace_id`,
  `idempotency_key` (and `job_id`); records about an unusable message body carry `amqp_message_id`,
  `amqp_correlation_id` and `body_bytes`; the start record carries `sdk_version`.

### Changed

- `aio-pika` 10.x is accepted (`>=9.4.0,<11.0`); `aiormq` (`>=6.8.0,<8.0`) and `pamqp`
  (`>=3.3.0,<5.0`) are declared, since the SDK imports them. `pydantic>=2.8.0` (was `>=2.5.0`),
  the first with Python 3.13 wheels; the `azure-storage-blob` range is unchanged.
- Errors that cannot succeed on a retry (a storage HTTP 4xx other than 408/429, an invalid blob URI,
  content that is not JSON or UTF-8, invalid SDK-call arguments) are replied `Permanent` and
  dead-lettered at once, with one ERROR line. Exception types are unchanged.
- A handler's `CancelledError` or other `BaseException` is a handler failure (retried, then one
  `UnexpectedError` reply); `SystemExit` / `KeyboardInterrupt` in a handler is replied `Permanent`
  instead of ending the process.
- A passed `deadline` replies `DeadlineExceeded` without a retry.
- A reply's `correlation_id` is returned exactly as sent (it was lowercased).
- Reply payload keys are converted as `json.dumps` does, at every level; colliding keys and keys
  with no JSON form are refused, with an error naming where.
- `upload_json` writes `NaN` / `Infinity` as `null`; `upload_json` / `upload_text` set a JSON /
  text content type.
- `download_stream` honours `chunk_size` (one ranged request per chunk, pinned to the blob's ETag)
  and retries a blob that changed mid-download.
- Blob calls give up sooner: about 10 s against a refused connection, about 40 s against storage
  that never answers.
- Every `KBM_LEDSAS_*` value has surrounding whitespace and one pair of matching quotes removed; an
  empty `KBM_LEDSAS_SERVICE_NAME` is read as unset.
- Startup failures (broker refused or unreachable, unknown virtual host, refused credentials, a
  queue declared with other arguments, a bad setting) are one ERROR line naming the cause, then the
  exit line; no traceback.
- Configured secrets (the broker password, the storage `AccountKey` and SAS) are masked in every
  log record and in every text the SDK sends to a caller: error messages, `user_message` and status
  `stage` / `note`; a handler's result is sent as returned. Credentials and tokens are masked in
  handler exceptions and `ctx.logger` records. `repr()` of `SDKConfig.rabbitmq_url` /
  `blob_conn_string` is `'**redacted**'` (`str()` and the value are unchanged).
- A broker outage or restart logs one WARNING and, on recovery, one INFO; no ERROR, no traceback.
- A consumer the broker cancels (its queue deleted, or its node down) is noticed: one ERROR,
  `/ready` answers `503`, and the SDK declares the queue and consumes again.
- Without your own logging setup, other code's warnings also use `KBM_LEDSAS_LOG_FORMAT`.
- Health checks time out after 2 s (`timeout=`, at most 60 s); a failing check is logged once when
  it starts failing, secrets masked; slow and idle health connections are closed.
- On SIGTERM the service stops consuming and gives back commands not yet started at once; a second
  SIGTERM or Ctrl+C cancels the running handlers.

### Fixed

- SIGTERM during a broker outage no longer hangs: each broker operation of the shutdown is bounded
  (5 s), and unconfirmed commands are left for the broker to redeliver.
- A retry backoff no longer delays shutdown, and handlers that can no longer reply are cancelled.
- A broker restart while a command runs no longer dead-letters it: the redelivery is settled by the
  command still in progress, which runs once.
- A refused reply no longer leaves a broker channel open.
- A message with AMQP properties that are not valid UTF-8 is dead-lettered once, instead of
  dropping the connection on every redelivery.
- A `message_id` can be reused as soon as its reply has arrived.
- A stop signal during startup exits cleanly, with code 0.
- `deployment_id` drops a matched pair of surrounding quotes. It is read from
  `KBM_LEDSAS_DEPLOYMENT_ID`, or from its older alias `KBM_LASTCOMMITID` when
  `KBM_LEDSAS_DEPLOYMENT_ID` is unset or blank, as in 0.3.3. The SDK sets neither.
- README: the quickstart reads only from its input container and writes its result to a container
  it chooses (see "Security model" in the API reference); the configuration table lists every
  common setting. `KBM_LEDSAS_CONTAINER` is documented as informational: it never
  selected a container.
- Example service: builds `FROM` the base image, keeps its code read-only, removes pip at the end of
  the build, logs in `KBM_LEDSAS_LOG_FORMAT`; its sender reports an error reply in one line, lets
  the command expire after `--timeout` and cleans up on SIGTERM.

### Security

- `aiohttp>=3.14.4` (was `>=3.8.0`): fixes CVE-2026-69244, CVE-2026-111063, CVE-2026-111064 and GHSA-2g87-pp6c-29x5.
- `cryptography>=50.0.0` (transitive, was `>=48.0.1`): fixes CVE-2026-69247 (GHSA-g6cj-pr64-35w5).
- `azure-core>=1.38.0,<2.0` (no floor before): fixes CVE-2026-21226 (PYSEC-2026-1208); an in-place
  upgrade no longer keeps an older `azure-core`.
- `KBM_LEDSAS_ALLOWED_CONTAINERS` limits what a command can make the service do with its storage
  credential; see "Security model" in the [API reference](docs/SDK_API_REFERENCE.md#security-model).

### Known limitations

- The 256-level nesting limit is checked on the parsed body. A body that Python's own JSON parser
  gives up on first (at about 1000 levels on 3.11), including a repeated key nested that deep, can
  be dead-lettered on 3.11 and processed on 3.13: send standard JSON without repeated keys.

## [0.3.5]

Not released; number skipped, no changes.

## [0.3.4]

Not released; number skipped, no changes.

## [0.3.3] - 2026-07-10

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
  infrastructure is unaffected. See [UPGRADING.md](UPGRADING.md) for the full
  upgrade guide and [COMPATIBILITY.md](COMPATIBILITY.md) for the support matrix.

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

## [0.3.2] - 2026-07-02

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

[Unreleased]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.6...HEAD
[0.3.6]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.3...v0.3.6
[0.3.3]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/Kubyk-Health/kbm-ledsas-sdk/releases/tag/v0.3.2
