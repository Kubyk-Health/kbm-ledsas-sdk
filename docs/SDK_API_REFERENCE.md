# LEDSAS SDK API Reference v0.3.6

Complete reference for all SDK APIs implemented in kbm-ledsas-sdk v0.3.6.

> **Note:** The SDK connects directly to RabbitMQ and Azure Blob Storage; you
> configure it through the connection environment variables documented below.
> What changed in each release, and what to do when you upgrade, is in
> [CHANGELOG.md](../CHANGELOG.md) and [UPGRADING.md](../UPGRADING.md).

---

## Public vs Internal modules

Public, supported API:

- `from kbm_ledsas_sdk import ServiceApp, ExecutionContext, errors`
- `from kbm_ledsas_sdk import json_log_formatter` (see "Logging note")
- `from kbm_ledsas_sdk import BlobOperations` (the type of `ctx.blob`, for
  type hints)
- `from kbm_ledsas_sdk.models import BlobRef, Command, Response, Status, Envelope`.
  They are frozen: assigning a field (`ctx.envelope.reply_to = "..."`) raises
  `pydantic.ValidationError`; use
  `envelope.model_copy(update={"reply_to": "..."})`. Uncaught in a handler,
  it is replied as `Permanent` (`SDK models are read-only (assignment to
  Envelope.reply_to): use model_copy(update=...)`). `Command`, `Response`
  and `Status` refuse an envelope of another `type` (`envelope.type is
  'command', not 'response' (a Response carries a response envelope)`);
  copy the envelope with `type=` set to build one from another.
- `errors.SDKError`, `errors.Retryable`, `errors.Permanent`,
  `errors.DeadlineExceeded`, `errors.ContainerNotAllowed` and
  `errors.ArgumentTypeError`. The first four are also importable from
  `kbm_ledsas_sdk.models` (the same classes); `ContainerNotAllowed` from
  `kbm_ledsas_sdk.errors` (`from kbm_ledsas_sdk import errors` or `from
  kbm_ledsas_sdk.errors import ContainerNotAllowed`), not from
  `kbm_ledsas_sdk.models`.
- Every class, method, env var, and behaviour documented in this file.

Internal, although reachable from the public modules:

- the `ExecutionContext` constructor (the SDK builds the context; use the
  class only as a type hint);
- the pydantic validator methods of the models and of the configuration
  (`Envelope.validate_timezone_aware`,
  `Envelope.validate_deadline_after_sent_at`, `BlobRef.validate_uri_format`,
  the configuration's `validate_*`): do not call them;
- `HandlerFunc` in `kbm_ledsas_sdk.app`, an undocumented alias kept for
  earlier code;
- every deeper import path (`kbm_ledsas_sdk.transport.*`,
  `kbm_ledsas_sdk.amqp.*`, `kbm_ledsas_sdk.runtime.*`, `kbm_ledsas_sdk.blob.*`,
  `kbm_ledsas_sdk.health.*`, `kbm_ledsas_sdk.utils.*`): it may change or go
  in any point release, with no deprecation period. Reach in only for test
  fixtures;
- the `ServiceApp` attributes that are not documented. On a `ServiceApp`, the documented members are public: its constructor, `service_name`, the decorators, `run()` and `health_server_running`. Attributes other than `service_name` are internal and may change.

**Code snippets.** Most blocks are fragments: a block that uses `ctx` or
`req` runs inside an `@app.handler` function, `app` is your `ServiceApp`,
and names such as `compute`, `metrics` or `db` stand for your own code.
Blocks that start with their imports and end with `app.run()` or
`asyncio.run(...)` are complete programs.

---

## Environment Variables

How every value is read:

- Surrounding whitespace and one matched pair of surrounding quotes are
  removed (`docker run --env-file` passes quotes through, a shell `source`
  removes them), so `KBM_LEDSAS_LOG_LEVEL="INFO"` and
  `KBM_LEDSAS_LOG_LEVEL=INFO` mean the same.
- A value the SDK cannot use stops the start with one `Configuration error`
  line naming the variable and its value (connection values are named but
  never repeated): `Configuration error: KBM_LEDSAS_PREFETCH=5000: Input
  should be less than or equal to 1000`. Each range is in the table. The
  error behind it keeps no connection setting: its `errors()` / `json()`
  input is `<not kept: the settings may hold connection secrets>`.
- The boolean settings (`KBM_LEDSAS_GENERIC_ERRORS`,
  `KBM_LEDSAS_HEALTH_VERBOSE`, `KBM_LEDSAS_ALLOW_INSECURE_AMQP`,
  `KBM_LEDSAS_DEBUG`) accept `true` / `false`, `1` / `0`, `yes` / `no`,
  `on` / `off`, `t` / `f`, `y` / `n`, in any letter case; unset or empty is
  `false`. Anything else stops the start: `Configuration error:
  KBM_LEDSAS_GENERIC_ERRORS='ture' is not a boolean (use true or false; ...)`.
- An empty value means the default for the booleans and for names (such as
  `KBM_LEDSAS_TENANT`); the numeric and choice settings (such as
  `KBM_LEDSAS_PREFETCH` or `KBM_LEDSAS_LOG_LEVEL`) refuse an empty value:
  unset the variable to use its default (`Configuration error:
  KBM_LEDSAS_PREFETCH='' is empty: unset it to use the default (10) or give
  a whole number`; for a log setting `Invalid log level: '' (empty: unset
  the variable to use the default)`).
- A `KBM_LEDSAS_` name that is not a setting below (a typo) is not read and
  gets one WARNING at start, with the closest setting: `Ignoring
  environment variable(s) that are not settings of this SDK:
  KBM_LEDSAS_PREFECH (did you mean KBM_LEDSAS_PREFETCH?)`.
  Their values are not logged. The variables Kubernetes sets for a Service
  in the namespace are not reported: nobody set them. For a Service named
  `kbm-ledsas-<name>` these are `KBM_LEDSAS_<NAME>_SERVICE_HOST`,
  `KBM_LEDSAS_<NAME>_SERVICE_PORT` (and `..._SERVICE_PORT_<PORT NAME>`),
  `KBM_LEDSAS_<NAME>_PORT` and `KBM_LEDSAS_<NAME>_PORT_<port>_TCP` (and its
  `_PROTO`, `_PORT` and `_ADDR` forms).

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_SERVICE_NAME` | Constructor arg required; env optional | Service name for queue binding. Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. `ServiceApp(service_name=...)` is always required and checked by the constructor: a non-string raises `TypeError` (`ServiceApp(service_name=None): the service name must be a string, got NoneType`), an invalid one `ValueError` (`ServiceApp(service_name='bad name'): a service name is 1 to 64 characters: letters, digits, '.', '_' and '-', starting with a letter or digit`). A non-empty env value **overrides** the constructor value and renames the exchange and queue (`cmd.<name>.v1` / `queue.<name>.v1`), with one WARNING at start when they differ. An empty or blank value (`KBM_LEDSAS_SERVICE_NAME=` in a `.env` file or `docker run -e`) is read as unset. |
| `KBM_LEDSAS_RABBITMQ_URL` | Required | RabbitMQ connection URL (see "Transport security") |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Required | Azure Blob connection string, required even if your handlers never call `ctx.blob`. A value the storage client cannot read stops the start with `Configuration error: KBM_LEDSAS_BLOB_CONN_STRING is not a usable storage connection string (...)` (the value is not repeated). So does a string that carries no credential, neither an `AccountKey` nor a `SharedAccessSignature` (or an empty one): storage would refuse every request with HTTP 403. So does an `AccountKey` the storage client cannot decode (for example one whose trailing `=` was lost in a copy); a key that decodes but is not the account's key is not caught here, and every storage request then fails with HTTP 403. A `BlobEndpoint` that is not an `http://` or `https://` URL with a host is refused at start too, with the same `Configuration error`. A shared access signature (SAS) connection string (`BlobEndpoint=https://<account>.blob.core.windows.net;SharedAccessSignature=<token>`) works too and can be limited to the containers and permissions the service needs. |
| `KBM_LEDSAS_ALLOWED_CONTAINERS` | _(unset)_ | Comma-separated containers `ctx.blob` may use (`input,results`); see "Container allowlist". Unset, empty or blank: no restriction. |
| `KBM_LEDSAS_CONTAINER` | `dev` | Informational: logged at startup as `default_container`. The `ctx.blob.upload_*` methods always take an explicit `container` and never read it. To use it as your output container, read it yourself: `os.getenv` returns the raw value: under `docker run --env-file` a quoted value keeps its quotes, so remove surrounding whitespace and one matched pair of quotes as the SDK does: `v = os.getenv("KBM_LEDSAS_CONTAINER", "").strip(); v = v[1:-1].strip() if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'" else v; container = v or "dev"`. |
| `KBM_LEDSAS_PREFETCH` | `10` | AMQP prefetch count (`1` to `1000`): commands received and held at once (see "Memory"). Handlers running at once are at most `min(KBM_LEDSAS_PREFETCH, KBM_LEDSAS_CONCURRENCY)`; below `KBM_LEDSAS_CONCURRENCY` the SDK logs one WARNING at start (`KBM_LEDSAS_PREFETCH=1 is below KBM_LEDSAS_CONCURRENCY=4: ... at most 1 handler(s) run at once ...`). |
| `KBM_LEDSAS_CONCURRENCY` | `4` | Max concurrent handlers (`1` to `100`). At most `min(KBM_LEDSAS_PREFETCH, KBM_LEDSAS_CONCURRENCY)` run at once: keep `KBM_LEDSAS_PREFETCH` at least this high. |
| `KBM_LEDSAS_LOG_LEVEL` | `INFO` | Level of the `kbm_ledsas_sdk` loggers (see "Logging"). |
| `KBM_LEDSAS_LOG_FORMAT` | `json` | `json` or `text` (see "Text format"). |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | `1800` | Handler time limit in seconds (`0` disables; max `86400`). Above `1800` (or `0`) the SDK logs one WARNING at start: keep RabbitMQ's `consumer_timeout` (30 min by default) above the longest handler run plus the time a command waits for a worker (see "Handler Timeout"). |
| `KBM_LEDSAS_MAX_RETRIES` | `3` | `0` to `100`. Retries, NOT counting the initial attempt, for `Retryable`, `Timeout` and unexpected exceptions: the default runs a failing handler **4 times total** before its message goes to DLQ. See "Retry Mechanism". |
| `KBM_LEDSAS_GENERIC_ERRORS` | `false` | Send a generic message when the handler set no `user_message` (see "Generic Error Messages"). |
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | HTTP port for `/health` and `/ready` (`0` disables; at most `65535`). A port in use stops the service at start (see "Behavior under failure"). |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Health server bind address: an IPv4 or IPv6 address or a host name. Write an IPv6 address without brackets (`::`, `::1`). Anything else (`999.1.1.1`, `[::]`) stops the start with one `Configuration error` line. `0.0.0.0` is what a container orchestrator's HTTP probes need; the SDK then logs one INFO line that the port is reachable from the network (a WARNING if `KBM_LEDSAS_HEALTH_VERBOSE` is also on). |
| `KBM_LEDSAS_HEALTH_VERBOSE` | `false` | If `true`, health responses include SDK version, service name and per-check status (fingerprintable; for development). |
| `KBM_LEDSAS_MAX_PAYLOAD_BYTES` | `16777216` (16 MiB) | Largest inbound message body (a larger one: one WARNING, DLQ) and largest serialized reply (a larger result gets one `Permanent` reply, `returned a result too large to send as a reply (... bytes serialized; the limit is ... bytes, KBM_LEDSAS_MAX_PAYLOAD_BYTES)`). A command without `reply_to` gets no reply, so its result is not size-checked. It is not serialized either: only its type is checked (see "Return value"). `0` disables both; max `268435456` (256 MiB). The broker's `max_message_size` (16 MiB by default on RabbitMQ 4.x) applies first: a larger message is refused at publish with `PRECONDITION_FAILED` and never reaches the SDK. |
| `KBM_LEDSAS_ALLOW_INSECURE_AMQP` | `0` | If `1` (or `true`), downgrade the refusal of cleartext `amqp://` to a non-loopback host, and of `no_verify_ssl`, to a WARNING (see "Transport security"). |
| `KBM_LEDSAS_TENANT` | _(unset)_ | Optional tenant for topology names (see "AMQP Topology"). Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. |
| `KBM_LEDSAS_DEBUG` | `0` | If `1` (or `true`), print the full traceback of a configuration error at start instead of the one-line message. |
| `KBM_LEDSAS_CONNECT_TIMEOUT` | `15` | Seconds one connection attempt to the broker may take (`1` to `300`), at start and on every reconnect. The SDK first logs `Connecting to the AMQP broker at amqp://***:***@<host>:<port>/ (timeout 15s, heartbeat 60s)` (the URL's user name and password, every value of its query string and its fragment are masked: `amqps://***:***@host:5671/vhost?heartbeat=***`). A broker that does not answer fails the start within this time (`Could not connect to the AMQP broker at ...: no answer within 15s (KBM_LEDSAS_CONNECT_TIMEOUT) ...`, then the closing ERROR line `ServiceApp exiting (startup failure already reported above). Reason class: ...` and exit code 1) instead of after the operating system's 75 s to 2+ minutes. |
| `KBM_LEDSAS_HEARTBEAT` | `60` | AMQP heartbeat interval in seconds that the SDK asks the broker for (`0` to `600`; `0` turns heartbeats off, and with it the detection of a broker that stops answering). The default `60` is the interval earlier releases already used. A dead broker or network path is detected after `3 × (interval + 1)` seconds (183 s at the default 60) plus up to half an interval, and the SDK reconnects (see "Broker outages are SDK log events"). A handler that blocks the event loop longer than about two heartbeat intervals loses the connection, and the redelivery counts toward `KBM_LEDSAS_MAX_RETRIES` (see "Blocking code is not timed out"): run blocking work with `asyncio.to_thread(...)` rather than raising the heartbeat. A `?heartbeat=` in `KBM_LEDSAS_RABBITMQ_URL` wins over this setting (only in lowercase: the AMQP client library ignores another spelling such as `?HEARTBEAT=`, and the SDK then adds its own); its value must be a whole number from `0` to `600`, else `Configuration error: KBM_LEDSAS_RABBITMQ_URL: ?heartbeat='abc' is not a whole number of seconds between 0 and 600 (0 turns heartbeats off, so a broker that stops answering is not detected)`. |
| `KBM_LEDSAS_DEPLOYMENT_ID` | _(unset)_ | Your build or deployment identifier (for example the commit id), reported as `deployment_id` by `/health` and `/ready` (see "Deployment id"). The SDK does not set it. Surrounding whitespace and one matched pair of surrounding quotes are removed, and a blank value counts as unset. |
| `KBM_LASTCOMMITID` | _(unset)_ | Older alias of `KBM_LEDSAS_DEPLOYMENT_ID`, read only when that is unset or blank; prefer `KBM_LEDSAS_DEPLOYMENT_ID`. |

**Inline comments.** A value of `KBM_LEDSAS_DEPLOYMENT_ID` (or its older
alias `KBM_LASTCOMMITID`) or `KBM_LEDSAS_CONTAINER` with a space then `#`
in it is kept as it is, with one WARNING at start (`KBM_LEDSAS_DEPLOYMENT_ID
looks like it carries an inline '# comment' (a space then '#'): ...`):
`docker run --env-file` passes the rest of the line through. Put comments
on their own line.

### Handler Timeout

- Default 1800 s (30 minutes); `0` disables it. What a timeout does is in
  "Timeout Enforcement".
- **The broker's `consumer_timeout` must be longer.** RabbitMQ takes back
  a command left unacknowledged longer than its `consumer_timeout` (30
  minutes by default; a command is acknowledged when its handler has
  finished) and delivers it again (see "The broker's consumer_timeout
  closed the channel"). Keep `consumer_timeout` above the longest handler
  run plus the time a command waits for a worker. Above `1800`, or at `0`,
  the SDK logs one WARNING at start (`KBM_LEDSAS_HANDLER_TIMEOUT=0 (no
  limit) allows handler runs longer than RabbitMQ's default
  consumer_timeout (30 min): ...`).

### Transport security

Each refusal below stops the start with one `Configuration error` line
that does not repeat any part of the URL.

- **Cleartext AMQP** (`amqp://`) is accepted only for a loopback host
  (`localhost`, `127.0.0.1`, `::1`). Use `amqps://`, or set
  `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` when TLS is terminated in between (a
  service mesh, a local Docker network); the SDK then logs one WARNING per
  host.
- **TLS certificate verification stays on.** A URL whose query sets
  `no_verify_ssl` to any value other than `0` (`1`, `true`, even `false`,
  or no value at all: the AMQP client library keeps verification on only
  for `no_verify_ssl=0`, which is accepted) stops the start; with
  `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` it is accepted with one WARNING
  (`KBM_LEDSAS_RABBITMQ_URL sets no_verify_ssl: the broker's TLS certificate
  is not verified. ...`). Only the lowercase spelling counts: the library
  ignores the option in another letter case (`NO_VERIFY_SSL=1`), and
  verification stays on. For a private
  certificate authority keep verification on and name its certificate:
  `amqps://user:password@broker:5671/?cafile=/etc/ssl/broker-ca.pem`;
  `?certfile=<path>&keyfile=<path>` give a client certificate. The paths
  are read inside the service's container; like every query value, they
  are masked in log records. A path that names no file (or a file the
  service cannot read; for `?capath=`, no directory) stops the start with
  `Configuration error: KBM_LEDSAS_RABBITMQ_URL: ?cafile= names a file that
  does not exist: '<path>'`: this line quotes the path.
- **Storage over `http://` to another machine** (`DefaultEndpointsProtocol=http`
  or an `http://` `BlobEndpoint`, host not `localhost`, `127.0.0.1`, `::1`)
  starts with one WARNING (`KBM_LEDSAS_BLOB_CONN_STRING reaches storage at
  <host> over http://: the requests, signed with the account key or SAS,
  travel in cleartext. ...`), a local emulator in another container
  (`BlobEndpoint=http://azurite:10000/...`) included.
- **Credentials in the URL must be percent-encoded.** A user name or
  password with `@`, `#` or `?` would parse to the wrong host and is
  refused. Encode them (`@` -> `%40`, `#` -> `%23`, `?` -> `%3F`, `/` ->
  `%2F`), e.g. with `urllib.parse.quote(password, safe="")`.
- **Only `amqp://` and `amqps://` URLs with a host are accepted.** Another scheme, no
  scheme (`user:pass@host:5672`), no host, a port that is not 1 to 65535,
  or whitespace is refused (`Configuration error: KBM_LEDSAS_RABBITMQ_URL
  ...`). The message names the usual causes: an unmatched quote, and an
  inline `# comment`, which `docker run --env-file` passes through
  verbatim (put comments on their own line).

---

## Security model

**The command exchange is the trust boundary.** The SDK checks the shape
of every command (see "Message Format"), not who sent it: anyone who can
publish to a service's command exchange (`cmd.[{tenant}.]{service}.v1`)
can make it run any of its handlers with any payload, and the handler acts
with the **service's own** credentials:

- **Storage.** A blob URI in a payload (`azblob://<container>/<path>`) is
  chosen by the caller, and `ctx.blob` reads and writes it with
  `KBM_LEDSAS_BLOB_CONN_STRING`: a handler that downloads
  `req["input_uri"]`, or uploads to a container and path from the payload,
  reaches every blob that credential reaches.
- **Replies.** Replies and status messages go to the exchange named in
  `reply_to` (only `cmd.`, `dlq.` and `amq.` exchanges are refused), with
  the service's broker credentials.

Limit what a command can make the service do:

- give each service a storage credential scoped to the containers it
  needs (a container-scoped SAS connection string, see
  `KBM_LEDSAS_BLOB_CONN_STRING`), not the account key of a shared account;
- restrict who can publish to the command exchange with RabbitMQ
  permissions (a user per caller, with write permission only on the
  exchanges it may use; the service's user needs configure, write and
  read on its own `cmd.`, `queue.` and `dlq.` names and write on the reply
  exchanges);
- list the containers the service may use in
  `KBM_LEDSAS_ALLOWED_CONTAINERS` (see "Container allowlist" below);
- in the handler, check the container (and, where it matters, the path)
  of every blob URI in a payload and raise `errors.Permanent` for any
  other (`BlobRef.from_uri(uri).container`);
- treat payload text as untrusted when you log it (see "Payload-field
  safety" under "ExecutionContext").

### Container allowlist (`KBM_LEDSAS_ALLOWED_CONTAINERS`)

`KBM_LEDSAS_ALLOWED_CONTAINERS=input,results` limits every `ctx.blob`
operation to the listed containers: uploads (`upload_bytes`,
`upload_json`, `upload_text`, `upload_stream`) by their `container`
argument, downloads (`download_bytes`, `download_json`, `download_text`,
`download_stream`) by the container of the URI (string or `BlobRef`).

- **Value:** comma-separated names; spaces around a name are ignored, a
  name may repeat, names match exactly (container names are lowercase).
- **Unset, empty or blank:** no restriction (the default, and the
  behaviour of earlier releases).
- **Checked at start.** An empty item (`a,,b`, a leading or trailing
  comma) or an invalid name stops the start:
  `Configuration error: KBM_LEDSAS_ALLOWED_CONTAINERS='a,,b' has an empty
  item: list container names separated by commas (for example
  'input,results'), or unset it for no restriction` or
  `... KBM_LEDSAS_ALLOWED_CONTAINERS: 'Bad_Name' is not a valid container
  name (...)`. When set, the start logs one INFO `Blob access limited
  to the containers in KBM_LEDSAS_ALLOWED_CONTAINERS: input, results`
  (field `allowed_containers`).
- **A refused operation** raises `errors.ContainerNotAllowed` before
  anything is sent to storage and logs one WARNING `Blob upload refused:
  container 'other' is not in KBM_LEDSAS_ALLOWED_CONTAINERS` (or `Blob
  download refused: ...`; fields `container` and `operation`), even when
  the handler catches it. Uncaught, it is not retried: one ERROR
  `Non-retryable error in handler (not retried): container 'other' is not
  allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)` and one `Permanent` reply. An
  argument of the wrong type (`None`, a malformed URI) keeps its own
  `TypeError` / `ValueError`.

The allowlist limits what a command can make the service do with its
credential; it is not an authorization layer for callers. The credential's own scope (a
container-scoped SAS) remains the boundary.

---

## Message Format

Every message is a JSON object with an `envelope` and a `payload`.

### Command Message Structure

```json
{
  "envelope": {
    "schema_version": "1.0",
    "type": "command",
    "name": "ProcessData",
    "message_version": "1.0",
    "message_id": "550e8400-e29b-41d4-a716-446655440000",
    "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
    "idempotency_key": "unique-operation-key",
    "sent_at": "2026-05-26T10:30:00Z",
    "trace_id": "abc123-trace-id",
    "reply_to": "response.exchange.name",
    "priority": 5,
    "job_id": "batch-2026-05-26-001"
  },
  "payload": {
    "your_field": "your_data",
    "another_field": 123
  }
}
```

The optional `deadline` is left out. If you send one, it must be after
`sent_at` and still in the future when the command arrives: a command
whose deadline has passed is not run (it gets a `DeadlineExceeded` reply,
see "`errors.DeadlineExceeded`").

### Envelope Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | Yes | Use "1.0". `<major>.<minor>`, each 1 to 4 ASCII digits: pattern `^[0-9]{1,4}\.[0-9]{1,4}$`. |
| `type` | string | Yes | `"command"`, `"response"` or `"status"`. |
| `name` | string | Yes | Command name (e.g., "ProcessData"). Pattern `^[A-Za-z][A-Za-z0-9_]*$`, 1..64 chars. `@app.handler` refuses any other name with a `ValueError` at registration. |
| `message_version` | string | Yes | Same rule as `schema_version`: pattern `^[0-9]{1,4}\.[0-9]{1,4}$`. Must equal the `version` the handler was registered with. |
| `message_id` | string | Yes | Unique message identifier (UUID, either letter case), normalized to lowercase. The service's status updates and its reply carry the command's `message_id`, in the envelope and in the AMQP `message_id` property. Do not de-duplicate the messages you receive on it (an inbox keyed on `message_id` would drop the reply as a copy of the first status update); match them on `correlation_id` and the envelope `type`. |
| `correlation_id` | string | Yes | UUID, either letter case. Echoed **exactly as sent** in the reply and status updates (and the reply's AMQP `correlation_id` property): match replies with `==`. |
| `idempotency_key` | string | Yes | Key for idempotent processing. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..128 chars. |
| `sent_at` | string | Yes | ISO 8601 timestamp with a timezone (`2026-05-26T10:30:00Z`). Only a timestamp string is accepted: a number (Unix time such as `1760000000`) or numeric string is refused. |
| `trace_id` | string | Yes | Distributed tracing ID. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..255 chars. |
| `reply_to` | string | No | Name of a **pre-declared AMQP exchange** for the reply. URL-safe `^([A-Za-z0-9_\-:.]+)?$`, max 127 chars. Routing key `response` (`status` for `emit_status`). The SDK does not declare it. **Contract: the exchange must exist AND have at least one queue bound** for `response`: replies are published as mandatory, so a missing exchange or an unroutable reply fails the reply and dead-letters the command. A name starting with `cmd.` or `dlq.` (a service's command or dead-letter exchange) or with `amq.` (the broker's reserved exchanges, such as `amq.topic`) makes the envelope invalid (dead-lettered before the handler runs). Leave empty (or omit it, or send JSON `null`, which is read as empty) for fire-and-forget. See `examples/hello_world_service/scripts/send_hello.py` for a runnable caller. |
| `deadline` | string | No | ISO 8601 with a timezone, after `sent_at`. A number is refused. |
| `priority` | int | No | An integer 0..9 (`"5"`, `5.0` and `true` are refused). Informational only: copied to the reply; the queue is not a priority queue. |
| `job_id` | string | No | Business-level job identifier. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..128 chars. |

**Telling an error reply from a result.** A success reply and an error
reply are both `"response"` envelopes with the same AMQP properties; only
the payload tells them apart. A reply is an
error reply when `payload["error"]` is an object (a `dict` in Python)
with the keys `code`, `message` and `retryable`, as in `{"error":
{"code": "Permanent", "message": "...", "retryable": false}}`; the SDK's
error replies carry nothing else in `payload`. Any other reply is the
handler's result, which may have an `error` key of its own:
`{"valid": false, "error": "field x missing"}` is a success. In Python:
`error = payload.get("error")`, then the reply failed when
`isinstance(error, dict) and {"code", "message", "retryable"} <=
error.keys()`. Do not return an `error` value of that shape, or the caller takes your result for a failure. Raise instead (see "Error Handling").

**Invalid envelopes are dead-lettered without a reply.** A command whose
envelope fails validation (a `deadline` before `sent_at`, an invalid
`name`, a non-UUID `message_id`, ...) goes straight to the dead-letter
queue with one ERROR line and no reply, even with a valid `reply_to`;
the caller only times out. Unknown extra envelope fields and any
`schema_version` matching the pattern are accepted. The body must be a
JSON object with an `envelope` object and a `payload` object (`{}` is
fine); a missing or non-object `payload` is dead-lettered the same way
(`Envelope failed schema validation; dead-lettering`, with `payload:
Field required` or `payload: Input should be a valid dictionary`).

**Malformed message bodies are dead-lettered too.** A body that is not
UTF-8, not JSON, nested more than 256 levels deep, or holds an integer
literal longer than 4300 digits is dead-lettered with one ERROR line,
no traceback, no retry and no reply. The body object is level 1, its
`payload` level 2. The 256-level limit is the SDK's own, the same on
every supported Python version; a deeper body logs `Message body
exceeds JSON-parsing nesting limit; dead-lettering` with `error_type`
`ExcessiveNesting`. Nest data less deeply, or send it in a blob.
A body with more than 256 `[` and `{` characters in all is walked once
to measure its depth, holding the event loop like the parse; the time
grows with the number of values: about 0.1 s for 16 MiB of ordinary records, up to about 0.3 s for 16 MiB of
plain numbers or strings, and up to about 1 s on Python 3.13 and 1.3 s
on 3.11 for 16 MiB of only empty or nested arrays and objects (hardware
dependent). Large data belongs in a blob. The parser accepts `NaN`, `Infinity` and `-Infinity` (float
`nan` / `inf` in the handler, `null` in a reply), and the last of two
equal keys wins (only it counts toward the limit); send standard JSON
(RFC 8259). Two cases follow the Python version's own parser (about
1000 levels on 3.11, far deeper on 3.13): a repeated key whose
discarded value is nested past that point can be dead-lettered on 3.11
and processed on 3.13, and a body nested past it that is also invalid
JSON may be logged as either error.

**AMQP properties that are not UTF-8 are dead-lettered too.** A message
whose short-string property (`message_id`, `correlation_id`,
`reply_to`, `type`, `content_type`, `app_id`, ...) or header name holds
invalid UTF-8 is dead-lettered once, before its body is read (no retry,
no reply), with one WARNING naming the property
(`AMQP message property message_id is not valid UTF-8; dead-lettering`;
a header as `headers[<name>]`); `amqp_message_id` shows each such byte
as a `\xNN` escape. Consumption goes on and the connection stays up. To decode such messages the
service replaces the two short-string decoders of `pamqp` (the AMQP frame
decoder `aio-pika` uses) for the whole process when it starts (when it
sets up its broker connection; importing `kbm_ledsas_sdk` alone changes
nothing): only a
value that fails strict UTF-8 decoding changes, decoded with
`surrogateescape` (lone surrogates `\udcNN`); valid UTF-8 decodes exactly
as before. Your own `aio-pika` code in
the same process then receives such a value as a `str` with lone
surrogates instead of an error that closes its connection.

**Memory.** A parsed body stays in memory until its command finishes and
takes far more memory than its size (CPython 3.11 and 3.13, a 15.7 MB
body: about 380 MB (24 times the body) for only empty arrays or objects; about 15
times for small records (`{"t":1,"v":2}`) or short strings, 10 times for
floats, 4 times for small integers). Up to `KBM_LEDSAS_PREFETCH`
commands are held at once, so the worst case is about
`KBM_LEDSAS_PREFETCH` x `KBM_LEDSAS_MAX_PAYLOAD_BYTES` x 25: about 4 GiB
with the defaults (10 x 16 MiB), about 250 MiB with a 1 MiB limit. A
service killed for memory gets the same messages again after its
restart. Keep bodies small (send a blob reference) and size
`KBM_LEDSAS_MAX_PAYLOAD_BYTES` (and `KBM_LEDSAS_PREFETCH`) to the
service's memory. In a container whose cgroup memory limit is below
the worst case, the SDK says so in one WARNING at start (`Up to
KBM_LEDSAS_PREFETCH=10 command bodies of up to 16 MiB each ... about 3.9
GiB in the worst case, above this container's memory limit of 512 MiB
...`) with a `KBM_LEDSAS_MAX_PAYLOAD_BYTES` (or, for a high prefetch, a
lower `KBM_LEDSAS_PREFETCH`) that keeps it at half the limit or less; it
changes nothing itself. With `KBM_LEDSAS_MAX_PAYLOAD_BYTES=0` the
WARNING assumes 128 MiB bodies, RabbitMQ 3.x's default
`max_message_size` (4.x's is 16 MiB).

Each of these lines, the envelope-validation line, the `Unexpected
error processing message` line and the WARNING for a body over
`KBM_LEDSAS_MAX_PAYLOAD_BYTES` carry `amqp_message_id` and
`amqp_correlation_id` (the message's AMQP `message_id` and
`correlation_id` properties, when set; at most 128 characters) and
`body_bytes` (the body's size in bytes), to find the message in the
dead-letter queue.

**Use a new `message_id` for every command.** A second command with the
`message_id` of one still in progress is dead-lettered at once without a
reply (one WARNING, `Duplicate message_id while original still
pending`); its caller only times out. To retry, send a new envelope
with a new `message_id` and the same `idempotency_key`. Once the first
command has finished, its `message_id` is accepted again. A command that
reuses it as soon as the first command's reply has arrived (while the
first command is still being acknowledged) waits for that
acknowledgement, at most 10 s, and then runs. The broker's own
redelivery is not such a duplicate: if the connection to the broker was
lost and restored, or the broker took the command back at its
`consumer_timeout`, while the command is still in progress here, the
redelivery is handed to that command (see "Broker connection lost while a command runs").

### Response Message Structure

The reply goes to the command's `reply_to` exchange with routing key `response`:

```json
{
  "envelope": {
    "schema_version": "1.0",
    "type": "response",
    "name": "ProcessData",
    "message_version": "1.0",
    "message_id": "550e8400-e29b-41d4-a716-446655440000",
    "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
    "idempotency_key": "unique-operation-key",
    "sent_at": "2026-05-26T10:30:02.418000+00:00",
    "deadline": null,
    "trace_id": "abc123-trace-id",
    "reply_to": "response.exchange.name",
    "priority": 5,
    "job_id": "batch-2026-05-26-001"
  },
  "payload": {
    "result_uri": "azblob://dev/results/output.json"
  }
}
```

| Field | Value in a reply |
|-------|------------------|
| `envelope.type` | `"response"` |
| `envelope.name`, `message_version`, `message_id`, `correlation_id`, `idempotency_key`, `trace_id`, `reply_to`, `priority`, `job_id` | Copied from the command (`message_id` lowercase, `correlation_id` exactly as sent; an absent optional field is `null`) |
| `envelope.sent_at` | When the service sent the reply (UTC) |
| `envelope.deadline` | Always `null` |
| `payload` | The handler's return value (`{}` for `None`), or, when the command failed, an `error` object and nothing else: |

```json
{
  "payload": {
    "error": {
      "code": "Permanent",
      "message": "input is missing field x",
      "retryable": false
    }
  }
}
```

`code` is the error type (`Permanent`, `Retryable`, `Timeout`,
`DeadlineExceeded`, `UnexpectedError`, ...), `message` the text the
caller may see (see "Generic Error Messages"), `retryable` whether a
later resubmission may succeed (see "Replies: one per command").

**AMQP properties** of a reply: `content_type` `application/json`,
`delivery_mode` 2 (persistent), `message_id` and `correlation_id` as in
the envelope. **Headers:** `message_type` (`response`), `schema_version`,
`message_version`, `trace_id` and `sent_at`. Match a reply on
`correlation_id`, not on `message_id`.

### Status Message Structure

Each `ctx.emit_status(...)` publishes a status message to the same
`reply_to` exchange with routing key `status` (none without `reply_to`),
built like a reply with `type` `"status"`:

```json
{
  "envelope": {
    "schema_version": "1.0",
    "type": "status",
    "name": "ProcessData",
    "message_version": "1.0",
    "message_id": "550e8400-e29b-41d4-a716-446655440000",
    "correlation_id": "123e4567-e89b-12d3-a456-426614174000",
    "idempotency_key": "unique-operation-key",
    "sent_at": "2026-05-26T10:30:01.052000+00:00",
    "deadline": null,
    "trace_id": "abc123-trace-id",
    "reply_to": "response.exchange.name",
    "priority": 5,
    "job_id": "batch-2026-05-26-001"
  },
  "payload": {
    "stage": "downloading",
    "progress": 0.25,
    "note": "Downloading input"
  }
}
```

| Field | Value in a status message |
|-------|---------------------------|
| `envelope.type` | `"status"` |
| other envelope fields | As in a reply |
| `payload.stage` | The `stage` passed to `emit_status` |
| `payload.progress` | A number from `0.0` to `1.0` |
| `payload.note` | The `note`; absent when none was given |

AMQP properties as for a reply; the `message_type` header is `status`.
Status messages are informational: any number per command, and one that
cannot be published does not fail the command. To receive them, bind
your reply queue for the routing key `status` as well as `response`.

### Replies: one per command

When `reply_to` is set, the service sends **exactly one reply per
command, when it has finished with the command**:

- the handler's return value on success;
- the error, for a non-retryable failure (`Permanent`,
  `DeadlineExceeded`, `HandlerNotFound`, and the non-retryable cases
  under "Error Handling");
- the error from the **last** attempt, for a retryable failure
  (`Retryable`, `Timeout`, unexpected exception) once
  `KBM_LEDSAS_MAX_RETRIES` retries are spent.

An attempt that is going to be retried sends **no** reply, so the first
reply whose `correlation_id` matches yours is the outcome: stop waiting
when it arrives (as the example's `scripts/send_hello.py` does). Tell it from
status updates by the envelope `type`, not by `message_id`. Allow for
retries in your reply timeout: with the default 3 retries the backoff
alone adds 7 to 10 seconds, plus the handler's run time per attempt. A
storage call against unreachable storage takes about 7.5 s of each attempt
(the storage client's own retries), so the reply then arrives after about
40 s (see "Backend outages and concurrency").

In an error reply, `retryable: true` means the failure was transient and
the service has stopped retrying (the command is in the dead-letter
queue): resubmitting later may succeed. `retryable: false` means it will
fail the same way again.

The one reply is per run of the command. A command run again after a lost
broker connection or a shutdown (see "Broker connection lost while a
command runs" and "Shutdown" under "Logging") replies again, so a caller
can receive a second reply for the same `correlation_id`: ignore it.

### Payload

The `payload` is passed to your handler as `req`:

```python
@app.handler("ProcessData")  # Must match envelope.name in message
async def handle(ctx, req: dict):
    # req == the "payload" from the message
    your_field = req.get("your_field")  # "your_data"
    another_field = req.get("another_field")  # 123
```

**Signature:** `@app.handler(command_name: str, version: str = "1.0")`. A
command is routed by its `envelope.name` and `envelope.message_version`;
leave `version` at the default unless you run side-by-side versions.
`command_name` must match `^[A-Za-z][A-Za-z0-9_]*$` (at most 64
characters) and `version` the `message_version` rule (such as `1.0`);
anything else raises `ValueError` at registration. The handler must be an
`async def` function accepting two positional arguments (`ctx`, `req`): a
plain `def`, a lambda, an async generator, something that is not
callable, or a function that cannot be called with two arguments raises
`TypeError` at registration (`...
two positional arguments: the SDK calls it with (ctx, payload), but it
takes 1 positional argument.`). A name or version that is not a string
raises `errors.ArgumentTypeError` (a `TypeError` that is also a
`ValueError`). Run blocking work with
`await asyncio.to_thread(...)`.

**Return value:** a JSON-serializable `dict`, which becomes the reply
payload; `None` is treated as `{}`. A result may hold any keys, `error`
included: whether the command failed is decided
by how the handler ended (it returned, or it raised), never by the
result's keys: a returned `{"valid": False, "error": "field x missing"}` is
replied as it is and the command is acknowledged. To fail a command,
raise (see "Error Handling"); do not return an `error` value shaped like
an error reply (see "Telling an error reply from a result").

- **Keys** follow `json.dumps` at every level, the top level included: `str` kept; `int`,
  `float`, `bool` and `None` become the text `json.dumps` writes (`1` ->
  `"1"`, `True` -> `"true"`, `None` -> `"null"`), so
  `df.to_dict(orient="index")` works. Other key types (a tuple, a date)
  and two keys that become the same text (`1` and `"1"`) are refused.
- **Values** are written as JSON. Besides what `json.dumps` accepts,
  pydantic (which serializes replies) converts many values; among them:
  `datetime` / `date` / `time` -> ISO 8601 text (UTC `datetime` with a
  `Z`), a `timedelta` -> an ISO 8601 duration such as `PT1M30S`, `Decimal`, `UUID`, `Fraction` and
  `complex` -> text (`"1.10"`, `"1/3"`, `"1j"`), `pathlib.Path`, an IP
  address, network or interface and a compiled regular expression (its
  pattern) -> text, an `Enum` -> its value, `NaN` / `Infinity` -> `null`;
  `set` / `frozenset` / `tuple` (a named tuple too) / `deque` -> list; a
  generator or another iterator (`map(...)`, `zip(...)`, an open text file)
  -> the list of its items, read once (an exception while reading refuses
  the result: `payload["g"]: reading this generator raised ValueError:
  ...`); a dataclass -> an object of its fields; a pydantic model -> what
  its own serialization writes. A value with no JSON form is refused
  (`payload["o"] is an instance of Weird, which has no JSON form`);
  `range`, `dict_keys` / `dict_values` and `array.array` are among them:
  convert them with `list(...)`.
- **Where these rules apply:** in everything written as a JSON object or
  array, at every level: dicts, lists, tuples, sets, deques, iterator
  items, dataclass fields and `Enum` values.
  **A pydantic model (or pydantic dataclass) is the exception:** it is
  written with its own serialization settings and the SDK does not look
  inside it for keys or binary data (pydantic writes `bytes` in a model
  as UTF-8 text unless the model sets `ser_json_bytes`; equal-text dict
  keys keep the last). Convert binary data first, or set `ser_json_bytes="base64"`.
  The text rule below applies inside a model too.
- **Binary data (`bytes`, `bytearray`, `memoryview`) is refused wherever
  these rules apply, whatever its content** (`payload["b"] is bytes; a
  reply carries text, not binary data: ...`), UTF-8-looking bytes too.
  Decode text (`data.decode()`), base64-encode binary
  (`base64.b64encode(data).decode()`), or store it in a blob and return
  the reference.
- **Text** (keys and values) must be valid Unicode text, at every level
  and inside a pydantic model too: a lone surrogate (`"\ud800"`, which a JSON request can carry) is
  refused (`payload["s"] is text with a lone surrogate U+D800 at position
  0, which UTF-8 cannot encode`), since strict JSON readers (.NET's
  System.Text.Json) cannot read its escape. The same applies to
  `emit_status` and `upload_json`. An error reply is not refused: in its
  message each lone surrogate is replaced by U+FFFD, the Unicode
  replacement character, so every JSON reader can read the error reply
  too.

**Without `reply_to`** (a fire-and-forget command) no reply is sent, so
only the result's type is checked: a `dict` or `None` is accepted; the
rules above do not apply, and the command is acknowledged. **With
`reply_to`** the result is checked and serialized on the service's event
loop before the reply is sent, so a large result holds the loop
(health probes and other handlers wait): put large data in blob storage
and return its reference.

A return value that is not a dict, or (with `reply_to`) a dict with a
refused key or value, is a handler bug: one ERROR, a `Permanent` reply,
and the command is dead-lettered **without retrying** (its side effects
have happened). The error names where: `Handler for <name> returned a dict
that is not JSON-serializable (payload["rows"][3] is bytes; ...)`,
`(payload: the key (1, 2) (tuple) is not a JSON object key; ...)` or
`(payload["n"] is an integer with more than 4300 digits; ...)`. A reply
larger than `KBM_LEDSAS_MAX_PAYLOAD_BYTES` is refused the same way
(`returned a result too large to send as a reply`), and so is a dict
nested too deeply to serialize: pydantic, which serializes replies, stops
at 254 or 255 levels (depending on its version; the returned dict is level
1), and the ERROR says `not JSON-serializable (Circular reference detected
(depth exceeded))`. A request `payload` can
be 255 levels deep, so a handler that returns it, or wraps it, can reach
this limit.

### AMQP Topology

The SDK creates this topology from `service_name`:

| Component | Name Pattern | Type | Description |
|-----------|--------------|------|-------------|
| Command Exchange | `cmd.[{tenant}.]{service_name}.v1` | TOPIC | Receives incoming commands |
| Command Queue | `queue.[{tenant}.]{service_name}.v1` | - | Service consumes from here |
| DLQ Exchange | `dlq.[{tenant}.]{service_name}.v1` | TOPIC | Failed message routing |
| DLQ Queue | `dlq.queue.[{tenant}.]{service_name}.v1` | - | Stores failed messages (see "Dead-letter queue" below) |

**Multi-tenant note:** with `KBM_LEDSAS_TENANT` set, the tenant follows the
prefix: `cmd.{tenant}.{service_name}.v1`. Changing the tenant after
deployment means new queues and new publisher-side permissions.

**Example:** For `ServiceApp(service_name="csv-processor")`:
- Command exchange: `cmd.csv-processor.v1` (or `cmd.acme.csv-processor.v1` with `KBM_LEDSAS_TENANT=acme`)
- Command queue: `queue.csv-processor.v1` (or `queue.acme.csv-processor.v1`)

**Dotted names can share a queue.** `.` is also the name separator, so
tenant `acme` with service `csv-processor` and no tenant with service
`acme.csv-processor` both use `queue.acme.csv-processor.v1` (as do tenant
`a.b` with service `c` and tenant `a` with service `b.c`); services with
the same names on one virtual host share the queue's commands. The tenant
is a naming prefix, not an isolation boundary: keep tenant and service
names free of `.`, or give each tenant a virtual host (and broker user) of
its own.

**Queue type and high availability.** Both queues are durable and get the
virtual host's default queue type (classic unless you set another; no
`x-queue-type` is passed). For replicated queues set `default-queue-type`
to `quorum` on the virtual host **before** the service first starts
(`rabbitmqctl add_vhost <vhost> --default-queue-type quorum`, or
`rabbitmqctl update_vhost_metadata <vhost> --default-queue-type quorum`);
the SDK's queue arguments work on quorum queues. Each retry counts toward
a quorum queue's delivery limit (20 by default on RabbitMQ 4.x, set with
the `delivery-limit` policy): keep `KBM_LEDSAS_MAX_RETRIES` well below it,
or the broker dead-letters the command itself, without a reply. A queue's
type and arguments are fixed at creation (see "A queue that already
exists with other settings"): to change them, drain and delete the queue
and let the service create it again.

#### Dead-letter queue

The dead-letter queue has two fixed limits: a message expires after **7
days** (`x-message-ttl` 604800000 ms), and it holds at most **100,000**
messages (`x-max-length`; at the limit RabbitMQ silently drops the
**oldest** message). Monitor its depth and handle dead letters within 7
days. These two arguments are fixed (there is no setting); to cap the size in bytes, or to
lower a limit, apply a broker **policy** (the lower of policy and argument
applies), for example
`rabbitmqctl set_policy dlq-bytes '^dlq\.queue\.' '{"max-length-bytes": 1073741824}' --apply-to queues`.
An existing dead-letter queue with other arguments is not changed: the
start fails with `PRECONDITION_FAILED` (see "A queue that already exists
with other settings" under "Logging").

**Finding the reason.** A dead-lettered message carries the broker's
`x-death` header (reason `rejected`, queue, time), not the SDK's reason,
which is in the service log. Take the `correlation_id` from the
dead-lettered body's envelope and search the log for it: the record
holding the reason carries it (for example ERROR `Permanent error in handler` with its
`error_message`, WARNING `Deadline already exceeded`, ERROR `Max retries
(3) exceeded, sending to DLQ`). Every attempt and replay keeps the
`correlation_id`; each delivery's `Received command` record pairs its
`message_id` with it. A command refused on arrival never runs and has one
record:

| Refused because | The record carries |
|-----------------|--------------------|
| duplicate `message_id`, or an envelope whose `type` is not `command` | the command's `message_id` |
| the envelope failed validation | the body's `envelope.message_id` and `envelope.correlation_id` when readable (each a plain id of at most 128 characters) |
| the body could not be read (too large, not UTF-8, not JSON, nested more than 256 levels deep, an integer literal longer than 4300 digits) | `amqp_message_id` / `amqp_correlation_id` (if the sender set them) and `body_bytes` |

The dead-letter queue keeps the **full command body** for up to 7 days:
if bodies can carry personal or health data, control who can read it, or
keep such data out of bodies (send a blob reference).

**Replaying.** Once the cause is fixed, take each command from the
dead-letter queue, publish its body again to the command exchange with a
**new** `message_id` (keep the `idempotency_key`; see "Use a new
`message_id` for every command"), then acknowledge it. Run the recipe,
with the service running, as
`python replay_dlq.py csv-processor` (with a tenant,
`<tenant>.<service>`); it reads `KBM_LEDSAS_RABBITMQ_URL`. Save it as `replay_dlq.py`:

```python
import asyncio, json, logging, os, re, sys, uuid
from urllib.parse import urlsplit
import aio_pika

# The AMQP client library logs its own copy of a connection error; the
# recipe reports it in one line below.
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

# The service name (with a tenant: "<tenant>.<service>"), from the command line.
SERVICE = sys.argv[1] if len(sys.argv) > 1 else "csv-processor"
URL = os.environ.get("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
# False: a message that is not a readable command stays in the queue.
# True: it is saved to a file in the current directory (readable by your
# user only; it can hold personal data), then removed.
REMOVE_UNREADABLE = False
# A command whose reply_to exchange no longer exists (its caller's
# temporary reply exchange is gone) could not send its reply: the service
# would run it and dead-letter it again. Such a command is replayed with
# this reply_to instead: "" sends no reply; or name a reply exchange you own.
MISSING_REPLY_TO = ""

def too_deep(value, limit=256) -> bool:
    # The service dead-letters a body nested more than 256 levels on arrival.
    stack = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, (dict, list)):
            if depth > limit:
                return True
            stack.extend((child, depth + 1)
                         for child in (item.values() if isinstance(item, dict) else item))
    return False

async def exchange_exists(connection, name: str) -> bool:
    # On a channel of its own: a missing exchange closes the channel it is
    # checked on.
    channel = await connection.channel()
    try:
        await channel.get_exchange(name, ensure=True)
        return True
    except aio_pika.exceptions.ChannelNotFoundEntity:
        return False
    finally:
        if not channel.is_closed:
            await channel.close()

async def replay(url: str) -> None:
    parts = urlsplit(url)
    try:
        broker = f"{parts.hostname}:{parts.port or 5672}"  # never print the password
    except ValueError:
        sys.exit("error: the port in KBM_LEDSAS_RABBITMQ_URL is not a number from 1 to 65535")
    try:
        connection = await aio_pika.connect(url, timeout=10)
    except aio_pika.exceptions.ProbableAuthenticationError:
        sys.exit(f"error: the broker at {broker} refused the user name or "
                 "password in KBM_LEDSAS_RABBITMQ_URL")
    except (aio_pika.exceptions.AMQPConnectionError, OSError, asyncio.TimeoutError) as e:
        # Not running, wrong host or port, unknown virtual host, ...
        sys.exit(f"error: cannot connect to the broker at {broker} ({type(e).__name__})")
    async with connection:
        channel = await connection.channel()
        try:
            dlq = await channel.declare_queue(f"dlq.queue.{SERVICE}.v1", passive=True)
            exchange = await channel.get_exchange(f"cmd.{SERVICE}.v1", ensure=True)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            sys.exit(f"error: no dead-letter queue dlq.queue.{SERVICE}.v1 or command "
                     f"exchange cmd.{SERVICE}.v1 on this broker; check the service "
                     f"name (and tenant) and that the service has started once")
        if MISSING_REPLY_TO and not await exchange_exists(connection, MISSING_REPLY_TO):
            sys.exit(f"error: no exchange {MISSING_REPLY_TO} (MISSING_REPLY_TO) on this broker")
        # Only the messages in the queue now: a replayed command that fails
        # again is dead-lettered again, behind them, and this run does not
        # take it a second time.
        waiting = dlq.declaration_result.message_count
        replayed = 0
        unreadable = []  # held (not acknowledged) until the end of the run
        exists = {}  # reply_to exchange name -> whether it exists
        gone = set()  # the reply_to exchanges that no longer exist
        rerouted = 0
        for _ in range(waiting):
            message = await dlq.get(fail=False)
            if message is None:
                break
            try:
                body = json.loads(message.body)
                if too_deep(body):
                    raise ValueError("nested more than 256 levels")
                envelope = body["envelope"]
                envelope["message_id"] = str(uuid.uuid4())
                reply_to = envelope.get("reply_to")
                data = json.dumps(body).encode()
            except Exception:  # not JSON, no envelope, nested too deep, ...
                unreadable.append(message)
                continue
            # Only a reply_to the service accepts: with another one (for example
            # one naming a cmd., dlq. or amq. exchange) the command is an
            # invalid envelope and is dead-lettered again before it runs.
            if (isinstance(reply_to, str) and re.fullmatch(r"[A-Za-z0-9_\-:.]{1,127}", reply_to)
                    and not reply_to.startswith(("cmd.", "dlq.", "amq."))):
                if reply_to not in exists:
                    exists[reply_to] = await exchange_exists(connection, reply_to)
                if not exists[reply_to]:
                    envelope["reply_to"] = MISSING_REPLY_TO
                    data = json.dumps(body).encode()
                    gone.add(reply_to)
                    rerouted += 1
            await exchange.publish(
                aio_pika.Message(data, content_type="application/json",
                                 delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key="command",
            )
            await message.ack()
            replayed += 1
        for message in unreadable:
            if REMOVE_UNREADABLE:
                name = f"dlq-unreadable-{uuid.uuid4()}.bin"
                # Mode 0600: readable by your user only.
                with open(os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as copy:
                    copy.write(message.body)
                await message.ack()
                print(f"removed a message that is not a readable command (saved as {name})")
            else:
                await message.nack(requeue=True)
        print(f"replayed {replayed} of {waiting} message(s)")
        if rerouted:
            how = f"with reply_to {MISSING_REPLY_TO}" if MISSING_REPLY_TO else "without a reply"
            print(f"{rerouted} of them replayed {how}: their reply exchange no longer "
                  f"exists ({', '.join(sorted(gone))})")
        if unreadable and not REMOVE_UNREADABLE:
            print(f"{len(unreadable)} message(s) are not readable commands; left in the queue")

asyncio.run(replay(URL))
```

What the recipe does:

- It replays the messages that are in the queue when it starts, each
  once. A replayed command that fails again is dead-lettered again and
  stays in the queue for a later run; that always happens to a command
  whose envelope was invalid or whose `deadline` has passed (the replay
  keeps the envelope: the sender must send such a command again).
- Each replay of a command with a `reply_to` sends the caller another
  reply with the same `correlation_id`, if its reply exchange still
  exists. Often it does not (the example's `scripts/send_hello.py` deletes its reply queue and exchange when it exits),
  while the dead-letter queue keeps a command for 7 days. So the recipe
  replays a command whose exchange is gone with `reply_to` set to
  `MISSING_REPLY_TO`: empty (the default) sends no reply, or name a reply
  exchange you own (with a queue bound for `response`). The run then
  prints `N of them replayed without a reply: their reply exchange no
  longer exists (<names>)` (`with reply_to <name>` when set). A
  `reply_to` the service refuses (`cmd.`, `dlq.`, `amq.`) is left as it
  is (one naming a `cmd.`, `dlq.` or `amq.` exchange).
- A body that is not a readable command (not UTF-8, not JSON, no
  envelope, nested more than 256 levels, the service's limit) is held until
  the end of the run, then left in the queue. With
  `REMOVE_UNREADABLE = True` each is saved to a
  `dlq-unreadable-<uuid>.bin` file in the current directory and removed.
  The files are readable by your user only (mode 0600): a body can hold
  personal data, so delete them once read.
- A broker that cannot be reached or refuses the credentials: one
  `error:` line, exit code 1.

### Sending Messages to a Service

Publish to the service's command exchange. This sender targets the README's quickstart service (`csv-processor`,
handler `ProcessCSV`); change the exchange, `name` and payload for yours.
It needs only `aio-pika`, which the SDK depends on:

```python
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit

import aio_pika

# The AMQP client library logs its own copy of a connection error; the
# sender reports it in one line below.
logging.getLogger("aiormq").setLevel(logging.CRITICAL)

# Same broker as the service.
URL = os.environ.get("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
EXCHANGE = "cmd.csv-processor.v1"  # cmd.{service_name}.v1


async def main() -> None:
    url = urlsplit(URL)
    try:
        broker = f"{url.hostname}:{url.port or 5672}"  # never print the password
    except ValueError:
        sys.exit("error: the port in KBM_LEDSAS_RABBITMQ_URL is not a number from 1 to 65535")
    try:
        connection = await aio_pika.connect(URL, timeout=10)
    except aio_pika.exceptions.ProbableAuthenticationError:
        sys.exit(f"error: the broker at {broker} refused the user name or "
                 "password in KBM_LEDSAS_RABBITMQ_URL")
    except (aio_pika.exceptions.AMQPConnectionError, OSError, asyncio.TimeoutError) as e:
        # Not running, wrong host or port, unknown virtual host, ...
        sys.exit(f"error: cannot connect to the broker at {broker} ({type(e).__name__})")

    message = {
        "envelope": {
            "schema_version": "1.0",
            "type": "command",
            "name": "ProcessCSV",  # Must match @app.handler("ProcessCSV")
            "message_version": "1.0",
            "message_id": str(uuid.uuid4()),
            "correlation_id": str(uuid.uuid4()),
            "idempotency_key": str(uuid.uuid4()),
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "trace_id": str(uuid.uuid4()),
        },
        "payload": {
            "csv_uri": "azblob://test-container/sample.csv",
        },
    }

    try:
        # Publisher confirms are on by default; on_return_raises=True makes a
        # command that no queue receives raise instead of vanishing.
        channel = await connection.channel(on_return_raises=True)
        # Passive lookup: fails if the service has never started on this broker.
        exchange = await channel.get_exchange(EXCHANGE, ensure=True)
        await exchange.publish(
            aio_pika.Message(
                json.dumps(message).encode("utf-8"),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="command",  # any non-empty key matches the queue's `#` bind
            mandatory=True,         # no queue bound: an error, not a silent drop
        )
    except (aio_pika.exceptions.ChannelNotFoundEntity, aio_pika.exceptions.DeliveryError) as e:
        # No exchange (404 NOT_FOUND: the service has never started on this
        # broker, or the name is wrong) or no queue bound to it.
        sys.exit(f"error: the command was not delivered ({type(e).__name__})")
    finally:
        await connection.close()
    print("command sent:", message["envelope"]["message_id"])


asyncio.run(main())
```

**Key Points:**
- Exchange: `cmd.{service_name}.v1` (TOPIC type), declared by the service
  when it starts: before the first start there is nothing to publish to.
- Routing key: any non-empty string (the queue binds with `#`); the
  examples use `"command"`.
- `envelope.name` MUST match the handler's command name (e.g.,
  `@app.handler("ProcessCSV")`).
- Publisher confirms, `on_return_raises=True` and `mandatory=True` make an
  undeliverable command fail the sender (exit code 1, one `error:` line)
  instead of vanishing; so do an unreachable broker and refused
  credentials.
- This sender does not wait for the reply. A caller that does declares its
  own reply exchange and queue and sets `reply_to` (see "Response Message
  Structure"). See `examples/hello_world_service/scripts/send_hello.py` for a runnable caller.

---

## ExecutionContext

The `ExecutionContext` (`ctx`) passed to every handler gives access to
message metadata, blob operations, status reporting and logging.

### Properties

Read-only. Each envelope field with a property is also
`ctx.envelope.<field>`.

| Property | Type | Equivalent | Notes |
|---|---|---|---|
| `ctx.message_id` | `str` | `ctx.envelope.message_id` | UUID for this specific message |
| `ctx.correlation_id` | `str` | `ctx.envelope.correlation_id` | Correlate related messages |
| `ctx.idempotency_key` | `str` | `ctx.envelope.idempotency_key` | Stable across retries |
| `ctx.trace_id` | `str` | `ctx.envelope.trace_id` | opaque distributed-tracing ID (URL-safe) |
| `ctx.deadline` | `datetime \| None` | `ctx.envelope.deadline` | Timezone-aware, as sent (the sender's UTC offset; compare with `datetime.now(timezone.utc)`) |
| `ctx.job_id` | `str \| None` | `ctx.envelope.job_id` | Business-level job id |
| `ctx.envelope` | `Envelope` | — | Full envelope object, read-only: assigning a field raises `pydantic.ValidationError` (use `ctx.envelope.model_copy(update={...})`) |
| `ctx.payload` | `dict` | — | Same dict the handler receives as `req` |
| `ctx.blob` | `BlobOperations` | — | See "BlobOperations" below |
| `ctx.logger` | `logging.Logger` | — | See **Logging** note |

Also on `ctx.envelope`: `name`, `message_version`, `sent_at`
(timezone-aware, as sent) and `trace_id`.

#### Logging note

`ctx.logger` is a stdlib `logging.Logger` (`handler.<envelope.name>`)
whose records propagate to your root logging configuration. The SDK
attaches only its secret masking (`ctx.logger`'s records are masked too,
see "Secrets in these records are masked") and otherwise does **not** configure it: without a logging
configuration of your own, `ctx.logger.info(...)` prints nothing (WARNING
and above come out in the SDK's format, see "When you configure no
logging yourself"). Configure the root logger once at startup, for
example with the SDK's JSON format:

```python
import logging
import kbm_ledsas_sdk

handler = logging.StreamHandler()
handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.captureWarnings(True)  # Python warnings as JSON records too
```

This writes your records as JSON whatever `KBM_LEDSAS_LOG_FORMAT` says; to
follow it, set this formatter instead when it is `text`:
`logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")`.

`kbm_ledsas_sdk.json_log_formatter()` returns the SDK's JSON formatter, so
your records come out in the SDK's shape **with** your `extra={...}`
fields (a plain percent-format formatter never shows them).
**Signature:** `json_log_formatter(datefmt: str | None = None)`. Without
`datefmt` the `timestamp` is UTC, ISO 8601 with milliseconds and a `Z`
(see "Timestamps"); a `datefmt` (`time.strftime` format) writes it in the
**local** time zone, as `logging.Formatter` does.

Structured fields go in `extra={...}`, **not as keyword arguments**:

```python
# ✓ correct
ctx.logger.info("Processing", extra={"correlation_id": ctx.correlation_id})

# ✗ raises TypeError (Logger._log() got an unexpected keyword argument)
#   whenever INFO is enabled: the command fails as an UnexpectedError, is
#   retried and then dead-lettered (with INFO off it is not noticed at all)
ctx.logger.info("Processing", correlation_id=ctx.correlation_id)
```

**Payload-field safety:** payload fields are caller-controlled and may
hold ANSI escape sequences, newlines or other control characters. Pass
them in `extra={...}` (the JSON formatter escapes control characters
there), never interpolated into the message, where they can move the
terminal cursor or forge log lines. Validate a value before you log it: a
mistyped blob URI may be an https URL with a SAS signature (a credential).
Log the parsed `BlobRef`'s `.uri` (it carries no query but `versionId`):

```python
from kbm_ledsas_sdk.models import BlobRef

# ✓ correct — validated first, then logged via extra (the JSON formatter
#   escapes \x1b etc. in extra values). An invalid value raises
#   ValueError: the SDK replies Permanent and quotes the value with its
#   query and user info redacted, so a SAS signature or a password is
#   not repeated.
input_ref = BlobRef.from_uri(req.get("input_uri", ""))
ctx.logger.info("Processing request",
                extra={"correlation_id": ctx.correlation_id,
                       "input_uri": input_ref.uri})

# ✗ wrong — payload field interpolated into message string, unvalidated
ctx.logger.info(f"Processing {req.get('input_uri')}",
                extra={"correlation_id": ctx.correlation_id})
```

#### Examples — `message_id` vs `idempotency_key` for output paths

Key output blob paths (and any write-once key) on `ctx.idempotency_key`,
which is stable across retries and DLQ replays of the same request, not
on `ctx.message_id`, a fresh UUID on every send.

```python
# Stable across retries / DLQ replays of the same logical request.
# overwrite=True is what makes a replay work: without it the second
# write to the same path fails with ResourceExistsError.
ref = await ctx.blob.upload_json(
    container="output",
    obj=result,
    path=f"result-{ctx.idempotency_key}.json",
    overwrite=True,
)
```

#### `deadline` example

```python
from datetime import datetime, timezone, timedelta

if ctx.deadline and datetime.now(timezone.utc) > ctx.deadline - timedelta(seconds=5):
    raise errors.DeadlineExceeded("Not enough time to process")
```

#### `job_id`

Both `job_id` and `correlation_id` are set by the caller and echoed back
exactly as sent: `correlation_id` matches a reply to its command, `job_id`
is your own business identifier (in replies, status updates and errors).
Optional; handlers that ignore it keep working.

```python
@app.handler("ProcessBatch")
async def handle(ctx, req: dict) -> dict:
    if ctx.job_id:
        ctx.logger.info("Processing job", extra={"job_id": ctx.job_id})
    return {"status": "success", "job_id": ctx.job_id}
```

### Methods

#### `async emit_status(stage: str | Enum, progress: float, note: str | Enum | None = None) -> None`

Report progress to the caller: a status message to the `reply_to`
exchange with routing key `status` (none without `reply_to`).
`emit_status` is a coroutine: call it as `await ctx.emit_status(...)`.
Without `await` nothing is sent; the only trace is Python's
`RuntimeWarning: coroutine 'ExecutionContext.emit_status' was never awaited`. Best-effort:
the caller receives it only if a queue is bound for `status` as well as
`response`; otherwise each update logs one WARNING
(`Failed to send status to reply_to exchange ...`) and the handler goes
on.

**Parameters:**
- `stage` (str): Current processing stage (e.g., "downloading", "processing", "uploading")
- `progress` (float): Completion fraction in `[0.0, 1.0]`
- `note` (str, optional): Human-readable status message

The configured secrets (see "Secrets in these records are masked") are masked in `stage` and
`note`; other text is sent as written.

An `Enum` member whose value is a `str` (`class Stage(Enum): DOWNLOADING
= "downloading"`) is accepted for `stage` and `note` and sent as that
value. A type checker reports an `IntEnum` member (or any `Enum` with
values that are not text) as an error, as the call refuses it.

**Raises** (before anything is published; uncaught, replied as
`Permanent`, not retried):
- `TypeError` if `stage` is not a `str`, or `note` is neither `None` nor a `str`
  (`emit_status() takes stage as a str, got int`; pass `str(n)` for a number).
- `ValueError` if `progress` is outside `[0.0, 1.0]`; `TypeError` if it
  is not a number (`True` and `False` too).
- `ValueError` if `stage` or `note` holds a lone surrogate (see
  "Payload": **Text**).

**Example:**
```python
await ctx.emit_status("downloading", 0.0, "Starting download")
await ctx.emit_status("processing", 0.5, "Halfway through CSV rows")
await ctx.emit_status("done", 1.0, "Processing complete")
```

---

## BlobOperations (8 APIs)

Access via `ctx.blob`.

**Await every method except `download_stream`.** (`ref = await
ctx.blob.upload_bytes(...)`) Without `await` nothing is written or read
and Python warns `coroutine ... was never awaited`. `download_stream`
returns an async iterator: read it with `async for`, do not await it.

**Stored content type and generated names.** `upload_json` stores the
blob with the content type `application/json`, `upload_text` `text/plain; charset=utf-8`,
`upload_bytes` and `upload_stream` the storage default
`application/octet-stream`. Without `path` the name is a random UUID with
a matching suffix: `<uuid>.json`, `<uuid>.txt`, or `<uuid>.bin` for bytes
and streams.

**Whole-blob downloads.** `download_bytes`, `download_text` and
`download_json` read the whole blob into memory, with no size limit; for
a large blob, or one whose size the caller controls, use
`download_stream`.

**Container allowlist.** With `KBM_LEDSAS_ALLOWED_CONTAINERS` set, every
method below also raises `errors.ContainerNotAllowed` for an unlisted
container before anything is sent (see "Container allowlist").

**Arguments common to every `upload_*` method:**
- `container` (str): a valid container name (see `BlobRef`).
- `path` (str, optional): the blob name. Omitted, `None` or `""`: a UUID
  name. Used as given (one leading `/` is ignored) and must fit the
  returned `azblob://` URI: at most 1024 characters (one leading `/` is
  not counted); no `?` or `#`; no control characters (C0, DEL and C1,
  `U+0080` to `U+009F`), Unicode format characters, line and paragraph
  separators (`U+2028`, `U+2029`) or a lone surrogate (`\ud800`, which
  UTF-8 cannot encode; a JSON request can carry one); no backslash (`\`; storage may
  take it as `/`, so the returned URI would not name the blob written:
  separate segments with `/`); no empty segment (`a//b`, or more than one
  leading `/`); no `.` or `..` segment; no `/` at the end (`dir/`). Any
  other character (letters, digits, `/`, `-`, `_`, `.`, spaces, `%`,
  non-ASCII letters) is accepted. An invalid path raises `ValueError`
  **before anything is written**.
- `overwrite` (bool, default `False`): `True` replaces an existing blob;
  `False` raises `azure.core.exceptions.ResourceExistsError`. Set `True`
  for the idempotency-key replay pattern (see "Examples — `message_id` vs
  `idempotency_key` for output paths" above). Any value
  but `True` or `False` (`"false"`, `"no"`, `0`, `None`) raises
  `TypeError`: `overwrite` is not `True` or `False` (`overwrite must be
  True or False, got str`). Nothing is written (convert text first, e.g.
  `overwrite=value.lower() == "true"`).

Uncaught, every `TypeError` / `ValueError` above is replied as
`Permanent` (not retried). Each upload returns a `BlobRef` (its `uri`;
`version_id` when the container has versioning enabled).

**Raises, every `download_*` method:**
- `ValueError` (a pydantic `ValidationError`): `blob_ref` is not a valid
  `azblob://<container>/<path>[?versionId=...]` URI (see `BlobRef`).
- `azure.core.exceptions.ResourceNotFoundError`: the blob (or the
  requested version) does not exist.
- `azure.core.exceptions.HttpResponseError`: other storage errors
  (authentication, throttling, server errors).

How each is replied when your handler lets it propagate is in the table
under "Error Handling". Catch them yourself to reply with your own
message.

### Bytes Operations (2)

#### `async upload_bytes(container: str, data: bytes | bytearray | memoryview, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload raw bytes (`bytes`, `bytearray` or `memoryview`; a `str` belongs
in `upload_text`).

**Example:**
```python
data = b"Hello, World!"
# Keyed on the idempotency key: a retried command writes the same path,
# so overwrite=True (without it the retry fails with ResourceExistsError).
ref = await ctx.blob.upload_bytes(
    container="results",
    data=data,
    path=f"messages/{ctx.idempotency_key}.bin",
    overwrite=True,
)
print(ref.uri)  # "azblob://results/messages/abc-123.bin"
```

#### `async download_bytes(blob_ref: str | BlobRef) -> bytes`
Download a blob as bytes.

```python
data = await ctx.blob.download_bytes("azblob://input/file.bin")
print(len(data))  # Size in bytes
```

### JSON Operations (2)

#### `async upload_json(container: str, obj: dict | list, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload a dict or list as JSON. A value `json.dumps` cannot write is
written as a reply writes it (see "Return value": a `datetime`, a `UUID`,
a `Decimal`, a `set`, a dataclass, a pydantic model, ...), so `upload_json`
takes what a handler may return.

**Raises:**
- `TypeError` (or `ValueError` for a circular reference): `obj` has no
  JSON form. Nothing is written; uncaught, one ERROR (no traceback,
  `upload_json() got an object with no JSON form (...)`) and a
  `Permanent` reply.
- `TypeError`: `obj` is not a dict or a list (a number, a string, `True`,
  `None`, a tuple, ...): `upload_json() takes a dict or a list (the root
  of the JSON document), got <type>; use upload_text() or upload_bytes()
  for other content`. `Permanent` when uncaught. Earlier releases
  uploaded such a value.

**Example:**
```python
result = {"status": "success", "rows_processed": 1000}
ref = await ctx.blob.upload_json(
    container="results",
    obj=result,
    path=f"reports/{ctx.idempotency_key}.json",
    overwrite=True,           # required for the idempotency replay pattern
)
```

#### `async download_json(blob_ref: str | BlobRef) -> dict | list`
Download and parse a JSON blob.

**Raises**, besides the download errors (each logged once as ERROR
`Failed to parse blob as JSON`, no traceback, and uncaught, replied as
`Permanent`, not retried):
- `json.JSONDecodeError` (a `ValueError`): not valid JSON; reply `blob
  content is not valid JSON (...)`.
- `UnicodeDecodeError`: not UTF-8.
- `ValueError` (an integer with more than 4300 digits) or
  `RecursionError` when it is nested more levels deep than the parser
  supports (about 1,000 levels on Python 3.11, about 10,000 on 3.13); reply `blob content
  cannot be parsed as JSON (...)`.
- `ValueError` for valid JSON whose root is not an object or an array:
  `Blob <uri> holds JSON whose root is <kind>: download_json() returns a
  dict or a list; use download_text() or download_bytes() for other
  content`, logged as ERROR `Blob content is JSON but its root is <kind>,
  not an object or an array`. Earlier releases returned such a value.

**Example:**
```python
from kbm_ledsas_sdk import errors

config = await ctx.blob.download_json("azblob://config/settings.json")
# download_json returns a dict or a list: check the shape before indexing
# (a type checker in strict mode requires it, too).
if not isinstance(config, dict):
    raise errors.Permanent("settings.json must be a JSON object")
print(config["max_retries"])  # 3
```

### Text Operations (2)

#### `async upload_text(container: str, text: str, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload a string. `TypeError` if `text` is not a string (use
`upload_json()` for a dict or list); `ValueError` for a lone surrogate.

**Example:**
```python
report = "Processing Summary\n" + "=" * 50 + "\n"
report += f"Rows: 1000\nErrors: 0\n"
# Keyed on the idempotency key, so overwrite=True (see upload_bytes).
ref = await ctx.blob.upload_text(
    container="reports",
    text=report,
    path=f"summaries/{ctx.idempotency_key}.txt",
    overwrite=True,
)
```

#### `async download_text(blob_ref: str | BlobRef) -> str`
Download a blob as UTF-8 text. Besides the download errors,
`UnicodeDecodeError` when it is not UTF-8 (`Permanent`, not retried).

```python
content = await ctx.blob.download_text("azblob://logs/app.log")
for line in content.split("\n"):
    if "ERROR" in line:
        print(line)
```

### Streaming Operations (2)

#### `async upload_stream(container: str, stream: AsyncIterable[bytes | bytearray | memoryview], path: str | None = None, progress_callback: Callable[[int], None] | None = None, overwrite: bool = False) -> BlobRef`
Upload from an async iterable (such as an async generator) of chunks, for
large files.

- `stream`: each chunk must be `bytes`, `bytearray` or `memoryview`;
  encode text first (`text.encode()`, or use `upload_text`).
- `progress_callback` (callable, optional): called with the cumulative
  bytes uploaded after each chunk.

**Raises:**
- `TypeError`: a chunk of another type
  (`upload_stream() got a chunk of type str; the stream must yield bytes`).
  A `str` chunk is refused too. The upload stops and the blob is not
  written; `Permanent` when uncaught.
- An exception the `stream` itself raises (your generator failed while
  reading its source) is passed on unchanged and the blob is not written:
  it is reported only as your handler's own exception, with no
  `Streaming upload failed` record (see "Uncaught exceptions").

**Example:**
```python
async def file_chunks():
    with open("large_file.bin", "rb") as f:
        while chunk := f.read(8192):
            yield chunk

ref = await ctx.blob.upload_stream(
    container="uploads",
    stream=file_chunks(),
    path="large_files/data.bin"
)
```

#### `download_stream(blob_ref: str | BlobRef, chunk_size: int = 4194304, progress_callback: Callable[[int, int], None] | None = None) -> AsyncIterator[bytes]`
Download as an async iterator of chunks, for large files. Not a
coroutine: read the iterator it returns with `async for` (no `await`).

- `chunk_size` (int, default `4194304` = 4 MiB, at least 1): every chunk
  is exactly `chunk_size` bytes except the last. One ranged request per
  chunk, so memory is bounded by `chunk_size`. Without a version, every
  request is pinned to the blob's ETag at the start: a blob overwritten
  mid-download fails the download instead of mixing contents. That failure (`azure.core.exceptions.ResourceModifiedError`, HTTP 412) is not a refusal
  of the request: uncaught, it is retried (one WARNING, `Transient
  error in handler: the blob changed while it was being downloaded ...`)
  and the new attempt reads the new content. To read one fixed content,
  pass a URI with `?versionId=...` (storage accounts with blob
  versioning).
- `progress_callback` (callable, optional): called with
  `(bytes_downloaded_so_far, total_bytes)` after each chunk.

**Raises:** the download errors; `ValueError` if `chunk_size` is not a
positive integer. Argument errors are raised at the first iteration.

**Example:**
```python
total_size = 0
async for chunk in ctx.blob.download_stream("azblob://large/file.bin"):
    total_size += len(chunk)
    # Process chunk...
print(f"Total: {total_size} bytes")
```

---

## Error Handling

A handler steers retries by raising `errors.Retryable`, `errors.Permanent`
or `errors.DeadlineExceeded`, the subclasses of `errors.SDKError`. Every
`errors.SDKError` has a `user_message` attribute (the caller-facing
message, or `None`); a bare `DeadlineExceeded()` has the message
`Processing deadline exceeded`. A bare `errors.SDKError` (or a subclass of
your own) is handled like any uncaught exception: retried, then replied as
`UnexpectedError`. `except errors.SDKError` catches only these classes,
not what the SDK's own calls raise:

| Raised by `ctx.blob` / `ctx.emit_status` | Exception class | If your handler does not catch it |
|---|---|---|
| An invalid argument (wrong type, a `progress` outside `[0.0, 1.0]`, an invalid container or path, text with a lone surrogate, ...) | `TypeError` or `ValueError` | `Permanent`, not retried |
| An invalid blob URI | `pydantic.ValidationError` (a `ValueError`) | `Permanent`, not retried |
| Content `download_json` / `download_text` cannot decode, or JSON whose root is not an object or an array (`download_json`) | `json.JSONDecodeError`, `UnicodeDecodeError`, `ValueError` (all `ValueError`), `RecursionError` | `Permanent`, not retried |
| `upload_json` given something other than a dict or a list | `TypeError` | `Permanent`, not retried |
| A container not in `KBM_LEDSAS_ALLOWED_CONTAINERS` (when it is set) | `errors.ContainerNotAllowed` (a `PermissionError`) | `Permanent`, not retried |
| A missing blob, an existing blob with `overwrite=False`, an authorization failure, another HTTP 4xx except 408 and 429 | `azure.core.exceptions.ResourceNotFoundError`, `ResourceExistsError`, `ClientAuthenticationError`, `HttpResponseError` | `Permanent`, not retried |
| Storage unreachable or not answering, HTTP 408, 429, 500, 502, 503, 504 | `azure.core.exceptions.ServiceRequestError`, `ServiceResponseError`, `HttpResponseError`, a Python `ConnectionError` | Retried, then `UnexpectedError` |
| Any other storage failure | `azure.core.exceptions.HttpResponseError` and others | Retried, then `UnexpectedError` |

Catch a storage exception yourself for another outcome:

```python
from azure.core.exceptions import ResourceNotFoundError
from kbm_ledsas_sdk import errors

@app.handler("ProcessData")
async def handle(ctx, req: dict):
    try:
        data = await ctx.blob.download_json(req["input_uri"])
    except ResourceNotFoundError:
        raise errors.Permanent("input blob not found", user_message="Input not found")
    try:
        result = compute(data)
    except errors.SDKError as e:
        # Retryable / Permanent / DeadlineExceeded raised by compute()
        metrics.incr("handler.sdk_error", tags={"type": type(e).__name__})
        raise
    return result
```

**Backend outages and concurrency.** Against unreachable storage a blob
call does not fail at once: the storage client retries it (2 retries,
about 3 s and 5 s apart), waits up to 10 s to connect and gives up an
attempt after 10 s of silence (not 10 s of transfer). One call fails
after about 8-10 s against a refused connection, and against storage
that does not answer at all (dropped packets, a stalled proxy) after
about 40 s (3 attempts of 10 s plus the backoff).
The handler is then retried like any unexpected error, so with the
default `KBM_LEDSAS_MAX_RETRIES=3` the single reply arrives after about
40-50 s (refused) or about 3 minutes (not answering), and the command is
dead-lettered. Each failed attempt logs one WARNING, `Transient error in
handler: blob storage not reachable (...)`, without a traceback (the
storage client's own `Failed to download blob (transient storage
error)` line, or its upload / stream counterpart, is INFO); if storage
comes back meanwhile the next attempt succeeds. Size your reply timeout
for this (allow 30 s or more), or set `KBM_LEDSAS_HANDLER_TIMEOUT` (or lower
`KBM_LEDSAS_MAX_RETRIES`). A retrying call holds one
`KBM_LEDSAS_CONCURRENCY` worker slot.

### Uncaught exceptions

An exception your handler does not catch is treated as
`UnexpectedError`: retried like `Retryable`, then dead-lettered with the
reply `{"code": "UnexpectedError", "message": "An unexpected error
occurred", "retryable": true}`. That covers every exception class, an
`asyncio.CancelledError` the handler raises and custom `BaseException`
subclasses included, so a command is always settled with its one reply.
An exception whose `str()` fails is reported by its class name. Each
attempt logs one WARNING (see "Log lines per failed command"):

- a transient storage or network error (storage unreachable, HTTP 408,
  429, 500, 502, 503 or 504, a Python `ConnectionError`, a blob
  overwritten during `download_stream`): `Transient error in handler:
  <reason>`, never with a traceback;
- any other exception: `Uncaught exception in handler`, with the class,
  message and innermost frame (inside a library, the library's line). The
  traceback is attached only on the command's **last** attempt in one
  service process (a later delivery, to another replica or after a
  restart or a lost connection, can log it again).

Any other storage failure (neither transient nor an HTTP 4xx: HTTP 501,
505, 507, a response cut off mid-transfer) is retried as `Uncaught
exception in handler`, and the storage client also logs its own ERROR
**with a traceback on every attempt**: `Failed to download blob`, `Failed
to upload blob`, `Streaming download failed` or `Streaming upload failed`.
An exception of the stream you pass to `upload_stream` is not a storage
failure (no `Streaming upload failed` record).

A `TimeoutError` raised inside your handler (your own `asyncio.wait_for`,
a client library's timeout) is retried like a handler timeout; its reply
carries code `Timeout` with the message `Handler raised TimeoutError`
(the SDK's own timeout says `Handler timed out after <N>s`), and each
attempt logs one WARNING `Handler raised TimeoutError` with `error_class`,
`error_message` and `error_location`.

An exception group (from `asyncio.TaskGroup`, for example) whose leaves
are all `errors.Permanent`, `errors.ContainerNotAllowed`,
`errors.DeadlineExceeded` or `errors.Retryable` is handled like its most
severe leaf (`Permanent` and `ContainerNotAllowed` first, then
`DeadlineExceeded`, then `Retryable`); a group with any other leaf is an
uncaught exception.

A handler that raises `SystemExit` (for example by calling `sys.exit()`)
or `KeyboardInterrupt` does not end the service: it is a `Permanent` error (one ERROR
`Non-retryable error in handler (not retried): the handler raised
SystemExit (3); a handler must not exit the process`, one reply,
dead-lettered). A process that is killed or crashes (out of memory,
`os._exit()`, native code) is different; see "Where the retry count
lives".

**Exceptions that can never succeed on a retry** are `Permanent` instead:
one ERROR `Non-retryable error in handler (not retried): <cause>`, no
traceback, no retries (decode failures of `download_json` /
`download_text` and objects `upload_json` cannot serialize also get the
blob call's own ERROR just before). They are: storage HTTP 4xx other than
408 and 429 (a missing blob, an invalid name, an authorization failure,
`ResourceExistsError`; not the HTTP 412 of `download_stream`, which is
retried); an invalid blob URI; content `download_json` /
`download_text` cannot decode; every invalid argument to an SDK call,
refused before anything is sent (a `blob_ref` that is neither a string nor
a `BlobRef`, such as `None`; `upload_bytes` given something other than
`bytes`, `bytearray` or `memoryview`; an `upload_*` `overwrite` that is
not `True` or `False`; `emit_status` with a `stage` that is not a `str` or
a `note` that is neither `None` nor a `str`; the others listed under
"BlobOperations" and `emit_status`); and a handler that turns out not to be async (a
callable object whose call returns a value instead of an awaitable). A
handler that **returns** something other than a JSON-serializable dict
(or `None`) is also `Permanent` (see "Payload").

**Secrets in these records are masked.** Exception text may quote a
credential. Every record the SDK writes masks the secrets listed below, in
either log format: the handler layer's (`kbm_ledsas_sdk.runtime.handler`),
every other SDK module's (`kbm_ledsas_sdk.*`, for example the storage
client's `Failed to download blob` and its traceback), and the AMQP
client library's (`aio_pika`, `aiormq`; see "Library log records"). A
record is masked in its message, in every text field (`error`,
`error_message`, `internal_message`, `reason`, `uri`, ...), in its
traceback and in its stack.

| Masked | Shown as |
|--------|----------|
| the user name and password of a URL of any scheme (everything before the `@` that ends them, also with an unencoded `#`, `?` or `/` in the password) | `amqp://***:***@host`, `https://***:***@host` |
| every query value and the fragment of a broker URL (`amqps://host/?password=***`) | `amqps://***:***@host:5671/?heartbeat=***` |
| `AccountKey=` / `account_key=`, `SharedAccessSignature=`, `SharedAccessKey=`, `Password=` / `Pwd=` values, quoted or not | `AccountKey="***"`, `'account_key': '***'` |
| a `Bearer` token | `Authorization: Bearer ***` |
| the `sig=`, `code=`, `access_token=` and `token=` URL query values | `***` |
| the configured secrets (the broker password of `KBM_LEDSAS_RABBITMQ_URL`, also percent-encoded; the `AccountKey` and SAS signature of `KBM_LEDSAS_BLOB_CONN_STRING`), each value of 6 characters or more, in any form, in every record, error reply, `user_message` and status text | `***` |

A dict, list, tuple or set field is masked in a copy, item by item (one
too deep or large to walk becomes `<withheld: too deeply nested or too
large to mask>`). Masking fails closed: text it cannot mask becomes
`<withheld: secret masking failed>` (a field), `<record text withheld:
secret masking failed>` (the message) or `<traceback withheld: secret
masking failed>`, and the record is still written. Other secrets (a token
without `Bearer`, an API key) are not recognised. `ctx.logger`'s records
are masked; your own loggers' are not, except the WARNING and higher
records the SDK writes when you configure no logging (see "When you
configure no logging yourself"). An error reply's internal message (sent
when no `user_message` is set and `KBM_LEDSAS_GENERIC_ERRORS` is off) is
masked the same way. The configured secrets are masked in a
`user_message` too; other text in it is sent as written. A handler's
result is sent as returned.

### `errors.Retryable`
A transient error: retried with backoff (see "Retry Mechanism"). For
network timeouts, rate limiting (HTTP 429), a service temporarily
unavailable.

**Constructor:**
```python
errors.Retryable(message: str, user_message: str | None = None)
```

- `message` (str): internal message (logged; sent to the caller unless
  `KBM_LEDSAS_GENERIC_ERRORS` is on)
- `user_message` (str, optional): the caller-facing message (see
  "Custom User Messages")

The caller sees it only in the one reply after the last retry, with the
command already dead-lettered: do not promise a further attempt ("will
retry", "retrying..."); say what failed and that the caller can resubmit
later. Each attempt logs one WARNING `Retryable error in handler` (with
`error_message`, no traceback).

**Example:**
```python
from kbm_ledsas_sdk import errors

try:
    data = await external_api_call()
except TimeoutError:
    # Basic usage
    raise errors.Retryable("External API timed out")

    # With custom user message
    raise errors.Retryable(
        "Internal: connection pool exhausted after 30s",
        user_message="Service temporarily unavailable; please resubmit later",
    )
```

### `errors.Permanent`
A non-recoverable error (invalid input, a missing field, a business rule):
one reply, dead-lettered at once, no retries. Logged once at ERROR
(`Permanent error in handler`, no traceback).

**Constructor:**
```python
errors.Permanent(message: str, user_message: str | None = None)
```

**Example:**
```python
if not req.get("dataset_uri"):
    # Basic usage
    raise errors.Permanent("Missing required field: dataset_uri")

    # With custom user message
    raise errors.Permanent(
        "Validation failed: dataset_uri is None, req keys: ['foo', 'bar']",
        user_message="Missing required field: dataset_uri",
    )
```

### `errors.DeadlineExceeded`
Processing cannot finish within the command's deadline: **not retried**
(a retry would miss it too), dead-lettered; the caller may resend with a
new deadline. Logged at WARNING.

**Constructor:**
```python
errors.DeadlineExceeded(message: str = "Processing deadline exceeded", user_message: str | None = None)
```

**Example:**
```python
from datetime import datetime, timezone, timedelta

if ctx.deadline:
    time_remaining = ctx.deadline - datetime.now(timezone.utc)
    if time_remaining < timedelta(seconds=10):
        raise errors.DeadlineExceeded(
            f"Only {int(time_remaining.total_seconds())}s remaining - need at least 10s",
            user_message="Request arrived too close to deadline",
        )
```

The SDK raises it too: for a command whose deadline has passed on
arrival. A deadline that passes while the handler runs cancels the
handler, and the reply is `DeadlineExceeded` (see "Timeout Enforcement").

### `errors.ContainerNotAllowed`
Raised by `ctx.blob` when `KBM_LEDSAS_ALLOWED_CONTAINERS` is set and an
operation names an unlisted container (see "Container allowlist"); a
handler may raise or re-raise it too. A `PermissionError`, not an
`errors.SDKError`.

```python
errors.ContainerNotAllowed(message: str, container: str)
```

- `e.container` holds the refused container name.
- Message: `Blob upload refused: container 'other' is not in
  KBM_LEDSAS_ALLOWED_CONTAINERS (allowed: input, results); nothing was
  sent to storage` (`Blob download refused: ...` for a download).
- **Not retried**, wherever it is raised. Uncaught, the caller gets one reply
  `{"code": "Permanent", "message": "container 'other' is not allowed
  (KBM_LEDSAS_ALLOWED_CONTAINERS)", "retryable": false}` (the generic
  `Processing failed` with `KBM_LEDSAS_GENERIC_ERRORS=true`); the allowed
  list is never sent to the caller.

Wrong-typed arguments to `@app.handler`, `@app.liveness_check` and
`@app.readiness_check` (a command name that is not a string, a timeout
that is not a number) raise `errors.ArgumentTypeError`, a `TypeError`
that is also a `ValueError`, so an existing `except ValueError` still
catches it.

Every error constructor refuses a `message` (and `ContainerNotAllowed` a
`container`) that is not a `str` with a `TypeError`
(`Permanent: message must be a str, got int`); raised in a handler, it is
replied as `Permanent` and not retried.

---

## Timeout Enforcement

### Configuration

```bash
export KBM_LEDSAS_HANDLER_TIMEOUT=1800  # 30 minutes (default)
export KBM_LEDSAS_HANDLER_TIMEOUT=0     # Disable timeout
```

### Behavior

When `KBM_LEDSAS_HANDLER_TIMEOUT` > 0, or the command has a `deadline`,
the handler runs inside `asyncio.timeout()`, in the same task, with the
smaller of the two as its limit. At the limit it is cancelled
(`asyncio.CancelledError` at its current `await`; let it propagate, see
"Cancellation Safety"):

- limit `KBM_LEDSAS_HANDLER_TIMEOUT`: retried like `Retryable` (one
  WARNING `Handler timeout`, with `timeout_seconds`, per attempt); the
  one reply, after the last retry, has code `Timeout` and the message
  `Handler timed out after <N>s`;
- limit the `deadline`: the reply is `DeadlineExceeded` at once
  (`Deadline <time> passed while the handler ran (after <N>s)`, not
  retried; one WARNING `Deadline exceeded in handler`).

A `TimeoutError` your code raises itself is retried like a timeout, with
the reply message `Handler raised TimeoutError` (see "Uncaught
exceptions").

**Blocking code is not timed out.** The limit can only stop a handler at an `await`. Blocking code in an `async def` handler (`time.sleep`, a
CPU-heavy loop, a synchronous client call) runs to its end, and a handler
that blocks past the limit and returns without another `await` succeeds.
While it blocks nothing else in the service runs: no other command, no
health probe (a liveness probe with a shorter timeout fails, and a
container orchestrator may restart the container), no heartbeat. A block
longer than about two heartbeat intervals (`KBM_LEDSAS_HEARTBEAT`, 60 s
by default) makes the broker close the connection: the command is
delivered again and the redelivery counts as an attempt toward
`KBM_LEDSAS_MAX_RETRIES`, so a handler that blocks that long on every
attempt ends in the DLQ (see "Broker connection lost while a command
runs"). After a block of 1 s or more the SDK logs one WARNING (at most
one a minute), `The event loop was blocked for at least <N> s: nothing
else in this service ran meanwhile ...`, naming the handlers that ran
during it (`<N>` is measured every 0.1 s, so it can be up to 0.1 s short
of the block, and a block a little under 1 s may be reported too). When the SDK's own reading of
a very large command body caused the block (see "Memory"), the WARNING
says so (`The SDK spent <M> s of it reading a command body of <n> bytes
...`). Run blocking work with `asyncio.to_thread(...)` and keep bodies
small.

### Work in threads and processes

Cancelling a handler stops only its coroutine. Work it started with
`asyncio.to_thread(...)` or in an executor **keeps running** after the
timeout: it uses CPU and memory for a result nobody receives, holds an
executor thread, and the retry starts a second copy beside it. At
shutdown the process exits only once such threads have finished (after
`ServiceApp shutdown complete`). A subprocess also keeps running unless the handler
stops it. For work that can outlast `KBM_LEDSAS_HANDLER_TIMEOUT`, make it
stoppable:

- **A flag the worker checks.** Pass a `threading.Event`, check it between
  steps, set it when the handler is cancelled:

  ```python
  import asyncio, threading

  @app.handler("Analyze")
  async def analyze(ctx, req: dict) -> dict:
      cancel = threading.Event()
      try:
          return await asyncio.to_thread(run_analysis, req, cancel)
      finally:
          cancel.set()  # the timeout (or a forced stop) cancelled us: the worker gives up

  def run_analysis(req: dict, cancel: threading.Event) -> dict:
      for step in steps(req):
          if cancel.is_set():
              raise RuntimeError("analysis cancelled")
          do(step)
      return {"status": "ok"}
  ```

- **A process you can stop.** Run the work with
  `asyncio.create_subprocess_exec` and `process.kill()` it in a `finally`
  block. A subprocess inherits the service's environment, including
  `KBM_LEDSAS_RABBITMQ_URL` and `KBM_LEDSAS_BLOB_CONN_STRING` (an account
  key grants the whole storage account); if the tool might log or pass it
  on, give it one without them:
  `env={k: v for k, v in os.environ.items() if not k.startswith("KBM_LEDSAS_")}`.
  This is hygiene, not isolation: a process running as the same user can
  still read the service's environment from the operating system.

Also bound the input first (for example an image's decoded size).

### Cancellation Safety

Make handlers cancellation-safe with `try/finally`:

```python
@app.handler("ProcessData")
async def handle(ctx, req: dict) -> dict:
    resource = None
    try:
        resource = await acquire_resource()
        # Long-running processing...
        result = await process_data(resource, req)
        return {"status": "success", "result": result}
    finally:
        # Always cleanup, even on cancellation
        if resource:
            await release_resource(resource)
```

### Checking Remaining Time

```python
@app.handler("ProcessData")
async def handle(ctx, req: dict) -> dict:
    if ctx.deadline:
        remaining = (ctx.deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining < 30:
            raise errors.DeadlineExceeded(f"Only {remaining:.0f}s remaining")

    # Proceed with processing...
```

---

## Retry Mechanism

### Configuration

```bash
export KBM_LEDSAS_MAX_RETRIES=3  # Default: 3 retries → 4 total attempts
```

### Behavior

When a handler raises `errors.Retryable`, times out, or raises an
uncaught exception (other than the non-retryable cases under "Error
Handling"):

1. If retry count < `KBM_LEDSAS_MAX_RETRIES`:
   - **No reply is sent** for this attempt.
   - Backoff: `min(2^retry_count + jitter, 60.0)` seconds (jitter ∈ [0, 1)).
   - One INFO line: `Retry k/N, waiting <delay>s before requeue (no
     reply for this attempt)` (the failure itself was logged once, at
     WARNING).
   - Wait (cut short by shutdown), then requeue the message and count the
     retry.
2. If retry count >= max retries:
   - Send the one reply (the error from this last attempt) when
     `reply_to` is set.
   - Send the message to the Dead Letter Queue (DLQ).
   - Log one ERROR, `Max retries (N) exceeded, sending to DLQ`, with the
     final `retry_count`.

**Shutdown during a backoff.** After SIGTERM or SIGINT a command waiting
out its backoff is requeued at once (another consumer, or this service
after its restart, retries it).

**The backoff wait holds a worker slot.** Until it is requeued, a command
in backoff keeps its `KBM_LEDSAS_CONCURRENCY` slot: with the default 4,
four commands in backoff delay a fifth by up to the longest wait (at most
60 s, from the 7th retry on). Raise `KBM_LEDSAS_CONCURRENCY` if you expect
many retries at once.

**Where the retry count lives.** In the memory of the service process,
keyed by `message_id` (a requeue redelivers the message unchanged), so it
starts again from zero on another replica or after a restart. A handler
that **crashes the process** (out-of-memory kill, `os._exit`, a segfault
in a native library; `SystemExit` and `KeyboardInterrupt` do not) never
reaches the requeue: the broker redelivers the message after the restart,
it crashes again, and the loop ends only when the message is removed. The
other commands the process held (up to `KBM_LEDSAS_PREFETCH`) are
delivered again with it after each restart and run again; one that runs longer than the time to
the crash never finishes. Validate sizes and formats before heavy native
processing, and monitor the restart count. A command redelivered after a
lost broker connection keeps the attempts it had used (see "Broker
connection lost while a command runs").

### Backoff Schedule

`retry_count` is 0-indexed (the first retry uses `retry_count=0`);
`jitter = random.uniform(0, 1)`.

| `retry_count` | Base (`2^retry_count`) | Actual delay range |
|-------|------------|----------------------|
| 0 | 1s | 1.0s – 2.0s |
| 1 | 2s | 2.0s – 3.0s |
| 2 | 4s | 4.0s – 5.0s |
| 3 | 8s | 8.0s – 9.0s |
| 4 | 16s | 16.0s – 17.0s |
| 5 | 32s | 32.0s – 33.0s |
| 6+ | 64s+ → clamped | 60.0s (cap) |

With the default `KBM_LEDSAS_MAX_RETRIES=3` only retry_counts 0–2 fire.

### Example

```python
@app.handler("ProcessData")
async def handle(ctx, req: dict) -> dict:
    try:
        result = await call_external_service(req["url"])
        return {"status": "success", "data": result}
    except ConnectionError:
        # Will be retried up to MAX_RETRIES times
        raise errors.Retryable(
            "External service connection failed",
            user_message="Service temporarily unavailable",
        )
    except ValueError:
        # Will NOT be retried - goes straight to DLQ. Say what failed in
        # your own words: an exception's text can be long or reveal
        # internals, and without KBM_LEDSAS_GENERIC_ERRORS it reaches the
        # caller.
        raise errors.Permanent(
            "External service returned data that is not valid",
            user_message="The request could not be processed",
        ) from None
```

---

## Generic Error Messages

### Configuration

```bash
export KBM_LEDSAS_GENERIC_ERRORS=true   # Hide internal details
export KBM_LEDSAS_GENERIC_ERRORS=false  # Show full details (default)
```

### Behavior

When `KBM_LEDSAS_GENERIC_ERRORS=true` and no `user_message` is set, the
caller sees:

| Error Code | Generic Message | Retry semantics |
|------------|-----------------|-----------------|
| `Retryable` | "Processing failed temporarily" | Retried with exponential backoff |
| `Permanent` | "Processing failed" | Dead-lettered, no retry |
| `Timeout` | "Processing timed out" | Retried — `KBM_LEDSAS_HANDLER_TIMEOUT` expired, or the handler raised `TimeoutError` |
| `DeadlineExceeded` | "Processing deadline exceeded" | Dead-lettered, no retry — the deadline had passed when the command arrived, or passed while the handler ran (resend with a new deadline) |
| `UnexpectedError` | "An unexpected error occurred" | Retried (treat as transient). The caller always sees this generic text, whatever the flag. |
| `HandlerNotFound` | "Handler not available" | Dead-lettered, no retry |

### Custom User Messages

The `user_message` parameter **always wins** when set, whatever
`KBM_LEDSAS_GENERIC_ERRORS` says; the flag only chooses between the
internal message and the generic one when it is **not** set.
`user_message` is a `str`, or `None` (the default: not set). Any other
type raises `TypeError` where the error is created
(`errors.Permanent("...", user_message=5)`: `user_message must be a str
or None, got int`). Raised in a handler, that `TypeError` is replied as
`Permanent` with that text (the generic message when
`KBM_LEDSAS_GENERIC_ERRORS` is true) and not retried, so the internal
message never reaches the caller. An empty string is a message too: the
caller then sees an empty message.

```python
# generic_errors=false (default):
raise errors.Retryable("DB pool exhausted")
# Caller sees: "DB pool exhausted"  (internal message)

raise errors.Retryable("DB pool exhausted", user_message="Service busy, try again later")
# Caller sees: "Service busy, try again later"   (user_message wins)

# generic_errors=true:
raise errors.Retryable("DB pool exhausted")
# Caller sees: "Processing failed temporarily"   (generic fallback)

raise errors.Retryable("DB pool exhausted", user_message="Service busy, try again later")
# Caller sees: "Service busy, try again later"   (user_message still wins)
```

For a retryable error the caller sees the message of the **last**
attempt.

### Logging

The internal `message` is always logged, whatever reaches the caller (the
`user_message` is not logged). `raise errors.Retryable("DB connection
pool exhausted after 30s timeout")` logs:

```json
{
  "timestamp": "2026-10-14T05:43:15.123Z",
  "level": "WARNING",
  "logger": "kbm_ledsas_sdk.runtime.handler",
  "message": "Retryable error in handler",
  "correlation_id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "command_name": "ProcessData",
  "error_message": "DB connection pool exhausted after 30s timeout",
  "service_name": "my-service"
}
```

`errors.Permanent` logs `"level": "ERROR"`, `"message": "Permanent error
in handler"` with the same `error_message`. An uncaught exception logs
one WARNING per attempt:

```json
{
  "timestamp": "2026-10-14T05:43:16.456Z",
  "level": "WARNING",
  "logger": "kbm_ledsas_sdk.runtime.handler",
  "message": "Uncaught exception in handler",
  "correlation_id": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
  "command_name": "ProcessData",
  "internal_message": "Unexpected error (KeyError): 'rows'",
  "error_class": "KeyError",
  "will_retry": true,
  "error_location": "main.py:42 in handle",
  "service_name": "my-service"
}
```

`error_location` is the file, line and function of the innermost frame
of the traceback (inside a library, the library's line; the last
attempt's traceback shows the whole path). `will_retry` is `false` on the
command's last attempt, whose record also carries the traceback in
`exception`. A `Transient error in handler: <reason>` record has the same
fields except `error_location`, and never a traceback.

---

## Logging

The SDK configures its own `kbm_ledsas_sdk.*` loggers when `app.run()`
starts, from `KBM_LEDSAS_LOG_LEVEL` (default `INFO`) and
`KBM_LEDSAS_LOG_FORMAT` (default `json`); there is no setup step.

```bash
export KBM_LEDSAS_LOG_LEVEL=DEBUG
export KBM_LEDSAS_LOG_FORMAT=text     # human-readable / colored
```

**Scope:** only the `kbm_ledsas_sdk.*` namespace. Your own loggers
(`__main__`, your modules, `handler.<command_name>` from `ctx.logger`)
use your root logging configuration.

**When you configure no logging yourself.** If the root logger has no
handler when `app.run()` starts, it replaces Python's last-resort handler
(`logging.lastResort`, which writes only a record's message) with one in `KBM_LEDSAS_LOG_FORMAT` on
stderr and routes Python warnings through logging
(`logging.captureWarnings(True)`). The records no handler takes (WARNING
and above: the event loop's `Task exception was never retrieved`, the
client libraries', your own loggers' without a handler, warnings from
`py.warnings`) then come out one line each in the SDK's format; your INFO
records still need a configuration of your own. A logger you gave a
handler of your own is written by that handler only (no record comes out
twice), and a later
`logging.basicConfig(...)` still takes effect. If you configure root
logging yourself, the SDK leaves it alone and does not capture warnings
(call `logging.captureWarnings(True)` yourself; the examples do).

**When the SDK's records start.** From `app.run()`: nothing is logged
while your module is imported. The handlers are listed at start
(`Registered handler for <name> v<version>`); with none registered the
service starts with one WARNING, `No command handler is registered: every
command this service receives is replied HandlerNotFound and
dead-lettered. Register handlers with @app.handler(...) in modules that
are imported before app.run()` (typically a module never imported).
`KBM_LEDSAS_LOG_LEVEL` applies as soon as the settings have been read and
checked (before `Starting <service> service`); while they are being read,
a WARNING about the settings themselves (the cleartext-AMQP acknowledgment) and a `Configuration error` found while reading them are written whatever `KBM_LEDSAS_LOG_LEVEL` says. Records your code writes before `app.run()`
use your own configuration; give it `json_log_formatter()` for one JSON
stream.

**Timestamps.** UTC, ISO 8601 with milliseconds and a `Z` suffix
(`2026-10-14T05:43:15.123Z`), in both formats.

**Text format.** `KBM_LEDSAS_LOG_FORMAT=text` writes one line per record:
`<timestamp> <LEVEL> <logger>: <message>`, then `[corr=...]` (the
command's `correlation_id`) and `[trace=...]` (the first 16 characters of
its `trace_id`) on every per-command record (JSON carries the whole
`trace_id`), then every
other field as `key=value` (the fields JSON carries, such as
`message_id`, `command_name`, `idempotency_key`, `job_id`,
`error_class`, `error_message`, `error_location`, `internal_message`,
`will_retry`, `uri`, `amqp_message_id`, `body_bytes`), and
`service_name=`. Values that are not a plain token are JSON strings.
Control characters (terminal escapes, carriage returns, newlines) and
Unicode format characters (such as `U+202E`) are written as escapes
(`\x1b`, `\n`), so caller text cannot recolor the terminal or forge a
line; a traceback follows its record, each line indented by `  | `. The
level name is colored when stderr is a terminal.

**The JSON formatter always writes valid JSON.** `json_log_formatter()`
writes one JSON line (RFC 8259) per record whatever `extra` holds: `NaN`
/ `Infinity` as the strings `"NaN"` / `"Infinity"` / `"-Infinity"`, a
field with no JSON form (tuple keys, a self-containing container) as its
`repr()`, a value whose own `__repr__` raises as `"<TypeName (repr
failed)>"`, a message whose `%` arguments fail as its template; a lone
surrogate is written as U+FFFD, as in error replies.

**Library log records.** The AMQP and storage client libraries log
through `aio_pika`, `aiormq` and `azure`, which the SDK caps at WARNING;
their records go to your root configuration (or the SDK's, when you
configured none). The AMQP library's records are masked like the SDK's
(see "Secrets in these records are masked"): a broker URL as
`amqps://***:***@host:5671/?heartbeat=***`. Expected-error tracebacks of
`aio_pika` / `aiormq` (the `ChannelNotFoundEntity` of a missing
`reply_to` exchange, the return of an unroutable reply) are suppressed:
you get the SDK's own ERROR line. Genuine connection and channel errors
still surface.

**The configuration does not print its credentials.** `repr()` of the
broker URL and storage connection string the SDK holds shows
`'**redacted**'` (also in `vars()` and `rich` views of `SDKConfig` and the
SDK's own transport attributes); `str()` and comparisons give the value.
The client-library objects the transport holds keep the credentials: do
not dump them (`rich.inspect(all=True)`, crash reporters that serialise
locals).

**Broker outages are SDK log events.** When the broker connection drops,
the SDK logs one WARNING (`Lost the connection to the AMQP broker (...);
reconnecting automatically ...`) and, once the client has reconnected,
one INFO (`Reconnected to the AMQP broker; consuming commands again`).
`/ready` answers 503 in between, and unacknowledged commands are
redelivered by the broker. A broker or network path that stops answering
without closing the connection is
found by the AMQP heartbeat (`KBM_LEDSAS_HEARTBEAT`, 60 s by default) after `3 × (interval + 1)`
seconds (183 s at the default, plus up to half an interval): `Lost the connection to the AMQP broker (no
frames from the broker within the heartbeat timeout, about 183 s);
reconnecting automatically ...` (`error_class` `HeartbeatTimeout`), and a
reply this close cancelled logs `Reply to exchange '<name>' not sent: the
connection to the AMQP broker was lost (ConnectionLost)`. The client
library's own records of the close and its reconnect attempts are
WARNINGs too (the SDK lowers its `Unexpected connection close from remote
..., CONNECTION_FORCED ...` ERROR). A routine broker restart therefore
produces no ERROR record. An abrupt loss (the broker killed or crashed, the
TCP connection cut) is reported the same way: the library's
`Cancelling cause reader exited abnormally` record is one WARNING naming
the cause (`(AMQPConnectionError: Server connection unexpectedly closed
...)`), without a traceback; a reader error that is not a lost connection
(a malformed frame) keeps its traceback.

**Broker connection lost while a command runs.** A delivery of a lost
connection cannot be acknowledged or answered on it. The SDK does not
dead-letter the command or log a traceback:

- If the client has reconnected and the broker delivers the command again
  to this instance while it is still in progress here (running, or
  waiting for a worker), the redelivery is handed to that command, whose
  outcome (the one reply, then the ACK, requeue or dead-letter) settles
  it, so it runs once. One INFO line (`Redelivered command arrived while
  it is still in progress here ...`); the `AMQP consumer stopped` INFO at
  shutdown reports the total as `redeliveries_adopted`. Another instance
  may receive it instead, and runs it too.
- Otherwise the SDK logs one WARNING and the broker delivers the command
  again (after a broker restart, only a persistent one), and it runs
  again: `Reply to exchange '<name>' not sent: the connection to the AMQP
  broker was lost ...` when the reply could not be published, else `ACK
  not sent: ...` (after a success, also one whose reply went out on the
  restored connection), `Requeue not sent: ...` or `Dead-letter not sent:
  ...`.

A redelivery after a lost connection counts as one more attempt toward
`KBM_LEDSAS_MAX_RETRIES`. When the attempts are spent (with the default
`3`, the fourth redelivery), that delivery is not run: one reply
`{"code": "UnexpectedError", "message": "An unexpected error occurred", "retryable": true}`,
the command is dead-lettered, and one ERROR `Max retries (N) exceeded,
sending to DLQ: command <name> was delivered again after <n> attempt(s)
that did not finish, the last one(s) because the connection to the AMQP
broker was lost while the command ran; it is not run again. A handler
that blocks the event loop for longer than about two heartbeat intervals
(KBM_LEDSAS_HEARTBEAT) loses the connection: run blocking work with
asyncio.to_thread`. The count is kept in the service process, keyed on
the command's `message_id` (see "Where the retry count lives").

**The broker's consumer_timeout closed the channel.** A command left
unacknowledged longer than the broker's `consumer_timeout` (see "Handler
Timeout") is taken back: the broker closes the consumer channel (the
connection stays up) and delivers every command unacknowledged on it
again. One WARNING names the cause (`The AMQP broker closed the consumer
channel: a command stayed unacknowledged longer than the broker's
consumer_timeout. The broker delivers the N command(s) in progress here
again: ...`, with the broker's reason in `error`, `in_progress` and
`message_ids`); the library's `Got frame for closed channel ...` records
are WARNINGs. The commands in progress end as in the list above: a
redelivery reaching this instance is handed over (the INFO says `the
broker closed the channel of its first delivery at its
consumer_timeout`), and a settlement that can no longer be sent logs one
WARNING `ACK not sent: the AMQP broker closed the channel of this
command's delivery at its consumer_timeout while the command was in
progress ...` (or `Requeue not sent` / `Dead-letter not sent`), `reason`
`consumer_timeout`, and runs again on its redelivery. While every run
outlasts the `consumer_timeout` this repeats without end.

So a handler may run twice for one command, and its caller can receive
two replies; keep handlers idempotent (key writes on
`ctx.idempotency_key`).

**Shutdown.** On SIGTERM or SIGINT the service stops taking new commands
at once: `/ready` answers `503` from that moment (`/health` stays `200`
while the running handlers finish), it stops receiving from the broker
(one INFO line, `AMQP consumer no longer receives new commands`), and it
gives back at once every command it has taken but not started, without a
reply or a counted retry (one INFO at the end of the drain, `Gave back N
command(s) without running them (shutdown requested); the broker delivers
them again: ...`). The service then waits only for the handlers already
running (shutdown does not cap them; `KBM_LEDSAS_HANDLER_TIMEOUT` does),
so each can reply and acknowledge.
From then on every broker operation (publishes, ACK / requeue /
dead-letter, consumer cancel, connection close) gets at most 5 s; once
the broker is found unreachable, the rest are skipped, the connection is
closed locally, and one WARNING says so (`AMQP broker not reachable during
shutdown ...`, naming the cause). A command whose reply was not confirmed,
or that could not be settled, is left unacknowledged: the broker delivers
it again and it runs again (a stalled broker may still deliver the
unconfirmed reply, so the caller can get it twice). An acknowledgement,
requeue or dead-letter gets no answer from the broker, so one sent after
the stop signal is written at once and logged (`ACKed command ...`), but a
broker that had stopped answering (frozen rather than disconnected) may
never read it; the WARNING counts them (`N
acknowledgement(s), requeue(s) or dead-letter(s) were sent after the stop
signal; the broker does not answer them, so they may not have reached
it, ...`). Once the broker is found unreachable, the handlers still
running are cancelled, as by a second stop signal (below) (INFO `Replies and acknowledgements can no
longer be sent while stopping (the connection was lost or does not
answer): N running command(s) cancelled; the broker delivers them again:
<name> <message_id>, ...`). A connection already lost at the stop signal
counts as unreachable from the start: the running handlers are cancelled at
once.

A **second** SIGTERM or SIGINT (Ctrl+C pressed again) stops at once:
running handlers are cancelled (one WARNING, `Received signal 2 again: stopping now. N running
command(s) cancelled and given back without a reply; the broker delivers
them again.`) and their commands are given back without a reply or a
counted retry. A command whose handler has already returned is not
cancelled: it still sends its one reply and is acknowledged or
dead-lettered (each step at most 5 s). Threads a handler
started are not stopped (see "Work in threads and processes").

A signal **while the service is still starting** stops the start at once,
logged at INFO with no traceback: `Received signal <n> while starting:
stopping the start`, then the transport closes what it had opened (INFO
`DirectTransport start cancelled; stopping`, `Stopping DirectTransport`,
`Azure Blob client closed`; the connection being made does not keep
reconnecting), and the process exits with code 0 after INFO `Stopped
before the service started ...`, instead of waiting up to
`KBM_LEDSAS_CONNECT_TIMEOUT`.

**Shutdown in containers.** A container runtime sends SIGTERM, waits a
grace period, then sends SIGKILL. An idle service exits within about a
second; a busy one waits for its handlers, up to
`KBM_LEDSAS_HANDLER_TIMEOUT`. A command still running at the SIGKILL gets
no reply and runs again after the restart. Set the grace period above the
longest handler run plus about 10 s: `docker stop -t <seconds>` (Docker's
default is 10 s), `stop_grace_period:` in Compose,
`terminationGracePeriodSeconds` in Kubernetes (30 s by default); or lower
`KBM_LEDSAS_HANDLER_TIMEOUT`.

**Log lines per failed command.**

| Failure | Handler-layer records | Then |
|---------|-----------------------|------|
| non-retryable (`Permanent`, `HandlerNotFound`, an invalid return value, the cases under "Uncaught exceptions") | one ERROR | INFO "sending to DLQ" |
| `DeadlineExceeded` | one WARNING (two, `Deadline already exceeded` then `Deadline exceeded in handler`, when it had passed on arrival) | INFO "sending to DLQ" |
| retryable | one WARNING per attempt (`Retryable error in handler`, `Handler timeout`, `Handler raised TimeoutError`, `Transient error in handler: ...` or `Uncaught exception in handler`, only the last with a traceback) and an INFO `Retry k/N, waiting ...` before each retry | one ERROR `Max retries (N) exceeded, sending to DLQ`, logged once the dead-letter has been sent |

Blob calls add their own records, per attempt:

- content `download_json` / `download_text` cannot decode, or an object
  `upload_json` cannot serialize: one ERROR from the blob call;
- a storage failure neither transient nor HTTP 4xx: the storage client's
  ERROR with a traceback (see "Uncaught exceptions");
- a storage refusal: one WARNING from `kbm_ledsas_sdk.blob.azure_client`
  (no traceback) before the handler layer's line: `Blob not found` or
  `Container not found` (with `container`, `path`, `version_id` and
  `error_code` `BlobNotFound` / `ContainerNotFound`) for a download, else
  `Failed to download blob (client error)` / `Failed to upload blob
  (client error)` / `Streaming download failed (client error)` /
  `Streaming upload failed (client error)` with the short storage error
  (`HTTP 404 ContainerNotFound ...`);
- an upload refused because the blob already exists (`overwrite=False`,
  HTTP 409 `BlobAlreadyExists`) gets one INFO line instead,
  `Blob already exists (overwrite=False)` (`Blob already exists (streaming
  upload, overwrite=False)`), with `container` and `path`;
- an account key that cannot sign requests (normally refused at start):
  one WARNING `Failed to download blob: the storage credentials cannot
  sign requests (...)`, and the command fails `Permanent`;
- a transient storage failure: one INFO `... (transient storage error)`.

**Attempt fields.** Every per-attempt WARNING carries `attempt` (1 for
the first run) and `will_retry`; the `Max retries (N) exceeded` ERROR
carries `error_class`, `error_message` and `attempt`.

**What to alert on.**

- the `Max retries (N) exceeded` ERROR: one per command dead-lettered after
  its retries, including one ended after lost connections; it is logged
  once the dead-letter has been sent: when the dead-letter cannot be sent
  (lost connection, unreachable broker at shutdown), neither is this line;
  a WARNING says so and the last attempt runs again on the redelivery;
- the ERROR saying the broker cancelled the command consumer (see
  "Default checks");
- the ERROR lines of non-retryable failures (or the WARNINGs, to see
  failed attempts).

An `Uncaught exception in handler` record is never an ERROR (it was before
v0.3.6). A broker restart the SDK recovers from logs WARNING and INFO
records only. The storage client's ERROR lines come once per attempt.

Every record of the handler layer about a command (`No handler for <name>
v<version>`, `Permanent error in handler`, `Retryable error in handler`,
`Uncaught exception in handler`, `Transient error in handler`,
`Non-retryable error in handler (not retried)`, `Handler timeout`,
`Handler raised TimeoutError`, `Deadline already exceeded ...`, `Deadline
exceeded in handler ...`, `Handler returned an invalid result ...`) and
the SDK's per-command records (`Received command`, `Handling command
<name>`, `Sent response for <name>`, `ACKed command <name>`, `Max retries
(N) exceeded`, `Command failed with a non-retryable error, sending to
DLQ`, `Retry k/N ...`, `Reply publish failed for <name> ...`, `Failed to
send response to reply_to exchange ...`, `Failed to send status to
reply_to exchange ...`, `Acknowledging message`, `Rejecting message`,
`Failed to ACK message`, `Failed to NACK message`, the `not sent: the
connection to the AMQP broker was lost`, `not sent: the AMQP broker closed
the channel` and `not sent: AMQP broker not reachable during shutdown`
records, and the `Duplicate message_id while original still pending;
rejecting duplicate to DLQ` and `pending_messages dict at cap; rejecting
new message to DLQ ...` WARNINGs) carry the command's identity: its
`correlation_id`, `message_id`, `command_name`, `trace_id` and
`idempotency_key`, and its `job_id` when it has one, so an alert can be
joined to the dead-lettered message. `Cannot ACK unknown message` and `Cannot NACK unknown message`
carry only the `message_id`.

**Reply and status failures.** A reply that cannot be sent (missing
`reply_to` exchange, or no queue bound for `response`) logs one ERROR
`Failed to send response to reply_to exchange ...` (for a missing
exchange ending `verify the caller declared this exchange and bound a
queue to it for routing key 'response'.`) and the command is
dead-lettered; a status update that cannot be sent logs one WARNING
`Failed to send status to reply_to exchange ...` and the handler goes on.
Running counts since start are fields of these records:
`reply_publish_failures` and `reply_unroutable_failures` on the ERROR,
`status_publish_failures` on the WARNING. There is no metrics endpoint.

**A queue that already exists with other settings.** If a queue of the
service exists with other arguments
(typically a queue declared by hand or by another
tool with other limits, or kept
in a reused local broker volume), the start fails with one ERROR naming the queue and the
fix (delete it once, draining it first if needed; the service re-creates
it), then the closing line below.

**How a failed start ends.** A start that fails while connecting (no
answer, refused credentials, a queue with other settings, an unknown
virtual host) logs one ERROR naming the cause, then
`ServiceApp exiting (startup failure already reported above). Reason class: <class>`,
and exits with code 1. So a failed start writes two ERROR records, with
no traceback; alert on the first and the exit code, not the class name.
A failed start is not retried: run the service under a restart policy
(`docker run --restart on-failure`, Compose `restart: on-failure`,
Kubernetes `restartPolicy`). Refused credentials:
`AMQP authentication refused by the broker at
amqp://***:***@<host>:<port>/: check the user name and password in
KBM_LEDSAS_RABBITMQ_URL and the user's access to the virtual host. Broker
reply: ...`; a host name that does not resolve: `... the host name
'<host>' does not resolve (...); check the host in KBM_LEDSAS_RABBITMQ_URL
and the DNS of the network the service runs in`. A setting the SDK
refuses writes one `Configuration error` line, its only ERROR record (exit code
1, no closing line), mostly before any other record; a missing or
unusable `KBM_LEDSAS_RABBITMQ_URL` or `KBM_LEDSAS_BLOB_CONN_STRING` is
reported (naming every missing one) after `Starting <service> service`
and `Registered handler for ...`.

---

## Supporting Types

### `BlobRef`
Reference to a blob.

**Properties:**
- `uri` (str): Full blob URI (e.g., "azblob://container/path")
- `container` (str): Container name (derived from `uri`)
- `path` (str): Blob path within container (derived from `uri`)
- `version_id` (Optional[str]): from the `?versionId=` query, else
  `None`; the parameter name matches in any letter case (`?versionid=`
  too).

**Class Methods:**
- `BlobRef.from_uri(uri: str) -> BlobRef`: Parse URI string into BlobRef

**Validation.** `BlobRef.from_uri()` (and every `ctx.blob.download_*`
call given a string) raises `ValueError` (a pydantic `ValidationError`)
unless the URI is `azblob://<container>/<path>[?versionId=<version>]`. It
rejects:

- a container name Azure would refuse: a container name is 3 to 63
  characters: lowercase letters, digits and single hyphens, starting and
  ending with a letter or digit (`$root`,
  `$logs`, `$web` accepted). The same rule applies to the `container` of
  every `ctx.blob.upload_*` call;
- user info or a port (`azblob://user:pw@c/...`);
- any query parameter but `versionId` (a SAS `sig`, for example), so
  nothing else rides along in `.uri`, which the SDK logs;
- a `versionId` that, percent-decoded, holds anything but letters,
  digits, `:`, `.`, `-` and `_`, or more than 128 characters (Azure
  version ids are timestamps such as `2026-07-21T10:00:00.1234567Z`), so
  `1;sig=...` or `%0a` is refused;
- a `#` fragment, and a `?` with no `versionId`;
- a `.` or `..` path segment, and URIs over 2048 characters;
- a control character (C0, DEL and the C1 range `U+0080` to `U+009F`), a
  Unicode format character, a line or paragraph separator (`U+2028`,
  `U+2029`; every character the text log format escapes is among them) or a lone surrogate (`\ud800`, which UTF-8 cannot encode, so no blob can have it in its name), the same rule as for upload paths;
- a backslash (`\`) in the blob name, as for upload paths: storage may read
  `a\b` as `a/b`, so the blob read would not be the one `.path` names;
- a URI that does not start with exactly `azblob://` (the message says
  what is wrong: `AZBLOB://`, leading whitespace, a missing `//`);
- an empty blob name (`azblob://data/`, `azblob://data//`), an empty
  segment (`azblob://data//x.json`, `azblob://data/a//b.json`) and a name
  ending in `/` (`azblob://data/dir/`);
- any constructor argument but `uri`: `BlobRef(uri=..., version_id="v1")`
  raises. To pin a version, put it in the URI (`?versionId=<id>`).

Messages never quote the query, fragment or user info of the rejected
value (they show `scheme://host/path?<redacted>`); `str(error)`,
`errors()` and `json()` do not include the input (`input` is `<not kept:
a rejected blob URI may hold a secret>`), also for an assignment and for
a `BlobRef` field of your own model.

A `BlobRef` is immutable (assigning raises a `ValidationError`); build a
new one with `BlobRef.from_uri(...)`. `ref.model_copy(update={"uri":
...})`, `copy.replace(ref, uri=...)`, `BlobRef.model_construct(uri=...)`
and `BlobRef.model_validate(ref)` check the URI the same way, and every
`ctx.blob` call checks a `BlobRef` again before it uses or logs its URI, so
a `BlobRef` the SDK reads holds a URI that passed these checks.

**The blob name is literal.** The part after `azblob://<container>/` is
the blob's name exactly as written: it is not percent-decoded.
`azblob://data/a b.json` names `a b.json`, and `azblob://data/a%20b.json`
names another blob, `a%20b.json`. Only the `versionId` value is
percent-decoded. Upload calls
return names the same way (`azblob://data/a b.bin`,
`azblob://data/café/ü.bin`). Pass URIs on as plain strings: a URI library
that percent-encodes or normalizes them (.NET `System.Uri`, JavaScript
`encodeURI`, Java `java.net.URI`) names another blob.

**Example:**
```python
from kbm_ledsas_sdk.models import BlobRef

# Parse URI
ref = BlobRef.from_uri("azblob://data/input.json")
print(ref.container)  # "data"
print(ref.path)        # "/input.json" (leading slash; use ref.path.lstrip("/")
                       #  for a container-relative blob name)

# A versioned URI keeps the query out of the path
ref = BlobRef.from_uri("azblob://data/input.json?versionId=2026-07-21T10:00:00.1234567Z")
print(ref.path)        # "/input.json"
print(ref.version_id)  # "2026-07-21T10:00:00.1234567Z"

# Use in download
data = await ctx.blob.download_json(ref)
```

---

## Complete Example: Using All APIs

The example reads `azblob://config/settings.json` and
`azblob://docs/README.txt` and writes to the containers `results` and
`logs`. Uploads do not create containers, so create them and the two
blobs first. Against the local stack (`examples/hello_world_service/deploy/local/`), with
`KBM_LEDSAS_BLOB_CONN_STRING` set as for the service:

```python
import os
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

client = BlobServiceClient.from_connection_string(os.environ["KBM_LEDSAS_BLOB_CONN_STRING"])
for name in ("config", "docs", "results", "logs"):
    try:
        client.create_container(name)
    except ResourceExistsError:
        pass  # created by an earlier run
client.get_blob_client("config", "settings.json").upload_blob(b'{"mode": "demo"}', overwrite=True)
client.get_blob_client("docs", "README.txt").upload_blob(b"Example notes\n", overwrite=True)
```

Then send it a `ProcessData` command whose `input_uri` names a CSV, for
example `azblob://test-container/sample.csv` (upload one first). It reports progress with `emit_status`: bind the reply queue to
the `status` routing key as well as `response` to
receive the updates.

```python
import logging
from datetime import datetime, timezone, timedelta

import kbm_ledsas_sdk
from kbm_ledsas_sdk import ServiceApp, errors
from kbm_ledsas_sdk.models import BlobRef

# ctx.logger is a plain stdlib logger: configure logging once, or its
# lines go nowhere. json_log_formatter() also prints the extra= fields.
_handler = logging.StreamHandler()
_handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])

app = ServiceApp(service_name="complete-example")

@app.handler("ProcessData")  # Handler for "ProcessData" commands
async def handle(ctx, req: dict) -> dict:
    """Complete example showing the full API surface."""
    # Structured fields go in extra={...}: a keyword argument such as
    # correlation_id=... raises TypeError in the stdlib logger whenever the
    # level is enabled, which fails the command (retried, then dead-lettered).
    ctx.logger.info(
        "Starting",
        extra={
            "message_id": ctx.message_id,
            "correlation_id": ctx.correlation_id,
            "idempotency_key": ctx.idempotency_key,
        },
    )

    if ctx.deadline and datetime.now(timezone.utc) > ctx.deadline - timedelta(seconds=5):
        raise errors.DeadlineExceeded("Not enough time remaining")

    await ctx.emit_status("downloading", 0.0, "Starting download")

    # A missing or invalid input_uri is the caller's error (Permanent, one
    # reply, not retried). The reply never repeats the value (a mistyped
    # value may be a URL with a SAS signature).
    try:
        input_ref = BlobRef.from_uri(req.get("input_uri", ""))
    except ValueError:
        raise errors.Permanent(
            "input_uri missing or not an azblob:// URI",
            user_message="input_uri must be an azblob://<container>/<path> URI",
        ) from None

    # A missing blob, content that is not JSON or an invalid upload path
    # are reported by the SDK itself (Permanent, one line): no need to
    # catch them (see "Error Handling").
    try:
        data_bytes = await ctx.blob.download_bytes(input_ref)
        config = await ctx.blob.download_json("azblob://config/settings.json")
        if not isinstance(config, dict):  # a dict or a list: check before indexing
            raise errors.Permanent("settings.json must be a JSON object")
        readme = await ctx.blob.download_text("azblob://docs/README.txt")

        # The CSV's data rows: its lines without the header line.
        await ctx.emit_status("processing", 0.5, f"Processing {len(data_bytes)} bytes")
        rows = max(len(data_bytes.splitlines()) - 1, 0)

        # Key the paths on idempotency_key and pass overwrite=True, so a
        # retry or DLQ replay rewrites the same blob instead of failing.
        result = {"rows": rows, "options": req.get("options", {})}
        result_ref = await ctx.blob.upload_json(
            container="results",
            obj=result,
            path=f"output/{ctx.idempotency_key}.json",
            overwrite=True,
        )
        await ctx.blob.upload_text(
            container="logs",
            text=f"Processed {rows} rows successfully",
            path=f"summaries/{ctx.idempotency_key}.txt",
            overwrite=True,
        )

        await ctx.emit_status("done", 1.0, "Complete")
        return {
            "result_uri": result_ref.uri,
            "rows_processed": rows,
            "status": "success",
        }

    except TimeoutError:
        raise errors.Retryable("Network timeout") from None

if __name__ == "__main__":
    app.run()
```

---

## Health Endpoints

The SDK serves liveness and readiness HTTP endpoints next to the message
consumer, for any HTTP probe.

### Endpoints

| Path | Purpose | Aliases |
|------|---------|---------|
| `GET /health` | Liveness — is the process alive? | `/livez` |
| `GET /ready`  | Readiness — should the instance be reported ready? | `/readyz` |

Readiness only decides what `/ready` answers; it does not stop command
delivery (the service consumes commands while `/ready` answers `503`).
Do warm-up work (loading a model, ...) **before** `app.run()`.

Both return JSON: `200` when all checks pass, `503` when any fail. Each
path answers `GET` and `HEAD`; another method gets a JSON `405`
(`{"status": "error", "code": 405, "message": "Method Not Allowed",
"allowed": ["GET", "HEAD"]}`), another path a JSON `404`, and a request
the server cannot parse (an over-long line or header, a bad
`Content-Length`) a JSON `400` that does not repeat the request (one DEBUG
line). No response carries a `Server` header. The default body names no
service, version or check, so a `/health` exposed on `0.0.0.0` does not
fingerprint the service:

```json
{ "status": "healthy" }
```

A failing default-mode response names the failing checks:

```json
{ "status": "unhealthy", "failed": ["db_alive"] }
```

With `KBM_LEDSAS_HEALTH_VERBOSE=true` (for development):

```json
{
  "status": "healthy",
  "service": "csv-processor",
  "version": "0.3.6",
  "checks": {
    "process": "healthy",
    "transport": "healthy",
    "db_alive": "healthy"
  }
}
```

A failing check appears as `"unhealthy: <reason>"`. Each endpoint lists
only its own checks: `/health` (and `/livez`) `process` plus your
`@app.liveness_check`s, `/ready` (and `/readyz`) `transport` plus your
`@app.readiness_check`s.

### Deployment id

When `KBM_LEDSAS_DEPLOYMENT_ID` is set, its value is added as
`deployment_id` to **every** `/health` and `/ready` body, minimal and
verbose. `KBM_LASTCOMMITID` is an older alias of
`KBM_LEDSAS_DEPLOYMENT_ID`, read only when that is unset or blank.
The SDK sets neither; set one at deploy time, for example to the build's
commit id. Surrounding whitespace and a matched pair of quotes are removed:

```json
{ "status": "healthy", "deployment_id": "a1b2c3d4" }
```

The key is **omitted** when both are unset or blank, so treat
`deployment_id` as an optional field.

### Default checks

- **Liveness**: `process` — `healthy` whenever the event loop can serve
  the request.
- **Readiness**: `transport` — healthy while the broker connection is up:
  connected, not reconnecting, consumer channel open. During a broker
  outage `/ready` returns `503` (`transport not connected`) until the SDK
  has reconnected by itself. When the broker cancels the service's command
  consumer while the connection stays up (its queue was deleted, or the node
  holding it is down), `/ready` returns `503` too and the SDK logs one
  ERROR `The AMQP broker cancelled the command consumer on <queue> (the
  queue was deleted, or the node that holds it is down): no command
  arrives, and /ready answers 503. Declaring the queue again and
  consuming again.`, then a WARNING `Could not consume from <queue> again
  (<cause>); retrying in N s` per failed try and one INFO `Consuming
  commands from <queue> again` on success. From the first stop signal on
  it returns `503` while the running handlers finish (the verbose body
  shows `shutting down`; the minimal body lists `transport`, as for an
  outage); after the drain the health server stops and probes are refused. Blob
  storage is **not** probed (a
  storage outage shows as failing handlers); add a readiness check if you
  need that.

### Custom checks

Two decorators register checks: functions that take no arguments, sync
or async, and return truthy for healthy. Registering something that is not
callable, or a function that cannot be called without arguments, raises
`TypeError` at once. An exception is reported as unhealthy with its type and message
(masked, see "Behavior under failure"). A check whose result's truth
value cannot be computed (a numpy array of more than one element, a pandas
Series) counts as failing, like one that raises (`Health check 'name'
raised ValueError: The truth value of an array ...`): return `bool(...)` of what you mean, such as
`bool(arr.all())`.

In your service, register checks on its existing `app`. This complete
program uses a service name of its own (`health-demo`): a second program
started with another service's name would consume that service's
commands.

```python
from kbm_ledsas_sdk import ServiceApp


# Stand-ins for your own code: a database client and a model loader.
class _Db:
    async def ping(self) -> bool:
        return True


db = _Db()


def load_model():
    return object()


app = ServiceApp(service_name="health-demo")

@app.handler("Ping")
async def ping(ctx, req: dict) -> dict:
    return {"pong": True}

@app.liveness_check("event_loop_responsive")
def event_loop_responsive():
    return True   # reaching here proves the loop is alive

@app.readiness_check("db_connected")
async def db_connected():
    return await db.ping()

# Load models and other warm-up work before app.run(): readiness does not
# hold back commands.
model = load_model()
app.run()
```

A check name must be unique within its registry; registering it twice
raises `ValueError` at decoration time.

**Time limit.** Each check has a time limit: 2 seconds, or the `timeout` you register it
with (`@app.readiness_check("db_connected", timeout=1.5)`; more than 0, at
most 60). An async check still running at its limit is cancelled and
reported unhealthy (`timed out after 2s`; one WARNING `Health check
'db_connected' did not finish within 2s; reported as unhealthy` when it
starts failing), so a hanging dependency answers 503 instead of hanging
the probe. The checks of one request run concurrently. Keep every limit
below your probe's timeout. A **sync** check runs on the event loop and
cannot be interrupted, so it blocks the whole service: keep it to a quick
in-memory test and write I/O checks as `async def`.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | TCP port for the server. Set `0` to disable entirely. A port the server cannot listen on stops the service at start (see "Behavior under failure"). |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Bind address (loopback only by default; see the "Environment Variables" row). Set `0.0.0.0` when probes come from another host (the probes of a Kubernetes pod do), and restrict who can reach the port, for example with a network policy. A non-loopback bind logs one INFO line (`Health server on 0.0.0.0:8090 is reachable from the network ...`), or a WARNING when `KBM_LEDSAS_HEALTH_VERBOSE` is also on. |

### Behavior under failure

- **An aiohttp version whose internals the health server cannot harden:**
  the endpoints still answer, with one ERROR per gap and process,
  `Health server: <what> cannot be applied with the installed aiohttp
  <version>; the endpoints still answer, without that protection.
  Install an aiohttp version this SDK release supports.`
- **Port already in use, or an address the server cannot listen on:** the
  service stops at start, before running any command: one ERROR naming the
  setting (`Could not start the health server on
  KBM_LEDSAS_HEALTH_PORT=8090 (KBM_LEDSAS_HEALTH_HOST=127.0.0.1): the
  address is already in use (...); another process, or another instance
  of this service, listens on it. Set KBM_LEDSAS_HEALTH_PORT to a free
  port, or to 0 to run without health endpoints.`; for a host that is not
  an address of the machine it names `KBM_LEDSAS_HEALTH_HOST`), then
  `ServiceApp exiting (startup failure already reported above). Reason
  class: kbm_ledsas_sdk.health.server.HealthServerStartFailure`, exit
  code 1, no traceback. Commands already received go back to the queue.
  Before v0.3.6 the service ran on without health endpoints. On macOS a
  process on the wildcard address (`0.0.0.0:8090`) does not stop the SDK
  binding `127.0.0.1:8090`, so `http://localhost:8090` may reach the other
  process; probe `http://127.0.0.1:<port>`, as the examples do.
- **Custom check raises an exception:** that check is `unhealthy` with the
  exception text; the other checks still run. In the WARNING and the
  verbose body the text is masked (a bare `sig=` outside a URL query is
  not masked). Every record the SDK writes masks the same secrets, the exceptions of your handlers included (see "Secrets in these records are masked").
- **A check keeps failing:** one WARNING when it starts failing (`Health
  check 'db' raised RuntimeError: ...` or `... did not finish within 2s
  ...`), again when its reason changes and every 10 minutes while it
  fails the same way (ending `(still failing)`); one INFO `Health check
  'db' passes again` when it recovers. A falsy result is not logged (the
  body names it).
- **Custom check hangs:** cancelled at its time limit, `unhealthy`.
- **Shutdown:** from the first stop signal on, `/ready` answers `503`
  (verbose body `shutting down`, minimal body `"failed": ["transport"]`)
  while handlers finish (up to
  `KBM_LEDSAS_HANDLER_TIMEOUT`) and `/health` stays `200`, so a liveness
  probe does not restart a draining service. The health server stops
  before the transport (waiting at most about 4 s for requests in
  progress); probes are refused from then on.
- **Slow or idle connections:** a connection without a complete request
  within 10 s is closed (silent, partial, or idle after its answer). At
  most 100 connections are open at a time: a connection made above that
  closes the oldest connection that is not answering a request, so idle
  connections a client holds cannot keep the probes out; it is itself
  closed only when all others are answering requests (one WARNING a minute: `Health server has 100 open connections
  (the limit); the oldest idle ones are closed to accept new connections
  (...)`). With `KBM_LEDSAS_HEALTH_HOST=0.0.0.0` still let only the probes
  reach the port: a client keeping 100 requests in progress can delay
  them.

### `ServiceApp.health_server_running`

Read-only `bool`: `True` while the health server is bound and serving,
`False` when disabled (`KBM_LEDSAS_HEALTH_PORT=0`), not started yet or
stopped. A test and diagnostic helper.

```python
if app.health_server_running:
    ...
```

### `ServiceApp.service_name`

Read-only `str`: the name given to `ServiceApp(service_name=...)`.
Assigning it raises `AttributeError`; a non-empty
`KBM_LEDSAS_SERVICE_NAME`, read when `app.run()` starts, overrides it for
the queue and the logs.

---

**Version:** 0.3.6
**Release:** 2026-10-21

### Version history

See [CHANGELOG.md](../CHANGELOG.md) for the full release history.
