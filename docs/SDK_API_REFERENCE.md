# LEDSAS SDK API Reference v0.3.6

Complete reference for all SDK APIs implemented in kbm-ledsas-sdk v0.3.6.

> **Note:** The SDK connects directly to RabbitMQ and Azure Blob Storage; you
> configure it through the connection environment variables documented below.
> What changed in each release, and what to do when you upgrade, is in
> [CHANGELOG.md](../CHANGELOG.md) and [UPGRADING.md](../UPGRADING.md).

---

## Public vs Internal modules

The following are part of the **public, supported API**:

- `from kbm_ledsas_sdk import ServiceApp, ExecutionContext, errors`
- `from kbm_ledsas_sdk import json_log_formatter` (logging helper — see "Logging note")
- `from kbm_ledsas_sdk import BlobOperations` (the type of `ctx.blob`, for
  type hints; see "BlobOperations")
- `from kbm_ledsas_sdk.models import BlobRef, Command, Response, Status, Envelope`
  (`Command` / `Response` / `Status` / `Envelope` are read-only message
  models — stable; use them to inspect or build wire messages). They are
  frozen: assigning a field (`ctx.envelope.reply_to = "..."`) raises
  `pydantic.ValidationError`. To change a field, make a copy:
  `envelope.model_copy(update={"reply_to": "..."})`. Each message model
  checks that its envelope is of its own kind: `Command`, `Response` and
  `Status` refuse an envelope whose `type` is not `command`, `response`
  or `status` respectively (`pydantic.ValidationError`: `envelope.type
  is 'command', not 'response' (a Response carries a response
  envelope)`); copy the envelope with `type=` set to build one from
  another.
- The error classes `errors.SDKError`, `errors.Retryable`,
  `errors.Permanent`, `errors.DeadlineExceeded` and
  `errors.ContainerNotAllowed` (see "Error Handling"). The first four are
  also importable from `kbm_ledsas_sdk.models` (`from kbm_ledsas_sdk.models
  import Permanent`): the same classes, so either import path works.
  `ContainerNotAllowed` is only in `kbm_ledsas_sdk.errors`.
- Every class, method, env var, and behaviour documented in this file.

Some names are reachable from the public modules but are **internal**
too:

- the `ExecutionContext` constructor: the SDK builds the context for each
  command and passes it to your handler; use `ExecutionContext` only as a
  type hint (its signature may change);
- the pydantic validator methods of the models and of the configuration
  (`Envelope.validate_timezone_aware`,
  `Envelope.validate_deadline_after_sent_at`, `BlobRef.validate_uri_format`
  and the `validate_*` methods of the SDK's configuration): the models run
  them themselves when they are built; do not call them;
- `HandlerFunc` in `kbm_ledsas_sdk.app`, an undocumented type alias kept
  importable for earlier code; annotate handlers with your own types.

Anything reached through a deeper import path (`kbm_ledsas_sdk.transport.*`,
`kbm_ledsas_sdk.amqp.*`, `kbm_ledsas_sdk.runtime.*`, `kbm_ledsas_sdk.blob.*`,
`kbm_ledsas_sdk.health.*`, `kbm_ledsas_sdk.utils.*`) is **internal**.
Internal modules may change layout, signatures, or be removed entirely
in any point release with no deprecation period. Reach in only for
test fixtures (use the public API in production code). On a
`ServiceApp`, the documented members are public: its constructor,
`service_name`, the decorators, `run()` and `health_server_running`.
Attributes other than `service_name` are internal and may change.

**Code snippets.** Most code blocks in this reference are fragments. A
block that uses `ctx` or `req` runs inside an `@app.handler` function,
`app` is your `ServiceApp`, and names such as `compute`, `metrics` or
`db` stand for your own code. Blocks that start with their imports and
end with `app.run()` or `asyncio.run(...)` are complete programs.

---

## Environment Variables

Configure SDK behavior via environment variables. Every value is read
the same way however it was set: surrounding whitespace and one matched
pair of surrounding quotes are removed (`docker run --env-file` passes
quotes through, a shell `source` removes them), so `KBM_LEDSAS_LOG_LEVEL="INFO"`
and `KBM_LEDSAS_LOG_LEVEL=INFO` mean the same.

A value the SDK cannot use stops the start with one `Configuration error`
line that names the variable and the value it holds, for example
`Configuration error: KBM_LEDSAS_PREFETCH=5000: Input should be less than
or equal to 1000` (connection values are named but never repeated). Each
range is in the table. The boolean settings (`KBM_LEDSAS_GENERIC_ERRORS`,
`KBM_LEDSAS_HEALTH_VERBOSE`, `KBM_LEDSAS_ALLOW_INSECURE_AMQP`,
`KBM_LEDSAS_DEBUG`) accept `true` / `false` (also `1` / `0`, `yes` / `no`,
`on` / `off`, `t` / `f`, `y` / `n`, in any letter case); unset or empty is
`false`. Any other value (a typo such as `ture`) stops the start with
`Configuration error: KBM_LEDSAS_GENERIC_ERRORS='ture' is not a boolean
(use true or false; ...)`, instead of being read as `false`. An empty
value means the default for the boolean settings and for names (such as
`KBM_LEDSAS_TENANT`); the numeric and choice settings (such as
`KBM_LEDSAS_PREFETCH` or `KBM_LEDSAS_LOG_LEVEL`) refuse an empty value
with a `Configuration error`: unset the variable to use its default
(`Configuration error: KBM_LEDSAS_PREFETCH='' is empty: unset it to use
the default (10) or give a whole number`; for a log setting `Invalid log
level: '' (empty: unset the variable to use the default)`).

A variable whose name starts with `KBM_LEDSAS_` but that is not one of
the settings below (a typo such as `KBM_LEDSAS_PREFECH`) is not read,
and its setting keeps its default. The SDK says so in one WARNING when
the service starts, naming each such variable and the closest setting:
`Ignoring environment variable(s) that are not settings of this SDK:
KBM_LEDSAS_PREFECH (did you mean KBM_LEDSAS_PREFETCH?)`.
Their values are not logged. The variables Kubernetes sets
for a Service in the namespace
are not reported: nobody set them. For a Service named
`kbm-ledsas-<name>` these are `KBM_LEDSAS_<NAME>_SERVICE_HOST`,
`KBM_LEDSAS_<NAME>_SERVICE_PORT` (and `..._SERVICE_PORT_<PORT NAME>`),
`KBM_LEDSAS_<NAME>_PORT` and `KBM_LEDSAS_<NAME>_PORT_<port>_TCP` (and its
`_PROTO`, `_PORT` and `_ADDR` forms).

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_SERVICE_NAME` | Constructor arg required; env optional | Service name for queue binding. Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. The constructor argument `ServiceApp(service_name=...)` is always required, and is checked by the constructor itself, as `@app.handler` checks a command name when it decorates: a value that is not a string raises `TypeError` (`ServiceApp(service_name=None): the service name must be a string, got NoneType`), and one that breaks this rule raises `ValueError` (`ServiceApp(service_name='bad name'): a service name is 1 to 64 characters: letters, digits, '.', '_' and '-', starting with a letter or digit`). When this env var is also set to a non-empty value it **overrides** the constructor value and renames the service's exchange and queue (`cmd.<name>.v1` / `queue.<name>.v1`). The SDK logs a WARNING at startup when the two differ. An empty or blank value (`KBM_LEDSAS_SERVICE_NAME=` in a `.env` file or `docker run -e`) is read as unset, as for `KBM_LEDSAS_TENANT`: the constructor's name applies. Leave it unset unless you mean to rename the service. |
| `KBM_LEDSAS_RABBITMQ_URL` | Required | RabbitMQ connection URL |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Required | Azure Blob connection string (required even if your handlers never call `ctx.blob` — the blob client initializes at startup). A value the storage client cannot read stops the start with `Configuration error: KBM_LEDSAS_BLOB_CONN_STRING is not a usable storage connection string (...)` (the value is not repeated). So does a string that carries no credential, neither an `AccountKey` nor a `SharedAccessSignature` (or an empty one): the storage client would accept it and send every request without credentials, which storage refuses with HTTP 403. So does an `AccountKey` the storage client cannot decode (for example one whose trailing `=` or `==` was lost in a copy; the storage client would accept it and then fail every request). The check decodes the key the way the storage client does, so a key the client can sign with is accepted (the client ignores anything after the padding); a key that decodes but is not the account's key is not caught here, and every storage request then fails with HTTP 403. A `BlobEndpoint` that is not an `http://` or `https://` URL with a host is refused at start too, with the same `Configuration error` (without a scheme the storage client would silently use `https://`, which the local emulator does not speak). A shared access signature (SAS) connection string (`BlobEndpoint=https://<account>.blob.core.windows.net;SharedAccessSignature=<token>`) works as well as an account-key string and can be limited to the containers and permissions the service needs. |
| `KBM_LEDSAS_ALLOWED_CONTAINERS` | _(unset)_ | Comma-separated container names that `ctx.blob` may read and write (`input,results`); every upload and download outside them raises `errors.ContainerNotAllowed` before anything is sent to storage. Unset, empty or blank: no restriction. An empty item or an invalid container name stops the start with one `Configuration error` line. See "Container allowlist" under "Security model". |
| `KBM_LEDSAS_CONTAINER` | `dev` | Informational: logged at startup as `default_container`. The `ctx.blob.upload_*` methods always take an explicit `container` argument and never read this variable; read it yourself if you want a configurable output container. `os.getenv` returns the raw value: under `docker run --env-file` a quoted value keeps its quotes, so remove surrounding whitespace and one matched pair of quotes as the SDK does, for example `v = os.getenv("KBM_LEDSAS_CONTAINER", "").strip(); v = v[1:-1].strip() if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'" else v; container = v or "dev"`. |
| `KBM_LEDSAS_PREFETCH` | `10` | AMQP message prefetch count (`1` to `1000`). Up to this many commands are received, parsed and held at once; see "Memory" under "Malformed message bodies" for what that costs with large bodies. A command stays unacknowledged while its handler runs, so this also caps the handlers that run at once: at most `min(KBM_LEDSAS_PREFETCH, KBM_LEDSAS_CONCURRENCY)`. Below `KBM_LEDSAS_CONCURRENCY` the SDK logs one WARNING at start (`KBM_LEDSAS_PREFETCH=1 is below KBM_LEDSAS_CONCURRENCY=4: ... at most 1 handler(s) run at once ...`). |
| `KBM_LEDSAS_CONCURRENCY` | `4` | Max concurrent handlers (`1` to `100`). The handlers that actually run at once are at most `min(KBM_LEDSAS_PREFETCH, KBM_LEDSAS_CONCURRENCY)`: keep `KBM_LEDSAS_PREFETCH` at least this high to use every worker. |
| `KBM_LEDSAS_LOG_LEVEL` | `INFO` | Logging level. Applied to the `kbm_ledsas_sdk` logger at startup. |
| `KBM_LEDSAS_LOG_FORMAT` | `json` | `json` (machine-parseable) or `text` (colored human-readable). Applied at startup. |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | `1800` | Handler timeout in seconds (`0` disables; max `86400` = 1 day). Above `1800` (or `0`) the SDK logs one WARNING at start: keep RabbitMQ's `consumer_timeout` (30 min by default) above the longest handler run plus the time a command waits for a worker, or the broker takes the command back and delivers it again (see "Handler Timeout" below). |
| `KBM_LEDSAS_MAX_RETRIES` | `3` | `0` to `100`. Maximum number of retries (NOT counting the initial attempt) for retryable failures (`Retryable`, `Timeout`, unexpected exceptions). Default `3` means a handler that always raises `Retryable` runs **4 times total** (1 initial + 3 retries) before its message goes to DLQ. The caller gets one reply, after the last attempt. The count is kept in the service process (see "Retry Mechanism"). |
| `KBM_LEDSAS_GENERIC_ERRORS` | `false` | Fallback to generic message when handler did not set `user_message`. A value that is not a boolean stops the start (see above). |
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | HTTP port for `/health` and `/ready` (`0` disables; at most `65535`). A port in use stops the service at start, with one ERROR naming this setting |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Bind address for the health server (loopback only by default): an IPv4 or IPv6 address, or a host name. Write an IPv6 address without brackets (`::`, `::1`). Any other value (e.g. `999.1.1.1`, `[::]`) stops the start with one `Configuration error` line. `0.0.0.0` (all interfaces) is what a container orchestrator's HTTP probes need; the SDK then logs one INFO line that the port is reachable from the network (a WARNING if `KBM_LEDSAS_HEALTH_VERBOSE` is also on). |
| `KBM_LEDSAS_HEALTH_VERBOSE` | `false` | If `true`, health responses include SDK version + service name + per-check status (fingerprintable; opt-in for dev). Default minimal `{"status": ...}`. |
| `KBM_LEDSAS_MAX_PAYLOAD_BYTES` | `16777216` (16 MiB) | Reject inbound AMQP messages whose body exceeds this size (single WARNING + DLQ), and refuse to send a reply larger than this: a handler result whose serialized reply would exceed it gets one `Permanent` error reply instead (`returned a result too large to send as a reply (... bytes serialized; the limit is ... bytes, KBM_LEDSAS_MAX_PAYLOAD_BYTES)`), see "Payload". A command without `reply_to` gets no reply, so its result is not size-checked. It is not serialized either: only its type is checked (see "Return value"). `0` disables both checks. Max `268435456` (256 MiB). Large payloads and results belong in blob storage. **Note:** the effective inbound limit is `min(broker max_message_size, this value)`. RabbitMQ 4.x defaults `max_message_size` to 16 MiB, so a message over the broker cap is refused at *publish* time with a channel-closing `PRECONDITION_FAILED` and never reaches this SDK gate. To exercise the SDK gate (single WARNING + DLQ) the broker cap must be raised above this value. |
| `KBM_LEDSAS_ALLOW_INSECURE_AMQP` | `0` | If `1` (or `true`), downgrade the SDK's refusal of `amqp://` to non-loopback hosts to a WARNING. Escape hatch for TLS-terminated-upstream deployments. A value that is not a boolean stops the start. |
| `KBM_LEDSAS_TENANT` | _(unset)_ | Optional tenant identifier for multi-tenant AMQP topology naming. Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. See "AMQP Topology" below for how this affects exchange and queue names. |
| `KBM_LEDSAS_DEBUG` | `0` | If `1` (or `true`), dump the full Python traceback on configuration-error startup failures instead of the one-line message. A value that is not a boolean stops the start. |
| `KBM_LEDSAS_CONNECT_TIMEOUT` | `15` | Seconds one connection attempt to the AMQP broker may take (`1` to `300`). The SDK logs `Connecting to the AMQP broker at amqp://***:***@<host>:<port>/ (timeout 15s, heartbeat 60s)` first (the heartbeat in effect, see `KBM_LEDSAS_HEARTBEAT`) (the URL's user name and password, every value of its query string and its fragment are masked: `amqps://***:***@host:5671/vhost?heartbeat=***`; the same in the `Could not connect` line); a broker that does not answer (wrong host, a firewall dropping the packets) then fails the start within this time: one ERROR line naming the cause (`Could not connect to the AMQP broker at ...: no answer within 15s (KBM_LEDSAS_CONNECT_TIMEOUT) ...`), the closing ERROR line `ServiceApp exiting (startup failure already reported above). Reason class: ...` (see "How a failed start ends") and exit code 1, instead of after the operating system's own connect timeout (75 s to over 2 minutes), which a liveness probe may not wait for. Later reconnects use the same bound per attempt. |
| `KBM_LEDSAS_HEARTBEAT` | `60` | AMQP heartbeat interval in seconds that the SDK asks the broker for (`0` to `600`; `0` turns heartbeats off, and with it the detection of a broker that stops answering). The default `60` is the interval earlier releases already used (the AMQP client library's default, which RabbitMQ's default also accepts). A broker or a network path that stops answering without closing the connection is detected after about three times this interval (the AMQP client library allows three missed heartbeats: `3 × (interval + 1)` seconds, 183 s at the default 60, plus up to half an interval until its next check), and the SDK reconnects (see "Broker outages are SDK log events" under "Logging"). **The same limit applies to your handlers:** while a handler blocks the event loop (`time.sleep`, a CPU-heavy loop, a synchronous client call) no heartbeat is sent, and a block longer than about two heartbeat intervals makes the broker close the connection; the command is then delivered again and the redelivery counts as an attempt toward `KBM_LEDSAS_MAX_RETRIES`, so a command that blocks that long on every attempt ends in the DLQ (see "Blocking code is not timed out" under "Timeout Enforcement"). Run blocking work with `asyncio.to_thread(...)` (or `loop.run_in_executor(...)`) rather than raising the heartbeat. A lower value finds a dead broker sooner but leaves handlers less room to block. A `?heartbeat=` in `KBM_LEDSAS_RABBITMQ_URL` wins over this setting (only in lowercase: the AMQP client library ignores another spelling such as `?HEARTBEAT=`, and the SDK then adds its own); its value must be a whole number from `0` to `600`, and any other value is refused at start with one line, `Configuration error: KBM_LEDSAS_RABBITMQ_URL: ?heartbeat='abc' is not a whole number of seconds between 0 and 600 (0 turns heartbeats off, so a broker that stops answering is not detected)`. |
| `KBM_LEDSAS_DEPLOYMENT_ID` | _(unset)_ | Optional build/deployment identifier, reported as the `deployment_id` field in `/health` and `/ready` responses so you can confirm which build is live behind a probe. The SDK does not set it; set it at deploy time (for example to the commit id). Takes precedence over its older alias `KBM_LASTCOMMITID` when both are set. Surrounding whitespace and one matched pair of surrounding quotes are removed, and a blank value counts as unset. Leave it unset locally (the field is then omitted). See "Deployment id" under "Health Endpoints". |
| `KBM_LASTCOMMITID` | _(unset)_ | Older alias of `KBM_LEDSAS_DEPLOYMENT_ID`, read only when that is unset or blank; prefer `KBM_LEDSAS_DEPLOYMENT_ID`. |

### Handler Timeout (`KBM_LEDSAS_HANDLER_TIMEOUT`)

- Default: 1800 seconds (30 minutes)
- Set to `0` to disable timeout enforcement
- A handler still running at this limit is cancelled at its current
  `await`. Work it started in a thread (`asyncio.to_thread`, an executor)
  or a subprocess is **not** stopped; see "Work in threads and
  processes" under "Timeout Enforcement".
- **The broker's `consumer_timeout` must be longer.** RabbitMQ takes back
  a command that stays unacknowledged longer than its `consumer_timeout`
  (30 minutes by default; a command is acknowledged only when its handler
  has finished): it closes the service's channel and delivers the command
  again. So keep the broker's `consumer_timeout` above the longest handler
  run plus the time a command waits for a worker. When
  `KBM_LEDSAS_HANDLER_TIMEOUT` is above `1800` or `0` (no limit), the SDK
  logs one WARNING at start (`KBM_LEDSAS_HANDLER_TIMEOUT=0 (no limit)
  allows handler runs longer than RabbitMQ's default consumer_timeout (30
  min): ...`). What happens when the broker does take a command back is
  described under "The broker's consumer_timeout closed the channel" in
  "Logging".

### Max Retries (`KBM_LEDSAS_MAX_RETRIES`)

- Default: 3 retries (NOT counting the initial attempt). A handler
  that always raises `Retryable` runs **4 times total** (1 initial +
  3 retries) before its message goes to DLQ.
- Applies to retryable failures: `Retryable`, `Timeout` and unexpected
  (uncaught) exceptions. `Permanent`, `DeadlineExceeded` and the
  non-retryable cases listed under "Error Handling" are never retried.
- After the configured number of retries is exhausted, the message
  goes to the Dead Letter Queue.
- No reply is sent for an attempt that is retried; the caller gets one
  reply when the command finishes (see "Replies: one per command").

### Generic Errors (`KBM_LEDSAS_GENERIC_ERRORS`)

- Default: `false` — caller sees the handler's internal `message`
- When `true` — caller sees a generic fallback (e.g. `"Processing failed"`)
- `user_message` **always wins** when set, regardless of this flag

### Transport security (`KBM_LEDSAS_RABBITMQ_URL`)

- **Cleartext AMQP refusal.** An `amqp://` URL (no TLS) is accepted only
  for a loopback host (`localhost`, `127.0.0.1`, `::1`). For any other
  host the service refuses to start with one `Configuration error`
  line, because credentials and messages would cross the network
  unencrypted. Use `amqps://`, or set `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1`
  when TLS is terminated by something in between (for example a
  service mesh, or a local Docker network in development); the SDK
  then logs one WARNING per host.
- **TLS certificate verification stays on.** A URL whose query sets
  `no_verify_ssl` to any value other than `0` (`1`, `true`, even `false`,
  or no value at all: the AMQP client library keeps verification on only
  for `no_verify_ssl=0`, which is accepted) stops the start
  with one `Configuration error` line, because a machine in the path
  could then read the credentials and the messages. Set
  `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` to accept an unverified broker
  anyway; the SDK then logs one WARNING (`KBM_LEDSAS_RABBITMQ_URL sets
  no_verify_ssl: the broker's TLS certificate is not verified. ...`).
  Only the lowercase spelling counts: the library ignores the option in
  another letter case (`NO_VERIFY_SSL=1`), and verification stays on.
  To trust a broker certificate signed by a private certificate
  authority, keep verification on and name the authority's certificate
  file: `amqps://user:password@broker:5671/?cafile=/etc/ssl/broker-ca.pem`.
  `?certfile=<path>&keyfile=<path>` give a client certificate and its key
  to a broker that asks for one. The paths are read by the service
  process (inside its container); like every query value, they are
  masked in log records.
- **Storage over `http://` to another machine is reported.** When
  `KBM_LEDSAS_BLOB_CONN_STRING` reaches storage over `http://`
  (`DefaultEndpointsProtocol=http`, or an `http://` `BlobEndpoint`) at a
  host other than this machine (`localhost`, `127.0.0.1`, `::1`), the SDK
  logs one WARNING at start (`KBM_LEDSAS_BLOB_CONN_STRING reaches storage
  at <host> over http://: the requests, signed with the account key or
  SAS, travel in cleartext. ...`) and starts. A local emulator in another
  container (`BlobEndpoint=http://azurite:10000/...`) gets the WARNING
  too; for any real storage use `https://`.
- **Credentials in the URL must be percent-encoded.** A user name or
  password containing `@`, `#` or `?` makes the URL parse to the wrong
  host, so the service refuses to start with one `Configuration error`
  line that names the fix (it does not repeat any part of the URL).
  Encode them (`@` -> `%40`, `#` -> `%23`, `?` -> `%3F`, `/` -> `%2F`),
  e.g. with `urllib.parse.quote(password, safe="")`.
- **Only `amqp://` and `amqps://` URLs with a host are accepted.** Any
  other scheme (`http://`, a typo), a URL without a scheme
  (`user:pass@host:5672`) or without a host, a port that is not a number
  from 1 to 65535, and a URL containing whitespace stop the start with
  one `Configuration error: KBM_LEDSAS_RABBITMQ_URL ...` line that does
  not repeat any part of the URL. The client library would otherwise
  speak plain AMQP for any scheme, bypassing the cleartext refusal above,
  or retry forever while logging the URL. The message names the usual
  causes: an unmatched quote, and an inline `# comment`, which
  `docker run --env-file` passes through verbatim (put comments on their
  own line).

---

## Security model

**The command exchange is the trust boundary.** The SDK checks the shape
of every command (see "Message Format"), not who sent it: anyone who can
publish to a service's command exchange (`cmd.[{tenant}.]{service}.v1`)
can make it run any of its handlers with any payload. Everything a
handler does on behalf of a command, it does with the **service's own**
credentials:

- **Storage.** A blob URI in a payload (`azblob://<container>/<path>`) is
  chosen by the caller, and `ctx.blob` reads and writes it with the
  service's storage credential (`KBM_LEDSAS_BLOB_CONN_STRING`). A handler
  that downloads `req["input_uri"]` or uploads to a container and path
  taken from the payload reads, or overwrites, any blob that credential
  reaches, also in containers the caller could not reach itself.
- **Replies.** The reply and status messages go to the exchange the
  command names in `reply_to` (the SDK refuses only `cmd.`, `dlq.` and
  `amq.` exchanges), with the service's broker credentials.

Limit what a command can make the service do:

- give each service a storage credential scoped to the containers it
  needs: a shared access signature (SAS) connection string whose SAS is
  container-scoped (limited to those containers and the permissions the
  service needs; see `KBM_LEDSAS_BLOB_CONN_STRING`), not the account key
  of a shared storage account;
- restrict who can publish to the command exchange with RabbitMQ
  permissions (a user per caller, with write permission only on the
  exchanges it may use; the service's user needs configure, write and
  read permission on its own `cmd.`, `queue.` and `dlq.` names and write
  permission on the reply exchanges);
- list the containers the service may use in
  `KBM_LEDSAS_ALLOWED_CONTAINERS` (see "Container allowlist" below);
- in the handler, check the container (and, where it matters, the path)
  of every blob URI in a payload against the ones the service is meant to
  use, and raise `errors.Permanent` for any other
  (`BlobRef.from_uri(uri).container`);
- treat payload text as untrusted when you log it (see "Payload-field
  safety" under "ExecutionContext").

### Container allowlist (`KBM_LEDSAS_ALLOWED_CONTAINERS`)

`KBM_LEDSAS_ALLOWED_CONTAINERS=input,results` limits every operation of
`ctx.blob` to the listed containers: the uploads (`upload_bytes`,
`upload_json`, `upload_text`, `upload_stream`) by their `container`
argument, and the downloads (`download_bytes`, `download_json`,
`download_text`, `download_stream`) by the container of the blob URI
(an `azblob://` string or a `BlobRef`). `ctx.blob` is the only blob
interface a handler gets, and every operation names its container, so
the service's storage credential is then used for the listed containers
only, whatever a command asks for.

- **Value.** Container names separated by commas; spaces around each
  name are ignored, and a name may repeat. Names are matched exactly
  (container names are lowercase).
- **Unset, empty or blank:** no restriction (the default, and the
  behaviour of earlier releases).
- **Checked at start.** An empty item (`a,,b`, a leading or trailing
  comma) or a name storage would refuse stops the start with one
  `Configuration error` line, for example
  `Configuration error: KBM_LEDSAS_ALLOWED_CONTAINERS='a,,b' has an empty
  item: list container names separated by commas (for example
  'input,results'), or unset it for no restriction` or
  `... KBM_LEDSAS_ALLOWED_CONTAINERS: 'Bad_Name' is not a valid container
  name (...)`. When it is set, the start logs one INFO `Blob access limited
  to the containers in KBM_LEDSAS_ALLOWED_CONTAINERS: input, results`
  (field `allowed_containers`).
- **A refused operation** raises `errors.ContainerNotAllowed` before
  anything is sent to storage (see "Error Handling") and logs one WARNING
  `Blob upload refused: container 'other' is not in
  KBM_LEDSAS_ALLOWED_CONTAINERS` (or `Blob download refused: ...`; fields
  `container` and `operation`), even when the handler catches the error.
  Uncaught, it is not retried: the handler layer logs its one ERROR
  `Non-retryable error in handler (not retried): container 'other' is not
  allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)` and the caller gets one
  `Permanent` reply. An argument of the wrong type (`None`, a malformed
  URI) keeps its own `TypeError` / `ValueError`.

The allowlist limits what a command can make the service do with its
credential (the risk above); it is not an authorization layer for
callers. The storage credential's own scope (a container-scoped SAS)
remains the boundary.

---

## Message Format

The SDK expects incoming messages to follow a specific structure with an
`envelope` and `payload`.

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

The example leaves out the optional `deadline`. If you send one, it must
be after `sent_at`, and it must still be in the future when the command
arrives: a command whose deadline has already passed is not run (it gets
a `DeadlineExceeded` reply, see "`errors.DeadlineExceeded`").

### Envelope Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | Yes | Schema version (use "1.0"). `<major>.<minor>`, each 1 to 4 ASCII digits (`0`-`9`), at most 9 characters: pattern `^[0-9]{1,4}\.[0-9]{1,4}$`. |
| `type` | string | Yes | Message type: `"command"`, `"response"`, or `"status"` (see "Type values" note below) |
| `name` | string | Yes | Command name (e.g., "ProcessData"). Pattern `^[A-Za-z][A-Za-z0-9_]*$` (a letter, then letters, digits or underscores), 1..64 chars. `@app.handler` refuses any other name with a `ValueError` at registration, because a command carrying it fails envelope validation. |
| `message_version` | string | Yes | Message schema version (e.g., "1.0"). The same rule as `schema_version`: pattern `^[0-9]{1,4}\.[0-9]{1,4}$`. Must equal the `version` the handler was registered with. |
| `message_id` | string | Yes | Unique message identifier (UUID, either letter case). Normalized to lowercase. The service's status updates and its reply carry the command's `message_id` (the lowercase form), in the envelope and in the AMQP `message_id` property: it names the command they belong to, so it is the same on every message one command produces. Do not de-duplicate the messages you receive on it (an inbox or idempotent-receiver keyed on `message_id` would drop the reply as a copy of the first status update); match them on `correlation_id` and the envelope `type` (`status`, `response`). |
| `correlation_id` | string | Yes | Correlation ID for tracking related messages (UUID, either letter case). Echoed **exactly as sent** in the reply and in status updates (and in the reply's AMQP `correlation_id` property), so a caller can match replies with `==` on the value it sent. |
| `idempotency_key` | string | Yes | Key for idempotent processing. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..128 chars. |
| `sent_at` | string | Yes | ISO 8601 timestamp with a timezone (`2026-05-26T10:30:00Z`), when the message was sent. Only a timestamp string is accepted: a number (a Unix time such as `1760000000`) or a numeric string is refused. |
| `trace_id` | string | Yes | Distributed tracing ID. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..255 chars. |
| `reply_to` | string | No | Name of a **pre-declared AMQP exchange** to publish the response to. URL-safe: `^([A-Za-z0-9_\-:.]+)?$`, max 127 chars (matches AMQP's protocol-level cap on exchange-name length and the shape of `trace_id` / `idempotency_key` / `job_id`). SDK publishes with routing key `response` (or `status` for `emit_status`). The SDK does not declare this exchange — the caller owns it. **Contract: the exchange must exist AND have at least one queue bound** for the `response` routing key; the SDK publishes replies as mandatory, so a missing exchange *or* an unroutable response (no bound queue) fails the reply and dead-letters the command. A name starting with `cmd.` or `dlq.` (a LEDSAS service's command or dead-letter exchange) or with `amq.` (the broker's reserved exchanges, such as `amq.topic` and `amq.direct`) is refused: the command is an invalid envelope, dead-lettered before its handler runs. Leave empty (or omit it, or send JSON `null`, which is read as empty) for fire-and-forget. See `examples/hello_world_service/scripts/send_hello.py` for a runnable caller. |
| `deadline` | string | No | ISO 8601 deadline for processing (with a timezone; must be after `sent_at`). A number is refused, as for `sent_at`. |
| `priority` | int | No | An integer 0..9 (`"5"`, `5.0` and `true` are refused). Informational only: it is validated and copied to the reply envelope, but the service queue is not a priority queue, so it does not change processing order. |
| `job_id` | string | No | Business-level job identifier. URL-safe `^[A-Za-z0-9_\-:.]+$`, 1..128 chars. |

> **Type values:** the SDK accepts and emits exactly `"command"`,
> `"response"`, and `"status"`. Handler-raised errors come back as a
> `"response"` envelope whose `payload` contains an `error` object
> (`{code, message, retryable}`).

> **Telling an error reply from a result.** A success reply and an error
> reply are both `"response"` envelopes with the same AMQP properties;
> only the payload tells them apart. A reply is an error reply when
> `payload["error"]` is an object (a `dict` in Python) with the keys
> `code`, `message` and `retryable`, as in `{"error": {"code":
> "Permanent", "message": "...", "retryable": false}}`; the SDK's error
> replies carry nothing else in `payload`. Any other reply is the
> handler's result, and a result may have an `error` key of its own (see
> "Return value"): `{"valid": false, "error": "field x missing"}` is a
> success. In Python: `error = payload.get("error")`, then the reply
> failed when `isinstance(error, dict) and {"code", "message",
> "retryable"} <= error.keys()`. A handler must therefore not return an
> `error` object of that shape: raise instead (see "Error Handling").

> **Invalid envelopes are dead-lettered without a reply.** A command
> whose envelope fails validation (for example a `deadline` before
> `sent_at`, a `name` that does not match the pattern above, or a
> non-UUID `message_id`) goes straight to the dead-letter queue with
> one ERROR line in the service log. No reply is sent, even when
> `reply_to` is valid, because nothing in the envelope can be trusted;
> the caller learns of it only by timing out. Unknown extra envelope
> fields and any `schema_version` matching the pattern are accepted.
> The body must be a JSON object with an `envelope` object and a
> `payload` object (an empty `{}` is fine): a body without `payload`, or
> whose `payload` is not an object (an array, a string, a number,
> `null`), is dead-lettered the same way, without a reply
> (`Envelope failed schema validation; dead-lettering`, with `payload:
> Field required` or `payload: Input should be a valid dictionary`).

> **Malformed message bodies are dead-lettered too.** A body that is not
> UTF-8, not JSON, nested more than 256 levels deep, or holds an integer
> literal longer than 4300 digits is dead-lettered with one ERROR line and
> no traceback, before its envelope is validated or a handler runs (no
> retry, no reply). Nesting counts arrays and objects anywhere in the
> parsed body: the body object is level 1, its `payload` object level 2.
> The 256-level limit is the SDK's own, so it is the same on every
> supported Python version; a deeper body logs `Message body exceeds
> JSON-parsing nesting limit; dead-lettering` with `error_type`
> `ExcessiveNesting`. Nest data less deeply, or send it in a blob.
> Measuring the depth costs CPU time on large bodies: a body with more
> than 256 `[` and `{` characters in all (any inside strings included)
> is walked once when it arrives, and, like the parse, the walk holds
> the event loop while it runs. Its time grows with the number of values
> (array elements and object members) in the body, and a value that is
> an array or object costs about three to four times as much as a
> number or string: about 0.1 s for 16 MiB of ordinary records, up to
> about 0.3 s for 16 MiB of plain numbers or strings, and up to about
> 1 s on Python 3.13 and 1.3 s on 3.11 for 16 MiB made only of empty or
> deeply nested arrays and objects (for empty objects that is several
> times the parse). These times depend on the hardware.
> Large data belongs in a blob. The
> parser accepts the non-standard literals `NaN`, `Infinity` and
> `-Infinity` (the handler receives float `nan` / `inf`, and a reply
> serializes them as `null`), and when a key appears twice the last
> value wins, and only that value counts toward the nesting limit. Send
> standard JSON (RFC 8259). Two cases still follow the Python version's
> own parser, which gives up at about 1000 levels on Python 3.11 and
> goes far deeper on 3.13: a body with a repeated key whose discarded
> value is nested past that point can be dead-lettered on 3.11 and
> processed on 3.13, and a body nested past that point that is also
> not valid JSON is dead-lettered on every version, but may be logged
> as either error.
>
> **AMQP properties that are not UTF-8 are dead-lettered too.** A message
> whose AMQP short-string property (`message_id`, `correlation_id`,
> `reply_to`, `type`, `content_type`, `app_id`, ...) or header name holds
> bytes that are not valid UTF-8 is dead-lettered once, before its body
> is read (no retry, no reply), with one WARNING that names the property
> (`AMQP message property message_id is not valid UTF-8; dead-lettering`;
> a header name is named `headers[<name>]`). In that record
> `amqp_message_id` shows each such byte as a `\xNN` escape. Consumption
> goes on and the connection stays up. (Before, such a message made the
> AMQP client library drop the connection, and the broker delivered it
> again on the next one, so the service processed nothing else.) To
> decode these messages at all, the service replaces the two
> short-string decoders of the AMQP frame decoder (`pamqp`, which
> `aio-pika` uses) for the whole process when it starts (when it sets up
> its broker connection; importing `kbm_ledsas_sdk` alone changes
> nothing): only a value that fails strict
> UTF-8 decoding is decoded differently, with `surrogateescape` (each
> byte that is not UTF-8 becomes one lone surrogate, `\udcNN`); valid
> UTF-8 decodes exactly as before. Your own `aio-pika` code in the same
> process therefore receives such a value, once the service has started,
> as a `str` holding lone surrogates, instead of an error that closes
> its connection.
>
> **Memory.** A command's parsed body stays in memory until the command
> finishes, and it takes far more memory than the body's size. Measured
> on CPython 3.11 and 3.13 for a 15.7 MB body: about 380 MB (24 times
> the body) when it holds only empty arrays or objects, about 15 times
> for small records (`{"t":1,"v":2}`) or short strings, 10 times for
> floats and 4 times for small integers. Up to `KBM_LEDSAS_PREFETCH`
> commands (default 10) are received and held at once while earlier
> ones run, so the worst case is about `KBM_LEDSAS_PREFETCH` x
> `KBM_LEDSAS_MAX_PAYLOAD_BYTES` x 25: with the defaults (10 x 16 MiB)
> about 4 GiB, above a typical container memory limit. A service killed
> for memory gets the same messages again after its restart. Keep
> bodies small (send the data in a blob and its reference in the body),
> and set `KBM_LEDSAS_MAX_PAYLOAD_BYTES` (and if needed
> `KBM_LEDSAS_PREFETCH`) so that this product fits the memory you give
> the service: with a 1 MiB limit and the default prefetch the worst
> case is about 250 MiB. When the service runs in a container with a
> memory limit (read from its cgroup) and this product exceeds it, the
> SDK says so in one WARNING at start (`Up to KBM_LEDSAS_PREFETCH=10
> command bodies of up to 16 MiB each ... about 3.9 GiB in the worst
> case, above this container's memory limit of 512 MiB ...`), with a
> `KBM_LEDSAS_MAX_PAYLOAD_BYTES` value that keeps the worst case at half
> the limit or less (for a high `KBM_LEDSAS_PREFETCH`, where even 64 KiB
> bodies would not, a lower `KBM_LEDSAS_PREFETCH` with it); the settings
> are not changed for you. With `KBM_LEDSAS_MAX_PAYLOAD_BYTES=0` the SDK
> sets no limit and a body is bounded only by the broker's
> `max_message_size`, which the SDK cannot read: the WARNING then assumes
> 128 MiB bodies, RabbitMQ 3.x's default (4.x's is 16 MiB).
>
> Each of these lines, the envelope-validation line above, the
> `Unexpected error processing message` line and the WARNING for a body
> over `KBM_LEDSAS_MAX_PAYLOAD_BYTES` carry `amqp_message_id` and
> `amqp_correlation_id` (the message's AMQP `message_id` and
> `correlation_id` properties, when set; at most 128 characters) and
> `body_bytes` (the body's size in bytes), so you can find the message
> in the dead-letter queue.

> **Use a new `message_id` for every command.** While a command is being
> processed, a second command with the same `message_id` (for example a
> resend that reuses the envelope) is dead-lettered at once without a
> reply and logged as one WARNING (`Duplicate message_id while original
> still pending`); its caller only times out. Only the first command is
> answered. To retry a command, send a new envelope with a new
> `message_id` and the same `idempotency_key`; once the first command
> has finished, its `message_id` is accepted again. A command that reuses
> it as soon as the first command's reply has arrived (while the first
> command is still being acknowledged) waits for that acknowledgement, at
> most 10 s, and then runs. The broker's own
> redelivery of a command is not such a duplicate: if the connection to
> the broker was lost and restored, or the broker took the command back
> at its `consumer_timeout`, while the command is still in progress
> here, the redelivery is handed to that command, which then settles it
> (see "Broker connection lost while a command runs" and "The broker's
> consumer_timeout closed the channel" under "Logging").

### Response Message Structure

The service publishes its reply to the command's `reply_to` exchange
with routing key `response`. The reply has the same `envelope` +
`payload` shape as a command:

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
| `envelope.name`, `message_version`, `message_id`, `correlation_id`, `idempotency_key`, `trace_id`, `reply_to`, `priority`, `job_id` | Copied from the command (`message_id` in its lowercase form, `correlation_id` exactly as sent; an optional field the command did not carry is `null`) |
| `envelope.sent_at` | When the service sent the reply (UTC) |
| `envelope.deadline` | Always `null` (a deadline applies to the command) |
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
caller may see (see "Generic Error Messages") and `retryable` whether a
later resubmission may succeed (see "Replies: one per command"). The
envelope `type` is `"response"` for both outcomes: see "Telling an error
reply from a result" above.

**AMQP properties** of a reply: `content_type` `application/json`,
`delivery_mode` 2 (persistent), `message_id` and `correlation_id` as in
the envelope. **Headers:** `message_type` (`response`), `schema_version`,
`message_version`, `trace_id` and `sent_at`. Match a reply on
`correlation_id`, not on `message_id`.

### Status Message Structure

Each `ctx.emit_status(...)` call publishes a status message to the same
`reply_to` exchange with routing key `status` (no `reply_to`: no status
message is sent). The envelope is built like a reply's, with `type`
`"status"`; the payload holds `stage`, `progress` and, when given, `note`:

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
| other envelope fields | As in a reply (see the table above) |
| `payload.stage` | The `stage` text passed to `emit_status` |
| `payload.progress` | A number from `0.0` to `1.0` |
| `payload.note` | The `note` text; the key is absent when no note was given |

AMQP properties as for a reply; the `message_type` header is `status`.
Status messages are informational: a command can send any number of
them, and a status message that cannot
be published does not fail the command. To receive them, bind your reply
queue for the routing key `status` as well as `response`.

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

An attempt that is going to be retried sends **no** reply. So the first
reply whose `correlation_id` matches yours is the outcome; stop waiting
when it arrives (as the example's `scripts/send_hello.py` does). Tell the reply
from the status updates by the envelope `type` (`response`, `status`),
not by `message_id`: every message of one command carries the command's
`message_id` (see "Envelope Fields"). Allow for the
retries in your reply timeout: with the default 3 retries the backoff
alone adds 7 to 10 seconds, plus the handler's run time per
attempt.

In an error reply, `retryable: true` means the failure was transient and
the service has stopped retrying it (the command is in the dead-letter
queue); resubmitting it later may succeed. `retryable: false` means it
will fail the same way again.

The one reply is per run of the command. If the service loses its
broker connection while the command runs, or stops while the broker is
unreachable, the broker can deliver the command again and it runs (and
replies) again, so a caller can receive a second reply for the same
`correlation_id`: ignore it (see "Broker connection lost while a command
runs" and "Shutdown" under "Logging").

### Payload

The `payload` field contains your actual request data. This is what
gets passed to your handler as the `req` parameter:

```python
@app.handler("ProcessData")  # Must match envelope.name in message
async def handle(ctx, req: dict):
    # req == the "payload" from the message
    your_field = req.get("your_field")  # "your_data"
    another_field = req.get("another_field")  # 123
```

**Signature:** `@app.handler(command_name: str, version: str = "1.0")`. A
command is routed to the handler registered for its `envelope.name` and
`envelope.message_version`; leave `version` at the default unless you
run side-by-side handler versions. `command_name` must match
`^[A-Za-z][A-Za-z0-9_]*$` (at most 64 characters) and `version` must
follow the `message_version` rule (`<major>.<minor>`, each 1 to 4 ASCII
digits, such as `1.0`); anything else raises `ValueError` at
registration, since a command with that name or version could never
reach the handler. The handler must be an `async def` function that
accepts two positional arguments (`ctx`, `req`): the SDK awaits it. A
plain `def`, a lambda, an async generator (an `async def` with
`yield`), something that is not callable, or a function that cannot be
called with two arguments raises `TypeError` at registration (for
example `... two positional arguments: the SDK calls it with (ctx,
payload), but it takes 1 positional argument.`). A name or version that
is not a string raises a `TypeError` that is also a `ValueError`. Run
blocking work inside the handler
with `await asyncio.to_thread(...)`.

**Return value:** a handler returns a JSON-serializable `dict`; it
becomes the reply payload. Returning `None` (for example a
fire-and-forget handler with no `return`) is treated as `{}`. A result
may hold any keys, `error` included: whether the command failed is
decided by how the handler ended (it returned, or it raised), never by
the result's keys, so a returned `{"valid": False, "error": "field x
missing"}` is replied as it is and the command is acknowledged. To fail
a command, raise (see "Error Handling"). Callers recognise an error reply by
the shape of `payload["error"]`: an object with `code`, `message` and
`retryable` (see "Telling an error reply from a result" above). Do not
return an `error` value of that shape, or the caller takes your result
for a failure.

- **Keys** follow `json.dumps` at every level, the top level included:
  `str` keys are kept; `int`, `float`, `bool` and `None` keys become the
  text `json.dumps` writes (`1` -> `"1"`, `True` -> `"true"`, `None` ->
  `"null"`), so `df.to_dict(orient="index")` with integer row keys works.
  Any other key type (a tuple, a date, ...) and two keys that become the
  same text (`1` and `"1"`) are refused.
- **Values** are written as JSON. Besides what `json.dumps` accepts,
  pydantic (which serializes replies) converts many values; among them:
  `datetime` / `date` / `time` -> ISO 8601 text (UTC `datetime` with a
  `Z`), a `timedelta` -> an ISO 8601 duration such as `PT1M30S`,
  `Decimal`, `UUID`, `Fraction` and `complex` -> text (`"1.10"`,
  `"1/3"`, `"1j"`), a `pathlib.Path`, an IP address, network or
  interface and a compiled regular expression (its pattern) -> text, an
  `Enum` -> its value, `NaN` / `Infinity` -> `null`; `set` / `frozenset`
  / `tuple` (a named tuple too) / `deque` -> list; a generator or another
  iterator (`map(...)`, `zip(...)`, an open text file) -> the list of its
  items, read once while the reply is built (the reply carries the items
  read; an exception while reading it refuses the result:
  `payload["g"]: reading this generator raised ValueError: ...`); a
  dataclass -> an object of its fields; a pydantic model -> what its own
  serialization writes (see below). A value with no JSON form is
  refused (`payload["o"] is an instance of Weird, which has no JSON
  form`); `range`, `dict_keys` / `dict_values` and `array.array` are
  among them: convert them with `list(...)`.
- **Where these rules apply:** in everything written as a JSON object or
  array, at every level: dicts, lists, tuples, sets, deques, the items of
  a generator or iterator, a dataclass's fields and an `Enum`'s value.
  **A pydantic model (or pydantic dataclass) is the exception:** it is
  written with its own serialization settings, and the SDK does not look
  inside it for keys or binary data (pydantic writes `bytes` in a model
  as UTF-8 text unless the model sets `ser_json_bytes`, and refuses other
  bytes; two of its dict keys that become the same text keep the last).
  Convert binary data before you put it in a model, or set
  `ser_json_bytes="base64"` on it. The text rule below applies inside a
  model too.
- **Binary data (`bytes`, `bytearray`, `memoryview`) is refused wherever
  these rules apply, whatever its content** (`payload["b"] is bytes; a
  reply carries text, not binary data: ...`): bytes that happen to be
  UTF-8 text are refused too, so a handler tested with text-like bytes
  cannot fail later on real binary data. Decode text (`data.decode()`),
  base64-encode binary data (`base64.b64encode(data).decode()`), or store
  it in a blob and return the reference.
- **Text** (keys and values) must be valid Unicode text, at every level
  and inside a pydantic model too: a string with a lone surrogate (such as
  `"\ud800"`, which a JSON request can carry and a handler can echo) is
  refused (`payload["s"] is text with a lone surrogate U+D800 at position
  0, which UTF-8 cannot encode`). JSON would
  write it as an escape that strict JSON readers refuse (.NET's
  System.Text.Json reads the document but throws when the string is
  read). The same applies to `emit_status` and `upload_json`. An error
  reply is not refused: in its message (an exception's message or its
  `user_message`, which can quote such a request value) each lone
  surrogate is replaced by U+FFFD, the Unicode replacement character, so
  every JSON reader can read the error reply too.

**Without `reply_to`** (a fire-and-forget command) no reply is sent, so
only the result's type is checked: a `dict` or `None` is accepted, and a
value that is not a dict, or an awaitable that was never awaited, is
refused as described below. The rules above do not apply to such a
result: its keys and values are neither walked nor serialized, so a value
with no JSON form, binary data, two keys that become the same text or a
lone surrogate in it is not an error, and the command is acknowledged.
**With `reply_to`** the result is checked and serialized on the service's
event loop before the reply is sent: a large result holds the loop for
that time (the health probes and the other running handlers wait), so
put large data in blob storage and return its reference.

A return value that is not a dict, or (for a command with `reply_to`) a
dict with a refused key or value, is a bug in the handler: the SDK logs one ERROR, replies with a
`Permanent` error and dead-letters the command **without retrying**,
because the handler's side effects have already happened. The error
names where: `Handler for <name> returned a dict that is not
JSON-serializable (payload["rows"][3] is bytes; ...)`, `(payload: the
key (1, 2) (tuple) is not a JSON object key; ...)`
or `(payload["n"] is an integer with more than 4300 digits; ...)`. A
result whose serialized reply is larger than `KBM_LEDSAS_MAX_PAYLOAD_BYTES`
(16 MiB by default, the broker's default maximum message size) is
refused the same way (`returned a result too large to send as a reply`):
store a large result in blob storage and return its reference. This size
check applies only when the command has a `reply_to`; a command without
one publishes no reply, so a large result is acknowledged as usual. The same
applies to a dict nested so deeply that its reply cannot be serialized:
pydantic, which serializes replies, stops at 254 or 255 levels
(depending on its version; the returned dict is level 1), and the ERROR
line then says `not JSON-serializable (Circular reference detected
(depth exceeded))`. A request `payload` can be up to 255 levels deep
(see "Message Format"), so a handler that returns a deep payload, or
wraps it in another dict or list, can reach this limit: keep reply data
shallower, or put it in a blob and return its reference.

### AMQP Topology

The SDK automatically creates this topology based on `service_name`:

| Component | Name Pattern | Type | Description |
|-----------|--------------|------|-------------|
| Command Exchange | `cmd.[{tenant}.]{service_name}.v1` | TOPIC | Receives incoming commands |
| Command Queue | `queue.[{tenant}.]{service_name}.v1` | - | Service consumes from here |
| DLQ Exchange | `dlq.[{tenant}.]{service_name}.v1` | TOPIC | Failed message routing |
| DLQ Queue | `dlq.queue.[{tenant}.]{service_name}.v1` | - | Stores failed messages (see "Dead-letter queue" below) |

**Multi-tenant note:** when `KBM_LEDSAS_TENANT` is set, the tenant is
inserted *after* the prefix — `cmd.{tenant}.{service_name}.v1` — not at
the very start of the name. Adding or changing the tenant after
deployment requires re-declaring topology (the broker refuses to
redeclare an existing queue with a different name) and re-issuing
publisher-side ACLs.

**Example:** For `ServiceApp(service_name="csv-processor")`:
- Command exchange: `cmd.csv-processor.v1` (or `cmd.acme.csv-processor.v1` with `KBM_LEDSAS_TENANT=acme`)
- Command queue: `queue.csv-processor.v1` (or `queue.acme.csv-processor.v1`)

**Dotted names can share a queue.** A service name and a tenant may
contain `.`, which is also the separator in these names, so different
settings can give the same names: tenant `acme` with service
`csv-processor`, and no tenant with service `acme.csv-processor`, both
use `queue.acme.csv-processor.v1` (as do tenant `a.b` with service `c`
and tenant `a` with service `b.c`). Services that use the same names on
the same broker virtual host consume from the same queue, each getting
some of the commands. The tenant is a naming prefix, not an isolation
boundary: keep tenant and service names free of `.`, or give each tenant
a virtual host (and broker user) of its own.

**Queue type and high availability.** The SDK declares both queues
durable and passes no `x-queue-type`, so they get the virtual host's
default queue type: a classic queue unless you set another. For queues
replicated across a RabbitMQ cluster, set `default-queue-type` to
`quorum` on the virtual host **before** the service first starts (for
example `rabbitmqctl add_vhost <vhost> --default-queue-type quorum`, or
`rabbitmqctl update_vhost_metadata <vhost> --default-queue-type quorum`
for an existing one); the SDK's queue arguments (the dead-letter
exchange, and the message TTL and length limit of the dead-letter
queue) work on quorum queues. Retries put a command back on its queue
(see "Retry Mechanism"), so on a quorum queue each retry counts toward
the queue's delivery limit (20 by default on RabbitMQ 4.x, set with the
`delivery-limit` policy): keep `KBM_LEDSAS_MAX_RETRIES` well below it,
or the broker dead-letters the command itself, before its last attempt
and without a reply. A queue's type and arguments are fixed when it is
created, and the broker refuses a declaration that differs (see "A
queue that already exists with other settings" under "Logging"): to
change them, drain the queue, delete it, and let the service create it
again on its next start.

#### Dead-letter queue

The SDK declares the dead-letter queue with two limits: a message
expires after **7 days** (`x-message-ttl` 604800000 ms), and the queue
holds at most **100,000** messages (`x-max-length`). Both are silent:
an expired message is dropped, and at the limit RabbitMQ drops the
**oldest** message to make room for a new one (its default overflow
behavior). Monitor the queue's depth and handle dead letters well
within 7 days.

These two arguments are fixed: the SDK always declares the queue with
exactly them, and there is no setting to change them. To also cap the
queue's size in bytes, or to lower either limit, apply a broker
**policy**, which leaves the queue's arguments as they are (where a
policy and an argument both set a limit, RabbitMQ applies the lower),
for example
`rabbitmqctl set_policy dlq-bytes '^dlq\.queue\.' '{"max-length-bytes": 1073741824}' --apply-to queues`.
A dead-letter queue that already exists with other arguments (declared
by hand or by another tool) is not changed: the broker refuses the
SDK's declaration with `PRECONDITION_FAILED` and the start fails (see
"A queue that already exists with other settings" under "Logging").

A dead-lettered message carries the broker's `x-death` header (reason
`rejected`, the queue, the time) but not the SDK's reason. The reason is
in the service log. Take the `correlation_id` from the dead-lettered
body's envelope and search the log for it: the record that holds the
reason carries it (for example ERROR `Permanent error in handler` with
its `error_message`, WARNING `Deadline already exceeded`, ERROR `Max
retries (3) exceeded, sending to DLQ`). Every attempt of a command keeps
its `correlation_id` (its retries, and a replay: see below), so several
records can match; the `Received command` record of each delivery pairs
its `message_id` with the `correlation_id`. A command refused when it
arrives (too large, unreadable, an invalid envelope, a duplicate
`message_id`) never runs: it has one record, with the reason. What else
that record holds depends on how much of the body could be read. For a
duplicate `message_id` or an envelope whose `type` is not `command`, it
holds the command's `message_id` (search for the dead-lettered body's
`envelope.message_id`). For an envelope that failed validation, the
record carries the body's `envelope.message_id` and
`envelope.correlation_id` when they could be read (each a plain id of at
most 128 characters), even though another field was invalid, so the
`correlation_id` search finds it too. For a body that could not be read
at all (too large, not UTF-8, not JSON, nested more than 256 levels
deep, or holding an integer literal longer than 4300 digits), the
record has no id from the body: it carries the message's AMQP
`message_id` / `correlation_id` properties if the sender set them
(`amqp_message_id` / `amqp_correlation_id`) and the body's size
(`body_bytes`; see "Message Format").

The dead-letter queue keeps the **full command body** for up to 7
days. If command bodies can carry personal or health data, treat the
queue like any other store of that data (who can read it, how long it
is kept), or keep such data out of bodies (send a blob reference).

To replay a dead-lettered command once its cause is fixed, take it from
the dead-letter queue and publish its body again to the command
exchange with a **new** `message_id` (keep the `idempotency_key`; see
"Use a new `message_id` for every command"), then acknowledge it. A
minimal replay with aio-pika, run with the service running. It replays
the messages that are in the queue when it starts, each once, and skips
the ones that are not readable commands. Save it as `replay_dlq.py` and
run it with the service name (`python replay_dlq.py csv-processor`;
with a tenant, `<tenant>.<service>`); it reads the broker URL from
`KBM_LEDSAS_RABBITMQ_URL`, like the service:

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
                envelope = body["envelope"]
                envelope["message_id"] = str(uuid.uuid4())
                reply_to = envelope.get("reply_to")
                data = json.dumps(body).encode()
            except Exception:  # not JSON, no envelope, nested too deep to parse, ...
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

A replayed command that fails again is dead-lettered again and stays in
the queue for a later run. That happens while its cause is not fixed,
and always for a command whose envelope was invalid or whose `deadline`
has passed: the replay keeps the envelope, deadline included, so the
sender has to send such a command again (with a new deadline). Each
replay of a command with a `reply_to` sends the caller another reply,
with the same `correlation_id`, if the caller's reply exchange still
exists. Often it does not: the example's `scripts/send_hello.py` deletes its
reply queue and exchange when it exits, while the
dead-letter queue keeps a command for 7 days. The service would then run
the command, fail to send its reply, and dead-letter it again, and every
later replay would repeat its work. So the recipe checks each command's
`reply_to` exchange (on a channel of its own, since a missing exchange
closes the channel it is checked on) and replays a command whose
exchange is gone with `reply_to` set to `MISSING_REPLY_TO`: empty (the
default) sends no reply, so the command runs once and is acknowledged;
or name a reply exchange you own (with a queue bound to it for the
`response` routing key) to collect those replies. A `reply_to` the
service refuses (one naming a `cmd.`, `dlq.` or `amq.` exchange) is left
as it is, so that command is still an invalid envelope, dead-lettered
again. When the broker cannot be reached or refuses the credentials, the
recipe prints one `error:` line and exits with code 1.
The run then prints
`N of them replayed without a reply: their reply exchange no longer
exists (<names>)` (`with reply_to <name>` when `MISSING_REPLY_TO` is
set). A body that is not a readable command
(not UTF-8, not JSON, no envelope, or nested more deeply than Python's
JSON parser reads: the service dead-letters such bodies on arrival) is
never replayed: the recipe holds it until the end of the
run, so the commands behind it are still replayed, and then leaves it in
the queue. To inspect and remove such messages, run the recipe with
`REMOVE_UNREADABLE = True`: each one is saved to a
`dlq-unreadable-<uuid>.bin` file in the current directory and removed
from the queue. The files are readable by your user only (mode 0600):
a command body can hold personal data, so delete them once you have
looked at them.

### Sending Messages to a Service

To send messages to a LEDSAS service, publish to its command exchange.
This sender targets the README's quickstart service (`csv-processor`,
handler `ProcessCSV`); change the exchange, `name` and payload for yours.
It uses `aio-pika`, the AMQP client the SDK itself depends on, so it
runs in the SDK's environment with nothing else to install:

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
- Exchange: `cmd.{service_name}.v1` (TOPIC type)
- Routing key: any non-empty string — the queue binds with pattern `#`,
  so every routing key matches. Use the literal `"command"` for
  consistency with the examples.
- `envelope.name` MUST match the handler's command name (e.g.,
  `@app.handler("ProcessCSV")`).
- Publisher confirms, `on_return_raises=True` and `mandatory=True` make a
  command that cannot be delivered fail the sender (exit code 1, one
  `error:` line) instead of vanishing: the command exchange is declared
  by the service when it starts, so before the first start there is
  nothing to publish to. A broker that cannot be reached, or that refuses
  the user name and password, also ends the sender with one `error:` line
  naming the cause.
- This sender does not wait for the reply. A caller that reads the reply
  declares its own reply exchange and queue and sets `reply_to` (see
  "Response Message Structure").
  See `examples/hello_world_service/scripts/send_hello.py` for a runnable caller.

---

## ExecutionContext

The `ExecutionContext` is passed to every handler function and provides
access to message metadata, blob operations, status reporting, and
structured logging.

### Properties

The following read-only properties are exposed directly on `ctx`. Every
envelope field that has a direct property is also reachable via
`ctx.envelope.<field>` — pick whichever reads better at the call site.

| Property | Type | Equivalent | Notes |
|---|---|---|---|
| `ctx.message_id` | `str` | `ctx.envelope.message_id` | UUID for this specific message |
| `ctx.correlation_id` | `str` | `ctx.envelope.correlation_id` | Correlate related messages |
| `ctx.idempotency_key` | `str` | `ctx.envelope.idempotency_key` | Stable across retries |
| `ctx.trace_id` | `str` | `ctx.envelope.trace_id` | opaque distributed-tracing ID (URL-safe) |
| `ctx.deadline` | `datetime \| None` | `ctx.envelope.deadline` | Handler deadline, timezone-aware, as sent (it keeps the sender's UTC offset; compare it with an aware `datetime.now(timezone.utc)`) |
| `ctx.job_id` | `str \| None` | `ctx.envelope.job_id` | Business-level job id |
| `ctx.envelope` | `Envelope` | — | Full envelope object, read-only: assigning a field raises `pydantic.ValidationError` (use `ctx.envelope.model_copy(update={...})` for a changed copy) |
| `ctx.payload` | `dict` | — | Same dict the handler receives as `req` |
| `ctx.blob` | `BlobOperations` | — | See "BlobOperations" below |
| `ctx.logger` | `logging.Logger` | — | See **Logging** note |

#### Logging note

`ctx.logger` is a stdlib `logging.Logger` (`handler.<envelope.name>`),
whose records propagate to your root logging configuration. The SDK
attaches only its secret masking to it (its records are masked like the
SDK's own, see "Secrets in these records are masked" under "Error
Handling"); it does **not** otherwise configure it (the SDK configures
only its own `kbm_ledsas_sdk.*` loggers): without a
logging configuration of your own, `ctx.logger.info(...)` prints
nothing (its WARNING and ERROR records are written in the SDK's format,
see "When you configure no logging yourself" under "Logging"). Configure
the root logger once at startup, for example with the SDK's JSON format
(see `json_log_formatter()` below):

```python
import logging
import kbm_ledsas_sdk

handler = logging.StreamHandler()
handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.captureWarnings(True)  # Python warnings as JSON records too
```

Structured fields go in `extra={...}`,
**not as keyword arguments**:

```python
# ✓ correct
ctx.logger.info("Processing", extra={"correlation_id": ctx.correlation_id})

# ✗ raises TypeError (Logger._log() got an unexpected keyword argument)
#   whenever INFO is enabled: the command fails as an UnexpectedError, is
#   retried and then dead-lettered (with INFO off it is not noticed at all)
ctx.logger.info("Processing", correlation_id=ctx.correlation_id)
```

**Payload-field safety:** payload fields are caller-controlled and
may contain ANSI escape sequences, embedded newlines, or other control
characters. Always pass them via `extra={...}` rather than interpolating
into the message string — the JSON log formatter escapes control
characters in `extra` values, but a string-interpolated message can
let a hostile caller manipulate an operator's terminal (color/cursor
moves, fake log lines, bell spam). And validate a value before you log
it: a mistyped blob URI may be an https URL with a SAS signature (a
credential), which the SDK's own refusals never repeat. Log the parsed
`BlobRef`'s `.uri` instead (it cannot carry a query other than
`versionId`):

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

**Seeing your `extra={...}` fields in your own logs (`json_log_formatter()`).**
The SDK auto-formats only its own `kbm_ledsas_sdk.*` namespace; a plain
`logging.basicConfig(...)` percent-format formatter renders the message
string but **never** renders the `extra={...}` fields — so the structured
fields you moved into `extra` for safety are invisible in *your* logs.
The public helper `kbm_ledsas_sdk.json_log_formatter()` returns the SDK's
JSON formatter; attach it to your own handler to emit the same JSON shape
(with your `extra` fields) the SDK's loggers use:

```python
import logging, kbm_ledsas_sdk

handler = logging.StreamHandler()
handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
```

**Signature:** `json_log_formatter(datefmt: str | None = None)`. Without
`datefmt` the `timestamp` field is UTC, ISO 8601 with milliseconds and a
`Z` (see "Timestamps" under "Logging"). A `datefmt` (a `time.strftime`
format) replaces that form: the timestamp is then written in that
format in the **local** time zone, as `logging.Formatter` writes it, so
leave it unset to keep your records in line with the SDK's.

#### Examples — `message_id` vs `idempotency_key` for output paths

Use `ctx.idempotency_key` (not `ctx.message_id`) when computing output
blob paths or any other write-once key: idempotency_key is stable
across DLQ replays of the same logical request, message_id is a fresh
UUID on every send.

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

Business-level identifier set by the caller. Both `job_id` and
`correlation_id` are set by the caller and echoed back exactly as sent:
`correlation_id` matches a reply to its command, while `job_id` is your
own business identifier, echoed back in responses, status updates, and
errors. Use it for batch/workflow tracking. Optional and backwards-compatible — handlers
that ignore it keep working.

```python
@app.handler("ProcessBatch")
async def handle(ctx, req: dict) -> dict:
    if ctx.job_id:
        ctx.logger.info("Processing job", extra={"job_id": ctx.job_id})
    return {"status": "success", "job_id": ctx.job_id}
```

### Methods

`emit_status` is a coroutine: call it as `await ctx.emit_status(...)`.
Called without `await` it does nothing: no update is sent, the handler
goes on, and the only trace is Python's `RuntimeWarning: coroutine
'ExecutionContext.emit_status' was never awaited`.

#### `async emit_status(stage: str | Enum, progress: float, note: str | Enum | None = None) -> None`

Report processing progress back to the caller.

The update is published to the command's `reply_to` exchange with
routing key `status` (a command without `reply_to` sends none). It is
best-effort: the caller receives it only if a queue is bound to that
exchange for `status` as well as `response`; without
that binding the update is unroutable and the service logs one WARNING
(`Failed to send status to reply_to exchange ...`) per update, and the
handler continues.

**Parameters:**
- `stage` (str): Current processing stage (e.g., "downloading", "processing", "uploading")
- `progress` (float): Completion fraction in `[0.0, 1.0]`
- `note` (str, optional): Human-readable status message

An `Enum` member whose value is a `str` (`class Stage(Enum): DOWNLOADING
= "downloading"`) is accepted for `stage` and `note` and sent as that
value. `Enum` in the signature means such a member: a type checker
accepts it and reports an `IntEnum` member (or any `Enum` with values
that are not text) as an error, as the call refuses it.

**Raises:**
- `TypeError` if `stage` is not a `str`, or `note` is neither `None` nor a `str`
  (`emit_status() takes stage as a str, got int`; pass `str(n)` for a number).
- `ValueError`: if `progress` is outside `[0.0, 1.0]`, `TypeError` if it
  is not a number (`True` and `False` are refused too) — validated at
  call time, before anything is published. Uncaught, either is replied as `Permanent` (not retried).
- `ValueError`: if `stage` or `note` holds a lone surrogate (text UTF-8
  cannot encode, e.g. `"\ud800"` from a JSON request), also before
  anything is published (see "Payload": **Text**).

**Example:**
```python
await ctx.emit_status("downloading", 0.0, "Starting download")
await ctx.emit_status("processing", 0.5, "Halfway through CSV rows")
await ctx.emit_status("done", 1.0, "Processing complete")
```

### Envelope properties (useful members of `ctx.envelope`)

```python
ctx.envelope.name           # Message name (e.g., "ProcessData")
ctx.envelope.message_version # Message schema version
ctx.envelope.sent_at        # When message was sent (timezone-aware, as sent: the sender's UTC offset)
ctx.envelope.trace_id       # Distributed tracing ID
```

---

## BlobOperations (8 APIs)

Access via `ctx.blob` for all blob storage operations.

**Await every method except `download_stream`.** The upload and download
methods are coroutines (`ref = await ctx.blob.upload_bytes(...)`, `data =
await ctx.blob.download_bytes(...)`; their headings below say `async`).
Called without `await`, such a method does nothing: nothing is written or
read, and Python warns `coroutine ... was never awaited`. `download_stream`
is different: it returns an async iterator, which you read with `async
for` (see its example) and do not await.

**Stored content type and generated names.** `upload_json` stores the
blob with the content type `application/json`, and `upload_text` with
`text/plain; charset=utf-8` (what a browser, a download tool or a
storage browser sees). `upload_bytes` and `upload_stream` store the
storage service's default, `application/octet-stream`. When `path` is
omitted, the generated blob name is a random UUID with a suffix that
matches: `<uuid>.json`, `<uuid>.txt`, or `<uuid>.bin` for bytes and
streams. Give a `path` to choose the name.

**Container allowlist.** When `KBM_LEDSAS_ALLOWED_CONTAINERS` is set,
every method below also raises `errors.ContainerNotAllowed` for a
container the setting does not list (an upload by its `container`
argument, a download by the container of its URI), before anything is
sent to storage (see "Container allowlist" under "Security model").

### Bytes Operations (2)

#### `async upload_bytes(container: str, data: bytes | bytearray | memoryview, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload raw bytes to blob storage.

**Parameters:**
- `container` (str): Target container name
- `data` (bytes, bytearray or memoryview): Raw bytes to upload
- `path` (str, optional): Blob path within container. If omitted (or
  `None`, or the empty string `""`), the SDK auto-generates a UUID path.
  The path is used as the blob name as given (one leading `/` is
  ignored) and must be representable in the returned `azblob://` URI:
  at most 1024 characters (one leading `/` is not counted), no `?` or
  `#`, no control characters (C0, DEL and C1, `U+0080` to `U+009F`),
  Unicode format characters, line and paragraph separators (`U+2028`,
  `U+2029`) or a lone surrogate (`\ud800`, which UTF-8 cannot encode;
  a JSON request can carry one), no backslash (`\`;
  storage may take it as `/`, so the
  returned URI would not name the blob written: separate segments with
  `/`), no empty segment (`a//b`, or more than one leading `/`), no
  `.` or `..` segment, and no `/` at the end (`dir/`). Any other
  character (letters, digits, `/`, `-`, `_`, `.`, spaces, `%`,
  non-ASCII letters) is accepted. An invalid path raises `ValueError`
  **before anything is written** (applies to every `upload_*` method).
- `overwrite` (bool, default `False`): If `True`, replace an existing
  blob at `path`. Default `False` refuses to clobber existing blobs
  (raises `azure.core.exceptions.ResourceExistsError`). Set `True` for
  the idempotency-key replay pattern (see "Examples — `message_id` vs
  `idempotency_key` for output paths" above). It must be
  `True` or `False` (applies to every `upload_*` method): any other value
  (`"false"`, `"no"`, `0`, `None`) raises `TypeError` before anything is
  written, so a flag read from the environment or a request field as text
  cannot replace a blob by accident (convert it first, e.g.
  `overwrite=value.lower() == "true"`).

**Returns:** `BlobRef` (carries the blob `uri`; `version_id` is populated
when the container has versioning enabled)

**Raises:**
- `ValueError`: `path` is not a valid blob path (see above). Nothing is
  written; uncaught, it is replied as `Permanent` (not retried).
- `TypeError`: `overwrite` is not `True` or `False` (`overwrite must be
  True or False, got str`). Nothing is written; uncaught, it is replied
  as `Permanent` (not retried). The same for every `upload_*` method.
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

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
Download blob as raw bytes.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef object

**Returns:** bytes

**Raises** (the same for every `download_*` method):
- `ValueError` (a pydantic `ValidationError`): `blob_ref` is not a valid
  `azblob://<container>/<path>[?versionId=...]` URI (see `BlobRef`).
- `azure.core.exceptions.ResourceNotFoundError`: the blob (or the
  requested version) does not exist.
- `azure.core.exceptions.HttpResponseError`: other storage errors
  (authentication, throttling, server errors).

If your handler lets one of these propagate, a missing blob, an invalid
URI, any other storage 4xx except 408/429, and (for `download_json`)
content that is not JSON are replied as `Permanent` and **not retried**;
throttling, timeouts and server errors are retried (see "Error
Handling"). Catch them yourself to reply with your own message.

**Example:**
```python
data = await ctx.blob.download_bytes("azblob://input/file.bin")
print(len(data))  # Size in bytes
```

### JSON Operations (2)

#### `async upload_json(container: str, obj: dict | list, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload Python dict/list as JSON blob.

**Parameters:**
- `container` (str): Target container
- `obj` (dict | list): JSON-serializable Python object
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
- `TypeError` (or `ValueError` for a circular reference): if `obj` has no
  JSON form. Nothing is written. Uncaught, it is logged once (the
  handler layer's one ERROR, no traceback, with the text
  `upload_json() got an object with no JSON form (...)`) and replied as
  `Permanent` (not retried).
- `TypeError`: `obj` is not a dict or a list (a number, a string, `True`,
  `None`, a tuple, ...), before anything is serialized or sent:
  `upload_json() takes a dict or a list (the root of the JSON document),
  got <type>; use upload_text() or upload_bytes() for other content`. Not
  retried (`Permanent` when uncaught). Earlier releases uploaded such a
  value.
- `TypeError`: `overwrite` is not `True` or `False` (see `upload_bytes`).
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

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
Download and parse JSON blob.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef

**Returns:** Parsed JSON object (dict or list)

**Raises:** as `download_bytes`, plus `json.JSONDecodeError` (a
`ValueError`) when the content is not valid JSON, `UnicodeDecodeError`
when it is not UTF-8, and for valid JSON that Python's parser cannot
read: `ValueError` when it holds an integer with more than 4300 digits
(Python's limit on converting text to an integer) and `RecursionError`
when it is nested more levels deep than the parser supports (about 1,000
levels on Python 3.11, about 10,000 on Python 3.13). Each is logged once
(ERROR `Failed to parse blob as JSON`, no traceback) and, uncaught, is
replied as `Permanent` (not retried): the same content fails the same
way again. The reply message says `blob content is not valid JSON (...)`
or, for the last two, `blob content cannot be parsed as JSON (...)`.
Valid JSON whose root is not an object or an array (a number, a string,
`true`/`false`, `null`) raises a `ValueError` (`Blob <uri> holds JSON
whose root is <kind>: download_json() returns a dict or a list; use
download_text() or download_bytes() for other content`), logged once as
the ERROR `Blob content is JSON but its root is <kind>, not an object or
an array` and, uncaught, replied as `Permanent` (not retried). Earlier
releases returned such a value.

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
Upload text string to blob storage.

**Parameters:**
- `container` (str): Target container
- `text` (str): Text content to upload (must be a string)
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
- `TypeError`: If `text` is not a string (e.g., passing a list or dict).
  Use `upload_json()` for dict/list data. Uncaught, it is replied as
  `Permanent` (not retried).
- `TypeError`: `overwrite` is not `True` or `False` (see `upload_bytes`).
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

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
Download blob as text string.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef

**Returns:** Text string (decoded as UTF-8)

**Raises:** as `download_bytes`, plus `UnicodeDecodeError` when the
content is not UTF-8. Uncaught, it is replied as `Permanent` (not
retried): the same content fails the same way again.

**Example:**
```python
content = await ctx.blob.download_text("azblob://logs/app.log")
for line in content.split("\n"):
    if "ERROR" in line:
        print(line)
```

### Streaming Operations (2)

#### `async upload_stream(container: str, stream: AsyncIterable[bytes | bytearray | memoryview], path: str | None = None, progress_callback: Callable[[int], None] | None = None, overwrite: bool = False) -> BlobRef`
Upload data from async byte stream (for large files).

**Parameters:**
- `container` (str): Target container
- `stream` (AsyncIterable[bytes | bytearray | memoryview]): Async iterable (such as an async generator) yielding chunks. Each
  chunk must be `bytes` (`bytearray` and `memoryview` are accepted too);
  encode text first (`text.encode()`, or use `upload_text`).
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `progress_callback` (callable, optional): Called with the cumulative
  number of bytes uploaded so far after each chunk.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
- `TypeError`: a chunk is not `bytes`, `bytearray` or `memoryview`
  (`upload_stream() got a chunk of type str; the stream must yield
  bytes`). A `str` chunk is refused too: it is not stored UTF-8 encoded.
  The upload stops at that chunk and the blob is not written; uncaught,
  it is replied as `Permanent` (not retried).
- `TypeError`: `overwrite` is not `True` or `False` (see `upload_bytes`).
- An exception the `stream` itself raises (your generator failed while
  reading its source) is passed on unchanged, and the blob is not
  written. The SDK does not log it as a storage failure: uncaught, it is
  reported once by the handler layer like any exception of your handler
  (see "Uncaught exceptions").
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

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
Download blob as async byte stream (for large files). Not a coroutine:
read the iterator it returns with `async for` (no `await`).

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef
- `chunk_size` (int, default `4194304` = 4 MiB): Size of each yielded
  chunk, in bytes (at least 1). Every chunk is exactly `chunk_size`
  bytes except the last, which may be shorter. The blob is read with one
  ranged request per chunk, so memory use is bounded by `chunk_size`
  however large the blob is. Without a version, every request is pinned
  to the blob's ETag at the start: a blob overwritten mid-download fails
  the download instead of mixing old and new content. That failure
  (`azure.core.exceptions.ResourceModifiedError`, HTTP 412) is not a
  refusal of the request: uncaught in the handler, the command is
  retried (one WARNING, `Transient error in handler: the blob changed
  while it was being downloaded ...`), and the new attempt reads the
  new content. To read one fixed content even while the blob is
  replaced, pass a URI with `?versionId=...` (storage accounts with
  blob versioning).
- `progress_callback` (callable, optional): Called with
  `(bytes_downloaded_so_far, total_bytes)` after each chunk.

**Returns:** AsyncIterator[bytes] yielding chunks

**Raises:** as `download_bytes`; `ValueError` if `chunk_size` is not a
positive integer.

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

`errors.SDKError` is the base of the errors a handler raises to steer
retries: `Retryable`, `Permanent` and `DeadlineExceeded`. Every
`errors.SDKError` carries `user_message` as an attribute (the
caller-facing message, or `None`), so `except errors.SDKError as e:
e.user_message` works for each of them; a bare `DeadlineExceeded()` has
the message `Processing deadline exceeded`. Raise one of
those three; raising a bare `errors.SDKError` (or a subclass of your
own) is handled like any uncaught exception: retried, then replied as
`UnexpectedError` (see "Uncaught exceptions"). Catching
`errors.SDKError` catches only these error types, in code of yours that
raises them (shared metric, common cleanup); it does not catch what the
SDK's own calls raise. The SDK's blob calls and `emit_status` raise the
storage, validation and built-in exceptions listed per method:

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

The sections below give the details. Catch a storage exception yourself
when you want another outcome, and raise `errors.Retryable` or
`errors.Permanent` from it:

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

> **Backend outages and concurrency.** A blob operation against
> unreachable blob storage does not fail instantly: the storage client
> retries it first (2 retries, about 3 s and 5 s apart), waits up to 10 s
> to open each connection, and gives up on an attempt after 10 s without
> any answer (the read timeout restarts with each piece of the answer,
> so it bounds silence, not the time a large transfer takes). Against a
> refused connection one call fails after about 8-10 s; against storage
> that does not answer at all (a host that drops the packets, or one
> that accepts the connection and never answers, like a stalled proxy)
> after about 40 s (3 attempts of 10 s plus the backoff). The handler
> then fails and is retried like any
> other unexpected error, so with the default `KBM_LEDSAS_MAX_RETRIES=3`
> the command's single reply arrives after about 4 x that time plus the
> backoff (about 40-50 s for a refused connection, about 3 minutes for
> storage that does not answer), and the command is then dead-lettered. Each failed attempt logs one WARNING,
> `Transient error in handler: blob storage not reachable (...)`, never
> with a traceback (the storage client's own line, `Failed to download
> blob (transient storage error)` or its upload / stream counterpart, is
> INFO). If storage comes back meanwhile, the next attempt
> succeeds, with no message loss. Size your caller's reply timeout for
> this (allow 30 s or more), or set `KBM_LEDSAS_HANDLER_TIMEOUT` (or lower
> `KBM_LEDSAS_MAX_RETRIES`) so a blocked handler is cancelled with a
> `Timeout` sooner. While a call is retrying it occupies
> one of your `KBM_LEDSAS_CONCURRENCY` worker slots.

### Uncaught exceptions

An exception your handler does not catch is treated as
`UnexpectedError`: it is retried like `Retryable` and, once
`KBM_LEDSAS_MAX_RETRIES` is spent, dead-lettered with the reply
`{"code": "UnexpectedError", "message": "An unexpected error occurred",
"retryable": true}`. This covers every exception class, not only
`Exception` subclasses: an `asyncio.CancelledError` the handler raises
(for example by awaiting a task or future that something cancelled) and
a custom `BaseException` subclass are handled the same way, so a command
is always settled with its one reply and never left unacknowledged. An
exception whose own `str()` fails is reported by its class name. The
handler layer logs each attempt as one WARNING, not an ERROR:

- a transient storage or network error (blob storage unreachable or not
  answering, an HTTP 408, 429, 500, 502, 503 or 504 from storage, a
  Python `ConnectionError`, a blob overwritten while `download_stream`
  read it): `Transient error in handler: <reason>`, never with a
  traceback;
- any other exception: `Uncaught exception in handler`, with the
  exception class, its message and where it was raised (the innermost
  frame: for an exception raised inside a library, such as the storage
  client, that is the library's line, not your handler's). The
  traceback is attached only on the command's **last** attempt, so a
  command that succeeds on a retry logs none. This holds per set of
  retries in one service process: a command delivered again after its
  last attempt could not be dead-lettered (lost broker connection), or
  redelivered to another replica or to a restarted process, can log the
  traceback again on that delivery's last attempt.

**Secrets in these records are masked.** An exception's text comes from
code the SDK does not control and may quote a credential (a client
library's error that repeats a connection URL or a connection string).
Every record the SDK writes masks the secrets listed below, in either log
format: the records of the handler layer (`kbm_ledsas_sdk.runtime.handler`:
the records above, and the `Permanent` / `Retryable` / non-retryable /
timeout records below), those of every other SDK module
(`kbm_ledsas_sdk.*`, for example the storage client's `Failed to
download blob` ERROR and its traceback) and the records of the AMQP
client library that the SDK filters (`aio_pika`, `aiormq`; see "Library
log records" under "Logging"). A record is masked in its message, in
every text field (`error`, `error_message`, `internal_message`,
`reason`, `uri`, ...), in its traceback and in its stack. The secrets are
the user name and password of a URL of any scheme
(`amqp://***:***@host`, `https://***:***@host`; everything before the
`@` that ends them, also when the password holds an unencoded `#`, `?`
or `/`), every query value and the fragment of a broker URL
(`amqps://host/?password=***`), the values of `AccountKey=` /
`account_key=`, `SharedAccessSignature=`, `SharedAccessKey=` and
`Password=` / `Pwd=`, quoted or not (`AccountKey="***"`,
`'account_key': '***'`), a `Bearer` token (`Authorization: Bearer ***`),
and the values of the `sig=`, `code=`, `access_token=` and `token=` URL
query parameters (`***`). The secrets the service is configured with
(the broker password of `KBM_LEDSAS_RABBITMQ_URL`, also percent-encoded,
and the `AccountKey` and SAS signature of `KBM_LEDSAS_BLOB_CONN_STRING`)
are masked wherever they appear, in any form, in every record and every
reply text of the process (each such value of 6 characters or more). A field that holds a dict, a list, a tuple or a
set is masked in a copy, item by item (a value too deeply nested or too
large to walk is replaced by `<withheld: too deeply nested or too large to
mask>`). The masking fails closed: a text it cannot mask is
replaced by a placeholder (`<withheld: secret masking failed>` for a
field, `<record text withheld: secret masking failed>` for the message,
`<traceback withheld: secret masking failed>` for the traceback), and
the record is still written. Other secrets (a token without a
`Bearer` prefix, an API key) are not recognised. `ctx.logger`'s records
are masked too; the records of your own loggers are not, except the
WARNING and higher records that the SDK's last-resort handler writes
when you configure no logging yourself (see "When you configure no
logging yourself" under "Logging"). An error reply's internal message
(the handler's exception text, sent when no `user_message` is set and
`KBM_LEDSAS_GENERIC_ERRORS` is off) is masked the same way; a
`user_message` is sent to the caller as you wrote it.

Any other storage failure, one that is neither transient as above nor
an HTTP 4xx (for example an HTTP 501, 505 or 507, or a response cut off
mid-transfer), is retried as `Uncaught exception in handler`, and the
storage client also logs its own ERROR **with a traceback on every
attempt**: `Failed to download blob`, `Failed to upload blob`,
`Streaming download failed` or `Streaming upload failed`. An exception
that the stream you pass to `upload_stream` raises (your generator
failed reading its source) is not a storage failure: it is reported only
as your handler's own exception, with no `Streaming upload failed`
record.

A `TimeoutError` raised inside your handler (your own
`asyncio.wait_for`, or a client library's timeout) is retried like a
handler timeout and its one reply carries code `Timeout`, with the
message `Handler raised TimeoutError` (the SDK's own timeout says
`Handler timed out after <N>s`; see "Timeout Enforcement"). Each attempt
logs one WARNING `Handler raised TimeoutError` with `error_class`,
`error_message` and `error_location`. Catch it and raise
`errors.Retryable` or `errors.Permanent` if you want another outcome.

A handler that raises `SystemExit` (for example by calling `sys.exit()`)
or `KeyboardInterrupt` does not end the service: it is a `Permanent`
handler error (one ERROR `Non-retryable error in handler (not retried):
the handler raised SystemExit (3); a handler must not exit the process`,
one reply, dead-lettered, never retried), and the service keeps
consuming. A process that is killed or crashes outright (out of memory,
`os._exit()`, a crash in native code) is different; see "Shutdown".

Apart from those storage-client lines, the command's one ERROR is
`Max retries (N) exceeded, sending to DLQ`, when its retries are spent.
See "Logging" under "Generic Error Messages" for the record fields, and
"What to alert on" under "Logging" for alerting.

Exceptions that can never succeed on a retry are classified `Permanent`
instead (one ERROR line naming the cause, `Non-retryable error in
handler (not retried): <cause>`, no traceback, no retries; for content
that `download_json` / `download_text` cannot decode and for an object
`upload_json` cannot serialize, the blob call logs its own ERROR line
just before it):

- storage client errors with an HTTP 4xx status other than 408 and 429:
  a missing blob (`ResourceNotFoundError`), an invalid container or blob
  name, an authorization failure, an existing blob with
  `overwrite=False` (`ResourceExistsError`). Not the HTTP 412 of
  `download_stream`'s own consistency check (the blob changed while it
  was read): that one is retried;
- an invalid blob URI (`BlobRef` validation);
- content that is not valid JSON in `download_json` (or valid JSON that
  Python's parser cannot read: an integer with more than 4300 digits, or
  nesting deeper than the parser supports), or not UTF-8 text in
  `download_text`;
- an invalid argument to an SDK call: a `blob_ref` that is neither a
  string nor a `BlobRef` (for example `None` from a request field that is
  missing; `TypeError`), an `upload_*` `container` that is not a valid
  container name or `path` that is not a valid blob path, `upload_bytes`
  given something other than `bytes`, `bytearray` or `memoryview` (a
  `str` belongs in `upload_text`), `upload_stream` given something that
  is not an async iterable or a stream that yields a chunk other than
  `bytes`, `bytearray` or `memoryview` (a `str` chunk too), an `upload_*`
  `overwrite` that is not `True` or `False`, `upload_text` given a non-string or text with
  a lone surrogate (e.g. `"\ud800"`, which UTF-8 cannot encode),
  `upload_json` given an object with no JSON form (or text with a lone
  surrogate), `emit_status` with a `stage` that is not a `str` or a `note` that is
  neither `None` nor a `str`, a `progress` that is not a number in
  `[0.0, 1.0]` (or a `stage` / `note` with a lone surrogate), `download_stream` with
  a `chunk_size` that is not a positive integer. These are refused before
  anything is sent to storage;
- a handler that turns out not to be async (registration refuses a plain
  function; a callable object whose call returns a value instead of an
  awaitable is reported here).

A handler that **returns** something other than a JSON-serializable dict
(or `None`) is also replied as `Permanent` without retries; see
"Payload".

### `errors.Retryable`
Raised for transient errors that should be retried with backoff.

**Use Cases:**
- Network timeouts
- Rate limiting (HTTP 429)
- Temporary service unavailability
- Transient blob storage errors

**Constructor:**
```python
errors.Retryable(message: str, user_message: str | None = None)
```

**Parameters:**
- `message` (str): Internal error message (logged, may be hidden from caller)
- `user_message` (str, optional): Custom user-facing message

The caller sees this error only in the command's one reply, sent after
the last retry, when the command is already in the dead-letter queue.
Word `message` and `user_message` for that moment: they reach the
caller (`message` unless `KBM_LEDSAS_GENERIC_ERRORS` is on), so do not
promise a further attempt ("will retry", "retrying..."); say what failed
and that the caller can resubmit later.

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

**SDK Behavior:**
- Automatic exponential backoff: `min(2^retry_count + jitter, 60.0)` seconds (jitter ∈ [0, 1))
- Configurable via `KBM_LEDSAS_MAX_RETRIES` — number of retries NOT
  counting the initial attempt. Default `3` → 4 total attempts.
- No reply for an attempt that is retried; after max retries the caller
  gets one reply with this error and the message goes to DLQ
- Logged at WARNING on every attempt (`Retryable error in handler`, with
  `error_message`, no traceback); the command's one ERROR is
  `Max retries (N) exceeded, sending to DLQ`

### `errors.Permanent`
Raised for non-recoverable errors that should go to Dead Letter Queue.

**Use Cases:**
- Invalid input data
- Missing required fields
- Schema validation failures
- Business logic violations

**Constructor:**
```python
errors.Permanent(message: str, user_message: str | None = None)
```

**Parameters:**
- `message` (str): Internal error message (logged, may be hidden from caller)
- `user_message` (str, optional): Custom user-facing message

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

**SDK Behavior:**
- Message sent to DLQ immediately
- No retries attempted
- Includes error details for debugging (unless `generic_errors=true`)
- Logged once at ERROR (`Permanent error in handler`, no traceback); the
  "sending to DLQ" line that follows is INFO

### `errors.DeadlineExceeded`
Raised when processing cannot complete within deadline.

**Use Cases:**
- Not enough time remaining to start safely
- Long-running operations that can't finish
- Resource contention delays

**Constructor:**
```python
errors.DeadlineExceeded(message: str = "Processing deadline exceeded", user_message: str | None = None)
```

Symmetric with `Retryable` / `Permanent`: when `user_message` is set,
the caller always sees it (regardless of `KBM_LEDSAS_GENERIC_ERRORS`).

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

**SDK Behavior:**
- **Not retried** — the deadline is already in the past, so a retry
  would just exceed it again. Message goes to DLQ. The caller
  is expected to resend with a fresh deadline if appropriate.
- Logged at WARNING (not ERROR — the SDK has nothing to do here).
- A deadline that passes while the handler runs cancels the handler, and
  the reply is `DeadlineExceeded` (`Deadline <time> passed while the
  handler ran (after <N>s)`), not retried (see "Timeout Enforcement").
  `Timeout` is a different case: `KBM_LEDSAS_HANDLER_TIMEOUT` expired, or
  the handler raised `TimeoutError` itself; it is retried.

### `errors.ContainerNotAllowed`
Raised by `ctx.blob` when `KBM_LEDSAS_ALLOWED_CONTAINERS` is set and an
operation names a container it does not list (see "Container allowlist"
under "Security model"). It is a `PermissionError`, not an
`errors.SDKError`: the SDK raises it, your handler does not need to.

```python
errors.ContainerNotAllowed(message: str, container: str)
```

- `e.container` holds the refused container name.
- Message: `Blob upload refused: container 'other' is not in
  KBM_LEDSAS_ALLOWED_CONTAINERS (allowed: input, results); nothing was
  sent to storage` (`Blob download refused: ...` for a download).
- Raised before anything is sent to storage; for `download_stream` at the
  first iteration, like its other argument errors.
- **Not retried.** Uncaught, the caller gets one reply
  `{"code": "Permanent", "message": "container 'other' is not allowed
  (KBM_LEDSAS_ALLOWED_CONTAINERS)", "retryable": false}` (the generic
  `Processing failed` with `KBM_LEDSAS_GENERIC_ERRORS=true`); the allowed
  list is never sent to the caller.

Wrong-typed arguments to `@app.handler`, `@app.liveness_check` and
`@app.readiness_check` (a command name that is not a string, a timeout
that is not a number) raise a `TypeError` that is also a `ValueError`, so
an existing `except ValueError` still catches it.

---

## Timeout Enforcement

The SDK enforces handler execution timeouts to prevent runaway processing.

### Configuration

```bash
export KBM_LEDSAS_HANDLER_TIMEOUT=1800  # 30 minutes (default)
export KBM_LEDSAS_HANDLER_TIMEOUT=0     # Disable timeout
```

### Behavior

When `KBM_LEDSAS_HANDLER_TIMEOUT` > 0, or the command carries a
`deadline`, the handler runs with a time limit: the smaller of
`KBM_LEDSAS_HANDLER_TIMEOUT` and the time left to the deadline.

1. The SDK runs the handler inside `asyncio.timeout()` (Python 3.11+),
   in the same task.
2. If the handler exceeds the limit, the SDK cancels it (your handler
   sees `asyncio.CancelledError` at its current `await`; let it
   propagate, see "Cancellation Safety").
3. If the limit was `KBM_LEDSAS_HANDLER_TIMEOUT`, the timeout is
   retryable: the command is retried like `Retryable`, and the caller
   gets one reply with code `Timeout` and the message
   `Handler timed out after <N>s` only when the retries are spent (see
   "Retry Mechanism"). Each timed-out attempt logs one WARNING
   (`Handler timeout`, with `timeout_seconds`).
4. If the limit was the time left to the command's `deadline`, the
   deadline has passed: the reply is `DeadlineExceeded` (`Deadline <time>
   passed while the handler ran (after <N>s)`, not retried; one WARNING
   `Deadline exceeded in handler`), at once, since a retry would arrive
   after the deadline too.

A `TimeoutError` that your handler code raises itself (with the timeout
enabled or not) is retried like a timeout, with the reply message
`Handler raised TimeoutError` (the SDK does not claim its own timeout
elapsed); see "Uncaught exceptions".

**Blocking code is not timed out.** The limit can only stop a handler at
an `await`. Blocking code inside an `async def` handler (`time.sleep`, a
CPU-heavy loop, a synchronous client or library call) runs to its end:
a handler that blocks past the limit and then returns without another
`await` is replied as a success. While it blocks nothing else in the
service runs: no other command, no health probe (`/health` answers only
after the block, so a liveness probe with a shorter timeout fails and a
container orchestrator may restart the container), and no traffic on the broker
connection: no heartbeat is sent, so a block longer than about two
heartbeat intervals (`KBM_LEDSAS_HEARTBEAT`, 60 s by default) makes the
broker close the connection. The command is then delivered again, and
each such redelivery counts as an attempt toward
`KBM_LEDSAS_MAX_RETRIES`: a handler that blocks that long on every
attempt is answered once with the final error and dead-lettered after
the last attempt (see "Broker connection lost while a command runs"
under "Logging"). After a block of 1 s or more the SDK logs one
WARNING, `The event loop was blocked for at least <N> s: nothing else
in this service ran meanwhile ...`, naming the handlers that ran during
it (at most one such WARNING a minute). `<N>` is measured by a check
that runs every 0.1 s, so it can be up to 0.1 s short of the block, and
a block a little under 1 s may be reported too. The SDK's own reading
of a very large command body blocks the loop the same way (decoding,
parsing and checking it, see "Memory" and the nesting walk under
"Malformed message bodies"): the WARNING then says so (`The SDK spent
<M> s of it reading a command body of <n> bytes ...`) and names the
handlers only when that reading does not explain the block. Run
blocking work with `asyncio.to_thread(...)`, as below, and keep command
bodies small.

### Work in threads and processes

Cancelling a handler stops only the handler's coroutine. Python cannot
stop a thread, so work the handler started with `asyncio.to_thread(...)`
(the way this reference recommends for blocking or CPU-heavy work) or in
an executor **keeps running** after the timeout: it uses CPU and memory
for a result nobody receives, it holds a thread of the executor (later
`asyncio.to_thread` calls can queue behind it), and the command's retry
starts a second copy next to it. When the service stops, the process
exits only once such threads have finished: the SDK logs `ServiceApp
shutdown complete`, then the exit waits for them. A subprocess the
handler started also keeps running unless the handler stops it.

For work that can outlast `KBM_LEDSAS_HANDLER_TIMEOUT`, make it stoppable
from the handler:

- **A flag the worker checks.** Pass a `threading.Event` to the
  function, check it between steps (each frame, chunk or batch), and set
  it when the handler is cancelled:

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
- **A process you can stop.** Run the work in a subprocess
  (`asyncio.create_subprocess_exec`) and kill it when the handler is
  cancelled (`process.kill()` in a `finally` block), for work that has
  no natural place to check a flag. A subprocess inherits the service's
  environment, including `KBM_LEDSAS_RABBITMQ_URL` and
  `KBM_LEDSAS_BLOB_CONN_STRING` (an account key in it grants the whole
  storage account). If the tool you run might log, report or pass on its
  environment, give it one without them:
  `env={k: v for k, v in os.environ.items() if not k.startswith("KBM_LEDSAS_")}`.
  This is hygiene, not isolation: a process running as the same user can
  still read the service's environment from the operating system.

Also bound the input before starting the work (for example the decoded
size of an image), so one request cannot take minutes in the first
place.

### Cancellation Safety

**Important:** Handlers should be cancellation-safe. Use `try/finally`
blocks to clean up resources:

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

Use `ctx.deadline` to check remaining time proactively:

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

The SDK provides automatic retry with exponential backoff for transient errors.

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
   - Calculate backoff: `min(2^retry_count + jitter, 60.0)` seconds (jitter ∈ [0, 1)).
   - Log one INFO line: `Retry k/N, waiting <delay>s before requeue (no
     reply for this attempt)`. The attempt's failure itself was already
     logged once by the handler layer, at WARNING (see "Log lines per
     failed command" under "Logging").
   - Wait for backoff duration (cut short by shutdown, see below).
   - Requeue the message and count the retry.
2. If retry count >= max retries:
   - Send the one reply (the error from this last attempt) when
     `reply_to` is set.
   - Send the message to the Dead Letter Queue (DLQ).
   - Log one ERROR, `Max retries (N) exceeded, sending to DLQ`, with the
     final `retry_count` (the handler layer logs each attempt's own
     failure as a WARNING; for the storage failures that also log an
     ERROR per attempt, see "Uncaught exceptions").

**Shutdown during a backoff.** Once shutdown is requested (SIGTERM or
SIGINT), a command waiting out its backoff is requeued at once instead
of holding the shutdown for the rest of its wait (up to 60 s); another
consumer, or this service after its restart, retries it.

**The backoff wait holds a worker slot.** A command waiting out its
backoff keeps its `KBM_LEDSAS_CONCURRENCY` slot until it is requeued,
so other commands wait for a free slot meanwhile. With the default
concurrency of 4, four commands in backoff delay a fifth command by up
to the longest of their waits (the wait is capped at 60 s, reached from
the 7th retry on). Raise `KBM_LEDSAS_CONCURRENCY` if you expect many
retries at once.

**Where the retry count lives.** The count is kept in memory by the
service process that requeued the message, keyed by `message_id`; it
is not stored in the message (a requeue redelivers the original message
unchanged). So it starts again from zero when the redelivery lands on a
different replica or the process restarts. In particular a handler that
**crashes the process** (killed by the out-of-memory killer,
`os._exit`, a segfault in a native library; a handler that raises
`SystemExit` or `KeyboardInterrupt` does not, see "Uncaught exceptions")
never reaches the requeue step: the broker redelivers the message after the restart, it crashes
again, and the loop only ends when the message is removed. The other
commands the process held at the crash (received but not yet
acknowledged, up to `KBM_LEDSAS_PREFETCH`) are delivered again with it
after each restart and run again; one that takes longer than the time
from the start to the crash never finishes, so its caller gets no reply
until the crashing message is removed. Guard
handlers against crashing inputs (validate sizes and formats before
heavy native processing) and monitor the service's restart count.
If the connection to the broker is lost while a command runs, a requeue
that could not be sent still counts as an attempt, and a command whose
dead-letter could not be sent keeps the attempts it had used, so its
redelivery to this process does not start a new set of retries.

### Backoff Schedule

The formula is `min(2^retry_count + jitter, 60.0)` seconds, with
`jitter = random.uniform(0, 1)` (range `[0, 1)`). `retry_count` is
0-indexed: the first retry uses `retry_count=0`, etc.

| `retry_count` | Base (`2^retry_count`) | Actual delay range |
|-------|------------|----------------------|
| 0 | 1s | 1.0s – 2.0s |
| 1 | 2s | 2.0s – 3.0s |
| 2 | 4s | 4.0s – 5.0s |
| 3 | 8s | 8.0s – 9.0s |
| 4 | 16s | 16.0s – 17.0s |
| 5 | 32s | 32.0s – 33.0s |
| 6+ | 64s+ → clamped | 60.0s (cap) |

With the default `KBM_LEDSAS_MAX_RETRIES=3` only retry_counts 0–2
fire before the message goes to DLQ; the clamp at 60s only matters
if you raise `MAX_RETRIES` to 7 or more.

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

Enable generic error messages to hide internal error details from callers.

### Configuration

```bash
export KBM_LEDSAS_GENERIC_ERRORS=true   # Hide internal details
export KBM_LEDSAS_GENERIC_ERRORS=false  # Show full details (default)
```

### Behavior

When `KBM_LEDSAS_GENERIC_ERRORS=true`:

| Error Code | Generic Message | Retry semantics |
|------------|-----------------|-----------------|
| `Retryable` | "Processing failed temporarily" | Retried with exponential backoff |
| `Permanent` | "Processing failed" | Dead-lettered, no retry |
| `Timeout` | "Processing timed out" | Retried — `KBM_LEDSAS_HANDLER_TIMEOUT` expired, or the handler raised `TimeoutError` |
| `DeadlineExceeded` | "Processing deadline exceeded" | Dead-lettered, no retry — the deadline had passed when the command arrived, or passed while the handler ran (resend with a new deadline) |
| `UnexpectedError` | "An unexpected error occurred" | Retried (treat as transient). The caller always sees this generic text, whatever the flag. |
| `HandlerNotFound` | "Handler not available" | Dead-lettered, no retry |

### Custom User Messages

The `user_message` parameter **always wins** when set — it's what the
caller sees regardless of `KBM_LEDSAS_GENERIC_ERRORS`. The flag only
chooses between the internal message and the generic fallback when
`user_message` is **not** supplied.

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

The internal `message` is always logged regardless of which value reaches
the caller. For a retryable error the caller sees the message from the
**last** attempt, after the retries (one reply per command).

### Logging

Internal error details are always logged regardless of the
`generic_errors` setting (the `user_message` is not logged; it is what
the caller sees). For example, `raise errors.Retryable("DB connection
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
in handler"` with the same `error_message` field. An uncaught exception
logs one WARNING per attempt:

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

`error_location` is the file name, line and function of the innermost
frame of the exception's traceback, where it was raised: for an
exception raised inside a library (for example the storage client) it
names that library's line, not your handler's, and the traceback on the
last attempt shows the whole call path, your handler included.
`will_retry` is `true` while the command has retries
left. On the command's last attempt `will_retry` is `false` and the
record also carries the traceback in `exception`; no other attempt in
that set of retries does (a later delivery of the same command can log
its own, see "Uncaught exceptions"). A transient storage or network
error logs `"message": "Transient error in handler: <reason>"` with the
same fields except `error_location`, and never a traceback. A storage
failure that is neither transient nor an HTTP 4xx also gets the storage
client's own ERROR record, with a traceback, on every attempt (see
"Uncaught exceptions").

---

## Logging

The SDK auto-configures the `kbm_ledsas_sdk` logger namespace at
startup, based on `KBM_LEDSAS_LOG_LEVEL` (default `INFO`) and
`KBM_LEDSAS_LOG_FORMAT` (default `json`). Both env vars are applied
when `app.run()` boots — there is no manual setup step required.

```bash
export KBM_LEDSAS_LOG_LEVEL=DEBUG
export KBM_LEDSAS_LOG_FORMAT=text     # human-readable / colored
```

**Scope:** the SDK's auto-configuration only touches the
`kbm_ledsas_sdk.*` logger namespace. Your own loggers
(`__main__`, your module names, `handler.<command_name>` from
`ctx.logger`) continue to use whatever root logging config you set up
(usually `logging.basicConfig(...)`).

**When you configure no logging yourself.** If the root logger has no
handler when `app.run()` starts (your code did not call
`logging.basicConfig(...)` or add a handler to the root logger), the SDK
writes the records that no handler takes in `KBM_LEDSAS_LOG_FORMAT` (JSON
by default) to stderr, and routes Python warnings through logging
(`logging.captureWarnings(True)`): it replaces Python's last-resort
handler (`logging.lastResort`, which writes only a record's message) with
one in its own format. The records of other code then come out in the
same format as the SDK's, one line each, instead of as plain multi-line
text between the JSON lines: the event loop's (`Task exception was never
retrieved`, with its traceback in the record), the client libraries',
those of your own loggers that have no handler, and warnings (logger
`py.warnings`). Which records come out does not change: those at WARNING
or above that no handler takes, so your `INFO` records still need a
configuration of your own. A logger you gave a handler of your own is
written by that handler only (no record comes out twice), and a
`logging.basicConfig(...)` you call later still takes effect. If you
configure root logging yourself, the SDK leaves it as it is and does not
capture warnings: call `logging.captureWarnings(True)` yourself if you
want them in your log stream too (the examples do).

**On expected error cases the SDK now suppresses upstream-library
traceback noise** (`aio_pika`/`aiormq` `ChannelNotFoundEntity` raised
by a missing reply_to exchange, and the unroutable-return raised when
the exchange exists but has no bound queue) — you still get the SDK's
own clean ERROR line and the DLQ counter. Genuine connection / channel
errors still surface unchanged.

**Broker outages are SDK log events.** When the RabbitMQ connection drops,
the SDK logs one WARNING (`Lost the connection to the AMQP broker (...);
reconnecting automatically ...`) and, once the client has reconnected by
itself, one INFO (`Reconnected to the AMQP broker; consuming commands
again`), in the SDK log format. `/ready` answers 503 in between. Commands
that were not yet acknowledged are redelivered by the broker. A broker or
a network path that stops answering without closing the connection is
found by the AMQP heartbeat (`KBM_LEDSAS_HEARTBEAT`, 60 s by default)
after about three heartbeat intervals (`3 × (interval + 1)` seconds, 183 s
at the default, plus up to half an interval), and is then reported and
recovered the same way: the WARNING says `Lost the connection to the
AMQP broker (no frames from the broker within the heartbeat timeout,
about 183 s); reconnecting automatically ...` (`error_class`
`HeartbeatTimeout`), and a reply that this close cancelled logs `Reply to
exchange '<name>' not sent: the connection to the AMQP broker was lost
(ConnectionLost)`. A handler that blocks the event loop longer than
about two heartbeat intervals makes the broker close the connection the
same way (see "Blocking code is not timed out" under "Timeout Enforcement"). The AMQP
client library also logs the close and its own reconnect attempts
(`aio_pika` / `aiormq` loggers, through your root logging
configuration, or the SDK's when you configured none), all at WARNING: the library logs the broker's close
reason (`Unexpected connection close from remote ..., CONNECTION_FORCED
...`) as an ERROR, which the SDK lowers to WARNING, since the connection
is recovered automatically. A routine broker restart therefore produces
no ERROR record. An abrupt loss (the broker killed or crashed, the TCP
connection cut) is reported the same way: the client library's
`Cancelling cause reader exited abnormally` record is one WARNING line
naming the cause (`(AMQPConnectionError: Server connection unexpectedly
closed ...)`), without a traceback; a reader error that is not a lost
connection (a malformed frame from the broker) keeps its traceback.

**Broker connection lost while a command runs.** A command whose
delivery belongs to a lost connection cannot be acknowledged or
answered on it. The lost connection does not make the SDK dead-letter
the command or log a traceback:

- If the client has reconnected and the broker delivers the command
  again to this service instance while it is still in progress here
  (its handler running, or the command waiting for a worker), the
  redelivery is handed to that command: its outcome (the one reply,
  then the ACK, requeue or dead-letter) settles the redelivered
  command, which therefore runs once. One INFO line (`Redelivered
  command arrived while it is still in progress here ...`); the
  `AMQP consumer stopped` INFO line at shutdown reports the total as
  `redeliveries_adopted`. With several instances the broker may deliver
  it to another instance instead, which runs it too.
- Otherwise (the handler finishes while the connection is still down, or
  before a redelivery has reached it), the SDK logs one WARNING and
  leaves the command to the broker, which delivers it again (after a
  broker restart, only a command published as persistent): it runs
  again. The WARNING is `Reply to exchange '<name>' not sent: the
  connection to the AMQP broker was lost ...` when the reply could not
  be published, and otherwise names the settlement that could not be
  sent: `ACK not sent: ...` (after a success, including one whose reply
  was published on the restored connection), `Requeue not sent: ...` or
  `Dead-letter not sent: ...`.

A command delivered again after a lost connection counts as one more
attempt toward `KBM_LEDSAS_MAX_RETRIES`, as a retry does. When the
attempts are spent (with the default `3`: four runs, so the fourth
redelivery of the same command), that delivery is not run: the caller
gets one reply `{"code": "UnexpectedError", "message": "An unexpected
error occurred", "retryable": true}`, the command is dead-lettered, and
the SDK logs one ERROR `Max retries (N) exceeded, sending to DLQ: command
<name> was delivered again after <n> attempt(s) that did not finish, the
last one(s) because the connection to the AMQP broker was lost while the
command ran; it is not run again. A handler that blocks the event loop
for longer than about two heartbeat intervals (KBM_LEDSAS_HEARTBEAT)
loses the connection: run blocking work with asyncio.to_thread`. So a
handler that loses the connection on every attempt ends in the DLQ
rather than running without end. The count is kept in the service
process, keyed on the command's `message_id`: a restart of the service,
or a redelivery that reaches another instance, starts it again.

**The broker's consumer_timeout closed the channel.** A command left
unacknowledged longer than the broker's `consumer_timeout` (30 minutes
by default on RabbitMQ; see "Handler Timeout") is taken back: the broker
closes the channel the service receives commands on (the connection
stays up) and delivers every command that was unacknowledged on it
again. The SDK logs one WARNING that names the cause (`The AMQP broker
closed the consumer channel: a command stayed unacknowledged longer than
the broker's consumer_timeout. The broker delivers the N command(s) in
progress here again: ...`, with the broker's reason in `error`, and
`in_progress` and `message_ids`); the client library's records about
deliveries that reached the closed channel (`Got frame for closed
channel ...`) are WARNINGs too, not ERRORs. The commands in progress then
end as in the list above: a redelivery that reaches this instance while
the command still runs is handed to that command (the INFO line says
`the broker closed the channel of its first delivery at its
consumer_timeout`), and a settlement that can no longer be sent logs one
WARNING `ACK not sent: the AMQP broker closed the channel of this
command's delivery at its consumer_timeout while the command was in
progress ...` (or `Requeue not sent` / `Dead-letter not sent`), with
`reason` `consumer_timeout`; the broker delivers that command again and
it runs again. With several instances the broker may deliver the command
to another instance, which runs it again, and while every run outlasts
the `consumer_timeout` this can go on without end. Raise the broker's
`consumer_timeout` above the longest handler run plus the time a command
waits for a worker.

So a handler may run twice for one command, and its caller can receive
two replies (one per run); keep handlers idempotent (key writes on
`ctx.idempotency_key`).

**Shutdown.** On SIGTERM or SIGINT the service stops taking new commands
at once: `/ready` answers `503` from that moment (`/health` stays `200`
while the running handlers finish), it stops receiving from the broker
(one INFO line, `AMQP consumer no longer receives new commands`), so
the broker delivers the
queue's commands to the other replicas from then on, and it gives back
at once every command it has taken but not started (waiting for a
worker), without running it: no reply, no retry counted, and the broker
delivers it again, to another replica, or to this service after its
restart. One INFO line at the end of the drain names them (`Gave back N
command(s) without running them (shutdown requested); the broker
delivers them again: ...`). The service then waits only for the handlers
already running (shutdown does not cap their run time;
`KBM_LEDSAS_HANDLER_TIMEOUT` does), so each can reply and acknowledge its
command. From that moment
every broker operation the SDK still makes (reply and status publishes,
ACK / requeue / dead-letter, consumer cancel, connection close) gets at
most 5 s; once the broker is found unreachable, the remaining ones are
skipped and the connection is closed locally, reported in one WARNING
(`AMQP broker not reachable during shutdown ...`, naming the cause). A
command whose reply could not be confirmed, or that could not be
acknowledged, requeued or dead-lettered at that point, is left
unacknowledged instead of being dead-lettered: the broker delivers it
again and it runs again. A broker that was only stalled may still
deliver the unconfirmed reply late, so the caller can receive it twice.
An acknowledgement, requeue or dead-letter gets no answer from the
broker, so one sent after the stop signal is written at once and logged
(`ACKed command ...`), but a broker that had stopped answering (frozen
rather than disconnected) may never read it: that command is then
delivered again and runs again. The WARNING counts these (`N
acknowledgement(s), requeue(s) or dead-letter(s) were sent after the stop
signal; the broker does not answer them, so they may not have reached
it, ...`).
Once the broker is found unreachable, the handlers still running are
cancelled, as by a second stop signal (below): their reply and
acknowledgement could not be sent any more, and the broker delivers
their commands again. One INFO line names them (`Replies and
acknowledgements can no longer be sent while stopping (the connection
was lost or does not answer): N running command(s) cancelled; the broker
delivers them again: <name> <message_id>, ...`). A connection that was
already lost when the stop signal arrived (the client still
reconnecting) counts as unreachable from the start: the running handlers
are cancelled at once and the service exits without waiting for them.
With a reachable broker none of this applies.

A **second** SIGTERM or SIGINT (Ctrl+C pressed again) stops at once: the
handlers still running are cancelled (one WARNING, `Received signal 2
again: stopping now. N running command(s) cancelled and given back
without a reply; the broker delivers them again.`) and each of their
commands is given back without a reply and without counting a retry, so
the broker delivers it again (to this service after its restart, or to
another replica) and it runs again. A command whose handler has already
returned is not cancelled: it still sends its one reply and is
acknowledged or dead-lettered (each broker step at most 5 s, as above),
so it is not given back after its reply was sent. Work a handler started in
a thread is not stopped by this either; the process exits once it ends
(see "Work in threads and processes" under "Timeout Enforcement").

A SIGTERM or SIGINT that arrives **while the service is still starting**
(connecting to the broker, declaring its queues) stops the start at
once, logged at INFO with no traceback: `Received signal <n> while
starting: stopping the start`, then the transport closes what it had
opened (INFO `DirectTransport start cancelled; stopping`, `Stopping
DirectTransport`, `Azure Blob client closed`; the connection being made
is closed and does not keep reconnecting), and the process exits with
code 0 after INFO `Stopped before the service started ...`. Without it,
a broker that does not answer would hold the start until
`KBM_LEDSAS_CONNECT_TIMEOUT`.

**Shutdown in containers.** A container runtime sends SIGTERM, waits for
a grace period and then kills the process with SIGKILL. An idle service
exits within about a second; a service with commands running waits for
them, up to `KBM_LEDSAS_HANDLER_TIMEOUT` (30 minutes by default). A
command still running at the SIGKILL gets no reply and is delivered
again (and runs again) after the restart. So set the grace period above
the longest handler run, plus about 10 s for the broker steps above:
`docker stop -t <seconds>` (Docker's default is 10 s),
`stop_grace_period:` in a Compose file, or
`terminationGracePeriodSeconds` in a Kubernetes pod (30 s by default);
or lower `KBM_LEDSAS_HANDLER_TIMEOUT` to fit the grace period you have.

**Log lines per failed command.** A command that fails with a
non-retryable error is logged once by the handler layer: ERROR for
`Permanent`, `HandlerNotFound`, an invalid return value and the other
non-retryable cases, WARNING for `DeadlineExceeded` (two WARNING lines,
`Deadline already exceeded` then `Deadline exceeded in handler`, when
the deadline had already passed when the command arrived). The following
"sending to DLQ" outcome line is INFO. (Content that `download_json` /
`download_text` cannot decode, or an object `upload_json` cannot
serialize, also gets the blob call's own ERROR line, so such a command
logs two.) A retryable failure logs one
WARNING per attempt from the handler layer (`Retryable error in
handler`, `Handler timeout`, `Handler raised TimeoutError`, `Transient error in handler: ...` or
`Uncaught exception in handler`; only the last attempt's `Uncaught
exception in handler` in a set of retries carries a traceback), an INFO
`Retry k/N, waiting ...` line before each retry, and one ERROR (`Max
retries (N) exceeded, sending to DLQ`) when the retries are spent, logged
once the dead-letter has been sent (the "sending to DLQ" INFO of a
non-retryable failure likewise). A
storage failure that is neither transient nor an HTTP 4xx (for example
an HTTP 501, 505 or 507, or a response cut off mid-transfer) also gets
the storage client's own ERROR line, with a traceback, on every attempt
(`Failed to download blob`, `Failed to upload blob`, `Streaming download
failed`, `Streaming upload failed`).

Storage refusals add one WARNING line from the storage client
(`kbm_ledsas_sdk.blob.azure_client`, no traceback) before the handler
layer's line, on each attempt that meets them: `Blob not found` or
`Container not found` (with `container`, `path`, `version_id` and the
storage `error_code`, `BlobNotFound` or `ContainerNotFound`) for a
download, and `Failed to download blob (client error)` / `Failed to
upload blob (client error)` / `Streaming download failed (client
error)` / `Streaming upload failed (client error)` with the short
storage error (`HTTP 404 ContainerNotFound ...`) for any other HTTP 4xx
(an upload to a container that does not exist, a refused credential,
an invalid name). One HTTP 4xx is logged differently: an upload refused
because the blob already exists (`overwrite=False`, HTTP 409
`BlobAlreadyExists`) gets one INFO line instead, `Blob already exists
(overwrite=False)` (`Blob already exists (streaming upload,
overwrite=False)` for `upload_stream`), with `container` and `path`, and
then the handler layer's ERROR if the handler lets the error propagate. A storage account key that cannot sign requests (see
`KBM_LEDSAS_BLOB_CONN_STRING`; normally refused at start) gives one
WARNING `Failed to download blob: the storage credentials cannot sign
requests (...)` and the command fails `Permanent` without retries. A
transient storage failure adds one INFO line per attempt (`... (transient
storage error)`).

**What to alert on.** Alert on the `Max retries (N) exceeded` ERROR (one
per command dead-lettered after its retries are spent; it is logged once
the dead-letter has been sent: if the broker connection was lost, or the
broker is unreachable while the service stops, the dead-letter is not
sent and neither is this line; the transport's WARNING says so, the
broker delivers the command again and its last attempt runs again: see
"Broker connection lost while a command runs"; the same ERROR ends a
command that was delivered again after lost connections until its
attempts were spent), on the ERROR saying the broker
cancelled the command consumer (see "Default checks" under "Health
Endpoints"), and on
the ERROR lines of non-retryable failures, or on the WARNING records if
you want to see failed attempts. Every record of the handler layer
about a command (`No handler for <name> v<version>`, `Permanent error in
handler`, `Retryable error in handler`, `Uncaught exception in handler`,
`Transient error in handler`, `Non-retryable error in handler (not
retried)`, `Handler timeout`, `Handler raised TimeoutError`, `Deadline
already exceeded ...`, `Deadline exceeded in handler ...`, `Handler
returned an invalid result ...`) and the SDK's
own per-command records (`Received command`, `Handling command <name>`,
`Sent response for <name>`, `ACKed command <name>`, `Max retries (N)
exceeded`, `Command failed with a non-retryable error, sending to DLQ`,
`Retry k/N ...`, `Reply publish failed for <name> ...`, and the reply and
status records `Failed to send response to reply_to exchange ...` and
`Failed to send status to reply_to exchange ...`, and the consumer's
records about settling the command: `Acknowledging message`, `Rejecting
message` (each retry requeue and each dead-letter), `Failed to ACK
message`, `Failed to NACK message`, the WARNING and INFO records saying
that an ACK, a requeue or a dead-letter was `not sent: the connection to
the AMQP broker was lost`, `not sent: the AMQP broker closed the channel`
or `not sent: AMQP broker not reachable during shutdown`, and
the `Duplicate message_id while original still pending; rejecting
duplicate to DLQ` and `pending_messages dict at cap; rejecting new message
to DLQ ...` WARNINGs) carry the command's
identity: its `correlation_id`, `message_id`, `command_name`,
`trace_id` and `idempotency_key`, and its `job_id` when it has one, so
an alert can be joined to the dead-lettered message. A record about a
`message_id` the service does not hold (`Cannot ACK unknown message`,
`Cannot NACK unknown message`) carries only that `message_id`. The storage client's ERROR lines above come once per
attempt, not once per command. An `Uncaught exception in handler`
record is never an ERROR (it was before v0.3.6, so an alert keyed on
that message at ERROR level no longer fires). A broker restart the SDK
recovers from logs WARNING and INFO records only (see "Broker outages
are SDK log events").

**Reply and status failures.** A reply that cannot be sent (the
`reply_to` exchange is missing, or no queue is bound for `response`)
logs one ERROR `Failed to send response to reply_to exchange ...` (for
a missing exchange it ends `verify the caller declared this exchange and
bound a queue to it for routing key 'response'.`) and the command is
dead-lettered; a status update that cannot be sent logs
one WARNING `Failed to send status to reply_to exchange ...` and the
handler continues. Their running counts since the service started are
fields of these records: `reply_publish_failures` and
`reply_unroutable_failures` (the replies the broker returned as
unroutable) on the ERROR, `status_publish_failures` on the WARNING.
There is no metrics endpoint; count or alert on these records.

**Timestamps.** In both formats the timestamp is UTC, ISO 8601 with
milliseconds and a `Z` suffix (`2026-10-14T05:43:15.123Z`).

**Text format.** `KBM_LEDSAS_LOG_FORMAT=text` writes one line per record:
`<timestamp> <LEVEL> <logger>: <message>`, then `[corr=...]` (the
command's `correlation_id`) and `[trace=...]` (the first 16 characters of
its `trace_id`) on every per-command record, then every other field of the
record as `key=value` (the same fields the JSON format carries, for
example `message_id`, `command_name`, `idempotency_key`, `job_id`,
`error_class`, `error_message`, `error_location`, `internal_message`,
`will_retry`, `uri`, `amqp_message_id`, `body_bytes`), so the text format
shows why something failed just as the JSON one does. The JSON format
carries the whole `trace_id`. Values that are not a plain
token are written as JSON strings. Control characters (terminal escape
sequences, carriage returns, newlines) in the message and in field
values are written as escapes (`\x1b`, `\n`), so caller-controlled
text cannot recolor the terminal or forge a log line; a traceback
follows its record, each line indented by `  | `. Unicode format
characters (for example the right-to-left override `U+202E`) are written
as escapes too. Each record also carries `service_name=`. The level name
is colored when stderr is a terminal.

**Attempt fields.** Every per-attempt WARNING of the handler layer
(`Uncaught exception in handler`, `Transient error in handler`, `Handler
timeout`, `Handler raised TimeoutError`, `Retryable error in handler`)
carries `attempt` (1 for the first run) and `will_retry`; the final
`Max retries (N) exceeded` ERROR carries `error_class`, `error_message`
and `attempt`.

**The configuration does not print its credentials.** `repr()` of the
broker URL and of the storage connection string the SDK holds (and so
`repr()`, `vars()` and debugger or `rich` views of the SDK's
configuration and transport objects) shows `'**redacted**'`; `str()` and
comparisons give the value itself, as before.

**Library log records.** The AMQP and storage client libraries log
through their own loggers (`aio_pika`, `aiormq`, `azure`), which the SDK
caps at WARNING; their records go to your root logging configuration
(or, when you configured none, are written in the SDK's format; see
"When you configure no logging yourself" above). In the AMQP client
library's records the SDK masks the same secrets as in its own (see
"Secrets in these records are masked" under "Uncaught exceptions"): a
broker URL has its user name and password replaced by `***:***`, and
every value of its query string and its fragment by `***`
(`amqps://***:***@host:5671/?heartbeat=***`). Every record the SDK's
last-resort handler writes (when you configured no logging) is masked
the same way.

**The JSON formatter always writes valid JSON.** `json_log_formatter()`
writes each record as one JSON line (RFC 8259) whatever its `extra`
fields hold: a `NaN` or `Infinity` value is written as the string
`"NaN"` / `"Infinity"` / `"-Infinity"`, a field that still has no
JSON form (a dict with tuple keys, a container that contains itself) is
written as its `repr()` text, and a value whose own `__repr__` raises
(an ORM object detached from its session, say) as `"<TypeName (repr
failed)>"`. A message whose `%` arguments cannot be formatted is written
as its template. A lone surrogate in the message or a field (a `\ud800`
escape a JSON request can carry, quoted into an error) is written as
U+FFFD, as in error replies: strict JSON readers refuse an escaped lone
surrogate, and it has no UTF-8 form. Before, such a field produced a
non-standard `NaN` token or dropped the record with a `--- Logging
error ---` traceback.

**When the SDK's records start.** The SDK writes its records in
`KBM_LEDSAS_LOG_FORMAT` from the moment `app.run()` starts; it logs
nothing while your module is imported (the handlers are listed once the
service starts, `Registered handler for <name> v<version>`). A service
with no handler registered at that point starts, but logs one WARNING
instead (`No command handler is registered: every command this service
receives is replied HandlerNotFound and dead-lettered. Register handlers
with @app.handler(...) in modules that are imported before app.run()`):
typically the module with the `@app.handler` functions was never
imported.
`KBM_LEDSAS_LOG_LEVEL` applies as soon as the settings have been read
and checked (before the `Starting <service> service` record). While
they are being read the level is not known yet, so the SDK writes at
INFO: a WARNING about the settings themselves (the cleartext-AMQP acknowledgment) and a `Configuration error`
found while reading them are written whatever `KBM_LEDSAS_LOG_LEVEL`
says. Records your code writes before `app.run()` use
your own logging configuration; give your handler
`json_log_formatter()` (as the examples do) to have one JSON stream.

**A queue that already exists with other settings.** If the broker
already has one of the service's queues with arguments different from
the ones this SDK declares (typically a queue declared by hand or by another
tool with other limits, or kept in a reused local broker volume),
startup fails with one ERROR line that names the queue and the fix
(then the closing line below): delete that queue once (drain it first if
you need its messages); the service re-creates it with the current
settings.

**How a failed start ends.** A start that fails while the service
connects (the broker does not answer or refuses the credentials, a queue
exists with other settings, an unknown virtual host) logs one ERROR line
that names the cause, then one closing ERROR line,
`ServiceApp exiting (startup failure already reported above). Reason class: <class>`
(`<class>` names the exception class that ended the start; alert on
the first line and the exit code, not on that name), and exits with
code 1. So a failed start writes two
ERROR records, with no traceback; alert on the first for the cause.
Refused credentials give `AMQP authentication refused by the broker at
amqp://***:***@<host>:<port>/: check the user name and password in
KBM_LEDSAS_RABBITMQ_URL and the user's access to the virtual host. Broker
reply: ...`; a broker host name
that does not resolve gives `... the host name '<host>' does not resolve
(...); check the host in KBM_LEDSAS_RABBITMQ_URL and the DNS of the
network the service runs in`. A start refused because of a setting writes one `Configuration error`
line, its only ERROR record (exit code 1, no closing line). Most
settings are checked while they are read, before any other record. The
connection settings are checked when the connections are set up: for a
missing (or unusable) `KBM_LEDSAS_RABBITMQ_URL` or
`KBM_LEDSAS_BLOB_CONN_STRING` the `Configuration error` (naming every
missing one) follows the INFO records `Starting <service> service` and
`Registered handler for ...`.

---

## Supporting Types

### `BlobRef`
Reference to a blob storage object.

**Properties:**
- `uri` (str): Full blob URI (e.g., "azblob://container/path")
- `container` (str): Container name (derived from `uri`)
- `path` (str): Blob path within container (derived from `uri`)
- `version_id` (Optional[str]): Blob version identifier (parsed from the
  `?versionId=` query when present, else `None`). The parameter name is
  matched in any letter case, so `?versionid=` (the storage REST API's
  spelling) selects the same version.

**Class Methods:**
- `BlobRef.from_uri(uri: str) -> BlobRef`: Parse URI string into BlobRef

**Validation.** `BlobRef.from_uri()` (and every `ctx.blob.download_*`
call given a string) raises `ValueError` (a pydantic `ValidationError`)
when the URI is not `azblob://<container>/<path>[?versionId=<version>]`.
In particular it rejects, instead of silently ignoring:

- a container name Azure would refuse: a container name is 3 to 63
  characters: lowercase letters, digits and single hyphens, starting and
  ending with a letter or digit (`$root`, `$logs`, `$web` are also
  accepted). The same rule applies to the `container` argument of every
  `ctx.blob.upload_*` call;
- user info or a port before the container (`azblob://user:pw@c/...`);
- any query parameter other than `versionId` (for example a SAS `sig`),
  so nothing else rides along in `.uri`, which the SDK logs;
- a `versionId` value that is not a blob version id: after
  percent-decoding it may hold only letters, digits, `:`, `.`, `-` and
  `_` (at most 128 characters; Azure version ids are timestamps such as
  `2026-07-21T10:00:00.1234567Z`), so `1;sig=...` or an encoded newline
  (`%0a`) is refused;
- a `#` fragment (the text after `#` would drop out of the blob path),
  and a `?` with no `versionId` after it;
- a `.` or `..` path segment (as for upload paths: an HTTP client would
  remove a `.` segment after signing the request, and storage would then
  refuse it with a misleading authentication error), and URIs over 2048
  characters;
- a control character (C0, DEL and the C1 range `U+0080` to `U+009F`,
  which holds the one-byte terminal escape `U+009B` and the line break
  `U+0085`), a Unicode format character (zero-width characters,
  bidirectional overrides), a line or paragraph separator (`U+2028`,
  `U+2029`; every character the text log format escapes is among
  them) or a lone surrogate (`\ud800`, which UTF-8 cannot encode, so
  no blob can have it in its name), the same rule as for upload paths;
- a backslash (`\`) in the blob name, as for upload paths: storage may
  take `a\b` as `a/b`, so the blob read would not be the one `.path`
  names (a check on `.path`, such as a required prefix, would not hold
  for the blob actually read). Separate path segments with `/`;
- a URI that does not start with exactly `azblob://`: the message says
  what is wrong with its start (an upper-case scheme such as
  `AZBLOB://`, leading whitespace, a missing `//`) and quotes the value
  as received (redacted as below);
- an empty blob name (`azblob://data/` or `azblob://data//`), an empty
  segment (`azblob://data//x.json`, `azblob://data/a//b.json`; the
  extra `/` would otherwise be dropped and another blob read) and a
  name ending in `/` (`azblob://data/dir/`). Each is refused as an
  invalid blob URI (`Permanent`, no traceback), as upload paths are;
- any constructor argument other than `uri`:
  `BlobRef(uri=..., version_id="v1")` raises instead of dropping the
  version. To pin a version, put it in the URI (`?versionId=<id>`).

Validation messages never quote the query string, fragment or user info
of the rejected value (an https URL pasted by mistake can carry a SAS
signature): they show `scheme://host/path?<redacted>`, and
`str(error)` does not include the input value. Neither do the
error's `errors()` and `json()` (what error reporters serialise): their
`input` is a placeholder (`<not kept: a rejected blob URI may hold a
secret>`), also for an assignment to a `BlobRef` and for a `BlobRef`
field of your own pydantic model.

A `BlobRef` is immutable: assigning to it (`ref.uri = ...`) raises a
`ValidationError`. Build a new one with `BlobRef.from_uri(...)` instead.
pydantic's copy and construct helpers check the URI the same way
(`ref.model_copy(update={"uri": ...})`, `copy.replace(ref, uri=...)`,
`BlobRef.model_construct(uri=...)`, `BlobRef.model_validate(ref)`), and
every `ctx.blob` call checks a `BlobRef` again before it uses or logs its
URI, so a `BlobRef` the SDK reads holds a URI that passed these checks.

**The blob name is literal.** The part after `azblob://<container>/` is
the blob's name exactly as written: it is not percent-decoded.
`azblob://data/a b.json` names the blob `a b.json`, and
`azblob://data/a%20b.json` names another blob, `a%20b.json` (reading it
fails with HTTP 404, `Permanent`, when there is no such blob). Only the
`versionId` value is percent-decoded. The URIs that the upload calls
return carry the name the same way (`azblob://data/a b.bin`,
`azblob://data/café/ü.bin`). Pass blob URIs on as plain strings: a URI
library that percent-encodes or normalizes them (.NET `System.Uri`,
JavaScript `encodeURI`, Java `java.net.URI`) turns a name with a space or
a non-ASCII letter into another blob's name.

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

The example reads two fixed blobs (`azblob://config/settings.json`,
`azblob://docs/README.txt`) and writes to the containers `results` and
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
    """
    Complete example showing the full API surface.

    Args:
        ctx: ExecutionContext with all SDK operations
        req: Request dict with input_uri and options

    Returns:
        dict: Response with result_uri and rows_processed
    """
    # === Context Properties ===
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

    # === Deadline Check ===
    if ctx.deadline and datetime.now(timezone.utc) > ctx.deadline - timedelta(seconds=5):
        raise errors.DeadlineExceeded("Not enough time remaining")

    # === Status Reporting ===
    await ctx.emit_status("downloading", 0.0, "Starting download")

    # === Input Validation ===
    # Check required request fields first: a missing or invalid input_uri
    # is the caller's error (Permanent, one reply, not retried). Parsing it
    # with BlobRef gives a one-line reason; the reply never repeats the
    # value (a mistyped value may be a URL with a SAS signature).
    try:
        input_ref = BlobRef.from_uri(req.get("input_uri", ""))
    except ValueError:
        raise errors.Permanent(
            "input_uri missing or not an azblob:// URI",
            user_message="input_uri must be an azblob://<container>/<path> URI",
        ) from None

    # === Blob Operations ===
    # A missing blob, content that is not JSON or an invalid upload path
    # are reported by the SDK itself (Permanent, one line): no need to
    # catch them (see "Error Handling").
    try:
        # Download (bytes, json, text)
        data_bytes = await ctx.blob.download_bytes(input_ref)
        config = await ctx.blob.download_json("azblob://config/settings.json")
        if not isinstance(config, dict):  # a dict or a list: check before indexing
            raise errors.Permanent("settings.json must be a JSON object")
        readme = await ctx.blob.download_text("azblob://docs/README.txt")

        # Process data: count the CSV's data rows (its lines without the
        # header line; splitlines() does not count a final line break as
        # an extra, empty line).
        await ctx.emit_status("processing", 0.5, f"Processing {len(data_bytes)} bytes")
        rows = max(len(data_bytes.splitlines()) - 1, 0)

        # Upload (bytes, json, text)
        result = {"rows": rows, "options": req.get("options", {})}
        # Key the path on idempotency_key and pass overwrite=True, so a
        # retry or DLQ replay rewrites the same blob instead of failing.
        result_ref = await ctx.blob.upload_json(
            container="results",
            obj=result,
            path=f"output/{ctx.idempotency_key}.json",
            overwrite=True,
        )

        # Upload summary as text. As above, key on idempotency_key so a
        # DLQ-replay overwrites the same blob (not a fresh orphan).
        summary = f"Processed {rows} rows successfully"
        await ctx.blob.upload_text(
            container="logs",
            text=summary,
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
        # Retryable error
        raise errors.Retryable("Network timeout") from None

if __name__ == "__main__":
    app.run()
```

---

## Health Endpoints

The SDK exposes liveness and readiness HTTP endpoints alongside the
message consumer so any HTTP probe (a process supervisor, an internal
monitoring tool, your orchestration platform of choice) can check the
service.

### Endpoints

| Path | Purpose | Aliases |
|------|---------|---------|
| `GET /health` | Liveness — is the process alive? | `/livez` |
| `GET /ready`  | Readiness — should the instance be reported ready? | `/readyz` |

Readiness only decides what `/ready` answers (for example whether an
container orchestrator marks the instance ready and sends it HTTP traffic); it does
not stop command delivery: the service consumes commands while `/ready`
answers `503` too. Do warm-up work (loading a model, ...) **before**
`app.run()`.

Both return JSON, status `200` when all checks pass and `503` when any
fail. Each path answers `GET` and `HEAD`; another method gets a JSON
`405` (`{"status": "error", "code": 405, "message": "Method Not
Allowed", "allowed": ["GET", "HEAD"]}`), another path a JSON `404`, and a
request the server cannot parse (an over-long request line or header, a
bad `Content-Length`) a JSON `400` that does not repeat the request,
logged as one DEBUG line without a traceback. No response carries a
`Server` header. The default body is intentionally minimal — no service name, no
SDK version, no per-check status — so a `/health` exposed on
`0.0.0.0` doesn't fingerprint the service:

```json
{ "status": "healthy" }
```

A failing default-mode response also includes the names of the failing
checks so operators can diagnose:

```json
{ "status": "unhealthy", "failed": ["db_alive"] }
```

For the verbose body (useful during development), set
`KBM_LEDSAS_HEALTH_VERBOSE=true`:

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

In verbose mode, a failing check appears as `"unhealthy: <reason>"` in
the `checks` map. The example above is illustrative — each endpoint lists
only its own checks: `/health` (and `/livez`) report `process` plus your
`@app.liveness_check`s, while `/ready` (and `/readyz`) report `transport`
plus your `@app.readiness_check`s.

### Deployment id

When the environment variable `KBM_LEDSAS_DEPLOYMENT_ID` is set, its value
is added as a `deployment_id` field to **every** `/health` and `/ready`
body — both the minimal and verbose forms. `KBM_LASTCOMMITID` is an older
alias of `KBM_LEDSAS_DEPLOYMENT_ID`, read only when that is unset or blank;
prefer `KBM_LEDSAS_DEPLOYMENT_ID`. The SDK sets neither; set one at deploy
time, for example to the commit id of the build. The value is reported as set, after surrounding
whitespace and a matched pair of surrounding quotes are removed (as for
every setting):

```json
{ "status": "healthy", "deployment_id": "a1b2c3d4" }
```

This lets an operator confirm *which build* is live behind a probe
without exposing any other detail. The key is **omitted** when both
variables are unset or blank (e.g. a local dev run), so the minimal
default response stays fingerprint-free outside a real deployment. A
strict health-body consumer should treat `deployment_id` as an optional
field.

### Default checks

- **Liveness**: `process` — trivially `healthy` if the asyncio event
  loop is responsive enough to serve the HTTP request.
- **Readiness**: `transport` — healthy while the RabbitMQ connection is
  actually up: connected, not in the middle of reconnecting, and the
  consumer channel open. During a broker outage `/ready` returns `503`
  (`transport not connected`) and returns to `200` once the SDK has
  reconnected by itself. When the broker cancels the service's command
  consumer while the connection stays up (its queue was deleted, or the
  node holding it is down), `/ready` returns `503` too, and the SDK logs
  one ERROR `The AMQP broker cancelled the command consumer on <queue>
  (the queue was deleted, or the node that holds it is down): no command
  arrives, and /ready answers 503. Declaring the queue again and
  consuming again.` It keeps trying, with a WARNING `Could not consume
  from <queue> again (<cause>); retrying in N s` per failed try, and logs
  one INFO `Consuming commands from <queue> again` when it succeeds;
  `/ready` then returns to `200`. From the first
  stop signal on it returns `503`
  (`shutting down`) while the running handlers finish; after the drain
  the health server stops and probes are refused. Blob storage is
  **not** probed (a storage
  outage surfaces as failing handlers, not as readiness); add a custom
  readiness check if you need that.

### Custom checks

`ServiceApp` exposes two decorators. Each registered function takes no
arguments and returns truthy = healthy. Registering something that is
not callable, or a function that cannot be called without arguments,
raises `TypeError` at once (it would fail on every probe). Both sync and async callables
are supported. Exceptions are caught and reported as unhealthy with the
exception type and message (secrets in the message masked, see
"Behavior under failure"). A check whose result's truth value cannot be
computed (a numpy array of more than one element, a pandas Series)
counts as failing, like one that raises (`Health check 'name' raised
ValueError: The truth value of an array ...`): return `bool(...)` of
what you mean, such as `bool(arr.all())`.

In your own service, register the checks on its existing `app`. The
program below is complete on its own, so it uses a service name of its
own (`health-demo`) and a trivial handler: a second program started with
another service's name would consume that service's commands and
dead-letter those it has no handler for.

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

A check name must be unique within its registry; registering twice
raises `ValueError` at decoration time (loud, immediate).

**Time limit.** Each check has a time limit: 2 seconds, or the
`timeout` you register it with (`@app.readiness_check("db_connected",
timeout=1.5)`; more than 0, at most 60). An async check still running at
its limit is cancelled and reported unhealthy (`timed out after 2s`, one
WARNING `Health check 'db_connected' did not finish within 2s; reported
as unhealthy` when it starts failing, see "Behavior under failure"), so a
dependency that hangs makes the endpoint answer 503 instead of hanging
the probe. The checks of one request run at the same
time, so a request takes about as long as its slowest check. Keep every
limit below your probe's timeout (for example a 3 s probe timeout). A
**sync** check runs on the event loop and cannot be interrupted, so it
blocks the whole service while it runs: keep it to a quick in-memory
test, and write any check that waits on I/O as `async def`.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | TCP port for the server. Set `0` to disable entirely. A port the server cannot listen on stops the service at start (see "Behavior under failure"). |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Bind address: an IP address or a host name, an IPv6 address without brackets (anything else, `[::]` included, stops the start with one `Configuration error` line). Default is loopback only — safer on a dev laptop. Set to `0.0.0.0` when a container orchestrator on a different host needs to reach `/health` and `/ready` (the probes of a Kubernetes pod do); restrict who can reach the port, for example with a network policy. After a successful bind on a non-loopback address the SDK logs one INFO line (`Health server on 0.0.0.0:8090 is reachable from the network ...`), or a WARNING when `KBM_LEDSAS_HEALTH_VERBOSE` is also on. |

### Behavior under failure

- **An aiohttp version whose internals the health server cannot harden:**
  the endpoints still answer, and the SDK logs one ERROR per gap and
  process, `Health server: <what> cannot be applied with the installed
  aiohttp <version>; the endpoints still answer, without that protection.
  Install an aiohttp version this SDK release supports.`
- **Port already in use, or an address the server cannot listen on:** the service
  stops at start, before it runs any command: one ERROR naming the
  setting to change (`Could not start the health server on
  KBM_LEDSAS_HEALTH_PORT=8090 (KBM_LEDSAS_HEALTH_HOST=127.0.0.1): the
  address is already in use (...); another process, or another instance
  of this service, listens on it. Set KBM_LEDSAS_HEALTH_PORT to a free
  port, or to 0 to run without health endpoints.`; for a host that is
  not an address of the machine it names `KBM_LEDSAS_HEALTH_HOST`), then
  `ServiceApp exiting (startup failure already reported above). Reason
  class: kbm_ledsas_sdk.health.server.HealthServerStartFailure`, exit
  code 1, no traceback. The connection to the broker, opened first, is
  closed; commands it had already received go back to the queue
  unacknowledged. Before v0.3.6 the service logged a WARNING and ran
  without health endpoints, so probes reached whatever else listened on
  the port. On macOS a process listening on the wildcard address
  (`0.0.0.0:8090`) does not stop the SDK from binding `127.0.0.1:8090`,
  so there is no error and `http://localhost:8090` may reach the other
  process; probe `http://127.0.0.1:<port>` explicitly, as the examples
  do.
- **Custom check raises an exception:** that check is `unhealthy` with
  the exception text in the detail. Other checks still run; status
  rolls up across all of them. The exception text comes from code the SDK
  does not control and may quote a secret, so in the WARNING and in the
  verbose body the user name and password of a URL are masked
  (`amqp://***:***@host`), as are the values of `AccountKey=`,
  `SharedAccessSignature=`, `SharedAccessKey=`, `Password=` / `Pwd=` and
  a `sig=` URL query parameter (`***`; a bare `sig=` outside a URL query
  is not masked), and every query value and the fragment of a broker URL
  (`amqps://host/?password=***`). Every record the SDK writes masks the
  same secrets, the exceptions of your handlers included (see "Uncaught
  exceptions").
- **A check keeps failing:** probes come every few seconds, so a failing
  check is logged once when it starts failing, as one WARNING (`Health
  check 'db' raised RuntimeError: ...` or `... did not finish within
  2s ...`), again when its reason changes, and every 10 minutes while it
  keeps failing the same way (the message then ends `(still failing)`);
  one INFO `Health check 'db' passes again` when it recovers. A check
  that returns a falsy value is not logged (the probe body names it).
- **Custom check hangs:** it is cancelled at its time limit and reported
  `unhealthy` (see "Time limit" above).
- **Shutdown:** from the first stop signal on, `/ready` answers `503`
  (`shutting down`) while the running handlers finish (that can take up
  to `KBM_LEDSAS_HANDLER_TIMEOUT`); `/health` stays `200` meanwhile, so a
  liveness probe does not restart a service that is draining. When the
  drain ends the health server is stopped before the transport, so
  probes are refused from then on. Stopping it waits at most about 4 s
  for probe requests still being answered.
- **Slow or idle connections:** a connection that has not sent a
  complete request within 10 s is closed: one that sends nothing, one
  that sends part of a request and stops, and an idle keep-alive
  connection after its answer. At most 100 connections are open at a
  time: a connection made above that closes the oldest connection that
  is not answering a request, so idle connections a client holds cannot
  keep the probes out; only when every other connection is answering a
  request is the new connection closed (one WARNING a minute while this
  happens: `Health server has 100 open connections (the limit); the
  oldest idle ones are closed to accept new connections (...)`). Probes
  use one short connection each, far below both limits. With
  `KBM_LEDSAS_HEALTH_HOST=0.0.0.0` still let only the probes reach the
  port (for example with a network policy): the limits bound the
  server's memory and keep idle connections from locking the probes
  out, but a client that keeps 100 requests in progress can still delay
  them.

### `ServiceApp.health_server_running`

Read-only property returning `bool` — `True` while the embedded health
server is actually bound and serving, `False` when it is disabled
(`KBM_LEDSAS_HEALTH_PORT=0`), not started yet or stopped (a port it
cannot bind stops the service at start). Primarily a
test/diagnostic helper, e.g. for an integration test that needs to assert
"the health endpoint is up before I probe it."

```python
if app.health_server_running:
    ...
```

---

**Version:** 0.3.6
**Release:** 2026-10-18

### Version history

See [CHANGELOG.md](../CHANGELOG.md) for the full release history.
