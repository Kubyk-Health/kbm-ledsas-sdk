# Upgrading

How to upgrade `kbm-ledsas-sdk` safely between releases. The public API stays
additive and the AMQP wire contract stays stable within `0.x` (see
[COMPATIBILITY.md](COMPATIBILITY.md)), so most upgrades are drop-in. Any behavioral
change is documented here and in [CHANGELOG.md](CHANGELOG.md).

## Before any upgrade

1. **Pin the exact version** you are moving to (for example
   `kbm-ledsas-sdk==0.3.6`) and upgrade deliberately. Upgrade with
   `pip install -U --upgrade-strategy eager kbm-ledsas-sdk==0.3.6`: a plain
   `pip install -U` keeps dependencies that still satisfy the new ranges (an
   installed `aio-pika` 9.x stays, where a fresh install gets 10.x).
2. **Read the section below** for your target version, alongside the matching
   [CHANGELOG.md](CHANGELOG.md) entry.
3. **Run your test suite** against the new version in a non-production environment
   before rolling out.

---

## Upgrading to 0.3.6 (from 0.3.3)

**Type:** dependency, security and reliability release. **No public method or
parameter was removed or renamed** and the AMQP envelope (schema `1.0`) and
topology are unchanged, but several behaviors change, and input that was never
valid but used to be accepted is now refused with a clear error. 0.3.4 and 0.3.5
were not released (numbers skipped, no changes); upgrade straight from 0.3.3. The
full list of changes is in the [CHANGELOG.md](CHANGELOG.md) `[0.3.6]` entry; this
section lists what may need action.

### Dependencies

- **`cryptography>=50.0.0`** (security floor, was `>=48.0.1`) and
  **`aiohttp>=3.14.3`** (was `>=3.8.0`). If you pin either below the new floor in
  your own requirements or constraints, the install fails to resolve: raise your
  pin.
- **`aio-pika` 10.x is accepted** (`>=9.4.0,<11.0`). Nothing to do; if you pin
  `aio-pika` yourself, both 9.x and 10.x work. An in-place `pip install -U` keeps
  an installed 9.x (see "Before any upgrade" for `--upgrade-strategy eager`).

### Callers: one reply per command

- **A command gets one reply, when it has finished.** An attempt that will be
  retried sends no reply any more; the one reply is the handler's result, a
  non-retryable error, or the last attempt's error once `KBM_LEDSAS_MAX_RETRIES` is
  spent. A caller that waited for several replies can stop at the first. **Allow
  for the retries in the caller's reply timeout:** a command can take up to its
  handler's run time × (`KBM_LEDSAS_MAX_RETRIES` + 1) plus the backoff between
  attempts (7 to 10 seconds in all with the default 3 retries). For a handler
  that answers in a few seconds, wait at least 30 seconds with the defaults
  (the example's `scripts/send_hello.py` waits 30); for longer handlers use the
  formula.
- **Envelope timestamps and `priority` are not converted any more.** Send
  `sent_at` / `deadline` as ISO 8601 text and `priority` as an integer; a command
  with any other type is dead-lettered as an invalid envelope.
- **A `reply_to` naming a service's own command or dead-letter exchange**
  (`cmd.…` / `dlq.…`) **or a reserved broker exchange** (`amq.…`, such as
  `amq.topic` or `amq.direct`) is refused: the command is dead-lettered before its
  handler runs. A caller that named `amq.topic` as its `reply_to` declares a reply
  exchange of its own. `"reply_to": null` now means fire-and-forget, like `""`.
- **Version fields are ASCII digits, 1 to 4 on each side of the dot.**
  `schema_version` and `message_version` look like `1.0` or `12.3`; a command with
  longer numbers or other Unicode digits is dead-lettered as an invalid envelope.
- **A result with a top-level `"error"` key is a successful result:** any dict a
  handler returns is replied as it is and the command acknowledged (such a result
  used to have its command dead-lettered after the reply). To fail a command,
  raise (`errors.Permanent`, `errors.Retryable`, ...).

### Handlers

- **`@app.handler` checks what it registers.** The command name must match
  `^[A-Za-z][A-Za-z0-9_]*$` (at most 64 characters), the version must look like
  `1.0`, and the handler must be an `async def` function that takes two
  positional arguments (`ctx, payload`); the version, like the envelope's, is
  ASCII digits, 1 to 4 on each side of the dot. The check runs when the decorator runs,
  that is **when your module is imported**, so a refusal is a traceback at
  import (before `app.run()`), ending in one of:

  ```text
  ValueError: Invalid command name 'Process-CSV': use a letter followed by letters, digits or underscores (at most 64 characters), e.g. 'ProcessDataset' or 'process_data'. ...
  ValueError: Invalid message version '1' for ProcessCSV: use '<major>.<minor>' digits, e.g. '1.0'.
  TypeError: Handler for ProcessCSV v1.0 must be an async function ('async def'): ...
  TypeError: Handler for ProcessCSV v1.0 must accept two positional arguments (ctx, payload): ...
  ```

  Rename the command (callers must send the new name), fix the version, or
  define the handler with `async def handler(ctx, payload)` (run blocking work
  with `await asyncio.to_thread(...)`).
- **Message models and `BlobRef` are read-only.** `Envelope`, `Command`,
  `Response`, `Status` and `BlobRef` are immutable (all five could be changed
  in 0.3.3): assigning a field (for example `ctx.envelope.reply_to = ...`,
  `ref.uri = ...` on a `BlobRef`, or setting a field of a `Command` you built)
  raises `pydantic.ValidationError` ("Instance is frozen"). Build a changed copy
  with `model_copy(update={"reply_to": ...})`, or construct a new object
  (`BlobRef.from_uri(...)` for a blob reference).
- **`Command`, `Response` and `Status` check their envelope's `type`.** Building
  one around an envelope of another type (for example
  `Response(envelope=Envelope(type="command", ...))`) raises
  `pydantic.ValidationError` (`envelope.type is 'command', not 'response'`).
  Copy the envelope with `model_copy(update={"type": "response"})`. A response
  or status sent to a service's command exchange is still dead-lettered with one
  WARNING, as before.
- **Return a JSON-serializable `dict` (or nothing).** A handler that returns
  `None` replies `{}`. Any other value without a JSON form, or a result larger than
  `KBM_LEDSAS_MAX_PAYLOAD_BYTES`, is replied as `Permanent` and dead-lettered
  without a retry (return large results as a blob reference). A handler that
  returns `bytes` holding text must return `data.decode()` instead.
- **Errors that cannot succeed on a retry are not retried:** a storage error with
  an HTTP 4xx status other than 408/429, an invalid blob URI, content that is not
  valid JSON / UTF-8, and invalid arguments to an SDK call. They reply `Permanent`
  and dead-letter at once, with one ERROR line. The exception types your handler
  sees are unchanged.

### Blob storage

- **`BlobRef` takes only `uri`.** `BlobRef(uri=..., version_id=...)` (accepted,
  and ignored, by 0.3.3) now raises `pydantic.ValidationError` ("BlobRef takes
  only 'uri'"): put the version in the URI instead,
  `azblob://<container>/<path>?versionId=<version>`, and read it back from
  `.version_id`.
- **`BlobRef` follows the storage naming rules.** A container name Azure would
  refuse, user info or a port in the URI, a query parameter other than
  `versionId`, a `#` fragment, a backslash, and control characters in a blob name
  now raise `ValueError`. Parse URIs with `BlobRef.from_uri(uri)` and use its
  `.container` / `.path` / `.version_id`; do not split the string yourself (a URI
  from a storage account with blob versioning ends in `?versionId=...`).
- **An invalid upload `path` is refused before anything is written.** If an upload
  now raises `ValueError`, choose another path. A handler that passed a `str` to
  `upload_bytes` should call `upload_text`.
- **`download_stream` honors `chunk_size`** (it used to read 4 MiB chunks
  whatever you asked for).
- **`download_json` and `upload_json` take only a JSON object or array.** A blob
  whose JSON root is a number, a string, `true` / `false` or `null` now makes
  `download_json` raise `ValueError` (not retried; read such content with
  `download_text` or `download_bytes`), where 0.3.3 returned the scalar.
  `upload_json` raises `TypeError` before anything is written for anything but a
  `dict` or a `list` (a tuple included); upload other values with `upload_text`.
- **New, optional `KBM_LEDSAS_ALLOWED_CONTAINERS`.** Unset (the default) changes
  nothing. Set to comma-separated container names (`input,results`), it limits
  every `ctx.blob` operation to them: an upload by its `container` argument, a
  download by the container of its URI. Any other container raises
  `kbm_ledsas_sdk.errors.ContainerNotAllowed` (a `PermissionError`) before
  anything is sent to storage; uncaught, the caller gets one `Permanent` reply
  (`container 'other' is not allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)`), never the
  list itself. An empty item (`a,,b`) or a name the storage service would refuse
  stops the start with one `Configuration error` line. Anyone who can publish a
  command chooses the blob URIs in it; the setting keeps the service's storage
  credential to the containers you list.

### Configuration and startup

- **Connection settings are checked at start.** Only `amqp://` and `amqps://`
  broker URLs with a host are accepted, credentials with unencoded special
  characters are refused with a clear message, and a storage connection string
  must carry a credential (an `AccountKey` the storage client can decode, or a
  `SharedAccessSignature`). Fix the setting the error names.
- **A broker URL that sets `no_verify_ssl` to any value except `0` is refused at
  start** (`1`, `true`, even `false` or an empty value: the AMQP client library
  then skips the verification of the broker's TLS certificate;
  `no_verify_ssl=0` keeps it on and is accepted), unless
  `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` acknowledges it (then one WARNING is logged).
  Remove it; to trust a private certificate authority, name its certificate with
  `?cafile=<path>` instead.
- **New `KBM_LEDSAS_HEARTBEAT`** (default `60` seconds, `0` to `600`, `0` turns
  heartbeats off): the AMQP heartbeat interval the SDK asks the broker for. The
  default is the interval 0.3.3 used (0.3.3 set none of its own, so the AMQP
  client library's default of 60 seconds applied), so nothing changes unless you
  set it. A `?heartbeat=` in `KBM_LEDSAS_RABBITMQ_URL` still wins, but **an
  invalid `?heartbeat=` value** (not a whole number from `0` to `600`, for
  example `abc` or `-5`) **is refused at start**, where the client library used
  to turn heartbeats off without a word. A dead broker or network path is
  noticed within 183 to 213 seconds with the default (three intervals plus
  three seconds, checked every half interval), and the SDK reconnects.
- **Blocking code still loses the connection, and such a command now ends in
  the dead-letter queue.** A handler that holds the event loop (for example
  `time.sleep()` or CPU-bound work in an `async def`) longer than about two
  heartbeat intervals (about two minutes with the default) makes the broker
  close the connection; the command is redelivered after the reconnect. **A
  redelivery after a lost connection now counts toward
  `KBM_LEDSAS_MAX_RETRIES`**: once the retries are spent, the command is
  dead-lettered with one ERROR record and one reply, instead of running again
  after every reconnect. The count is kept per process (a restart starts it
  again). Run blocking work with `await asyncio.to_thread(...)` (see "Blocking
  code is not timed out" in the [API reference](docs/SDK_API_REFERENCE.md));
  a lower `KBM_LEDSAS_HEARTBEAT` notices a dead broker sooner but shortens how
  long a handler may block.
- **A boolean setting must be a boolean.** `KBM_LEDSAS_GENERIC_ERRORS`,
  `KBM_LEDSAS_HEALTH_VERBOSE`, `KBM_LEDSAS_ALLOW_INSECURE_AMQP` and
  `KBM_LEDSAS_DEBUG` accept `true` / `false` (or `1` / `0`, `yes` / `no`,
  `on` / `off`, `t` / `f`, `y` / `n`); any other value now stops the start (it was
  read as `false`, so `KBM_LEDSAS_GENERIC_ERRORS=ture` used to send detailed error
  messages to callers).
- **`KBM_LEDSAS_SERVICE_NAME` is an optional override.** The required
  `ServiceApp(service_name=...)` argument names the service; the variable, when
  set, overrides it and the SDK logs a WARNING if they differ. An empty value is
  read as unset.
- **New `KBM_LEDSAS_CONNECT_TIMEOUT`** (default 15 seconds): a broker that does not
  answer fails the start within this time.
- **A health port that cannot be bound stops the service at start.** Give each
  service on one host its own `KBM_LEDSAS_HEALTH_PORT` (or `0` to turn the health
  server off).
- **A misspelled `KBM_LEDSAS_*` variable is reported** at startup (one WARNING
  naming the closest setting).

### Operations

- **`deployment_id` is read as before**: from `KBM_LEDSAS_DEPLOYMENT_ID`, or
  from its older alias `KBM_LASTCOMMITID`, read only when
  `KBM_LEDSAS_DEPLOYMENT_ID` is unset or blank. The older alias stays
  supported; prefer `KBM_LEDSAS_DEPLOYMENT_ID`. The SDK sets neither; set one
  at deploy time, for example to the commit id.
- **`/ready` reports a broker outage** (`503` until the SDK has reconnected by
  itself) and answers `503` while the service stops. A readiness probe that
  expected `200` throughout an outage now sees the outage.
- **A consumer the broker cancels is noticed.** When the broker cancels the
  service's consumer (its queue was deleted, or the node that holds a classic
  queue is down), 0.3.3 consumed nothing more while `/ready` answered `200`.
  The SDK now logs one ERROR (`The AMQP broker cancelled the command consumer on
  <queue> ...`), answers `503` on `/ready`, declares the queue again and keeps
  trying to consume, with a growing delay (at most 30 seconds); one INFO
  `Consuming commands from <queue> again` when it is back. Nothing to do; alert
  on that ERROR if you watch the logs.
- **Configured secrets are masked wherever they appear.** The broker password,
  the storage `AccountKey` and the `SharedAccessSignature` (6 characters or
  longer), in their percent-encoded forms too, are replaced by `***` in every
  log record and reply text the SDK writes, not only where they appear in a
  URL or a connection string; more key forms are masked as well
  (`client_secret=`, `api_key=`, `AccountKey = ...`, `%26sig%3D...`). A log
  parser or a test that expected one of these values in the output now sees
  `***`.
- **`repr()` of the connection settings is redacted.** `SDKConfig.rabbitmq_url`
  and `SDKConfig.blob_conn_string` are a `str` subclass whose `repr()` is
  `'**redacted**'`, so `repr(config)`, `vars()`, `pprint` and `rich.inspect`
  show no credential. The value itself, `str()` and comparisons are
  unchanged.
- **Log alerts.** A retried failure logs one WARNING per attempt, with a traceback
  only on the last attempt; an alert on `Uncaught exception in handler` at ERROR
  level no longer fires. Alert on the `Max retries (N) exceeded` ERROR (one per
  command whose retries are spent) or on the WARNING records.
- **Log formats.** JSON log timestamps are UTC ISO 8601 with milliseconds and a `Z`
  suffix, and the text format shows the failure details as `key=value` fields;
  update a log parser that expected the old shapes.
- **A message whose AMQP properties are not valid UTF-8 is dead-lettered once**
  (one WARNING, `AMQP message property message_id is not valid UTF-8;
  dead-lettering`), instead of dropping the connection on every redelivery.
  Nothing to do; such a message ends up in the dead-letter queue.
- **Storage reached over `http://` on another machine logs one WARNING at start**
  (its requests travel in cleartext); use `https://` outside local development.
- **Message bodies nested more than 256 levels deep are dead-lettered.** Flatten
  such data, or upload it to blob storage and send its reference.

### Containers

- **Build service images `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6`**,
  which comes with the SDK installed (the tag is the SDK version), instead of
  installing `kbm-ledsas-sdk` on a generic Python image; never list the SDK in your
  `requirements.txt`. See the [README](README.md#container-base-image) and
  [`examples/hello_world_service/`](examples/hello_world_service/). This is a
  recommendation: `pip install kbm-ledsas-sdk==0.3.6` on any Python 3.11+ image
  still works.

---

## Upgrading to 0.3.3 (from 0.3.2)

**Type:** backwards-compatible — no code changes required. The public API is
unchanged and the AMQP envelope + topology are identical to 0.3.2. There is **one
behavioral change**, and it only affects a *misconfigured* reply setup.

### Behavioral change: unroutable replies now dead-letter the command

Responses (and status updates) are now published with the AMQP **mandatory** flag.

- **Before (0.3.2):** if a caller's `reply_to` exchange existed but had **no queue
  bound** for the `response` routing key, the broker silently discarded the
  response and the command was acknowledged as if it had been delivered — a silent
  data loss.
- **Now (0.3.3):** the broker returns the unroutable message, the SDK
  reports the failure, and the command is NACKed to the dead-letter queue with one
  clean ERROR line (and a new `reply_unroutable_failures` counter). The failure is
  now **visible** instead of silent.

Status updates remain best-effort: an unroutable status logs a single WARNING and
processing continues.

### Are you affected?

You are affected **only if** a caller sends commands with `reply_to` set to an
exchange that **exists but has no bound reply queue**. If your callers either leave
`reply_to` empty (fire-and-forget) or correctly bind a reply queue, you will see
**no difference** — replies are delivered exactly as before.

Signs you might be affected: callers that appeared to "work" but never actually
received responses, or commands that completed while their replies seemed to vanish.

### What to do

- **Recommended:** bind a **durable, non-auto-delete** queue to the reply exchange
  for routing key `response`. See
  [`examples/hello_world_service/scripts/send_hello.py`](examples/hello_world_service/scripts/send_hello.py)
  for a runnable caller that does exactly this. An `auto_delete` reply queue
  disappears when its consumer disconnects and turns every in-flight reply
  unroutable, so declare it `durable=True, auto_delete=False`.
- **Or**, for commands that don't need a response, set `reply_to` to `""`
  (fire-and-forget) — the SDK skips the reply entirely.

A correctly-wired service and caller need **no changes**.

### Also in 0.3.3 (no action needed)

- **`reply_unroutable_failures`** counter on the transport, so operators can tell
  "reply exchange missing" apart from "reply queue missing/unbound".
- The per-message **retry counter is now size-bounded** (oldest-entry eviction at
  10,000 tracked messages) — a memory-growth fix for long-running, multi-replica
  deployments. No configuration change.

---

## Upgrading from a clone / source install

If you install from source, check out the target release, reinstall with the
test toolchain (the `dev` extra), and re-run the unit tests (see
[CONTRIBUTING.md](CONTRIBUTING.md) for the integration tests, which need a
broker):

```bash
git fetch --tags
git checkout v0.3.6
pip install -e ".[dev]"
pytest -m "not integration"
```
