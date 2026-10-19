# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.6] - 2026-10-18

Dependency, security and reliability release. **No public method or
parameter was removed or renamed**, but several behaviors change, and input
that was never valid but used to be accepted is now refused: read "Changed"
and [UPGRADING.md](UPGRADING.md) before upgrading. 0.3.4 and 0.3.5 were not
released (numbers skipped), so this entry lists every change since 0.3.3.

**Check before upgrading.** Each line names a change below that can need
an action on your side; the details are in its entry.

- Dependency pins: a pin of `cryptography` below 50.0.0 or of
  `azure-core` below 1.38.0 no longer resolves ("Security").
- Callers: one reply per command, after the last attempt (allow for the
  retries in a reply timeout); `reply_to` may not name a `cmd.`, `dlq.`
  or `amq.` exchange; envelope timestamps must be ISO 8601 text and
  `priority` an integer; blob URIs with a backslash, C1 control
  characters or line / paragraph separators are refused.
- Handlers: handlers must be `async def`, with names a command can
  carry; results must be JSON (no `bytes`) and small enough to reply
  with; `emit_status` takes text; `overwrite` takes a bool;
  `upload_stream` chunks must be bytes; invalid upload paths are
  refused; the message models are read-only; command bodies nested more
  than 256 levels deep are dead-lettered.
- Storage: `download_json` / `upload_json` take only a JSON object or
  array; `KBM_LEDSAS_ALLOWED_CONTAINERS` (new, optional) limits `ctx.blob`
  to listed containers; the message models refuse an envelope of another
  kind.
- Configuration: broker URLs must be `amqp://` or `amqps://` with a
  host, without `no_verify_ssl`, and with a whole-number `?heartbeat=`
  from 0 to 600; boolean settings refuse values that are not booleans;
  each service on one host needs its own health port.
- Operations: alert on `Max retries (N) exceeded` rather than on
  `Uncaught exception in handler`; JSON and text log timestamps changed;
  a handler that can run longer than 30 minutes needs a higher broker
  `consumer_timeout`; a handler that blocks the event loop longer than
  about two heartbeat intervals (60 s by default) loses the connection,
  and those redeliveries now count toward `KBM_LEDSAS_MAX_RETRIES`.

### Security

- **Raised the `aiohttp` floor to `>=3.14.3`** (was `>=3.8.0`), which
  fixes CVE-2026-69244. `aiohttp` serves the SDK's health endpoints and
  is the async transport of `azure-storage-blob`.
- **Raised the floor of the transitive `cryptography` dependency to
  `>=50.0.0`** (was `>=48.0.1`), which fixes CVE-2026-69247
  (GHSA-g6cj-pr64-35w5). `cryptography` still arrives through
  `azure-storage-blob`; the floor keeps every install off the vulnerable releases.

  **Action required only if you pin `cryptography` below 50.0.0** in
  your own requirements or constraints: the install then fails to
  resolve. Raise your pin.
- **Added an `azure-core` floor of `>=1.38.0`** (`azure-core>=1.38.0,<2.0`;
  there was none), which fixes CVE-2026-21226 (PYSEC-2026-1208). The SDK
  uses `azure-core` directly, and it also arrives through
  `azure-storage-blob`; without the floor, upgrading the SDK in place
  (`pip install -U kbm-ledsas-sdk`) kept an older, vulnerable
  `azure-core` that was already installed. **Action required only if you
  pin `azure-core` below 1.38.0**: raise your pin.

### Added

- **Container base image.** `ghcr.io/kebormed/kbm-ledsas-base-direct:<version>`
  (published under the KeborMed GitHub organization) is the official Python
  3.11 Alpine Linux image (`linux/amd64` and `linux/arm64`) with
  `kbm-ledsas-sdk` of that version and its runtime dependencies installed
  from PyPI, a pip constraints file
  (`PIP_CONSTRAINT=/etc/kbm-ledsas-pip-constraints.txt`) that pins the SDK
  version and the `cryptography` floor, and the non-root user `kbmuser` (uid
  1000) with the working directory `/app`; its tag is the SDK version. The
  README section "Container base image" and
  [COMPATIBILITY.md](COMPATIBILITY.md) describe it, and
  `examples/hello_world_service/Dockerfile` builds on it.

### Changed

- **A command gets one reply, when it has finished.** Before, every
  attempt replied: a handler that failed with a retryable error
  (`Retryable`, a timeout, an unexpected exception) sent an error reply
  and was then retried, so a caller that took the first reply as the
  outcome reported a failure for a command that then succeeded (and if
  it had already removed its reply exchange, the successful retry was
  dead-lettered). Now an attempt that will be retried sends no reply;
  the one reply is the handler's result, a non-retryable error, or the
  last attempt's error once `KBM_LEDSAS_MAX_RETRIES` is spent. The
  error payload keeps its shape (`{code, message, retryable}`);
  `retryable: true` in a reply means the failure was transient and the
  retries are spent. **Action:** a caller that waited for several replies
  can stop at the first; allow for the retries in its reply timeout
  (with the default 3 retries the backoff alone is 7 to 10 seconds).
  The generic `Timeout` message is now
  "Processing timed out" (it no longer says "will retry").
- **A handler's return value is checked before any retry decision.**
  Returning `None` (a handler with no `return`, typical for
  fire-and-forget) is now treated as `{}`; before, it failed the
  SDK's own response model, ran the handler 4 times (each logging a
  traceback) and dead-lettered the command. Returning anything else
  that is not a JSON-serializable dict is reported once (one ERROR, no
  traceback), replied as `Permanent` and dead-lettered **without
  retrying**, since the handler's side effects have already happened;
  before, a non-serializable value surfaced as a reply-publish failure
  that blamed the reply exchange. The dict is checked (and serialized)
  only for a command with `reply_to`, whose result becomes the reply: a
  fire-and-forget command gets no reply, so only its result's type is
  checked (a value that is not a dict, or an awaitable that was never
  awaited, is refused), and a dict is acknowledged whatever it holds,
  without being walked or serialized on the event loop.
- **Errors that cannot succeed on a retry are not retried.** If a
  handler lets one of these propagate, the SDK now logs one ERROR line
  (no traceback), replies `Permanent` and dead-letters the command at
  once: a storage error with an HTTP 4xx status other than 408/429
  (missing blob, invalid container or blob name, authorization,
  existing blob with `overwrite=False`), an invalid blob URI, and
  `download_json` content that is not valid JSON, `download_text`
  content that is not UTF-8, and invalid arguments to an SDK call
  (`upload_text` given a non-string, `upload_json` given an object with no
  JSON form, `emit_status` with a `progress` that is not a number in
  `[0.0, 1.0]`, `download_stream` with a `chunk_size` that is not a
  positive integer). Before, they were retried as `UnexpectedError`, with
  a traceback (and the storage service's XML error body) on every
  attempt. The exception types your handler sees are unchanged. The
  `download_*` methods' exceptions are now documented in the API
  reference.
- **`@app.handler` rejects names a command cannot carry.** The
  command name must match `^[A-Za-z][A-Za-z0-9_]*$` (at most 64
  characters) and the version must look like `1.0`, the same rules the
  envelope enforces. Before, `@app.handler("process-data")` registered
  silently and every command for it was dead-lettered without a reply.
  **Action:** if registration now raises `ValueError`, rename the
  command (callers must send the new name).
- **`@app.handler` refuses a handler that is not `async def`.** A plain
  `def`, a lambda or an async generator now raises `TypeError` at
  registration. Before, it registered, and every command ran its body
  (side effects included) and then failed with `TypeError: An
  asyncio.Future, a coroutine or an awaitable is required`, 4 times with
  a traceback each, before an `UnexpectedError` reply. **Action:** define
  handlers with `async def` (run blocking work with
  `await asyncio.to_thread(...)`).
- **`BlobRef` rejects malformed URIs it used to accept.** A container
  name Azure would refuse (uppercase, `_`, leading or trailing or
  double hyphen, over 63 characters), user info or a port
  (`azblob://user:pw@c/...`, which became part of the container name),
  any query parameter other than `versionId` (a SAS `sig` used to be
  kept in `.uri` and logged), and a `#` fragment (silently cut from the
  blob path) now raise `ValueError`. `versionId` is matched in any
  letter case: `?versionid=` (the storage REST API's spelling) used to
  be ignored, so the current blob was read instead of the requested
  version. The `versionId` value itself must look like a blob version id
  (letters, digits, `:`, `.`, `-`, `_`, checked after percent-decoding):
  `?versionId=1;sig=...` or `?versionId=%0a` used to be accepted, carrying
  the extra text into `.uri` and `.version_id`.
- **`/ready` reports a broker outage.** The default `transport`
  readiness check now requires the RabbitMQ connection to be up; during
  an outage `/ready` returns `503` until the SDK has reconnected by
  itself. Before, it stayed `200` throughout. Blob storage is not
  probed (the docs used to say it was). The outage and the recovery are
  also logged as SDK events, in the SDK log format: one WARNING when the
  connection is lost and one INFO when it is back. Before, only the AMQP
  client library's own lines (outside the SDK log format) showed them.
- **`download_stream` honors `chunk_size`.** It now reads the blob with
  one ranged request per chunk, so every chunk is `chunk_size` bytes
  (the last may be shorter) and memory use is bounded by `chunk_size`.
  Before, the argument was ignored: chunks were always 4 MiB, and the
  first request buffered up to 32 MiB. Without a version, the ranged
  reads are pinned to the blob's ETag, so a blob overwritten
  mid-download fails the download instead of mixing contents.
- **Startup failures are one line.** A broker that refuses the
  connection or is unreachable at startup, or that rejects the setup
  (for example an unknown virtual host), is logged as one ERROR line
  naming the broker URL (credentials removed) and the cause, followed by
  the exit line; no traceback (a traceback used to be embedded in the
  error record, and an unknown virtual host produced several). For an unknown virtual host the line
  carries the broker's own reason (`530 NOT_ALLOWED - vhost ... not
  found`) where the AMQP client library reports it (aio-pika 9.x), and
  otherwise names that cause instead of the library's
  `Expected one of ['Connection.OpenOk']` message.
  A queue that already exists on the broker with other arguments (for
  example one declared by hand or by another tool with other limits, or
  kept in a reused local broker volume)
  now gives one line that names the queue and says to delete it once
  (the service re-creates it); before, the first of three ERROR records
  embedded the client library's traceback and none named the fix.
  Configuration errors found while reading the environment now use the
  SDK log format (`KBM_LEDSAS_LOG_FORMAT`, JSON by default) like every
  other startup error, and so do the warnings logged while it is read (the `KBM_LEDSAS_ALLOW_INSECURE_AMQP` acknowledgment, now logged once
  instead of twice). Before, those warnings were bare text on stderr.
- **`KBM_LEDSAS_SERVICE_NAME` overriding the constructor is logged.**
  When the variable is set and differs from
  `ServiceApp(service_name=...)`, the SDK logs a WARNING naming both,
  since the variable renames the service's exchange and queue. The
  startup line names the effective service.
- **Replies carry `correlation_id` exactly as it was sent.** The SDK used
  to lowercase it, so a caller that sent an uppercase id (a SQL Server
  `uniqueidentifier`, .NET `ToUpper()`) and matched the reply with `==`
  never found its reply. `message_id` is still normalized to lowercase.
- **An invalid upload `path` is refused before anything is written.**
  Every `upload_*` method raises `ValueError` (replied `Permanent` if
  uncaught) for a path with `?` or `#`, control or Unicode format
  characters, `.` / `..` segments, or more than 1024 characters. Before,
  `path="q/x.bin?versionId=abc"` wrote a blob literally named that and
  returned a reference to a version that does not exist, and
  `path="q/x#y.bin"` wrote the blob and then failed, leaving it orphaned.
  **Action:** if an upload now raises `ValueError`, choose another path.
- **`BlobRef` takes only `uri`, and its errors do not repeat secrets.**
  `BlobRef(uri=..., version_id="v1")` now raises instead of silently
  dropping the version (put it in the URI as `?versionId=`). A rejected
  value is quoted only as `scheme://host/path` (query string, fragment
  and user info redacted), and `str()` of the error no longer includes
  the input value: an https URL with a SAS signature passed as a blob
  URI used to be copied verbatim into the ERROR log and the reply.
- **A broker restart while a command runs no longer dead-letters it or
  logs tracebacks.** When the connection to the broker is lost while a
  handler runs, the command's reply and ACK cannot be sent on that
  connection.
  - If the client has reconnected and the broker delivers the command
    again to this service instance while it is still in progress there
    (its handler running, or the command waiting for a worker), the
    redelivery is handed to that command: its outcome (the one reply,
    then the ACK, requeue or dead-letter) settles the redelivered
    command, which therefore runs once. One INFO line (`Redelivered
    command arrived while it is still in progress here ...`); the
    `AMQP consumer stopped ...` INFO line at shutdown carries the total
    as `redeliveries_adopted`. With several instances the broker may
    deliver it to another instance instead, which runs it too. Before,
    the duplicate `message_id` guard dead-lettered the redelivery, and
    the original's ACK then failed on the lost connection.
  - If the handler finishes while the connection is still down, the SDK
    logs one WARNING (`Reply to exchange '...' not sent: the connection
    to the AMQP broker was lost ...`, or `ACK not sent: ...` when there
    is no reply to send) and leaves the command to the broker, which
    delivers it again (after a broker restart, only a command published
    as persistent): it runs again, so keep handlers idempotent
    (`ctx.idempotency_key`). A requeue for a retry that could not be
    sent still counts as an attempt, and a command whose dead-letter
    could not be sent keeps the attempts it had used, so its redelivery
    does not start a new set of retries. Before, this logged `Failed to
    ACK message` with a traceback, `Uncaught exception in
    _handle_command`, a second NACK that failed too, and an ERROR saying
    the command would be dead-lettered (it was not) and blaming the
    reply exchange.

  The outage itself is one WARNING and the reconnection one INFO. The
  AMQP client library's own line with the broker's close reason no longer
  ends in a bare `NoneType: None`. A second command with the same
  `message_id` that arrives while the first one's delivery is still live
  (a sender reusing the id) is still dead-lettered. A message the SDK
  dead-letters as it arrives with one WARNING (for example a body over
  `KBM_LEDSAS_MAX_PAYLOAD_BYTES`, an envelope `type` other than
  `command`, or a `message_id` already in progress) gets no second SDK
  line when the connection is lost just as it is rejected; the broker
  delivers it again. Before, the SDK also logged `Unexpected error
  processing message; dead-lettering` with a traceback and tried to
  reject the message a second time.
- **A retried failure logs one WARNING per attempt and a traceback only
  on the last attempt.** An exception a handler does not catch is
  still retried as `UnexpectedError` (the reply is unchanged), but each
  attempt now logs one WARNING instead of an ERROR with a traceback:
  - a transient storage or network error (blob storage unreachable or
    not answering, an HTTP 408, 429, 500, 502, 503 or 504 from storage, a
    Python `ConnectionError`): `Transient error in handler: <reason>`,
    never with a traceback;
  - any other exception: `Uncaught exception in handler`, with
    `internal_message`, `error_class` and `error_location` (file, line,
    function). The traceback is attached only on the command's last
    attempt, so a command that succeeds on a retry logs none. (If the
    connection to the broker is lost before that last failure is
    dead-lettered, the broker's redelivery runs as a last attempt again
    and, if it fails, logs its own traceback.) This includes a storage
    failure that is neither transient as above nor an HTTP 4xx (for
    example an HTTP 501, 505 or 507, or a response cut off mid-transfer);
    for it the storage client also logs its own ERROR with a traceback on
    every attempt (`Failed to download blob`, `Failed to upload blob`,
    `Streaming download failed` or `Streaming upload failed`).

  Each record carries `will_retry` and `attempt`. The `Retry k/N, waiting ...` line is
  now INFO, and the storage client's own line for a transient failure
  (`Failed to download blob (transient storage error)`, and the same for
  uploads and streams) is INFO without a traceback. The failed command
  keeps its one ERROR, `Max retries (N) exceeded, sending to DLQ`.
  Before, unreachable storage logged two ERRORs per attempt (one with a
  traceback) and a WARNING. **Action:** a log alert on `Uncaught
  exception in handler` at ERROR level no longer fires; alert on the
  `Max retries (N) exceeded` ERROR (one per command whose retries are
  spent; after a lost broker connection it can come again for the
  redelivered command), or on the WARNING records.
- **A non-retryable failure logs one ERROR line.** The "sending to DLQ"
  line that follows the handler layer's own line is now INFO, so
  `Permanent`, an invalid return value or `HandlerNotFound` give one
  ERROR, and `DeadlineExceeded` gives one WARNING and no ERROR (before,
  each added a second ERROR, `Command failed with permanent error,
  sending to DLQ`). The same applies to a failed reply publish (the
  transport's line is the ERROR).
- **A message body with a huge number is dead-lettered with one line.**
  An integer literal longer than 4300 digits used to log a traceback and
  the interpreter's advice to raise its limit; it now logs one ERROR
  (`Message body contains a numeric literal too large to parse`).
- **A message body nested more than 256 levels deep is dead-lettered
  on every supported Python version.** Arrays and objects count
  anywhere in the parsed body; the body object is level 1 and its
  `payload` object level 2. A deeper body is dead-lettered when it
  arrives, before its envelope is validated or a handler runs: one ERROR
  line (`Message body exceeds JSON-parsing nesting limit;
  dead-lettering`, with `error_type` `ExcessiveNesting` and `error`
  `JSON nested more than 256 levels deep`), no traceback, no retry and
  no reply. Before, the only limit was the JSON parser's own, which
  differs between Python versions (about 1000 levels on Python 3.11,
  about 10000 on 3.13): a body nested more deeply than 256 levels
  reached the handler up to that depth, so the same body could be
  processed on Python 3.13 and dead-lettered on 3.11, and the `error`
  field carried the interpreter's own message. A body up to 256 levels
  deep is processed as before. Two cases still follow the parser's own
  limit (see "Known limitations" below). A body with more than 256 `[`
  and `{` characters in all is walked once when it arrives to measure
  its depth, which takes CPU time: up to about 1 s on Python 3.13 and
  1.3 s on 3.11 for a 16 MiB body (see "Malformed message bodies" in the
  API reference). A recursion error that the body's depth did not cause
  is now logged as `Unexpected error processing message;
  dead-lettering`, with its own text and a traceback, when it is raised
  after the body is parsed (for example during envelope validation) or
  by the parser on a body with at most 256 `[` and `{` characters (for
  example with the interpreter's recursion limit lowered far below its
  default); before, it was logged as `ExcessiveNesting`. If the parser
  gives up on a body with more than 256 of these characters, the line
  is still `ExcessiveNesting`, whatever the cause; with the
  interpreter's default limits the parser gives up only on a body
  nested far more than 256 levels deep. Either way the body is
  dead-lettered once, with no retry and no reply. **Action:** if your
  commands carry data nested more than 256 levels deep, flatten it, or
  upload it to blob storage and send its reference.
- **The log line for a message body that cannot be used identifies the
  message.** The line for a body that is too large, not UTF-8, not
  JSON, nested more than 256 levels deep, holds a numeric literal too
  large to parse or fails envelope validation, and the `Unexpected
  error processing message` line, now carry `amqp_message_id` and
  `amqp_correlation_id` (the message's AMQP `message_id` and
  `correlation_id` properties, when set; at most 128 characters) and
  `body_bytes` (the body's size in bytes), read without parsing the
  body, so you can find the message in the dead-letter queue. Before,
  these lines named no message.
- **JSON log timestamps are UTC with milliseconds.** `timestamp` is now
  ISO 8601 UTC with a `Z` suffix (`2026-10-14T05:43:15.123Z`); it was
  local time with whole seconds and no offset, so records from hosts in
  different time zones disagreed and records within a second could not
  be ordered. `json_log_formatter()` uses the same format by default
  (an explicit `datefmt` still applies). **Action:** a log pipeline that
  parsed the old format should accept ISO 8601 with a `Z` suffix.
- **A blob call against unreachable storage gives up sooner.** The
  storage client's own retry policy is bounded to 2 retries (about 3 s
  and 5 s apart, 10 s to open a connection): one call against a refused
  connection now fails after about 8-10 s instead of about 85 s, so the
  command's reply (after the SDK's own retries) arrives in about a
  minute instead of about 6 minutes.
- **A broker URL with unencoded credentials is refused clearly.** A
  user name or password holding `@`, `#` or `?` made the URL parse to a
  wrong host, and the startup error named a fragment of the password as
  the host. The service now refuses such a URL with one line that says
  to percent-encode the credentials and quotes no part of the URL.
- **Only `amqp://` and `amqps://` broker URLs with a host are accepted.**
  `KBM_LEDSAS_RABBITMQ_URL` with another scheme (`http://`, a typo), no
  scheme (`user:pass@host:5672`), no host, a port that is not 1-65535 or
  whitespace (for example an inline `# comment` that
  `docker run --env-file` passes through) now stops the start with one
  `Configuration error` line that quotes no part of the URL. Before, the
  client library spoke plain AMQP for any scheme (bypassing the cleartext
  refusal for remote hosts), and for a URL without scheme or host it
  retried forever, logging the URL with its password every 5 seconds.
  **Action:** fix such a URL.
- **Every `KBM_LEDSAS_*` value is read the same way.** Surrounding
  whitespace and one matched pair of surrounding quotes are removed from
  every variable (before, only from the two connection values), so
  `KBM_LEDSAS_LOG_LEVEL="INFO"` (or `KBM_LEDSAS_ALLOW_INSECURE_AMQP="1"`,
  `KBM_LEDSAS_DEBUG="1"`) from a `docker run --env-file` file works.
  `KBM_LEDSAS_HEALTH_HOST` must be an IP address or a host name (checked
  at startup); write an IPv6 address without brackets (`::`, not `[::]`).
- **New `KBM_LEDSAS_CONNECT_TIMEOUT` (default 15 s).** One connection
  attempt to the broker now gives up after this many seconds. A broker
  host that does not answer used to log nothing until the operating
  system gave up (75 s to over 2 minutes), so a liveness probe could
  restart the container before the cause was logged; the SDK now logs
  `Connecting to the AMQP broker at ... (timeout 15s)` first and fails
  the start within the timeout with one ERROR line naming the cause, the
  closing `ServiceApp exiting (startup failure already reported above)`
  line and exit code 1.
- **A handler's `asyncio.CancelledError` or other `BaseException` is a
  handler failure.** Before, a handler that raised `CancelledError` (for
  example by awaiting a task that was cancelled) or a custom
  `BaseException` subclass left its command unacknowledged with no reply
  and no log line: it held a prefetch slot until the process stopped,
  `/ready` stayed 200, and after a restart it did the same again, so
  enough such commands stopped the service. Now it is retried and
  dead-lettered with one `UnexpectedError` reply like any uncaught
  exception. A handler that raises `SystemExit` or `KeyboardInterrupt`
  no longer ends the process (which then crash-looped on the redelivered
  command): it is replied `Permanent` once and dead-lettered.
- **A refused reply no longer leaves a broker channel open.** Each reply
  and status is published on a short-lived channel. When the broker
  refused the publish (a missing or internal reply exchange, no write
  permission, a message over the broker's size limit), the client library
  reopened that channel by itself and kept it: every refused reply left
  one open channel, until the connection reached the broker's channel
  limit and could not reconnect any more; and after a broker restart the
  connection re-checked a deleted reply exchange and failed every
  reconnect. These channels are now never reopened. A refusal for missing
  permission or an oversized message is now one ERROR line without a
  traceback, with a hint that names the actual cause.
- **A result too large to reply with is refused before publishing.** A
  handler result whose serialized reply is larger than
  `KBM_LEDSAS_MAX_PAYLOAD_BYTES` (16 MiB by default, the broker's default
  maximum message size) now gets one `Permanent` error reply (`returned a
  result too large to send as a reply ...`). Before, the broker refused
  the reply, the caller got no reply at all and the log blamed the reply
  exchange. A command without `reply_to` publishes no reply, so its result
  is not size-checked. **Action:** return large results as a blob
  reference.
- **Reply payload keys and values are treated alike at every level.** A
  top-level non-string key (`{1: "a"}`, as `df.to_dict(orient="index")`
  returns) is now converted like a nested one (`"1"`), and a `None` key
  becomes `"null"` (it was `"None"`), as `json.dumps` writes them. A key
  type with no JSON form (a tuple, a date) is refused, and so are two keys
  that become the same text. A refused result's error names where
  (`payload["rows"][3]: the key (1, 2) (tuple) is not a JSON object
  key`) instead of `1 validation error for Response`, and no longer gives
  interpreter advice. The accepted value conversions are listed under
  "Payload" in the API reference.
- **Timeout replies say what happened.** The reply `Handler timed out
  after 1800s` is now sent only when the SDK's own timeout expired. A
  `TimeoutError` raised by the handler itself is replied `Handler raised
  TimeoutError` (still code `Timeout`, retried), with its message and
  location logged; with `KBM_LEDSAS_HANDLER_TIMEOUT=0` the old text read
  `Handler timed out after Nones`. When the time left to the command's
  `deadline` runs out, the reply is now `DeadlineExceeded` (not retried)
  at once, instead of a retryable `Timeout` and a redelivery.
- **Invalid blob-call arguments are refused before any I/O** and replied
  `Permanent` once: a `blob_ref` that is not a string or `BlobRef`
  (for example `None` from a missing request field; it used to fail as an
  `AttributeError`, retried 4 times with a traceback), an `upload_*`
  container that is not a valid container name, `upload_bytes` data that
  is not `bytes`, `bytearray` or `memoryview` (a `bytearray` used to fail
  inside the storage client; a `str` was uploaded as UTF-8: use
  `upload_text`), an `upload_stream` source that is not an async
  iterable, and `upload_text` text with a lone surrogate. **Action:** a
  handler that passed a `str` to `upload_bytes` should call `upload_text`.
- **`upload_json` writes standard JSON.** `NaN` and `Infinity` are
  written as `null` (as in replies); they were written as the `NaN` /
  `Infinity` tokens, which strict JSON parsers reject.
- **`BlobRef` follows the storage naming rules.** A container name must
  be 3 to 63 characters (one-character names were accepted, two-character
  ones refused; the message now states the rule), and a `.` path segment
  and a `?` with no `versionId` are refused, as for upload paths.
- **A `reply_to` naming a service exchange or a reserved broker exchange
  is refused.** A command whose `reply_to` starts with `cmd.` or `dlq.`
  (a LEDSAS service's command or dead-letter exchange) or with `amq.`
  (the broker's reserved exchanges, such as `amq.topic` and
  `amq.direct`) is now an invalid envelope, dead-lettered before its
  handler runs; its reply used to be published into that service's
  command or dead-letter queue, or to whatever was bound to the reserved
  exchange. **Action:** a caller that named `amq.topic` or `amq.direct`
  as its `reply_to` declares a reply exchange of its own.
- **Commands waiting for a worker at shutdown are given back at once, not
  run.** On SIGTERM the service now stops receiving from the broker (which
  then delivers the queue's commands to the other replicas) and returns
  every command that had not yet started to the broker at once, without
  running it (one INFO line names them), so another replica can take it
  while the exit waits only for the handlers already running. Before, the
  service kept receiving until it exited, and started the waiting
  commands after SIGTERM.
- **The text log format shows the failure details.**
  `KBM_LEDSAS_LOG_FORMAT=text` now writes every field of a record as
  `key=value` after the message (it kept only `[corr=...]`, so
  `Retryable error in handler` showed no reason), escapes control
  characters in messages, field values and tracebacks, and uses the same
  UTC timestamp as the JSON format (`2026-10-14T05:43:15.123Z`; it was
  local time with whole seconds). **Action:** a parser of the text format
  should expect the new timestamp and the `key=value` fields.
- **Health endpoints.** A request the health server cannot parse now gets
  a JSON `400` without a `Server` header and without repeating the
  request, logged as one DEBUG line (it was a plain-text `400` echoing
  the request, with the `Server` header, and an ERROR with a traceback
  per request). The "reachable from the network" line is logged only after
  a successful bind, at INFO (WARNING with `KBM_LEDSAS_HEALTH_VERBOSE` on),
  and no longer advises loopback, which would break an orchestrator's
  probes.
- **`SDKConfig` hides the credentials from display hooks too.**
  `rich.print(config)`, rich tracebacks with locals and devtools showed
  the broker password and the storage account key; they now show
  `**redacted**`, as `repr()`, `str()` and `model_dump()` already did.
- **Upload paths and blob URIs refuse names that would not round-trip.**
  An upload `path` with a backslash (storage may store `a\b` as `a/b`,
  so the returned URI named another blob, and `..\..\x` slipped past the
  `..` rule), an empty segment (`a//b`, or more than one leading `/`) or
  a trailing `/` (`dir/`) raises `ValueError` before anything is written
  (one leading `/` is still ignored; `""` still means an auto-generated
  name). `BlobRef` refuses an empty blob name (`azblob://data//`, which
  failed inside the storage client with a traceback on every attempt and
  was retried), an empty segment (`azblob://data//x.json`, which read
  `azblob://data/x.json`), a trailing `/`, and `.` / `..` segments
  separated by backslashes. Each is an invalid blob URI: `Permanent`,
  one line, no traceback. **`BlobRef` is immutable:** assigning
  `ref.uri = ...` raises (it skipped every URI check).
- **A blob overwritten during `download_stream` is retried.** The
  stream's ETag check (HTTP 412, `ResourceModifiedError`) was replied
  `Permanent` as if storage had refused the request; it is now retried
  (one WARNING per attempt: `Transient error in handler: the blob
  changed while it was being downloaded ...`) and the new attempt reads
  the new content.
- **A blob call against storage that never answers gives up in about
  40 seconds.** The storage client now stops waiting after 10 s without
  an answer (the library's own limit was 60 s per attempt, so one call
  against a stalled endpoint took about 190 s, and a command with the
  default retries about 13 minutes before its reply).
- **Health checks have a time limit.** A custom liveness or readiness
  check is cancelled after 2 s (or the `timeout=` it is registered
  with, at most 60 s) and reported unhealthy (`timed out after 2s`), and
  the checks of one probe run at the same time. A check that hung used to
  hang the endpoint (a probe with a 3 s timeout got no answer) and kept
  the service's exit waiting for about 60 s. Stopping the health server
  waits at most about 4 s.
- **A second SIGTERM / Ctrl+C stops at once.** The first still stops
  gracefully (waits for the running handlers); a second one cancels the
  handlers still running and gives their commands back without a reply,
  so they run again on the next start or on another replica. A command
  whose handler has already returned is not cancelled: it still sends
  its one reply and is acknowledged or dead-lettered. Before, a second
  Ctrl+C only logged the shutdown line again.
- **Configuration errors name the variable and its value.** A value
  outside its range is reported as, for example, `Configuration error:
  KBM_LEDSAS_PREFETCH=5000: Input should be less than or equal to 1000`
  (it named the internal field, `prefetch: ...`). A malformed
  `KBM_LEDSAS_BLOB_CONN_STRING` is reported as `KBM_LEDSAS_BLOB_CONN_STRING
  is not a usable storage connection string (...)` without repeating the
  value. **Action:** a boolean setting (`KBM_LEDSAS_GENERIC_ERRORS`,
  `KBM_LEDSAS_HEALTH_VERBOSE`, `KBM_LEDSAS_ALLOW_INSECURE_AMQP`,
  `KBM_LEDSAS_DEBUG`) with a value that is not `true` / `false` (or `1` /
  `0`, `yes` / `no`, `on` / `off`, `t` / `f`, `y` / `n`) now stops the
  start (`... is not a boolean (use true or false; ...)`). It was read
  as `false`: `KBM_LEDSAS_GENERIC_ERRORS=ture` silently sent detailed
  error messages to callers.
- **Envelope timestamps and priority are not converted any more.**
  `sent_at` and `deadline` must be ISO 8601 timestamp strings: a number
  (a Unix time such as `1760000000`) or a numeric string was read as a
  date. `priority` must be an integer: `"5"`, `5.0` and `true` were
  converted. **Action:** a sender that sends any of these gets its
  command dead-lettered as an invalid envelope; send ISO 8601 text and an
  integer.
- **Text with a lone surrogate is refused in what the SDK writes.** A
  reply, an `emit_status` `stage` / `note` or an `upload_json` object
  holding a string with a lone surrogate (such as `"\ud800"`, which a
  JSON request can carry) is refused with a message naming where
  (`emit_status() refused: stage is text with a lone surrogate ...`,
  `upload_json() refused: obj["k"] is text with a lone surrogate ...`);
  JSON wrote it as an escape that strict readers refuse (.NET's
  System.Text.Json throws when the string is read). A reply value with no
  JSON form is now named in the SDK's words (`payload["o"] is an
  instance of Weird, which has no JSON form`); the serializer's own text,
  with the handler's module path, reached the caller.
- **`DeadlineExceeded` has one generic text for both cases.** With
  `KBM_LEDSAS_GENERIC_ERRORS=true` the reply says "Processing deadline
  exceeded" (it said "Request arrived too close to deadline", which was
  wrong for a deadline that passed while the handler ran).
- **A broker restart logs no ERROR record.** The AMQP client library's
  `Unexpected connection close from remote ...` record, which it logs as
  an ERROR when the broker closes the connection, is lowered to WARNING:
  the SDK reconnects by itself and logs the outage (WARNING) and the
  recovery (INFO).
- **The JSON log formatter always writes valid JSON.** An `extra` field
  holding `NaN` / `Infinity` is written as text (`"NaN"`), and a field
  with no JSON form (a dict with tuple keys, a container that contains
  itself) as its `repr()`, and a value whose own `__repr__` raises as
  `"<TypeName (repr failed)>"`; before, the formatter wrote a non-standard
  `NaN` token or dropped the record with a `--- Logging error ---`
  traceback. A message whose `%` arguments cannot be formatted is written
  as its template, as in the text format.
- **A blob URI with a backslash is refused.** `BlobRef` (and every
  `ctx.blob.download_*` call given a string) refuses a backslash in the
  blob name, as upload paths already did: storage may take `a\b` as
  `a/b`, so the blob read was not the one `.path` named, and a check on
  `.path` (a required prefix) did not hold for it. **Action:** a caller
  that sends such a URI gets a `Permanent` reply; send `/`.
- **A refused blob URI is not kept in the error.** `errors()` and
  `json()` of the `ValidationError` hold a placeholder instead of the
  rejected value (they held it in full, so an error reporter could record
  a SAS signature or a password); `str()` already left it out. A URI that
  does not start with exactly `azblob://` is refused with a message that
  says what is wrong with its start (an upper-case scheme, leading
  whitespace, a missing `//`) and quotes it as received, redacted (it
  quoted a re-written, lower-cased value that did start with
  `azblob://`).
- **A storage account key that cannot sign requests stops the start.**
  A `KBM_LEDSAS_BLOB_CONN_STRING` whose `AccountKey` is not valid base64
  (its trailing `=` lost in a copy) or whose `BlobEndpoint` is not an
  `http://` / `https://` URL is refused at start with the documented
  `Configuration error: KBM_LEDSAS_BLOB_CONN_STRING is not a usable
  storage connection string (...)`. Before, the service started, and every
  blob call failed with a signing error that was retried with tracebacks
  and answered `UnexpectedError` (`retryable: true`). Should a request
  still fail to sign, it fails `Permanent`, once, with one WARNING.
- **A stop signal while the service is starting stops the start.** A
  SIGTERM or Ctrl+C while the service was still connecting to the broker
  was not handled: with a broker that did not answer, Ctrl+C left the
  connection's reconnect loop running (the process kept going, the
  connect timeout never applied) and SIGTERM killed the process without
  cleanup. Now it is logged at INFO (no traceback), the connection being
  made is closed, and the process exits with code 0.
- **The health server closes slow and idle connections.** A connection
  that has not sent a complete request within 10 s is closed (before, a
  client could hold a partial request open for about an hour), and at
  most 100 connections are open at a time. Above that, the oldest
  connection that is not answering a request is closed, so idle
  connections a client holds cannot keep the probes out; the new
  connection is closed only when every other one is answering a request
  (one WARNING a minute).
- **The SDK's log records all use `KBM_LEDSAS_LOG_FORMAT`.** The
  `Registered handler for <name> v<version>` records were written when
  the handlers were registered (as your module was imported), before the
  log settings were applied, so they came out in your root logger's
  format and ignored `KBM_LEDSAS_LOG_LEVEL`. They are now written when
  `app.run()` starts, in the SDK's format.
- **Log records name what is missing and what happens.** A download from
  a container that does not exist logs WARNING `Container not found` (it
  said `Blob not found`); both records carry the storage `error_code`. An
  envelope that fails validation is logged with the body's
  `envelope.message_id` / `envelope.correlation_id` when they can be
  read, so the dead letter can be matched to its record. A body that is
  not JSON logs `Failed to parse message JSON; dead-lettering`, like the
  other malformed-body records.
- **`download_json` treats JSON that Python cannot parse as a content
  error.** Content holding an integer with more than 4300 digits, or
  nested more deeply than Python's JSON parser reads (about 1,000 levels
  on Python 3.11, 10,000 on 3.13), used to fail with a `ValueError` /
  `RecursionError` the SDK retried as an unexpected error (a traceback,
  `UnexpectedError` after the retries). Now, like content that is not
  JSON, it is logged once (`Failed to parse blob as JSON`, no traceback)
  and, uncaught, replied `Permanent` (`blob content cannot be parsed as
  JSON (...)`) without a retry. The exception types are unchanged.
- **`upload_json` and `upload_text` store a content type.** Blobs written
  by `upload_json` are stored as `application/json` and by `upload_text`
  as `text/plain; charset=utf-8` (before: `application/octet-stream`,
  the storage default, which `upload_bytes` / `upload_stream` still use).
  A name the SDK generates (no `path`) ends in `.json` / `.txt` for these
  two (before: `.bin` for all).
- **`"reply_to": null` is fire-and-forget.** A command whose envelope
  has `reply_to` set to JSON `null` was dead-lettered as an invalid
  envelope (its handler never ran), while `null` was accepted for the
  other optional fields. It is now read as `""`: the command runs and no
  reply is sent.
- **An empty `KBM_LEDSAS_SERVICE_NAME` is read as unset.** A placeholder
  `KBM_LEDSAS_SERVICE_NAME=` (in a `.env` file, `docker run -e`) replaced
  the `ServiceApp(service_name=...)` value and stopped the start with a
  `Configuration error` that blamed the constructor. Now an empty or
  blank value is ignored, as for `KBM_LEDSAS_TENANT`.
- **A handler that returns a coroutine** (`return helper()` without
  `await`) still gets the `Permanent` reply, now with the hint `did you
  forget 'await'?`; the SDK closes the coroutine, so Python no longer
  prints its own `coroutine ... was never awaited` warning as raw text
  between the JSON log records.
- **Log texts.** A blob URI refused for a control character, an invisible
  (Unicode format) character or a `..` segment says the character or
  segment "is not allowed in a blob URI" (before: "reject at the
  boundary"). The health server's address is logged with an IPv6 address
  in brackets (`http://[::1]:8090`), so it can be copied into a probe. A
  failed reply or status publish no longer quotes the client library's
  connection object (with the broker user name and a memory address):
  it is shown as `<Connection>`, and a URL's user name and password as
  `***:***`.
- **A `message_id` can be reused as soon as its reply has arrived.** A
  command that reused the `message_id` of a command whose reply had just
  been sent (its acknowledgement still on the way) was dead-lettered as a
  duplicate, without a reply. It now waits for that acknowledgement, at
  most 10 s, and then runs.
- **Every way of making a `BlobRef` checks its URI.** `model_copy(update=...)`,
  `copy.replace()`, `model_construct()` and `model_validate()` of an
  instance gave a `BlobRef` whose URI was never checked (a SAS signature
  or a password in `.uri`, which the blob calls log). They now check it as
  `BlobRef.from_uri()` does, and every `ctx.blob` call checks a `BlobRef`
  again before it uses or logs its URI. The blob name is documented as
  literal: it is not percent-decoded (only the `versionId` value is).
- **Error replies are always readable text.** An error message or
  `user_message` that quotes a request value with a lone surrogate (such
  as `"\ud800"`) was sent as is, and strict JSON readers could not read
  the reply. Each lone surrogate is now replaced by U+FFFD.
- **`user_message` is a `str` or `None`.** Another type now raises
  `TypeError` when the error is created (replied `Permanent` with that
  text, or the generic message when `KBM_LEDSAS_GENERIC_ERRORS` is
  true; not retried). An empty `user_message` is sent as the (empty)
  message: the internal message is no longer sent in its place.
- **`emit_status` refuses `True` / `False` as `progress`** (`TypeError`,
  as for any value that is not a number).
- **`ServiceApp(service_name=...)` checks the name when it is created.** A
  value that is not a string raises `TypeError` and one that breaks the
  service-name rule raises `ValueError` there, instead of when
  `app.run()` starts. A `None` name is shown as `None` (it was shown as
  the string `'None'`).
- **A connection string without a credential is refused at start.**
  `KBM_LEDSAS_BLOB_CONN_STRING` with neither an `AccountKey` nor a
  `SharedAccessSignature` (or an empty one) started normally, and then
  every storage request failed with HTTP 403. It now stops the start
  with `Configuration error: KBM_LEDSAS_BLOB_CONN_STRING is not a usable
  storage connection string (it carries no credential: ...)`.
- **A misspelled `KBM_LEDSAS_*` variable is reported.** A variable with
  the prefix that is not one of the SDK's settings (`KBM_LEDSAS_PREFECH`)
  was ignored silently while the default applied. The SDK now logs one
  WARNING at start naming it and the closest setting (`did you mean
  KBM_LEDSAS_PREFETCH?`). The variables Kubernetes sets for a Service in
  the namespace (`KBM_LEDSAS_<NAME>_SERVICE_HOST` and the rest of that
  Service's group) are not reported. Documented settings such as
  `KBM_LEDSAS_DEPLOYMENT_ID` are never reported.
- **An IPv6 broker address keeps its brackets in log records**
  (`amqp://***:***@[::1]:5672/`; the brackets were dropped).
- **The per-command records carry the command's identity.** `Handling
  command <name>`, `Sent response for <name>`, `ACKed command <name>` and
  the `Max retries (N) exceeded` ERROR carry `message_id` and
  `command_name` besides `correlation_id`.
- **`/ready` answers `503` while the service stops.** From the first
  SIGTERM / SIGINT on, the default readiness check fails with `shutting
  down` while the running handlers finish (that can take up to
  `KBM_LEDSAS_HANDLER_TIMEOUT`); `/health` stays `200`. Before, `/ready`
  answered `200` until the process was gone, although the service no
  longer took commands.
- **The `Max retries (N) exceeded` ERROR is logged once the dead-letter
  has been sent** (and the `sending to DLQ` INFO of a non-retryable
  failure likewise). A dead-letter that could not be sent (the broker
  connection was lost, or the broker was unreachable while the service
  stopped) logged the ERROR all the same, although the broker delivered
  the command again; now only the transport's WARNING says so. Alerts on
  the ERROR now count the commands actually dead-lettered.
- **Blob URIs and upload paths refuse C1 control characters and the line
  / paragraph separators.** `BlobRef.from_uri()` and every `upload_*`
  `path` refused C0 control characters, DEL and Unicode format
  characters, but accepted `U+0080` to `U+009F` (among them the one-byte
  terminal escape `U+009B` and the line break `U+0085`) and `U+2028` /
  `U+2029`. They are now refused too: one rule for URIs and paths, which
  covers every character the log formatters escape as a control
  character. **Action:** a blob name that holds one of them
  can no longer be read or written through the SDK.
- **A failing health check is logged once, with secrets masked.** A
  custom check that raised (or timed out) logged one WARNING per probe,
  with the exception text as raised: a check quoting a connection URL
  repeated its password thousands of times a day, and served it in the
  verbose `/ready` body. Now the WARNING comes when the check starts
  failing, when its reason changes and every 10 minutes while it keeps
  failing (`(still failing)`), with one INFO `Health check '<name>' passes
  again` when it recovers; a URL's user name and password and the values
  of `AccountKey=`, `SharedAccessSignature=`, `SharedAccessKey=`,
  `Password=` / `Pwd=` and a `sig=` URL query parameter are masked in the
  record and the body.
- **A blocked event loop is reported.** Blocking code in an `async def`
  handler (`time.sleep`, a CPU-heavy loop, a synchronous client call)
  stops the whole service, health probes included, and the handler time
  limit cannot stop it. After a block of 1 s or more the SDK logs one
  WARNING (`The event loop was blocked for at least <N> s ...`; `<N>` can
  be up to 0.1 s short) naming the handlers that ran during it, at most
  once a minute. When the SDK's own reading of a very large command body
  caused the block, the WARNING says so (`The SDK spent <M> s of it
  reading a command body of <n> bytes ...`) instead of blaming the
  handlers.
- **The memory a service may need for its command bodies is checked at
  start.** In a container with a memory limit, when `KBM_LEDSAS_PREFETCH`
  x `KBM_LEDSAS_MAX_PAYLOAD_BYTES` x 25 (the documented worst case for
  parsed bodies) exceeds the limit, one WARNING says so and names a
  `KBM_LEDSAS_MAX_PAYLOAD_BYTES` value (for a high `KBM_LEDSAS_PREFETCH`,
  with a lower prefetch) that keeps the worst case at half the limit or
  less. With `KBM_LEDSAS_MAX_PAYLOAD_BYTES=0` (no SDK limit) it assumes
  128 MiB bodies, RabbitMQ 3.x's default `max_message_size` (4.x's is
  16 MiB). The defaults are unchanged.
- **An abrupt broker loss logs no traceback.** When the broker was killed
  or the TCP connection cut, the AMQP client library's `Cancelling cause
  reader exited abnormally` WARNING carried a two-part traceback next to
  the SDK's own `Lost the connection to the AMQP broker` WARNING. It is
  now one line naming the cause, as for a broker restart; a reader error
  that is not a lost connection keeps its traceback.
- **The JSON log formatter writes a lone surrogate as U+FFFD.** A message
  or field quoting a request value with a lone surrogate (`"\ud800"`)
  was written as that escape, which strict JSON readers refuse; it is now
  replaced by U+FFFD, as in error replies.
- **A result with an `error` key is a successful result.** Whether a
  command failed was read from its reply: a handler that returned a dict
  with a top-level `error` key (`{"valid": False, "error": "field x
  missing"}`, `{"error": None}`) had its reply sent and the command then
  dead-lettered, and `{"error": {"field": "x"}}` was even run again up to
  `KBM_LEDSAS_MAX_RETRIES` times before that. Now the outcome is decided
  by how the handler ended (it returned, or it raised): any returned dict
  is replied as it is and the command acknowledged. To fail a command,
  raise (`errors.Permanent`, `errors.Retryable`, ...).
- **Binary data in a result is refused whatever its content.** `bytes`
  in a returned dict were written as text when they happened to be UTF-8
  (`b"\x00\x01"` became `"\u0000\u0001"`) and refused otherwise, so a
  handler tested with text-like bytes failed later on real binary data.
  Now `bytes`, `bytearray` and `memoryview` are refused at every level of
  what the reply writes as a JSON object or array (dicts, lists, tuples,
  sets, deques, the items of a generator or iterator, a dataclass's
  fields, an `Enum`'s value), with one ERROR, a `Permanent` reply and no
  retry (`payload["b"] is bytes; a reply carries text, not binary data:
  decode it if it is text (data.decode()), base64-encode it otherwise
  (base64.b64encode(data).decode()), or store it in a blob and return the
  reference`). The key rules (two keys that become the same text, a key
  type with no JSON form) and the lone-surrogate rule now apply in those
  places too: colliding keys in the items of a generator, a deque or a
  dataclass silently lost a value, and a lone surrogate there or in a set
  was sent. A pydantic model is the one exception: it is written with its
  own serialization settings (bytes as UTF-8 text unless it sets
  `ser_json_bytes`), and only the lone-surrogate rule looks inside it. A
  generator or other iterator in a result is read once and sent as the
  list of its items (it was read by the check and then sent as an empty
  list); an exception while reading it refuses the result naming where.
  The value conversions the reply applies are listed under "Payload" in
  the API reference. **Action:** a handler that returns `bytes` holding
  text must return `data.decode()` instead (in a dataclass too); keep
  bytes in a pydantic model only with `ser_json_bytes="base64"`, or
  convert them first. These rules apply to the result of a command with
  `reply_to`, which becomes the reply; the result of a fire-and-forget
  command is not walked (see "A handler's return value is checked before
  any retry decision").
- **A health port that cannot be bound stops the service at start.**
  When `KBM_LEDSAS_HEALTH_PORT` was taken (or `KBM_LEDSAS_HEALTH_HOST` was
  not an address of the machine), the service logged a WARNING and ran
  without health endpoints, so the probes reached whatever else listened
  on the port and reported its answer as this service's health. Now it
  stops before running any command: one ERROR naming the setting to
  change (`Could not start the health server on
  KBM_LEDSAS_HEALTH_PORT=8090 (KBM_LEDSAS_HEALTH_HOST=127.0.0.1): the
  address is already in use ...`), the usual `ServiceApp exiting (startup
  failure already reported above)` line, exit code 1, no traceback; the
  broker connection opened first is closed and the commands it had
  received go back to the queue. `KBM_LEDSAS_HEALTH_PORT=0` still runs
  without a health server. **Action:** give each service on one host its
  own port.
- **A health check whose result has no truth value fails instead of answering 500.**
  A check returning an object whose truth value raises (a numpy array of
  more than one element, a pandas Series) made `/ready` answer HTTP 500
  and logged an ERROR with a traceback on every probe. It now counts as
  a failing check, like one that raises: `503` naming the check, one
  WARNING (`Health check '<name>' raised ValueError: ...`), logged again
  only as for any failing check.
- **Lone surrogates are refused in blob paths and blob URIs.** An upload
  path with a lone surrogate (`"s\ud800.txt"`, which a JSON request can
  carry) was sent to storage, which cannot encode it; a `BlobRef` with one
  was refused with pydantic's `unable to parse raw data as a unicode
  string`. Both now say `contains a lone surrogate U+D800, which UTF-8
  cannot encode` before anything is sent (`Permanent`, not retried).
- **`emit_status` refuses a `stage` or `note` that is not text.** A
  `stage` such as `123` or `["a"]` was sent, and one with no JSON form
  (`object()`) failed in the transport and was retried as an unexpected
  error. Now a `stage` that is not a `str`, or a `note` that is neither
  `None` nor a `str`, raises `TypeError` (`emit_status() takes stage as a
  str, got int`) before anything is sent; uncaught, it is replied as
  `Permanent` (not retried). An `Enum` member whose value is a `str`
  (`class Stage(Enum): DOWNLOADING = "downloading"`) is still accepted
  and sent as that value, as before. **Action:** pass text: `str(n)` for
  a number, `str(stage.value)` for an `Enum` whose value is a number.
- **The leading `/` of an upload path no longer counts against its 1024 characters.**
  `path="/" + "a" * 1024` was refused as longer than 1024 characters,
  although the blob name, which storage limits to 1024 characters, is the
  path without its leading `/`.
- **The broker's `consumer_timeout` is named, and a handler timeout longer than its default is reported at start.**
  RabbitMQ takes back a command left unacknowledged longer than its
  `consumer_timeout` (30 minutes by default; a command is acknowledged
  when its handler has finished): it closes the service's channel and
  delivers the command again. The SDK reported this as a lost connection
  (`ACK not sent: the connection to the AMQP broker was lost ...`), and
  the client library logged an ERROR for each delivery in flight (`Got
  frame for closed channel ...`). Now one WARNING names the cause (`The
  AMQP broker closed the consumer channel: a command stayed
  unacknowledged longer than the broker's consumer_timeout ...`), a
  settlement that cannot be sent says `... not sent: the AMQP broker
  closed the channel of this command's delivery at its consumer_timeout
  ...` (field `reason`: `consumer_timeout`), and the client library's
  records are WARNINGs. With `KBM_LEDSAS_HANDLER_TIMEOUT=0` or above
  `1800`, one WARNING at start says to keep the broker's
  `consumer_timeout` above the longest handler run plus the time a
  command waits for a worker. **Action:** if a handler can run longer
  than 30 minutes, have the broker's `consumer_timeout` raised (see
  "Handler Timeout" in the API reference).
- **`KBM_LEDSAS_PREFETCH` below `KBM_LEDSAS_CONCURRENCY` is reported at start.**
  A command stays unacknowledged while its handler runs, so at most
  `min(KBM_LEDSAS_PREFETCH, KBM_LEDSAS_CONCURRENCY)` handlers run at
  once: `KBM_LEDSAS_PREFETCH=1` with `KBM_LEDSAS_CONCURRENCY=4` ran the
  handlers one at a time without a word. One WARNING at start now says
  so. The defaults (10 and 4) are unchanged.
- **A service with no handler registered says so at start.** It started
  silently and then replied `HandlerNotFound` to every command and
  dead-lettered it. Now one WARNING at start (`No command handler is
  registered: every command this service receives is replied
  HandlerNotFound and dead-lettered ...`) names the usual cause, a module
  with the `@app.handler` functions that was never imported. The
  service still starts.
- **`overwrite` must be `True` or `False`.** The `upload_*` methods read
  it by its truth value, so `overwrite="false"` (a flag read as text from
  the environment or a request field) replaced an existing blob. Now any
  other value than `True` or `False` raises `TypeError` (`overwrite must
  be True or False, got str`) before anything is written; uncaught, it is
  replied as `Permanent` (not retried). **Action:** convert such a flag
  to a bool first.
- **`upload_stream` refuses a chunk that is not bytes.** A `str` chunk
  was stored UTF-8 encoded without a word, and a chunk such as `None`, a
  number or a dict failed inside the storage client, logged as an ERROR
  `Streaming upload failed` with a traceback on every attempt, and was
  retried. Now a chunk other than `bytes`, `bytearray` or `memoryview`
  raises `TypeError` (`upload_stream() got a chunk of type str; the
  stream must yield bytes`) and the blob is not written; uncaught, it is
  replied as `Permanent` (not retried). An exception your stream raises
  itself is passed on unchanged and is no longer logged as a storage
  failure. **Action:** encode text chunks (`text.encode()`).
- **Secrets in handler exceptions are masked in the log.** An exception
  that quoted a connection URL or a connection string (often the message
  of a client library's error) was logged as raised by `Uncaught
  exception in handler` and the other records of the handler layer,
  traceback included. Every record the SDK writes now masks a URL's user
  name and password (a URL of any scheme), every query value and the
  fragment of a broker URL, the values of `AccountKey=` / `account_key=`,
  `SharedAccessSignature=`, `SharedAccessKey=` and `Password=` / `Pwd=`
  (quoted or not), a `Bearer` token, and the values of the `sig=`,
  `code=`, `access_token=` and `token=` URL query parameters: the
  records of every SDK module (the handler layer, the
  storage client's `Failed to download blob` ERROR and its traceback,
  ...) and of the AMQP client library that the SDK filters, and whatever
  the SDK's own log handler and its last-resort handler write, in the
  message, every text field, the traceback and the stack, in either log
  format. A text that cannot be masked is not written: the record carries
  `<withheld: secret masking failed>` (a field), `<record text withheld:
  secret masking failed>` (the message) or `<traceback withheld: secret
  masking failed>` instead, and is still written. An error reply's
  internal message (the exception's text, sent when no `user_message` is
  set and `KBM_LEDSAS_GENERIC_ERRORS` is off) is masked the same way; a
  `user_message` is sent as given. The records of your own loggers are
  not masked (`ctx.logger`'s are, see below), except the WARNING and
  higher records that the SDK's last-resort handler writes when your
  service configures no logging of its own.
- **A refused configuration keeps no connection setting in its error.**
  The settings error behind a `Configuration error` held the configuration
  it checked: an error about a connection value, or about the
  configuration as a whole, carried the broker URL or the storage
  connection string as its input in `errors()` / `json()` (what error
  reporters serialise), and its text (printed with
  `KBM_LEDSAS_DEBUG=1`) showed input values. That input is now
  `<not kept: the settings may hold connection secrets>`, the text shows
  no input value, and the exit after a configuration error or a failed
  start carries no exception context. The `Configuration error` line is
  unchanged.
- **Other code's log records follow `KBM_LEDSAS_LOG_FORMAT` when you configure no logging.**
  A service that configured no root logging wrote Python warnings and the
  event loop's records (`Task exception was never retrieved`, with a
  multi-line traceback) as plain text between the SDK's JSON lines. Now,
  when the root logger has no handler, `app.run()` writes the records that
  no handler takes in `KBM_LEDSAS_LOG_FORMAT` (it replaces Python's
  last-resort handler, `logging.lastResort`) and routes Python warnings
  through logging (`logging.captureWarnings(True)`). The same records come
  out as before (WARNING and above), each once: a logger you gave a
  handler of your own is written by that handler only. An application
  that configures root logging keeps its own setup (call
  `logging.captureWarnings(True)` yourself to have the warnings in it).
- **The handler layer's records carry the command's `message_id` and `command_name`.**
  `Permanent error in handler`, `Uncaught exception in handler`, `Handler
  timeout` and the other records of the handler layer carried the
  `correlation_id` and the `command_name`; they now carry the command's
  `message_id` too. `No handler for ...` and the SDK's retry, reply and
  dead-letter records (`Retry k/N, waiting ... before requeue`, `Reply
  publish failed for <name>`, `Skipping response for <name> (no
  reply_to)`, the `Command failed with a non-retryable error, sending to
  DLQ` INFO) carried only the `correlation_id`; they now carry all three,
  as the `Max retries (N) exceeded` ERROR did, so an alert can be joined
  to the dead-lettered message.
- **Every per-command record carries the command's `trace_id` and `idempotency_key`.**
  The SDK's records about a command (`Received command`, `Handling
  command <name>`, `Sent response for <name>`, `ACKed command <name>`,
  the handler layer's records, the retry, dead-letter, reply and
  status records, and the consumer's records about settling it:
  `Acknowledging message`, `Rejecting message`, the ACK / NACK failure
  and `... not sent` records, and the duplicate and pending-cap
  dead-letter WARNINGs, which carried only the `message_id`) carried no
  `trace_id`, so the text format's
  `[trace=...]` never appeared and a log search on a caller's trace found
  nothing. They now carry `trace_id` and `idempotency_key`, and `job_id`
  when the command has one, besides `correlation_id`, `message_id` and
  `command_name`; the text format shows `[trace=<first 16 characters of
  the trace_id>]` and `idempotency_key=` / `job_id=` pairs. These values
  are logged as sent: keep identifiable data out of them.
- **A broker URL's query string is masked in log records.** The broker
  URL the SDK logged (at DEBUG, `Connecting to RabbitMQ: ...`) had only
  its user name and password masked; its query string was shown as given
  (`?login=guest&password=...`). Every value of the query string and the
  fragment are now masked too (`amqp://127.0.0.1:5672/?login=***&password=***`),
  in the new `Connecting to the AMQP broker at ...` and `Could not connect
  to the AMQP broker at ...` lines and wherever a broker URL appears in a
  record the SDK masks.
- **`deployment_id`: a matched pair of surrounding quotes is removed.**
  The `deployment_id` field of `/health` and `/ready` is read from the
  same variables as in 0.3.3: `KBM_LEDSAS_DEPLOYMENT_ID`, or its older
  alias `KBM_LASTCOMMITID`, read only when `KBM_LEDSAS_DEPLOYMENT_ID` is
  unset or blank (prefer `KBM_LEDSAS_DEPLOYMENT_ID`). A matched pair of
  quotes around the value is now removed, as from every setting (the older
  alias `KBM_LASTCOMMITID="a1b2c3d4"` under `docker run --env-file`
  reported the quotes). The SDK sets neither; set one at deploy time, for
  example to the commit id.
- **A broker URL with `no_verify_ssl` is refused.** A
  `KBM_LEDSAS_RABBITMQ_URL` whose query sets `no_verify_ssl` to any value
  other than `0` (`1`, `true`, `false`, or no value at all: the AMQP client
  library keeps TLS certificate verification on only for
  `no_verify_ssl=0`) was accepted, so the service trusted any
  certificate. It now stops the start with one `Configuration error`
  line, unless `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` acknowledges it, which
  then logs one WARNING. **Action:** remove `no_verify_ssl`; to trust a
  private certificate authority, name its certificate with
  `?cafile=<path>` (`?certfile=` and `?keyfile=` give a client
  certificate). See "Transport security" in `SDK_API_REFERENCE.md`.
- **Storage reached over `http://` on another machine is reported.** A
  `KBM_LEDSAS_BLOB_CONN_STRING` that reaches storage over `http://` at a
  host other than this machine (`localhost`, `127.0.0.1`, `::1`) now logs
  one WARNING at start: its requests, signed with the account key or
  SAS, travel in cleartext. The service still starts.
- **`Envelope`, `Command`, `Response` and `Status` are frozen.** Their
  fields could be assigned (`ctx.envelope.reply_to = ...`,
  `envelope.job_id = ...`), which skipped the fields' checks. Assigning a
  field now raises `pydantic.ValidationError`, and `ctx.envelope` is
  read-only. **Action:** make a changed copy with
  `model_copy(update={...})` instead.
- **New `KBM_LEDSAS_HEARTBEAT` (default 60 s, as before).** The AMQP
  heartbeat interval the SDK asks the broker for is now a setting (`0` to
  `600`; `0` turns heartbeats off, and with it the detection of a broker
  that stops answering). The default `60` is the interval earlier releases
  already used (the AMQP client library's default), now set by the SDK
  itself. A broker or a network path that stops answering without closing
  the connection is detected after about three intervals (`3 × (interval +
  1)` seconds, 183 s at the default, plus up to half an interval), and the
  SDK reconnects. A handler that blocks the event loop longer than about
  two heartbeat intervals makes the broker close the connection: run
  blocking work with `await asyncio.to_thread(...)`; a lower value finds a
  dead broker sooner but leaves handlers less room to block. A
  `?heartbeat=` in `KBM_LEDSAS_RABBITMQ_URL` still wins; its value must now
  be a whole number from `0` to `600`: any other value (`abc`, `-5`,
  `99999`) is refused at start with a `Configuration error`. Before, the
  AMQP client library silently turned heartbeats off for such a value.
- **A command delivered again after a lost broker connection counts toward
  `KBM_LEDSAS_MAX_RETRIES`.** Each delivery of a command that the broker
  makes again after the connection was lost (for example because its
  handler blocked the event loop past the heartbeat) counts as one
  attempt. Once the attempts are spent the SDK logs the `Max retries (N)
  exceeded, sending to DLQ` ERROR (saying that the command was delivered
  again after attempts that did not finish because the connection was
  lost), answers the caller once with the final error and dead-letters
  the command. Before, such a command could
  run again after every reconnect without end, with no reply and nothing
  in the DLQ. The count is kept in the service process (a restart starts
  it again).
- **A message whose AMQP properties are not UTF-8 is dead-lettered.** A
  message whose AMQP short-string property (`message_id`,
  `correlation_id`, `reply_to`, `type`, ...) or header name held bytes
  that are not valid UTF-8 made the AMQP client library drop the
  connection; the broker delivered the same message again on the next
  connection, so the service processed nothing else. It is now
  dead-lettered once, with one WARNING (`AMQP message property
  message_id is not valid UTF-8; dead-lettering`; the record's
  `amqp_message_id` shows such bytes as `\xNN` escapes); consumption
  goes on and the connection stays up. To decode such a message, the
  service replaces the two short-string decoders of the AMQP frame
  decoder (`pamqp`) for the whole process when it starts (importing
  `kbm_ledsas_sdk` alone changes nothing): only a value that fails strict
  UTF-8 decoding is decoded differently (with `surrogateescape`); valid
  UTF-8 decodes exactly as before. Your own `aio-pika` code in the same
  process then receives such a value as a `str` with lone surrogates
  instead of losing its connection.
- **`ctx.logger`'s records are masked.** `ctx.logger` is still the
  stdlib logger `handler.<command name>`, whose records go to your root
  logging configuration; the SDK now attaches its secret masking to it,
  so its records are masked like the SDK's own (see "Secrets in handler
  exceptions are masked in the log").
- **Version fields take ASCII digits only.** `schema_version` and
  `message_version` are `<major>.<minor>` with 1 to 4 ASCII digits
  (`0`-`9`) each, at most 9 characters; other Unicode digits (`١.٠`) and
  longer numbers used to be accepted. The `version` of `@app.handler`
  follows the same rule (`ValueError` at registration).
- **Handler and health-check registration checks the function.**
  `@app.handler` raises `TypeError` for something that is not callable
  or a function that cannot be called with the two arguments
  (`ctx`, `req`); `@app.liveness_check` / `@app.readiness_check` raise
  `TypeError` for something that is not callable or a function that
  cannot be called without arguments. Before, such a handler failed on
  every command and such a check on every probe.
- **`BlobRef` refuses blob names over 1024 characters,** the storage
  limit (a longer name used to be accepted and failed at the storage
  service), and its error names a lone surrogate in the URI even when a
  control character comes before it.
- **An `AccountKey` placeholder or an inline comment stops the start.**
  A `KBM_LEDSAS_BLOB_CONN_STRING` whose `AccountKey` decodes to nothing
  (a placeholder such as `...`), or whose `AccountKey` or `BlobEndpoint`
  holds whitespace (an inline `# comment` that `docker run --env-file`
  passes through), is refused at start with one `Configuration error`
  line; before, every storage request failed.
- **`BlobOperations` can be imported from `kbm_ledsas_sdk`.** `from
  kbm_ledsas_sdk import BlobOperations` gives the type of `ctx.blob`,
  for type hints; it used to be reachable only through an internal
  module.
- **More fields in the SDK's records.** The retry record of a
  `Retryable` failure carries `will_retry` and `attempt`. The start
  record (`Starting <service> service`) carries `sdk_version` and, when
  set, `deployment_id`. An `upload_json` call whose object has no JSON
  form is logged once (the handler layer's one ERROR). A stop signal
  that arrives before the connection is started logs `Stopped before the
  service started ...` and exits with code 0. Missing connection
  settings (`KBM_LEDSAS_RABBITMQ_URL`, `KBM_LEDSAS_BLOB_CONN_STRING`) are
  reported together in one `Configuration error`.

- **New `KBM_LEDSAS_ALLOWED_CONTAINERS`: a container allowlist for
  `ctx.blob`.** A comma-separated list of container names
  (`input,results`). When it is set, every upload (by its `container`)
  and every download (by the container of its URI) outside the list
  raises the new `errors.ContainerNotAllowed` (a `PermissionError`, with
  the refused name in `.container`) before anything is sent to storage,
  logs one WARNING `Blob upload refused: container 'other' is not in
  KBM_LEDSAS_ALLOWED_CONTAINERS` (or `Blob download refused: ...`) and,
  uncaught, is replied once as `Permanent` (`container 'other' is not
  allowed (KBM_LEDSAS_ALLOWED_CONTAINERS)`), not retried. An empty item
  or an invalid container name stops the start with one `Configuration
  error`; when set, the start logs one INFO `Blob access limited to the
  containers in KBM_LEDSAS_ALLOWED_CONTAINERS: ...`. Unset (the default),
  nothing changes. It limits what a command can make the service do with
  its storage credential; see "Security model" in the API reference.
- **`download_json` and `upload_json` keep to a dict or a list.**
  `download_json` of valid JSON whose root is a number, a string,
  `true`/`false` or `null` now raises a `ValueError` (`Blob <uri> holds
  JSON whose root is <kind>: download_json() returns a dict or a list;
  ...`) and logs one ERROR `Blob content is JSON but its root is <kind>,
  not an object or an array`; `upload_json` of anything but a dict or a
  list (tuples included) raises `TypeError` (`upload_json() takes a dict
  or a list (the root of the JSON document), got <type>; ...`) before
  anything is sent. Both are not retried (`Permanent` when uncaught).
  Before, `download_json` returned the scalar and `upload_json` uploaded
  it. **Action:** read such content with `download_text()` (then
  `json.loads`) and write it with `upload_text()`.
- **`Command`, `Response` and `Status` check their envelope's `type`.**
  Each refuses an envelope of another kind (`pydantic.ValidationError`:
  `envelope.type is 'command', not 'response' (a Response carries a
  response envelope)`). **Action:** to build a `Response` from a
  command's envelope, copy it with `model_copy(update={"type":
  "response"})`. A response or status that reaches a command queue is
  still dead-lettered with its one WARNING.
- **A broker-cancelled command consumer is noticed.** When the broker
  cancels the service's consumer while the connection stays up (its
  queue was deleted, or the node that holds it is down), the SDK logs one
  ERROR `The AMQP broker cancelled the command consumer on <queue> ...`,
  `/ready` answers 503, and the SDK declares the queue and consumes again
  until it succeeds (a WARNING `Could not consume from <queue> again
  (...); retrying in N s` per failed try, an INFO `Consuming commands
  from <queue> again` at the end). Before, the service consumed nothing,
  `/ready` stayed 200 and the SDK logged nothing.
- **The configured secrets are masked everywhere.** The broker password
  (also percent-encoded) and the storage `AccountKey` and SAS signature
  the service is configured with are masked in every record and reply
  text of the process, in any form, not only where a pattern recognises
  them; a dict, list, tuple or set in a record field is masked item by
  item. `repr()` of the broker URL and connection string the SDK holds
  (and so of the configuration and transport objects, in debuggers and
  `rich` views) shows `'**redacted**'` for `rabbitmq_url` and
  `blob_conn_string`; `str()` gives the value.
- **More precise start errors.** Refused credentials:
  `AMQP authentication refused by the broker at amqp://***:***@host:port/:
  check the user name and password in KBM_LEDSAS_RABBITMQ_URL and the
  user's access to the virtual host. Broker reply: ...`. A broker host
  name that does not resolve: `... the host name '<host>' does not
  resolve (...); check the host in KBM_LEDSAS_RABBITMQ_URL and the DNS of
  the network the service runs in`. An empty numeric setting:
  `KBM_LEDSAS_PREFETCH='' is empty: unset it to use the default (10) or
  give a whole number`; an empty log setting: `Invalid log level: ''
  (empty: unset the variable to use the default)`. A
  `KBM_LEDSAS_RABBITMQ_URL` that still starts or ends with a quote (an
  unmatched quote) is refused. A missing reply exchange's ERROR now ends
  `verify the caller declared this exchange and bound a queue to it for
  routing key 'response'.`. A heartbeat timeout logs `Lost the
  connection to the AMQP broker (no frames from the broker within the
  heartbeat timeout, about N s) ...` (`error_class` `HeartbeatTimeout`),
  and a reply that close cancelled logs `Reply to exchange '<name>' not
  sent: ... (ConnectionLost)`.
- **Errors and registration.** Every `errors.SDKError` carries
  `user_message` as an attribute; a bare `errors.DeadlineExceeded()` has
  the message `Processing deadline exceeded`. A wrong-typed argument to
  `@app.handler` or a health-check decorator (a name that is not a
  string, a timeout that is not a number) raises a `TypeError` that is
  also a `ValueError` (an existing `except ValueError` still catches it),
  and a handler with the wrong number of arguments is named with its
  arity (`... the SDK calls it with (ctx, payload), but it takes 1
  positional argument.`).
- **Health-server hardening gaps are reported.** With an aiohttp version
  whose internals the health server cannot harden, the endpoints still
  answer and the SDK logs one ERROR per gap, `Health server: <what>
  cannot be applied with the installed aiohttp <version>; ...`.
- **`aio-pika` 10.x is now accepted** (`aio-pika>=9.4.0,<11.0`; was
  `<10.0`). A fresh install resolves aio-pika 10 (with aiormq 7 and
  pamqp 4). The SDK's message handling, reply publishing and startup /
  authentication error handling behave the same on 9.x and 10.x. If you
  pin aio-pika 9.x yourself, nothing changes.
- **`aiormq` and `pamqp` are declared** (`aiormq>=6.8.0,<8.0`,
  `pamqp>=3.3.0,<5.0`): the SDK imports them directly; they already
  arrived through `aio-pika`. If the installed `pamqp` has a layout the
  SDK cannot adapt, the start logs a WARNING (a message property that is
  not UTF-8 then drops the connection instead of dead-lettering that one
  message).
- `pydantic` and `azure-storage-blob` ranges are unchanged.

### Fixed

- **SIGTERM during a broker outage no longer hangs until the platform
  kills the process.** Previously, stopping a service while RabbitMQ was
  unreachable or not answering could wait indefinitely: at `Stopping
  AMQP consumer`, or, when a command was in flight, on that command's
  reply. From the moment shutdown begins, every broker operation the SDK
  still makes is bounded (5 s per step): reply and status publishes,
  ACK / requeue / dead-letter, consumer cancel and connection close. Once
  the broker is found unreachable, the remaining round trips are skipped
  and the connection is closed locally. A single WARNING (`AMQP broker
  not reachable during shutdown ...`, naming the cause) replaces the
  hang. It counts the unacknowledged messages this consumer held, the
  commands whose reply or acknowledgement could not be confirmed, and the
  acknowledgements, requeues and dead-letters sent after the stop signal:
  the broker does not answer these, so one sent to a broker that had
  stopped answering (frozen, not disconnected) is written and logged
  (`ACKed command ...`) but may never be read, and the WARNING then says
  `N acknowledgement(s), requeue(s) or dead-letter(s) were sent after the
  stop signal; the broker does not answer them, so they may not have
  reached it, and those commands may be delivered and run again.` A command whose reply could not be confirmed, or that could not
  be acknowledged, requeued or dead-lettered at that point, is left
  unacknowledged for the broker to redeliver, instead of being
  dead-lettered. That command runs again on redelivery. A broker that was
  only stalled may still deliver the unconfirmed reply late, so the
  caller can receive it twice (at-least-once). Handler run time is not
  bounded, and shutdown with a healthy broker is unchanged.
- **A retry backoff no longer delays shutdown.** A command that failed
  with a retryable error waits its backoff (up to 60 s) before being
  requeued. Once shutdown is requested, it is now requeued immediately
  instead of holding the shutdown for the rest of the backoff.
- **Handlers that can no longer reply are cancelled during shutdown.**
  When the broker is found unreachable while the service stops, the
  handlers still running are cancelled, as by a second stop signal (their
  reply and acknowledgement could not be sent; the broker delivers their
  commands again), with one INFO line naming them, instead of running on
  until they end or the process is killed. This includes a stop signal
  that arrives while the connection is already lost (the client still
  reconnecting): the handlers are cancelled at once, instead of only
  after a broker operation failed during the drain, which a long handler
  delayed until it ended.
- **README.** The quickstart takes the output container from
  `BlobRef.from_uri(uri).container` instead of splitting the URI string by
  hand, writes its own log records in the
  SDK's JSON format, and the configuration table lists both variables that
  set `deployment_id` (`KBM_LEDSAS_DEPLOYMENT_ID`, and its older alias
  `KBM_LASTCOMMITID`, read only when the first is unset or blank; the table
  named only the first) and
  `KBM_LEDSAS_CONNECT_TIMEOUT`, `KBM_LEDSAS_GENERIC_ERRORS` and
  `KBM_LEDSAS_DEBUG`. The "Local development" commands load the example's
  `.env` and say they run from a clone of this repository.
- **`KBM_LEDSAS_CONTAINER` is informational.** The 0.3.3 README called it
  the default blob container, but it never selected a container: the SDK
  logs it at startup, and every upload method takes an explicit
  `container`. The README and the API reference now say so; nothing changes
  in what the SDK does.
- **Example service.** `examples/hello_world_service` builds `FROM` the LEDSAS
  base image (one stage; the code stays read-only for the non-root service
  user; pip is removed at the end of the build), no longer lists the SDK in
  its `requirements.txt`, and logs one JSON stream. Its `Dockerfile` ends
  with the numeric `USER 1000:1000` (the base image's `kbmuser`; in 0.3.3
  it ended with the named `USER appuser`), so a Kubernetes pod with `runAsNonRoot: true` and
  no `runAsUser` is admitted: the kubelet cannot verify that a named user
  is not root. End a `Dockerfile` of your own the same way, or set
  `runAsUser: 1000` in the pod.
- **API reference** (`docs/SDK_API_REFERENCE.md`) brought up to date with this
  release, including how a blob URI with `?versionId=` is parsed
  (`BlobRef.version_id`).

### Known limitations

- **Two kinds of message body still depend on the Python version's own
  JSON parser.** The 256-level nesting limit is checked on the parsed
  body, so it cannot apply to a body the parser gives up on first, and
  the parser gives up at about 1000 levels on Python 3.11 and goes far
  deeper on 3.13. When a key appears twice, only the kept (last) value
  counts toward the limit, but a discarded value nested past the
  parser's own limit still makes it give up, so such a body can be
  dead-lettered on Python 3.11 and processed on 3.13: send standard
  JSON, without repeated keys. A body nested past the parser's own
  limit that is also not valid JSON is dead-lettered on every version,
  but may be logged as either error.

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
