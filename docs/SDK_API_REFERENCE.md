# LEDSAS SDK API Reference v0.3.3

Complete reference for all SDK APIs implemented in kbm-ledsas-sdk v0.3.3.

> **Note:** The SDK connects directly to RabbitMQ and Azure Blob Storage; you
> configure it through the connection environment variables documented below.

---

## Public vs Internal modules

The following are part of the **public, supported API**:

- `from kbm_ledsas_sdk import ServiceApp, ExecutionContext, errors`
- `from kbm_ledsas_sdk import json_log_formatter` (logging helper — see "Logging note")
- `from kbm_ledsas_sdk.models import BlobRef, Command, Response, Status, Envelope`
  (`Command` / `Response` / `Status` / `Envelope` are read-only message
  models — stable; use them to inspect or build wire messages)
- Every class, method, env var, and behaviour documented in this file.

Anything reached through a deeper import path (`kbm_ledsas_sdk.transport.*`,
`kbm_ledsas_sdk.amqp.*`, `kbm_ledsas_sdk.runtime.*`, `kbm_ledsas_sdk.blob.*`,
`kbm_ledsas_sdk.health.*`, `kbm_ledsas_sdk.utils.*`) is **internal**.
Internal modules may change layout, signatures, or be removed entirely
in any point release with no deprecation period. Reach in only for
test fixtures (use the public API in production code).

---

## Environment Variables

Configure SDK behavior via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_SERVICE_NAME` | Required (env or constructor) | Service name for queue binding. Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. When set, takes precedence over `ServiceApp(service_name=...)`. At least one of the two must be provided. |
| `KBM_LEDSAS_RABBITMQ_URL` | Required | RabbitMQ connection URL |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Required | Azure Blob connection string (required even if your handlers never call `ctx.blob` — the blob client initializes at startup) |
| `KBM_LEDSAS_CONTAINER` | `dev` | Default blob container |
| `KBM_LEDSAS_PREFETCH` | `10` | AMQP message prefetch count |
| `KBM_LEDSAS_CONCURRENCY` | `4` | Max concurrent handlers |
| `KBM_LEDSAS_LOG_LEVEL` | `INFO` | Logging level. Applied to the `kbm_ledsas_sdk` logger at startup. |
| `KBM_LEDSAS_LOG_FORMAT` | `json` | `json` (machine-parseable) or `text` (colored human-readable). Applied at startup. |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | `1800` | Handler timeout in seconds (`0` disables; max `86400` = 1 day) |
| `KBM_LEDSAS_MAX_RETRIES` | `3` | Maximum number of retries (NOT counting the initial attempt) for `Retryable` errors. Default `3` means a handler that always raises `Retryable` runs **4 times total** (1 initial + 3 retries) before its message goes to DLQ. |
| `KBM_LEDSAS_GENERIC_ERRORS` | `false` | Fallback to generic message when handler did not set `user_message` |
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | HTTP port for `/health` and `/ready` (`0` disables) |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Bind address for the health server (loopback only by default) |
| `KBM_LEDSAS_HEALTH_VERBOSE` | `false` | If `true`, health responses include SDK version + service name + per-check status (fingerprintable; opt-in for dev). Default minimal `{"status": ...}`. |
| `KBM_LEDSAS_MAX_PAYLOAD_BYTES` | `16777216` (16 MiB) | Reject inbound AMQP messages whose body exceeds this size (single WARNING + DLQ). `0` disables. Max `268435456` (256 MiB). Large payloads belong in blob storage. **Note:** the effective inbound limit is `min(broker max_message_size, this value)`. RabbitMQ 4.x defaults `max_message_size` to 16 MiB, so a message over the broker cap is refused at *publish* time with a channel-closing `PRECONDITION_FAILED` and never reaches this SDK gate. To exercise the SDK gate (single WARNING + DLQ) the broker cap must be raised above this value. |
| `KBM_LEDSAS_ALLOW_INSECURE_AMQP` | `0` | If `1`, downgrade the SDK's refusal of `amqp://` to non-loopback hosts to a WARNING. Escape hatch for TLS-terminated-upstream deployments. |
| `KBM_LEDSAS_TENANT` | _(unset)_ | Optional tenant identifier for multi-tenant AMQP topology naming. Validated `^[A-Za-z0-9][A-Za-z0-9._\-]*$`, 1..64 chars. See "AMQP Topology" below for how this affects exchange and queue names. |
| `KBM_LEDSAS_DEBUG` | `0` | If `1`, dump the full Python traceback on configuration-error startup failures instead of the one-line message. |
| `KBM_LEDSAS_DEPLOYMENT_ID` | _(unset)_ | Optional build/commit identifier. When set, its value is surfaced verbatim as the `deployment_id` field in `/health` and `/ready` responses, so you can confirm which build is live behind a probe. Leave unset locally (the field is then omitted). See "Health Endpoints" below. |
| `KBM_LASTCOMMITID` | _(platform-injected)_ | Fallback source for `deployment_id` when `KBM_LEDSAS_DEPLOYMENT_ID` is unset. Usually injected by the deployment platform. See "Health Endpoints" below. |

### Handler Timeout (`KBM_LEDSAS_HANDLER_TIMEOUT`)

- Default: 1800 seconds (30 minutes)
- Set to `0` to disable timeout enforcement
- Handlers exceeding this duration are automatically cancelled

### Max Retries (`KBM_LEDSAS_MAX_RETRIES`)

- Default: 3 retries (NOT counting the initial attempt). A handler
  that always raises `Retryable` runs **4 times total** (1 initial +
  3 retries) before its message goes to DLQ.
- Applies to `Retryable` errors only.
- After the configured number of retries is exhausted, the message
  goes to the Dead Letter Queue.

### Generic Errors (`KBM_LEDSAS_GENERIC_ERRORS`)

- Default: `false` — caller sees the handler's internal `message`
- When `true` — caller sees a generic fallback (e.g. `"Processing failed"`)
- `user_message` **always wins** when set, regardless of this flag

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
    "deadline": "2026-05-26T10:35:00Z",
    "priority": 5,
    "job_id": "batch-2026-05-26-001"
  },
  "payload": {
    "your_field": "your_data",
    "another_field": 123
  }
}
```

### Envelope Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema_version` | string | Yes | Schema version (use "1.0") |
| `type` | string | Yes | Message type: `"command"`, `"response"`, or `"status"` (see "Type values" note below) |
| `name` | string | Yes | Command name (e.g., "ProcessData") |
| `message_version` | string | Yes | Message schema version (e.g., "1.0") |
| `message_id` | string | Yes | Unique message identifier (UUID, case-insensitive — normalized to lowercase internally) |
| `correlation_id` | string | Yes | Correlation ID for tracking related messages (UUID, case-insensitive) |
| `idempotency_key` | string | Yes | Key for idempotent processing |
| `sent_at` | string | Yes | ISO8601 timestamp when message was sent |
| `trace_id` | string | Yes | Distributed tracing ID |
| `reply_to` | string | No | Name of a **pre-declared AMQP exchange** to publish the response to. URL-safe: `^([A-Za-z0-9_\-:.]+)?$`, max 127 chars (matches AMQP's protocol-level cap on exchange-name length and the shape of `trace_id` / `idempotency_key` / `job_id`). SDK publishes with routing key `response` (or `status` for `emit_status`). The SDK does not declare this exchange — the caller owns it. **Contract: the exchange must exist AND have at least one queue bound** for the `response` routing key; the SDK publishes replies as mandatory, so a missing exchange *or* an unroutable response (no bound queue) fails the reply and dead-letters the command. Leave empty for fire-and-forget. See `examples/hello_world_service/scripts/send_hello.py` for a runnable consumer example. |
| `deadline` | string | No | ISO8601 deadline for processing |
| `priority` | int | No | Message priority (higher = more urgent) |
| `job_id` | string | No | Business-level job identifier |

> **Type values:** the SDK accepts and emits exactly `"command"`,
> `"response"`, and `"status"`. Handler-raised errors come back as a
> `"response"` envelope whose `payload` contains an `error` object
> (`{code, message, retryable}`).

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

**Signature:** `@app.handler(command_name: str, version: str = "1.0")`. The
optional `version` is recorded in the registration log line
(`Registered handler for ProcessData v1.0`); leave it at the default
unless you are running side-by-side handler versions.

### AMQP Topology

The SDK automatically creates this topology based on `service_name`:

| Component | Name Pattern | Type | Description |
|-----------|--------------|------|-------------|
| Command Exchange | `cmd.[{tenant}.]{service_name}.v1` | TOPIC | Receives incoming commands |
| Command Queue | `queue.[{tenant}.]{service_name}.v1` | - | Service consumes from here |
| DLQ Exchange | `dlq.[{tenant}.]{service_name}.v1` | TOPIC | Failed message routing |
| DLQ Queue | `dlq.queue.[{tenant}.]{service_name}.v1` | - | Stores failed messages |

**Multi-tenant note:** when `KBM_LEDSAS_TENANT` is set, the tenant is
inserted *after* the prefix — `cmd.{tenant}.{service_name}.v1` — not at
the very start of the name. Adding or changing the tenant after
deployment requires re-declaring topology (the broker refuses to
redeclare an existing queue with a different name) and re-issuing
publisher-side ACLs.

**Example:** For `ServiceApp(service_name="csv-processor")`:
- Command exchange: `cmd.csv-processor.v1` (or `cmd.acme.csv-processor.v1` with `KBM_LEDSAS_TENANT=acme`)
- Command queue: `queue.csv-processor.v1` (or `queue.acme.csv-processor.v1`)

### Sending Messages to a Service

To send messages to a LEDSAS service, publish to its command exchange:

```python
import pika
import json
import uuid
from datetime import datetime, timezone

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.URLParameters("amqp://guest:guest@127.0.0.1:5672/"))
channel = connection.channel()

# Build message
message = {
    "envelope": {
        "schema_version": "1.0",
        "type": "command",
        "name": "ProcessData",  # Must match @app.handler("ProcessData")
        "message_version": "1.0",
        "message_id": str(uuid.uuid4()),
        "correlation_id": str(uuid.uuid4()),
        "idempotency_key": str(uuid.uuid4()),
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "trace_id": str(uuid.uuid4())
    },
    "payload": {
        "input_uri": "azblob://data/input.csv"
    }
}

# Publish to service's command exchange
channel.basic_publish(
    exchange="cmd.csv-processor.v1",  # cmd.{service_name}.v1
    routing_key="command",             # any non-empty key matches the queue's `#` bind
    body=json.dumps(message),
    properties=pika.BasicProperties(content_type="application/json")
)

connection.close()
```

**Key Points:**
- Exchange: `cmd.{service_name}.v1` (TOPIC type)
- Routing key: any non-empty string — the queue binds with pattern `#`,
  so every routing key matches. Use the literal `"command"` for
  consistency with the examples.
- `envelope.name` MUST match the handler's command name (e.g.,
  `@app.handler("ProcessData")`).

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
| `ctx.deadline` | `datetime \| None` | `ctx.envelope.deadline` | Handler deadline (UTC) |
| `ctx.job_id` | `str \| None` | `ctx.envelope.job_id` | Business-level job id |
| `ctx.envelope` | `Envelope` | — | Full envelope object |
| `ctx.payload` | `dict` | — | Same dict the handler receives as `req` |
| `ctx.blob` | `BlobOperations` | — | See "BlobOperations" below |
| `ctx.logger` | `logging.Logger` | — | See **Logging** note |

#### Logging note

`ctx.logger` is a vanilla stdlib `logging.Logger`
(`handler.<envelope.name>`). Structured fields go in `extra={...}`,
**not as keyword arguments**:

```python
# ✓ correct
ctx.logger.info("Processing", extra={"correlation_id": ctx.correlation_id})

# ✗ silently dropped (kwargs are not part of stdlib logger's contract)
ctx.logger.info("Processing", correlation_id=ctx.correlation_id)
```

**Payload-field safety:** payload fields are caller-controlled and
may contain ANSI escape sequences, embedded newlines, or other control
characters. Always pass them via `extra={...}` rather than interpolating
into the message string — the JSON log formatter escapes control
characters in `extra` values, but a string-interpolated message can
let a hostile caller manipulate an operator's terminal (color/cursor
moves, fake log lines, bell spam):

```python
# ✓ correct — JSON formatter escapes \x1b etc. in extra values
ctx.logger.info("Processing request",
                extra={"correlation_id": ctx.correlation_id,
                       "input_uri": req.get("input_uri")})

# ✗ wrong — payload field interpolated into message string
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

#### Examples — `message_id` vs `idempotency_key` for output paths

Use `ctx.idempotency_key` (not `ctx.message_id`) when computing output
blob paths or any other write-once key: idempotency_key is stable
across DLQ replays of the same logical request, message_id is a fresh
UUID on every send.

```python
# Stable across retries / DLQ replays of the same logical request.
ref = await ctx.blob.upload_json(
    container="output",
    obj=result,
    path=f"result-{ctx.idempotency_key}.json",
)
```

#### `deadline` example

```python
from datetime import datetime, timezone, timedelta

if ctx.deadline and datetime.now(timezone.utc) > ctx.deadline - timedelta(seconds=5):
    raise errors.DeadlineExceeded("Not enough time to process")
```

#### `job_id`

Business-level identifier set by the orchestrator. Unlike
`correlation_id` (SDK-controlled), `job_id` is set by the caller and
echoed back in responses, status updates, and errors. Use for
batch/workflow tracking. Optional and backwards-compatible — handlers
that ignore it keep working.

```python
@app.handler("ProcessBatch")
async def handle(ctx, req: dict) -> dict:
    if ctx.job_id:
        ctx.logger.info("Processing job", extra={"job_id": ctx.job_id})
    return {"status": "success", "job_id": ctx.job_id}
```

### Methods

#### `emit_status(stage: str, progress: float, note: str | None = None) -> None`

Report processing progress back to the orchestrator.

**Parameters:**
- `stage` (str): Current processing stage (e.g., "downloading", "processing", "uploading")
- `progress` (float): Completion fraction in `[0.0, 1.0]`
- `note` (str, optional): Human-readable status message

**Raises:**
- `ValueError`: if `progress` is outside `[0.0, 1.0]` — validated at
  call time, before any I/O.

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
ctx.envelope.sent_at        # When message was sent
ctx.envelope.trace_id       # Distributed tracing ID
```

---

## BlobOperations (8 APIs)

Access via `ctx.blob` for all blob storage operations.

### Bytes Operations (2)

#### `upload_bytes(container: str, data: bytes, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload raw bytes to blob storage.

**Parameters:**
- `container` (str): Target container name
- `data` (bytes): Raw bytes to upload
- `path` (str, optional): Blob path within container. If omitted, the
  SDK auto-generates a UUID path.
- `overwrite` (bool, default `False`): If `True`, replace an existing
  blob at `path`. Default `False` refuses to clobber existing blobs
  (raises `azure.core.exceptions.ResourceExistsError`). Set `True` for
  the idempotency-key replay pattern — see `examples/hello_world_service/`.

**Returns:** `BlobRef` (carries the blob `uri`; `version_id` is populated
when the container has versioning enabled)

**Raises:**
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

**Example:**
```python
data = b"Hello, World!"
ref = await ctx.blob.upload_bytes(
    container="results",
    data=data,
    path=f"messages/{ctx.idempotency_key}.bin"
)
print(ref.uri)  # "azblob://results/messages/abc-123.bin"
```

#### `download_bytes(blob_ref: str | BlobRef) -> bytes`
Download blob as raw bytes.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef object

**Returns:** bytes

**Example:**
```python
data = await ctx.blob.download_bytes("azblob://input/file.bin")
print(len(data))  # Size in bytes
```

### JSON Operations (2)

#### `upload_json(container: str, obj: dict, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload Python dict/list as JSON blob.

**Parameters:**
- `container` (str): Target container
- `obj` (dict | list): JSON-serializable Python object
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
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

#### `download_json(blob_ref: str | BlobRef) -> dict | list`
Download and parse JSON blob.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef

**Returns:** Parsed JSON object (dict or list)

**Example:**
```python
config = await ctx.blob.download_json("azblob://config/settings.json")
print(config["max_retries"])  # 3
```

### Text Operations (2)

#### `upload_text(container: str, text: str, path: str | None = None, overwrite: bool = False) -> BlobRef`
Upload text string to blob storage.

**Parameters:**
- `container` (str): Target container
- `text` (str): Text content to upload (must be a string)
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
- `TypeError`: If `text` is not a string (e.g., passing a list or dict).
  Use `upload_json()` for dict/list data.
- `azure.core.exceptions.ResourceExistsError`: If `overwrite=False` and the blob already exists.

**Example:**
```python
report = "Processing Summary\n" + "=" * 50 + "\n"
report += f"Rows: 1000\nErrors: 0\n"
ref = await ctx.blob.upload_text(
    container="reports",
    text=report,
    path=f"summaries/{ctx.idempotency_key}.txt"
)
```

#### `download_text(blob_ref: str | BlobRef) -> str`
Download blob as text string.

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef

**Returns:** Text string (decoded as UTF-8)

**Example:**
```python
content = await ctx.blob.download_text("azblob://logs/app.log")
for line in content.split("\n"):
    if "ERROR" in line:
        print(line)
```

### Streaming Operations (2)

#### `upload_stream(container: str, stream: AsyncIterator[bytes], path: str | None = None, progress_callback: Callable[[int], None] | None = None, overwrite: bool = False) -> BlobRef`
Upload data from async byte stream (for large files).

**Parameters:**
- `container` (str): Target container
- `stream` (AsyncIterator[bytes]): Async generator yielding chunks
- `path` (str, optional): Blob path. If omitted, the SDK auto-generates a UUID path.
- `progress_callback` (callable, optional): Called with the cumulative
  number of bytes uploaded so far after each chunk.
- `overwrite` (bool, default `False`): See `upload_bytes`.

**Returns:** `BlobRef`

**Raises:**
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
Download blob as async byte stream (for large files).

**Parameters:**
- `blob_ref`: Blob URI string or BlobRef
- `chunk_size` (int, default `4194304` = 4 MiB): Size of each yielded chunk.
- `progress_callback` (callable, optional): Called with
  `(bytes_downloaded_so_far, total_bytes)` after each chunk.

**Returns:** AsyncIterator[bytes] yielding chunks

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

The SDK ships three concrete error types — `Retryable`, `Permanent`,
and `DeadlineExceeded` — plus a shared base class `errors.SDKError`.
Catch the base class when you want one branch for "any SDK-raised
error" (shared metric, common cleanup); catch a concrete subclass
when retry-semantics differ.

```python
from kbm_ledsas_sdk import errors

try:
    ...
except errors.SDKError as e:
    # Any of Retryable / Permanent / DeadlineExceeded
    metrics.incr("handler.sdk_error", tags={"type": type(e).__name__})
    raise
```

> **Backend outages and concurrency.** A blob operation against an
> unreachable Azure endpoint does not fail instantly — the Azure Storage
> SDK runs its own internal connection-retry policy first (tens of
> seconds), and the handler recovers automatically once the backend is
> back, with no DLQ churn or message loss. This is the right behavior for
> a transient blip. A *sustained* outage, however, holds the in-flight
> handler on that internal retry, occupying one of your
> `KBM_LEDSAS_CONCURRENCY` worker slots until it returns; a long outage
> can therefore stall throughput. If you prefer to shed stuck work to a
> retry/DLQ during long outages, set a shorter `KBM_LEDSAS_HANDLER_TIMEOUT`
> so a blocked handler raises `DeadlineExceeded` and frees its slot.

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

**Example:**
```python
from kbm_ledsas_sdk import errors

try:
    data = await external_api_call()
except TimeoutError:
    # Basic usage
    raise errors.Retryable("API timeout - will retry")

    # With custom user message
    raise errors.Retryable(
        "Internal: connection pool exhausted after 30s",
        user_message="Service temporarily unavailable, please wait",
    )
```

**SDK Behavior:**
- Automatic exponential backoff: `min(2^retry_count + jitter, 60.0)` seconds (jitter ∈ [0, 1))
- Configurable via `KBM_LEDSAS_MAX_RETRIES` — number of retries NOT
  counting the initial attempt. Default `3` → 4 total attempts.
- After max retries, message goes to DLQ

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

### `errors.DeadlineExceeded`
Raised when processing cannot complete within deadline.

**Use Cases:**
- Not enough time remaining to start safely
- Long-running operations that can't finish
- Resource contention delays

**Constructor:**
```python
errors.DeadlineExceeded(message: str = "", user_message: str | None = None)
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
            f"Only {time_remaining.seconds}s remaining - need at least 10s",
            user_message="Request arrived too close to deadline",
        )
```

**SDK Behavior:**
- **Not retried** — the deadline is already in the past, so a retry
  would just exceed it again. Message goes to DLQ. The orchestrator
  is expected to resend with a fresh deadline if appropriate.
- Logged at WARNING (not ERROR — the SDK has nothing to do here).
- Handler is cancelled if the deadline is exceeded during execution
  (`asyncio.wait_for` raises `asyncio.TimeoutError`, which the SDK
  classifies as `Timeout` — retryable; distinct from
  `DeadlineExceeded`, which is for handler-raised deadline checks).

---

## Timeout Enforcement

The SDK enforces handler execution timeouts to prevent runaway processing.

### Configuration

```bash
export KBM_LEDSAS_HANDLER_TIMEOUT=1800  # 30 minutes (default)
export KBM_LEDSAS_HANDLER_TIMEOUT=0     # Disable timeout
```

### Behavior

When `KBM_LEDSAS_HANDLER_TIMEOUT` > 0:

1. SDK wraps handler execution with `asyncio.wait_for()`.
2. If handler exceeds timeout, `asyncio.CancelledError` is raised.
3. SDK catches cancellation and treats it as a timeout error.
4. Error response sent to caller with code `Timeout`.

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

When a handler raises `errors.Retryable`:

1. SDK increments retry counter (stored in message headers).
2. If retry count < `KBM_LEDSAS_MAX_RETRIES`:
   - Calculate backoff: `min(2^retry_count + jitter, 60.0)` seconds (jitter ∈ [0, 1)).
   - Wait for backoff duration.
   - Requeue message for retry.
3. If retry count >= max retries:
   - Send message to Dead Letter Queue (DLQ).
   - Log error with final retry count.

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
if you raise `MAX_RETRIES` above 5.

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
    except ValueError as e:
        # Will NOT be retried - goes straight to DLQ
        raise errors.Permanent(f"Invalid input: {e}")
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
| `Timeout` | "Processing timed out (will retry)" | Retried — SDK-imposed timeout via `asyncio.wait_for` |
| `DeadlineExceeded` | "Request arrived too close to deadline" | Dead-lettered, no retry — deadline already past |
| `UnexpectedError` | "An unexpected error occurred" | Retried (treat as transient) |
| `HandlerNotFound` | "Handler not available" | Dead-lettered, no retry |

### Custom User Messages

The `user_message` parameter **always wins** when set — it's what the
caller sees regardless of `KBM_LEDSAS_GENERIC_ERRORS`. The flag only
chooses between the internal message and the generic fallback when
`user_message` is **not** supplied.

```python
# generic_errors=false (default):
raise errors.Retryable("DB pool exhausted")
# Caller sees: "DB pool exhausted"  (internal message)

raise errors.Retryable("DB pool exhausted", user_message="Retrying...")
# Caller sees: "Retrying..."        (user_message wins)

# generic_errors=true:
raise errors.Retryable("DB pool exhausted")
# Caller sees: "Processing failed temporarily"   (generic fallback)

raise errors.Retryable("DB pool exhausted", user_message="Retrying...")
# Caller sees: "Retrying..."        (user_message still wins)
```

The internal `message` is always logged regardless of which value reaches
the caller.

### Logging

Internal error details are always logged regardless of `generic_errors` setting:

```json
{
  "level": "ERROR",
  "message": "Handler error",
  "error_code": "Retryable",
  "internal_message": "DB connection pool exhausted after 30s timeout",
  "user_message": "Database temporarily unavailable, retrying...",
  "correlation_id": "abc-123"
}
```

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

**On expected error cases the SDK now suppresses upstream-library
traceback noise** (`aio_pika`/`aiormq` `ChannelNotFoundEntity` raised
by a missing reply_to exchange, and the unroutable-return raised when
the exchange exists but has no bound queue) — you still get the SDK's own clean
ERROR line and the DLQ counter. Genuine connection / channel errors
still surface unchanged.

---

## Supporting Types

### `BlobRef`
Reference to a blob storage object.

**Properties:**
- `uri` (str): Full blob URI (e.g., "azblob://container/path")
- `container` (str): Container name (derived from `uri`)
- `path` (str): Blob path within container (derived from `uri`)
- `version_id` (Optional[str]): Blob version identifier (parsed from the
  `?versionId=` query when present, else `None`)

**Class Methods:**
- `BlobRef.from_uri(uri: str) -> BlobRef`: Parse URI string into BlobRef

**Example:**
```python
from kbm_ledsas_sdk.models import BlobRef

# Parse URI
ref = BlobRef.from_uri("azblob://data/input.json")
print(ref.container)  # "data"
print(ref.path)        # "/input.json"

# Use in download
data = await ctx.blob.download_json(ref)
```

---

## Complete Example: Using All APIs

```python
from datetime import datetime, timezone, timedelta
from kbm_ledsas_sdk import ServiceApp, errors
from kbm_ledsas_sdk.models import BlobRef

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
    # Structured fields go in extra={...} (kwargs are silently dropped
    # by the stdlib logger — this is the canonical pattern).
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

    # === Blob Operations ===
    try:
        # Download (bytes, json, text)
        input_uri = req.get("input_uri")
        data_bytes = await ctx.blob.download_bytes(input_uri)
        config = await ctx.blob.download_json("azblob://config/settings.json")
        readme = await ctx.blob.download_text("azblob://docs/README.txt")

        # Process data
        await ctx.emit_status("processing", 0.5, f"Processing {len(data_bytes)} bytes")
        rows = len(data_bytes.split(b"\n"))

        # Upload (bytes, json, text)
        result = {"rows": rows, "options": req.get("options", {})}
        result_ref = await ctx.blob.upload_json(
            container="results",
            obj=result,
            path=f"output/{ctx.idempotency_key}.json",
        )

        # Upload summary as text. As above, key on idempotency_key so a
        # DLQ-replay overwrites the same blob (not a fresh orphan).
        summary = f"Processed {rows} rows successfully"
        await ctx.blob.upload_text(
            container="logs",
            text=summary,
            path=f"summaries/{ctx.idempotency_key}.txt",
        )

        await ctx.emit_status("done", 1.0, "Complete")
        return {
            "result_uri": result_ref.uri,
            "rows_processed": rows,
            "status": "success",
        }

    except TimeoutError:
        # Retryable error
        raise errors.Retryable("Network timeout")
    except ValueError as e:
        # Permanent error
        raise errors.Permanent(f"Invalid data: {e}")

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
| `GET /ready`  | Readiness — ready to accept work? | `/readyz` |

Both return JSON, status `200` when all checks pass and `503` when any
fail. The default body is intentionally minimal — no service name, no
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
  "version": "0.3.3",
  "checks": {
    "process": "healthy",
    "transport": "healthy",
    "db_alive": "healthy"
  }
}
```

In verbose mode, a failing check appears as `"unhealthy: <reason>"` in
the `checks` map.

### Deployment id

When the environment variable `KBM_LEDSAS_DEPLOYMENT_ID` is set (or, as a
fallback, `KBM_LASTCOMMITID`, typically injected by your deployment
platform), its value is added verbatim as a `deployment_id` field to
**every** `/health` and `/ready` body — both the minimal and verbose
forms:

```json
{ "status": "healthy", "deployment_id": "a1b2c3d4" }
```

`KBM_LEDSAS_DEPLOYMENT_ID` takes precedence; `KBM_LASTCOMMITID` is used
only when it is unset or blank. This lets an operator confirm *which
build* is live behind a probe without exposing any other detail. The key
is **omitted** when neither variable is set or the value is blank (e.g. a
local dev run), so the minimal default response stays fingerprint-free
outside a real deployment. A strict health-body consumer should treat
`deployment_id` as an optional field.

### Default checks

- **Liveness**: `process` — trivially `healthy` if the asyncio event
  loop is responsive enough to serve the HTTP request.
- **Readiness**: `transport` — `Transport.is_ready()` returns true
  (RabbitMQ connection open).

### Custom checks

`ServiceApp` exposes two decorators. Each registered function takes no
arguments and returns truthy = healthy. Both sync and async callables
are supported. Exceptions are caught and reported as unhealthy with the
exception type and message.

```python
from kbm_ledsas_sdk import ServiceApp

app = ServiceApp(service_name="csv-processor")

@app.liveness_check("event_loop_responsive")
def _():
    return True   # reaching here proves the loop is alive

@app.readiness_check("db_connected")
async def _():
    return await db.ping()

@app.readiness_check("model_loaded")
def _():
    return model is not None
```

A check name must be unique within its registry; registering twice
raises `ValueError` at decoration time (loud, immediate).

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KBM_LEDSAS_HEALTH_PORT` | `8090` | TCP port for the server. Set `0` to disable entirely. |
| `KBM_LEDSAS_HEALTH_HOST` | `127.0.0.1` | Bind address. Default is loopback only — safer on a dev laptop. Set to `0.0.0.0` when an orchestrator on a different host needs to reach `/health` and `/ready`. |

### Behavior under failure

- **Port already in use:** the SDK logs a `WARNING` and continues
  without health endpoints. Your service still consumes commands; you
  just lose probes until the port is freed.
- **Custom check raises an exception:** that check is `unhealthy` with
  the exception text in the detail. Other checks still run; status
  rolls up across all of them.
- **Shutdown:** the health server is stopped *before* the transport, so
  probes correctly see "not ready" during teardown.

### `ServiceApp.health_server_running`

Read-only property returning `bool` — `True` while the embedded health
server is actually bound and serving, `False` when it is disabled
(`KBM_LEDSAS_HEALTH_PORT=0`) or failed to bind (port in use). Primarily a
test/diagnostic helper, e.g. for an integration test that needs to assert
"the health endpoint is up before I probe it."

```python
if app.health_server_running:
    ...
```

---

**Version:** 0.3.3
**Release:** 2026-07-09

### Version history

See [CHANGELOG.md](../CHANGELOG.md) for the full release history.
