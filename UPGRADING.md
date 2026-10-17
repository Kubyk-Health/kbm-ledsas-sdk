# Upgrading

How to upgrade `kbm-ledsas-sdk` safely between releases. The public API stays
additive and the AMQP wire contract stays stable within `0.x` (see
[COMPATIBILITY.md](COMPATIBILITY.md)), so most upgrades are drop-in. Any behavioral
change is documented here and in [CHANGELOG.md](CHANGELOG.md).

## Before any upgrade

1. **Pin the exact version** you are moving to (for example
   `kbm-ledsas-sdk==0.3.5`) and upgrade deliberately.
2. **Read the section below** for your target version, alongside the matching
   [CHANGELOG.md](CHANGELOG.md) entry.
3. **Run your test suite** against the new version in a non-production environment
   before rolling out.

---

## Upgrading to 0.3.5 (from 0.3.3)

**Type:** dependency, security and reliability release. **No public method or
parameter was removed or renamed** and the AMQP envelope (schema `1.0`) and
topology are unchanged, but several behaviors change, and input that was never
valid but used to be accepted is now refused with a clear error. 0.3.4 was not
published to PyPI; upgrade straight from 0.3.3. The full list of changes is in the
[CHANGELOG.md](CHANGELOG.md) `[0.3.5]` entry; this section lists what may need
action.

### Dependencies

- **`cryptography>=50.0.0`** (security floor, was `>=48.0.1`) and
  **`aiohttp>=3.14.3`** (was `>=3.8.0`). If you pin either below the new floor in
  your own requirements or constraints, the install fails to resolve: raise your
  pin.
- **`aio-pika` 10.x is accepted** (`>=9.4.0,<11.0`). Nothing to do; if you pin
  `aio-pika` yourself, both 9.x and 10.x work.

### Callers: one reply per command

- **A command gets one reply, when it has finished.** An attempt that will be
  retried sends no reply any more; the one reply is the handler's result, a
  non-retryable error, or the last attempt's error once `KBM_LEDSAS_MAX_RETRIES` is
  spent. A caller that waited for several replies can stop at the first. **Allow
  for the retries in the caller's reply timeout:** with the default 3 retries the
  backoff alone is 7 to 10 seconds (30 seconds or more is a safe timeout).
- **Envelope timestamps and `priority` are not converted any more.** Send
  `sent_at` / `deadline` as ISO 8601 text and `priority` as an integer; a command
  with any other type is dead-lettered as an invalid envelope.
- **A `reply_to` naming a service's own command or dead-letter exchange**
  (`cmd.…` / `dlq.…`) is refused: the command is dead-lettered before its handler
  runs. `"reply_to": null` now means fire-and-forget, like `""`.
- **A result with a top-level `"error"` key is a successful result:** any dict a
  handler returns is replied as it is and the command acknowledged (such a result
  used to have its command dead-lettered after the reply). To fail a command,
  raise (`errors.Permanent`, `errors.Retryable`, ...).

### Handlers

- **`@app.handler` checks what it registers.** The command name must match
  `^[A-Za-z][A-Za-z0-9_]*$` (at most 64 characters) and the version must look like
  `1.0`; a handler that is not `async def` raises `TypeError` at registration. If
  registration now raises, rename the command (callers must send the new name) or
  define the handler with `async def` (run blocking work with
  `await asyncio.to_thread(...)`).
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

### Configuration and startup

- **Connection settings are checked at start.** Only `amqp://` and `amqps://`
  broker URLs with a host are accepted, credentials with unencoded special
  characters are refused with a clear message, and a storage connection string
  must carry a credential (an `AccountKey` the storage client can decode, or a
  `SharedAccessSignature`). Fix the setting the error names.
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

- **`/ready` reports a broker outage** (`503` until the SDK has reconnected by
  itself) and answers `503` while the service stops. A readiness probe that
  expected `200` throughout an outage now sees the outage.
- **Log alerts.** A retried failure logs one WARNING per attempt, with a traceback
  only on the last attempt; an alert on `Uncaught exception in handler` at ERROR
  level no longer fires. Alert on the `Max retries (N) exceeded` ERROR (one per
  command whose retries are spent) or on the WARNING records.
- **Log formats.** JSON log timestamps are UTC ISO 8601 with milliseconds and a `Z`
  suffix, and the text format shows the failure details as `key=value` fields;
  update a log parser that expected the old shapes.
- **Message bodies nested more than 256 levels deep are dead-lettered.** Flatten
  such data, or upload it to blob storage and send its reference.

### Containers

- **Build service images `FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.5`**,
  which comes with the SDK installed (the tag is the SDK version), instead of
  installing `kbm-ledsas-sdk` on a generic Python image; never list the SDK in your
  `requirements.txt`. See the [README](README.md#container-base-image) and
  [`examples/hello_world_service/`](examples/hello_world_service/). This is a
  recommendation: `pip install kbm-ledsas-sdk==0.3.5` on any Python 3.11+ image
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
- **Now (0.3.3):** the broker returns the unroutable message, `send_response()`
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

If you install from source (`pip install -e .`), check out the target release,
reinstall, and re-run your tests:

```bash
git fetch --tags
git checkout v0.3.5
pip install -e .
pytest
```
