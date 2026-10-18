# Compatibility

What `kbm-ledsas-sdk` is compatible with, and the backwards-compatibility
guarantees between releases. For step-by-step upgrade instructions see
[UPGRADING.md](UPGRADING.md); for the full change history see
[CHANGELOG.md](CHANGELOG.md).

## Python

| Python | Supported |
|--------|-----------|
| 3.11   | ✅ |
| 3.12   | ✅ |
| 3.13   | ✅ |

The wheel declares `Requires-Python >=3.11` with no upper bound, so it installs on
newer interpreters as they are released. CI runs the test suite on 3.11, 3.12, and
3.13.

## Runtime dependencies

| Dependency | Range |
|------------|-------|
| `pydantic` | `>=2.5.0,<3.0` |
| `aio-pika` | `>=9.4.0,<11.0` (9.x and 10.x) |
| `azure-storage-blob` | `>=12.19.0,<13.0` |
| `aiohttp` | `>=3.14.3,<4.0` (security floor) |
| `cryptography` | `>=50.0.0` (security floor; pulled in transitively) |

## Release compatibility matrix

| kbm-ledsas-sdk | Python | AMQP envelope schema | Upgrade from previous |
|----------------|--------|----------------------|-----------------------|
| **0.3.6**      | 3.11–3.13 | `1.0` | From 0.3.3: no public method or parameter removed or renamed, but several behaviors change and some invalid input that used to be accepted is now refused — read [UPGRADING.md](UPGRADING.md#upgrading-to-036-from-033) |
| 0.3.5          | — | — | Not released; number skipped, no changes |
| 0.3.4          | — | — | Not released; number skipped, no changes |
| 0.3.3          | 3.11–3.13 | `1.0` | Drop-in from 0.3.2 — one behavioral change for *misconfigured* reply routing (see [UPGRADING.md](UPGRADING.md)) |
| 0.3.2          | 3.11–3.13 | `1.0` | First public release |

## Container base image

The LEDSAS base image `ghcr.io/kebormed/kbm-ledsas-base-direct` (published under
the KeborMed GitHub organization) is the official Python 3.11 Alpine Linux image
with the SDK and its runtime dependencies installed. Its tag is the SDK version it
carries. What it contains:

- **Python 3.11** (the latest 3.11.x patch release of the official
  `python:3.11-alpine` image when the image was built) on **Alpine Linux** (musl
  libc), for `linux/amd64` and `linux/arm64`;
- **`kbm-ledsas-sdk`** of the tag's version, installed from PyPI with its
  runtime dependencies (the ranges in "Runtime dependencies" above, resolved
  when the image was built);
- **a pip constraints file**, `PIP_CONSTRAINT=/etc/kbm-ledsas-pip-constraints.txt`
  (also `UV_CONSTRAINT`), which pins `kbm-ledsas-sdk` to the tag's version and
  keeps the `cryptography` security floor, so a requirement for another SDK
  version fails the build;
- **the non-root user `kbmuser` (uid 1000)** and the working directory `/app`.

The image is rebuilt from the same recipe to pick up Alpine and Python security
patches; a rebuild keeps the tag and changes the digest, so pin the digest you
tested (`docker buildx imagetools inspect ghcr.io/kebormed/kbm-ledsas-base-direct:<tag>`
prints it).

| Image tag | kbm-ledsas-sdk | Python | OS | Platforms | Digest at release |
|-----------|----------------|--------|----|-----------|-------------------|
| `0.3.6`   | 0.3.6 | 3.11 | Alpine Linux (musl libc) | `linux/amd64`, `linux/arm64` | listed here when the image is published |
| `latest`  | the newest release | | | | |

There is no image for a version number that was not released (no `0.3.4` or
`0.3.5`). `latest` moves with every release: build on a version tag, and pin the
digest for a reproducible build. See the [README](README.md#container-base-image)
and [`examples/hello_world_service/`](examples/hello_world_service/) for how to
build a service on it.

## Backwards-compatibility policy

While the SDK is in `0.x`, the following are treated as compatibility contracts and
are not broken in a minor/patch release; any exception is called out in
[CHANGELOG.md](CHANGELOG.md) and [UPGRADING.md](UPGRADING.md):

- **The public API is additive.** The handler-facing surface — `ServiceApp` and its
  `@app.handler` / `@app.liveness_check` / `@app.readiness_check` decorators,
  `ExecutionContext` (including `ctx.blob`, `ctx.emit_status`, and the id
  properties), the blob helpers, the error classes
  (`Retryable` / `Permanent` / `DeadlineExceeded`), `BlobRef`, and the health
  endpoints — grows only by addition. Handler code written for an earlier release
  keeps importing and running on a newer one.
- **The AMQP wire contract is stable.** The message envelope (schema `1.0`), the
  message shapes (command / response / status), the topology naming
  (`{cmd,queue,dlq,dlq.queue}.[{tenant}.]{service}.v1`), and the publish routing
  keys (`response`, `status`) do not change between releases. A service and the
  caller that drives it can therefore be upgraded **independently** — a newer
  service interoperates with an existing caller, and vice versa.
- **Configuration is additive.** New environment variables ship with
  backwards-compatible defaults; no previously-valid configuration is rejected by a
  minor upgrade.

**Exception in 0.3.6:** input that was never valid but used to be accepted silently
is now refused with a clear error — for example a handler name a command cannot
carry, a handler that is not `async def`, a malformed blob URI, a broker URL that
is not `amqp://` / `amqps://`, a storage connection string without a credential,
a broker URL whose `no_verify_ssl` turns certificate verification off (any value
except `0`; unless `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` acknowledges it), or a boolean setting with a value that is not a boolean. Some
changes are visible on the wire, in the configuration or in the public API:

- **one reply per command**: an attempt that will be retried sends no reply; the
  one reply comes when the command has finished;
- **strict envelope types**: `sent_at` and `deadline` must be ISO 8601 text and
  `priority` an integer, or the command is dead-lettered;
- **a `reply_to` naming a service's own `cmd.…` or `dlq.…` exchange, or a
  reserved broker exchange (`amq.…`), is refused** (the command is dead-lettered
  before its handler runs);
- **version fields are ASCII digits, 1 to 4 on each side of the dot**
  (`schema_version`, `message_version`, the `@app.handler` version);
- **the SDK asks the broker for a 15-second heartbeat** (new
  `KBM_LEDSAS_HEARTBEAT`; 0.3.3 set none of its own, so the AMQP client library's
  default of 60 seconds applied):
  blocking code that holds the event loop for more than about 30 seconds now loses
  the broker connection;
- **`Envelope`, `Command`, `Response` and `Status` are read-only**: assigning a
  field raises `ValidationError` (build a changed copy with `model_copy(update=...)`);
- **a health port that cannot be bound stops the service at start**.

The full list, with what to do for each, is in
[UPGRADING.md](UPGRADING.md#upgrading-to-036-from-033).

## Interoperability with callers

The SDK is the **service** side of the exchange. Any caller — an orchestrator, a
test rig, or another service — interoperates as long as it:

1. publishes a command envelope in the documented format (see the
   [API reference](docs/SDK_API_REFERENCE.md)), and
2. to receive a response, declares its reply exchange **and binds a queue** to it
   for routing key `response` — durable and not auto-delete (see
   [`examples/hello_world_service/scripts/send_hello.py`](examples/hello_world_service/scripts/send_hello.py)).

Because the wire contract is stable across releases, the caller's own version is
independent of the SDK version the service runs on.
