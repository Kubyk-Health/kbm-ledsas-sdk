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
| `aio-pika` | `>=9.4.0,<10.0` |
| `azure-storage-blob` | `>=12.19.0,<13.0` |
| `aiohttp` | `>=3.8.0,<4.0` |
| `cryptography` | `>=48.0.1` (security floor; pulled in transitively) |

## Release compatibility matrix

| kbm-ledsas-sdk | Python | AMQP envelope schema | Upgrade from previous |
|----------------|--------|----------------------|-----------------------|
| **0.3.3**      | 3.11–3.13 | `1.0` | Drop-in from 0.3.2 — one behavioral change for *misconfigured* reply routing (see [UPGRADING.md](UPGRADING.md)) |
| 0.3.2          | 3.11–3.13 | `1.0` | First public release |

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
