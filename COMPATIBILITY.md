# Compatibility

What `kbm-ledsas-sdk` runs with, and what stays stable between releases. What to do when you
upgrade: [UPGRADING.md](UPGRADING.md). What changed: [CHANGELOG.md](CHANGELOG.md).

## Python

| Python | Supported |
|--------|-----------|
| 3.11   | ✅ |
| 3.12   | ✅ |
| 3.13   | ✅ |
| 3.14 and newer | Installs, not yet tested |

The wheel declares `Requires-Python >=3.11` with no upper bound; CI tests 3.11 to 3.13.

## Runtime dependencies

| Dependency | Range |
|------------|-------|
| `pydantic` | `>=2.8.0,<3.0` (the first with Python 3.13 wheels) |
| `aio-pika` | `>=9.4.0,<11.0` (9.x and 10.x) |
| `azure-storage-blob` | `>=12.19.0,<13.0` |
| `aiohttp` | `>=3.14.3,<4.0` (security floor) |
| `cryptography` | `>=50.0.0` (security floor; transitive) |
| `azure-core` | `>=1.38.0,<2.0` (security floor; also via `azure-storage-blob`) |
| `aiormq` | `>=6.8.0,<8.0` (used directly) |
| `pamqp` | `>=3.3.0,<5.0` (used directly) |

## Releases

| kbm-ledsas-sdk | Python | AMQP envelope schema | Upgrade from the previous release |
|----------------|--------|----------------------|-----------------------------------|
| **0.3.6**      | 3.11–3.13 | `1.0` | From 0.3.3: no public method removed, but some behaviour changes and invalid input is refused; see [UPGRADING.md](UPGRADING.md#upgrading-to-036-from-033) |
| 0.3.5          | — | — | Not released; number skipped, no changes |
| 0.3.4          | — | — | Not released; number skipped, no changes |
| 0.3.3          | 3.11–3.13 | `1.0` | Drop-in from 0.3.2, except for misconfigured reply routing |
| 0.3.2          | 3.11–3.13 | `1.0` | First public release |

## Container base image

`ghcr.io/kebormed/kbm-ledsas-base-direct`, published under the KeborMed GitHub organization: the
official `python:3.11-alpine` image (Python 3.11 on Alpine Linux, musl libc; `linux/amd64`, `linux/arm64`) with `kbm-ledsas-sdk`
of the tag's version and its runtime dependencies installed from PyPI, a pip constraints file
(`PIP_CONSTRAINT=/etc/kbm-ledsas-pip-constraints.txt`, also `UV_CONSTRAINT`) that pins that SDK
version and the `cryptography` floor, and the non-root user `kbmuser` (uid 1000) with the working
directory `/app`.

| Image tag | kbm-ledsas-sdk | Python | OS | Platforms | Digest at release |
|-----------|----------------|--------|----|-----------|-------------------|
| `0.3.6`   | 0.3.6 | 3.11 | Alpine Linux (musl libc) | `linux/amd64`, `linux/arm64` | listed here when the image is published, with whether it carries an SBOM attestation |
| `latest`  | the newest release | | | | |

There is no image for a number that was not released. The image is built by the project
maintainers; its build recipe is maintained by them and is not published, so the image carries no
`org.opencontainers.image.source` label (its `org.opencontainers.image.documentation` label names
this project). Verify an image by its digest: each release lists the image's digest in the table
above (`docker buildx imagetools inspect ghcr.io/kebormed/kbm-ledsas-base-direct:<tag>`), and,
where the table says the image carries an SBOM attestation, read it with
`docker buildx imagetools inspect ghcr.io/kebormed/kbm-ledsas-base-direct@<digest> --format '{{json .SBOM}}'`.
A rebuild for Alpine or Python security patches keeps the tag and changes the digest: pin the
digest you tested, and never build on `latest`.

## Stability guarantees within 0.x

- **Public API:** additive. `ServiceApp` and its decorators, `ExecutionContext`, the blob helpers,
  the error classes, `BlobRef` and the health endpoints only grow.
- **AMQP wire contract:** stable. The envelope (schema `1.0`), the message shapes, the topology
  names (`{cmd,queue,dlq,dlq.queue}.[{tenant}.]{service}.v1`) and the routing keys (`response`,
  `status`) do not change, so a service and its callers upgrade independently.
- **Configuration:** additive; new settings ship with compatible defaults.

**Exception in 0.3.6:** input that was never valid but used to be accepted is now refused, and
some behaviour visible on the wire changes (one reply per command, strict envelope types,
refused `reply_to` exchanges, a health port in use stops the start). Each change, with what to
do, is in [UPGRADING.md](UPGRADING.md#upgrading-to-036-from-033).

## Callers

Any caller interoperates if it publishes a command envelope in the documented format (see the
[API reference](docs/SDK_API_REFERENCE.md)) and, to receive the reply, declares its reply exchange
and binds a durable, non-auto-delete queue to it for routing key `response` (as
[`send_hello.py`](examples/hello_world_service/scripts/send_hello.py) does).
