# KeborMed LEDSAS SDK (Python)

[![CI](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

A Python client SDK for building **LEDSAS** (Local External Data Sources Action Service)
data-processing services. You write async handler functions; the SDK consumes command
messages from **RabbitMQ**, runs your handler, reads and writes payloads in **Azure Blob
Storage**, and (optionally) publishes the response back to the caller.

It handles the plumbing — message consumption, acknowledgement, retries, timeouts,
structured logging, health/readiness endpoints, and blob I/O — so your service code stays
focused on the data transformation.

---

## Features

- **Handler-based programming model** — register `async` handlers with a decorator.
- **RabbitMQ consumption** with prefetch, bounded concurrency, ack/nack semantics, and a
  dead-letter path for poison messages.
- **Azure Blob Storage** helpers: `download_bytes/text/json/stream`, `upload_bytes/text/json`,
  and memory-bounded streaming for large files.
- **Typed models** (Pydantic v2) for blob references and message envelopes.
- **Built-in retries & timeouts** with permanent vs. transient error classes.
- **Health & readiness** HTTP endpoints (`/health`, `/ready`, `/livez`, `/readyz`) with
  pluggable liveness/readiness checks.
- **Structured JSON logging** out of the box.
- **Fully type-hinted** (`py.typed`), Python 3.11+.

## Installation

Requires **Python 3.11 or newer**.

```bash
pip install kbm-ledsas-sdk
```

To work from a clone — or before a release is published to PyPI — install from source:

```bash
git clone https://github.com/Kubyk-Health/kbm-ledsas-sdk.git
cd kbm-ledsas-sdk
pip install -e .            # add "[dev]" for the test/lint toolchain: pip install -e ".[dev]"
```

## Quickstart

```python
import logging

import kbm_ledsas_sdk
from kbm_ledsas_sdk import ServiceApp, errors
from kbm_ledsas_sdk.models import BlobRef

# Your own log records in the SDK's JSON format, extra= fields included. The SDK
# writes its own records itself, in KBM_LEDSAS_LOG_FORMAT (JSON by default), once
# app.run() starts.
handler = logging.StreamHandler()
handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

app = ServiceApp(service_name="csv-processor")


@app.handler("ProcessCSV")
async def handle(ctx, req: dict) -> dict:
    csv_uri = req.get("csv_uri")
    if not csv_uri:
        # user_message is what the caller sees; the internal message goes to logs.
        raise errors.Permanent(
            "Validation: csv_uri missing",
            user_message="csv_uri is required",
        )

    text = await ctx.blob.download_text(csv_uri)
    # Take the container from BlobRef, never from a hand-split string: with blob
    # versioning enabled the URI carries a ?versionId= query.
    container = BlobRef.from_uri(csv_uri).container

    out = await ctx.blob.upload_json(
        container=container,
        obj={"rows": len(text.splitlines()) - 1},
        # Use idempotency_key (not message_id) so a redelivery of the same logical
        # request overwrites the same blob. overwrite=True is required for that.
        path=f"result-{ctx.idempotency_key}.json",
        overwrite=True,
    )
    return {"result_uri": out.uri}


# Optional liveness/readiness hooks (the SDK ships sensible defaults; these layer
# on top). Readiness only decides what /ready answers; it does not hold back
# commands, so do warm-up work (loading a model, ...) before app.run().
@app.liveness_check("worker_alive")
def _() -> bool:
    return True


if __name__ == "__main__":
    app.run()
```

Run it against a local RabbitMQ + Azure Blob (Azurite):

```bash
export KBM_LEDSAS_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/"
export KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=...;AccountKey=...;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
python my_service.py
```

The service also exposes health endpoints at
`http://127.0.0.1:8090/health` and `/ready` (loopback only by default; set
`KBM_LEDSAS_HEALTH_HOST=0.0.0.0` to bind all interfaces, or
`KBM_LEDSAS_HEALTH_PORT=0` to turn them off).

## Local development

A ready-to-run stack (RabbitMQ + Azurite) is provided with the example service:

```bash
cd examples/hello_world_service
pip install -e ../..                                        # install the SDK from this repo
docker compose -f deploy/local/docker-compose.yml up -d     # start RabbitMQ + Azurite
python main.py                                              # run the example service
```

See [`examples/hello_world_service/`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/main/examples/hello_world_service) for a complete,
runnable service (handler, sender script, tests, `Dockerfile`, and a local
`docker compose` stack under `deploy/local/`).

## Container base image

To run a service in a container, build it `FROM` the LEDSAS base image
[`ghcr.io/kebormed/kbm-ledsas-base-direct`](https://github.com/orgs/KeborMed/packages/container/package/kbm-ledsas-base-direct),
which comes with the SDK and its runtime dependencies installed (Python 3.11 on
Alpine Linux, amd64 and arm64):

```dockerfile
FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.5
WORKDIR /app
USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY main.py .
USER kbmuser
CMD ["python", "main.py"]
```

- **The image tag is the SDK version on PyPI**: `:0.3.5` carries `kbm-ledsas-sdk`
  0.3.5. `latest` moves with every release; build on a version tag, and pin the
  digest as well (`:0.3.5@sha256:<digest>`) for a reproducible build.
- **Never list `kbm-ledsas-sdk` in your `requirements.txt`** and never install it
  again in the image: the base image pins its SDK version, and a requirement for
  another version fails the build.
- **The image runs as the non-root user `kbmuser` (uid 1000) in `/app`.** Switch
  to `USER root` only to install your own dependencies, then back to `kbmuser`.
- **Alpine uses musl libc.** Packages that publish no `musllinux` wheels (for
  example torch, onnxruntime, tensorflow, opencv-python, scikit-learn) do not
  install on it; numpy, pandas, scipy, pillow, scikit-image, pyarrow and pydicom
  do.

[`examples/hello_world_service/Dockerfile`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/examples/hello_world_service/Dockerfile)
is the complete version (it also clears the base image's labels, keeps the code
read-only for the service user and removes pip at the end of the build), and its
[README](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/examples/hello_world_service/README.md#base-image)
explains each step.

## Configuration

The SDK is configured through environment variables. The most common are below; see the [API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/docs/SDK_API_REFERENCE.md#environment-variables) for the complete list (including `KBM_LEDSAS_ALLOW_INSECURE_AMQP`, `KBM_LEDSAS_MAX_PAYLOAD_BYTES`, and other advanced settings):

| Variable | Description | Default |
|----------|-------------|---------|
| `KBM_LEDSAS_SERVICE_NAME` | Overrides the required `ServiceApp(service_name=...)` argument, which names the AMQP exchange and queue (a WARNING is logged when they differ); leave it unset, or keep it equal to the constructor value | *(optional)* |
| `KBM_LEDSAS_TENANT` | Tenant identifier, part of the AMQP exchange and queue names | *(optional)* |
| `KBM_LEDSAS_RABBITMQ_URL` | RabbitMQ connection URL (`amqp://` or `amqps://`) | *(required)* |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Azure Blob Storage connection string (with an `AccountKey` or a `SharedAccessSignature`) | *(required)* |
| `KBM_LEDSAS_CONTAINER` | Informational only: logged at startup; the upload methods always take an explicit `container` | `dev` |
| `KBM_LEDSAS_PREFETCH` | AMQP prefetch count | `10` |
| `KBM_LEDSAS_CONCURRENCY` | Maximum concurrent handlers | `4` |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | Per-handler timeout in seconds (`0` disables; at most 86400) | `1800` |
| `KBM_LEDSAS_MAX_RETRIES` | Retries for transient failures before the command is dead-lettered | `3` |
| `KBM_LEDSAS_GENERIC_ERRORS` | When `true`, callers get a generic error message unless the handler set `user_message` | `false` |
| `KBM_LEDSAS_CONNECT_TIMEOUT` | Seconds one connection attempt to the broker may take (`1` to `300`) | `15` |
| `KBM_LEDSAS_LOG_LEVEL` | Log level | `INFO` |
| `KBM_LEDSAS_LOG_FORMAT` | Log format (`json` or `text`) | `json` |
| `KBM_LEDSAS_DEBUG` | When `1`, a configuration error at startup prints the full traceback instead of one line | `0` |
| `KBM_LEDSAS_HEALTH_HOST` | Health server bind host | `127.0.0.1` |
| `KBM_LEDSAS_HEALTH_PORT` | Health server port (`0` disables; a port in use stops the service at start) | `8090` |
| `KBM_LASTCOMMITID` | Build/commit id surfaced as `deployment_id` in `/health` and `/ready` (usually set by the deployment platform) | *(optional)* |

A `KBM_LEDSAS_*` variable the SDK does not know (a typo, for example) is reported
at startup with one WARNING.

## Documentation

- [API Reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/docs/SDK_API_REFERENCE.md)
- [Example service](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/main/examples/hello_world_service)
- [Compatibility & supported versions](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/COMPATIBILITY.md)
- [Upgrade guide](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/UPGRADING.md)
- [Changelog](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/CHANGELOG.md)

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/CONTRIBUTING.md) first — it covers
the development setup, how to run the tests, the coding style, and the **Developer Certificate
of Origin (DCO)** sign-off we require on every commit. Also see our
[Code of Conduct](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately — see [SECURITY.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/SECURITY.md). Do **not** open a
public issue for security reports.

## License

Licensed under the [Apache License 2.0](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/LICENSE). Copyright © 2025-2026 KeborMed.
See [THIRD_PARTY_NOTICES.txt](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/main/THIRD_PARTY_NOTICES.txt) for third-party dependency licenses.

Maintained by **KeborMed** (the software's copyright holder) under the
[**Kubyk-Health**](https://github.com/Kubyk-Health) GitHub organization.
