# KeborMed LEDSAS SDK (Python)

[![CI](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/LICENSE)

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
- **Azure Blob Storage** helpers: `download_bytes/text/json/stream`,
  `upload_bytes/text/json/stream`, and memory-bounded streaming for large files.
- **Typed models** (Pydantic v2) for blob references and message envelopes.
- **Built-in retries & timeouts** with permanent vs. transient error classes.
- **Health & readiness** HTTP endpoints (`/health`, `/ready`, `/livez`, `/readyz`) with
  pluggable liveness/readiness checks.
- **Structured JSON logging** out of the box.
- **Type hints** (`py.typed`) on the public API, Python 3.11+.

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

Save this as `my_service.py`:

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

# Whoever can publish a command chooses the blob URIs in it, and the service
# reads and writes with its own storage credential: read only from the
# container meant for input, and write only where the service decides.
INPUT_CONTAINER = "test-container"
RESULTS_CONTAINER = "results"


@app.handler("ProcessCSV")
async def handle(ctx, req: dict) -> dict:
    csv_uri = req.get("csv_uri")
    if not csv_uri:
        # user_message is what the caller sees; the internal message goes to logs.
        raise errors.Permanent(
            "Validation: csv_uri missing",
            user_message="csv_uri is required",
        )
    # Take the container from BlobRef, never from a hand-split string: with blob
    # versioning enabled the URI carries a ?versionId= query.
    if BlobRef.from_uri(csv_uri).container != INPUT_CONTAINER:
        raise errors.Permanent(
            "Validation: csv_uri outside the input container",
            user_message=f"csv_uri must be in the {INPUT_CONTAINER} container",
        )

    text = await ctx.blob.download_text(csv_uri)
    out = await ctx.blob.upload_json(
        container=RESULTS_CONTAINER,
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

The handler reads only from `INPUT_CONTAINER` and writes only to
`RESULTS_CONTAINER`, because anyone who can publish to the service's command
exchange chooses the blob URIs it receives. To hold every handler of a
service to a fixed set of containers, also set `KBM_LEDSAS_ALLOWED_CONTAINERS`
(for example `test-container,results`): `ctx.blob` then refuses any other
container before anything is sent to storage. It limits what the service's
storage credential is used for; it does not authenticate callers, and the
storage account's own access control stays the boundary. See "Security
model" in the
[API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#security-model).

Run it against a local RabbitMQ and Azure Blob Storage emulator (Azurite), from
the directory where you saved `my_service.py` (nothing else is needed there).
The two `docker run` lines start both on loopback, and the `until` line waits
until RabbitMQ has started:

```bash
docker run -d --name ledsas-quickstart-rabbitmq -p 127.0.0.1:5672:5672 rabbitmq:4.1-management-alpine
docker run -d --name ledsas-quickstart-azurite -p 127.0.0.1:10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:3.34.0 azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
until docker logs ledsas-quickstart-rabbitmq 2>&1 | grep -q "Server startup complete"; do sleep 1; done

# Azurite's documented development account (devstoreaccount1) and its
# well-known public key: not a secret, it only works against the emulator.
export KBM_LEDSAS_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/"
export KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
python my_service.py
```

Stop and remove the two containers afterwards with
`docker rm -f -v ledsas-quickstart-rabbitmq ledsas-quickstart-azurite`. (The example
service under `examples/hello_world_service/` has the same stack as a
`docker compose` file; see "Local development" below.)

The service also exposes health endpoints at
`http://127.0.0.1:8090/health` and `/ready` (loopback only by default; set
`KBM_LEDSAS_HEALTH_HOST=0.0.0.0` to bind all interfaces, or
`KBM_LEDSAS_HEALTH_PORT=0` to turn them off).

### Send it a command

A caller publishes a command envelope to the service's command exchange
(`cmd.csv-processor.v1`) and, to get the reply, binds a queue of its own to a
reply exchange for routing key `response`. Save this as `send_command.py` and
run it in a second shell with the same two `export` lines, while
`my_service.py` runs: it creates the `test-container` and `results`
containers in Azurite, uploads `test-container/sample.csv`, sends
`ProcessCSV` and prints the reply. If the service is not running, it prints
one `error:` line and exits with status 1; the unanswered command expires
after 30 seconds (the broker moves it to the service's dead-letter queue)
instead of running when the service starts later.

```python
import asyncio
import json
import logging
import os
import sys
import uuid
from datetime import UTC, datetime

import aio_pika
from aio_pika.exceptions import AMQPError, ChannelNotFoundEntity
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient

CSV = "name,value\na,1\nb,2\n"
WAIT = 30  # seconds to wait for the reply
logging.getLogger("aiormq").setLevel(logging.CRITICAL)  # errors are reported below
logging.getLogger("aio_pika").setLevel(logging.CRITICAL)


async def main() -> None:
    connection = await aio_pika.connect(os.environ["KBM_LEDSAS_RABBITMQ_URL"], timeout=10)
    async with connection:
        channel = await connection.channel()
        try:
            command_exchange = await channel.get_exchange("cmd.csv-processor.v1", ensure=True)
        except ChannelNotFoundEntity:
            sys.exit("error: no exchange cmd.csv-processor.v1: start my_service.py first")
        blobs = BlobServiceClient.from_connection_string(os.environ["KBM_LEDSAS_BLOB_CONN_STRING"])
        for name in ("test-container", "results"):
            if not blobs.get_container_client(name).exists():
                blobs.create_container(name)
        blobs.get_blob_client("test-container", "sample.csv").upload_blob(CSV, overwrite=True)

        correlation_id = str(uuid.uuid4())
        reply_exchange = f"reply-ex-quickstart-{uuid.uuid4().hex[:8]}"
        rx = await channel.declare_exchange(reply_exchange, aio_pika.ExchangeType.TOPIC, durable=True)
        rq = await channel.declare_queue(f"{reply_exchange}-q", durable=True, auto_delete=False)
        try:
            await rq.bind(rx, routing_key="response")
            envelope = {
                "schema_version": "1.0",
                "type": "command",
                "name": "ProcessCSV",
                "message_version": "1.0",
                "message_id": str(uuid.uuid4()),
                "correlation_id": correlation_id,
                "idempotency_key": f"quickstart-{uuid.uuid4().hex}",
                "sent_at": datetime.now(UTC).isoformat(),
                "trace_id": str(uuid.uuid4()),
                "reply_to": reply_exchange,
            }
            body = {"envelope": envelope, "payload": {"csv_uri": "azblob://test-container/sample.csv"}}
            await command_exchange.publish(
                aio_pika.Message(
                    json.dumps(body).encode(), content_type="application/json", expiration=WAIT
                ),
                routing_key="command",
            )
            try:
                async with rq.iterator(timeout=WAIT) as replies:
                    async for message in replies:
                        async with message.process():
                            reply = json.loads(message.body)
                        if reply["envelope"].get("correlation_id") == correlation_id:
                            print(reply["payload"])  # {'result_uri': 'azblob://results/result-...json'}
                            break
            except TimeoutError:
                sys.exit(f"error: no reply within {WAIT} s (is my_service.py running?)")
        finally:
            await rq.delete(if_unused=False, if_empty=False)
            await rx.delete()


try:
    asyncio.run(main())
except (AMQPError, AzureError, OSError, LookupError, ValueError) as error:
    sys.exit(f"error: {type(error).__name__}: {error}")
except KeyboardInterrupt:
    sys.exit("interrupted")
```

[`examples/hello_world_service/scripts/send_hello.py`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/examples/hello_world_service/scripts/send_hello.py)
is the same caller with command-line options and the error messages spelled out.

## Local development

A ready-to-run example service, with its local stack (RabbitMQ + Azurite), is
in [`examples/hello_world_service/`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/v0.3.6/examples/hello_world_service)
(handler, sender script, tests, `Dockerfile`, and a `docker compose` stack
under `deploy/local/`). From a clone of this repository:

```bash
cd examples/hello_world_service
pip install -e ../..                                            # the SDK from this clone
docker compose -f deploy/local/docker-compose.yml up -d --wait  # RabbitMQ + Azurite, healthy
cp .env.example .env                                            # the local settings
(set -a; . ./.env; set +a; exec python main.py)                 # the service, with .env loaded
```

and, in a second shell in the same directory:

```bash
(set -a; . ./.env; set +a; exec python scripts/send_hello.py Ada)   # prints "greeting: hello Ada"
```

The subshell `(set -a; . ./.env; set +a; exec ...)` exports the settings in
`.env` for that one command only. Without a clone, take the example from a
release's source archive and install the SDK from PyPI instead of
`pip install -e ../..`:

```bash
curl -L https://github.com/Kubyk-Health/kbm-ledsas-sdk/archive/refs/tags/v0.3.6.tar.gz | tar -xz
cd kbm-ledsas-sdk-0.3.6/examples/hello_world_service
pip install kbm-ledsas-sdk==0.3.6
```

then continue with the `docker compose` line above.

## Container base image

To run a service in a container, build it `FROM` the LEDSAS base image
[`ghcr.io/kebormed/kbm-ledsas-base-direct`](https://github.com/orgs/KeborMed/packages/container/package/kbm-ledsas-base-direct)
(published under the KeborMed GitHub organization, the SDK's maintainer). It is
the official Python 3.11 Alpine Linux image (`linux/amd64` and `linux/arm64`)
with `kbm-ledsas-sdk` of the tag's version and its runtime dependencies
installed from PyPI, a pip constraints file that pins that SDK version
(`PIP_CONSTRAINT=/etc/kbm-ledsas-pip-constraints.txt`), and the non-root user
`kbmuser` (uid 1000) with the working directory `/app`.
[COMPATIBILITY.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/COMPATIBILITY.md#container-base-image)
lists the tags.

The minimal shape of a service image (for an image you deploy, copy
[`examples/hello_world_service/Dockerfile`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/examples/hello_world_service/Dockerfile)
instead: it keeps `/app` read-only for the service user, clears the base
image's labels and removes pip at the end of the build):

```dockerfile
FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6
WORKDIR /app
USER root
ENV PIP_ROOT_USER_ACTION=ignore
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY main.py .
USER 1000:1000
CMD ["python", "main.py"]
```

- **The image tag is the SDK version on PyPI**: `:0.3.6` carries `kbm-ledsas-sdk`
  0.3.6. `latest` moves with every release; build on a version tag, and pin the
  digest as well (`:0.3.6@sha256:<digest>`, from `docker buildx imagetools
  inspect ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6`) for a reproducible
  build.
- **Never list `kbm-ledsas-sdk` in your `requirements.txt`** and never install it
  again in the image: the base image pins its SDK version, and a requirement for
  another version fails the build.
- **The image runs as the non-root user `kbmuser` (uid 1000) in `/app`.** Switch
  to `USER root` only to install your own dependencies, then back with the
  numeric `USER 1000:1000` (that is `kbmuser`): Kubernetes `runAsNonRoot` can
  only verify a numeric user, so it then starts the pod without `runAsUser`.
- **Alpine uses musl libc.** Packages that publish no `musllinux` wheels (for
  example torch, onnxruntime, tensorflow, opencv-python, scikit-learn) do not
  install on it; numpy, pandas, scipy, pillow, scikit-image, pyarrow and pydicom
  do.
- **Inside the container, `127.0.0.1` is the container itself.** Point
  `KBM_LEDSAS_RABBITMQ_URL` and the `BlobEndpoint` at addresses the container
  can reach, use `amqps://` for any broker that is not on loopback (an `amqp://`
  URL to another host is refused unless `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1`,
  which belongs on a local Docker network only), and set
  `KBM_LEDSAS_HEALTH_HOST=0.0.0.0` when the health endpoints are probed from
  outside the container. The example's
  [README](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/examples/hello_world_service/README.md#container-image)
  has a complete `docker run` line for its local stack.
- **Give it time to stop.** On SIGTERM the service stops taking commands and
  waits for the handlers already running (each bounded by
  `KBM_LEDSAS_HANDLER_TIMEOUT`); with none running it exits within about a
  second. `docker stop` kills the container after 10 seconds by default: set
  `docker stop -t`, Compose `stop_grace_period` or Kubernetes
  `terminationGracePeriodSeconds` above your longest handler run.

## Configuration

The SDK is configured through environment variables. The most common are below; see the [API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#environment-variables) for the complete list (including `KBM_LEDSAS_ALLOW_INSECURE_AMQP`, `KBM_LEDSAS_MAX_PAYLOAD_BYTES`, and other advanced settings):

| Variable | Description | Default |
|----------|-------------|---------|
| `KBM_LEDSAS_SERVICE_NAME` | Overrides the required `ServiceApp(service_name=...)` argument, which names the AMQP exchange and queue (a WARNING is logged when they differ); leave it unset, or keep it equal to the constructor value | *(optional)* |
| `KBM_LEDSAS_TENANT` | Tenant identifier, part of the AMQP exchange and queue names | *(optional)* |
| `KBM_LEDSAS_RABBITMQ_URL` | RabbitMQ connection URL (`amqp://` or `amqps://`) | *(required)* |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Azure Blob Storage connection string (with an `AccountKey` or a `SharedAccessSignature`) | *(required)* |
| `KBM_LEDSAS_CONTAINER` | Informational only: logged at startup; the upload methods always take an explicit `container` | `dev` |
| `KBM_LEDSAS_ALLOWED_CONTAINERS` | Comma-separated container names that `ctx.blob` may read and write (for example `input,results`); any other container is refused with `errors.ContainerNotAllowed` (a `Permanent` reply, not retried) before anything is sent to storage. An empty item or an invalid container name stops the start | *(unset: no restriction)* |
| `KBM_LEDSAS_PREFETCH` | AMQP prefetch count | `10` |
| `KBM_LEDSAS_CONCURRENCY` | Maximum concurrent handlers | `4` |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | Per-handler timeout in seconds (`0` disables; at most 86400) | `1800` |
| `KBM_LEDSAS_MAX_RETRIES` | Retries for transient failures before the command is dead-lettered | `3` |
| `KBM_LEDSAS_GENERIC_ERRORS` | When `true`, callers get a generic error message unless the handler set `user_message` | `false` |
| `KBM_LEDSAS_CONNECT_TIMEOUT` | Seconds one connection attempt to the broker may take (`1` to `300`) | `15` |
| `KBM_LEDSAS_HEARTBEAT` | AMQP heartbeat interval in seconds asked of the broker (`0` to `600`, `0` off; a valid `?heartbeat=` in the broker URL wins, an invalid one is refused at start). Blocking code that holds the event loop longer than about two intervals loses the connection: run blocking work with `await asyncio.to_thread(...)` | `60` |
| `KBM_LEDSAS_LOG_LEVEL` | Log level | `INFO` |
| `KBM_LEDSAS_LOG_FORMAT` | Log format (`json` or `text`) | `json` |
| `KBM_LEDSAS_DEBUG` | When `1`, a configuration error at startup prints the full traceback instead of one line | `0` |
| `KBM_LEDSAS_HEALTH_HOST` | Health server bind host | `127.0.0.1` |
| `KBM_LEDSAS_HEALTH_PORT` | Health server port (`0` disables; a port in use stops the service at start) | `8090` |
| `KBM_LEDSAS_DEPLOYMENT_ID` | Build/commit id surfaced as `deployment_id` in `/health` and `/ready`; takes precedence over its older alias `KBM_LASTCOMMITID`. The SDK sets neither; set one at deploy time, for example to the commit id | *(optional)* |
| `KBM_LASTCOMMITID` | Older alias of `KBM_LEDSAS_DEPLOYMENT_ID`, read only when `KBM_LEDSAS_DEPLOYMENT_ID` is unset or blank. Prefer `KBM_LEDSAS_DEPLOYMENT_ID` | *(optional)* |

A `KBM_LEDSAS_*` variable the SDK does not know (a typo, for example) is reported
at startup with one WARNING.

## Documentation

- [Upgrade guide](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/UPGRADING.md) — what to do when upgrading (start here)
- [API Reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md)
- [Example service](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/v0.3.6/examples/hello_world_service)
- [Compatibility & supported versions](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/COMPATIBILITY.md)
- [Changelog](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CHANGELOG.md) — full detail of every change

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CONTRIBUTING.md) first — it covers
the development setup, how to run the tests, the coding style, and the **Developer Certificate
of Origin (DCO)** sign-off we require on every commit. Also see our
[Code of Conduct](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CODE_OF_CONDUCT.md).

## Security

Please report vulnerabilities privately — see [SECURITY.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/SECURITY.md). Do **not** open a
public issue for security reports.

## License

Licensed under the [Apache License 2.0](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/LICENSE). Copyright © 2025-2026 KeborMed.
See [THIRD_PARTY_NOTICES.txt](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/THIRD_PARTY_NOTICES.txt) for third-party dependency licenses.

Maintained by **KeborMed** (the software's copyright holder) under the
[**Kubyk-Health**](https://github.com/Kubyk-Health) GitHub organization.
