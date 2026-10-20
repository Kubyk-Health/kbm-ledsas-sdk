# KeborMed LEDSAS SDK (Python)

[![CI](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/Kubyk-Health/kbm-ledsas-sdk/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/kbm-ledsas-sdk.svg)](https://pypi.org/project/kbm-ledsas-sdk/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/LICENSE)

A Python SDK for building **LEDSAS** (Local External Data Sources Action Service)
data-processing services. You write async handlers; the SDK consumes command messages from
**RabbitMQ**, runs your handler, reads and writes **Azure Blob Storage**, and replies to the
caller. It handles acknowledgement, retries, timeouts, dead-lettering, structured logging and
health endpoints.

## Features

- **Handlers** registered with `@app.handler`, run with bounded concurrency.
- **Blob helpers**: `download_bytes/text/json/stream`, `upload_bytes/text/json/stream`.
- **Typed models** (Pydantic v2) for envelopes and blob references.
- **Health & readiness** endpoints (`/health`, `/ready`, `/livez`, `/readyz`).
- **Type hints** (`py.typed`) on the public API, Python 3.11+.

## Installation

```bash
pip install kbm-ledsas-sdk
```

Requires Python 3.11 or newer. From a clone: `pip install -e ".[dev]"`.

## Quickstart

Save this as `my_service.py`:

```python
import logging
import os

import kbm_ledsas_sdk
from kbm_ledsas_sdk import ServiceApp, errors
from kbm_ledsas_sdk.models import BlobRef

# Your own records as JSON, like the SDK's, or as text with KBM_LEDSAS_LOG_FORMAT=text.
handler = logging.StreamHandler()
if os.environ.get("KBM_LEDSAS_LOG_FORMAT", "").strip().strip("\"'").lower() == "text":
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
else:
    handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

app = ServiceApp(service_name="csv-processor")

# Callers choose the blob URIs: read only from the input container, write only where you decide.
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
    # Parse with BlobRef, never by hand: a versioned blob's URI ends in ?versionId=...
    if BlobRef.from_uri(csv_uri).container != INPUT_CONTAINER:
        raise errors.Permanent(
            "Validation: csv_uri outside the input container",
            user_message=f"csv_uri must be in the {INPUT_CONTAINER} container",
        )

    text = await ctx.blob.download_text(csv_uri)
    out = await ctx.blob.upload_json(
        container=RESULTS_CONTAINER,
        obj={"rows": len(text.splitlines()) - 1},
        # idempotency_key + overwrite=True: a redelivery overwrites the same blob.
        path=f"result-{ctx.idempotency_key}.json",
        overwrite=True,
    )
    return {"result_uri": out.uri}


# Optional extra health check (the SDK has defaults). Do warm-up work before app.run().
@app.liveness_check("worker_alive")
def _() -> bool:
    return True


if __name__ == "__main__":
    app.run()
```

The handler reads only from `INPUT_CONTAINER` and writes only to `RESULTS_CONTAINER`: anyone who
can publish a command chooses the blob URIs in it. To hold every handler to a fixed set of
containers, also set `KBM_LEDSAS_ALLOWED_CONTAINERS` (for example `test-container,results`). It
limits what the service's storage credential is used for; it does not authenticate callers. See
"Security model" in the
[API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#security-model).

Run it against a local RabbitMQ and Azurite (the Azure Blob Storage emulator), from the directory
where you saved `my_service.py`:

```bash
docker run -d --name ledsas-quickstart-rabbitmq -p 127.0.0.1:5672:5672 rabbitmq:4.1-management-alpine
docker run -d --name ledsas-quickstart-azurite -p 127.0.0.1:10000:10000 \
  mcr.microsoft.com/azure-storage/azurite:3.34.0 azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
# Wait until RabbitMQ has started (at most 2 minutes; stops at once if the container is not running):
for i in $(seq 60); do
  docker logs ledsas-quickstart-rabbitmq 2>&1 | grep -q "Server startup complete" && break
  [ "$(docker inspect -f '{{.State.Running}}' ledsas-quickstart-rabbitmq)" = true ] || break
  sleep 2
done

# Azurite's documented development account (devstoreaccount1) and its
# well-known public key: not a secret, it only works against the emulator.
export KBM_LEDSAS_RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/"
export KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
python my_service.py
```

If a `docker run` reports a port already in use, stop what listens on it (or map another host
port and use it in the URL or the `BlobEndpoint`). Remove the two containers afterwards with
`docker rm -f -v ledsas-quickstart-rabbitmq ledsas-quickstart-azurite`. The health endpoints are
at `http://127.0.0.1:8090/health` and `/ready`.

### Send it a command

Save this as `send_command.py` and run it in a second shell with the same two `export` lines
while `my_service.py` runs. It creates the `test-container` and `results` containers, uploads
`test-container/sample.csv`, publishes `ProcessCSV` to the exchange `cmd.csv-processor.v1` with a
reply exchange of its own (bound for routing key `response`), prints the reply and deletes the
reply exchange and queue (also on Ctrl+C or a kill). If the service is not running, or replies
with an error, it prints one `error:` line and exits with status 1.

```python
import asyncio
import contextlib
import json
import logging
import os
import signal
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
    with contextlib.suppress(NotImplementedError):  # a kill cleans up like Ctrl+C (not on Windows)
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, asyncio.current_task().cancel)
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
        # Should the script be killed outright, the broker deletes the queue WAIT + 60 s
        # after its last use, and the auto_delete exchange with it.
        rq = await channel.declare_queue(
            f"{reply_exchange}-q", durable=True, arguments={"x-expires": (WAIT + 60) * 1000}
        )
        rx = None
        try:
            rx = await channel.declare_exchange(
                reply_exchange, aio_pika.ExchangeType.TOPIC, durable=True, auto_delete=True
            )
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
                            break
            except TimeoutError:
                sys.exit(
                    f"error: no reply within {WAIT} s "
                    "(is my_service.py running, and are the broker and storage reachable?)"
                )
        finally:
            await rq.delete(if_unused=False, if_empty=False)
            if rx is not None:
                await rx.delete()
    error = reply["payload"].get("error")
    if isinstance(error, dict) and {"code", "message", "retryable"} <= error.keys():
        sys.exit(f"error: {error['code']}: {' '.join(str(error['message']).split())}")
    print(reply["payload"])  # {'result_uri': 'azblob://results/result-...json'}


try:
    asyncio.run(main())
except (AMQPError, AzureError, OSError, LookupError, ValueError) as error:
    sys.exit(f"error: {type(error).__name__}: {error}")
except (KeyboardInterrupt, asyncio.CancelledError):
    sys.exit("interrupted")
```

## Local development

The example service in
[`examples/hello_world_service/`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/v0.3.6/examples/hello_world_service) has a handler, a
sender, tests, a `Dockerfile` and a local stack. From a clone of this repository:

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

Without a clone, take the example from the release's source archive and install the SDK from
PyPI instead of `pip install -e ../..`, then continue with the `docker compose` line:

```bash
curl -L https://github.com/Kubyk-Health/kbm-ledsas-sdk/archive/refs/tags/v0.3.6.tar.gz | tar -xz
cd kbm-ledsas-sdk-0.3.6/examples/hello_world_service
pip install kbm-ledsas-sdk==0.3.6
```

## Container base image

[`ghcr.io/kebormed/kbm-ledsas-base-direct`](https://github.com/orgs/KeborMed/packages/container/package/kbm-ledsas-base-direct)
is Python 3.11 on Alpine Linux with `kbm-ledsas-sdk` of the tag's version installed and pinned
(`PIP_CONSTRAINT`), running as `kbmuser` (uid 1000) in `/app`. The minimal shape of an image for the quickstart's
`my_service.py`, next to a `requirements.txt` with your service's own dependencies (empty for the
quickstart; for production, copy the example's
[`Dockerfile`](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/examples/hello_world_service/Dockerfile)):

```dockerfile
FROM ghcr.io/kebormed/kbm-ledsas-base-direct:0.3.6
WORKDIR /app
USER root
ENV PIP_ROOT_USER_ACTION=ignore
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY my_service.py .
USER 1000:1000
CMD ["python", "my_service.py"]
```

- Never list `kbm-ledsas-sdk` in your `requirements.txt`: the image pins its SDK version.
- Alpine uses musl libc: packages without `musllinux` wheels (torch, onnxruntime, ...) do not install.
- In a container, point the broker and storage at reachable hosts, use `amqps://` off loopback
  (or `KBM_LEDSAS_ALLOW_INSECURE_AMQP=1` on a local Docker network), and set
  `KBM_LEDSAS_HEALTH_HOST=0.0.0.0` for probes from outside.
- Set `docker stop -t`, Compose `stop_grace_period` or Kubernetes `terminationGracePeriodSeconds`
  above your longest handler run.
- A failed start (for example the broker not reachable) exits with code 1 and is not retried: run
  the service under a restart policy (`docker run --restart on-failure`, Compose
  `restart: on-failure`, Kubernetes `restartPolicy`).
- Under a memory limit, lower `KBM_LEDSAS_MAX_PAYLOAD_BYTES` (for example `1048576`; send large data
  as blob references) or `KBM_LEDSAS_PREFETCH`: the defaults allow about 4 GiB of parsed command
  bodies, and a start under a smaller limit logs a WARNING.

[COMPATIBILITY.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/COMPATIBILITY.md#container-base-image) lists the tags and digests.

## Configuration

Environment variables; the
[API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#environment-variables) has the complete
list. A `KBM_LEDSAS_*` variable the SDK does not know is reported at startup.

| Variable | Description | Default |
|----------|-------------|---------|
| `KBM_LEDSAS_RABBITMQ_URL` | Broker URL (`amqp://` or `amqps://`) | *(required)* |
| `KBM_LEDSAS_BLOB_CONN_STRING` | Storage connection string (`AccountKey` or `SharedAccessSignature`) | *(required)* |
| `KBM_LEDSAS_SERVICE_NAME` | Overrides `ServiceApp(service_name=...)`, which names the exchange and queue | *(optional)* |
| `KBM_LEDSAS_TENANT` | Tenant, part of the exchange and queue names | *(optional)* |
| `KBM_LEDSAS_ALLOWED_CONTAINERS` | Containers `ctx.blob` may use; others raise `errors.ContainerNotAllowed` | *(unset: no restriction)* |
| `KBM_LEDSAS_CONTAINER` | Informational only (logged); uploads always take a `container` | `dev` |
| `KBM_LEDSAS_PREFETCH` | AMQP prefetch count | `10` |
| `KBM_LEDSAS_CONCURRENCY` | Maximum concurrent handlers | `4` |
| `KBM_LEDSAS_HANDLER_TIMEOUT` | Per-handler timeout in seconds (`0` off) | `1800` |
| `KBM_LEDSAS_MAX_RETRIES` | Retries before a command is dead-lettered | `3` |
| `KBM_LEDSAS_GENERIC_ERRORS` | `true`: callers get a generic message unless `user_message` is set | `false` |
| `KBM_LEDSAS_CONNECT_TIMEOUT` | Seconds for one broker connection attempt | `15` |
| `KBM_LEDSAS_HEARTBEAT` | AMQP heartbeat in seconds (`0` off); run blocking work with `asyncio.to_thread` | `60` |
| `KBM_LEDSAS_LOG_LEVEL` | Log level | `INFO` |
| `KBM_LEDSAS_LOG_FORMAT` | `json` or `text` | `json` |
| `KBM_LEDSAS_DEBUG` | `1`: a configuration error prints its traceback | `0` |
| `KBM_LEDSAS_HEALTH_HOST` | Health server bind address | `127.0.0.1` |
| `KBM_LEDSAS_HEALTH_PORT` | Health server port (`0` off) | `8090` |
| `KBM_LEDSAS_DEPLOYMENT_ID` | `deployment_id` in `/health` and `/ready`; wins over its older alias `KBM_LASTCOMMITID`. The SDK sets neither | *(optional)* |
| `KBM_LASTCOMMITID` | Older alias of `KBM_LEDSAS_DEPLOYMENT_ID`, read only when that is unset or blank | *(optional)* |

## Documentation

- [Upgrade guide](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/UPGRADING.md): what to do when upgrading (start here)
- [API reference](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md), including the
  [security model](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/docs/SDK_API_REFERENCE.md#security-model)
- [Changelog](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CHANGELOG.md)
- [Compatibility](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/COMPATIBILITY.md)
- [Example service](https://github.com/Kubyk-Health/kbm-ledsas-sdk/tree/v0.3.6/examples/hello_world_service)

## Contributing, security and license

See [CONTRIBUTING.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CONTRIBUTING.md) (commits need a DCO sign-off) and the
[Code of Conduct](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/CODE_OF_CONDUCT.md). Report vulnerabilities privately, never in a
public issue: see [SECURITY.md](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/SECURITY.md).

Licensed under the [Apache License 2.0](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/LICENSE). Copyright © 2025-2026 KeborMed,
which maintains the SDK under the [Kubyk-Health](https://github.com/Kubyk-Health) GitHub
organization. Third-party licenses: [THIRD_PARTY_NOTICES.txt](https://github.com/Kubyk-Health/kbm-ledsas-sdk/blob/v0.3.6/THIRD_PARTY_NOTICES.txt).
