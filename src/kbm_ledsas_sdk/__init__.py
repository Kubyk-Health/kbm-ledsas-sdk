"""
KeborMed LEDSAS SDK - Python client for Local External Data Sources Action Service.

The SDK lets you build LEDSAS data-processing services in Python. A
service registers handler functions; the SDK
consumes commands from RabbitMQ, runs the handler, and
(optionally) publishes the response back to the caller.

Quick Start:
    import logging
    from kbm_ledsas_sdk import ServiceApp, errors
    from kbm_ledsas_sdk.models import BlobRef

    logging.basicConfig(level=logging.INFO)

    app = ServiceApp(service_name="csv-processor")

    @app.handler("ProcessCSV")
    async def handle(ctx, req: dict) -> dict:
        csv_uri = req.get("csv_uri")
        if not csv_uri:
            # user_message is what the caller sees; the internal
            # `message` always goes to logs.
            raise errors.Permanent(
                "Validation: csv_uri missing",
                user_message="csv_uri is required",
            )

        text = await ctx.blob.download_text(csv_uri)
        # Take the container from BlobRef, never from a hand-split string:
        # with blob versioning enabled the URI carries a ?versionId= query.
        container = BlobRef.from_uri(csv_uri).container
        out = await ctx.blob.upload_json(
            container=container,
            obj={"rows": len(text.splitlines()) - 1},
            # idempotency_key (not message_id) so a DLQ replay of the
            # same logical request overwrites the same blob. overwrite=True
            # is required for that pattern — without it, a replay would
            # fail because the blob already exists.
            path=f"result-{ctx.idempotency_key}.json",
            overwrite=True,
        )
        return {"result_uri": out.uri}

    # Optional liveness / readiness hooks. The SDK already exposes
    # sensible defaults; these layer on top. Readiness only decides what
    # /ready answers; it does not hold back commands, so do warm-up work
    # (loading a model, ...) before app.run().
    @app.liveness_check("worker_alive")
    def _():
        return True

    if __name__ == "__main__":
        app.run()

Run with:
    export KBM_LEDSAS_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
    export KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;..."
    python my_service.py

The service also exposes liveness/readiness HTTP endpoints at
``http://127.0.0.1:${KBM_LEDSAS_HEALTH_PORT:-8090}/health`` and ``/ready``
(loopback only by default; set ``KBM_LEDSAS_HEALTH_HOST=0.0.0.0`` to
bind on all interfaces). See the release tarball's
``SDK_API_REFERENCE.md`` for the full API.
"""

from . import models
from .app import ServiceApp
from .models import errors
from .runtime.context import ExecutionContext
from .utils.logging import json_log_formatter

__version__ = "0.3.5"

__all__ = [
    "ServiceApp",
    "ExecutionContext",
    "models",
    "errors",
    "json_log_formatter",
    "__version__",
]
