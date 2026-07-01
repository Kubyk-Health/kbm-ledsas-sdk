"""
Hello-world LEDSAS service — the smallest possible LEDSAS handler.

Replies ``{"greeting": "hello <name>"}`` (default ``"hello world"``) to
every ``SayHello`` command published on its command exchange
(``cmd.hello-world.v1`` when no tenant is set).

This file is the entire service. Install the SDK with
``pip install kbm-ledsas-sdk``; it talks straight to RabbitMQ + Azurite using
``KBM_LEDSAS_RABBITMQ_URL`` / ``KBM_LEDSAS_BLOB_CONN_STRING``. See README
"Run it locally".

Send a test command with ``scripts/send_hello.py``.
"""

import logging

from kbm_ledsas_sdk import ServiceApp

# The SDK configures its own kbm_ledsas_sdk.* loggers from
# KBM_LEDSAS_LOG_LEVEL / KBM_LEDSAS_LOG_FORMAT at startup; this basicConfig
# is for *our* logger (__main__). The azure line quiets the Azure SDK's
# chatty INFO-level HTTP logger (canonical pattern).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Must match KBM_LEDSAS_SERVICE_NAME in .env.example.
app = ServiceApp(service_name="hello-world")


@app.handler("SayHello")  # must match envelope.name in incoming messages
async def say_hello(ctx, req: dict) -> dict:
    # `name` is caller-controlled: cap its length, and never interpolate it
    # into the log message string (SDK_API_REFERENCE.md "Logging note" —
    # structured fields go in extra={...}).
    name = str(req.get("name") or "world")[:100]
    logger.info("Greeting requested", extra={"correlation_id": ctx.correlation_id})
    return {"greeting": f"hello {name}"}


if __name__ == "__main__":
    app.run()
