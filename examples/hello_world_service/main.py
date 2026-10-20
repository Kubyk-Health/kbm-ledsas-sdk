"""
Hello-world LEDSAS service — the smallest possible LEDSAS handler.

Replies ``{"greeting": "hello <name>"}`` (default ``"hello world"``) to
every ``SayHello`` command published on its command exchange
(``cmd.hello-world.v1`` when no tenant is set).

This file is the entire service. The container image gets the SDK from the
base image the Dockerfile builds FROM; a local venv installs it with
``pip install kbm-ledsas-sdk==0.3.6``. The SDK connects to RabbitMQ and
Azurite using ``KBM_LEDSAS_RABBITMQ_URL`` / ``KBM_LEDSAS_BLOB_CONN_STRING``.
See README "Run it locally".

Send a test command with ``scripts/send_hello.py``.
"""

import logging
import os

import kbm_ledsas_sdk
from kbm_ledsas_sdk import ServiceApp

# Our own log records (logger __main__) in the format KBM_LEDSAS_LOG_FORMAT
# names, as the SDK writes its own once app.run() starts: JSON (the default,
# extra= fields included) or text. The azure line quiets the Azure SDK's
# chatty INFO-level HTTP logger.
_handler = logging.StreamHandler()
if os.environ.get("KBM_LEDSAS_LOG_FORMAT", "").strip().strip("\"'").lower() == "text":
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
else:
    _handler.setFormatter(kbm_ledsas_sdk.json_log_formatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])
logging.captureWarnings(True)
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
