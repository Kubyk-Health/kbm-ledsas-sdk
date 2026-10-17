"""
Liveness and readiness HTTP endpoints for the SDK.

The SDK runs a small aiohttp server alongside the message consumer so
external orchestrators and monitoring tools can answer two questions:

- /health  → is the process alive? (liveness)
- /ready   → should the instance be reported ready? (readiness; it only
  decides what /ready answers and does not hold back command delivery,
  so do warm-up work before app.run())

Both endpoints return JSON:

    {
      "status": "healthy" | "unhealthy",
      "service": "<service_name>",
      "version": "<sdk version>",
      "checks": { "<check_name>": "healthy" | "unhealthy: <reason>" }
    }

The SDK ships fully-functional defaults so the endpoints are useful even
when the customer registers nothing:

- Default liveness: trivially healthy (responding = process is alive).
- Default readiness: ``transport.is_ready()`` (AMQP connection open).

Customers can layer additional checks via decorators on ``ServiceApp``:

    @app.liveness_check("db_alive")
    async def _(): return await db.ping()

    @app.readiness_check("db_connected")
    async def _(): return await db.ping()

Set ``KBM_LEDSAS_HEALTH_PORT=0`` to disable the server entirely.
"""

from .checks import CheckResult, HealthCheckRegistry
from .server import HealthServer

__all__ = ["CheckResult", "HealthCheckRegistry", "HealthServer"]
