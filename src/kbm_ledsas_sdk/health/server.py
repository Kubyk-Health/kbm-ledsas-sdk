"""
aiohttp server that exposes /health and /ready for the SDK.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable

from aiohttp import web

from .checks import CheckResult, HealthCheckRegistry

logger = logging.getLogger(__name__)

ReadinessSignal = Callable[[], Awaitable[CheckResult]]
LivenessSignal = Callable[[], Awaitable[CheckResult]]


# Two response-shaping hooks applied across
# every response served by the health server.
#
# Aiohttp's ``StreamResponse._start`` injects a default ``Server:
# Python/3.11 aiohttp/X.Y.Z`` header during header preparation. The
# fingerprintable string undoes the no-fingerprint guarantee the
# minimal response body establishes. We use the ``on_response_prepare``
# signal (which fires AFTER ``_start`` sets defaults but BEFORE the
# headers hit the wire) to pop ``Server`` — middleware runs too early
# and would be overwritten by setdefault.
#
# Aiohttp's default 404/405 responses are plaintext (e.g.
# "405: Method Not Allowed") — inconsistent with the JSON 200/503
# responses on /health and /ready. A middleware that catches the
# HTTPException subclasses converts them to JSON so the response-body
# contract is uniform across every path on this server.


async def _strip_server_header(request, response):
    """Pop the ``Server`` header just before headers go to the wire.

    Hooked into ``aiohttp.web.Application.on_response_prepare`` rather
    than middleware: ``_start`` sets ``Server`` via ``setdefault`` BEFORE
    middleware-returned responses are written, so a middleware-level
    ``response.headers.pop`` would be silently re-added. ``_start`` runs
    setdefaults, then awaits ``request._prepare_hook(self)`` (which
    fires this signal), then writes headers — so popping here sticks.
    """
    response.headers.pop("Server", None)


@web.middleware
async def _jsonify_error_responses(request, handler):
    """Re-shape 404 / 405 plaintext bodies into the JSON contract.

    The bodies for unknown routes and disallowed methods now match the
    shape downstream parsers see on the /health and /ready endpoints,
    so an ops dashboard that JSON-decodes every health-server response
    doesn't fall over on a stray ``HEAD /admin``.
    """
    try:
        return await handler(request)
    except web.HTTPNotFound:
        return web.json_response(
            {"status": "error", "code": 404, "message": "Not Found"},
            status=404,
        )
    except web.HTTPMethodNotAllowed as exc:
        return web.json_response(
            {
                "status": "error",
                "code": 405,
                "message": "Method Not Allowed",
                "allowed": sorted(exc.allowed_methods),
            },
            status=405,
        )


class HealthServer:
    """
    HTTP server exposing two endpoints:

    - GET /health → 200 if all liveness checks pass; 503 otherwise.
    - GET /ready  → 200 if all readiness checks pass; 503 otherwise.

    The server is started before the SDK begins consuming commands and
    stopped during SDK shutdown. If the configured port is already in use
    the server logs a warning and continues — the SDK never crashes the
    customer's service over a health-port collision.
    """

    def __init__(
        self,
        service_name: str,
        host: str,
        port: int,
        liveness_registry: HealthCheckRegistry,
        readiness_registry: HealthCheckRegistry,
        default_readiness: ReadinessSignal,
        default_liveness: LivenessSignal | None = None,
        verbose: bool = False,
    ) -> None:
        self.service_name = service_name
        self.host = host
        self.port = port
        self.liveness_registry = liveness_registry
        self.readiness_registry = readiness_registry
        self.default_readiness = default_readiness
        self.default_liveness = default_liveness or _trivial_liveness
        # By default, return minimal {"status": ...} body.
        # Verbose mode (opt-in via KBM_LEDSAS_HEALTH_VERBOSE=1) adds
        # service name, SDK version, and per-check status — useful for
        # development but fingerprintable when HEALTH_HOST=0.0.0.0.
        self.verbose = verbose

        # JSON-ify 404/405 via middleware
        # (so the body shape is uniform), and strip the fingerprintable
        # Server header via the on_response_prepare signal (the right
        # hook because aiohttp sets ``Server`` via setdefault inside
        # ``StreamResponse._start`` after middleware has already run —
        # popping in middleware would just get re-added).
        self.app = web.Application(
            middlewares=[_jsonify_error_responses],
        )
        self.app.on_response_prepare.append(_strip_server_header)
        self.app.router.add_get("/health", self._handle_liveness)
        self.app.router.add_get("/ready", self._handle_readiness)
        # Also expose the K8s-conventional names as aliases.
        self.app.router.add_get("/livez", self._handle_liveness)
        self.app.router.add_get("/readyz", self._handle_readiness)

        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        if self.port == 0:
            logger.info("Health server disabled (KBM_LEDSAS_HEALTH_PORT=0)")
            return
        self._runner = web.AppRunner(self.app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        try:
            await self._site.start()
        except OSError as e:
            # Port in use, permission denied, etc. — degrade gracefully.
            logger.warning(
                "Health server failed to bind %s:%d (%s). Endpoints unavailable.",
                self.host,
                self.port,
                e,
            )
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            return
        logger.info(
            "Health server listening on http://%s:%d (/health, /ready)",
            self.host,
            self.port,
        )

    async def stop(self) -> None:
        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    @property
    def bound_port(self) -> int | None:
        """Port the server is listening on; ``None`` if disabled or bind failed.

        Returns ``self.port`` when ``start()`` successfully bound the
        socket (the configured port equals the actual port because
        ``TCPSite`` is initialised with an explicit port — no
        ephemeral-port binding). Returns ``None`` when the server has
        not been started, was disabled via ``KBM_LEDSAS_HEALTH_PORT=0``,
        or when the bind failed with ``OSError`` (port in use,
        permission denied) — in all of which cases ``self._site`` is
        ``None``.

        ``ServiceApp.health_server_running`` is the public way to query
        this from outside the SDK.
        """
        if self._site is None:
            return None
        return self.port

    async def _handle_liveness(self, request: web.Request) -> web.Response:
        checks: list[CheckResult] = [await self.default_liveness()]
        checks.extend(await self.liveness_registry.run_all())
        return _build_response(
            self.service_name,
            checks,
            verbose=self.verbose,
        )

    async def _handle_readiness(self, request: web.Request) -> web.Response:
        checks: list[CheckResult] = [await self.default_readiness()]
        checks.extend(await self.readiness_registry.run_all())
        return _build_response(
            self.service_name,
            checks,
            verbose=self.verbose,
        )


async def _trivial_liveness() -> CheckResult:
    # Reaching here proves the asyncio loop is responsive.
    return CheckResult(name="process", healthy=True)


def _deployment_id() -> str | None:
    """Deployed-service identifier injected at deploy time.

    Read from ``KBM_LEDSAS_DEPLOYMENT_ID``, falling back to
    ``KBM_LASTCOMMITID`` when the namespaced variable is unset or blank.
    Returned as ``deployment_id`` in every /health and /ready body so an
    operator can confirm which build is actually live behind a probe. Read
    from the environment at request time (the value is stable for the life
    of the process). Returns ``None`` when neither variable is set or the
    value is blank — in which case the key is omitted entirely, keeping the
    minimal default response fingerprint-free outside a real deployment.
    """
    value = os.environ.get("KBM_LEDSAS_DEPLOYMENT_ID") or os.environ.get("KBM_LASTCOMMITID")
    if value is None:
        return None
    value = value.strip()
    return value or None


def _build_response(
    service_name: str,
    checks: list[CheckResult],
    verbose: bool = False,
) -> web.Response:
    all_healthy = all(c.healthy for c in checks)
    status = "healthy" if all_healthy else "unhealthy"
    deployment_id = _deployment_id()

    if not verbose:
        # Minimal default — no fingerprintable detail. Failing checks
        # do report names so an operator can diagnose, but no
        # version/service identifiers. The deploy id is the one exception:
        # it is present only when injected at deploy time, so it never
        # leaks build info in a dev/local run where the var is unset.
        body = {"status": status}
        if deployment_id is not None:
            body["deployment_id"] = deployment_id
        if not all_healthy:
            body["failed"] = [c.name for c in checks if not c.healthy]
        return web.json_response(body, status=200 if all_healthy else 503)

    # Lazy import to avoid a circular import at module load time
    # (kbm_ledsas_sdk/__init__.py imports ServiceApp which imports this module).
    from .. import __version__

    body = {
        "status": status,
        "service": service_name,
        "version": __version__,
        "checks": {c.name: c.to_response_value() for c in checks},
    }
    if deployment_id is not None:
        body["deployment_id"] = deployment_id
    return web.json_response(body, status=200 if all_healthy else 503)
