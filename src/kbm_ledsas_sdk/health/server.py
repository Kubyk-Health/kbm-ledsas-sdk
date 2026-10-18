"""
aiohttp server that exposes /health and /ready for the SDK.
"""

from __future__ import annotations

import asyncio
import errno
import json
import socket
import time
from collections.abc import Awaitable, Callable
from http import HTTPStatus

from aiohttp import hdrs, web, web_protocol
from aiohttp.http_exceptions import HttpProcessingError

from ..runtime.security import _read_env
from ..utils.logging import masked_logger
from .checks import CheckResult, HealthCheckRegistry

logger = masked_logger(__name__)
# aiohttp's own request-handling records (passed to aiohttp as its logger):
# a child of the SDK logger, so they use the SDK's log format and level.
http_logger = masked_logger(f"{__name__}.http")

ReadinessSignal = Callable[[], Awaitable[CheckResult]]
LivenessSignal = Callable[[], Awaitable[CheckResult]]


# Response shaping, applied to EVERY response the health server sends:
#
# - No ``Server: Python/3.x aiohttp/X.Y.Z`` header. aiohttp adds it while
#   preparing each response (``headers.setdefault``); ``_NoServerHeader*``
#   responses drop it right after that step, which also covers the
#   responses aiohttp builds itself.
# - JSON bodies: ``{"status": "error", "code": <status>, "message": ...}``
#   for unknown paths (404), other methods (405) and requests aiohttp
#   cannot parse (400: an over-long request line or header, a bad
#   Content-Length, ...). aiohttp's own 400 is plain text that echoes the
#   rejected request bytes; ``_HealthRequestHandler`` replaces it, logs it
#   as one DEBUG line (no traceback) and closes the connection.
#
# The server is aiohttp's low-level ``web.Server`` with a small router, so
# no application-level hook is needed for either.

_ALLOWED_METHODS = ("GET", "HEAD")

# How long stop() waits for probe requests still being answered (seconds).
# aiohttp waits this long for a running request, then cancels it and waits
# this long again, so stop() takes at most about twice this. Each check has
# its own time limit (checks.DEFAULT_CHECK_TIMEOUT_S, 2 s), so a probe
# normally ends within the first wait; this bounds the wait for a check
# registered with a longer limit (aiohttp's default is 60 s).
SHUTDOWN_TIMEOUT_S = 2.0

# A connection that has not sent a complete request within this many
# seconds is closed: one that sends nothing, one that sends part of a
# request and stops (a "slowloris" client), and an idle keep-alive
# connection after a request. aiohttp's default is about an hour, so on a
# network-reachable port (KBM_LEDSAS_HEALTH_HOST=0.0.0.0) anyone who can
# connect could hold connections, and their memory, indefinitely. A probe
# sends its whole request at once and gets its answer within the check
# time limits.
IDLE_CONNECTION_TIMEOUT_S = 10.0

# At most this many connections are open at a time. A connection made
# above it closes the oldest connection that is not answering a request
# (so idle connections held by a client cannot lock the probes out); only
# when every other connection is answering one is the new connection
# closed. One WARNING a minute while that happens. Probes use one short
# connection each, far below the limit.
MAX_CONNECTIONS = 100
_LIMIT_WARNING_INTERVAL_S = 60.0


class HealthServerStartFailure(RuntimeError):
    """The health server could not listen on its configured address
    (``KBM_LEDSAS_HEALTH_HOST`` / ``KBM_LEDSAS_HEALTH_PORT``). Logged once,
    as an ERROR naming the setting to change, where it was detected; the
    service then stops at start (exit code 1)."""


def _bind_failure(host: str, port: int, error: OSError) -> str:
    """The startup ERROR for a health server that could not listen: the
    cause, and the setting to change for it."""
    where = (
        f"Could not start the health server on KBM_LEDSAS_HEALTH_PORT={port} "
        f"(KBM_LEDSAS_HEALTH_HOST={host})"
    )
    cause = getattr(error, "strerror", None) or str(error) or type(error).__name__
    if error.errno == errno.EADDRINUSE:
        return (
            f"{where}: the address is already in use ({cause}); another "
            f"process, or another instance of this service, listens on it. "
            f"Set KBM_LEDSAS_HEALTH_PORT to a free port, or to 0 to run "
            f"without health endpoints."
        )
    if (
        isinstance(error, socket.gaierror)
        or error.errno == errno.EADDRNOTAVAIL
        # asyncio's message when every address of the host failed with
        # EADDRNOTAVAIL (it drops the errno).
        or "could not bind on any address" in str(error)
    ):
        return (
            f"{where}: the host is not an address of this machine or does "
            f"not resolve ({cause}). Set KBM_LEDSAS_HEALTH_HOST to an "
            f"address of this machine (0.0.0.0 for every interface, "
            f"127.0.0.1 for the loopback only)."
        )
    if error.errno in (errno.EACCES, errno.EPERM):
        return (
            f"{where}: permission denied ({cause}); a port below 1024 needs "
            f"privileges the service does not have. Set "
            f"KBM_LEDSAS_HEALTH_PORT to a port above 1023, or to 0 to run "
            f"without health endpoints."
        )
    return (
        f"{where}: {cause}. Check KBM_LEDSAS_HEALTH_HOST and "
        f"KBM_LEDSAS_HEALTH_PORT, or set KBM_LEDSAS_HEALTH_PORT=0 to run "
        f"without health endpoints."
    )


def _host_port(host: str, port: int) -> str:
    """``host:port`` as written in a URL: an IPv6 address in brackets
    (``[::1]:8090``), so the logged ``http://`` address can be used as is."""
    if ":" in host and not host.startswith("["):
        return f"[{host}]:{port}"
    return f"{host}:{port}"


class _NoServerHeaderResponse(web.Response):
    """A response sent without aiohttp's fingerprinting ``Server`` header."""

    async def _prepare_headers(self) -> None:
        await super()._prepare_headers()
        self.headers.pop(hdrs.SERVER, None)


def _json_response(body: dict, status: int) -> web.Response:
    return _NoServerHeaderResponse(
        text=json.dumps(body), status=status, content_type="application/json"
    )


def _error_body(status: int, **extra: object) -> dict:
    try:
        phrase = HTTPStatus(status).phrase
    except ValueError:
        phrase = "Error"
    return {"status": "error", "code": status, "message": phrase, **extra}


class _HealthRequestHandler(web_protocol.RequestHandler):
    """aiohttp's per-connection protocol, with its error response replaced
    (see the module notes above)."""

    __slots__ = ()

    def handle_error(
        self,
        request: web.BaseRequest,
        status: int = 500,
        exc: BaseException | None = None,
        message: str | None = None,
    ) -> web.StreamResponse:
        if isinstance(exc, HttpProcessingError) or 400 <= status < 500:
            # A request aiohttp could not parse (scanners, broken clients):
            # expected on a network-reachable port, not an SDK fault.
            http_logger.debug(
                "Health server rejected a malformed HTTP request from %s " "(%s, HTTP %d)",
                request.remote,
                type(exc).__name__ if exc is not None else "error",
                status,
            )
        else:
            http_logger.error(
                "Health server failed to answer a request from %s",
                request.remote,
                exc_info=exc,
            )
        if request.writer.output_size > 0:
            raise ConnectionError(
                "Response is sent already, cannot send another response " "with the error message"
            )
        resp = _json_response(_error_body(status), status)
        resp.force_close()
        return resp


class _HealthHttpServer(web.Server):
    """``web.Server`` whose connections use :class:`_HealthRequestHandler`,
    with at most ``MAX_CONNECTIONS`` open connections."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._last_limit_warning = float("-inf")
        self.connections_refused = 0
        self.connections_evicted = 0

    def connection_made(self, handler: web_protocol.RequestHandler, transport) -> None:
        super().connection_made(handler, transport)
        open_now = [h for h in self._connections if getattr(h, "_force_close", False) is not True]
        if len(open_now) <= MAX_CONNECTIONS:
            return
        # Over the limit: close the oldest connection that is not answering
        # a request (the dict keeps the order the connections were made),
        # or the new one if every other one is busy, so idle connections
        # held by a client cannot keep the probes out. An older
        # connection's request-handler task started long ago: it is closed
        # at once. The new one is closed with a deferred force_close: closed
        # right here, on Python 3.11 (whose tasks do not start eagerly)
        # connection_lost ran first and the task then failed with an
        # AssertionError that asyncio logs as "Task exception was never
        # retrieved" with a traceback. force_close cancels the handler's
        # wait for a request and closes the transport, so the task ends as
        # for a client that disconnects; the connection is still closed
        # before anything is read.
        victim = next(
            (
                h
                for h in open_now
                if h is not handler and getattr(h, "_request_in_progress", False) is not True
            ),
            None,
        )
        if victim is None:
            victim = handler
            self.connections_refused += 1
        else:
            self.connections_evicted += 1
        now = time.monotonic()
        if now - self._last_limit_warning >= _LIMIT_WARNING_INTERVAL_S:
            self._last_limit_warning = now
            logger.warning(
                "Health server has %d open connections (the limit); the oldest "
                "idle ones are closed to accept new connections (%d closed so "
                "far, %d new connections refused while all were busy)",
                MAX_CONNECTIONS,
                self.connections_evicted,
                self.connections_refused,
            )
        if victim is handler:
            asyncio.get_running_loop().call_soon(handler.force_close)
        else:
            victim.force_close()

    def __call__(self) -> web_protocol.RequestHandler:
        handler = super().__call__()
        try:
            handler.__class__ = _HealthRequestHandler
        except TypeError:  # pragma: no cover - an aiohttp with another layout
            pass
        return handler


class HealthServer:
    """
    HTTP server exposing two endpoints:

    - GET /health → 200 if all liveness checks pass; 503 otherwise.
    - GET /ready  → 200 if all readiness checks pass; 503 otherwise.

    The server is started before the SDK runs any command and stopped
    during SDK shutdown. If the configured address cannot be bound (port in
    use, permission denied, a host that is not an address of this machine),
    :meth:`start` logs one ERROR naming the setting to change and raises
    :class:`HealthServerStartFailure`: the service stops at start, so its
    probes never report another process's answer as its health.
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
        emit_deployment_id: bool = True,
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
        # Whether to include deployment_id (from KBM_LEDSAS_DEPLOYMENT_ID,
        # else KBM_LASTCOMMITID); ServiceApp decides. Defaults to True.
        self.emit_deployment_id = emit_deployment_id

        # /health and /ready, plus the Kubernetes-conventional /livez and
        # /readyz aliases. GET and HEAD only.
        self._routes = {
            "/health": self._handle_liveness,
            "/livez": self._handle_liveness,
            "/ready": self._handle_readiness,
            "/readyz": self._handle_readiness,
        }

        self._runner: web.ServerRunner | None = None
        self._site: web.TCPSite | None = None

    async def start(self) -> None:
        if self.port == 0:
            logger.info("Health server disabled (KBM_LEDSAS_HEALTH_PORT=0)")
            return
        server = _HealthHttpServer(
            self._dispatch,
            access_log=None,
            logger=http_logger,
            keepalive_timeout=IDLE_CONNECTION_TIMEOUT_S,
        )
        self._runner = web.ServerRunner(server, shutdown_timeout=SHUTDOWN_TIMEOUT_S)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        try:
            await self._site.start()
        except OSError as e:
            # Port in use, an address that is not this machine's, permission
            # denied: a startup failure, not a service running without its
            # endpoints. Otherwise whatever reads this port (the probes)
            # would read whatever else listens on it and take its answer as
            # this service's health.
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            message = _bind_failure(self.host, self.port, e)
            logger.error(message)
            raise HealthServerStartFailure(message) from None
        logger.info(
            "Health server listening on http://%s (/health, /ready)",
            _host_port(self.host, self.port),
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
        """Port the server is listening on; ``None`` if not listening.

        Returns ``self.port`` when ``start()`` successfully bound the
        socket (the configured port equals the actual port because
        ``TCPSite`` is initialised with an explicit port — no
        ephemeral-port binding). Returns ``None`` when the server has
        not been started, was disabled via ``KBM_LEDSAS_HEALTH_PORT=0``,
        has been stopped, or when ``start()`` failed to bind (it then
        raised ``HealthServerStartFailure``) — in all of which cases
        ``self._site`` is ``None``.

        ``ServiceApp.health_server_running`` is the public way to query
        this from outside the SDK.
        """
        if self._site is None:
            return None
        return self.port

    async def _dispatch(self, request: web.BaseRequest) -> web.StreamResponse:
        """Route a request: the four health paths answer GET and HEAD; any
        other method there is a JSON 405, any other path a JSON 404."""
        handler = self._routes.get(request.path)
        if handler is None:
            return _json_response(_error_body(404), 404)
        if request.method not in _ALLOWED_METHODS:
            response = _json_response(_error_body(405, allowed=list(_ALLOWED_METHODS)), 405)
            response.headers[hdrs.ALLOW] = ", ".join(_ALLOWED_METHODS)
            return response
        return await handler(request)

    async def _handle_liveness(self, request: web.BaseRequest) -> web.Response:
        checks: list[CheckResult] = [await self.default_liveness()]
        checks.extend(await self.liveness_registry.run_all())
        return _build_response(
            self.service_name,
            checks,
            verbose=self.verbose,
            emit_deployment_id=self.emit_deployment_id,
        )

    async def _handle_readiness(self, request: web.BaseRequest) -> web.Response:
        checks: list[CheckResult] = [await self.default_readiness()]
        checks.extend(await self.readiness_registry.run_all())
        return _build_response(
            self.service_name,
            checks,
            verbose=self.verbose,
            emit_deployment_id=self.emit_deployment_id,
        )


async def _trivial_liveness() -> CheckResult:
    # Reaching here proves the asyncio loop is responsive.
    return CheckResult(name="process", healthy=True)


def _deployment_id() -> str | None:
    """Deployed-service identifier: ``KBM_LEDSAS_DEPLOYMENT_ID``, else
    ``KBM_LASTCOMMITID`` (the same identifier under another name, read when
    ``KBM_LEDSAS_DEPLOYMENT_ID`` is unset or blank).

    Returned as ``deployment_id`` in every /health and /ready body so an
    operator can confirm which build is actually live behind a probe. Read
    from the environment at request time (the value is stable for the life
    of the process) with the settings reader: surrounding whitespace and
    one matched pair of surrounding quotes are removed. Returns ``None``
    when neither variable holds a value, in which case the key is omitted
    entirely, keeping the minimal default response fingerprint-free
    outside a real deployment.
    """
    return _read_env("KBM_LEDSAS_DEPLOYMENT_ID") or _read_env("KBM_LASTCOMMITID") or None


def _build_response(
    service_name: str,
    checks: list[CheckResult],
    verbose: bool = False,
    emit_deployment_id: bool = True,
) -> web.Response:
    all_healthy = all(c.healthy for c in checks)
    status = "healthy" if all_healthy else "unhealthy"
    deployment_id = _deployment_id() if emit_deployment_id else None

    if not verbose:
        # Minimal default — no fingerprintable detail. Failing checks
        # do report names so an operator can diagnose, but no
        # version/service identifiers. The deploy id is the one exception:
        # it is present only when set at deploy time, so it never
        # leaks build info in a dev/local run where the var is unset.
        body = {"status": status}
        if deployment_id is not None:
            body["deployment_id"] = deployment_id
        if not all_healthy:
            body["failed"] = [c.name for c in checks if not c.healthy]
        return _json_response(body, 200 if all_healthy else 503)

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
    return _json_response(body, 200 if all_healthy else 503)
