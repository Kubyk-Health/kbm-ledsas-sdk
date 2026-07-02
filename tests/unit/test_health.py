"""
Tests for the SDK health server: default checks, customer-registered checks,
disable via port=0, and the @app.liveness_check / @app.readiness_check
decorators on ServiceApp.
"""

from __future__ import annotations

import socket

import aiohttp
import pytest

from kbm_ledsas_sdk import ServiceApp
from kbm_ledsas_sdk.health.checks import CheckResult, HealthCheckRegistry
from kbm_ledsas_sdk.health.server import HealthServer


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _ok_readiness() -> CheckResult:
    return CheckResult(name="transport", healthy=True)


async def _bad_readiness() -> CheckResult:
    return CheckResult(name="transport", healthy=False, detail="not connected")


@pytest.fixture
async def started_server():
    """Helper: yields (server, base_url) and tears down."""

    async def _build(
        liveness: HealthCheckRegistry | None = None,
        readiness: HealthCheckRegistry | None = None,
        default_ready=_ok_readiness,
        verbose: bool = True,
    ):
        port = _free_port()
        server = HealthServer(
            service_name="test-svc",
            host="127.0.0.1",
            port=port,
            liveness_registry=liveness or HealthCheckRegistry(),
            readiness_registry=readiness or HealthCheckRegistry(),
            default_readiness=default_ready,
            verbose=verbose,
        )
        await server.start()
        return server, f"http://127.0.0.1:{port}"

    servers: list[HealthServer] = []

    async def make(**kwargs):
        server, url = await _build(**kwargs)
        servers.append(server)
        return server, url

    yield make

    for s in servers:
        await s.stop()


class TestHealthRegistry:
    @pytest.mark.asyncio
    async def test_empty_registry_runs_nothing(self):
        reg = HealthCheckRegistry()
        assert await reg.run_all() == []

    @pytest.mark.asyncio
    async def test_sync_check_truthy_is_healthy(self):
        reg = HealthCheckRegistry()
        reg.register("ok", lambda: True)
        results = await reg.run_all()
        assert len(results) == 1
        assert results[0].healthy is True

    @pytest.mark.asyncio
    async def test_async_check_supported(self):
        reg = HealthCheckRegistry()

        async def db_ping():
            return True

        reg.register("db", db_ping)
        results = await reg.run_all()
        assert results[0].healthy is True

    @pytest.mark.asyncio
    async def test_exception_reported_as_unhealthy(self):
        reg = HealthCheckRegistry()

        def boom():
            raise RuntimeError("kaboom")

        reg.register("boom", boom)
        results = await reg.run_all()
        assert results[0].healthy is False
        assert "kaboom" in results[0].detail

    def test_duplicate_name_rejected(self):
        reg = HealthCheckRegistry()
        reg.register("a", lambda: True)
        with pytest.raises(ValueError):
            reg.register("a", lambda: True)

    def test_empty_name_rejected(self):
        reg = HealthCheckRegistry()
        with pytest.raises(ValueError):
            reg.register("", lambda: True)


class TestHealthServer:
    @pytest.mark.asyncio
    async def test_default_health_returns_200(self, started_server):
        _, url = await started_server()
        async with aiohttp.ClientSession() as s, s.get(f"{url}/health") as r:
            assert r.status == 200
            body = await r.json()
            assert body["status"] == "healthy"
            assert body["service"] == "test-svc"
            assert "version" in body
            assert body["checks"]["process"] == "healthy"

    @pytest.mark.asyncio
    async def test_ready_uses_default_readiness(self, started_server):
        _, url = await started_server(default_ready=_ok_readiness)
        async with aiohttp.ClientSession() as s, s.get(f"{url}/ready") as r:
            assert r.status == 200
            assert (await r.json())["checks"]["transport"] == "healthy"

    @pytest.mark.asyncio
    async def test_ready_503_when_transport_not_ready(self, started_server):
        _, url = await started_server(default_ready=_bad_readiness)
        async with aiohttp.ClientSession() as s, s.get(f"{url}/ready") as r:
            assert r.status == 503
            body = await r.json()
            assert body["status"] == "unhealthy"
            assert "not connected" in body["checks"]["transport"]

    @pytest.mark.asyncio
    async def test_custom_readiness_check_layered_on_default(self, started_server):
        reg = HealthCheckRegistry()
        reg.register("db", lambda: True)
        reg.register("model", lambda: False)
        _, url = await started_server(readiness=reg)
        async with aiohttp.ClientSession() as s, s.get(f"{url}/ready") as r:
            assert r.status == 503
            body = await r.json()
            assert body["checks"]["db"] == "healthy"
            assert body["checks"]["model"] == "unhealthy"
            assert body["checks"]["transport"] == "healthy"  # default still ran

    @pytest.mark.asyncio
    async def test_livez_and_readyz_aliases(self, started_server):
        _, url = await started_server()
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/livez") as r:
                assert r.status == 200
            async with s.get(f"{url}/readyz") as r:
                assert r.status == 200

    @pytest.mark.asyncio
    async def test_default_minimal_body_omits_fingerprintable_fields(self, started_server):
        """Default (non-verbose) response is minimal — no version, no service, no check names.

        With KBM_LEDSAS_HEALTH_HOST=0.0.0.0 the endpoint is reachable
        from anywhere on the network. Default body must not give a
        fingerprinter useful detail.
        """
        # Override the fixture default of verbose=True.
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert r.status == 200
                body = await r.json()
                assert body == {"status": "healthy"}
            async with s.get(f"{url}/ready") as r:
                assert r.status == 200
                body = await r.json()
                assert body == {"status": "healthy"}

    @pytest.mark.asyncio
    async def test_default_minimal_body_lists_failing_check_names(self, started_server):
        """On failure the minimal body still reports which checks failed.

        Operators need *some* signal to diagnose unhealthy services.
        Only the names of failing checks are included — not their
        detail messages, and not the names of passing checks.
        """

        def _failing_default():
            from kbm_ledsas_sdk.health.checks import CheckResult

            async def _f():
                return CheckResult(name="transport", healthy=False, detail="not connected")

            return _f

        _, url = await started_server(verbose=False, default_ready=_failing_default())
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/ready") as r:
                assert r.status == 503
                body = await r.json()
                assert body["status"] == "unhealthy"
                assert body["failed"] == ["transport"]
                # No version, no service, no detail message in default mode
                assert "version" not in body
                assert "service" not in body
                assert "checks" not in body

    @pytest.mark.asyncio
    async def test_port_zero_disables_server(self):
        # port=0 → SDK never binds. Confirm start() is a no-op.
        server = HealthServer(
            service_name="off",
            host="127.0.0.1",
            port=0,
            liveness_registry=HealthCheckRegistry(),
            readiness_registry=HealthCheckRegistry(),
            default_readiness=_ok_readiness,
        )
        await server.start()
        assert server.bound_port is None
        await server.stop()  # safe to call when nothing started


class TestDeploymentId:
    """``KBM_LASTCOMMITID`` is surfaced as ``deployment_id`` in /health and
    /ready so an operator can confirm which build is live behind a probe."""

    @pytest.mark.asyncio
    async def test_deployment_id_present_in_minimal_body_when_set(
        self, started_server, monkeypatch
    ):
        monkeypatch.setenv("KBM_LASTCOMMITID", "abc1234")
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert r.status == 200
                assert await r.json() == {
                    "status": "healthy",
                    "deployment_id": "abc1234",
                }
            async with s.get(f"{url}/ready") as r:
                assert await r.json() == {
                    "status": "healthy",
                    "deployment_id": "abc1234",
                }

    @pytest.mark.asyncio
    async def test_deployment_id_present_in_verbose_body_when_set(
        self, started_server, monkeypatch
    ):
        monkeypatch.setenv("KBM_LASTCOMMITID", "deadbeef")
        _, url = await started_server(verbose=True)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert r.status == 200
                body = await r.json()
                assert body["deployment_id"] == "deadbeef"
                assert body["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_deployment_id_omitted_when_unset(self, started_server, monkeypatch):
        monkeypatch.delenv("KBM_LEDSAS_DEPLOYMENT_ID", raising=False)
        monkeypatch.delenv("KBM_LASTCOMMITID", raising=False)
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert await r.json() == {"status": "healthy"}

    @pytest.mark.asyncio
    async def test_deployment_id_omitted_when_blank(self, started_server, monkeypatch):
        monkeypatch.delenv("KBM_LEDSAS_DEPLOYMENT_ID", raising=False)
        monkeypatch.setenv("KBM_LASTCOMMITID", "   ")
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert await r.json() == {"status": "healthy"}

    @pytest.mark.asyncio
    async def test_deployment_id_prefers_namespaced_var(self, started_server, monkeypatch):
        """KBM_LEDSAS_DEPLOYMENT_ID wins over the KBM_LASTCOMMITID fallback."""
        monkeypatch.setenv("KBM_LEDSAS_DEPLOYMENT_ID", "namespaced")
        monkeypatch.setenv("KBM_LASTCOMMITID", "fallback")
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert await r.json() == {
                    "status": "healthy",
                    "deployment_id": "namespaced",
                }

    @pytest.mark.asyncio
    async def test_deployment_id_falls_back_to_lastcommitid(self, started_server, monkeypatch):
        """KBM_LASTCOMMITID is used when the namespaced var is unset."""
        monkeypatch.delenv("KBM_LEDSAS_DEPLOYMENT_ID", raising=False)
        monkeypatch.setenv("KBM_LASTCOMMITID", "fallback")
        _, url = await started_server(verbose=False)
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/health") as r:
                assert await r.json() == {
                    "status": "healthy",
                    "deployment_id": "fallback",
                }

    @pytest.mark.asyncio
    async def test_deployment_id_present_on_unhealthy_response(self, started_server, monkeypatch):
        monkeypatch.setenv("KBM_LASTCOMMITID", "v9")

        def _failing_default():
            from kbm_ledsas_sdk.health.checks import CheckResult

            async def _f():
                return CheckResult(name="transport", healthy=False, detail="x")

            return _f

        _, url = await started_server(verbose=False, default_ready=_failing_default())
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{url}/ready") as r:
                assert r.status == 503
                body = await r.json()
                assert body["status"] == "unhealthy"
                assert body["deployment_id"] == "v9"
                assert body["failed"] == ["transport"]


class TestServiceAppHealthDecorators:
    def test_liveness_decorator_registers(self):
        app = ServiceApp("svc")

        @app.liveness_check("alive")
        def _():
            return True

        assert "alive" in app.liveness_checks.names()

    def test_readiness_decorator_registers(self):
        app = ServiceApp("svc")

        @app.readiness_check("ready")
        async def _():
            return True

        assert "ready" in app.readiness_checks.names()

    def test_duplicate_check_name_rejected_at_decoration(self):
        app = ServiceApp("svc")

        @app.liveness_check("dup")
        def _():
            return True

        with pytest.raises(ValueError):

            @app.liveness_check("dup")
            def _2():
                return True
