"""
Regression tests locking in specific SDK boundary behaviours.

These tests lock in specific boundary behaviours to guard against
regressions. Each test corresponds to one hardening fix.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.runtime.config import SDKConfig, _bool_env
from kbm_ledsas_sdk.runtime.security import (
    LOOPBACK_HOSTS,
    _is_truthy,
    check_topology_name_budget,
    check_transport_security,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _envelope_kwargs(**overrides) -> dict:
    """Minimum-valid Envelope kwargs (every required field), so a test
    can override exactly the one field it cares about."""
    base = dict(
        schema_version="1.0",
        type="command",
        name="Probe",
        message_version="1.0",
        message_id="00000000-0000-0000-0000-000000000001",
        correlation_id="00000000-0000-0000-0000-000000000002",
        idempotency_key="abc-123",
        sent_at=datetime(2026, 1, 1, tzinfo=UTC),
        trace_id="trace-1",
    )
    base.update(overrides)
    return base


@pytest.fixture
def isolated_env(monkeypatch) -> Iterator[None]:
    """Drop every KBM_LEDSAS_* env var so a test starts from a known
    blank slate. Restores everything via monkeypatch teardown."""
    for key in [k for k in os.environ if k.startswith("KBM_LEDSAS_")]:
        monkeypatch.delenv(key, raising=False)
    yield


# ---------------------------------------------------------------------------
# H1a: envelope.reply_to length boundary matches AMQP's 127-byte cap.
# ---------------------------------------------------------------------------


class TestReplyToLengthBoundary:
    """envelope.reply_to max_length tightened 255 → 127."""

    def test_reply_to_at_amqp_limit_accepted(self):
        """127 chars fits AMQP's exchange-name protocol cap exactly."""
        kwargs = _envelope_kwargs(reply_to="r" + "x" * 126)
        env = Envelope(**kwargs)
        assert len(env.reply_to) == 127

    def test_reply_to_just_over_amqp_limit_rejected(self):
        """128 chars exceeds AMQP's cap — schema must reject up front."""
        kwargs = _envelope_kwargs(reply_to="r" + "x" * 127)
        with pytest.raises(ValidationError) as exc_info:
            Envelope(**kwargs)
        assert "reply_to" in str(exc_info.value)
        assert "127" in str(exc_info.value)

    def test_reply_to_under_old_255_now_rejected(self):
        """The pre-fix 255-char ceiling let pamqp ValueError leak through."""
        kwargs = _envelope_kwargs(reply_to="r" + "x" * 199)
        with pytest.raises(ValidationError):
            Envelope(**kwargs)


# ---------------------------------------------------------------------------
# H1b: SDKConfig refuses tenant + service_name combos that overflow the
# AMQP 127-byte cap on the resulting dlq.queue.{tenant}.{service}.v1 name.
# ---------------------------------------------------------------------------


class TestTopologyNameBudget:
    """Combined tenant + service_name length must fit the worst-case
    'dlq.queue.{tenant}.{service}.v1' template."""

    def test_service_alone_within_budget(self):
        """No-tenant case: 64-char service_name (max allowed individually)
        produces 'dlq.queue.{64}.v1' = 77 chars — well under 127."""
        check_topology_name_budget("a" * 64, None)

    def test_service_alone_too_long_rejected(self):
        """If service_name somehow exceeds the no-tenant budget."""
        with pytest.raises(ValueError) as exc_info:
            check_topology_name_budget("a" * 115, None)
        assert "127" in str(exc_info.value)

    def test_tenant_plus_service_within_budget(self):
        """Combined length up to 113 chars fits the with-tenant template."""
        check_topology_name_budget("a" * 56, "b" * 56)

    def test_tenant_plus_service_overflow_rejected(self):
        """64 + 64 = 128 chars overflows the with-tenant template's budget
        (127 - 14 fixed overhead = 113 char budget)."""
        with pytest.raises(ValueError) as exc_info:
            check_topology_name_budget("s" * 64, "t" * 64)
        assert "127" in str(exc_info.value)
        assert "service_name" in str(exc_info.value)
        assert "tenant" in str(exc_info.value).lower() or "TENANT" in str(exc_info.value)

    def test_sdkconfig_load_refuses_oversize_combo(self, isolated_env, monkeypatch):
        """The model_validator wires the budget check into SDKConfig
        load so app.py sees a clean 'Configuration error: ...' line
        BEFORE any AMQP / blob client is constructed."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_SERVICE_NAME", "s" * 64)
        monkeypatch.setenv("KBM_LEDSAS_TENANT", "t" * 64)
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            SDKConfig.from_env(service_name="ignored")
        assert "127" in str(exc_info.value)


# ---------------------------------------------------------------------------
# M3: cleartext-AMQP refusal fires at config-load (was: deferred to
# DirectTransport.start, after ~7 INFO lines of half-built state).
# ---------------------------------------------------------------------------


class TestCleartextAmqpAtConfigLoad:
    """SDKConfig refuses amqp:// to non-loopback at LOAD time (M3)."""

    def test_loopback_amqp_accepted(self, isolated_env, monkeypatch):
        """Loopback hosts don't trigger the refusal."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.rabbitmq_url == "amqp://guest:guest@127.0.0.1:5672/"

    def test_amqps_to_remote_accepted(self, isolated_env, monkeypatch):
        """amqps:// to non-loopback is fine (TLS) — only amqp:// refused."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv(
            "KBM_LEDSAS_RABBITMQ_URL", "amqps://guest:guest@broker.example.com:5671/"
        )
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.rabbitmq_url.startswith("amqps://")

    def test_cleartext_to_remote_refused_at_load(self, isolated_env, monkeypatch):
        """amqp:// to non-loopback raises at SDKConfig.from_env — NOT
        after AzureBlobClient construction."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@10.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            SDKConfig.from_env(service_name="svc")
        msg = str(exc_info.value)
        assert "cleartext" in msg.lower() or "amqp" in msg.lower()
        assert "ALLOW_INSECURE_AMQP" in msg

    def test_allow_insecure_amqp_escape_hatch(self, isolated_env, monkeypatch):
        """KBM_LEDSAS_ALLOW_INSECURE_AMQP downgrades refusal to WARNING."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@10.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_ALLOW_INSECURE_AMQP", "1")
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.rabbitmq_url == "amqp://guest:guest@10.0.0.1:5672/"

    @pytest.mark.parametrize("truthy", ["1", "true", "TRUE", "yes", "YES", "on", "t", "y"])
    def test_allow_insecure_amqp_accepts_truthy_strings(self, isolated_env, monkeypatch, truthy):
        """L2: ALLOW_INSECURE_AMQP now accepts any truthy form, not only '1'."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@10.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_ALLOW_INSECURE_AMQP", truthy)
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.rabbitmq_url == "amqp://guest:guest@10.0.0.1:5672/"

    @pytest.mark.parametrize("falsy", ["0", "false", "no", "off", "", "FALSE", "garbage", "2"])
    def test_allow_insecure_amqp_falsy_strings_still_refuse(self, isolated_env, monkeypatch, falsy):
        """Security-sensitive: anything not in the accepted truthy set
        fails closed (the refusal still fires)."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@10.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_ALLOW_INSECURE_AMQP", falsy)
        with pytest.raises((ValueError, ValidationError)):
            SDKConfig.from_env(service_name="svc")


# ---------------------------------------------------------------------------
# L2: unified boolean env-var parsing.
# ---------------------------------------------------------------------------


class TestBoolEnvUnified:
    """All four bool knobs use the same lenient parser."""

    @pytest.mark.parametrize(
        "raw", ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON", "t", "y"]
    )
    def test_truthy_forms(self, raw, monkeypatch):
        """Each of these values, when read from the env, must be truthy."""
        assert _is_truthy(raw) is True
        monkeypatch.setenv("KBM_LEDSAS_TASK022_BOOL", raw)
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "false") is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", "", "FALSE", "anything", "  "])
    def test_falsy_forms(self, raw, monkeypatch):
        """Anything not in the truthy set is falsy — including unknown
        tokens like 'anything' (security-sensitive flags fail closed)."""
        assert _is_truthy(raw) is False
        monkeypatch.setenv("KBM_LEDSAS_TASK022_BOOL", raw)
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "true") is False

    def test_none_is_falsy(self):
        assert _is_truthy(None) is False

    def test_bool_env_default_when_unset(self, monkeypatch):
        """When env var is unset, the default string is parsed via _is_truthy."""
        monkeypatch.delenv("KBM_LEDSAS_TASK022_BOOL", raising=False)
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "false") is False
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "true") is True
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "1") is True
        assert _bool_env("KBM_LEDSAS_TASK022_BOOL", "0") is False

    def test_generic_errors_accepts_one(self, isolated_env, monkeypatch):
        """L2 regression: GENERIC_ERRORS=1 used to be silently false."""
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_GENERIC_ERRORS", "1")
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.generic_errors is True

    def test_health_verbose_accepts_yes(self, isolated_env, monkeypatch):
        monkeypatch.setenv("KBM_LEDSAS_MODE", "direct")
        monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:5672/")
        monkeypatch.setenv("KBM_LEDSAS_BLOB_CONN_STRING", "x")
        monkeypatch.setenv("KBM_LEDSAS_HEALTH_VERBOSE", "yes")
        cfg = SDKConfig.from_env(service_name="svc")
        assert cfg.health_verbose is True


# ---------------------------------------------------------------------------
# Sanity: the security module's public API surface is what config + transport
# rely on. Lock in the names so an internal rename can't silently break them.
# ---------------------------------------------------------------------------


class TestHealthServerHardening:
    """Server header stripped, 404/405 responses are JSON.

    Same pattern as test_health.py — bind a real socket on an ephemeral
    port and hit it with aiohttp.ClientSession. Avoids the pytest-aiohttp
    fixture dependency.
    """

    @pytest.fixture
    async def started_health_server(self):
        """Yield (server, base_url). Tears the server down on exit."""
        import socket

        from kbm_ledsas_sdk.health.checks import CheckResult, HealthCheckRegistry
        from kbm_ledsas_sdk.health.server import HealthServer

        async def _ready() -> CheckResult:
            return CheckResult(name="transport", healthy=True)

        # Find a free ephemeral port (race-prone but fine for unit tests)
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()

        server = HealthServer(
            service_name="svc",
            host="127.0.0.1",
            port=port,
            liveness_registry=HealthCheckRegistry(),
            readiness_registry=HealthCheckRegistry(),
            default_readiness=_ready,
        )
        await server.start()
        try:
            yield server, f"http://127.0.0.1:{port}"
        finally:
            await server.stop()

    @pytest.mark.asyncio
    async def test_server_header_stripped_on_200(self, started_health_server):
        """M1: success responses must not advertise Python/aiohttp version."""
        import aiohttp

        _, url = started_health_server
        async with aiohttp.ClientSession() as s, s.get(f"{url}/health") as r:
            assert r.status == 200
            assert "Server" not in r.headers
            body = await r.json()
            assert body["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_404_is_json(self, started_health_server):
        """L3: unknown route responds with JSON, not plaintext."""
        import aiohttp

        _, url = started_health_server
        async with aiohttp.ClientSession() as s, s.get(f"{url}/admin") as r:
            assert r.status == 404
            assert "Server" not in r.headers  # M1 covers error paths too
            body = await r.json()
            assert body["status"] == "error"
            assert body["code"] == 404

    @pytest.mark.asyncio
    async def test_405_is_json(self, started_health_server):
        """L3: disallowed method responds with JSON listing allowed verbs."""
        import aiohttp

        _, url = started_health_server
        async with aiohttp.ClientSession() as s, s.post(f"{url}/health") as r:
            assert r.status == 405
            assert "Server" not in r.headers
            body = await r.json()
            assert body["status"] == "error"
            assert body["code"] == 405
            assert "GET" in body["allowed"]


class TestSecurityModuleSurface:
    """runtime/security.py is the canonical home for transport-security
    helpers. Lock in the public names."""

    def test_loopback_hosts_constant(self):
        assert "127.0.0.1" in LOOPBACK_HOSTS
        assert "::1" in LOOPBACK_HOSTS
        assert "localhost" in LOOPBACK_HOSTS

    def test_check_transport_security_is_callable(self):
        assert callable(check_transport_security)

    def test_check_topology_name_budget_is_callable(self):
        assert callable(check_topology_name_budget)
