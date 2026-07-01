"""Unit tests for the hello-world service.

Covers the ``say_hello`` handler (mocked ctx), the sender's pure
envelope/exchange helpers, and the exchange-name cross-check against the
SDK's real topology builder.

No broker, no Azurite, no SDK runtime required.
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

TEMPLATE_ROOT = Path(__file__).resolve().parent.parent


def _load_module(path: Path, name: str):
    """Import a file under a UNIQUE module name (the template is not a
    package). A plain ``import main`` would silently reuse another
    template's already-imported ``main`` module when several template
    suites run in one pytest invocation."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


hello_main = _load_module(TEMPLATE_ROOT / "main.py", "hello_world_main")
sender = _load_module(TEMPLATE_ROOT / "scripts" / "send_hello.py", "hello_world_sender")


def _run(coro):
    return asyncio.run(coro)


def _ctx(correlation_id: str = "corr-test-1"):
    return SimpleNamespace(correlation_id=correlation_id)


# ---------------------------------------------------------------------------
# say_hello handler
# ---------------------------------------------------------------------------


def test_handler_default_greeting() -> None:
    result = _run(hello_main.say_hello(_ctx(), {}))
    assert result == {"greeting": "hello world"}


def test_handler_named_greeting() -> None:
    result = _run(hello_main.say_hello(_ctx(), {"name": "Ada"}))
    assert result == {"greeting": "hello Ada"}


def test_handler_empty_name_falls_back_to_world() -> None:
    result = _run(hello_main.say_hello(_ctx(), {"name": ""}))
    assert result == {"greeting": "hello world"}


def test_handler_non_string_name_is_coerced() -> None:
    result = _run(hello_main.say_hello(_ctx(), {"name": 42}))
    assert result == {"greeting": "hello 42"}


def test_handler_caps_oversized_name() -> None:
    result = _run(hello_main.say_hello(_ctx(), {"name": "x" * 5000}))
    assert result["greeting"] == "hello " + "x" * 100


def test_service_name_matches_env_example() -> None:
    # main.py hardcodes the service name; .env.example must agree so the
    # sender computes the same command exchange the service declares.
    env_example = (TEMPLATE_ROOT / ".env.example").read_text()
    assert "KBM_LEDSAS_SERVICE_NAME=hello-world" in env_example
    assert hello_main.app is not None


# ---------------------------------------------------------------------------
# sender pure helpers
# ---------------------------------------------------------------------------


def test_command_exchange_name_matches_sdk_topology() -> None:
    # Mirrors kbm_ledsas_sdk.amqp.topology.build_exchange_name("cmd", ...).
    from kbm_ledsas_sdk.amqp.topology import build_exchange_name

    assert (
        sender.build_command_exchange_name(None, "hello-world")
        == build_exchange_name("cmd", None, "hello-world")
        == "cmd.hello-world.v1"
    )
    assert (
        sender.build_command_exchange_name("acme", "hello-world")
        == build_exchange_name("cmd", "acme", "hello-world")
        == "cmd.acme.hello-world.v1"
    )


def test_build_idempotency_key_shape() -> None:
    now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    assert sender.build_idempotency_key(now=now) == "hello-20260102T030405Z"


def test_build_command_message_shape() -> None:
    msg = sender.build_command_message(
        reply_to="reply-ex-hello-abc123",
        idempotency_key="hello-20260102T030405Z",
        name="Ada",
        correlation_id="corr-123",
    )
    env = msg["envelope"]
    assert env["type"] == "command"
    assert env["name"] == "SayHello"
    assert env["schema_version"] == "1.0"
    assert env["reply_to"] == "reply-ex-hello-abc123"
    assert env["correlation_id"] == "corr-123"
    assert env["idempotency_key"] == "hello-20260102T030405Z"
    # message_id / trace_id auto-filled with UUIDs.
    assert env["message_id"] and env["trace_id"]
    assert msg["payload"] == {"name": "Ada"}


def test_build_command_message_omits_name_when_unset() -> None:
    msg = sender.build_command_message(
        reply_to="reply-ex-hello-abc123",
        idempotency_key="hello-20260102T030405Z",
    )
    assert msg["payload"] == {}


def test_env_defaults() -> None:
    env = sender.Env(environ={})  # no env vars -> documented defaults
    assert env.service_name == "hello-world"
    assert env.tenant is None
    assert env.command_exchange == "cmd.hello-world.v1"


def test_env_overrides() -> None:
    env = sender.Env(
        environ={
            "KBM_LEDSAS_SERVICE_NAME": "Svc",
            "KBM_LEDSAS_TENANT": "acme",
        }
    )
    assert env.command_exchange == "cmd.acme.Svc.v1"


def test_env_blank_tenant_means_no_tenant() -> None:
    env = sender.Env(environ={"KBM_LEDSAS_TENANT": ""})
    assert env.tenant is None
    assert env.command_exchange == "cmd.hello-world.v1"


# ---------------------------------------------------------------------------
# sender envelope drives the real handler (no broker)
# ---------------------------------------------------------------------------


def test_sender_message_drives_real_handler() -> None:
    msg = sender.build_command_message(
        reply_to="reply-ex-hello-test",
        idempotency_key=sender.build_idempotency_key(),
        name="LEDSAS",
    )
    result = _run(hello_main.say_hello(_ctx(), msg["payload"]))
    assert result == {"greeting": "hello LEDSAS"}
