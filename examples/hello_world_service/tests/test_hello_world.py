"""Unit tests for the hello-world service.

Covers the ``say_hello`` handler (mocked ctx), the sender's pure
envelope/exchange helpers and its reply-topology cleanup (aio_pika faked),
and the example's own files (compose stack, .dockerignore, README).

No broker, no Azurite, no SDK runtime required.
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

EXAMPLE_ROOT = Path(__file__).resolve().parent.parent


def _load_module(path: Path, name: str):
    """Import main.py under a unique module name so it never clashes with
    another module named ``main`` on sys.path (this directory is not a
    package)."""
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


hello_main = _load_module(EXAMPLE_ROOT / "main.py", "hello_world_main")
sender = _load_module(EXAMPLE_ROOT / "scripts" / "send_hello.py", "hello_world_sender")


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
    env_example = (EXAMPLE_ROOT / ".env.example").read_text()
    assert "KBM_LEDSAS_SERVICE_NAME=hello-world" in env_example
    assert hello_main.app is not None


# ---------------------------------------------------------------------------
# sender pure helpers
# ---------------------------------------------------------------------------


def test_command_exchange_name_matches_the_documented_topology() -> None:
    # The documented names (docs/SDK_API_REFERENCE.md): cmd.[{tenant}.]{service}.v1.
    service = "hello-world"
    assert sender.build_command_exchange_name(None, service) == f"cmd.{service}.v1"
    assert sender.build_command_exchange_name("acme", service) == f"cmd.acme.{service}.v1"


def test_tests_use_only_the_public_api() -> None:
    # The example must not lean on SDK internals (the AMQP helpers are not public API).
    for path in (
        Path(__file__),
        EXAMPLE_ROOT / "scripts" / "send_hello.py",
        EXAMPLE_ROOT / "main.py",
    ):
        assert "kbm_ledsas_sdk." + "amqp" not in path.read_text(encoding="utf-8"), path


def test_build_idempotency_key_is_new_for_every_request() -> None:
    keys = {sender.build_idempotency_key() for _ in range(1000)}
    assert len(keys) == 1000
    assert all(re.fullmatch(r"hello-[0-9a-f]{32}", k) for k in keys)


def test_idempotency_key_option_resends_a_request() -> None:
    parser_args = []

    async def fake_run(args):
        parser_args.append(args)
        return 0

    original = sender._run
    sender._run = fake_run
    try:
        assert sender.main(["Ada", "--idempotency-key", "hello-1"]) == 0
        assert sender.main(["Ada"]) == 0
    finally:
        sender._run = original
    assert parser_args[0].idempotency_key == "hello-1"
    assert parser_args[1].idempotency_key is None


def test_idempotency_key_option_is_validated() -> None:
    # A key outside the envelope's pattern would make the command an invalid
    # envelope, dead-lettered without a reply: refused by the option parser.
    parser = sender.build_parser()
    assert (
        parser.parse_args(["--idempotency-key", "hello-0a:b.c_d"]).idempotency_key
        == "hello-0a:b.c_d"
    )
    for bad in ("has space", "x" * 129, "é"):
        with pytest.raises(SystemExit):
            parser.parse_args(["--idempotency-key", bad])


def test_help_states_the_timeout_formula() -> None:
    help_text = sender.build_parser().format_help()
    assert "KBM_LEDSAS_MAX_RETRIES + 1" in " ".join(help_text.split())
    assert "(default 30.0)" in help_text


def test_default_timeout_allows_for_the_retries() -> None:
    # The one reply comes after the last attempt: at least 30 s with the defaults.
    captured = []

    async def fake_run(args):
        captured.append(args.timeout)
        return 0

    original = sender._run
    sender._run = fake_run
    try:
        sender.main([])
    finally:
        sender._run = original
    assert captured == [sender.DEFAULT_TIMEOUT_S]
    assert sender.DEFAULT_TIMEOUT_S >= 30


# ---------------------------------------------------------------------------
# sender: the reply topology is cleaned up whatever fails (aio_pika faked)
# ---------------------------------------------------------------------------


def _fake_aio_pika(monkeypatch, *, get_exchange_error=None, publish_error=None):
    rx, rq, cmd_ex = MagicMock(), MagicMock(), MagicMock()
    rx.delete = AsyncMock()
    rq.delete, rq.bind, rq.cancel = AsyncMock(), AsyncMock(), AsyncMock()
    rq.consume = AsyncMock(return_value="ctag")
    cmd_ex.publish = AsyncMock(side_effect=publish_error)
    channel = MagicMock()
    channel.get_exchange = AsyncMock(side_effect=get_exchange_error, return_value=cmd_ex)
    channel.declare_exchange = AsyncMock(return_value=rx)
    channel.declare_queue = AsyncMock(return_value=rq)
    connection = MagicMock()
    connection.channel = AsyncMock(return_value=channel)
    connection.close = AsyncMock()
    fake = SimpleNamespace(
        connect_robust=AsyncMock(return_value=connection),
        ExchangeType=SimpleNamespace(TOPIC="topic"),
        DeliveryMode=SimpleNamespace(PERSISTENT=2),
        Message=lambda **kw: kw,
        abc=SimpleNamespace(AbstractIncomingMessage=object),
    )
    monkeypatch.setitem(sys.modules, "aio_pika", fake)
    return SimpleNamespace(rx=rx, rq=rq, channel=channel, connection=connection)


def _publish_and_wait():
    return sender._publish_and_wait(
        sender.Env(environ={}),
        message={"envelope": {}, "payload": {}},
        reply_exchange="reply-ex-hello-test",
        reply_queue="reply-ex-hello-test-q",
        correlation_id="corr-1",
        timeout=0.01,
    )


def test_missing_service_exchange_creates_no_reply_topology(monkeypatch) -> None:
    fake = _fake_aio_pika(monkeypatch, get_exchange_error=RuntimeError("NOT_FOUND"))
    with pytest.raises(RuntimeError):
        _run(_publish_and_wait())
    fake.channel.declare_exchange.assert_not_called()
    fake.channel.declare_queue.assert_not_called()
    fake.connection.close.assert_awaited()


def test_failed_publish_deletes_the_reply_queue_and_exchange(monkeypatch) -> None:
    fake = _fake_aio_pika(monkeypatch, publish_error=RuntimeError("channel closed"))
    with pytest.raises(RuntimeError):
        _run(_publish_and_wait())
    fake.rq.delete.assert_awaited()
    fake.rx.delete.assert_awaited()
    fake.connection.close.assert_awaited()


def test_timeout_deletes_the_reply_queue_and_exchange(monkeypatch) -> None:
    fake = _fake_aio_pika(monkeypatch)
    with pytest.raises(TimeoutError):
        _run(_publish_and_wait())
    fake.rq.delete.assert_awaited()
    fake.rx.delete.assert_awaited()


# ---------------------------------------------------------------------------
# the example's own files
# ---------------------------------------------------------------------------

COMPOSE = EXAMPLE_ROOT / "deploy" / "local" / "docker-compose.yml"


def _compose_services() -> dict[str, str]:
    """service name -> its block of the compose file (two-space indented YAML)."""
    text = COMPOSE.read_text(encoding="utf-8")
    body = re.search(r"^services:\n(.*?)(?=^\S|\Z)", text, re.M | re.S).group(1)
    return dict(re.findall(r"^  ([A-Za-z0-9_-]+):\n((?:^(?:    .*|\s*)\n)*)", body, re.M))


def test_compose_host_ports_are_overridable() -> None:
    published = [
        port
        for block in _compose_services().values()
        for port in re.findall(r'^\s+- "([^"]+)"', block.split("ports:", 1)[1], re.M)
        if "ports:" in block
    ]
    published = [p for p in published if re.match(r"[0-9]|\$", p)]
    assert len(published) >= 3
    for port in published:
        assert re.fullmatch(r"127\.0\.0\.1:\$\{[A-Z_]+:-[0-9]+\}:[0-9]+", port), port


def test_azurite_writes_into_its_volume() -> None:
    blocks = [b for b in _compose_services().values() if "azurite" in b]
    assert blocks
    for block in blocks:
        if re.search(r"^\s+- [A-Za-z0-9_-]+:/data\s*$", block, re.M):
            command = re.search(r"^\s+command: (.+)$", block, re.M).group(1)
            assert re.search(r"(?:-l|--location) /data\b", command), command


def test_dockerignore_keeps_local_files_out_of_the_build_context() -> None:
    lines = (EXAMPLE_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    for entry in (".env", ".env.*", ".venv/", "!.env.example", "tests/", "scripts/", ".git/"):
        assert entry in lines, entry


def _readme_section(title: str) -> str:
    text = (EXAMPLE_ROOT / "README.md").read_text(encoding="utf-8")
    match = re.search(rf"^## {re.escape(title)}\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    assert match, title
    return match.group(1)


def test_readme_container_section_runs_against_the_local_stack() -> None:
    section = _readme_section("Container image")
    assert "docker run" in section
    for setting in (
        "KBM_LEDSAS_ALLOW_INSECURE_AMQP",
        "KBM_LEDSAS_HEALTH_HOST=0.0.0.0",
        "--network",
    ):
        assert setting in section, setting
    assert "stop_grace_period" in section or "docker stop -t" in section


def test_readme_shutdown_time_is_qualified() -> None:
    text = (EXAMPLE_ROOT / "README.md").read_text(encoding="utf-8")
    assert not re.search(r"<\s?1\s?s\b", text)


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


def test_compose_port_variables_are_the_documented_ones() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    readme = _readme_section("Run it locally")
    for var in ("HELLO_WORLD_AMQP_PORT", "HELLO_WORLD_MGMT_PORT", "HELLO_WORLD_BLOB_PORT"):
        assert "${" + var + ":-" in compose, var
        assert var in readme, var


def test_readme_shutdown_names_the_handler_timeout() -> None:
    assert "KBM_LEDSAS_HANDLER_TIMEOUT" in _readme_section("Run it locally")


def test_readme_does_not_claim_what_the_base_image_ships() -> None:
    # Whether the base image still carries setuptools, wheel or the ensurepip
    # bundle depends on how it was built: the README says what to do either way.
    text = (EXAMPLE_ROOT / "README.md").read_text(encoding="utf-8")
    assert "stays in the image" not in text
    assert "may still carry `setuptools`, `wheel`" in text


def test_example_wording_names_no_indirect_path() -> None:
    for path in (EXAMPLE_ROOT / "README.md", EXAMPLE_ROOT / "main.py"):
        assert "straight to" not in path.read_text(encoding="utf-8"), path
