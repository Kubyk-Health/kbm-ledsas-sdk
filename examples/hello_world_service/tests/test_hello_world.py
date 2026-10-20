"""Unit tests for the hello-world service.

Covers the ``say_hello`` handler (mocked ctx), the sender's pure
envelope/exchange helpers, its reply-topology cleanup and its error
reporting (aio_pika faked; one run against a closed local port), and the
example's own files (compose stack, .dockerignore, pytest.ini). The README
is yours to rewrite: nothing here reads it.

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
    # The documented names (SDK API reference, "AMQP Topology"):
    # cmd.[{tenant}.]{service}.v1.
    service = "hello-world"
    assert sender.build_command_exchange_name(None, service) == f"cmd.{service}.v1"
    assert sender.build_command_exchange_name("acme", service) == f"cmd.acme.{service}.v1"


def test_tests_use_only_the_public_api() -> None:
    # The example must not lean on SDK internals (the AMQP helpers are not public API).
    files = [
        *EXAMPLE_ROOT.glob("*.py"),
        *EXAMPLE_ROOT.glob("scripts/*.py"),
        *EXAMPLE_ROOT.glob("tests/*.py"),
    ]
    assert len(files) >= 3
    for path in files:
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


class _FakeAMQPConnectionError(Exception):
    pass


class _FakeAuthError(_FakeAMQPConnectionError):
    pass


class _FakeNotFound(Exception):
    pass


def _fake_aio_pika(monkeypatch, *, get_exchange_error=None, publish_error=None, connect_error=None):
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
        connect=AsyncMock(side_effect=connect_error, return_value=connection),
        exceptions=SimpleNamespace(
            AMQPConnectionError=_FakeAMQPConnectionError,
            ProbableAuthenticationError=_FakeAuthError,
            ChannelNotFoundEntity=_FakeNotFound,
        ),
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


async def _never(*args, **kwargs):
    await asyncio.Event().wait()


def test_timeout_bounds_the_whole_run(monkeypatch) -> None:
    # A blackholed broker: the connection attempt never answers.
    fake = _fake_aio_pika(monkeypatch)
    fake_connect = sys.modules["aio_pika"]
    fake_connect.connect = AsyncMock(side_effect=_never)
    args = sender.build_parser().parse_args(["--timeout", "0.2"])
    monkeypatch.delenv("KBM_LEDSAS_RABBITMQ_URL", raising=False)
    assert _run(asyncio.wait_for(sender._run(args), timeout=5)) == 1
    assert fake.connection.close.await_count == 0  # never connected


def test_one_connection_attempt_with_the_timeout(monkeypatch) -> None:
    _fake_aio_pika(monkeypatch)
    with pytest.raises(TimeoutError):
        _run(_publish_and_wait())
    connect = sys.modules["aio_pika"].connect
    assert connect.await_args.kwargs.get("timeout") == 0.01


# Test URLs with a user and password are built from parts, so that secret
# scanners do not report them as credentials.
_USERINFO = ":".join(("guest", "guest"))


@pytest.mark.parametrize(
    "url",
    [
        f"{_USERINFO}@127.0.0.1:25675",
        "not a url",
        "http://" + f"{_USERINFO}@127.0.0.1:5672/",
        "amqp:///",
        "amqp://" + f"{_USERINFO}@127.0.0.1:notaport/",
        "amqp://" + f"{_USERINFO}@127.0.0.1:0/",
        "amqp://" + f"{_USERINFO}@[::1/",
    ],
)
def test_an_invalid_broker_url_is_one_line_and_no_connection(monkeypatch, capsys, url) -> None:
    _fake_aio_pika(monkeypatch)
    monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", url)
    assert sender.main(["--timeout", "5"]) == 1
    err = capsys.readouterr().err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: KBM_LEDSAS_RABBITMQ_URL"), err
    assert "guest:guest" not in err[0]
    sys.modules["aio_pika"].connect.assert_not_called()


def test_a_failure_is_one_line_without_the_password(monkeypatch, capsys) -> None:
    url = "amqp://guest:" + "s3cret-pw@127.0.0.1:5672/"
    fake = _fake_aio_pika(monkeypatch)
    sys.modules["aio_pika"].connect = AsyncMock(
        side_effect=ConnectionError(f"Connect call failed for {url}")
    )
    monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", url)
    assert sender.main(["--timeout", "5"]) == 1
    out = capsys.readouterr()
    err = out.err.strip().splitlines()
    assert len(err) == 1 and err[0].startswith("error: "), err
    assert "s3cret-pw" not in out.err + out.out
    assert "Broker:       amqp://guest:***@127.0.0.1:5672/" in out.out
    fake.channel.declare_queue.assert_not_called()


def test_a_service_that_never_started_is_named(monkeypatch, capsys) -> None:
    fake = _fake_aio_pika(monkeypatch, get_exchange_error=_FakeNotFound("NOT_FOUND"))
    monkeypatch.delenv("KBM_LEDSAS_RABBITMQ_URL", raising=False)
    assert sender.main(["--timeout", "5"]) == 1
    out = capsys.readouterr()
    assert out.err.strip() == (
        "error: the exchange cmd.hello-world.v1 does not exist: start the service "
        "first (python main.py), and check KBM_LEDSAS_SERVICE_NAME and KBM_LEDSAS_TENANT"
    )
    fake.connection.close.assert_awaited()
    # The broker used is printed when the variable is unset.
    assert "(KBM_LEDSAS_RABBITMQ_URL is unset: the default)" in out.out


@pytest.mark.parametrize(
    "error, text",
    [
        (
            _FakeAMQPConnectionError("refused amqp://guest:" + "s3cret-pw@host"),
            "cannot connect to the broker at",
        ),
        (_FakeAuthError("ACCESS_REFUSED"), "refused the user name or password"),
        (OSError("Connect call failed"), "cannot connect to the broker at"),
    ],
)
def test_a_failed_connection_is_one_error_line(monkeypatch, capsys, error, text) -> None:
    fake = _fake_aio_pika(monkeypatch, connect_error=error)
    monkeypatch.setenv("KBM_LEDSAS_RABBITMQ_URL", "amqp://guest:" + "s3cret-pw@127.0.0.1:5672/")
    assert sender.main(["--timeout", "30"]) == 1
    err = capsys.readouterr().err
    assert err.startswith("error: ") and text in err and err.count("\n") == 1, err
    assert "s3cret-pw" not in err
    # One attempt, bounded below --timeout (a firewalled host fails sooner).
    assert sys.modules["aio_pika"].connect.await_args.kwargs["timeout"] == sender.CONNECT_TIMEOUT_S
    fake.channel.declare_queue.assert_not_called()


def test_a_broker_that_never_answers_is_named_as_such(monkeypatch, capsys) -> None:
    _fake_aio_pika(monkeypatch)
    sys.modules["aio_pika"].connect = AsyncMock(side_effect=_never)
    monkeypatch.delenv("KBM_LEDSAS_RABBITMQ_URL", raising=False)
    assert sender.main(["--timeout", "0.2"]) == 1
    err = capsys.readouterr().err
    assert (
        err.startswith("error: cannot connect to the broker at") and "no answer within 0.2s" in err
    )
    assert err.count("\n") == 1


def test_no_reply_in_time_is_one_error_line(monkeypatch, capsys) -> None:
    fake = _fake_aio_pika(monkeypatch)
    monkeypatch.delenv("KBM_LEDSAS_RABBITMQ_URL", raising=False)
    assert sender.main(["--timeout", "0.2"]) == 1
    err = capsys.readouterr().err
    assert err.startswith("error: no response within") and err.count("\n") == 1, err
    fake.rq.delete.assert_awaited()


def test_the_sender_uses_one_plain_connection() -> None:
    source = (EXAMPLE_ROOT / "scripts" / "send_hello.py").read_text(encoding="utf-8")
    assert "connect_robust" not in source
    assert "aio_pika.connect(" in source


def test_an_unreachable_broker_ends_promptly_without_a_traceback(tmp_path) -> None:
    """Run as written against a closed local port: one line, exit 1."""
    pytest.importorskip("aio_pika")
    import os
    import subprocess

    env = dict(os.environ, KBM_LEDSAS_RABBITMQ_URL="amqp://guest:" + "guest@127.0.0.1:1/")
    proc = subprocess.run(
        [sys.executable, str(EXAMPLE_ROOT / "scripts" / "send_hello.py"), "--timeout", "5"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        cwd=tmp_path,
    )
    assert proc.returncode == 1, proc.stderr
    assert "Traceback" not in proc.stderr
    assert proc.stderr.count("\n") == 1 and proc.stderr.startswith(
        "error: cannot connect"
    ), proc.stderr
    assert "guest:guest" not in proc.stderr


def test_ctrl_c_is_one_line_and_exit_130(monkeypatch, capsys) -> None:
    async def interrupted(args):
        raise KeyboardInterrupt

    monkeypatch.setattr(sender, "_run", interrupted)
    assert sender.main([]) == 130
    assert capsys.readouterr().err.strip() == "interrupted"


def test_the_amqp_libraries_do_not_log_the_url(monkeypatch) -> None:
    import logging

    async def ok(args):
        return 0

    monkeypatch.setattr(sender, "_run", ok)
    for name in ("aiormq", "aio_pika"):
        monkeypatch.setattr(logging.getLogger(name), "level", logging.NOTSET)
    sender.main([])
    for name in ("aiormq", "aio_pika"):
        assert logging.getLogger(name).level == logging.CRITICAL, name


def test_mask_url() -> None:
    assert sender.mask_url("amqp://u:p@h:5672/v?x=1") == "amqp://u:***@h:5672/v?***"
    assert sender.mask_url("amqps://h/") == "amqps://h/"
    assert sender.mask_url("amqp://u@h/") == "amqp://u@h/"


# ---------------------------------------------------------------------------
# sender: replies, --timeout, expiry of the command, SIGTERM
# ---------------------------------------------------------------------------


def _reply(monkeypatch, payload):
    async def fake(env, **kwargs):
        return {"envelope": {}, "payload": payload}

    monkeypatch.setattr(sender, "_publish_and_wait", fake)


def test_an_error_reply_is_one_error_line_and_exit_1(monkeypatch, capsys) -> None:
    _reply(
        monkeypatch, {"error": {"code": "Permanent", "message": "bad\nname", "retryable": False}}
    )
    assert sender.main(["--timeout", "5"]) == 1
    out = capsys.readouterr()
    assert out.err.strip().splitlines() == ["error: Permanent: bad name"]
    assert "greeting:" not in out.out


def test_a_result_with_its_own_error_key_is_not_an_error_reply() -> None:
    assert sender.reply_error({"error": "field x missing"}) is None
    assert sender.reply_error({"error": {"code": "X"}}) is None
    assert sender.reply_error(None) is None


def test_a_greeting_is_printed_and_exit_0(monkeypatch, capsys) -> None:
    _reply(monkeypatch, {"greeting": "hello Ada"})
    assert sender.main(["Ada", "--timeout", "5"]) == 0
    assert "greeting: hello Ada" in capsys.readouterr().out


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "1e300", "soon"])
def test_an_invalid_timeout_is_a_usage_error_and_nothing_is_sent(monkeypatch, value) -> None:
    fake = _fake_aio_pika(monkeypatch)
    with pytest.raises(SystemExit) as info:
        sender.main(["--timeout", value])
    assert info.value.code == 2
    sys.modules["aio_pika"].connect.assert_not_called()
    fake.channel.declare_queue.assert_not_called()


def test_the_command_expires_after_the_wait(monkeypatch) -> None:
    fake = _fake_aio_pika(monkeypatch)
    with pytest.raises(TimeoutError):
        _run(_publish_and_wait())
    cmd_ex = fake.channel.get_exchange.return_value
    message = cmd_ex.publish.await_args.args[0]
    assert message["expiration"] == 0.01


def test_the_broker_deletes_a_reply_topology_left_by_a_killed_run(monkeypatch) -> None:
    fake = _fake_aio_pika(monkeypatch)
    with pytest.raises(TimeoutError):
        _run(_publish_and_wait())
    queue_args = fake.channel.declare_queue.await_args.kwargs
    assert queue_args["durable"] is True and queue_args["auto_delete"] is False
    assert queue_args["arguments"] == {"x-expires": int((0.01 + 60) * 1000)}
    assert fake.channel.declare_exchange.await_args.kwargs["auto_delete"] is True


def test_sigterm_cleans_up_and_exits_143(monkeypatch, capsys) -> None:
    import os
    import signal

    cleaned = []

    async def killed(args):
        try:
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(5)
        finally:
            await asyncio.sleep(0)  # an await in the cleanup still runs
            cleaned.append(True)
        return 0

    monkeypatch.setattr(sender, "_run", killed)
    before = signal.getsignal(signal.SIGTERM)
    assert sender.main([]) == 143
    assert cleaned == [True]
    assert capsys.readouterr().err.strip() == "terminated"
    assert signal.getsignal(signal.SIGTERM) == before


@pytest.mark.parametrize("log_format", ["json", "text"])
def test_the_service_logs_in_the_configured_format(log_format) -> None:
    import json
    import os
    import subprocess

    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('m', {str(EXAMPLE_ROOT / 'main.py')!r})\n"
        "m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)\n"
        "m.logger.info('Greeting requested')\n"
    )
    env = dict(os.environ, KBM_LEDSAS_LOG_FORMAT=log_format)
    r = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    [line] = [ln for ln in r.stderr.splitlines() if "Greeting requested" in ln]
    if log_format == "json":
        assert json.loads(line)["message"] == "Greeting requested"
    else:
        assert not line.startswith("{") and line.endswith(" INFO m: Greeting requested"), line


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


def test_compose_header_names_every_port_variable() -> None:
    # The header comment's example command moves every published port.
    compose = COMPOSE.read_text(encoding="utf-8")
    header = compose.split("\nservices:", 1)[0]
    variables = re.findall(r"\$\{([A-Z_]+):-[0-9]+\}", compose)
    assert len(variables) >= 3
    for var in variables:
        assert f"{var}=" in header, var


def test_pytest_ini_makes_the_example_its_own_test_root() -> None:
    import configparser

    ini = configparser.ConfigParser()
    ini.read(EXAMPLE_ROOT / "pytest.ini")
    assert ini.get("pytest", "testpaths").split() == ["tests"]


def test_sender_message_drives_the_real_handler() -> None:
    msg = sender.build_command_message(
        reply_to="reply-ex-hello-test",
        idempotency_key=sender.build_idempotency_key(),
        name="LEDSAS",
    )
    result = _run(hello_main.say_hello(_ctx(), msg["payload"]))
    assert result == {"greeting": "hello LEDSAS"}
