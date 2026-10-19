"""The README Quickstart's ``send_command.py``, run as written against a live
RabbitMQ, when the service is not there to answer.

A caller that copies the snippet must get one ``error:`` line and exit
status 1, never a traceback, when the service has never started (its
command exchange does not exist), when the service does not answer in
time and when it answers with an error reply; a SIGTERM must leave no reply
queue or exchange behind. The broker is ``LEDSAS_TEST_RABBITMQ_URL`` when set, else
``amqp://guest:guest@127.0.0.1:5672/`` (see CONTRIBUTING.md); the second
case also needs the blob storage emulator (``LEDSAS_TEST_BLOB_CONN_STRING``,
else Azurite on 127.0.0.1:10000) and skips without it. Each run uses a
command exchange of its own (the snippet's ``cmd.csv-processor.v1``
renamed), so a quickstart service that ran on the same broker changes
nothing.

The README's shell block that starts RabbitMQ must also end when the
container cannot start (its host port taken): checked with Docker, and
skipped without it.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import aio_pika
import pytest

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/"
DEFAULT_BLOB_CONN_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)
RABBITMQ_URL = os.environ.get("LEDSAS_TEST_RABBITMQ_URL") or DEFAULT_RABBITMQ_URL
BLOB_CONN_STRING = os.environ.get("LEDSAS_TEST_BLOB_CONN_STRING") or DEFAULT_BLOB_CONN_STRING
README_EXCHANGE = "cmd.csv-processor.v1"
README_REPLY_EXCHANGE = 'f"reply-ex-quickstart-{uuid.uuid4().hex[:8]}"'


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _blob_endpoint() -> tuple[str, int]:
    for part in BLOB_CONN_STRING.split(";"):
        key, _, value = part.partition("=")
        if key.strip().lower() == "blobendpoint":
            parts = urlsplit(value.strip())
            return parts.hostname or "127.0.0.1", parts.port or 80
    return "127.0.0.1", 10000


_BROKER = urlsplit(RABBITMQ_URL)
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _port_open(_BROKER.hostname or "127.0.0.1", _BROKER.port or 5672),
        reason=(
            "RabbitMQ not reachable — start the local stack: docker compose -f "
            "examples/hello_world_service/deploy/local/docker-compose.yml up -d --wait "
            "(or point LEDSAS_TEST_RABBITMQ_URL at another local test broker)"
        ),
    ),
]


@pytest.fixture
def command_exchange() -> str:
    """A command exchange name of this test run's own."""
    return f"cmd.csv-processor-{uuid.uuid4().hex[:12]}.v1"


def send_command_source(exchange: str, wait: int | None = None) -> str:
    """The README's second python block (the file it says to save as
    send_command.py), publishing to ``exchange``."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    after = readme[readme.index("Save this as `send_command.py`") :]
    source = re.search(r"^```python\n(.*?)^```", after, re.M | re.S).group(1)
    assert README_EXCHANGE in source
    source = source.replace(README_EXCHANGE, exchange)
    if wait is not None:
        source, n = re.subn(r"^WAIT = [0-9]+\b", f"WAIT = {wait}", source, flags=re.M)
        assert n == 1, "no 'WAIT = <seconds>' line in send_command.py"
    return source


def run_send_command(tmp_path: Path, source: str) -> subprocess.CompletedProcess[str]:
    script = tmp_path / "send_command.py"
    script.write_text(source, encoding="utf-8")
    env = dict(
        os.environ,
        KBM_LEDSAS_RABBITMQ_URL=RABBITMQ_URL,
        KBM_LEDSAS_BLOB_CONN_STRING=BLOB_CONN_STRING,
    )
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_no_service_exchange_is_one_line(tmp_path, command_exchange):
    r = run_send_command(tmp_path, send_command_source(command_exchange))
    assert r.returncode == 1, r
    assert "Traceback" not in r.stderr
    assert r.stderr.strip().splitlines() == [
        f"error: no exchange {command_exchange}: start my_service.py first"
    ]


def test_no_reply_is_one_line(tmp_path, command_exchange):
    if not _port_open(*_blob_endpoint()):
        pytest.skip("no blob storage emulator (LEDSAS_TEST_BLOB_CONN_STRING / Azurite on :10000)")

    async def declare(create: bool) -> None:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
        async with connection:
            channel = await connection.channel()
            if create:
                await channel.declare_exchange(
                    command_exchange, aio_pika.ExchangeType.DIRECT, durable=True, passive=False
                )
            else:
                await channel.exchange_delete(command_exchange)

    asyncio.run(declare(True))  # a service that declared its exchange, then stopped
    try:
        r = run_send_command(tmp_path, send_command_source(command_exchange, wait=2))
    finally:
        asyncio.run(declare(False))
    assert r.returncode == 1, r
    assert "Traceback" not in r.stderr
    assert r.stderr.strip().splitlines() == [
        "error: no reply within 2 s "
        "(is my_service.py running, and are the broker and storage reachable?)"
    ], r.stderr


def _docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True).returncode == 0


def test_the_rabbitmq_wait_ends_when_the_port_is_taken(tmp_path):
    """The README's RabbitMQ lines with their host port already in use: the
    ``docker run`` fails and the wait that follows ends at once, instead of
    waiting for a log line that never comes."""
    if not _docker_available():
        pytest.skip("Docker is not available")
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    quickstart = readme[readme.index("## Quickstart") : readme.index("### Send it a command")]
    block = re.search(r"^```bash\n(.*?)^```", quickstart, re.M | re.S).group(1)
    lines = block.splitlines()
    start = next(i for i, ln in enumerate(lines) if "--name ledsas-quickstart-rabbitmq" in ln)
    end = next(i for i, ln in enumerate(lines) if ln.startswith("done")) + 1
    rabbit_and_wait = "\n".join(
        [lines[start]] + [ln for ln in lines[start + 1 : end] if "azurite" not in ln]
    )
    name = f"ledsas-quickstart-test-{uuid.uuid4().hex[:8]}"
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen()
        port = taken.getsockname()[1]
        script = rabbit_and_wait.replace("ledsas-quickstart-rabbitmq", name).replace(
            "127.0.0.1:5672:5672", f"127.0.0.1:{port}:5672"
        )
        assert name in script and f":{port}:5672" in script
        began = time.monotonic()
        try:
            r = subprocess.run(
                ["sh", "-c", script], capture_output=True, text=True, timeout=180, cwd=tmp_path
            )
        finally:
            subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True)
    assert time.monotonic() - began < 60, r
    assert "address already in use" in r.stderr or "port is already allocated" in r.stderr, r


def _needs_blob_storage() -> None:
    if not _port_open(*_blob_endpoint()):
        pytest.skip("no blob storage emulator (LEDSAS_TEST_BLOB_CONN_STRING / Azurite on :10000)")


def test_an_error_reply_is_one_line(tmp_path, command_exchange):
    """A service that answers with an error reply (here: no handler for the
    command): one ``error: <code>: <message>`` line on stderr, exit 1."""
    _needs_blob_storage()

    async def scenario() -> tuple[int, str, str]:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
        async with connection:
            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                command_exchange, aio_pika.ExchangeType.DIRECT, durable=True
            )
            queue = await channel.declare_queue(exclusive=True)
            await queue.bind(exchange, routing_key="command")

            async def answer(message: aio_pika.abc.AbstractIncomingMessage) -> None:
                async with message.process():
                    envelope = json.loads(message.body)["envelope"]
                reply = {
                    "envelope": {**envelope, "type": "response"},
                    "payload": {
                        "error": {
                            "code": "HandlerNotFound",
                            "message": "no handler\nfor ProcessCSV",
                            "retryable": False,
                        }
                    },
                }
                reply_exchange = await channel.get_exchange(envelope["reply_to"], ensure=True)
                await reply_exchange.publish(
                    aio_pika.Message(json.dumps(reply).encode()), routing_key="response"
                )

            await queue.consume(answer)
            script = tmp_path / "send_command.py"
            script.write_text(send_command_source(command_exchange, wait=20), encoding="utf-8")
            env = dict(
                os.environ,
                KBM_LEDSAS_RABBITMQ_URL=RABBITMQ_URL,
                KBM_LEDSAS_BLOB_CONN_STRING=BLOB_CONN_STRING,
            )
            sender = await asyncio.create_subprocess_exec(
                sys.executable,
                str(script),
                cwd=tmp_path,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                out, err = await asyncio.wait_for(sender.communicate(), timeout=60)
            finally:
                await channel.exchange_delete(command_exchange)
            return sender.returncode, out.decode(), err.decode()

    rc, out, err = asyncio.run(scenario())
    assert rc == 1, (rc, out, err)
    assert "Traceback" not in err
    assert err.strip().splitlines() == ["error: HandlerNotFound: no handler for ProcessCSV"], err
    assert out == ""


async def _exists(kind: str, name: str) -> bool:
    connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
    async with connection:
        channel = await connection.channel()
        try:
            if kind == "queue":
                await channel.declare_queue(name, passive=True)
            else:
                await channel.get_exchange(name, ensure=True)
            return True
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False


def test_sigterm_leaves_nothing_behind(tmp_path, command_exchange):
    """A SIGTERM while the snippet waits (a service that declared its
    exchange, then stopped): it ends, and its reply queue and exchange are
    gone."""
    _needs_blob_storage()
    reply_exchange = f"reply-ex-quickstart-test-{uuid.uuid4().hex[:8]}"
    source = send_command_source(command_exchange)
    assert README_REPLY_EXCHANGE in source
    source = source.replace(README_REPLY_EXCHANGE, repr(reply_exchange))

    async def declare(create: bool) -> None:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
        async with connection:
            channel = await connection.channel()
            if create:
                await channel.declare_exchange(
                    command_exchange, aio_pika.ExchangeType.DIRECT, durable=True
                )
            else:
                await channel.exchange_delete(command_exchange)

    asyncio.run(declare(True))
    script = tmp_path / "send_command.py"
    script.write_text(source, encoding="utf-8")
    env = dict(
        os.environ,
        KBM_LEDSAS_RABBITMQ_URL=RABBITMQ_URL,
        KBM_LEDSAS_BLOB_CONN_STRING=BLOB_CONN_STRING,
    )
    sender = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 30
        while not asyncio.run(_exists("exchange", reply_exchange)):
            assert sender.poll() is None and time.monotonic() < deadline, sender.communicate()
            time.sleep(0.2)
        time.sleep(1)  # waiting for the reply
        sender.send_signal(signal.SIGTERM)
        out, err = sender.communicate(timeout=30)
    finally:
        if sender.poll() is None:
            sender.kill()
        asyncio.run(declare(False))
    assert sender.returncode == 1, (out, err)
    assert "Traceback" not in err
    assert err.strip() == "interrupted", err
    assert not asyncio.run(_exists("queue", f"{reply_exchange}-q"))
    assert not asyncio.run(_exists("exchange", reply_exchange))
