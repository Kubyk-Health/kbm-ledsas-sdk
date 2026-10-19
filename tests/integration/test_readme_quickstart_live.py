"""The README Quickstart's ``send_command.py``, run as written against a live
RabbitMQ, when the service is not there to answer.

A caller that copies the snippet must get one ``error:`` line and exit
status 1, never a traceback, when the service has never started (its
command exchange does not exist) and when the service does not answer in
time. The broker is ``LEDSAS_TEST_RABBITMQ_URL`` when set, else
``amqp://guest:guest@127.0.0.1:5672/`` (see CONTRIBUTING.md); the second
case also needs the blob storage emulator (``LEDSAS_TEST_BLOB_CONN_STRING``,
else Azurite on 127.0.0.1:10000) and skips without it.
"""

from __future__ import annotations

import asyncio
import os
import re
import socket
import subprocess
import sys
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
COMMAND_EXCHANGE = "cmd.csv-processor.v1"


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


def send_command_source(wait: int | None = None) -> str:
    """The README's second python block (the file it says to save as send_command.py)."""
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    after = readme[readme.index("Save this as `send_command.py`") :]
    source = re.search(r"^```python\n(.*?)^```", after, re.M | re.S).group(1)
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


async def _exchange_exists() -> bool:
    connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
    async with connection:
        channel = await connection.channel()
        try:
            await channel.get_exchange(COMMAND_EXCHANGE, ensure=True)
            return True
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return False


def test_no_service_exchange_is_one_line(tmp_path):
    if asyncio.run(_exchange_exists()):
        pytest.skip(f"{COMMAND_EXCHANGE} exists on this broker (a quickstart service ran here)")
    r = run_send_command(tmp_path, send_command_source())
    assert r.returncode == 1, r
    assert "Traceback" not in r.stderr
    assert r.stderr.strip().splitlines() == [
        f"error: no exchange {COMMAND_EXCHANGE}: start my_service.py first"
    ]


def test_no_reply_is_one_line(tmp_path):
    if not _port_open(*_blob_endpoint()):
        pytest.skip("no blob storage emulator (LEDSAS_TEST_BLOB_CONN_STRING / Azurite on :10000)")

    async def declare(create: bool) -> None:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
        async with connection:
            channel = await connection.channel()
            if create:
                await channel.declare_exchange(
                    COMMAND_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True, passive=False
                )
            else:
                await channel.exchange_delete(COMMAND_EXCHANGE)

    if asyncio.run(_exchange_exists()):
        pytest.skip(f"{COMMAND_EXCHANGE} exists on this broker (a quickstart service ran here)")
    asyncio.run(declare(True))  # a service that declared its exchange, then stopped
    try:
        r = run_send_command(tmp_path, send_command_source(wait=2))
    finally:
        asyncio.run(declare(False))
    assert r.returncode == 1, r
    assert "Traceback" not in r.stderr
    assert r.stderr.strip().splitlines() == [
        "error: no reply within 2 s (is my_service.py running?)"
    ], r.stderr
