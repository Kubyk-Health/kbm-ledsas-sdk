"""The example's ``scripts/send_hello.py`` against a live RabbitMQ, with a
service that declared its command exchange and queue and then stopped.

The command it sends expires after ``--timeout`` (a service started later
never runs it), and the reply exchange and queue it declares are gone when
it ends, after a timeout and after a SIGTERM. The broker is
``LEDSAS_TEST_RABBITMQ_URL`` when set, else
``amqp://guest:guest@127.0.0.1:5672/`` (see CONTRIBUTING.md).
"""

from __future__ import annotations

import asyncio
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import aio_pika
import pytest

from .test_reply_unroutable_live import RABBITMQ_URL, pytestmark  # noqa: F401 (the skip)

SENDER = Path(__file__).resolve().parents[2] / "examples" / "hello_world_service" / "scripts"


@pytest.fixture
def stopped_service():
    """A command exchange and queue of a service that is not running."""
    service = f"hello-test-{uuid.uuid4().hex[:8]}"
    exchange, queue = f"cmd.{service}.v1", f"queue.{service}.v1"

    async def declare(create: bool) -> None:
        connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
        async with connection:
            channel = await connection.channel()
            if create:
                ex = await channel.declare_exchange(
                    exchange, aio_pika.ExchangeType.DIRECT, durable=True
                )
                q = await channel.declare_queue(queue, durable=True)
                await q.bind(ex, routing_key="command")
            else:
                await channel.queue_delete(queue)
                await channel.exchange_delete(exchange)

    asyncio.run(declare(True))
    try:
        yield service, queue
    finally:
        asyncio.run(declare(False))


def _start(service: str, *args: str) -> subprocess.Popen[str]:
    env = dict(os.environ, KBM_LEDSAS_RABBITMQ_URL=RABBITMQ_URL, KBM_LEDSAS_SERVICE_NAME=service)
    env["PYTHONUNBUFFERED"] = "1"  # the test reads the progress lines as they come
    env.pop("KBM_LEDSAS_TENANT", None)
    return subprocess.Popen(
        [sys.executable, str(SENDER / "send_hello.py"), *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


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


async def _messages(queue: str) -> int:
    connection = await aio_pika.connect(RABBITMQ_URL, timeout=10)
    async with connection:
        channel = await connection.channel()
        declared = await channel.declare_queue(queue, passive=True)
        return declared.declaration_result.message_count


def _reply_names(stdout: str) -> tuple[str, str]:
    match = re.search(r"^Reply to: +(\S+) \(queue (\S+);", stdout, re.M)
    assert match, stdout
    return match.group(1), match.group(2)


def test_the_command_expires_and_nothing_is_left(stopped_service):
    service, command_queue = stopped_service
    sender = _start(service, "--timeout", "2")
    out, err = sender.communicate(timeout=30)
    assert sender.returncode == 1, (out, err)
    assert err.strip().splitlines()[-1].startswith("error: no response within 2s"), err
    time.sleep(1)
    assert asyncio.run(_messages(command_queue)) == 0  # expired: a later start never runs it
    exchange, queue = _reply_names(out)
    assert not asyncio.run(_exists("queue", queue))
    assert not asyncio.run(_exists("exchange", exchange))


def test_sigterm_leaves_nothing_behind(stopped_service):
    service, _ = stopped_service
    sender = _start(service, "--timeout", "30")
    deadline = time.monotonic() + 15
    lines = []
    while time.monotonic() < deadline:
        line = sender.stdout.readline()
        lines.append(line)
        if line.startswith("Publishing SayHello"):
            break
    time.sleep(1)
    sender.send_signal(signal.SIGTERM)
    out, err = sender.communicate(timeout=30)
    assert sender.returncode == 143, (out, err)
    assert err.strip() == "terminated", err
    exchange, queue = _reply_names("".join(lines))
    assert not asyncio.run(_exists("queue", queue))
    assert not asyncio.run(_exists("exchange", exchange))
