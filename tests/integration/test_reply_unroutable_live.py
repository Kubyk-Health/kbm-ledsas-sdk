"""
Live reply-routability tests against a real RabbitMQ broker.

Empirical proof of the ``mandatory=True`` reply-publish behavior on a
real broker (the unit tests mock ``exchange.publish``; these pin the
actual aio-pika/aiormq semantics, guarding against aio-pika/aiormq version drift):

- reply_to exchange exists but has NO bound queue  -> ``send_response()``
  returns False and bumps ``reply_unroutable_failures`` (before v0.3.3
  this publish silently succeeded and the response vanished).
- reply_to exchange exists WITH a bound queue      -> returns True and
  the response actually lands in the queue.
- reply_to exchange missing                        -> returns False,
  ``reply_unroutable_failures`` NOT bumped (path unchanged).

All tests are marked ``@pytest.mark.integration`` and skip cleanly if
the Docker stack (RabbitMQ + Azurite) isn't available.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import aio_pika
import pytest
import pytest_asyncio

from kbm_ledsas_sdk.models.envelope import Envelope
from kbm_ledsas_sdk.models.messages import Response
from kbm_ledsas_sdk.transport.direct import DirectTransport

RABBITMQ_HOST = "127.0.0.1"
RABBITMQ_PORT = 5672
RABBITMQ_URL = f"amqp://guest:guest@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
AZURITE_CONN_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


_STACK_UP = _port_open(RABBITMQ_HOST, RABBITMQ_PORT) and _port_open("127.0.0.1", 10000)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _STACK_UP,
        reason=(
            "RabbitMQ/Azurite not reachable on loopback — start the stack: "
            "docker compose -f examples/hello_world_service/deploy/local/docker-compose.yml up -d"
        ),
    ),
]

# Unique per test session so leftover state from a crashed run can't
# make an "unbound" exchange accidentally bound. Used ONLY for the
# reply-side objects each test creates and deletes itself — NOT for the
# transport's service name, whose durable cmd/DLQ topology is never
# torn down and would accumulate on the broker if uniquified per run.
_RUN = uuid.uuid4().hex[:8]

# Fixed service name: declare_topology() is idempotent, so re-runs reuse
# the same 4 durable cmd/DLQ objects instead of leaking new ones.
SERVICE_NAME = "reply-pub-live"


def _response(reply_to: str) -> Response:
    return Response(
        envelope=Envelope(
            schema_version="1.0",
            type="response",
            name="TestCommand",
            message_version="1.0",
            message_id=str(uuid.uuid4()),
            correlation_id=str(uuid.uuid4()),
            idempotency_key=f"idem-reply-{uuid.uuid4().hex[:8]}",
            sent_at=datetime.now(UTC),
            trace_id="00-test-trace-id",
            reply_to=reply_to,
        ),
        payload={"result": "live"},
    )


@pytest_asyncio.fixture
async def transport() -> AsyncIterator[DirectTransport]:
    t = DirectTransport(
        rabbitmq_url=RABBITMQ_URL,
        blob_conn_string=AZURITE_CONN_STRING,
        service_name=SERVICE_NAME,
    )
    await t.start()
    try:
        yield t
    finally:
        await t.stop()


@pytest_asyncio.fixture
async def broker_channel() -> AsyncIterator[aio_pika.abc.AbstractChannel]:
    """A caller-side channel for declaring the reply infrastructure."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    try:
        yield channel
    finally:
        await channel.close()
        await connection.close()


@pytest.mark.asyncio
async def test_exists_unbound_exchange_returns_false_and_counts(
    transport: DirectTransport,
    broker_channel: aio_pika.abc.AbstractChannel,
):
    """The headline case: exchange declared, no queue bound.

    Pre-fix (mandatory=False) the publish succeeded and the response
    vanished; now the broker returns the message and send_response()
    reports the failure so the caller NACKs the command to DLQ.
    """
    exchange_name = f"replypub.{_RUN}.unbound.v1"
    await broker_channel.declare_exchange(exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)
    try:
        result = await transport.send_response(_response(exchange_name))

        assert result is False
        assert transport.reply_publish_failures == 1
        assert transport.reply_unroutable_failures == 1
    finally:
        await broker_channel.exchange_delete(exchange_name)


@pytest.mark.asyncio
async def test_bound_exchange_delivers_and_returns_true(
    transport: DirectTransport,
    broker_channel: aio_pika.abc.AbstractChannel,
):
    """Correctly wired reply infrastructure keeps working: True + delivery."""
    exchange_name = f"replypub.{_RUN}.bound.v1"
    queue_name = f"replypub.{_RUN}.bound.q"
    exchange = await broker_channel.declare_exchange(
        exchange_name, aio_pika.ExchangeType.TOPIC, durable=True
    )
    queue = await broker_channel.declare_queue(queue_name, durable=True, auto_delete=False)
    await queue.bind(exchange, routing_key="response")
    try:
        result = await transport.send_response(_response(exchange_name))

        assert result is True
        assert transport.reply_publish_failures == 0
        assert transport.reply_unroutable_failures == 0

        delivered = await queue.get(timeout=5)
        assert delivered is not None
        await delivered.ack()
    finally:
        await queue.unbind(exchange, routing_key="response")
        await broker_channel.queue_delete(queue_name)
        await broker_channel.exchange_delete(exchange_name)


@pytest.mark.asyncio
async def test_missing_exchange_returns_false_without_unroutable_count(
    transport: DirectTransport,
):
    """Missing-exchange path unchanged: False, aggregate counter only."""
    exchange_name = f"replypub.{_RUN}.missing.v1"

    result = await transport.send_response(_response(exchange_name))

    assert result is False
    assert transport.reply_publish_failures == 1
    assert transport.reply_unroutable_failures == 0
