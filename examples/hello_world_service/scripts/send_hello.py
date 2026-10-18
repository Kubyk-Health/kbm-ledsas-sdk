#!/usr/bin/env python3
"""Send a ``SayHello`` command to the hello-world service and print the reply.

This is the *caller* side of this example: it publishes a LEDSAS command
envelope to the service's command exchange, waits for the response on a
reply exchange it owns, and prints the greeting.

It deliberately uses **aio-pika directly** — NOT the LEDSAS SDK. The SDK is
the service/handler side; a caller (orchestrator, test rig, this script)
just speaks AMQP. aio-pika is already a transitive dependency of the SDK,
so no extra install is needed in the dev venv.

Reply-to contract (see docs/SDK_API_REFERENCE.md): the caller owns the
reply topology. This script declares a reply exchange + queue + binding
(routing key ``response``), sets ``envelope.reply_to`` to that exchange name,
and the SDK publishes the handler's return value there. The response is
matched on ``correlation_id``; the script deletes its reply queue and
exchange when it exits, whatever happened.

Usage::

    python scripts/send_hello.py            # -> "hello world"
    python scripts/send_hello.py Ada        # -> "hello Ada"
    python scripts/send_hello.py Ada --idempotency-key hello-1   # a deliberate resend

Each run sends a new request (a new idempotency key, printed at the
start). Pass the key of an earlier run with ``--idempotency-key`` to resend
that request on purpose.

Environment (read from process env; see ../.env.example):
    KBM_LEDSAS_SERVICE_NAME   (default hello-world)
    KBM_LEDSAS_TENANT         (default unset — no tenant)
    KBM_LEDSAS_RABBITMQ_URL   (default amqp://guest:guest@127.0.0.1:5672/)

Exit codes: 0 = greeting received; 1 = timeout / error.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

# ---------------------------------------------------------------------------
# Pure, import-and-test-without-a-broker helpers.
# ---------------------------------------------------------------------------

DEFAULT_SERVICE_NAME = "hello-world"
DEFAULT_RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/"
# The service sends one reply per command, after its last attempt, so the
# wait must cover the retries: with the SDK defaults (3 retries, 7-10 s of
# backoff) 30 s covers a handler that fails fast on every attempt.
DEFAULT_TIMEOUT_S = 30.0


def build_command_exchange_name(tenant: str | None, service_name: str) -> str:
    """The service's command exchange, as documented in docs/SDK_API_REFERENCE.md:

    - with tenant:    cmd.{tenant}.{service}.v1
    - without tenant: cmd.{service}.v1
    """
    if tenant:
        return f"cmd.{tenant}.{service_name}.v1"
    return f"cmd.{service_name}.v1"


def build_idempotency_key() -> str:
    """A new idempotency key, ``hello-<32 hex digits>``, for a new request.

    One key per logical request: two sends of the same request must carry
    the same key (pass it with ``--idempotency-key``), and two different
    requests never share one, however close together they are sent.
    """
    return f"hello-{uuid.uuid4().hex}"


# The envelope's idempotency-key pattern: a key outside it makes the command
# an invalid envelope, dead-lettered without a reply.
_IDEMPOTENCY_KEY_RE = re.compile(r"^[A-Za-z0-9_\-:.]{1,128}$")


def idempotency_key_arg(text: str) -> str:
    """argparse type for ``--idempotency-key``: 1 to 128 letters, digits or
    ``_ - : .`` characters (the envelope's pattern)."""
    if not _IDEMPOTENCY_KEY_RE.match(text):
        raise argparse.ArgumentTypeError("must be 1 to 128 letters, digits or _ - : . characters")
    return text


def build_command_envelope(
    *,
    handler_name: str,
    reply_to: str,
    idempotency_key: str,
    correlation_id: str | None = None,
    message_id: str | None = None,
    trace_id: str | None = None,
    sent_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a LEDSAS command envelope (the routing/metadata block).

    Matches the message envelope format documented in docs/SDK_API_REFERENCE.md.
    """
    sent_at = sent_at or datetime.now(UTC)
    return {
        "schema_version": "1.0",
        "type": "command",
        "name": handler_name,
        "message_version": "1.0",
        "message_id": message_id or str(uuid.uuid4()),
        "correlation_id": correlation_id or str(uuid.uuid4()),
        "idempotency_key": idempotency_key,
        "sent_at": sent_at.isoformat(),
        "trace_id": trace_id or str(uuid.uuid4()),
        "reply_to": reply_to,
    }


def build_command_message(
    *,
    reply_to: str,
    idempotency_key: str,
    name: str | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Build the full {envelope, payload} SayHello command message."""
    payload: dict[str, Any] = {}
    if name is not None:
        payload["name"] = name
    return {
        "envelope": build_command_envelope(
            handler_name="SayHello",
            reply_to=reply_to,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
        ),
        "payload": payload,
    }


class Env:
    """Resolved environment configuration (with documented defaults)."""

    def __init__(self, environ: dict[str, str] | None = None) -> None:
        e = environ if environ is not None else os.environ
        self.service_name = e.get("KBM_LEDSAS_SERVICE_NAME", DEFAULT_SERVICE_NAME)
        self.tenant = e.get("KBM_LEDSAS_TENANT") or None
        self.rabbitmq_url = e.get("KBM_LEDSAS_RABBITMQ_URL", DEFAULT_RABBITMQ_URL)

    @property
    def command_exchange(self) -> str:
        return build_command_exchange_name(self.tenant, self.service_name)


# ---------------------------------------------------------------------------
# Live send/receive (imports aio_pika lazily so the helpers above can be
# imported + unit-tested without a broker or the SDK installed).
# ---------------------------------------------------------------------------


async def _publish_and_wait(
    env: Env,
    *,
    message: dict[str, Any],
    reply_exchange: str,
    reply_queue: str,
    correlation_id: str,
    timeout: float,
) -> dict[str, Any]:
    """Declare reply topology, publish the command, await the matching reply.

    The reply queue and exchange are deleted however this ends (reply
    received, timeout, a failed publish), so a run leaves nothing behind on
    the broker; when the command exchange does not exist, none is created.
    """
    import aio_pika

    connection = await aio_pika.connect_robust(env.rabbitmq_url)
    try:
        channel = await connection.channel()

        # Reference the command exchange the service declares (do NOT
        # redeclare it with conflicting args — passive lookup). Looked up
        # first: when the service has never started, nothing is created.
        cmd_ex = await channel.get_exchange(env.command_exchange, ensure=True)

        rx = None
        rq = None
        consumer_tag = None
        try:
            # Caller-owned reply topology (the SDK does NOT declare this).
            rx = await channel.declare_exchange(
                reply_exchange, aio_pika.ExchangeType.TOPIC, durable=True
            )
            # durable=True + auto_delete=False: an auto_delete reply queue
            # vanishes the moment its consumer disconnects, unbinding the
            # exchange — after which every response the SDK publishes is
            # unroutable and dead-letters the command it answers (the SDK
            # publishes replies mandatory). This script owns the queue and
            # deletes it in the finally below.
            rq = await channel.declare_queue(reply_queue, durable=True, auto_delete=False)
            await rq.bind(rx, routing_key="response")

            # Await the matching response.
            future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()

            async def on_message(msg: aio_pika.abc.AbstractIncomingMessage) -> None:
                async with msg.process():
                    try:
                        body = json.loads(msg.body)
                    except Exception:
                        return
                    env_block = body.get("envelope", {})
                    if env_block.get("correlation_id") == correlation_id:
                        if not future.done():
                            future.set_result(body)

            # Consume before publishing, so a fast reply cannot be missed.
            consumer_tag = await rq.consume(on_message)
            await cmd_ex.publish(
                aio_pika.Message(
                    body=json.dumps(message).encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="command",
            )
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            # Cleanup, whatever happened above (a cleanup error never hides
            # the original one): the reply queue is durable and not
            # auto_delete, so delete it explicitly, then the reply exchange.
            if rq is not None:
                if consumer_tag is not None:
                    try:
                        await rq.cancel(consumer_tag)
                    except Exception:
                        pass
                try:
                    await rq.delete(if_unused=False, if_empty=False)
                except Exception:
                    pass
            if rx is not None:
                try:
                    await rx.delete(if_unused=False)
                except Exception:
                    pass
    finally:
        await connection.close()


async def _run(args: argparse.Namespace) -> int:
    env = Env()
    correlation_id = str(uuid.uuid4())
    idempotency_key = args.idempotency_key or build_idempotency_key()
    reply_exchange = f"reply-ex-hello-{uuid.uuid4().hex[:8]}"
    reply_queue = f"{reply_exchange}-q"

    print(f"Service:      {env.service_name} (tenant={env.tenant})")
    print(f"Cmd exchange: {env.command_exchange}")
    print(f"Idempotency:  {idempotency_key} (pass --idempotency-key to resend this request)")

    message = build_command_message(
        reply_to=reply_exchange,
        idempotency_key=idempotency_key,
        name=args.name,
        correlation_id=correlation_id,
    )

    print(f"Publishing SayHello, waiting <= {args.timeout:.0f}s for response ...")
    try:
        response = await _publish_and_wait(
            env,
            message=message,
            reply_exchange=reply_exchange,
            reply_queue=reply_queue,
            correlation_id=correlation_id,
            timeout=args.timeout,
        )
    except TimeoutError:
        print(
            f"error: no response within {args.timeout:.0f}s "
            "(is the service running and consuming?)",
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(f"error: publish/receive failed: {e}", file=sys.stderr)
        return 1

    payload = response.get("payload", {})
    print("\n=== Response ===")
    print(f"greeting: {payload.get('greeting')}")
    return 0 if payload.get("greeting") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish a SayHello command and print the greeting."
    )
    parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="Optional name to greet (default: world).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=(
            "Seconds to wait for the response (default %(default)s). The one reply "
            "comes after the last attempt: allow the handler's run time times "
            "(KBM_LEDSAS_MAX_RETRIES + 1) plus the retry backoff (7 to 10 s with the "
            "default 3 retries); at least 30 s with the SDK defaults."
        ),
    )
    parser.add_argument(
        "--idempotency-key",
        type=idempotency_key_arg,
        default=None,
        help=(
            "Resend an earlier request on purpose: reuse its idempotency key "
            "(1 to 128 letters, digits or _ - : .; default: a new key for every run)."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
