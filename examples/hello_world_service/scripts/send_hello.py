#!/usr/bin/env python3
"""Send a ``SayHello`` command to the hello-world service and print the reply.

This is the *caller* side of this example: it publishes a LEDSAS command
envelope to the service's command exchange, waits for the response on a
reply exchange it owns, and prints the greeting.

It deliberately uses **aio-pika directly** — NOT the LEDSAS SDK. The SDK is
the service/handler side; a caller (orchestrator, test rig, this script)
just speaks AMQP. aio-pika is already a transitive dependency of the SDK,
so no extra install is needed in the dev venv.

Reply-to contract (see "Replies: one per command" in the SDK API
reference): the caller owns the
reply topology. This script declares a reply exchange + queue + binding
(routing key ``response``), sets ``envelope.reply_to`` to that exchange name,
and the SDK publishes the handler's return value there. The response is
matched on ``correlation_id``; the script deletes its reply queue and
exchange when it exits (Ctrl+C and SIGTERM included). Should the process be
killed outright, the broker deletes the queue ``--timeout`` + 60 s after its
last use, and the exchange with it. The command expires after
``--timeout``: a service that starts later never runs it.

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
    KBM_LEDSAS_RABBITMQ_URL   (default amqp://guest:guest@127.0.0.1:5672/; the
                              broker used is printed at the start, password
                              masked)

Every failure prints one ``error:`` line: an invalid broker URL, a broker
that cannot be reached or does not answer, refused credentials, a service
that has not started (no command exchange), no reply in time, or an error
reply (``error: <code>: <message>``). ``--timeout`` bounds the whole run,
the connection attempt included.

Exit codes: 0 = greeting received; 1 = timeout / error / error reply;
2 = invalid arguments; 130 = interrupted (Ctrl+C); 143 = terminated
(SIGTERM).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import signal
import sys
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

# ---------------------------------------------------------------------------
# Pure, import-and-test-without-a-broker helpers.
# ---------------------------------------------------------------------------

DEFAULT_SERVICE_NAME = "hello-world"
DEFAULT_RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/"
# The service sends one reply per command, after its last attempt, so the
# wait must cover the retries: with the SDK defaults (3 retries, 7-10 s of
# backoff) 30 s covers a handler that fails fast on every attempt.
DEFAULT_TIMEOUT_S = 30.0
MAX_TIMEOUT_S = 86400.0
# The broker deletes a reply queue unused for this long after the wait (a
# backstop for a process killed before its own cleanup).
REPLY_QUEUE_GRACE_S = 60
# One connection attempt may take at most this long (within --timeout): an
# unreachable or firewalled host otherwise waits for the operating system's
# own timeout.
CONNECT_TIMEOUT_S = 10.0


class SendError(Exception):
    """A failure the script reports as one ``error:`` line (exit code 1)."""


def build_command_exchange_name(tenant: str | None, service_name: str) -> str:
    """The service's command exchange, as documented in the SDK API reference
    ("AMQP Topology"):

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


def timeout_arg(text: str) -> float:
    """argparse type for ``--timeout``: seconds, above 0 and at most a day."""
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a number of seconds") from None
    if not math.isfinite(value) or not 0 < value <= MAX_TIMEOUT_S:
        raise argparse.ArgumentTypeError(
            f"must be a number of seconds above 0 and at most {MAX_TIMEOUT_S:.0f}"
        )
    return value


def reply_error(payload: Any) -> dict[str, Any] | None:
    """The ``error`` object of an error reply (``code``, ``message`` and
    ``retryable``), or ``None`` for a result (see "Telling an error reply from
    a result" in the SDK API reference)."""
    error = payload.get("error") if isinstance(payload, dict) else None
    if isinstance(error, dict) and {"code", "message", "retryable"} <= error.keys():
        return error
    return None


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

    Matches the message envelope format documented in the SDK API reference
    ("Message Format").
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


def broker_url_problem(url: str) -> str | None:
    """Why ``url`` cannot name a broker (``None`` when it can): the scheme must
    be ``amqp://`` or ``amqps://``, a host must follow it, and a port is 1 to
    65535. Checked before connecting; the text never repeats the URL."""
    try:
        parts = urlsplit(url)
        host = parts.hostname
        port = parts.port  # raises ValueError for a port that is not a number
    except ValueError as e:
        return f"KBM_LEDSAS_RABBITMQ_URL is not a valid URL ({e})"
    if parts.scheme not in ("amqp", "amqps"):
        return "KBM_LEDSAS_RABBITMQ_URL must start with amqp:// or amqps://"
    if not host:
        return "KBM_LEDSAS_RABBITMQ_URL names no host (amqp://user:password@host:5672/)"
    if port is not None and not 1 <= port <= 65535:
        return "KBM_LEDSAS_RABBITMQ_URL has a port outside 1-65535"
    return None


def mask_url(url: str) -> str:
    """``url`` with its password (and anything after ``?``) replaced by ``***``."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "***"
    netloc = parts.netloc
    if "@" in netloc:
        userinfo, _, hostport = netloc.rpartition("@")
        user = userinfo.split(":", 1)[0]
        netloc = f"{user}:***@{hostport}" if ":" in userinfo else f"{user}@{hostport}"
    query = "?***" if parts.query else ""
    return f"{parts.scheme}://{netloc}{parts.path}{query}"


class Env:
    """Resolved environment configuration (with documented defaults)."""

    def __init__(self, environ: dict[str, str] | None = None) -> None:
        e = environ if environ is not None else os.environ
        self.service_name = e.get("KBM_LEDSAS_SERVICE_NAME", DEFAULT_SERVICE_NAME)
        self.tenant = e.get("KBM_LEDSAS_TENANT") or None
        self.rabbitmq_url_is_default = not e.get("KBM_LEDSAS_RABBITMQ_URL")
        self.rabbitmq_url = e.get("KBM_LEDSAS_RABBITMQ_URL") or DEFAULT_RABBITMQ_URL

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
    progress: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """Declare reply topology, publish the command, await the matching reply.

    The reply queue and exchange are deleted however this ends (reply
    received, timeout, a failed publish), so a run leaves nothing behind on
    the broker; when the command exchange does not exist, none is created.
    One connection attempt (no reconnecting): a broker that does not answer
    fails the run. ``progress["connected"]`` is set once the connection is
    open, so a run that times out can tell an unreachable broker from a
    missing reply.
    """
    import aio_pika

    broker = mask_url(env.rabbitmq_url)
    try:
        connection = await aio_pika.connect(
            env.rabbitmq_url, timeout=min(CONNECT_TIMEOUT_S, timeout)
        )
    except aio_pika.exceptions.ProbableAuthenticationError:
        raise SendError(
            f"the broker at {broker} refused the user name or password in "
            "KBM_LEDSAS_RABBITMQ_URL"
        ) from None
    except (aio_pika.exceptions.AMQPConnectionError, OSError, TimeoutError) as e:
        # Not running, wrong host or port, unknown virtual host, ... (the
        # class only: the library's text may quote the URL).
        raise SendError(f"cannot connect to the broker at {broker} ({type(e).__name__})") from None
    if progress is not None:
        progress["connected"] = True
    try:
        channel = await connection.channel()

        # Reference the command exchange the service declares (do NOT
        # redeclare it with conflicting args — passive lookup). Looked up
        # first: when the service has never started, nothing is created.
        try:
            cmd_ex = await channel.get_exchange(env.command_exchange, ensure=True)
        except aio_pika.exceptions.ChannelNotFoundEntity:
            raise SendError(
                f"the exchange {env.command_exchange} does not exist: start the "
                "service first (python main.py), and check KBM_LEDSAS_SERVICE_NAME "
                "and KBM_LEDSAS_TENANT"
            ) from None

        rx = None
        rq = None
        consumer_tag = None
        try:
            # Caller-owned reply topology (the SDK does NOT declare this).
            # auto_delete: the broker deletes the exchange once its queue is
            # gone (see x-expires below).
            rx = await channel.declare_exchange(
                reply_exchange, aio_pika.ExchangeType.TOPIC, durable=True, auto_delete=True
            )
            # durable=True + auto_delete=False: an auto_delete reply queue
            # vanishes the moment its consumer disconnects, unbinding the
            # exchange — after which every response the SDK publishes is
            # unroutable and dead-letters the command it answers (the SDK
            # publishes replies mandatory). This script owns the queue and
            # deletes it in the finally below; x-expires is the backstop
            # for a process killed before that runs.
            rq = await channel.declare_queue(
                reply_queue,
                durable=True,
                auto_delete=False,
                arguments={"x-expires": int((timeout + REPLY_QUEUE_GRACE_S) * 1000)},
            )
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
                    # A command not taken within the wait expires: a service
                    # that starts later never runs it for a caller gone.
                    expiration=timeout,
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


def _error_line(error: BaseException, env: Env) -> str:
    """One line for a failed run (the broker URL is never echoed)."""
    if isinstance(error, SendError):
        return f"error: {error}"
    text = " ".join(str(error).split()).replace(env.rabbitmq_url, mask_url(env.rabbitmq_url))
    return f"error: publish/receive failed: {type(error).__name__}: {text or 'no details'}"


async def _run(args: argparse.Namespace) -> int:
    env = Env()
    correlation_id = str(uuid.uuid4())
    idempotency_key = args.idempotency_key or build_idempotency_key()
    reply_exchange = f"reply-ex-hello-{uuid.uuid4().hex[:8]}"
    reply_queue = f"{reply_exchange}-q"

    problem = broker_url_problem(env.rabbitmq_url)
    if problem:
        print(f"error: {problem}", file=sys.stderr)
        return 1
    default = (
        " (KBM_LEDSAS_RABBITMQ_URL is unset: the default)" if env.rabbitmq_url_is_default else ""
    )
    print(f"Broker:       {mask_url(env.rabbitmq_url)}{default}")
    print(f"Service:      {env.service_name} (tenant={env.tenant})")
    print(f"Cmd exchange: {env.command_exchange}")
    print(f"Idempotency:  {idempotency_key} (pass --idempotency-key to resend this request)")
    print(f"Reply to:     {reply_exchange} (queue {reply_queue}; deleted when the run ends)")

    message = build_command_message(
        reply_to=reply_exchange,
        idempotency_key=idempotency_key,
        name=args.name,
        correlation_id=correlation_id,
    )

    print(f"Publishing SayHello, waiting <= {args.timeout:.0f}s for response ...")
    progress = {"connected": False}
    try:
        # --timeout bounds everything: the connection attempt, the lookup of
        # the command exchange and the wait for the reply.
        response = await asyncio.wait_for(
            _publish_and_wait(
                env,
                message=message,
                reply_exchange=reply_exchange,
                reply_queue=reply_queue,
                correlation_id=correlation_id,
                timeout=args.timeout,
                progress=progress,
            ),
            timeout=args.timeout,
        )
    except TimeoutError:
        if not progress["connected"]:
            print(
                f"error: cannot connect to the broker at {mask_url(env.rabbitmq_url)} "
                f"(no answer within {args.timeout:g}s)",
                file=sys.stderr,
            )
            return 1
        print(
            f"error: no response within {args.timeout:.0f}s "
            "(is the service running and consuming?)",
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(_error_line(e, env), file=sys.stderr)
        return 1

    payload = response.get("payload")
    error = reply_error(payload)
    if error is not None:
        text = " ".join(str(error["message"]).split())
        print(f"error: {error['code']}: {text}", file=sys.stderr)
        return 1
    if not isinstance(payload, dict) or not payload.get("greeting"):
        print(f"error: the reply carries no greeting: {payload!r}", file=sys.stderr)
        return 1
    print("\n=== Response ===")
    print(f"greeting: {payload['greeting']}")
    return 0


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
        type=timeout_arg,
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


async def _run_until_sigterm(args: argparse.Namespace) -> int:
    """``_run``, cancelled on SIGTERM so that its cleanup (the reply queue and
    exchange) runs, as on Ctrl+C. (No SIGTERM handler where the event loop
    cannot install one, as on Windows.)"""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    assert task is not None
    terminated = False

    def on_sigterm() -> None:
        nonlocal terminated
        terminated = True
        task.cancel()

    try:
        loop.add_signal_handler(signal.SIGTERM, on_sigterm)
        installed = True
    except (NotImplementedError, RuntimeError):
        installed = False
    try:
        return await _run(args)
    except asyncio.CancelledError:
        if not terminated:
            raise
        print("terminated", file=sys.stderr)
        return 143
    finally:
        if installed:
            loop.remove_signal_handler(signal.SIGTERM)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # The AMQP libraries log their own connection errors (with the broker URL);
    # this script reports every failure in one line of its own instead.
    for name in ("aiormq", "aio_pika"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    try:
        return asyncio.run(_run_until_sigterm(args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
