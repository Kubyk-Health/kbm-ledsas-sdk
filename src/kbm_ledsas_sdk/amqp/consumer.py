"""
AMQP command consumer for Direct transport mode.
"""

import asyncio
import json
import logging

from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from pydantic import ValidationError

from kbm_ledsas_sdk.models.messages import Command

logger = logging.getLogger(__name__)


class AMQPConsumer:
    """
    Consumes commands from RabbitMQ queue.

    This consumer is used in Direct transport mode (dev/testing).
    It provides a simple interface for consuming LEDSAS commands
    from a RabbitMQ queue.

    Features:
    - Manual message acknowledgment (no auto-ack)
    - Pushes commands to asyncio.Queue for async iteration
    - Tracks pending messages for ACK/NACK correlation
    - JSON parsing with validation

    Attributes:
        queue: RabbitMQ queue to consume from
        command_queue: Internal asyncio.Queue for commands
        pending_messages: Maps message_id -> AMQP IncomingMessage
        consumer_tag: RabbitMQ consumer tag (set when started)
    """

    def __init__(
        self,
        queue: AbstractQueue,
        prefetch_count: int = 10,
        max_pending_multiplier: int = 10,
        max_payload_bytes: int = 16 * 1024 * 1024,
    ):
        """
        Initialize AMQP consumer.

        Args:
            queue: RabbitMQ queue to consume from
            prefetch_count: Number of messages to prefetch
            max_pending_multiplier: Cap on ``pending_messages`` dict
                size, expressed as a multiple of ``prefetch_count``.
                A consumer with prefetch=10 and multiplier=10 caps at
                100 in-flight entries — well above legitimate steady
                state, low enough that a hostile sender flooding
                distinct message_ids can't grow it without bound.
            max_payload_bytes: Reject messages whose body exceeds this
                size (DLQ them, single WARNING line). 0 disables the
                check. Default 16 MiB — large payloads should be
                shipped via blob storage, not AMQP body.
        """
        self.queue = queue
        self.prefetch_count = prefetch_count
        self.max_pending = max(prefetch_count * max_pending_multiplier, prefetch_count)
        self.max_payload_bytes = max_payload_bytes

        # Internal queue for commands (consumed by Transport.subscribe())
        self.command_queue: asyncio.Queue[Command] = asyncio.Queue()

        # Track pending messages: message_id -> IncomingMessage
        self.pending_messages: dict[str, AbstractIncomingMessage] = {}

        # Retry attempt count per message_id. Incremented on every
        # nack(requeue=True) and cleared on ack() / nack(requeue=False).
        # app.py reads this via DirectTransport.get_retry_count() to
        # enforce KBM_LEDSAS_MAX_RETRIES. In-process only — survives
        # requeue cycles in the same consumer but resets across
        # consumer restarts (a restart is a fresh attempt from the
        # SDK's POV; the broker doesn't track per-message retry counts
        # for simple requeue, only for full DLX cycles).
        self.retry_counts: dict[str, int] = {}

        # Consumer tag (set when started)
        self.consumer_tag: str | None = None

    async def start(self) -> None:
        """
        Start consuming messages from the queue.

        Sets up consumer with manual acknowledgment and prefetch.

        Raises:
            Exception: If consumer startup fails
        """
        logger.info(
            "Starting AMQP consumer",
            extra={
                "queue": self.queue.name,
                "prefetch": self.prefetch_count,
            },
        )

        try:
            # Set prefetch count (QoS)
            await self.queue.channel.set_qos(prefetch_count=self.prefetch_count)

            # Start consuming with manual ack
            self.consumer_tag = await self.queue.consume(
                callback=self._on_message,
                no_ack=False,  # Manual acknowledgment
            )

            logger.info("AMQP consumer started", extra={"consumer_tag": self.consumer_tag})

        except Exception as e:
            logger.error("Failed to start AMQP consumer", exc_info=True, extra={"error": str(e)})
            raise

    async def stop(self) -> None:
        """
        Stop consuming messages.

        Cancels the consumer and clears pending messages.
        """
        if self.consumer_tag:
            logger.info("Stopping AMQP consumer")

            try:
                await self.queue.cancel(self.consumer_tag)
                self.consumer_tag = None

                # Log pending messages (they will be requeued by RabbitMQ)
                if self.pending_messages:
                    logger.warning(
                        f"Consumer stopped with {len(self.pending_messages)} "
                        "pending messages (will be requeued)"
                    )

                # Clear pending messages
                self.pending_messages.clear()

                logger.info("AMQP consumer stopped")

            except Exception as e:
                logger.error(
                    "Error stopping AMQP consumer",
                    exc_info=True,
                    extra={"error": str(e)},
                )

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """
        Handle incoming AMQP message.

        Parses JSON body to Command and pushes to internal queue.

        Args:
            message: Incoming AMQP message
        """
        try:
            # Payload size limit. Large payloads should travel
            # via blob storage, not the AMQP body. Reject oversize
            # messages straight to DLQ with a single WARNING — no
            # decode, no validate, bounded RSS pressure.
            if self.max_payload_bytes and len(message.body) > self.max_payload_bytes:
                logger.warning(
                    "Payload exceeds KBM_LEDSAS_MAX_PAYLOAD_BYTES; "
                    "dead-lettering (body=%d, max=%d). Use blob storage "
                    "for large data, not the AMQP body.",
                    len(message.body),
                    self.max_payload_bytes,
                )
                await message.reject(requeue=False)
                return

            # Deserialize message body to Command
            body = message.body.decode("utf-8")
            data = json.loads(body)
            command = Command.model_validate(data)

            # Enforce envelope.type=="command" on the command
            # exchange. The Envelope schema's Literal accepts four type
            # values (command/response/status/error) because the same
            # model is reused for outbound responses and status updates,
            # but only "command" is legitimate on a *command* queue. A
            # forged response/status/error envelope reaching this queue
            # is either a misconfigured upstream or a hostile reflection
            # attempt — dead-letter it without invoking the handler.
            envelope_type = command.envelope.type
            if envelope_type != "command":
                logger.warning(
                    "Envelope type %r is not 'command' on command " "exchange; dead-lettering",
                    envelope_type,
                    extra={
                        "message_id": command.envelope.message_id,
                        "envelope_type": envelope_type,
                    },
                )
                await message.reject(requeue=False)
                return

            message_id = command.envelope.message_id

            # Duplicate-id guard. If a sender resends the same
            # envelope.message_id while the original is still pending,
            # we can't safely overwrite the dict entry (the original
            # would become un-ack-able). Reject the duplicate to DLQ
            # immediately — almost certainly a replay attack or buggy
            # sender; the broker's x-death header records it.
            if message_id in self.pending_messages:
                logger.warning(
                    "Duplicate message_id while original still pending; "
                    "rejecting duplicate to DLQ",
                    extra={"message_id": message_id},
                )
                await message.reject(requeue=False)
                return

            # Cap pending_messages to bound memory. A hostile sender
            # can otherwise grow it without limit by streaming distinct
            # message_ids that crash before ack/nack. The cap is well
            # above legitimate steady state (prefetch × multiplier).
            if len(self.pending_messages) >= self.max_pending:
                logger.warning(
                    "pending_messages dict at cap; rejecting new message to DLQ "
                    "(in-flight=%d, cap=%d). Investigate stuck handlers.",
                    len(self.pending_messages),
                    self.max_pending,
                    extra={"message_id": message_id},
                )
                await message.reject(requeue=False)
                return

            self.pending_messages[message_id] = message

            logger.info(
                "Received command",
                extra={
                    "message_id": message_id,
                    "correlation_id": command.envelope.correlation_id,
                    "command_name": command.envelope.name,
                },
            )

            # Push to internal queue for Transport.subscribe()
            await self.command_queue.put(command)

        except UnicodeDecodeError as e:
            # The body isn't valid UTF-8 (e.g. a stray 0xff byte, or a
            # binary payload published to a text queue). This is the very
            # first step — ``message.body.decode("utf-8")`` — and it is
            # deterministic: the same bytes always fail the same way, so
            # requeueing would hot-loop. Dead-letter immediately.
            #
            # exc_info=False — like the JSONDecodeError branch, the error
            # message (codec + byte offset) is fully self-contained; a
            # traceback adds no diagnostic value and only amplifies
            # log volume when an attacker floods the queue with binary
            # bodies. Without this branch the decode error falls through
            # to the generic catch-all below (exc_info=True), which is the
            # one traceback that breaks the zero-traceback invariant.
            logger.error(
                "Message body is not valid UTF-8; dead-lettering",
                extra={"error": str(e)},
            )
            await message.reject(requeue=False)

        except json.JSONDecodeError as e:
            # Deterministic parse failure: same input → same error. No
            # point retrying. Dead-letter immediately.
            #
            # exc_info=False — the decoder error message is fully
            # self-contained; a Python traceback adds nothing
            # actionable here and contributes to log-noise on
            # malformed-message floods.
            logger.error("Failed to parse message JSON", extra={"error": str(e)})
            await message.reject(requeue=False)

        except RecursionError as e:
            # Deeply-nested JSON body. Python's json.loads is
            # recursive for nested structures and hits sys.getrecursionlimit
            # well before the 16 MiB payload cap. Classify this branch
            # explicitly (rather than lumping it into the generic
            # "Unexpected error" catch-all below) so operators can
            # alert on the pattern — it's a well-known DoS attack
            # signature.
            #
            # exc_info=False — the traceback repeats the same json
            # decoder frames thousands of times and adds no diagnostic
            # value beyond "the parser ran out of stack".
            logger.error(
                "Message body exceeds JSON-parsing nesting limit; " "dead-lettering",
                extra={"error_type": "ExcessiveNesting", "error": str(e)},
            )
            await message.reject(requeue=False)

        except ValidationError as e:
            # Pydantic envelope validation failed (missing fields, wrong
            # types, etc.). This is deterministic: requeueing would just
            # produce the same error forever and the consumer would burn
            # CPU + disk in a hot loop. Dead-letter immediately.
            #
            # Strip pydantic-docs URL noise from the logged error —
            # walk e.errors() like app.py's config-error path so the
            # log line stays short.
            msgs = []
            for err in e.errors():
                loc = ".".join(str(p) for p in err.get("loc", ()))
                msg = err.get("msg", "")
                if msg.startswith("Value error, "):
                    msg = msg[len("Value error, ") :]
                msgs.append(f"{loc}: {msg}" if loc else msg)
            error_text = "; ".join(msgs) if msgs else str(e)
            logger.error(
                "Envelope failed schema validation; dead-lettering",
                extra={"error": error_text},
            )
            await message.reject(requeue=False)

        except Exception as e:
            # Deterministic-friendly catch-all. If something in
            # decoding/validation raises a non-ValidationError
            # exception (bug, OOM during model_validate), requeueing
            # would just hot-loop. Dead-letter immediately — a retry
            # won't help if the very first deserialization step crashed.
            # RecursionError is handled by its own branch above
            # so we always emit exc_info here.
            logger.error(
                "Unexpected error processing message; dead-lettering",
                exc_info=True,
                extra={"error": str(e)},
            )
            await message.reject(requeue=False)

    async def consume(self) -> Command:
        """
        Consume next command from the queue.

        This is an async generator method used by Transport.subscribe().

        Returns:
            Next command from the queue

        Raises:
            asyncio.QueueEmpty: If no commands available
        """
        return await self.command_queue.get()

    async def ack(self, message_id: str) -> None:
        """
        Acknowledge successful command processing.

        Removes message from pending set and sends ACK to RabbitMQ.
        Also clears the per-message retry counter so a future delivery
        of the same id (rare but possible) starts fresh.

        Args:
            message_id: Message ID to acknowledge

        Raises:
            ValueError: If message_id not found in pending messages
        """
        message = self.pending_messages.pop(message_id, None)
        self.retry_counts.pop(message_id, None)
        if not message:
            logger.warning("Cannot ACK unknown message", extra={"message_id": message_id})
            raise ValueError(f"Unknown message_id: {message_id}")

        logger.debug("Acknowledging message", extra={"message_id": message_id})

        try:
            await message.ack()
        except Exception as e:
            logger.error(
                "Failed to ACK message",
                exc_info=True,
                extra={"message_id": message_id, "error": str(e)},
            )
            raise

    async def nack(self, message_id: str, requeue: bool = False) -> None:
        """
        Reject command processing (negative acknowledgment).

        In Direct mode, NACKed messages go to DLQ (requeue=False)
        or back to queue (requeue=True) per RabbitMQ DLQ configuration.

        Retry-count bookkeeping:
        - ``requeue=True``: increment retry_counts[message_id] BEFORE
          calling nack — the next delivery will see the higher count.
        - ``requeue=False`` (DLQ): clear retry_counts — the message is
          terminal, no further attempt will read its counter.

        Args:
            message_id: Message ID to reject
            requeue: Whether to requeue (default: False -> DLQ)

        Raises:
            ValueError: If message_id not found in pending messages
        """
        message = self.pending_messages.pop(message_id, None)
        if not message:
            logger.warning("Cannot NACK unknown message", extra={"message_id": message_id})
            raise ValueError(f"Unknown message_id: {message_id}")

        # Capture the retry_count we want to *log* BEFORE mutating
        # the dict. For requeue=True we log the new (incremented)
        # count; for requeue=False (DLQ) we log the count this message
        # ended at, not 0 from the just-cleared slot — otherwise the
        # final-DLQ line misleadingly reports retry_count: 0 right
        # after the app-level "Max retries (N) exceeded" line.
        prior_count = self.retry_counts.get(message_id, 0)
        if requeue:
            self.retry_counts[message_id] = prior_count + 1
            logged_count = prior_count + 1
        else:
            self.retry_counts.pop(message_id, None)
            logged_count = prior_count

        logger.info(
            "Rejecting message",
            extra={
                "message_id": message_id,
                "requeue": requeue,
                "retry_count": logged_count,
            },
        )

        try:
            if requeue:
                # Requeue (retry)
                await message.nack(requeue=True)
            else:
                # Send to DLQ (via x-dead-letter-exchange)
                await message.reject(requeue=False)

        except Exception as e:
            logger.error(
                "Failed to NACK message",
                exc_info=True,
                extra={"message_id": message_id, "error": str(e)},
            )
            raise

    def get_retry_count(self, message_id: str) -> int:
        """How many times this message has been NACK-requeued.

        Returns 0 for never-retried or unknown ids. app.py consults
        this before deciding to requeue vs DLQ a Retryable failure,
        enforcing KBM_LEDSAS_MAX_RETRIES.
        """
        return self.retry_counts.get(message_id, 0)

    @property
    def pending_count(self) -> int:
        """Number of messages awaiting acknowledgment."""
        return len(self.pending_messages)
