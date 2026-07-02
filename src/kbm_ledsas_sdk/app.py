"""
ServiceApp - Main application class for LEDSAS SDK services.

The ServiceApp is the main entry point for building LEDSAS services.
It provides:
- Handler registration via @app.handler() decorator
- Transport lifecycle management
- Main execution loop
- Graceful shutdown
- Error handling and logging

Example usage:
    from kbm_ledsas_sdk import ServiceApp

    app = ServiceApp("my_service")

    @app.handler("ProcessDataset", "1.0")
    async def process_dataset(ctx, payload):
        # Download input
        data = await ctx.blob.download_bytes(...)

        # Process data
        result = process(data)

        # Upload result
        result_ref = await ctx.blob.upload_json("results", result)

        return {"result_uri": result_ref.uri}

    if __name__ == "__main__":
        app.run()
"""

import asyncio
import logging
import os
import random
import signal
import sys

from pydantic import ValidationError

from .health.checks import CheckResult, HealthCheckRegistry
from .health.server import HealthServer
from .models.messages import Command
from .runtime.config import SDKConfig
from .runtime.context import ExecutionContext
from .runtime.handler import HandlerFunc, HandlerRegistry
from .runtime.security import _is_truthy
from .transport.base import Transport
from .transport.factory import create_transport
from .utils.logging import setup_logging

logger = logging.getLogger(__name__)


class _SuppressExpectedAMQPErrors(logging.Filter):
    """
    Drop ``aio_pika`` / ``aiormq`` / ``pamqp`` log records whose attached
    exception is one the SDK already translates into a clean ERROR + NACK
    or one clean operator-facing message:

    - ``ChannelNotFoundEntity`` — caller-supplied ``reply_to`` exchange
      does not exist on the broker. The SDK logs a clean ERROR, bumps
      ``reply_publish_failures``, and dead-letters the command.
    - ``ProbableAuthenticationError`` — broker rejected the credentials
      in ``KBM_LEDSAS_RABBITMQ_URL``. ``DirectTransport.start`` catches
      this and logs one operator-facing ERROR; ``ServiceApp.run()``
      exits non-zero without re-dumping the upstream-library traceback.
    - ``pamqp`` ``ValueError("Max length exceeded ...")`` — exchange or
      queue name exceeds AMQP's 127-byte protocol cap. The publisher
      and topology layers both classify this as expected and log one
      operator-facing line pointing at the actual cause.

    Anything else (unexpected channel errors, connection issues, etc.)
    is left untouched so real problems still surface.
    """

    _SUPPRESSED_EXC_NAMES = frozenset(
        {
            "ChannelNotFoundEntity",
            "ProbableAuthenticationError",
        }
    )

    def filter(self, record: logging.LogRecord) -> bool:
        exc_info = record.exc_info
        if not exc_info:
            return True
        exc = exc_info[1]
        if exc is None:
            return True
        # Match by class name to avoid importing aiormq at SDK init time.
        if type(exc).__name__ in self._SUPPRESSED_EXC_NAMES:
            return False
        # pamqp raises stdlib ValueError with a "Max length exceeded"
        # message when an exchange/queue name overflows the AMQP 127-byte
        # protocol cap. Match on the message because the class is plain
        # ValueError shared with everything else in the language.
        if isinstance(exc, ValueError) and "Max length exceeded" in str(exc):
            return False
        return True


def _quiet_noisy_libraries() -> None:
    """
    Dial down third-party loggers that dump full tracebacks on expected
    error cases (notably ``aio_pika`` / ``aiormq`` channel-error logging,
    which prints a ~30-line stack when a publish hits ``NOT_FOUND`` on a
    caller-owned reply_to exchange — even though the SDK then translates
    the failure into a clean ERROR log and NACKs to DLQ) AND/OR leak
    credential-bearing data at DEBUG (the Azure SDK's HTTP-policy logger
    prints every request URL and auth header).

    Strategy: cap level at WARNING (drops INFO/DEBUG chatter) AND attach
    the SDK's filter that suppresses ERROR-level traceback dumps the
    SDK is already handling cleanly. Unknown errors still pass through.

    ``azure.*`` is included here as well. Customer code that does
    ``logging.basicConfig(level=DEBUG)`` (or sets
    ``KBM_LEDSAS_LOG_LEVEL=DEBUG`` with a default root config that
    propagates to azure) would otherwise route Azure SDK HTTP traces —
    including SAS tokens in URLs and account keys in request signing
    paths — to the customer log aggregator. The example services have
    been advising ``logging.getLogger('azure').setLevel(WARNING)``
    defensively; doing it here covers customers who forget.

    ``pamqp`` is included for the Max-length-exceeded ValueError case;
    capping at WARNING drops its INFO-level frame chatter and the
    filter drops the ERROR-with-traceback record the SDK classifies
    itself.
    """
    suppressor = _SuppressExpectedAMQPErrors()
    for name in ("aio_pika", "aiormq", "pamqp", "azure"):
        lib_logger = logging.getLogger(name)
        lib_logger.setLevel(logging.WARNING)
        # Avoid attaching the same filter twice if run() is called more than once.
        if not any(isinstance(f, _SuppressExpectedAMQPErrors) for f in lib_logger.filters):
            lib_logger.addFilter(suppressor)


class ServiceApp:
    """
    Main application class for LEDSAS SDK services.

    ServiceApp manages:
    - Handler registration via decorators
    - Transport initialization and lifecycle
    - Main execution loop (consuming commands, executing handlers)
    - Graceful shutdown on SIGTERM/SIGINT
    - Error handling and logging

    Example:
        app = ServiceApp("my_service")

        @app.handler("ProcessDataset", "1.0")
        async def process_dataset(ctx, payload):
            return {"result": "success"}

        app.run()
    """

    def __init__(self, service_name: str):
        """
        Initialize ServiceApp.

        Args:
            service_name: Service name (used in config and logging)

        Example:
            app = ServiceApp("image_processor")
        """
        self.service_name = service_name
        self.registry = HandlerRegistry()
        self.config: SDKConfig | None = None
        self.transport: Transport | None = None
        self.liveness_checks = HealthCheckRegistry()
        self.readiness_checks = HealthCheckRegistry()
        self.health_server: HealthServer | None = None
        self._shutdown_event = asyncio.Event()
        self._running = False

    def handler(self, command_name: str, version: str = "1.0"):
        """
        Decorator to register a command handler.

        Args:
            command_name: Command name (e.g., "ProcessDataset")
            version: Message version (default: "1.0")

        Returns:
            Decorator function

        Example:
            @app.handler("ProcessDataset", "1.0")
            async def process_dataset(ctx: ExecutionContext, payload: dict) -> dict:
                # Handler logic
                return {"result": "success"}
        """

        def decorator(func: HandlerFunc):
            self.registry.register(command_name, version, func)
            return func

        return decorator

    def liveness_check(self, name: str):
        """
        Register a liveness check. Decorate a sync or async callable that
        returns truthy when the process is alive and able to make progress.

        The decorated function should be cheap — liveness probes fire often
        and any raised exception is reported as "unhealthy".

        Example:
            @app.liveness_check("event_loop_responsive")
            def _():
                return not main_loop.is_blocked()
        """

        def decorator(func):
            self.liveness_checks.register(name, func)
            return func

        return decorator

    def readiness_check(self, name: str):
        """
        Register a readiness check. Decorate a sync or async callable that
        returns truthy when the service is ready to accept new work.

        The SDK already ships a default readiness check (transport connected),
        so customer checks are layered on top — all must pass for /ready to
        return 200.

        Example:
            @app.readiness_check("db_connected")
            async def _():
                return await db.ping()
        """

        def decorator(func):
            self.readiness_checks.register(name, func)
            return func

        return decorator

    def run(self):
        """
        Start the service (blocks until shutdown).

        This is the main entry point - it:
        1. Loads configuration from environment
        2. Creates transport
        3. Starts transport
        4. Sets up signal handlers for graceful shutdown
        5. Runs main execution loop
        6. Shuts down gracefully on SIGTERM/SIGINT

        Example:
            if __name__ == "__main__":
                app = ServiceApp("my_service")

                @app.handler("ProcessDataset")
                async def process_dataset(ctx, payload):
                    return {"result": "ok"}

                app.run()
        """
        try:
            asyncio.run(self._run_async())
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down...")
        except (ValueError, ValidationError) as e:
            # Configuration-class error: the SDK already raised with a
            # clean actionable message (e.g. "Set KBM_LEDSAS_RABBITMQ_URL
            # ... see the README Configuration section"). Log just that
            # message and exit non-zero. Do NOT re-raise — propagating
            # would dump 30+ lines of asyncio/factory/pydantic internals
            # on top of the clean error and bury the actionable line.
            #
            # ValidationError wraps the field-validator's message in
            # pydantic boilerplate ("1 validation error for SDKConfig",
            # "[type=value_error, input_value=...]", a docs URL).
            # Unwrap it to the underlying human messages so the customer
            # sees the same one-liner the README troubleshooting
            # section advertises.
            #
            # Set KBM_LEDSAS_DEBUG=1 to keep the full traceback for
            # development.
            if isinstance(e, ValidationError):
                # Mirror consumer.py's unwrap — include the field
                # location ("{loc}: {msg}") so a customer with several
                # env vars set knows *which* one failed validation
                # (e.g. "health_port: Input should be less than or
                # equal to 65535" beats the bare "Input should be...").
                msgs = []
                for err in e.errors():
                    loc = ".".join(str(p) for p in err.get("loc", ()))
                    msg = str(err.get("msg", ""))
                    # pydantic prefixes field-validator messages with
                    # "Value error, " — strip it.
                    if msg.startswith("Value error, "):
                        msg = msg[len("Value error, ") :]
                    if msg:
                        msgs.append(f"{loc}: {msg}" if loc else msg)
                error_text = "; ".join(msgs) if msgs else str(e)
            else:
                error_text = str(e)
            # Use the unified _is_truthy parser so
            # KBM_LEDSAS_DEBUG accepts 1/true/yes/on consistently with
            # every other boolean knob in the SDK. Security-flavored
            # knobs still fail closed on unrecognized tokens.
            if _is_truthy(os.getenv("KBM_LEDSAS_DEBUG")):
                logger.exception(f"Configuration error: {error_text}")
            else:
                logger.error(f"Configuration error: {error_text}")
            sys.exit(1)
        except Exception as e:
            # Expected startup-error classes are logged ONCE by
            # the layer that detected them (DirectTransport for AMQP
            # auth, reply-publish path for ChannelNotFoundEntity, etc).
            # Re-raising from here lets Python dump its own uncaught
            # traceback on stderr — a third copy of the same info.
            # Recognize the known classes by qualified name (cheap, no
            # extra import of aiormq/aio_pika at app-layer) and exit
            # non-zero without re-logging. Anything genuinely unknown
            # still goes through the verbose path so it's diagnosable.
            exc_qualname = f"{type(e).__module__}.{type(e).__qualname__}"
            _expected_startup_errors = frozenset(
                {
                    "aiormq.exceptions.ProbableAuthenticationError",
                    "aio_pika.exceptions.ProbableAuthenticationError",
                    "aio_pika.exceptions.ChannelNotFoundEntity",
                    "aiormq.exceptions.ChannelNotFoundEntity",
                    # aio_pika re-exports the aiormq class object, so the
                    # computed qualname is always the aiormq spelling — both
                    # are listed, matching the dual-spelling pattern above.
                    "aio_pika.exceptions.AMQPConnectionError",
                    "aiormq.exceptions.AMQPConnectionError",
                }
            )
            if exc_qualname in _expected_startup_errors:
                logger.error(
                    "ServiceApp exiting (startup failure already "
                    "reported above). Reason class: %s",
                    exc_qualname,
                )
                sys.exit(1)
            logger.exception(f"Fatal error in ServiceApp: {e}")
            raise

    async def _run_async(self):
        """
        Async entry point for the service.

        Manages:
        - Config loading
        - Transport initialization
        - Signal handlers
        - Main execution loop
        - Graceful shutdown
        """
        # Load configuration
        self.config = SDKConfig.from_env(service_name=self.service_name)

        # Configure SDK logging from env (KBM_LEDSAS_LOG_LEVEL /
        # KBM_LEDSAS_LOG_FORMAT). Previously these were parsed into
        # config but never applied — customers setting LOG_FORMAT=json
        # got whatever the customer's basicConfig produced.
        setup_logging(
            log_level=self.config.log_level,
            log_format=self.config.log_format,
            service_name=self.config.service_name,
        )
        _quiet_noisy_libraries()

        logger.info(
            f"Starting {self.service_name} service",
            extra={
                "log_level": self.config.log_level,
                "log_format": self.config.log_format,
                "concurrency": self.config.concurrency,
                "prefetch": self.config.prefetch,
            },
        )

        # Update registry with config (handlers already registered via decorators)
        self.registry._generic_errors = self.config.generic_errors

        # Create and start transport
        self.transport = create_transport(self.config)
        await self.transport.start()
        logger.info("Transport started successfully")

        # Start health server (after transport so default readiness reflects real state)
        await self._start_health_server()

        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()

        # Mark as running
        self._running = True

        try:
            # Run main execution loop
            await self._execution_loop()
        finally:
            # Shutdown
            await self._shutdown()

    def _setup_signal_handlers(self):
        """
        Set up signal handlers for graceful shutdown.

        Handles SIGTERM and SIGINT by setting shutdown event.

        ``loop.add_signal_handler`` is not implemented on Windows
        (raises ``NotImplementedError``). Skip on Windows and rely on
        the default KeyboardInterrupt path for development; production
        deployments target Linux/macOS where the loop hook works.
        """
        if sys.platform == "win32":
            logger.info(
                "Skipping loop signal handlers on Windows; Ctrl+C still "
                "triggers KeyboardInterrupt shutdown."
            )
            return

        loop = asyncio.get_event_loop()

        def signal_handler(sig):
            logger.info(f"Received signal {sig}, initiating graceful shutdown...")
            self._shutdown_event.set()

        # Register signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    async def _execution_loop(self):
        """
        Main execution loop.

        Consumes commands from transport and executes handlers with:
        - Concurrency limiting (via semaphore)
        - Error handling
        - Response/status publishing
        - ACK/NACK based on error type
        - Cooperative shutdown: races every subscribe-iteration await
          against ``_shutdown_event`` so SIGTERM/SIGINT interrupt the
          idle ``await consumer.consume()`` instead of being queued
          until the next message arrives.
        """
        logger.info("Starting execution loop")

        # Create semaphore for concurrency limiting
        concurrency = self.config.concurrency if self.config else 10
        semaphore = asyncio.Semaphore(concurrency)
        logger.info(f"Concurrency limit: {concurrency}")

        active_tasks: set[asyncio.Task] = set()

        # Pre-create the shutdown waiter so we can race it against each
        # subscribe-iteration await. Without this, when the consumer is
        # idle the signal handler that sets _shutdown_event has no way
        # to interrupt the parked `await consumer.consume()` call —
        # which means SIGTERM/SIGINT are effectively swallowed until the
        # next message arrives, and container shutdowns hard-kill on
        # every terminationGracePeriodSeconds boundary.
        shutdown_wait = asyncio.create_task(self._shutdown_event.wait(), name="sdk-shutdown-wait")
        sub_iter = self.transport.subscribe().__aiter__()

        try:
            while True:
                next_cmd = asyncio.create_task(sub_iter.__anext__(), name="sdk-next-command")
                done, _pending = await asyncio.wait(
                    {next_cmd, shutdown_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if shutdown_wait in done:
                    logger.info("Shutdown requested, stopping command consumption")
                    if not next_cmd.done():
                        next_cmd.cancel()
                        # Drain the cancellation so it doesn't surface
                        # as an unawaited-task warning.
                        try:
                            await next_cmd
                        except (asyncio.CancelledError, StopAsyncIteration):
                            pass
                        except Exception:
                            logger.debug(
                                "Suppressed exception from cancelled " "subscribe iterator",
                                exc_info=True,
                            )
                    break

                try:
                    command = next_cmd.result()
                except StopAsyncIteration:
                    # Transport's subscribe iterator finished naturally
                    break

                task = asyncio.create_task(self._handle_command(command, semaphore))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

        except Exception as e:
            logger.exception(f"Error in execution loop: {e}")
            raise
        finally:
            if not shutdown_wait.done():
                shutdown_wait.cancel()
                try:
                    await shutdown_wait
                except asyncio.CancelledError:
                    pass

            # Wait for active tasks to complete
            if active_tasks:
                logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
                await asyncio.gather(*active_tasks, return_exceptions=True)
                logger.info("All active tasks completed")

    async def _handle_command(self, command: Command, semaphore: asyncio.Semaphore):
        """
        Handle a single command.

        This method:
        1. Acquires semaphore (concurrency limiting)
        2. Creates ExecutionContext
        3. Executes handler via registry (with timeout)
        4. Sends response
        5. ACKs or NACKs based on error type
        6. Applies retry logic with exponential backoff

        Args:
            command: Command to handle
            semaphore: Concurrency limiter
        """
        async with semaphore:
            envelope = command.envelope
            correlation_id = envelope.correlation_id

            try:
                logger.info(
                    f"Handling command {envelope.name}",
                    extra={"correlation_id": correlation_id},
                )

                # Create execution context
                ctx = ExecutionContext(
                    transport=self.transport,
                    envelope=envelope,
                    payload=command.payload,
                )

                # Get timeout from config
                timeout_seconds = self.config.handler_timeout if self.config else 0

                # Execute handler (returns Response with success or error)
                response = await self.registry.execute(
                    ctx, command, timeout_seconds=timeout_seconds
                )

                # Send response (only if reply_to is set)
                reply_publish_ok = True
                if envelope.reply_to:
                    reply_publish_ok = await self.transport.send_response(response)
                    if reply_publish_ok:
                        logger.info(
                            f"Sent response for {envelope.name}",
                            extra={"correlation_id": correlation_id},
                        )
                    else:
                        # Reply publish failed (e.g. orchestrator's reply
                        # exchange missing). The transport has already
                        # logged the failure and bumped its counter.
                        # Treat this command as Permanent-failed: retrying
                        # would just keep failing the publish and the
                        # handler would re-execute uselessly. NACK to DLQ.
                        logger.error(
                            f"Reply publish failed for {envelope.name}; "
                            "command will be dead-lettered",
                            extra={"correlation_id": correlation_id},
                        )
                        await self.transport.nack(envelope.message_id, requeue=False)
                        return
                else:
                    logger.info(
                        f"Skipping response for {envelope.name} (no reply_to)",
                        extra={"correlation_id": correlation_id},
                    )

                # ACK if success, NACK if error with retry logic
                if "error" in response.payload:
                    error = response.payload["error"]
                    retryable = error.get("retryable", True)

                    if retryable:
                        # Get retry count from transport (if available)
                        retry_count = 0
                        if hasattr(self.transport, "get_retry_count"):
                            retry_count = self.transport.get_retry_count(envelope.message_id)

                        max_retries = self.config.max_retries if self.config else 3

                        if retry_count >= max_retries:
                            # Max retries exceeded - send to DLQ
                            logger.error(
                                f"Max retries ({max_retries}) exceeded, sending to DLQ",
                                extra={
                                    "correlation_id": correlation_id,
                                    "retry_count": retry_count,
                                },
                            )
                            await self.transport.nack(envelope.message_id, requeue=False)
                        else:
                            # Calculate backoff and wait before requeueing
                            delay = self._calculate_backoff(retry_count)
                            logger.warning(
                                f"Retry {retry_count + 1}/{max_retries}, "
                                f"waiting {delay:.2f}s before requeue",
                                extra={"correlation_id": correlation_id},
                            )
                            await asyncio.sleep(delay)
                            await self.transport.nack(envelope.message_id, requeue=True)
                    else:
                        # Non-retryable error - send to DLQ immediately
                        logger.error(
                            "Command failed with permanent error, sending to DLQ",
                            extra={"correlation_id": correlation_id},
                        )
                        await self.transport.nack(envelope.message_id, requeue=False)
                else:
                    # Success
                    await self.transport.ack(envelope.message_id)
                    logger.info(
                        f"ACKed command {envelope.name}",
                        extra={"correlation_id": correlation_id},
                    )

            except Exception:
                # Uncaught exception in _handle_command itself, NOT in
                # the registered handler (registry.execute already
                # catches handler exceptions and translates them).
                # By definition we don't know what to do with this, and
                # a retry would just hit the same bug. Dead-letter
                # immediately so operators see the failure instead of
                # the message bouncing forever.
                logger.exception(
                    "Uncaught exception in _handle_command; dead-lettering",
                    extra={"correlation_id": correlation_id},
                )
                try:
                    await self.transport.nack(envelope.message_id, requeue=False)
                except Exception as nack_error:
                    logger.exception(f"Failed to NACK message: {nack_error}")

    async def _shutdown(self):
        """
        Graceful shutdown.

        Stops transport and cleans up resources.
        """
        if not self._running:
            return

        logger.info("Shutting down ServiceApp...")
        self._running = False

        # Stop health server first so probes see "not ready" during the rest of shutdown
        if self.health_server:
            try:
                await self.health_server.stop()
            except Exception:
                logger.exception("Error stopping health server")
            finally:
                self.health_server = None

        # Stop transport
        if self.transport:
            try:
                await self.transport.stop()
                logger.info("Transport stopped successfully")
            except Exception as e:
                logger.exception(f"Error stopping transport: {e}")

        logger.info("ServiceApp shutdown complete")

    async def _start_health_server(self):
        """Boot the SDK health server, unless disabled by config."""
        assert self.config is not None  # set in _run_async before this call
        if self.config.health_port == 0:
            logger.info("Health server disabled by KBM_LEDSAS_HEALTH_PORT=0")
            return

        # Symmetric to the cleartext-AMQP non-loopback refusal — emit a
        # single WARNING when the health server binds to a non-loopback
        # interface. Combined with HEALTH_VERBOSE=true the rich body
        # fingerprints the service on every reachable interface; the
        # warning gives a heads-up so an operator who set
        # HEALTH_HOST=0.0.0.0 for orchestrator probing knows verbose
        # mode is potentially network-visible.
        loopback_hosts = {"127.0.0.1", "::1", "localhost"}
        if self.config.health_host not in loopback_hosts:
            logger.warning(
                "Health server bound to %s:%d — accessible from the "
                "network. Set KBM_LEDSAS_HEALTH_HOST=127.0.0.1 for "
                "loopback-only. KBM_LEDSAS_HEALTH_VERBOSE=%s.",
                self.config.health_host,
                self.config.health_port,
                str(self.config.health_verbose).lower(),
            )

        async def default_readiness() -> CheckResult:
            ready = bool(self.transport and self.transport.is_ready())
            return CheckResult(
                name="transport",
                healthy=ready,
                detail="" if ready else "transport not connected",
            )

        self.health_server = HealthServer(
            service_name=self.service_name,
            host=self.config.health_host,
            port=self.config.health_port,
            liveness_registry=self.liveness_checks,
            readiness_registry=self.readiness_checks,
            default_readiness=default_readiness,
            verbose=self.config.health_verbose,
        )
        await self.health_server.start()

    @property
    def health_server_running(self) -> bool:
        """Whether the health server is actually listening.

        Returns False both when the health server is disabled
        (``KBM_LEDSAS_HEALTH_PORT=0``) and when ``HealthServer.start``
        failed to bind (port in use, permission denied — logged as a
        WARNING but does not crash the service). Useful for integration
        tests that need to assert "the health endpoint is up before I
        probe it."
        """
        return self.health_server is not None and self.health_server.bound_port is not None

    def _calculate_backoff(self, retry_count: int) -> float:
        """
        Calculate exponential backoff with jitter.

        Formula: min(base_delay * 2^retry_count + jitter, max_delay)

        Args:
            retry_count: Current retry attempt (0-indexed)

        Returns:
            Delay in seconds before next retry
        """
        base_delay = 1.0
        max_delay = 60.0
        jitter = random.uniform(0, 1)
        delay = min(base_delay * (2**retry_count) + jitter, max_delay)
        return delay

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"ServiceApp(service_name={self.service_name!r}, "
            f"handlers={len(self.registry.list_handlers())}, "
            f"running={self._running})"
        )
