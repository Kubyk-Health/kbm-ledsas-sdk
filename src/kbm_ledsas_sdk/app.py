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
import contextlib
import inspect
import logging
import math
import random
import re
import signal
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any, TypeVar, cast

from pydantic import ValidationError

from .health.checks import CheckResult, HealthCheckRegistry, _check_registration
from .health.server import HealthServer, HealthServerStartFailure, _deployment_id, _host_port
from .models.messages import Command
from .runtime.config import SDKConfig, _shown_setting, describe_config_errors, service_name_problem
from .runtime.context import ExecutionContext

# HandlerFunc stays importable from this module (it has always been).
from .runtime.handler import HandlerFunc, HandlerRegistry, _check_command_name_and_version
from .runtime.security import _is_truthy, _read_env
from .transport.base import ERROR_REPLY_SENT, REPLY_UNCONFIRMED, DeliveryLost, Transport
from .transport.factory import create_transport
from .utils.logging import (
    SecretMaskFilter,
    _command_fields,
    install_root_handler,
    masked_logger,
    setup_logging,
)

logger = masked_logger(__name__)


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
            # The broker closed the connection (restart, shutdown, forced
            # close). aiormq logs the close reason once without a traceback
            # ("Unexpected connection close from remote ...") and the robust
            # connection logs each reconnect attempt; this record only repeats
            # the close as a stack trace.
            "ConnectionClosed",
        }
    )

    # user:password@ in a broker URL quoted by a library log line, with or
    # without a scheme ("amqp://u:p@host", "u:p@host").
    _URL_USERINFO = re.compile(r"(\w+://)[^\s/@]+@")
    _BARE_USERINFO = re.compile(r"(?<![\w/:.%*-])[^\s/@:'\"<>*]+:[^\s/@'\"<>]+@(?=[\w\[])")

    @classmethod
    def _scrub_credentials(cls, record: logging.LogRecord) -> None:
        """Remove broker credentials from a library record's message. The
        client library masks the password of a URL it could parse, but
        not of one it could not (it logs such a URL verbatim)."""
        try:
            text = record.getMessage()
        except Exception:
            return
        scrubbed = cls._URL_USERINFO.sub(r"\1***:***@", text)
        scrubbed = cls._BARE_USERINFO.sub("***:***@", scrubbed)
        if scrubbed != text:
            record.msg = scrubbed
            record.args = None

    # The SDK's secret masking (URL user info, a broker URL's query values,
    # storage keys, SAS signatures), on the message, the text fields and the
    # traceback of a library record that is kept; it fails closed.
    _MASK = SecretMaskFilter()

    def filter(self, record: logging.LogRecord) -> bool:
        # Decide on the record as the library logged it, then remove
        # credentials from the text of a record that is kept.
        keep = self._keep(record)
        if keep:
            self._scrub_credentials(record)
            self._MASK.filter(record)
        return keep

    def _keep(self, record: logging.LogRecord) -> bool:
        # A routine broker restart or shutdown: aiormq logs the broker's
        # close reason ("Unexpected connection close from remote ...,
        # CONNECTION_FORCED ...") as an ERROR, next to the SDK's own WARNING
        # (connection lost) and INFO (reconnected). The connection is
        # recovered automatically, so the record is kept as a WARNING: an
        # alert on ERROR records then does not fire for every restart.
        if (
            record.name.startswith("aiormq")
            and record.levelno > logging.WARNING
            and self._message_starts_with(record, "Unexpected connection close from remote")
        ):
            record.levelno = logging.WARNING
            record.levelname = logging.getLevelName(logging.WARNING)
        # When the broker closes a channel (at its consumer_timeout, for
        # example) while it is still sending deliveries on it, aiormq logs
        # each frame that arrives for the closed channel as an ERROR ("Got
        # frame for closed channel N: <Basic.Deliver ...>"). The broker
        # takes such deliveries back with the channel and delivers them
        # again, and the SDK logs the close itself, so the record is kept
        # as a WARNING.
        if (
            record.name.startswith("aiormq")
            and record.levelno > logging.WARNING
            and self._message_starts_with(record, "Got frame for closed channel")
        ):
            record.levelno = logging.WARNING
            record.levelname = logging.getLevelName(logging.WARNING)
        # aiormq logs every failed connection attempt as
        # "error when creating transport: <error>" (no exc_info). At startup
        # the transport reports the same failure as one clean line, and
        # while reconnecting aio_pika logs its own warning per attempt, so
        # this record only duplicates them.
        if (
            record.name.startswith("aiormq")
            and isinstance(record.msg, str)
            and record.msg.startswith("error when creating transport")
        ):
            return False
        # While the broker connection is down, aio_pika's channel close
        # callback fails with "... closed" on every reconnect cycle and
        # aio_pika logs it ("Callback <... _on_close ...> error"). The SDK
        # logs the outage and the recovery itself (one WARNING, one INFO),
        # so this record is noise.
        if self._is_closed_channel_callback_error(record):
            return False
        exc_info = record.exc_info
        if not exc_info:
            return True
        exc = exc_info[1]
        if exc is None:
            # aiormq 6.x logs the broker's close reason ("Unexpected
            # connection close from remote ...") with log.exception()
            # outside an except block, so the record carries an empty
            # exception and every formatter prints a bare "NoneType: None"
            # line after it, which reads like a traceback. Keep the record,
            # drop the empty exception.
            record.exc_info = None
            record.exc_text = None
            return True
        # Match by class name to avoid importing aiormq at SDK init time.
        if type(exc).__name__ in self._SUPPRESSED_EXC_NAMES:
            return False
        if self._is_reader_connection_loss(record, exc):
            # aiormq 7.x logs "Cancelling cause reader exited abnormally"
            # as a WARNING with the reader's exception attached. When that
            # exception is the connection dropping (the broker killed or
            # crashed, a TCP connection cut), the traceback only repeats
            # what the SDK's own WARNING ("Lost the connection to the AMQP
            # broker (...); reconnecting automatically") already says, so
            # the record keeps one line naming the cause. Any other reader
            # error (a malformed frame) keeps its traceback.
            record.msg = f"{_message_or_template(record)} ({type(exc).__name__}: {exc})"
            record.args = None
            record.exc_info = None
            record.exc_text = None
            return True
        # pamqp raises stdlib ValueError with a "Max length exceeded"
        # message when an exchange/queue name overflows the AMQP 127-byte
        # protocol cap. Match on the message because the class is plain
        # ValueError shared with everything else in the language.
        if isinstance(exc, ValueError) and "Max length exceeded" in str(exc):
            return False
        return True

    @classmethod
    def _is_reader_connection_loss(cls, record: logging.LogRecord, exc: BaseException) -> bool:
        """aiormq's "reader exited abnormally" record whose exception is
        the connection dropping (``ConnectionError`` / ``OSError``, which
        include aiormq's ``AMQPConnectionError``, or an incomplete read)."""
        return (
            record.name.startswith("aiormq")
            and cls._message_starts_with(record, "Cancelling cause reader exited abnormally")
            and isinstance(exc, (ConnectionError, OSError, asyncio.IncompleteReadError, EOFError))
        )

    @staticmethod
    def _message_starts_with(record: logging.LogRecord, prefix: str) -> bool:
        try:
            return record.getMessage().startswith(prefix)
        except Exception:
            return False

    @staticmethod
    def _is_closed_channel_callback_error(record: logging.LogRecord) -> bool:
        """aio_pika's "Callback <channel _on_close> error" record whose
        error says the channel or its connection is gone (closed, or no
        active transport). aio-pika 9.x logs it with a traceback, 10.x as
        ``Callback %r error: %s: %s``."""
        if record.name != "aio_pika.tools" or not isinstance(record.msg, str):
            return False
        if not record.msg.startswith("Callback %r error"):
            return False
        args = record.args if isinstance(record.args, tuple) else ()
        if not args or "_on_close" not in repr(args[0]):
            return False
        error = record.exc_info[1] if record.exc_info else None
        if error is None:
            error = next((a for a in args if isinstance(a, BaseException)), None)
        if error is None:
            return False
        text = str(error).lower()
        return (
            type(error).__name__ == "ChannelInvalidStateError"
            or "closed" in text
            or "no active transport" in text
        )


def _message_or_template(record: logging.LogRecord) -> str:
    try:
        return record.getMessage()
    except Exception:
        return str(record.msg)


_FILTERED_LIBRARY_LOGGERS = (
    "aio_pika",
    "aio_pika.channel",
    "aio_pika.connection",
    "aio_pika.exchange",
    "aio_pika.queue",
    "aio_pika.robust_channel",
    "aio_pika.robust_connection",
    "aio_pika.tools",
    "aiormq",
    "aiormq.channel",
    "aiormq.connection",
    "pamqp",
    "azure",
)

# The client library's other module loggers: no record of theirs is dropped,
# but the secrets they may quote (a connection's URL) are masked.
_MASKED_LIBRARY_LOGGERS = (
    "aio_pika.message",
    "aio_pika.pool",
    "aio_pika.robust_exchange",
    "aio_pika.robust_queue",
)


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
    # A logger's filters only see records created at that exact logger,
    # not records propagated from its children, and these libraries log
    # through per-module loggers (aiormq.connection, aio_pika.channel, ...).
    # Attach the filter to the package loggers AND to the module loggers
    # that emit the records it drops.
    for name in _FILTERED_LIBRARY_LOGGERS:
        lib_logger = logging.getLogger(name)
        # Avoid attaching the same filter twice if run() is called more than once.
        if not any(isinstance(f, _SuppressExpectedAMQPErrors) for f in lib_logger.filters):
            lib_logger.addFilter(suppressor)
    for name in _MASKED_LIBRARY_LOGGERS:
        masked_logger(name)


# The function a decorator returns unchanged (a handler: an async function;
# a health check: a function taking no arguments).
# A handler: called with (ctx, payload), returns a dict or None (any other
# value, a read-only Mapping included, is refused at run time). The payload
# is Any so a handler may annotate it as a dict, a TypedDict or a model it
# builds itself; dict[Any, Any] so the documented non-str keys type-check.
H = TypeVar("H", bound=HandlerFunc)
C = TypeVar("C", bound=Callable[[], object])


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

    The public API is the constructor, the ``handler`` /
    ``liveness_check`` / ``readiness_check`` decorators, ``run()``, the
    ``health_server_running`` property and the read-only ``service_name``
    attribute. The other attributes (``registry``, ``config``,
    ``transport``, ``health_server``, ``liveness_checks``,
    ``readiness_checks``, ...) are internal and may change.
    """

    def __init__(self, service_name: str):
        """
        Initialize ServiceApp.

        Args:
            service_name: Service name (used in config and logging): 1 to
                64 characters, letters, digits, ``.``, ``_`` and ``-``,
                starting with a letter or digit.

        Raises:
            TypeError: ``service_name`` is not a string.
            ValueError: ``service_name`` does not follow that rule. Both
                are raised here, as ``@app.handler`` refuses an invalid
                command name when it decorates, not when ``app.run()``
                starts. (``KBM_LEDSAS_SERVICE_NAME``, which overrides this
                name, is checked when ``app.run()`` reads the settings.)

        Example:
            app = ServiceApp("image_processor")
        """
        problem = service_name_problem(service_name)
        if problem is not None:
            error_class = TypeError if not isinstance(service_name, str) else ValueError
            raise error_class(f"ServiceApp(service_name={_shown_setting(service_name)}): {problem}")
        self._service_name: str = service_name
        self.registry: HandlerRegistry = HandlerRegistry()
        self.config: SDKConfig | None = None
        self.transport: Transport | None = None
        self.liveness_checks: HealthCheckRegistry = HealthCheckRegistry()
        self.readiness_checks: HealthCheckRegistry = HealthCheckRegistry()
        self.health_server: HealthServer | None = None
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._running: bool = False
        # Set when the execution loop stops taking commands (shutdown, or the
        # transport connection is gone): a command still waiting for a
        # worker is then given back at once instead of being started
        # (see _stop_starting_commands and _worker_slot).
        self._stop_starting_event = asyncio.Event()
        self._transport_lost = False
        # "<name> <message_id>" of the commands given back unstarted; logged
        # once (INFO) when the drain ends.
        self._released_unstarted: list[str] = []
        # While the execution loop runs, an unstarted command is given back
        # only after the transport has stopped receiving new commands (set
        # by _stop_receiving_commands), so the broker cannot hand it
        # straight back to this stopping instance.
        self._in_execution_loop = False
        self._give_back_ready = asyncio.Event()
        # The command tasks the execution loop is running (handlers, and
        # commands waiting for a worker), and the stop signals received: a
        # second SIGTERM / SIGINT cancels the running handlers.
        self._active_tasks: set[asyncio.Task[Any]] = set()
        self._stop_signals = 0
        # The command tasks whose handler is running (inside
        # registry.execute). A second stop signal cancels only these: a task
        # whose handler has returned is sending its reply or settling its
        # delivery, and is left to finish (each broker round trip is bounded
        # once shutdown has begun), so a command that was answered is never
        # given back and run again.
        self._tasks_in_handler: set[asyncio.Task[Any]] = set()
        # "<name> <message_id>" of each task in _tasks_in_handler, for the
        # record that names the handlers cancelled when the connection is
        # lost while stopping (_drain_active_tasks).
        self._handler_task_commands: dict[asyncio.Task[Any], str] = {}
        # The transport's start while it runs: a stop signal that arrives
        # then cancels it (see _start_transport).
        self._start_task: asyncio.Task[Any] | None = None
        # The event-loop watchdog (see _watch_event_loop) and the
        # "<name> <message_id>" of the handlers started since its last tick.
        self._loop_watchdog: asyncio.Task[Any] | None = None
        self._handlers_started_since_tick: set[str] = set()

    @property
    def service_name(self) -> str:
        """The service name given to the constructor (read-only:
        ``KBM_LEDSAS_SERVICE_NAME``, read when ``app.run()`` starts,
        overrides it for the queue and the logs)."""
        return self._service_name

    @service_name.setter
    def service_name(self, value: object) -> None:
        raise AttributeError(
            "ServiceApp.service_name is read-only: pass the name to "
            "ServiceApp(service_name=...), or set KBM_LEDSAS_SERVICE_NAME"
        )

    @property
    def _stop_starting_commands(self) -> bool:
        return self._stop_starting_event.is_set()

    @_stop_starting_commands.setter
    def _stop_starting_commands(self, value: bool) -> None:
        if value:
            self._stop_starting_event.set()
        else:
            self._stop_starting_event.clear()

    def handler(self, command_name: str, version: str = "1.0") -> Callable[[H], H]:
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
        # Checked now, not when decorating: ``@app.handler`` without
        # parentheses (or a name that is not a string) would otherwise
        # return the inner decorator and register nothing.
        _check_command_name_and_version(command_name, version)

        def decorator(func: H) -> H:
            self.registry.register(command_name, version, func)
            return func

        return decorator

    def liveness_check(self, name: str, timeout: float | None = None) -> Callable[[C], C]:
        """
        Register a liveness check. Decorate a sync or async callable that
        returns truthy when the process is alive and able to make progress.

        The decorated function should be cheap — liveness probes fire often
        and any raised exception is reported as "unhealthy".

        ``timeout``: seconds an async check may run before it is cancelled
        and reported as unhealthy (default 2; at most 60). The checks of
        one probe run concurrently. A sync check runs on the event loop and
        cannot be interrupted: keep it quick, and write a check that waits
        on I/O as ``async def``.

        Example:
            @app.liveness_check("event_loop_responsive")
            def event_loop_responsive():
                return True  # reaching here proves the event loop runs
        """
        _check_registration(name, timeout, "liveness_check")

        def decorator(func: C) -> C:
            self.liveness_checks.register(name, func, timeout=timeout)
            return func

        return decorator

    def readiness_check(self, name: str, timeout: float | None = None) -> Callable[[C], C]:
        """
        Register a readiness check. Decorate a sync or async callable that
        returns truthy when the service should be reported ready.

        ``timeout``: seconds an async check may run before it is cancelled
        and reported as unhealthy (default 2; at most 60), so a dependency
        that hangs makes ``/ready`` answer 503 instead of hanging the probe.

        Readiness only decides what ``/ready`` answers (for example whether
        an orchestrator marks the instance ready); it does not stop command
        delivery: commands are consumed while ``/ready`` answers 503 too.
        Do warm-up work (loading a model, ...) before ``app.run()``.

        The SDK already ships a default readiness check (transport connected),
        so customer checks are layered on top — all must pass for /ready to
        return 200.

        Example:
            @app.readiness_check("db_connected")
            async def _():
                return await db.ping()
        """
        _check_registration(name, timeout, "readiness_check")

        def decorator(func: C) -> C:
            self.readiness_checks.register(name, func, timeout=timeout)
            return func

        return decorator

    def run(self) -> None:
        """
        Start the service (blocks until shutdown).

        This is the main entry point - it:
        1. Loads configuration from environment
        2. Creates transport
        3. Sets up signal handlers for graceful shutdown (a SIGTERM /
           SIGINT during the next step stops the start, exit code 0)
        4. Starts transport
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
        # Set when the service exits 1 after a reported failure. The exit
        # happens after the except block, so the SystemExit does not carry
        # the error as its __context__ (a configuration error's settings
        # input, for example, stays out of an error reporter's chain).
        exit_code: int | None = None
        try:
            asyncio.run(self._run_async())
        except KeyboardInterrupt:
            logger.info("Received KeyboardInterrupt, shutting down...")
        except (ValueError, ValidationError) as e:
            # Configuration-class error: the SDK already raised with a
            # clean actionable message (e.g. "KBM_LEDSAS_RABBITMQ_URL is
            # not set. Set it to the broker URL ..."). Log just that
            # message and exit non-zero. Do NOT re-raise — propagating
            # would dump 30+ lines of asyncio/factory/pydantic internals
            # on top of the clean error and bury the actionable line.
            #
            # ValidationError wraps the field-validator's message in
            # pydantic boilerplate ("1 validation error for SDKConfig",
            # "[type=value_error, input_value=...]", a docs URL).
            # Unwrap it to the underlying human messages so the operator
            # sees one actionable line per problem.
            #
            # Set KBM_LEDSAS_DEBUG=1 to keep the full traceback for
            # development.
            if isinstance(e, ValidationError) and e.title == "SDKConfig":
                # Each problem names the setting to fix: the KBM_LEDSAS_*
                # variable and its value ("KBM_LEDSAS_PREFETCH=5000: Input
                # should be less than or equal to 1000"), or
                # ServiceApp(service_name=...) for a name from the code.
                error_text = describe_config_errors(e)
            elif isinstance(e, ValidationError):
                # Mirror consumer.py's unwrap — include the field
                # location ("{loc}: {msg}").
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
            # KBM_LEDSAS_DEBUG was already checked when the configuration
            # was read (parse_bool_env: a value that is not a boolean is
            # itself a configuration error). Here a true token adds the
            # traceback; anything else, including the invalid value being
            # reported, gives the one-line message.
            # A configuration error can be raised before the SDK installed
            # its log handler (while the environment is being parsed), and
            # would then go out in whatever format the root logger uses.
            # Install the SDK handler first so every configuration error
            # uses KBM_LEDSAS_LOG_FORMAT (JSON by default).
            self._ensure_sdk_logging()
            if _is_truthy(_read_env("KBM_LEDSAS_DEBUG")):
                logger.exception(f"Configuration error: {error_text}")
            else:
                logger.error(f"Configuration error: {error_text}")
            exit_code = 1
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
                    # Transport could not connect at startup: the
                    # transport logged the cause as one clean line.
                    "kbm_ledsas_sdk.transport.base.TransportUnavailable",
                    # The health server could not listen on
                    # KBM_LEDSAS_HEALTH_HOST / KBM_LEDSAS_HEALTH_PORT:
                    # it logged the cause and the setting to change.
                    "kbm_ledsas_sdk.health.server.HealthServerStartFailure",
                }
            )
            if exc_qualname in _expected_startup_errors:
                logger.error(
                    "ServiceApp exiting (startup failure already "
                    "reported above). Reason class: %s",
                    exc_qualname,
                )
                exit_code = 1
            else:
                logger.exception(f"Fatal error in ServiceApp: {e}")
                raise
        if exit_code is not None:
            sys.exit(exit_code)

    def _ensure_sdk_logging(self) -> None:
        """Install the SDK log handler if config loading failed before
        ``setup_logging`` ran. Uses KBM_LEDSAS_LOG_FORMAT when it holds a
        valid value (json/text), else JSON; the level is INFO."""
        if logging.getLogger("kbm_ledsas_sdk").handlers:
            return
        log_format = (_read_env("KBM_LEDSAS_LOG_FORMAT") or "json").lower()
        if log_format not in ("json", "text"):
            log_format = "json"
        service_name = (_read_env("KBM_LEDSAS_SERVICE_NAME") or "") or self.service_name
        try:
            setup_logging(log_level="INFO", log_format=log_format, service_name=service_name)
        except Exception:  # pragma: no cover - never block the error line
            pass

    async def _run_async(self) -> None:
        """
        Async entry point for the service.

        Manages:
        - Config loading
        - Transport initialization
        - Signal handlers
        - Main execution loop
        - Graceful shutdown
        """
        # Install the SDK log handler before reading the environment, so a
        # warning raised while it is read (the cleartext-AMQP acknowledgment) uses KBM_LEDSAS_LOG_FORMAT (JSON
        # by default) instead of Python's bare fallback text. It is
        # reconfigured from the loaded config right below.
        self._ensure_sdk_logging()

        # Stop signals are handled from the very start: a SIGTERM / SIGINT
        # that arrives before the transport starts (while the settings are
        # read or the transport is built) stops the service with a log line
        # and exit code 0, instead of the default action killing the
        # process silently (exit 143). While the transport is connecting, a
        # stop signal stops the start (a broker that does not answer would
        # otherwise hold the process until the connect timeout, and
        # Python's own Ctrl+C handling left the connection's reconnect
        # loop running).
        self._setup_signal_handlers()

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
        # Records of other code (the event loop's "Task exception was never
        # retrieved", the client libraries, Python warnings) in the same
        # format, when the application has not configured logging itself.
        install_root_handler(
            log_format=self.config.log_format,
            service_name=self.config.service_name,
        )
        _quiet_noisy_libraries()

        if self.config.service_name != self.service_name:
            # KBM_LEDSAS_SERVICE_NAME takes precedence over the constructor
            # argument. Say so: a sender still targeting the constructor
            # name reaches a queue nobody consumes.
            logger.warning(
                "KBM_LEDSAS_SERVICE_NAME=%r overrides "
                "ServiceApp(service_name=%r): this service consumes from the "
                "exchange and queue named after %r. Unset the variable (or "
                "make the two match) if senders target %r.",
                self.config.service_name,
                self.service_name,
                self.config.service_name,
                self.service_name,
            )

        from . import __version__

        start_fields: dict[str, object] = {
            "log_level": self.config.log_level,
            "log_format": self.config.log_format,
            "concurrency": self.config.concurrency,
            "sdk_version": __version__,
        }
        # The deployed build's id (as in the health responses), so a log
        # search can tell which build started.
        start_fields["prefetch"] = self.config.prefetch
        deployment_id = _deployment_id()
        if deployment_id:
            start_fields["deployment_id"] = deployment_id
        logger.info(
            f"Starting {self.config.service_name} service",
            extra=start_fields,
        )
        # Logged here, not when the handlers were registered (at import
        # time, before this method applied the log settings).
        self.registry.log_registered()
        allowed = getattr(self.config, "allowed_containers", None)
        if isinstance(allowed, (set, frozenset)) and allowed:
            logger.info(
                "Blob access limited to the containers in " "KBM_LEDSAS_ALLOWED_CONTAINERS: %s",
                ", ".join(sorted(allowed)),
                extra={"allowed_containers": sorted(allowed)},
            )

        # Update registry with config (handlers already registered via decorators)
        self.registry._generic_errors = self.config.generic_errors
        # A successful result whose reply would exceed the message-size
        # limit becomes a Permanent error reply instead of a reply the
        # broker refuses (0 disables the check).
        self.registry.max_reply_bytes = self.config.max_payload_bytes

        # Create and start transport
        self.transport = create_transport(self.config)
        # The command names this service handles, before the transport
        # starts (an optional hook; the default does nothing).
        if isinstance(self.transport, Transport):
            self.transport.set_handled_commands(
                sorted({name for name, _version in self.registry.list_handlers()})
            )
        # A stop signal handled while the transport was being built: stop
        # before connecting at all.
        await asyncio.sleep(0)
        if self._shutdown_event.is_set():
            self._log_stopped_before_start()
            return
        if not await self._start_transport():
            return
        logger.info("Transport started successfully")

        # Start health server (after transport so default readiness reflects
        # real state). A port it cannot listen on is a startup failure: the
        # transport is stopped first (commands it may already have received
        # go back to the broker unacknowledged; no handler has run).
        try:
            await self._start_health_server()
        except HealthServerStartFailure:
            await self._stop_transport_after_failed_start()
            raise

        # Report blocks of the event loop (blocking code in a handler).
        self._loop_watchdog = asyncio.create_task(
            self._watch_event_loop(), name="sdk-event-loop-watchdog"
        )

        # Mark as running
        self._running = True

        try:
            # Run main execution loop
            await self._execution_loop()
        finally:
            # Shutdown
            await self._shutdown()

    def _log_stopped_before_start(self) -> None:
        logger.info(
            "Stopped before the service started (a stop signal "
            "arrived while the transport was starting); exiting"
        )

    async def _start_transport(self) -> bool:
        """Start the transport. False when a stop signal stopped the start
        (the transport has closed what it opened; nothing else to stop)."""
        start = asyncio.ensure_future(cast(Transport, self.transport).start())
        self._start_task = start
        try:
            await start
        except asyncio.CancelledError:
            if start.cancelled() and self._stop_signals:
                self._log_stopped_before_start()
                return False
            raise
        finally:
            self._start_task = None
        return True

    async def _stop_transport_after_failed_start(self) -> None:
        """Stop a transport that started before a later startup step
        failed (the reason is logged by that step; this logs nothing of
        its own unless the stop itself fails)."""
        if self.transport is None:
            return
        try:
            await self.transport.stop()
        except Exception as e:  # pragma: no cover - never hide the reason
            logger.warning(
                "Could not stop the transport after the failed start: %s: %s",
                type(e).__name__,
                e,
            )

    def _setup_signal_handlers(self) -> None:
        """
        Set up signal handlers for graceful shutdown.

        The first SIGTERM or SIGINT starts the graceful shutdown (sets the
        shutdown event). A second one stops at once: the running handlers
        are cancelled and their commands given back without a reply (see
        ``_stop_now``).

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

        # Register signal handlers
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._on_stop_signal, sig)

    def _on_stop_signal(self, sig: int) -> None:
        """SIGTERM / SIGINT: the first starts the graceful shutdown, any
        later one stops at once (``_stop_now``). While the transport is
        still starting, a stop signal stops the start instead."""
        self._stop_signals += 1
        start = self._start_task
        if start is not None and not start.done():
            logger.info(
                "Received signal %s while starting: stopping the start",
                int(sig),
            )
            self._shutdown_event.set()
            start.cancel()
            return
        if self._stop_signals == 1:
            logger.info(
                "Received signal %s, initiating graceful shutdown (send "
                "it again to stop at once)...",
                int(sig),
            )
            self._shutdown_event.set()
            return
        self._stop_now(int(sig))

    def _stop_now(self, sig: int) -> None:
        """A second stop signal: cancel the running handlers.

        Only the tasks whose handler is still running are cancelled
        (``_tasks_in_handler``). Each cancelled command is given back
        without a reply and without counting a retry (``_handle_command``
        settles a cancelled delivery with ``_settle_after_escape``), so the
        broker delivers it again (to this service after a restart, or to
        another instance); a handler cut off this way may run again. A
        command whose handler has already returned is not cancelled: it
        finishes sending its one reply and is acknowledged or dead-lettered
        (each of those broker round trips is bounded during shutdown), so
        it is never answered and then run again. A command still waiting
        for a worker is given back by the shutdown itself. Work a handler
        started in a thread (``asyncio.to_thread``) cannot be cancelled;
        the process exits once it ends.
        """
        self._shutdown_event.set()
        running = [task for task in self._tasks_in_handler if not task.done()]
        logger.warning(
            "Received signal %s again: stopping now. %d running command(s) "
            "cancelled and given back without a reply; the broker delivers "
            "them again.",
            sig,
            len(running),
            extra={"cancelled_commands": len(running)},
        )
        for task in running:
            task.cancel()

    async def _execution_loop(self) -> None:
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

        active_tasks = self._active_tasks

        # Pre-create the shutdown waiter so we can race it against each
        # subscribe-iteration await. Without this, when the consumer is
        # idle the signal handler that sets _shutdown_event has no way
        # to interrupt the parked `await consumer.consume()` call —
        # which means SIGTERM/SIGINT are effectively swallowed until the
        # next message arrives, and container shutdowns hard-kill on
        # every terminationGracePeriodSeconds boundary.
        shutdown_wait = asyncio.create_task(self._shutdown_event.wait(), name="sdk-shutdown-wait")
        sub_iter = cast(
            AsyncIterator[Command], cast(Transport, self.transport).subscribe()
        ).__aiter__()
        self._in_execution_loop = True
        self._give_back_ready.clear()
        # A command the transport handed over at the moment shutdown was
        # requested: never started, so it is given back with the others.
        leftover: Command | None = None

        try:
            while True:
                next_cmd: asyncio.Task[Command] = asyncio.create_task(
                    cast("Coroutine[Any, Any, Command]", sub_iter.__anext__()),
                    name="sdk-next-command",
                )
                done, _pending = await asyncio.wait(
                    {next_cmd, shutdown_wait},
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if shutdown_wait in done:
                    logger.info("Shutdown requested, stopping command consumption")
                    if next_cmd.done():
                        if not next_cmd.cancelled() and next_cmd.exception() is None:
                            leftover = next_cmd.result()
                    else:
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

            # From here on no command that is still waiting for a worker is
            # started: it could neither reply nor be acknowledged reliably,
            # and it would hold the exit for up to
            # ceil(PREFETCH / CONCURRENCY) handler runs. It is given back
            # at once instead (see _worker_slot and _release_unstarted).
            self._stop_starting_commands = True

            # Shutdown begins here, before the drain: let the transport
            # bound the broker round trips that in-flight commands still
            # make (reply/status publishes), so an unreachable or stalled
            # broker cannot hold the drain until the platform kills us.
            await self._begin_transport_shutdown()

            # Stop receiving new commands before giving any back, so the
            # broker delivers them to another instance, not to this one;
            # this also opens the gate for the waiting commands.
            await self._stop_receiving_commands()
            if leftover is not None:
                await self._release_unstarted(leftover)

            # Wait for active tasks to complete
            if active_tasks:
                logger.info(f"Waiting for {len(active_tasks)} active tasks to complete...")
                await self._drain_active_tasks(set(active_tasks))
                logger.info("All active tasks completed")
            if self._released_unstarted:
                shown = self._released_unstarted[:20]
                more = len(self._released_unstarted) - len(shown)
                logger.info(
                    "Gave back %d command(s) without running them (%s); the "
                    "broker delivers them again: %s%s",
                    len(self._released_unstarted),
                    self._stop_reason(),
                    ", ".join(shown),
                    f" (and {more} more)" if more > 0 else "",
                    extra={"released_commands": len(self._released_unstarted)},
                )
                self._released_unstarted = []
            self._in_execution_loop = False

    # How often the drain checks whether the transport can still reach the
    # broker (see _drain_active_tasks).
    _DRAIN_CHECK_INTERVAL_S = 0.5

    async def _drain_active_tasks(self, tasks: set[asyncio.Task[Any]]) -> None:
        """Wait for the command tasks still running at shutdown to end.

        If, while they run, the transport reports that it can no longer
        reach the broker (``broker_unreachable_at_shutdown``: the
        connection was lost, or does not answer, during the shutdown), the
        handlers still running are cancelled once, as by a second stop
        signal, with one INFO record naming them: their reply and their
        acknowledgement could no longer be sent, and the broker delivers
        them again, so running them on would only repeat their work. A
        command whose handler has returned is not cancelled (it finishes
        its bounded reply and settlement). Every task's outcome is
        retrieved, as ``gather(return_exceptions=True)`` did.
        """
        pending = set(tasks)
        checked = False
        while pending:
            # Checked before each wait, so a broker already known to be
            # unreachable when the drain starts (the connection was lost
            # before the stop signal) cancels the handlers at once.
            if not checked and self._broker_unreachable_at_shutdown():
                checked = True
                self._cancel_running_handlers_at_shutdown()
            _done, pending = await asyncio.wait(pending, timeout=self._DRAIN_CHECK_INTERVAL_S)
        for task in tasks:
            if task.done() and not task.cancelled():
                task.exception()

    def _cancel_running_handlers_at_shutdown(self) -> None:
        """Cancel the handlers still running (see ``_drain_active_tasks``),
        with one INFO record naming them."""
        running = [task for task in self._tasks_in_handler if not task.done()]
        if running:
            names = [self._handler_task_commands.get(task, "?") for task in running]
            shown = names[:20]
            more = len(names) - len(shown)
            logger.info(
                "Replies and acknowledgements can no longer be sent while "
                "stopping (the connection was lost or does not answer): %d "
                "running command(s) cancelled; the broker delivers them "
                "again: %s%s",
                len(running),
                ", ".join(shown),
                f" (and {more} more)" if more > 0 else "",
                extra={"cancelled_commands": len(running)},
            )
            for task in running:
                task.cancel()

    def _stop_reason(self) -> str:
        if self._transport_lost:
            return "the transport connection was lost"
        if self._shutdown_event.is_set():
            return "shutdown requested"
        return "command consumption stopped"

    async def _release_unstarted(self, command: Command) -> None:
        """Give back a command the service took but did not start (it was
        waiting for a worker when the service stopped taking commands),
        without running its handler (no reply, no retry counted). On a lost
        transport connection nothing can be sent: the connection's
        deliveries are already back with the broker. While the execution
        loop runs, it waits until the transport has stopped receiving new
        commands (``_give_back_ready``)."""
        envelope = command.envelope
        self._released_unstarted.append(f"{envelope.name} {envelope.message_id}")
        if self._transport_lost:
            return
        if self._in_execution_loop:
            await self._give_back_ready.wait()
        if self._transport_lost:
            return
        try:
            release = getattr(self.transport, "release", None)
            if callable(release):
                await release(envelope.message_id)
            else:
                await cast(Transport, self.transport).nack(envelope.message_id, requeue=True)
        except Exception:
            logger.debug(
                "Giving back an unstarted command failed (the broker "
                "redelivers it once the connection closes)",
                exc_info=True,
                extra=_command_fields(envelope),
            )

    async def _stop_receiving_commands(self) -> None:
        """At shutdown, ask the transport to stop receiving new commands
        (optional hook ``stop_consuming()``, sync or async), then let the
        commands not started be given back (``_give_back_ready``).

        The hook keeps the commands already received settleable (the
        running ones still reply and acknowledge) and gives back the ones
        it received but had not handed over yet; it returns those, so the
        drain's INFO line counts them too. Transports without the hook are
        unaffected. Never raises."""
        try:
            stop = getattr(self.transport, "stop_consuming", None)
            if callable(stop) and not self._transport_lost:
                given_back = None
                try:
                    given_back = stop()
                    if inspect.isawaitable(given_back):
                        given_back = await given_back
                except Exception:
                    logger.debug(
                        "Stopping the delivery of new commands failed (the "
                        "transport stops it when it closes)",
                        exc_info=True,
                    )
                if isinstance(given_back, (list, tuple)):
                    for command in given_back:
                        envelope = getattr(command, "envelope", None)
                        if envelope is not None:
                            self._released_unstarted.append(
                                f"{envelope.name} {envelope.message_id}"
                            )
        finally:
            self._give_back_ready.set()

    def _stopped_taking_commands(self) -> bool:
        return self._stop_starting_commands or self._shutdown_event.is_set()

    @contextlib.asynccontextmanager
    async def _worker_slot(self, semaphore: asyncio.Semaphore) -> AsyncIterator[bool]:
        """Hold a worker slot (``semaphore``) for the body.

        Yields True with the slot held, or False without one when the
        service stopped taking commands first: a command still waiting for
        a worker is then given back at once, not when a running handler
        frees a worker (which can take up to KBM_LEDSAS_HANDLER_TIMEOUT).
        """
        acquired = await self._wait_for_worker(semaphore)
        try:
            yield acquired
        finally:
            if acquired:
                semaphore.release()

    async def _wait_for_worker(self, semaphore: asyncio.Semaphore) -> bool:
        """Wait for a free worker or for the service to stop taking
        commands, whichever comes first. True: the slot is held (the caller
        releases it). False: no slot is held."""
        if self._stopped_taking_commands():
            return False
        if not semaphore.locked():
            await semaphore.acquire()  # a worker is free: no wait
            return True
        acquire: asyncio.Future[bool] = asyncio.ensure_future(semaphore.acquire())
        stop_waits = [
            asyncio.ensure_future(self._stop_starting_event.wait()),
            asyncio.ensure_future(self._shutdown_event.wait()),
        ]
        try:
            waits: set[asyncio.Future[Any]] = {acquire, *stop_waits}
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            for wait in stop_waits:
                wait.cancel()
            await self._abandon_acquire(acquire, semaphore)
            raise
        for wait in stop_waits:
            wait.cancel()
        if not self._stopped_taking_commands() and self._acquired(acquire):
            return True
        await self._abandon_acquire(acquire, semaphore)
        return False

    @staticmethod
    def _acquired(acquire: "asyncio.Future[bool]") -> bool:
        return acquire.done() and not acquire.cancelled() and acquire.exception() is None

    @classmethod
    async def _abandon_acquire(
        cls, acquire: "asyncio.Future[bool]", semaphore: asyncio.Semaphore
    ) -> None:
        """Withdraw a pending slot request; a slot it already obtained is
        released again."""
        if not acquire.done():
            acquire.cancel()
            await asyncio.wait({acquire})
        if cls._acquired(acquire):
            semaphore.release()

    async def _settle_after_escape(self, envelope: Any) -> None:
        """Settle a delivery whose command task is ending on a
        BaseException that escaped the SDK (the task was cancelled, or a
        SystemExit / KeyboardInterrupt came from outside the handler): give
        it back without counting a retry, so it neither stays unacknowledged
        (holding a prefetch slot) nor loses its place. Never raises."""
        try:
            release = getattr(self.transport, "release", None)
            if callable(release):
                await release(envelope.message_id)
            else:
                await cast(Transport, self.transport).nack(envelope.message_id, requeue=True)
        except BaseException:
            logger.debug(
                "Settling a command whose task ended early failed",
                exc_info=True,
                extra=_command_fields(envelope),
            )

    async def _handle_command(self, command: Command, semaphore: asyncio.Semaphore) -> None:
        """
        Handle a single command.

        This method:
        1. Acquires semaphore (concurrency limiting)
        2. Creates ExecutionContext
        3. Executes handler via registry (with timeout)
        4. Retryable failure with retry budget left: waits (exponential
           backoff) and requeues WITHOUT replying
        5. Otherwise (terminal outcome): sends the one reply, then ACKs
           (success) or dead-letters (error)

        Args:
            command: Command to handle
            semaphore: Concurrency limiter
        """
        assert self.transport is not None, "the transport is built before commands arrive"
        async with self._worker_slot(semaphore) as has_worker:
            envelope = command.envelope

            if not has_worker or self._stopped_taking_commands():
                # The service stopped taking commands while this one waited
                # for a worker: give it back at once instead of running it.
                await self._release_unstarted(command)
                return

            try:
                logger.info(
                    f"Handling command {envelope.name}",
                    extra=_command_fields(envelope),
                )

                # Create execution context
                allowed = getattr(self.config, "allowed_containers", None)
                ctx = ExecutionContext(
                    transport=self.transport,
                    envelope=envelope,
                    payload=command.payload,
                    allowed_containers=(
                        frozenset(allowed) if isinstance(allowed, (set, frozenset)) else None
                    ),
                )

                # Get timeout from config
                timeout_seconds = self.config.handler_timeout if self.config else 0

                # Whether this is the command's last attempt: the handler
                # layer then attaches the traceback of an unexpected error
                # (once per command), and only to that attempt's record.
                # A transport that keeps no retry count gives no retry
                # context (None): the handler layer then logs an
                # unexpected error with its traceback on every attempt,
                # rather than never.
                max_retries = self.config.max_retries if self.config else 3
                attempts_used = (
                    self.transport.get_retry_count(envelope.message_id)
                    if hasattr(self.transport, "get_retry_count")
                    else None
                )
                final_attempt = (
                    attempts_used >= max_retries
                    if isinstance(attempts_used, int) and isinstance(max_retries, int)
                    else None
                )

                # Every earlier attempt of this command ended with the broker
                # connection lost (its run finished, or was still running,
                # when the connection dropped, so neither its reply nor its
                # acknowledgement reached the broker), and those attempts
                # used up the retry budget: it is not run again, but replied
                # and dead-lettered like a retryable failure whose retries
                # are spent. Otherwise such a command (typically a handler
                # that blocks the event loop past the heartbeat timeout)
                # would run forever. The count is kept in this process. Only
                # a transport that counts such attempts (counts_lost_attempts)
                # takes this path; any other keeps running the final attempt.
                lost_attempts_exhausted = (
                    getattr(self.transport, "counts_lost_attempts", False) is True
                    and isinstance(attempts_used, int)
                    and isinstance(max_retries, int)
                    and attempts_used > max_retries
                )
                if lost_attempts_exhausted:
                    outcome = self.registry.connection_lost_outcome(envelope)
                else:
                    # Execute handler (returns Response with success or error).
                    # While it runs, a second stop signal may cancel this task
                    # (_stop_now); from the moment it returns, the reply and the
                    # settlement below are never cancelled by that signal.
                    this_task = asyncio.current_task()
                    if this_task is not None:
                        self._tasks_in_handler.add(this_task)
                        self._handler_task_commands[this_task] = (
                            f"{envelope.name} {envelope.message_id}"
                        )
                    if self._loop_watchdog is not None:
                        # Read and reset by the watchdog on every tick.
                        self._handlers_started_since_tick.add(
                            f"{envelope.name} {envelope.message_id}"
                        )
                    try:
                        outcome = await self.registry.execute_with_outcome(
                            ctx,
                            command,
                            timeout_seconds=timeout_seconds,
                            final_attempt=final_attempt,
                            attempt=(attempts_used + 1 if isinstance(attempts_used, int) else None),
                        )
                    finally:
                        if this_task is not None:
                            self._tasks_in_handler.discard(this_task)
                            self._handler_task_commands.pop(this_task, None)

                # Decide the outcome BEFORE replying. A command gets at most
                # ONE reply, sent when the SDK has finished with it: success,
                # a non-retryable error, or a retryable error whose retry
                # budget (KBM_LEDSAS_MAX_RETRIES) is spent. An attempt that
                # will be retried sends no reply. A reply per attempt would
                # make a caller that takes the first reply as the outcome
                # see a failure for a command that then succeeds on retry
                # (and, if it had already removed its reply exchange, the
                # successful attempt would be dead-lettered).
                # The outcome is what the handler layer decided from how the
                # handler ended (it raised, timed out, or returned an
                # invalid result), never the reply's keys: a successful
                # result may carry an "error" key of its own.
                response = outcome.response
                is_error = outcome.failed
                retryable = is_error and outcome.retryable
                retry_count = 0
                if retryable and hasattr(self.transport, "get_retry_count"):
                    retry_count = self.transport.get_retry_count(envelope.message_id)

                if is_error and retryable and retry_count < max_retries:
                    # Interim failure: retry without replying. The caller
                    # receives one reply once the command finishes. INFO:
                    # the handler layer already logged this attempt's
                    # failure as its one WARNING.
                    delay = self._calculate_backoff(retry_count)
                    logger.info(
                        f"Retry {retry_count + 1}/{max_retries}, "
                        f"waiting {delay:.2f}s before requeue (no reply for "
                        f"this attempt)",
                        extra=_command_fields(envelope),
                    )
                    await self._backoff_unless_shutdown(delay)
                    await self.transport.nack(envelope.message_id, requeue=True)
                    return

                # Terminal outcome: reply once (only if reply_to is set).
                reply_unconfirmed = False
                if envelope.reply_to:
                    reply_publish_ok: object = await self.transport.send_response(response)
                    if reply_publish_ok is REPLY_UNCONFIRMED:
                        # The transport handed the reply on but could not
                        # confirm it was published (it says so in its own
                        # WARNING); an acknowledgement takes effect only once
                        # the reply is confirmed. Not logged as sent / ACKed,
                        # so neither line counts a command whose outcome is
                        # still open (it may be delivered and run again).
                        reply_unconfirmed = True
                        logger.info(
                            f"Reply to {envelope.name} handed to the transport "
                            "unconfirmed"
                            + (
                                ""
                                if is_error
                                else "; acknowledgement deferred until the " "reply is confirmed"
                            ),
                            extra={**_command_fields(envelope), "reply_confirmed": False},
                        )
                    elif reply_publish_ok:
                        logger.info(
                            f"Sent response for {envelope.name}",
                            extra=_command_fields(envelope),
                        )
                    elif reply_publish_ok is ERROR_REPLY_SENT:
                        # The transport sent an error reply in place of the
                        # result (it could not be sent as a reply) and
                        # logged that once, as an ERROR. The caller has its
                        # reply; the command is dead-lettered, and this is
                        # not a failed publish.
                        await self.transport.nack(envelope.message_id, requeue=False)
                        logger.info(
                            f"Replied an error in place of the result of "
                            f"{envelope.name} (reported above); command "
                            "dead-lettered",
                            extra=_command_fields(envelope),
                        )
                        return
                    elif self._broker_unreachable_at_shutdown():
                        # Shutdown in progress and the transport found the
                        # broker unreachable: the reply could not be
                        # confirmed, and neither an ACK nor a reject would
                        # reach the broker reliably. Leave the command
                        # unacknowledged: the broker redelivers it once the
                        # connection is gone (the transport logged this and
                        # reports the outage once at stop).
                        return
                    else:
                        # Reply publish failed (e.g. the caller's reply
                        # exchange missing). The transport has already
                        # logged the failure (ERROR) and bumped its
                        # counter. Treat this command as Permanent-failed:
                        # retrying would just keep failing the publish and
                        # the handler would re-execute uselessly. NACK to
                        # DLQ. INFO here: the cause is the transport's line.
                        logger.info(
                            f"Reply publish failed for {envelope.name}; "
                            "command will be dead-lettered",
                            extra=_command_fields(envelope),
                        )
                        await self.transport.nack(envelope.message_id, requeue=False)
                        return
                else:
                    logger.info(
                        f"Skipping response for {envelope.name} (no reply_to)",
                        extra=_command_fields(envelope),
                    )

                if not is_error:
                    # Success
                    await self.transport.ack(envelope.message_id)
                    if reply_unconfirmed:
                        logger.info(
                            f"ACK of command {envelope.name} deferred until "
                            "its reply is confirmed",
                            extra=_command_fields(envelope),
                        )
                    else:
                        logger.info(
                            f"ACKed command {envelope.name}",
                            extra=_command_fields(envelope),
                        )
                elif retryable:
                    # Retry budget spent - send to DLQ. The ERROR is logged
                    # once the dead-letter has been sent: a dead-letter
                    # that cannot be sent (the connection was lost, or the
                    # broker is unreachable while stopping) raises, the
                    # transport logs that once and the broker delivers the
                    # command again, so the record would claim a
                    # dead-letter that did not happen. It names the error
                    # of the last attempt (the handler layer's per-attempt
                    # WARNINGs carry the details).
                    await self.transport.nack(envelope.message_id, requeue=False)
                    final_fields = {
                        **_command_fields(envelope),
                        "retry_count": retry_count,
                        "attempt": (attempts_used + 1 if isinstance(attempts_used, int) else None),
                        "error_class": getattr(outcome, "error_class", None),
                        "error_message": getattr(outcome, "error_message", None),
                    }
                    if lost_attempts_exhausted:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded, sending to "
                            f"DLQ: command {envelope.name} was delivered again "
                            f"after {attempts_used} attempt(s) that did not "
                            f"finish, the last one(s) because the connection to "
                            f"the AMQP broker was lost while the command ran; "
                            f"it is not run again. A handler that blocks the "
                            f"event loop for longer than about two heartbeat "
                            f"intervals (KBM_LEDSAS_HEARTBEAT) loses the "
                            f"connection: run "
                            f"blocking work with asyncio.to_thread",
                            extra=final_fields,
                        )
                    else:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded, sending to DLQ",
                            extra=final_fields,
                        )
                else:
                    # Non-retryable error - send to DLQ immediately. The
                    # handler layer has already logged the cause once, at
                    # its own level (ERROR for a permanent failure or an
                    # invalid result, WARNING for DeadlineExceeded), so
                    # this outcome line is INFO: one failure, one ERROR.
                    # Logged once the dead-letter has been sent (as above).
                    await self.transport.nack(envelope.message_id, requeue=False)
                    logger.info(
                        "Command failed with a non-retryable error, sending to DLQ",
                        extra=_command_fields(envelope),
                    )

            except DeliveryLost:
                # The broker connection this command arrived on was lost
                # while it ran (e.g. a broker restart): its reply or its
                # ACK / NACK could not be sent. The transport logged that
                # once (WARNING); the broker delivers the command again.
                return
            except Exception as e:
                if isinstance(e, ConnectionError) and self._broker_unreachable_at_shutdown():
                    # Shutdown in progress and the transport found the
                    # broker unreachable while settling this command (ACK,
                    # requeue or dead-letter): it kept the command
                    # unacknowledged, logged that once, and the broker
                    # redelivers it once the connection is gone. A reject
                    # here would not reach the broker either.
                    return
                # Uncaught exception in _handle_command itself, NOT in
                # the registered handler (registry.execute already
                # catches handler exceptions and translates them).
                # By definition we don't know what to do with this, and
                # a retry would just hit the same bug. Dead-letter
                # immediately so operators see the failure instead of
                # the message bouncing forever.
                logger.exception(
                    "Uncaught exception in _handle_command; dead-lettering",
                    extra=_command_fields(envelope),
                )
                try:
                    await self.transport.nack(envelope.message_id, requeue=False)
                except Exception as nack_error:
                    logger.exception(
                        "Failed to NACK message: %s",
                        nack_error,
                        extra=_command_fields(envelope),
                    )
            except BaseException:
                # The handler's own BaseExceptions (CancelledError it raised,
                # SystemExit, ...) are handled by registry.execute. Reaching
                # here means this task itself was cancelled, or SystemExit /
                # KeyboardInterrupt came from outside the handler: settle the
                # delivery (given back for redelivery) so it never stays
                # unacknowledged, then let the exception propagate.
                await self._settle_after_escape(envelope)
                raise

    async def _begin_transport_shutdown(self) -> None:
        """Tell the transport that shutdown has begun (optional hook).

        A transport may expose ``begin_shutdown()`` (sync or async) to bound
        its remaining broker round trips from this point on. Transports
        without it are unaffected.
        """
        begin = getattr(self.transport, "begin_shutdown", None)
        if not callable(begin):
            return
        try:
            result = begin()
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.debug("Transport begin_shutdown hook failed", exc_info=True)

    async def _backoff_unless_shutdown(self, delay: float) -> None:
        """Wait ``delay`` seconds before requeueing a retried command, but
        stop waiting as soon as shutdown is requested: the command is then
        requeued at once (another consumer, or this service after its
        restart, retries it) instead of holding the drain for up to the
        maximum backoff, which could outlast the termination grace period.
        Outside shutdown this is a plain sleep of ``delay`` seconds.
        """
        if delay <= 0 or self._shutdown_event.is_set():
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._shutdown_event.wait(), timeout=delay)
        except TimeoutError:
            pass

    # The event-loop watchdog wakes up every _LOOP_WATCH_INTERVAL_S seconds.
    # A wake-up L seconds late means the loop was held for at least L and at
    # most L + _LOOP_WATCH_INTERVAL_S seconds (the block may have started up
    # to one interval before the wake-up was due). It warns once
    # L + _LOOP_WATCH_INTERVAL_S reaches _LOOP_BLOCK_WARN_S, so every block
    # of _LOOP_BLOCK_WARN_S or more is reported (one up to an interval
    # shorter may be), with L as the length ("at least"), at most once
    # every _LOOP_BLOCK_WARN_EVERY_S seconds.
    _LOOP_WATCH_INTERVAL_S = 0.1
    _LOOP_BLOCK_WARN_S = 1.0
    _LOOP_BLOCK_WARN_EVERY_S = 60.0

    async def _watch_event_loop(self) -> None:
        """Log one WARNING when the event loop was blocked for
        ``_LOOP_BLOCK_WARN_S`` seconds or more.

        Blocking code on the event loop (``time.sleep``, a CPU-heavy loop
        or a synchronous client call in an ``async def`` handler, a sync
        health check) stops everything else in the process while it runs:
        other commands, the health endpoints and the connection's
        heartbeats, and ``KBM_LEDSAS_HANDLER_TIMEOUT`` cannot stop it (a
        handler can only be cancelled at an ``await``). The SDK's own
        reading of a very large command body does the same; when the
        transport reports such reads (``take_slow_body_reads``) the WARNING
        names them, and names the handlers only when the reads do not
        explain the block. This task cannot run during the block either,
        so the WARNING comes right after it, with its length and the
        commands whose handlers ran during it. At most one WARNING every
        ``_LOOP_BLOCK_WARN_EVERY_S`` seconds; the next one counts the blocks
        not logged in between. Never raises.
        """
        loop = asyncio.get_running_loop()
        last_warning: float | None = None
        not_logged = 0
        # A wake-up this late may follow a block of _LOOP_BLOCK_WARN_S.
        late_enough = self._LOOP_BLOCK_WARN_S - self._LOOP_WATCH_INTERVAL_S
        try:
            # Reads from before the watchdog started belong to no tick.
            self._take_slow_body_reads()
            while True:
                ran = set(self._handler_task_commands.values())
                self._handlers_started_since_tick = set()
                before = loop.time()
                await asyncio.sleep(self._LOOP_WATCH_INTERVAL_S)
                blocked = loop.time() - before - self._LOOP_WATCH_INTERVAL_S
                # Nothing runs between this wake-up and the next tick's
                # start, so these are exactly the reads of this tick.
                reads = self._take_slow_body_reads()
                ran |= self._handlers_started_since_tick
                ran |= set(self._handler_task_commands.values())
                if blocked < late_enough:
                    continue
                now = loop.time()
                if last_warning is not None and now - last_warning < self._LOOP_BLOCK_WARN_EVERY_S:
                    not_logged += 1
                    continue
                last_warning = now
                self._warn_blocked_loop(blocked, reads, sorted(ran), not_logged)
                not_logged = 0
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - a watchdog never stops the service
            logger.debug("Event-loop watchdog stopped", exc_info=True)

    def _take_slow_body_reads(self) -> list[tuple[float, int]]:
        """``(seconds, body bytes)`` of each command body whose reading held
        the event loop noticeably since the last call, from the transport's
        optional ``take_slow_body_reads`` (an empty list without one)."""
        take = getattr(self.transport, "take_slow_body_reads", None)
        if not callable(take):
            return []
        try:
            reads = take()
            if not isinstance(reads, list):
                return []
            return [(float(seconds), int(size)) for seconds, size in reads]
        except Exception:
            return []

    def _warn_blocked_loop(
        self,
        blocked: float,
        reads: list[tuple[float, int]],
        ran: list[str],
        not_logged: int,
    ) -> None:
        """The watchdog's WARNING for one block (see ``_watch_event_loop``):
        ``blocked`` is how late the wake-up came, ``reads`` the SDK's own
        slow body reads during the block, ``ran`` the handlers in progress."""
        read_s = sum(seconds for seconds, _size in reads)
        # A read is one uninterrupted hold, so the block lasted at least as
        # long as the longest one.
        held = max([blocked, *(seconds for seconds, _size in reads)])

        # Rounded down to tenths (the epsilon absorbs float error, 2.3 being
        # 22.999... tenths), so the lengths logged stay lower bounds.
        def tenths_down(value: float) -> float:
            return math.floor(value * 10 + 1e-9) / 10

        held_shown = tenths_down(held)
        text = (
            f"The event loop was blocked for at least {held_shown:.1f} s: "
            "nothing else in this service ran meanwhile (other commands, the "
            "health endpoints, keeping the connection alive), and "
            "KBM_LEDSAS_HANDLER_TIMEOUT cannot stop such a block."
        )
        if reads:
            largest = max(size for _seconds, size in reads)
            what = (
                f"a command body of {largest} bytes"
                if len(reads) == 1
                else f"{len(reads)} command bodies (the largest {largest} bytes)"
            )
            text += (
                f" The SDK spent {tenths_down(read_s):.1f} s of it reading {what}: it "
                "decodes, parses and checks each command body on the event "
                "loop. Keep command bodies small (send the data in a blob and "
                "its reference in the body) and set "
                "KBM_LEDSAS_MAX_PAYLOAD_BYTES to what your commands need."
            )
        # The handlers are named unless the SDK's reads explain the block
        # (to within the watchdog's interval).
        if not reads or held - read_s > self._LOOP_WATCH_INTERVAL_S:
            # A transport that reports its slow reads has had none here.
            reads_reported = callable(getattr(self.transport, "take_slow_body_reads", None))
            shown = ran[:20]
            more = len(ran) - len(shown)
            text += (
                (" The rest comes" if reads else " It comes")
                + " from blocking code in an async handler (time.sleep, a "
                "CPU-heavy loop, a synchronous client call) or in a sync "
                "health check (run it with asyncio.to_thread), or from the "
                "SDK's own work on a very large "
                + ("result" if reads_reported else "command body or result")
                + " (send large data in a blob). Handlers running during the "
                "block: "
                + (", ".join(shown) if shown else "none")
                + (f" (and {more} more)" if more > 0 else "")
            )
        extra: dict[str, Any] = {
            "loop_blocked_s": round(held, 3),
            "blocks_not_logged": not_logged,
        }
        if reads:
            extra["body_read_s"] = round(read_s, 3)
        logger.warning("%s", text, extra=extra)

    def _broker_unreachable_at_shutdown(self) -> bool:
        """True when the transport reports the broker unreachable during
        shutdown (optional ``broker_unreachable_at_shutdown`` attribute;
        only an explicit True counts)."""
        return getattr(self.transport, "broker_unreachable_at_shutdown", False) is True

    async def _shutdown(self) -> None:
        """
        Graceful shutdown.

        Stops transport and cleans up resources.
        """
        if not self._running:
            return

        logger.info("Shutting down ServiceApp...")
        self._running = False

        watchdog, self._loop_watchdog = self._loop_watchdog, None
        if watchdog is not None:
            watchdog.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog

        # Stop the health server before the transport. It answered 503 on
        # /ready from the stop signal on (see default_readiness), through
        # the drain; from here on probes are refused.
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

    async def _start_health_server(self) -> None:
        """Boot the SDK health server, unless disabled by config."""
        assert self.config is not None  # set in _run_async before this call
        if self.config.health_port == 0:
            logger.info("Health server disabled by KBM_LEDSAS_HEALTH_PORT=0")
            return

        async def default_readiness() -> CheckResult:
            if self._stopped_taking_commands() and not self._transport_lost:
                # A stop signal arrived: the service takes no new commands
                # and only finishes the running ones (the drain can last
                # up to KBM_LEDSAS_HANDLER_TIMEOUT), so it is not ready
                # from this moment on. /health still answers 200.
                return CheckResult(name="transport", healthy=False, detail="shutting down")
            ready = bool(self.transport and not self._transport_lost and self.transport.is_ready())
            return CheckResult(
                name="transport",
                healthy=ready,
                detail="" if ready else "transport not connected",
            )

        # deployment_id (KBM_LEDSAS_DEPLOYMENT_ID, else KBM_LASTCOMMITID) in /health and /ready.
        emit_deployment_id = True

        self.health_server = HealthServer(
            service_name=getattr(self.config, "service_name", None) or self.service_name,
            host=self.config.health_host,
            port=self.config.health_port,
            liveness_registry=self.liveness_checks,
            readiness_registry=self.readiness_checks,
            default_readiness=default_readiness,
            verbose=self.config.health_verbose,
            emit_deployment_id=emit_deployment_id,
        )
        await self.health_server.start()

        # Say once that the endpoints are reachable from the network, and
        # only after the bind succeeded (a failed bind stops the start:
        # HealthServer.start logs one ERROR and raises
        # HealthServerStartFailure). Binding all interfaces is what an
        # orchestrator's HTTP probes need, so it is INFO; with
        # KBM_LEDSAS_HEALTH_VERBOSE the bodies also name the service and
        # SDK version to anyone who can reach the port, so that
        # combination is a WARNING.
        loopback_hosts = {"127.0.0.1", "::1", "localhost"}
        if (
            self.health_server.bound_port is not None
            and self.config.health_host not in loopback_hosts
        ):
            if self.config.health_verbose:
                logger.warning(
                    "Health server on %s is reachable from the network and "
                    "KBM_LEDSAS_HEALTH_VERBOSE is on: its responses name the "
                    "service and the SDK version to anyone who can reach the "
                    "port. Turn KBM_LEDSAS_HEALTH_VERBOSE off outside "
                    "development, or restrict who can reach the port.",
                    _host_port(self.config.health_host, self.config.health_port),
                )
            else:
                logger.info(
                    "Health server on %s is reachable from the network (as "
                    "an orchestrator's HTTP probes need). Restrict who can "
                    "reach the port (for example with a network policy), or "
                    "set KBM_LEDSAS_HEALTH_HOST=127.0.0.1 when nothing outside "
                    "the container probes it.",
                    _host_port(self.config.health_host, self.config.health_port),
                )

    @property
    def health_server_running(self) -> bool:
        """Whether the health server is actually listening.

        Returns False when the health server is disabled
        (``KBM_LEDSAS_HEALTH_PORT=0``), before the service has started it
        and after it was stopped. A port it cannot listen on (in use,
        permission denied) stops the service at start with one ERROR
        naming the setting. Useful for integration tests that need to
        assert "the health endpoint is up before I probe it."
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
        delay: float = min(base_delay * (2**retry_count) + jitter, max_delay)
        return delay

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"ServiceApp(service_name={self.service_name!r}, "
            f"handlers={len(self.registry.list_handlers())}, "
            f"running={self._running})"
        )
