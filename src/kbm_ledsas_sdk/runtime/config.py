"""
SDK configuration from environment variables.

Loads connection, performance, logging, error-handling, and health-endpoint
settings from ``KBM_LEDSAS_*`` environment variables. See
:meth:`SDKConfig.from_env` for the full variable list.
"""

import ipaddress
import os
import re
from typing import Any, ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from kbm_ledsas_sdk.blob.operations import parse_allowed_containers
from kbm_ledsas_sdk.runtime.security import (
    _is_truthy,
    _read_env,
    check_topology_name_budget,
    check_transport_security,
    parse_bool_env,
    redacted,
)
from kbm_ledsas_sdk.utils.logging import masked_logger, register_connection_secrets

_logger = masked_logger(__name__)


# Free-text settings: any value is accepted, so an inline "# comment" that
# ``docker run --env-file`` passes through verbatim would be kept in them
# without a word (the broker URL refuses it, a number does not parse, the
# service name and tenant are refused by their pattern).
_FREE_TEXT_SETTINGS = ("KBM_LEDSAS_DEPLOYMENT_ID", "KBM_LASTCOMMITID", "KBM_LEDSAS_CONTAINER")
_INLINE_COMMENT = re.compile(r"\s#")


def _warn_inline_comments() -> None:
    """One WARNING naming the free-text settings whose value looks like it
    carries an inline ``# comment`` (the value is kept, and not logged)."""
    found = [name for name in _FREE_TEXT_SETTINGS if _INLINE_COMMENT.search(_env(name) or "")]
    if found:
        _logger.warning(
            "%s looks like it carries an inline '# comment' (a space then "
            "'#'): docker run --env-file passes the rest of the line "
            "through verbatim, so the comment is part of the value. Put "
            "comments on their own line.",
            " / ".join(found),
            extra={"settings": found},
        )


def _env(name: str, default: str | None = None) -> str | None:
    """Read a ``KBM_LEDSAS_*`` variable the same way however it was set.

    Surrounding whitespace (a trailing carriage return from a Windows
    ``.env`` file, a newline from a secret file) and ONE matched pair of
    surrounding quotes are removed. ``docker run --env-file`` passes quotes
    through verbatim, while the shell ``set -a; . ./.env`` path removes
    them, so ``KBM_LEDSAS_LOG_LEVEL="INFO"`` gives ``INFO`` either way.
    Returns ``default`` when the variable is unset. (Shared with the other
    readers of ``KBM_LEDSAS_*`` variables: ``security._read_env``.)
    """
    return _read_env(name, default)


def _int_env(name: str, default: str) -> int:
    """Parse an integer env var, naming the variable on parse failure.

    ``int(os.getenv(name, default))`` raises ValueError with only
    ``invalid literal for int() with base 10: 'abc'`` — a customer with
    multiple env vars set can't tell which one was misconfigured. This
    helper names the offending variable in the error so the startup
    message points at the actual knob.
    """
    raw = _env(name, default)
    try:
        return int(raw)
    except ValueError:
        if raw == "":
            # Set but empty (``KBM_LEDSAS_PREFETCH=`` in a .env file): refused
            # like any other value that is not a number, saying how to get
            # the default.
            raise ValueError(
                f"{name}={os.getenv(name, default)!r} is empty: unset it to use "
                f"the default ({default}) or give a whole number"
            ) from None
        raise ValueError(f"{name}={os.getenv(name, default)!r} is not a valid integer") from None


def _bool_env(name: str, default: str = "false") -> bool:
    """Parse a boolean env var, refusing a value that is not a boolean.

    One parser for every boolean setting (GENERIC_ERRORS, HEALTH_VERBOSE,
    ALLOW_INSECURE_AMQP, DEBUG): ``1``, ``true``, ``yes``, ``on``, ``t``,
    ``y`` are true, ``0``, ``false``, ``no``, ``off``, ``f``, ``n`` false
    (any letter case); unset or empty gives the default. Any other value (a
    typo such as ``ture``) is a configuration error naming the variable:
    treating it as false would, for example, leave
    ``KBM_LEDSAS_GENERIC_ERRORS=ture`` sending detailed error messages to
    callers, the opposite of what was asked.
    """
    return parse_bool_env(name, _is_truthy(default))


# The environment variable behind each SDKConfig field, for configuration
# error messages.
_FIELD_ENV_VARS = {
    "service_name": "KBM_LEDSAS_SERVICE_NAME",
    "tenant": "KBM_LEDSAS_TENANT",
    "rabbitmq_url": "KBM_LEDSAS_RABBITMQ_URL",
    "blob_conn_string": "KBM_LEDSAS_BLOB_CONN_STRING",
    "blob_container": "KBM_LEDSAS_CONTAINER",
    "prefetch": "KBM_LEDSAS_PREFETCH",
    "concurrency": "KBM_LEDSAS_CONCURRENCY",
    "log_level": "KBM_LEDSAS_LOG_LEVEL",
    "log_format": "KBM_LEDSAS_LOG_FORMAT",
    "handler_timeout": "KBM_LEDSAS_HANDLER_TIMEOUT",
    "max_retries": "KBM_LEDSAS_MAX_RETRIES",
    "generic_errors": "KBM_LEDSAS_GENERIC_ERRORS",
    "max_payload_bytes": "KBM_LEDSAS_MAX_PAYLOAD_BYTES",
    "health_port": "KBM_LEDSAS_HEALTH_PORT",
    "health_host": "KBM_LEDSAS_HEALTH_HOST",
    "health_verbose": "KBM_LEDSAS_HEALTH_VERBOSE",
    "connect_timeout": "KBM_LEDSAS_CONNECT_TIMEOUT",
    "heartbeat": "KBM_LEDSAS_HEARTBEAT",
    "allowed_containers": "KBM_LEDSAS_ALLOWED_CONTAINERS",
}
# Fields whose value is never repeated in a message.
_SECRET_FIELDS = frozenset({"rabbitmq_url", "blob_conn_string"})
# Placeholder for the input of an SDKConfig ValidationError's errors() /
# json() that may hold a connection secret (see
# _config_error_without_secrets).
_INPUT_NOT_KEPT = "<not kept: the settings may hold connection secrets>"


def _config_error_without_secrets(exc: ValidationError) -> ValidationError:
    """``exc`` (an SDKConfig ValidationError) without connection secrets in
    its ``errors()`` / ``json()`` (what error reporters serialise; pydantic
    keeps the input there even with ``hide_input_in_errors``, which only
    governs ``str()`` / ``repr()``). The input of an error about one
    setting that is not a connection value is kept (configuration error
    messages quote it); the input of an error about a connection value, or
    of a check of the whole configuration (its input is every setting, the
    broker URL and the storage connection string included), becomes a
    placeholder."""
    details: list = []
    for err in exc.errors(include_url=False):
        loc = err.get("loc", ())
        field = str(loc[0]) if loc else ""
        keep = field in _FIELD_ENV_VARS and field not in _SECRET_FIELDS
        detail: dict = {
            "type": err["type"],
            "loc": loc,
            "input": err.get("input") if keep else _INPUT_NOT_KEPT,
        }
        if "ctx" in err:
            detail["ctx"] = err["ctx"]
        details.append(detail)
    try:
        return ValidationError.from_exception_data(exc.title, details, hide_input=True)
    except Exception:  # an error type that cannot be rebuilt: keep the text only
        return ValidationError.from_exception_data(
            exc.title,
            [
                {
                    "type": "value_error",
                    "loc": err.get("loc", ()),
                    "input": _INPUT_NOT_KEPT,
                    "ctx": {
                        "error": ValueError(str(err.get("msg", "")).removeprefix("Value error, "))
                    },
                }
                for err in exc.errors(include_url=False)
            ],
            hide_input=True,
        )


_PLAIN_NUMBER = re.compile(r"^-?\d{1,30}$")


def _shown_choice(value: str) -> str:
    """A refused value of an enumerated setting as its message shows it:
    as written, or ``''`` with how to get the default when it is empty."""
    if not value.strip():
        return "'' (empty: unset the variable to use the default)"
    return value[:80]


# AMQP heartbeat interval, in seconds, the SDK asks the broker for by
# default (KBM_LEDSAS_HEARTBEAT): the interval earlier releases ended up
# with (the client library's default request, which a broker with its own
# default of 60 s accepts), now set explicitly.
DEFAULT_HEARTBEAT_S = 60


def _shown_setting(value: Any) -> str:
    if not isinstance(value, str):
        # A value from the code (ServiceApp(service_name=None)) is shown
        # as written there: None, not the string 'None'.
        text = repr(value)
        return text if len(text) <= 80 else text[:77] + "..."
    if _PLAIN_NUMBER.fullmatch(value):
        return value
    return repr(value[:80])


# The rule the service_name field applies (pattern and length), for the
# ServiceApp constructor's own check.
_SERVICE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._\-]*$")
SERVICE_NAME_RULE = (
    "a service name is 1 to 64 characters: letters, digits, '.', '_' and "
    "'-', starting with a letter or digit"
)


def service_name_problem(value: Any) -> str | None:
    """Why ``value`` cannot be a service name (the rule of the
    ``service_name`` setting), or None."""
    if not isinstance(value, str):
        return f"the service name must be a string, got {type(value).__name__}"
    if not 1 <= len(value) <= 64 or not _SERVICE_NAME.fullmatch(value):
        return SERVICE_NAME_RULE
    return None


def describe_config_errors(error: ValidationError) -> str:
    """The text of a configuration error, one part per problem, each
    naming the setting to fix: the ``KBM_LEDSAS_*`` variable and the value
    it holds (``KBM_LEDSAS_PREFETCH=5000: Input should be less than or
    equal to 1000``), or ``ServiceApp(service_name=...)`` when the service
    name came from the code. Connection values are never repeated."""
    parts = []
    for err in error.errors():
        loc = err.get("loc", ())
        msg = str(err.get("msg", "")).removeprefix("Value error, ")
        field = str(loc[0]) if loc else ""
        var = _FIELD_ENV_VARS.get(field)
        if not field:
            label = ""
        elif var is None:
            label = ".".join(str(p) for p in loc)
        elif msg.startswith(var):
            label = ""  # the message already names the variable
        else:
            raw = _read_env(var)
            if raw is None or raw == "":
                if field == "service_name":
                    label = f"ServiceApp(service_name={_shown_setting(err.get('input', ''))})"
                else:
                    label = var
            elif field in _SECRET_FIELDS:
                label = var
            else:
                label = f"{var}={_shown_setting(raw)}"
        if msg:
            parts.append(f"{label}: {msg}" if label else msg)
    return "; ".join(parts) if parts else str(error)


# One host-name label: letters, digits and hyphens, not starting or ending
# with a hyphen, 1..63 characters.
_HOST_LABEL = re.compile(r"^(?!-)[A-Za-z0-9-]{1,63}(?<!-)$")


def _is_ipv6(value: str) -> bool:
    try:
        return isinstance(ipaddress.ip_address(value), ipaddress.IPv6Address)
    except ValueError:
        return False


def _is_valid_bind_host(value: str) -> bool:
    """True for an IPv4/IPv6 address or a syntactically valid host name.

    A dotted all-digits value that is not a valid IPv4 address (for example
    ``999.1.1.1``) is refused: it was meant as an address. So is an IPv6
    address in brackets (``[::]``, the URL form): the server cannot bind to
    it, it needs the plain address (``::``).
    """
    if not isinstance(value, str) or not value or len(value) > 253:
        return False
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        pass
    if re.fullmatch(r"[0-9.]+", value):
        return False
    labels = value[:-1].split(".") if value.endswith(".") else value.split(".")
    return all(_HOST_LABEL.fullmatch(label) for label in labels)


class SDKConfig(BaseModel):
    """
    SDK configuration loaded from environment variables.

    Connects to RabbitMQ and Azure Blob Storage. Configuration is loaded
    from environment variables; see :meth:`from_env` for the full variable
    list.
    """

    # A ValidationError does not repeat the settings it was given: not in
    # str() (hide_input_in_errors), and not in errors() / json() for the
    # connection values (see _input_not_kept).
    model_config: ClassVar[ConfigDict] = ConfigDict(hide_input_in_errors=True)

    # Core
    #
    # ``service_name`` and ``tenant`` are interpolated into AMQP
    # topology names (``cmd.{tenant}.{service_name}.v1``) and into every
    # SDK log line — same risk class as the envelope's trace_id /
    # idempotency_key / job_id fields. Constrain them to the same
    # URL-safe / log-safe shape so an env-var typo (`KBM_LEDSAS_SERVICE_NAME='my service'`,
    # newlines from a YAML scalar block) fails at config-load time with
    # a clean message rather than producing surprising topology names.
    service_name: str = Field(
        ...,
        description="Service name (e.g., 'image_processor'). URL-safe, 1..64 chars.",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._\-]*$",
    )
    tenant: str | None = Field(
        None,
        description="Tenant identifier (optional). Same URL-safe shape as service_name.",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._\-]*$",
    )

    rabbitmq_url: str | None = Field(None, description="RabbitMQ connection URL")

    # Blob storage
    blob_conn_string: str | None = Field(None, description="Azure Blob connection string")
    blob_container: str = Field(
        "dev",
        description=(
            "Informational: logged at startup as default_container; every "
            "upload and download names its own container"
        ),
    )

    # Performance
    prefetch: int = Field(10, ge=1, le=1000, description="Message prefetch count")
    concurrency: int = Field(4, ge=1, le=100, description="Max concurrent handlers")

    # Logging
    log_level: str = Field("INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")
    log_format: str = Field("json", description="Log format (json or text)")

    # Error handling and retry
    #
    # Cap ``handler_timeout`` at 86 400 s (1 day) and
    # ``max_payload_bytes`` at 256 MiB. Both knobs previously accepted
    # arbitrary ge=0 values, which silently masked operator typos
    # ("KBM_LEDSAS_HANDLER_TIMEOUT=99999999999"). The new ceilings are
    # well above any sane production setting and well below the broker's
    # 128 MiB frame-max + protocol limits.
    handler_timeout: int = Field(
        1800,
        ge=0,
        le=86400,
        description="Handler timeout in seconds (0=disabled, max 86400=1 day)",
    )
    max_retries: int = Field(3, ge=0, le=100, description="Maximum retry attempts")
    generic_errors: bool = Field(False, description="Return generic error messages")
    max_payload_bytes: int = Field(
        16 * 1024 * 1024,
        ge=0,
        le=256 * 1024 * 1024,
        description=(
            "Max AMQP message body in bytes (0 disables; default 16 MiB; "
            "max 256 MiB). Large payloads should travel via blob "
            "storage, not the AMQP body."
        ),
    )

    # Health endpoints
    health_port: int = Field(
        8090,
        ge=0,
        le=65535,
        description="HTTP port for liveness/readiness endpoints (0 disables)",
    )
    health_host: str = Field(
        "127.0.0.1",
        description=(
            "Bind address for the health server (default: loopback only): "
            "an IPv4 or IPv6 address, or a host name"
        ),
    )
    health_verbose: bool = Field(
        False,
        description=(
            "If True, health endpoints include SDK version, service "
            "name, and check-by-check status (fingerprintable, useful "
            "for development). If False (default), minimal "
            '{"status":"healthy"|"unhealthy"} response only.'
        ),
    )

    connect_timeout: int = Field(
        15,
        ge=1,
        le=300,
        description=(
            "Seconds one connection attempt to the message broker may take " "(1..300, default 15)"
        ),
    )

    heartbeat: int = Field(
        DEFAULT_HEARTBEAT_S,
        ge=0,
        le=600,
        description=(
            "AMQP heartbeat interval in seconds requested from the broker "
            "unless the broker URL sets ?heartbeat= (0..600, 0 = off, "
            "default 60)"
        ),
    )

    allowed_containers: frozenset[str] | None = Field(
        None,
        description=(
            "Containers handler code may use through ctx.blob "
            "(KBM_LEDSAS_ALLOWED_CONTAINERS, comma-separated); None (unset or "
            "blank) for no restriction"
        ),
    )

    @field_validator("allowed_containers", mode="before")
    @classmethod
    def _parse_allowed_containers(cls, v: Any) -> Any:
        """A comma-separated string or a collection of container names;
        each must be a valid container name (see
        ``parse_allowed_containers``)."""
        if v is None:
            return None
        if isinstance(v, str):
            return parse_allowed_containers(v)
        if isinstance(v, (list, tuple, set, frozenset)):
            if not all(isinstance(name, str) for name in v):
                raise ValueError("allowed_containers holds a value that is not a string")
            return parse_allowed_containers(",".join(v)) if v else None
        raise ValueError(
            f"allowed_containers must be a comma-separated string or a collection "
            f"of names, got {type(v).__name__}"
        )

    @field_validator("rabbitmq_url", "blob_conn_string")
    @classmethod
    def _hold_redacted(cls, v: str | None) -> str | None:
        """Keep the two connection settings as :class:`RedactedStr`, so a
        display of the config's attributes (``vars(config)`` in a repr,
        ``pprint``, ``rich.inspect``) does not show the credentials; the
        value itself is unchanged."""
        return redacted(v)

    @field_validator("health_host")
    @classmethod
    def validate_health_host(cls, v: str) -> str:
        """An IP address or a host name (refused at load, like the other
        settings, instead of failing to bind later).

        Internal: a pydantic validator the model runs itself when it is built;
        not part of the public API (it may change).
        """
        if _is_valid_bind_host(v):
            return v
        inner = v[1:-1] if len(v) > 2 and v.startswith("[") and v.endswith("]") else None
        if inner is not None and _is_ipv6(inner):
            raise ValueError(
                f"KBM_LEDSAS_HEALTH_HOST={v[:80]!r}: write an IPv6 address "
                f"without brackets (:: for all interfaces, ::1 for loopback "
                f"only)"
            )
        raise ValueError(
            f"KBM_LEDSAS_HEALTH_HOST={v[:80]!r} is not an IP address or a "
            f"host name (use 127.0.0.1 for loopback only, 0.0.0.0 for all "
            f"interfaces)"
        )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid.

        Internal: a pydantic validator the model runs itself when it is built;
        not part of the public API (it may change).
        """
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(
                f"Invalid log level: {_shown_choice(v)}. " f"Must be one of {valid_levels}"
            )
        return v_upper

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format is valid.

        Internal: a pydantic validator the model runs itself when it is built;
        not part of the public API (it may change).
        """
        valid_formats = ["json", "text"]
        v_lower = v.lower()
        if v_lower not in valid_formats:
            raise ValueError(
                f"Invalid log format: {_shown_choice(v)}. " f"Must be one of {valid_formats}"
            )
        return v_lower

    @model_validator(mode="after")
    def _validate_topology_and_transport(self) -> "SDKConfig":
        """Cross-field checks that run after every field is populated.

        Two independent failure modes both fire here:

        1. Worst-case AMQP topology name length. Each of
           ``service_name``/``tenant`` is independently capped at 64
           chars, but the SDK interpolates them into
           ``dlq.queue.{tenant}.{service_name}.v1`` which has its own
           14-char fixed overhead — combined length must fit AMQP's
           127-byte protocol limit.
        2. Cleartext-AMQP refusal. Running the check here at
           config-load (rather than inside ``DirectTransport.start``)
           means the customer sees a clean
           ``Configuration error: amqp:// to non-loopback host ...``
           on the first line of output, rather than the same error
           buried after ~7 INFO lines of half-built transport state.
        """
        # Only check when the SDK is actually configured (no point
        # complaining about topology length on an uninitialized config).
        if self.service_name:
            check_topology_name_budget(self.service_name, self.tenant)
        # Broker URL checks at config-load. Only fire when a URL is set; a
        # None url is handled later by the factory's "SDK is not
        # configured" path.
        if self.rabbitmq_url:
            check_transport_security(self.rabbitmq_url)
        # The configured secrets are masked wherever a record quotes them,
        # in any form (see register_secret).
        register_connection_secrets(self.rabbitmq_url, self.blob_conn_string)
        return self

    @model_validator(mode="wrap")
    @classmethod
    def _input_not_kept(cls, data: Any, handler: Any) -> Any:
        """A rejected configuration's connection values are not kept in the
        ValidationError (see _config_error_without_secrets). Defined after
        the other validators so it wraps every check. Raised outside the
        ``except`` block: ``raise ... from None`` would still keep the
        original error as ``__context__``."""
        try:
            return handler(data)
        except ValidationError as exc:
            clean = _config_error_without_secrets(exc)
        raise clean

    @classmethod
    def from_env(cls, service_name: str, tenant: str | None = None) -> "SDKConfig":
        """
        Create config from environment variables.

        Args:
            service_name: Service name. ``KBM_LEDSAS_SERVICE_NAME`` env var
                takes precedence over this argument when set and not empty
                (an empty or blank value is read as unset).
            tenant: Tenant name. ``KBM_LEDSAS_TENANT`` env var takes
                precedence over this argument when set.

        Environment Variables:
            KBM_LEDSAS_SERVICE_NAME: Service name (overrides arg)
            KBM_LEDSAS_TENANT: Tenant name (overrides arg)
            KBM_LEDSAS_RABBITMQ_URL: RabbitMQ connection URL (required)
            KBM_LEDSAS_BLOB_CONN_STRING: Azure Blob connection string (required)
            KBM_LEDSAS_CONTAINER: Informational (default: ``dev``): logged
                at startup as ``default_container``; every upload and
                download names its own container
            KBM_LEDSAS_PREFETCH: Message prefetch count (default: 10; 1 to 1000)
            KBM_LEDSAS_CONCURRENCY: Max concurrent handlers (default: 4; 1 to 100)
            KBM_LEDSAS_LOG_LEVEL: Log level (default: INFO)
            KBM_LEDSAS_LOG_FORMAT: Log format — json or text (default: json)
            KBM_LEDSAS_HANDLER_TIMEOUT: Handler timeout in seconds, 0 to disable (default: 1800; at most 86400)
            KBM_LEDSAS_MAX_RETRIES: Max retry attempts (default: 3; 0 to 100)
            KBM_LEDSAS_GENERIC_ERRORS: Generic-error fallback (default: false)
            KBM_LEDSAS_HEALTH_PORT: HTTP port for /health and /ready (default: 8090; 0 disables)
            KBM_LEDSAS_HEALTH_HOST: Bind address for the health server (default: 127.0.0.1)
            KBM_LEDSAS_HEALTH_VERBOSE: If truthy, ``/health`` and ``/ready``
                responses include SDK version + service name + per-check
                status (fingerprintable; opt-in for development). Default
                ``false`` — minimal ``{"status": ...}`` body.
            KBM_LEDSAS_MAX_PAYLOAD_BYTES: Reject inbound AMQP messages
                whose body exceeds this size (default 16777216, 16 MiB;
                max 268435456, 256 MiB; ``0`` disables).
            KBM_LEDSAS_ALLOW_INSECURE_AMQP: If truthy, downgrade the SDK's
                refusal of cleartext ``amqp://`` to non-loopback hosts to
                a WARNING. Escape hatch for TLS-terminated-upstream
                deployments. Default ``0``.
            KBM_LEDSAS_DEBUG: If truthy, dump the full Python traceback
                on configuration-error startup failures (default ``0``
                shows just the actionable one-liner).
            KBM_LEDSAS_CONNECT_TIMEOUT: Seconds one connection attempt to
                the message broker may take (default 15, 1..300).
            KBM_LEDSAS_ALLOWED_CONTAINERS: Comma-separated container names
                handler code may use through ``ctx.blob`` (every upload and
                download, by name or by URI); any other container is
                refused before a storage call (``ContainerNotAllowed``, not
                retried). Unset or blank: no restriction.
            KBM_LEDSAS_HEARTBEAT: AMQP heartbeat interval in seconds
                (default 60, 0..600, 0 = off) used unless the broker URL
                sets ``?heartbeat=`` (checked the same way); a broker that
                stops answering is detected within about 3 x (this
                interval + 1) s. A handler that blocks the event loop for
                longer than about two heartbeat intervals loses the broker
                connection: run blocking work with ``asyncio.to_thread``.

        Every value is read the same way, however it was set: surrounding
        whitespace and one matched pair of surrounding quotes are removed
        (``docker run --env-file`` keeps quotes that a shell ``source``
        removes).

        Boolean variables (GENERIC_ERRORS, HEALTH_VERBOSE,
        ALLOW_INSECURE_AMQP, DEBUG) accept ``1``, ``true``, ``yes``,
        ``on``, ``t``, ``y`` (true) and ``0``, ``false``, ``no``, ``off``,
        ``f``, ``n`` (false), in any letter case; unset or empty is false.
        Any other value is a configuration error naming the variable.

        A value outside its range is a configuration error too; the
        message names the variable and the value it holds (for example
        ``KBM_LEDSAS_PREFETCH=5000: Input should be less than or equal to
        1000``; see :func:`describe_config_errors`).

        Example:
            # Azurite's development account and well-known public key:
            # not a secret, it only works against the local emulator.
            export KBM_LEDSAS_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
            export KBM_LEDSAS_BLOB_CONN_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
            config = SDKConfig.from_env("my_service")
        """
        tenant_value = _env("KBM_LEDSAS_TENANT", tenant or "")
        tenant_final = tenant_value if tenant_value else None

        _warn_inline_comments()

        # KBM_LEDSAS_DEBUG is read when a startup error is reported; an
        # unrecognized value is refused here, like every other setting.
        parse_bool_env("KBM_LEDSAS_DEBUG")

        # Connection settings. Surrounding quotes are stripped so the
        # ``docker run --env-file`` path (which does no quote stripping) and
        # the shell ``source .env`` path (which requires quotes around the
        # URL) both yield the same parsed value — and so the cleartext-AMQP
        # guard in the model validator can't be bypassed by a quoted scheme.
        rabbitmq_url = _env("KBM_LEDSAS_RABBITMQ_URL")
        blob_conn_string = _env("KBM_LEDSAS_BLOB_CONN_STRING")

        return cls(
            # An empty or blank KBM_LEDSAS_SERVICE_NAME (a placeholder line
            # in a .env file, `docker run -e KBM_LEDSAS_SERVICE_NAME=`) is
            # read as unset, like an empty KBM_LEDSAS_TENANT: the
            # constructor's name applies.
            service_name=_env("KBM_LEDSAS_SERVICE_NAME") or service_name,
            tenant=tenant_final,
            rabbitmq_url=rabbitmq_url,
            blob_conn_string=blob_conn_string,
            blob_container=_env("KBM_LEDSAS_CONTAINER", "dev"),
            prefetch=_int_env("KBM_LEDSAS_PREFETCH", "10"),
            concurrency=_int_env("KBM_LEDSAS_CONCURRENCY", "4"),
            log_level=_env("KBM_LEDSAS_LOG_LEVEL", "INFO"),
            log_format=_env("KBM_LEDSAS_LOG_FORMAT", "json"),
            handler_timeout=_int_env("KBM_LEDSAS_HANDLER_TIMEOUT", "1800"),
            max_retries=_int_env("KBM_LEDSAS_MAX_RETRIES", "3"),
            # All four bool knobs use the same
            # ``_bool_env`` helper — see its docstring for the accepted
            # truthy values. Previously two parsers (``.lower()=="true"``
            # vs ``=="1"``) shipped in the same SDK.
            generic_errors=_bool_env("KBM_LEDSAS_GENERIC_ERRORS", "false"),
            max_payload_bytes=_int_env("KBM_LEDSAS_MAX_PAYLOAD_BYTES", str(16 * 1024 * 1024)),
            health_port=_int_env("KBM_LEDSAS_HEALTH_PORT", "8090"),
            health_host=_env("KBM_LEDSAS_HEALTH_HOST", "127.0.0.1"),
            health_verbose=_bool_env("KBM_LEDSAS_HEALTH_VERBOSE", "false"),
            connect_timeout=_int_env("KBM_LEDSAS_CONNECT_TIMEOUT", "15"),
            heartbeat=_int_env("KBM_LEDSAS_HEARTBEAT", str(DEFAULT_HEARTBEAT_S)),
            allowed_containers=parse_allowed_containers(_env("KBM_LEDSAS_ALLOWED_CONTAINERS")),
        )

    @field_serializer("rabbitmq_url", "blob_conn_string", when_used="always")
    def _redact_credentials(self, value: str | None) -> str | None:
        # str()/repr()/print/f-string are already covered by the
        # __repr__/__str__ overrides below, but ``model_dump()`` /
        # ``model_dump_json()`` are first-class pydantic methods a
        # developer is just as likely to reach for — redact there too.
        # Attribute access (config.rabbitmq_url) stays unredacted for the
        # transport builders.
        return None if value is None else "**redacted**"

    def __repr_args__(self) -> Any:
        """The field list pydantic's display hooks use (``__rich_repr__`` for
        rich.print and rich tracebacks, ``__pretty__`` for devtools,
        ``__repr_str__``): the credentials are redacted there too, as in
        ``repr()`` / ``str()`` / ``model_dump()``. Attribute access
        (``config.rabbitmq_url``), ``dict(config)``, ``vars(config)`` and
        pickling return the real values, as for any pydantic model."""
        for name, value in super().__repr_args__():
            if name in ("rabbitmq_url", "blob_conn_string") and value is not None:
                value = "**redacted**"
            yield name, value

    def __repr__(self) -> str:
        """Detailed representation for debugging (hide sensitive data)."""
        return (
            f"SDKConfig(service_name={self.service_name!r}, "
            f"tenant={self.tenant!r}, "
            f"concurrency={self.concurrency}, "
            f"log_level={self.log_level})"
        )

    # Pydantic v2's default ``__str__`` dumps every field — including
    # ``rabbitmq_url`` (user:password) and ``blob_conn_string``
    # (AccountKey=...). A developer who logs ``print(config)`` or
    # ``f"...{config}"`` while diagnosing a startup issue would otherwise
    # ship the broker password and Azure account key into their logs.
    # Route ``__str__`` through the credential-redacting ``__repr__`` so
    # str(), print(), and f-strings are all safe.
    __str__ = __repr__
