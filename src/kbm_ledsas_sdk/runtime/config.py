"""
SDK configuration from environment variables.

Loads connection, performance, logging, error-handling, and health-endpoint
settings from ``KBM_LEDSAS_*`` environment variables. See
:meth:`SDKConfig.from_env` for the full variable list.
"""

import logging
import os

from pydantic import (
    BaseModel,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from kbm_ledsas_sdk.runtime.security import (
    _is_truthy,
    check_topology_name_budget,
    check_transport_security,
)

_logger = logging.getLogger(__name__)

# Retired dual-mode env vars. The SDK ships a single direct-mode
# transport, so these are accepted but ignored; from_env warns once if
# any is present.
_LEGACY_MODE_VARS = ("KBM_LEDSAS_MODE", "KBM_LEDSAS_DEV", "KBM_LEDSAS_INTERNAL")
_legacy_mode_warned = False


def _warn_legacy_mode_vars_once() -> None:
    """Warn once per process if any retired mode-selection env var is set.

    These variables previously selected a transport at runtime. The SDK
    now always runs its built-in transport, so they are accepted but
    ignored — nothing breaks for existing deployments. A single
    warning lists whichever are present so operators can drop them.
    """
    global _legacy_mode_warned
    if _legacy_mode_warned:
        return
    present = [v for v in _LEGACY_MODE_VARS if os.getenv(v) not in (None, "")]
    if present:
        _legacy_mode_warned = True
        _logger.warning(
            "%s no longer used: this package always runs "
            "its built-in transport. Remove the variable(s) from your "
            "environment.",
            " / ".join(present),
        )


def _int_env(name: str, default: str) -> int:
    """Parse an integer env var, naming the variable on parse failure.

    ``int(os.getenv(name, default))`` raises ValueError with only
    ``invalid literal for int() with base 10: 'abc'`` — a customer with
    multiple env vars set can't tell which one was misconfigured. This
    helper names the offending variable in the error so the startup
    message points at the actual knob.
    """
    raw = os.getenv(name, default)
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name}={raw!r} is not a valid integer") from None


def _bool_env(name: str, default: str = "false") -> bool:
    """Parse a boolean env var with consistent, lenient semantics.

    The SDK previously had two boolean-parsing
    conventions. Feature flags (GENERIC_ERRORS, HEALTH_VERBOSE) accepted
    ``true`` only; security flags (ALLOW_INSECURE_AMQP, DEBUG) accepted
    ``1`` only. A customer setting ``KBM_LEDSAS_ALLOW_INSECURE_AMQP=true``
    (a reasonable guess from the GENERIC_ERRORS pattern) silently
    failed closed without any signal that the value wasn't honored.

    Standardize on a single permissive parser: ``1``, ``true``, ``yes``,
    ``on``, ``t``, ``y`` (case-insensitive, whitespace-stripped) are
    truthy. Everything else — including the empty string, ``0``,
    ``false``, ``no``, ``off``, and any unrecognized token — is falsy.
    Security-sensitive knobs (ALLOW_INSECURE_AMQP, DEBUG) keep their
    fail-closed semantics: unknown tokens are still treated as off.
    """
    return _is_truthy(os.getenv(name, default))


def _strip_surrounding_quotes(value: str | None) -> str | None:
    """Strip a single matched pair of surrounding quotes from a value.

    ``docker run --env-file`` (unlike compose's dotenv parser and the
    shell ``set -a; source .env`` path) performs NO quote stripping, so a
    ``.env`` line written ``KBM_LEDSAS_RABBITMQ_URL="amqp://..."`` — the
    quoting the shell-source path requires — reaches the process with the
    literal double quotes still attached. Left intact, the quoted scheme
    no longer parses as ``amqp://`` (yarl yields ``scheme=''``), which both
    breaks the connection with a confusing late error AND silently bypasses
    the cleartext-AMQP refusal in :func:`check_transport_security`. We
    normalize by removing one matched pair of surrounding ``"`` or ``'``
    so both env-delivery styles behave identically. Only a symmetric pair
    is stripped, so a credential that legitimately contains a quote on one
    side is left untouched.
    """
    if value is None:
        return None
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


class SDKConfig(BaseModel):
    """
    SDK configuration loaded from environment variables.

    Connects to RabbitMQ and Azure Blob Storage. Configuration is loaded
    from environment variables; see :meth:`from_env` for the full variable
    list.
    """

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
    blob_container: str = Field("dev", description="Default blob container name")

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
        description="Bind address for the health server (default: loopback only)",
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

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is valid."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format is valid."""
        valid_formats = ["json", "text"]
        v_lower = v.lower()
        if v_lower not in valid_formats:
            raise ValueError(f"Invalid log format: {v}. Must be one of {valid_formats}")
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
        # Cleartext-AMQP check at config-load. Only fires when a URL
        # is set (i.e. direct mode); a None url is handled later by the
        # factory's "SDK is not configured" path.
        if self.rabbitmq_url:
            check_transport_security(self.rabbitmq_url)
        return self

    @classmethod
    def from_env(cls, service_name: str, tenant: str | None = None) -> "SDKConfig":
        """
        Create config from environment variables.

        Args:
            service_name: Service name. ``KBM_LEDSAS_SERVICE_NAME`` env var
                takes precedence over this argument when set.
            tenant: Tenant name. ``KBM_LEDSAS_TENANT`` env var takes
                precedence over this argument when set.

        Environment Variables:
            KBM_LEDSAS_SERVICE_NAME: Service name (overrides arg)
            KBM_LEDSAS_TENANT: Tenant name (overrides arg)
            KBM_LEDSAS_RABBITMQ_URL: RabbitMQ connection URL (required)
            KBM_LEDSAS_BLOB_CONN_STRING: Azure Blob connection string (required)
            KBM_LEDSAS_CONTAINER: Default blob container (default: ``dev``)
            KBM_LEDSAS_PREFETCH: Message prefetch count (default: 10)
            KBM_LEDSAS_CONCURRENCY: Max concurrent handlers (default: 4)
            KBM_LEDSAS_LOG_LEVEL: Log level (default: INFO)
            KBM_LEDSAS_LOG_FORMAT: Log format — json or text (default: json)
            KBM_LEDSAS_HANDLER_TIMEOUT: Handler timeout in seconds, 0 to disable (default: 1800)
            KBM_LEDSAS_MAX_RETRIES: Max retry attempts (default: 3)
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

        Boolean variables (GENERIC_ERRORS, HEALTH_VERBOSE,
        ALLOW_INSECURE_AMQP, DEBUG) accept ``1``, ``true``, ``yes``,
        ``on``, ``t``, ``y`` (case-insensitive). Everything else —
        including ``0``, ``false``, ``no``, ``off``, and the empty
        string — is falsy. Security-sensitive flags (ALLOW_INSECURE_AMQP,
        DEBUG) fail closed on unrecognized tokens.

        Example:
            export KBM_LEDSAS_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/
            export KBM_LEDSAS_BLOB_CONN_STRING="..."
            config = SDKConfig.from_env("my_service")
        """
        tenant_value = os.getenv("KBM_LEDSAS_TENANT", tenant or "")
        tenant_final = tenant_value if tenant_value else None

        # The dual-mode env flags are retired: the SDK always runs its
        # built-in transport. Warn once if any leftover flag is set, then
        # ignore it — nothing breaks for existing deployments.
        _warn_legacy_mode_vars_once()

        # Read RABBITMQ_URL and BLOB_CONN_STRING into the in-memory config;
        # the direct SDK always needs them to reach RabbitMQ and Azure Blob.
        # Surrounding quotes are stripped so the
        # ``docker run --env-file`` path (which does no quote stripping) and
        # the shell ``source .env`` path (which requires quotes around the
        # URL) both yield the same parsed value — and so the cleartext-AMQP
        # guard in the model validator can't be bypassed by a quoted scheme.
        rabbitmq_url = _strip_surrounding_quotes(os.getenv("KBM_LEDSAS_RABBITMQ_URL"))
        blob_conn_string = _strip_surrounding_quotes(os.getenv("KBM_LEDSAS_BLOB_CONN_STRING"))

        return cls(
            service_name=os.getenv("KBM_LEDSAS_SERVICE_NAME", service_name),
            tenant=tenant_final,
            rabbitmq_url=rabbitmq_url,
            blob_conn_string=blob_conn_string,
            blob_container=os.getenv("KBM_LEDSAS_CONTAINER", "dev"),
            prefetch=_int_env("KBM_LEDSAS_PREFETCH", "10"),
            concurrency=_int_env("KBM_LEDSAS_CONCURRENCY", "4"),
            log_level=os.getenv("KBM_LEDSAS_LOG_LEVEL", "INFO"),
            log_format=os.getenv("KBM_LEDSAS_LOG_FORMAT", "json"),
            handler_timeout=_int_env("KBM_LEDSAS_HANDLER_TIMEOUT", "1800"),
            max_retries=_int_env("KBM_LEDSAS_MAX_RETRIES", "3"),
            # All four bool knobs use the same
            # ``_bool_env`` helper — see its docstring for the accepted
            # truthy values. Previously two parsers (``.lower()=="true"``
            # vs ``=="1"``) shipped in the same SDK.
            generic_errors=_bool_env("KBM_LEDSAS_GENERIC_ERRORS", "false"),
            max_payload_bytes=_int_env("KBM_LEDSAS_MAX_PAYLOAD_BYTES", str(16 * 1024 * 1024)),
            health_port=_int_env("KBM_LEDSAS_HEALTH_PORT", "8090"),
            health_host=os.getenv("KBM_LEDSAS_HEALTH_HOST", "127.0.0.1"),
            health_verbose=_bool_env("KBM_LEDSAS_HEALTH_VERBOSE", "false"),
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
