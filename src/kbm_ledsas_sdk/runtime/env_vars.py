"""
Canonical registry of all SDK environment variables.

Single source of truth for environment variable documentation. All
customer-facing docs should be validated against this file to prevent drift.

Usage:
    from kbm_ledsas_sdk.runtime.env_vars import ENV_VAR_REGISTRY, get_env_var

    # Get spec for a variable
    spec = get_env_var("KBM_LEDSAS_LOG_LEVEL")
    print(spec.description)  # "Logging verbosity (DEBUG/INFO/WARNING/ERROR)"
    print(spec.default)      # "INFO"
"""

from dataclasses import dataclass


@dataclass
class EnvVarSpec:
    """
    Specification for an SDK environment variable.

    Defines the canonical behavior for each environment variable,
    including defaults, valid values, and deprecation status.
    """

    name: str
    """Environment variable name (e.g., KBM_LEDSAS_SERVICE_NAME)"""

    description: str
    """Human-readable description of what this variable does"""

    default: str | None
    """Default value if not set, or None if required/no default"""

    required: bool = False
    """Whether this variable must be set (True) or has a default (False)"""

    deprecated: bool = False
    """Whether this variable is deprecated and should not be used"""

    deprecated_by: str | None = None
    """If deprecated, what to use instead (e.g., 'KBM_LEDSAS_CONTAINER')"""

    valid_values: list[str] | None = None
    """List of valid values, or None if any string is valid"""


# Canonical registry of customer-facing SDK environment variables.
#
# The retired dual-mode flags (KBM_LEDSAS_MODE, KBM_LEDSAS_DEV) are
# intentionally absent. The SDK always runs its built-in direct transport;
# those vars are accepted but ignored (config.from_env logs a one-time
# warning when one is set). Deployment tooling that enumerates
# ENV_VAR_REGISTRY should not nudge operators toward a retired knob.
ENV_VAR_REGISTRY: list[EnvVarSpec] = [
    EnvVarSpec(
        name="KBM_LEDSAS_SERVICE_NAME",
        description=(
            "Service name for queue binding and logging. When set, "
            "takes precedence over the ServiceApp(service_name=...) "
            "constructor argument."
        ),
        default=None,
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_TENANT",
        description="Tenant identifier for multi-tenant deployments",
        default=None,
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_RABBITMQ_URL",
        description=(
            "RabbitMQ connection URL (AMQP protocol). Required to " "connect to the broker."
        ),
        default=None,
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_BLOB_CONN_STRING",
        description=(
            "Azure Blob Storage connection string. Required to " "connect to blob storage."
        ),
        default=None,
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_CONTAINER",
        description="Default blob container name",
        default="dev",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_PREFETCH",
        description="AMQP message prefetch count",
        default="10",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_CONCURRENCY",
        description="Maximum concurrent command handlers",
        default="4",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_LOG_LEVEL",
        description="Logging level",
        default="INFO",
        required=False,
        valid_values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_LOG_FORMAT",
        description="Log output format",
        default="json",
        required=False,
        valid_values=["json", "text"],
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_HANDLER_TIMEOUT",
        description="Handler execution timeout in seconds (0 to disable)",
        default="1800",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_MAX_RETRIES",
        description="Maximum retry attempts for retryable errors",
        default="3",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_GENERIC_ERRORS",
        description=(
            "Return generic error messages (hide details from caller). "
            "Boolean: accepts 1/true/yes/on/t/y as truthy "
            "(case-insensitive); everything else is falsy. "
            "Default false."
        ),
        default="false",
        required=False,
        # Four boolean knobs (GENERIC_ERRORS,
        # HEALTH_VERBOSE, ALLOW_INSECURE_AMQP, DEBUG) share one lenient
        # parser. ``valid_values=None`` advertises "any string is
        # accepted"; the parsing rules are documented in the
        # description above. Previously the registry advertised
        # ["true", "false"] for two and ["0", "1"] for the other two —
        # confused customers and disagreed with the actual parser.
        valid_values=None,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_HEALTH_PORT",
        description="HTTP port for /health and /ready endpoints (0 disables)",
        default="8090",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_HEALTH_HOST",
        description="Bind address for the health server (default: loopback)",
        default="127.0.0.1",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_HEALTH_VERBOSE",
        description=(
            "If truthy, health endpoints include SDK version, service "
            "name, and per-check status (fingerprintable; useful for "
            'development). Default false → minimal {"status": ...} '
            "body only. Boolean: accepts 1/true/yes/on/t/y as truthy "
            "(case-insensitive); everything else is falsy."
        ),
        default="false",
        required=False,
        valid_values=None,  # L2: lenient parsing — see GENERIC_ERRORS
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_MAX_PAYLOAD_BYTES",
        description=(
            "Reject inbound AMQP messages whose body exceeds this "
            "size (single WARNING + DLQ). 0 disables the check. "
            "Default 16777216 (16 MiB). Use blob storage for large "
            "payloads."
        ),
        default="16777216",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_ALLOW_INSECURE_AMQP",
        description=(
            "If truthy, downgrade the SDK's refusal of amqp:// "
            "(cleartext AMQP) to non-loopback hosts to a WARNING. "
            "Escape hatch for deployments where TLS is terminated by "
            "an upstream proxy (e.g. an in-cluster service mesh). "
            "Default 0 (refuse). Boolean: accepts 1/true/yes/on/t/y "
            "as truthy (case-insensitive); fails closed on unrecognized "
            "tokens — security-sensitive."
        ),
        default="0",
        required=False,
        valid_values=None,  # L2: lenient parsing — see GENERIC_ERRORS
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_DEBUG",
        description=(
            "If truthy, configuration-error startup failures dump the "
            "full Python traceback instead of just the actionable "
            "one-line message. Useful for diagnosing custom validators. "
            "Boolean: accepts 1/true/yes/on/t/y as truthy "
            "(case-insensitive); fails closed on unrecognized tokens."
        ),
        default="0",
        required=False,
        valid_values=None,  # L2: lenient parsing — see GENERIC_ERRORS
    ),
]


def get_env_var(name: str) -> EnvVarSpec | None:
    """
    Get environment variable specification by name.

    Args:
        name: Environment variable name (e.g., "KBM_LEDSAS_SERVICE_NAME")

    Returns:
        EnvVarSpec if found, None otherwise
    """
    for spec in ENV_VAR_REGISTRY:
        if spec.name == name:
            return spec
    return None


def get_all_var_names() -> list[str]:
    """Get all registered environment variable names."""
    return [spec.name for spec in ENV_VAR_REGISTRY]


def get_deprecated_vars() -> list[EnvVarSpec]:
    """Get all deprecated environment variables."""
    return [spec for spec in ENV_VAR_REGISTRY if spec.deprecated]
