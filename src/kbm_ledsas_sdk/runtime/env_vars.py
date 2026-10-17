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

import difflib
import re
from collections.abc import Iterable, Mapping
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


# The rule of the four boolean settings (GENERIC_ERRORS, HEALTH_VERBOSE,
# ALLOW_INSECURE_AMQP, DEBUG), as runtime.security.parse_bool_env applies it.
BOOLEAN_RULE = (
    "Boolean: 1/true/yes/on/t/y are true and 0/false/no/off/f/n are false "
    "(any letter case); unset or empty gives the default; any other value "
    "(a typo such as 'ture') stops the start with a configuration error "
    "naming the variable."
)


# Canonical registry of customer-facing SDK environment variables.
#
# Retired variables are intentionally absent: since v0.3.0 they no longer
# configure anything and are accepted but ignored (config.from_env logs a
# one-time warning when one is set). Deployment tooling that enumerates
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
        description=(
            "Default blob container name. Informational: logged at "
            "startup; ctx.blob.upload_* always takes an explicit container."
        ),
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
            "Default false. " + BOOLEAN_RULE
        ),
        default="false",
        required=False,
        # Four boolean settings (GENERIC_ERRORS, HEALTH_VERBOSE,
        # ALLOW_INSECURE_AMQP, DEBUG) share one strict parser
        # (runtime.security.parse_bool_env), whose rule is BOOLEAN_RULE.
        # The accepted spellings are many (12 tokens in any letter case),
        # so they are given in the description rather than in
        # ``valid_values``.
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
        description=(
            "Bind address for the health server (default: loopback): an "
            "IP address or a host name, checked at startup"
        ),
        default="127.0.0.1",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_HEALTH_VERBOSE",
        description=(
            "If true, health endpoints include SDK version, service "
            "name, and per-check status (fingerprintable; useful for "
            'development). Default false → minimal {"status": ...} '
            "body only. " + BOOLEAN_RULE
        ),
        default="false",
        required=False,
        valid_values=None,  # boolean: accepted tokens are listed in the description
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_MAX_PAYLOAD_BYTES",
        description=(
            "Reject inbound AMQP messages whose body exceeds this "
            "size (single WARNING + DLQ), and refuse to send a reply "
            "larger than this (the command gets a Permanent error reply "
            "instead). 0 disables both checks. Default 16777216 (16 MiB, "
            "the broker's default maximum message size). Use blob "
            "storage for large payloads and results."
        ),
        default="16777216",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_ALLOW_INSECURE_AMQP",
        description=(
            "If true, downgrade the SDK's refusal of amqp:// "
            "(cleartext AMQP) to non-loopback hosts to a WARNING. "
            "Escape hatch for deployments where TLS is terminated by "
            "an upstream proxy (e.g. an in-cluster service mesh). "
            "Default 0 (refuse). " + BOOLEAN_RULE
        ),
        default="0",
        required=False,
        valid_values=None,  # boolean: accepted tokens are listed in the description
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_CONNECT_TIMEOUT",
        description=(
            "Seconds one connection attempt to the AMQP broker may take "
            "(1 to 300). A broker that does not answer then fails the "
            "startup within this time with one clean ERROR line. "
            "Default 15."
        ),
        default="15",
        required=False,
    ),
    EnvVarSpec(
        name="KBM_LEDSAS_DEBUG",
        description=(
            "If true, configuration-error startup failures dump the "
            "full Python traceback instead of just the actionable "
            "one-line message. Useful for diagnosing custom validators. "
            "Default 0. " + BOOLEAN_RULE
        ),
        default="0",
        required=False,
        valid_values=None,  # boolean: accepted tokens are listed in the description
    ),
]


def get_env_var(name: str) -> EnvVarSpec | None:
    """
    Get environment variable specification by name.

    Args:
        name: Environment variable name (e.g., "KBM_LEDSAS_PREFETCH")

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


# Prefix of the SDK's settings; a variable with it that is not one of them
# is most often a typo (KBM_LEDSAS_PREFECH), which would otherwise be
# ignored without a word while the default applies.
SETTINGS_PREFIX = "KBM_LEDSAS_"

# Kubernetes gives a container environment variables for each Service in
# its namespace (``enableServiceLinks``, on by default). For a Service
# named ``kbm-ledsas-<x>`` with port 8085 named ``http`` they are
# ``KBM_LEDSAS_<X>_SERVICE_HOST``, ``..._SERVICE_PORT``,
# ``..._SERVICE_PORT_HTTP``, ``..._PORT`` and ``..._PORT_8085_TCP`` with
# ``_PROTO`` / ``_PORT`` / ``_ADDR``. Nobody set them, so they are not
# reported. A group is recognised by its ``<NAME>_SERVICE_HOST`` variable,
# which Kubernetes always sets with the others, so a misspelled setting
# that merely ends in ``_PORT`` is still reported.
_SERVICE_HOST_SUFFIX = "_SERVICE_HOST"
_SERVICE_LINK_TAIL = re.compile(
    r"SERVICE_HOST|SERVICE_PORT(?:_[A-Z0-9_]+)?"
    r"|PORT(?:_[0-9]+_(?:TCP|UDP|SCTP)(?:_(?:PROTO|PORT|ADDR))?)?"
)


def _service_link_names(environ: "Iterable[str]") -> "set":
    """The names in ``environ`` that Kubernetes sets for a Service whose
    variables start with ``KBM_LEDSAS``: every name of the shape above
    after a prefix ``<NAME>_`` whose ``<NAME>_SERVICE_HOST`` is set."""
    names = list(environ)
    prefixes = {
        name[: -len(_SERVICE_HOST_SUFFIX)] + "_"
        for name in names
        if name.startswith(SETTINGS_PREFIX) and name.endswith(_SERVICE_HOST_SUFFIX)
    }
    return {
        name
        for name in names
        for prefix in prefixes
        if name.startswith(prefix) and _SERVICE_LINK_TAIL.fullmatch(name[len(prefix) :])
    }


def _retired_settings() -> "set":
    """The retired variables of this build (they configure nothing; their
    own WARNING asks to remove them), read where they are defined."""
    from . import config as config_module

    return set(getattr(config_module, "_LEGACY_MODE_VARS", ()))


def unknown_settings(
    environ: "Mapping[str, str]", known: "Iterable[str]"
) -> "list[tuple[str, str | None]]":
    """The variables in ``environ`` whose name starts with ``KBM_LEDSAS_``
    (in any letter case) but that are not in ``known``, sorted, each with
    the closest known name (or None): ``[("KBM_LEDSAS_PREFECH",
    "KBM_LEDSAS_PREFETCH")]``. A retired variable is never the closest
    name (it configures nothing, and its own WARNING asks to remove it):
    ``KBM_LEDSAS_TRANSPORT_MODE`` gets no suggestion rather than a retired
    one. The variables Kubernetes sets for a Service in the namespace
    (``KBM_LEDSAS_<NAME>_SERVICE_HOST`` and the rest of its group) are not
    settings anyone set and are left out. Values are never read."""
    known_names = sorted(set(known))
    retired = _retired_settings()
    candidates = [name for name in known_names if name not in retired]
    service_links = _service_link_names(environ)
    found = []
    for name in environ:
        if not name.upper().startswith(SETTINGS_PREFIX) or name in known_names:
            continue
        if name in service_links:
            continue
        close = difflib.get_close_matches(name.upper(), candidates, n=1, cutoff=0.75)
        found.append((name, close[0] if close else None))
    return sorted(found)


def describe_unknown_settings(found: "list[tuple[str, str | None]]") -> str:
    """``found`` (from :func:`unknown_settings`) as one text: each name
    (escaped if it holds anything but letters, digits and ``_``), with
    ``(did you mean <name>?)`` when a known name is close."""
    parts = []
    for name, close in found:
        shown = name if re.fullmatch(r"[A-Za-z0-9_]+", name) else ascii(name)
        parts.append(f"{shown} (did you mean {close}?)" if close else shown)
    return ", ".join(parts)
