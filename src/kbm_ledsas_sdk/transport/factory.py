"""Transport factory.

Builds the direct-mode transport (RabbitMQ + Azure Blob) used by the SDK.
"""

from ..runtime.config import SDKConfig
from .base import Transport


def create_transport(config: SDKConfig) -> Transport:
    """Create the direct-mode transport instance for this config.

    Example:
        >>> config = SDKConfig.from_env(service_name="my_service")
        >>> transport = create_transport(config)
        >>> await transport.start()
    """
    from ._build_direct import build_transport

    return build_transport(config)
