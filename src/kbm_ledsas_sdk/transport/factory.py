"""
Transport factory — builds the transport this package ships.

The transport's builder is imported lazily, so its dependencies are only
loaded when a service actually starts.
"""

from ..runtime.config import SDKConfig
from .base import Transport


def create_transport(config: SDKConfig) -> Transport:
    """
    Create the transport instance this package ships.

    Example:
        >>> config = SDKConfig.from_env(service_name="my_service")
        >>> transport = create_transport(config)
        >>> await transport.start()
    """
    from ._build_direct import build_transport

    return build_transport(config)
