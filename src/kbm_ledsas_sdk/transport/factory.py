"""Transport factory.

Builds the transport (RabbitMQ + Azure Blob) used by the SDK.
"""

from __future__ import annotations

from ..runtime.config import SDKConfig
from .base import Transport


def create_transport(config: SDKConfig) -> Transport:
    """Create the transport instance for this config.

    Validates the required connection settings, then constructs the
    transport.

    Example:
        >>> config = SDKConfig.from_env(service_name="my_service")
        >>> transport = create_transport(config)
        >>> await transport.start()
    """
    if not config.rabbitmq_url:
        raise ValueError(
            "This deployment requires a RabbitMQ connection URL. "
            "Set KBM_LEDSAS_RABBITMQ_URL (e.g. "
            "amqp://guest:guest@127.0.0.1:5672/). "
            "See the README Configuration section."
        )
    if not config.blob_conn_string:
        raise ValueError(
            "This deployment requires an Azure Blob connection string. "
            "Set KBM_LEDSAS_BLOB_CONN_STRING. "
            "See the README Configuration section."
        )

    # Imported here so the RabbitMQ/Azure dependencies are only loaded when
    # a transport is actually built.
    from .direct import DirectTransport

    return DirectTransport(
        rabbitmq_url=config.rabbitmq_url,
        blob_conn_string=config.blob_conn_string,
        service_name=config.service_name,
        tenant=config.tenant,
        prefetch_count=config.prefetch,
        default_container=config.blob_container,
        max_payload_bytes=config.max_payload_bytes,
    )
