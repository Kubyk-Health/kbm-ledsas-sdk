"""Construct the direct transport (RabbitMQ + Azure Blob Storage).

Imported lazily by :mod:`transport.factory` so the RabbitMQ/Azure
dependencies are only loaded when a transport is actually built.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import Transport
from .direct import DirectTransport

if TYPE_CHECKING:
    from ..runtime.config import SDKConfig


def build_transport(config: SDKConfig) -> Transport:
    """Construct DirectTransport, validating required connection env vars."""
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

    return DirectTransport(
        rabbitmq_url=config.rabbitmq_url,
        blob_conn_string=config.blob_conn_string,
        service_name=config.service_name,
        tenant=config.tenant,
        prefetch_count=config.prefetch,
        default_container=config.blob_container,
        max_payload_bytes=config.max_payload_bytes,
    )
