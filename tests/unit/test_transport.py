"""
Unit tests for transport abstraction layer.

Tests include:
- Transport factory mode selection
- Mock transport functionality
- Mock blob operations
- Transport interface compliance
"""

from datetime import UTC, datetime

import pytest

from kbm_ledsas_sdk.models import BlobRef, Command, Envelope, Response, Status
from kbm_ledsas_sdk.runtime.config import SDKConfig
from kbm_ledsas_sdk.transport import (
    MockBlobOperations,
    MockTransport,
    Transport,
    create_transport,
)


class TestTransportFactory:
    """Direct-mode transport construction."""

    # Azurite-style connection string used for direct-wheel construction.
    _BLOB_CONN = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=xxx;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
    )

    def test_direct_wheel_builds_direct_transport(self):
        """create_transport builds DirectTransport for a direct config."""
        config = SDKConfig(
            service_name="test",
            rabbitmq_url="amqp://guest:guest@127.0.0.1:5672/",
            blob_conn_string=self._BLOB_CONN,
        )
        transport = create_transport(config)

        from kbm_ledsas_sdk.transport.direct import DirectTransport

        assert isinstance(transport, DirectTransport)

    def test_direct_wheel_missing_rabbitmq_url_raises(self):
        """Direct wheel without a broker URL fails with an actionable error."""
        config = SDKConfig(service_name="test")
        with pytest.raises(ValueError) as excinfo:
            create_transport(config)
        assert "KBM_LEDSAS_RABBITMQ_URL" in str(excinfo.value)

    def test_direct_wheel_missing_blob_conn_raises(self):
        """Direct wheel with a broker URL but no blob connection string fails."""
        config = SDKConfig(
            service_name="test",
            rabbitmq_url="amqp://guest:guest@127.0.0.1:5672/",
        )
        with pytest.raises(ValueError) as excinfo:
            create_transport(config)
        assert "KBM_LEDSAS_BLOB_CONN_STRING" in str(excinfo.value)


class TestMockBlobOperations:
    """Test mock blob operations for unit testing."""

    @pytest.mark.asyncio
    async def test_upload_download_bytes(self):
        """Upload and download bytes round-trip."""
        blob_ops = MockBlobOperations()

        # Upload
        data = b"hello world"
        ref = await blob_ops.upload_bytes(container="test", data=data)

        assert ref.container == "test"
        assert ref.version_id.startswith("mock-")
        assert blob_ops.upload_count == 1

        # Download
        downloaded = await blob_ops.download_bytes(ref)
        assert downloaded == data
        assert blob_ops.download_count == 1

    @pytest.mark.asyncio
    async def test_upload_download_json(self):
        """Upload and download JSON round-trip."""
        blob_ops = MockBlobOperations()

        # Upload
        obj = {"status": "ok", "count": 42}
        ref = await blob_ops.upload_json(container="results", obj=obj)

        # Download
        downloaded = await blob_ops.download_json(ref)
        assert downloaded == obj

    @pytest.mark.asyncio
    async def test_download_nonexistent_blob(self):
        """Download nonexistent blob raises FileNotFoundError."""
        blob_ops = MockBlobOperations()

        ref = BlobRef(uri="azblob://test/missing.bin")
        with pytest.raises(FileNotFoundError) as exc_info:
            await blob_ops.download_bytes(ref)
        assert "Blob not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_add_blob_for_testing(self):
        """Pre-populate blobs for download tests."""
        blob_ops = MockBlobOperations()

        # Pre-populate
        uri = "azblob://test/input.json"
        blob_ops.add_blob(uri, b'{"test": true}')

        # Download
        ref = BlobRef(uri=uri)
        data = await blob_ops.download_json(ref)
        assert data == {"test": True}

    @pytest.mark.asyncio
    async def test_upload_with_custom_path(self):
        """Upload with custom path."""
        blob_ops = MockBlobOperations()

        ref = await blob_ops.upload_bytes(container="test", data=b"data", path="custom/path.bin")

        assert "custom/path.bin" in ref.uri


class TestMockTransport:
    """Test mock transport for unit testing."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Transport can be started and stopped."""
        transport = MockTransport()

        assert not transport.started
        assert not transport.stopped

        await transport.start()
        assert transport.started

        await transport.stop()
        assert transport.stopped

    @pytest.mark.asyncio
    async def test_subscribe_delivers_commands(self):
        """subscribe() delivers commands from queue."""
        transport = MockTransport()

        # Add commands
        cmd1 = Command(
            envelope=Envelope(
                schema_version="1.0",
                type="command",
                name="Test1",
                message_version="1.0",
                message_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                correlation_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                idempotency_key="cccccccc-cccc-cccc-cccc-cccccccccccc",
                sent_at=datetime.now(UTC),
                trace_id="00-trace-id",
                reply_to="resp.test",
            ),
            payload={"data": 1},
        )
        cmd2 = Command(
            envelope=Envelope(
                schema_version="1.0",
                type="command",
                name="Test2",
                message_version="1.0",
                message_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                correlation_id="eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                idempotency_key="ffffffff-ffff-ffff-ffff-ffffffffffff",
                sent_at=datetime.now(UTC),
                trace_id="00-trace-id-2",
                reply_to="resp.test",
            ),
            payload={"data": 2},
        )

        transport.add_command(cmd1)
        transport.add_command(cmd2)

        await transport.start()

        # Consume commands
        commands_received = []
        async for cmd in transport.subscribe():
            commands_received.append(cmd)
            if len(commands_received) == 2:
                await transport.stop()
                break

        assert len(commands_received) == 2
        assert commands_received[0].envelope.name == "Test1"
        assert commands_received[1].envelope.name == "Test2"

    @pytest.mark.asyncio
    async def test_ack_tracking(self):
        """ack() records message acknowledgments."""
        transport = MockTransport()

        await transport.ack("msg-1")
        await transport.ack("msg-2")

        assert transport.ack_count == 2
        assert "msg-1" in transport.acked_messages
        assert "msg-2" in transport.acked_messages

    @pytest.mark.asyncio
    async def test_nack_tracking(self):
        """nack() records message negative acknowledgments."""
        transport = MockTransport()

        await transport.nack("msg-1", requeue=True)
        await transport.nack("msg-2", requeue=False)

        assert transport.nack_count == 2
        assert ("msg-1", True) in transport.nacked_messages
        assert ("msg-2", False) in transport.nacked_messages

    @pytest.mark.asyncio
    async def test_send_response_tracking(self):
        """send_response() records sent responses."""
        transport = MockTransport()

        response = Response(
            envelope=Envelope(
                schema_version="1.0",
                type="response",
                name="Test",
                message_version="1.0",
                message_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                correlation_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                idempotency_key="cccccccc-cccc-cccc-cccc-cccccccccccc",
                sent_at=datetime.now(UTC),
                trace_id="00-trace",
                reply_to="resp.test",
            ),
            payload={"result": "ok"},
        )

        await transport.send_response(response)

        assert transport.response_count == 1
        assert transport.sent_responses[0].payload["result"] == "ok"

    @pytest.mark.asyncio
    async def test_send_status_tracking(self):
        """send_status() records sent status updates."""
        transport = MockTransport()

        status = Status(
            envelope=Envelope(
                schema_version="1.0",
                type="status",
                name="Test",
                message_version="1.0",
                message_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                correlation_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                idempotency_key="cccccccc-cccc-cccc-cccc-cccccccccccc",
                sent_at=datetime.now(UTC),
                trace_id="00-trace",
                reply_to="resp.test",
            ),
            payload={"stage": "processing", "progress": 0.5},
        )

        await transport.send_status(status)

        assert transport.status_count == 1
        assert transport.sent_statuses[0].payload["stage"] == "processing"

    @pytest.mark.asyncio
    async def test_get_blob_operations(self):
        """get_blob_operations() returns mock blob ops."""
        transport = MockTransport()

        blob_ops = transport.get_blob_operations()
        assert isinstance(blob_ops, MockBlobOperations)

        # Can use blob ops
        ref = await blob_ops.upload_bytes(container="test", data=b"data")
        assert ref.container == "test"

    @pytest.mark.asyncio
    async def test_transport_interface_compliance(self):
        """MockTransport implements Transport interface."""
        transport = MockTransport()
        assert isinstance(transport, Transport)


class TestSDKConfig:
    """Test SDK configuration."""

    def test_from_env_defaults(self):
        """from_env() with defaults."""
        config = SDKConfig.from_env(service_name="test_service")

        assert config.service_name == "test_service"
        assert config.prefetch == 10
        assert config.concurrency == 4

    def test_explicit_config(self):
        """Explicit configuration."""
        config = SDKConfig(
            service_name="my_service",
            tenant="dev",
            prefetch=20,
            concurrency=8,
        )

        assert config.service_name == "my_service"
        assert config.tenant == "dev"
        assert config.prefetch == 20
        assert config.concurrency == 8
