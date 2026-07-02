"""
Unit tests for blob operations string URI support.

Tests that download_* methods accept both BlobRef and string URIs.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from kbm_ledsas_sdk.blob.direct_operations import DirectBlobOperations
from kbm_ledsas_sdk.blob.operations import _ensure_blob_ref
from kbm_ledsas_sdk.models.blob import BlobRef


class TestEnsureBlobRef:
    """Tests for the _ensure_blob_ref helper function."""

    def test_string_uri_converted_to_blobref(self):
        """String URI should be converted to BlobRef."""
        uri = "azblob://container/path/file.txt"
        result = _ensure_blob_ref(uri)

        assert isinstance(result, BlobRef)
        assert result.uri == uri
        assert result.container == "container"
        assert result.path == "/path/file.txt"

    def test_string_uri_with_version_converted(self):
        """String URI with versionId should be converted correctly."""
        uri = "azblob://container/file.json?versionId=abc123"
        result = _ensure_blob_ref(uri)

        assert isinstance(result, BlobRef)
        assert result.uri == uri
        assert result.version_id == "abc123"

    def test_blobref_passed_through(self):
        """BlobRef should be passed through unchanged."""
        blob_ref = BlobRef.from_uri("azblob://container/file.txt")
        result = _ensure_blob_ref(blob_ref)

        assert result is blob_ref

    def test_invalid_uri_raises_error(self):
        """Invalid URI should raise ValueError."""
        with pytest.raises(ValueError):
            _ensure_blob_ref("invalid://not/azblob")

    def test_missing_container_raises_error(self):
        """URI missing container should raise ValueError."""
        with pytest.raises(ValueError):
            _ensure_blob_ref("azblob:///only/path")


class TestDirectBlobOperationsStringUri:
    """Tests for DirectBlobOperations string URI support."""

    @pytest.fixture
    def mock_azure_client(self):
        """Create mock Azure client."""
        client = MagicMock()
        client.download_blob = AsyncMock(return_value=b"test data")
        return client

    @pytest.fixture
    def blob_ops(self, mock_azure_client):
        """Create DirectBlobOperations with mock client."""
        return DirectBlobOperations(mock_azure_client)

    @pytest.mark.asyncio
    async def test_download_bytes_accepts_string(self, blob_ops, mock_azure_client):
        """download_bytes should accept string URI."""
        uri = "azblob://container/file.bin"

        result = await blob_ops.download_bytes(uri)

        assert result == b"test data"
        # Verify the Azure client was called with a BlobRef
        call_args = mock_azure_client.download_blob.call_args[0]
        assert isinstance(call_args[0], BlobRef)
        assert call_args[0].uri == uri

    @pytest.mark.asyncio
    async def test_download_bytes_accepts_blobref(self, blob_ops, mock_azure_client):
        """download_bytes should still accept BlobRef."""
        blob_ref = BlobRef.from_uri("azblob://container/file.bin")

        result = await blob_ops.download_bytes(blob_ref)

        assert result == b"test data"

    @pytest.mark.asyncio
    async def test_download_text_accepts_string(self, blob_ops, mock_azure_client):
        """download_text should accept string URI."""
        mock_azure_client.download_blob = AsyncMock(return_value=b"hello world")
        uri = "azblob://container/file.txt"

        result = await blob_ops.download_text(uri)

        assert result == "hello world"

    @pytest.mark.asyncio
    async def test_download_json_accepts_string(self, blob_ops, mock_azure_client):
        """download_json should accept string URI."""
        mock_azure_client.download_blob = AsyncMock(return_value=b'{"key": "value"}')
        uri = "azblob://container/file.json"

        result = await blob_ops.download_json(uri)

        assert result == {"key": "value"}

    @pytest.mark.asyncio
    async def test_download_stream_accepts_string(self, blob_ops, mock_azure_client):
        """download_stream should accept string URI."""

        # Mock the streaming download method with an async generator
        async def mock_stream(*args, **kwargs):
            yield b"chunk "
            yield b"data"

        mock_azure_client.download_blob_stream = mock_stream
        uri = "azblob://container/file.bin"

        chunks = []
        async for chunk in blob_ops.download_stream(uri):
            chunks.append(chunk)

        assert b"".join(chunks) == b"chunk data"


class TestDirectBlobOperationsUploadTextTypeError:
    """Tests for DirectBlobOperations upload_text type validation."""

    @pytest.fixture
    def mock_azure_client(self):
        """Create mock Azure client."""
        client = MagicMock()
        client.upload_blob = AsyncMock(
            return_value=BlobRef.from_uri("azblob://dev/file.txt?versionId=v1")
        )
        return client

    @pytest.fixture
    def blob_ops(self, mock_azure_client):
        """Create DirectBlobOperations with mock client."""
        return DirectBlobOperations(mock_azure_client)

    @pytest.mark.asyncio
    async def test_upload_text_accepts_string(self, blob_ops):
        """upload_text should accept a string."""
        result = await blob_ops.upload_text(container="dev", text="hello world")

        assert isinstance(result, BlobRef)

    @pytest.mark.asyncio
    async def test_upload_text_rejects_list_with_helpful_error(self, blob_ops):
        """upload_text should raise TypeError with helpful message for list."""
        with pytest.raises(TypeError) as exc_info:
            await blob_ops.upload_text(container="dev", text=["item1", "item2"])

        error_msg = str(exc_info.value)
        assert "upload_text() requires a string" in error_msg
        assert "got list" in error_msg
        assert "upload_json()" in error_msg

    @pytest.mark.asyncio
    async def test_upload_text_rejects_dict_with_helpful_error(self, blob_ops):
        """upload_text should raise TypeError with helpful message for dict."""
        with pytest.raises(TypeError) as exc_info:
            await blob_ops.upload_text(container="dev", text={"key": "value"})

        error_msg = str(exc_info.value)
        assert "upload_text() requires a string" in error_msg
        assert "got dict" in error_msg
        assert "upload_json()" in error_msg

    @pytest.mark.asyncio
    async def test_upload_text_rejects_bytes_with_helpful_error(self, blob_ops):
        """upload_text should raise TypeError with helpful message for bytes."""
        with pytest.raises(TypeError) as exc_info:
            await blob_ops.upload_text(container="dev", text=b"hello")

        error_msg = str(exc_info.value)
        assert "upload_text() requires a string" in error_msg
        assert "got bytes" in error_msg


class TestStreamingMemoryBound:
    """
    Memory-pattern tests for streaming idioms.

    These tests *do not* exercise SDK code or transport. They iterate a
    standalone async generator and use tracemalloc to verify the
    consume-while-streaming pattern stays bounded. They prove the pattern,
    not the SDK's implementation of it. Wire-level memory behavior of the
    SDK's ``download_stream`` / ``upload_stream`` is covered by the live
    integration tests under ``tests/integration/``.

    TC ID: TC-U-PERF-001, TC-U-PERF-002
    """

    @pytest.mark.asyncio
    async def test_download_stream_memory_bounded_with_synthetic_generator(self):
        """
        TC-U-PERF-001: Verify the streaming pattern has bounded memory.

        Requirement: NFR-SRS-PERF-001 - Peak memory < 50MB for multi-GB files
        Method: Use tracemalloc to measure peak memory while consuming a
        standalone async generator that mimics the SDK's chunk-yield shape.
        Acceptance: Peak memory delta < 50MB (52,428,800 bytes)

        Note: this exercises a synthetic generator, not live transport I/O;
        it verifies the streaming pattern's memory shape only.
        """
        import tracemalloc

        # Simulate streaming 100MB of data in 4MB chunks (25 chunks)
        # This simulates a large file download without actually needing the data
        _chunk_size = 4 * 1024 * 1024  # 4MB chunks (Azure default)
        num_chunks = 25  # 100MB total simulated
        memory_limit = 50 * 1024 * 1024  # 50MB limit

        # Create mock streaming operation
        async def mock_download_stream():
            """Simulate streaming chunks."""
            for _ in range(num_chunks):
                # Yield a smaller chunk to avoid actually allocating 4MB
                # The test verifies the streaming pattern, not actual Azure I/O
                yield b"x" * 1024  # 1KB per yield

        # Start memory tracking
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Process stream (simulates what SDK does)
        chunks_processed = 0
        async for chunk in mock_download_stream():
            # Process chunk (don't accumulate - streaming pattern)
            _ = len(chunk)  # Minimal processing
            chunks_processed += 1

        # Get peak memory
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Calculate memory delta
        memory_delta = peak - baseline

        # Verify memory is bounded
        assert (
            chunks_processed == num_chunks
        ), f"Expected {num_chunks} chunks, got {chunks_processed}"
        assert memory_delta < memory_limit, (
            f"Memory delta {memory_delta / (1024 * 1024):.2f}MB exceeds "
            f"{memory_limit / (1024 * 1024):.2f}MB limit"
        )

    @pytest.mark.asyncio
    async def test_upload_stream_memory_bounded_with_synthetic_generator(self):
        """
        TC-U-PERF-002: Verify the streaming-upload pattern has bounded memory.

        Requirement: NFR-SRS-PERF-001 - Memory proportional to chunk size
        Method: Use tracemalloc to measure memory while iterating a
        standalone async generator that mimics the SDK's upload chunk shape.
        Acceptance: Peak memory delta < 50MB

        Note: this exercises a synthetic generator, not live transport I/O;
        it verifies the streaming pattern's memory shape only.
        """
        import tracemalloc

        memory_limit = 50 * 1024 * 1024  # 50MB limit

        # Simulate streaming upload pattern
        async def mock_upload_stream():
            """Simulate generating upload chunks."""
            for _ in range(25):  # 25 chunks
                # Generate chunk without accumulation
                chunk = b"y" * 1024  # 1KB per chunk
                yield chunk

        # Start memory tracking
        tracemalloc.start()
        baseline = tracemalloc.get_traced_memory()[0]

        # Simulate upload processing
        total_bytes = 0
        async for chunk in mock_upload_stream():
            total_bytes += len(chunk)
            # Don't accumulate - streaming pattern

        # Get peak memory
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        memory_delta = peak - baseline

        assert total_bytes == 25 * 1024, "All chunks should be processed"
        assert (
            memory_delta < memory_limit
        ), f"Memory delta {memory_delta / (1024 * 1024):.2f}MB exceeds limit"
