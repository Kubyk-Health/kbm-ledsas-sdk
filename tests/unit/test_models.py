"""
Unit tests for LEDSAS SDK models.

Tests include:
- Error type hierarchy and behavior
- BlobRef URI parsing and validation
- Envelope validation with all constraints
- Message type validation (Command, Response, Status, Error)
- Golden JSON validation from client_facing specs
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from kbm_ledsas_sdk.models import (
    BlobRef,
    Command,
    DeadlineExceeded,
    Envelope,
    Permanent,
    Response,
    Retryable,
    SDKError,
    Status,
)


class TestErrors:
    """Test error type hierarchy and behavior."""

    def test_sdk_error_base(self):
        """SDKError is the base for all SDK errors."""
        err = SDKError("test error")
        assert isinstance(err, Exception)
        assert str(err) == "test error"

    def test_retryable_error(self):
        """Retryable errors inherit from SDKError."""
        err = Retryable("network timeout")
        assert isinstance(err, SDKError)
        assert isinstance(err, Exception)
        assert str(err) == "network timeout"

    def test_permanent_error(self):
        """Permanent errors inherit from SDKError."""
        err = Permanent("bad input")
        assert isinstance(err, SDKError)
        assert str(err) == "bad input"

    def test_deadline_exceeded(self):
        """DeadlineExceeded errors inherit from SDKError."""
        err = DeadlineExceeded("handler timeout")
        assert isinstance(err, SDKError)
        assert str(err) == "handler timeout"


class TestBlobRef:
    """Test BlobRef model and URI parsing."""

    def test_valid_uri_simple(self):
        """Valid simple blob URI."""
        ref = BlobRef(uri="azblob://dev/input.json")
        assert ref.uri == "azblob://dev/input.json"
        assert ref.container == "dev"
        assert ref.path == "/input.json"
        assert ref.version_id is None

    def test_valid_uri_with_version(self):
        """Valid blob URI with version ID."""
        ref = BlobRef(uri="azblob://dev/input.json?versionId=abc123")
        assert ref.container == "dev"
        assert ref.path == "/input.json"
        assert ref.version_id == "abc123"

    def test_valid_uri_with_path(self):
        """Valid blob URI with nested path."""
        ref = BlobRef(uri="azblob://results/folder/subfolder/output.json")
        assert ref.container == "results"
        assert ref.path == "/folder/subfolder/output.json"
        assert ref.version_id is None

    def test_valid_uri_with_path_and_version(self):
        """Valid blob URI with nested path and version."""
        ref = BlobRef(uri="azblob://results/folder/output.json?versionId=def456")
        assert ref.container == "results"
        assert ref.path == "/folder/output.json"
        assert ref.version_id == "def456"

    def test_from_uri_classmethod(self):
        """BlobRef.from_uri() creates instance correctly."""
        ref = BlobRef.from_uri("azblob://dev/test.json?versionId=xyz")
        assert isinstance(ref, BlobRef)
        assert ref.container == "dev"
        assert ref.path == "/test.json"
        assert ref.version_id == "xyz"

    def test_invalid_scheme(self):
        """Invalid URI scheme raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BlobRef(uri="https://dev/input.json")
        assert "URI must start with azblob://" in str(exc_info.value)

    def test_missing_container(self):
        """URI without container raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BlobRef(uri="azblob:///input.json")
        assert "missing container name" in str(exc_info.value)

    def test_missing_path(self):
        """URI without path raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            BlobRef(uri="azblob://dev/")
        assert "missing blob path" in str(exc_info.value)

    def test_str_representation(self):
        """str(BlobRef) returns URI."""
        ref = BlobRef(uri="azblob://dev/test.json")
        assert str(ref) == "azblob://dev/test.json"

    def test_repr_representation(self):
        """repr(BlobRef) returns detailed representation."""
        ref = BlobRef(uri="azblob://dev/test.json")
        assert repr(ref) == "BlobRef(uri='azblob://dev/test.json')"


class TestEnvelope:
    """Test Envelope model and validation."""

    def get_valid_envelope_data(self) -> dict:
        """Return valid envelope data for testing."""
        return {
            "schema_version": "1.0",
            "type": "command",
            "name": "ProcessDataset",
            "message_version": "1.0",
            "message_id": "2f8da6a4-3f0c-4e2e-8a2a-9f7b8d1a7a11",
            "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
            "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
            "sent_at": "2025-01-15T12:34:56Z",
            "deadline": "2025-01-15T12:39:56Z",
            "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "reply_to": "resp.dev.kebormed_orchestrator.v1",
        }

    def test_valid_envelope(self):
        """Valid envelope passes validation."""
        data = self.get_valid_envelope_data()
        envelope = Envelope(**data)
        assert envelope.schema_version == "1.0"
        assert envelope.type == "command"
        assert envelope.name == "ProcessDataset"

    def test_envelope_with_priority(self):
        """Envelope with optional priority field."""
        data = self.get_valid_envelope_data()
        data["priority"] = 5
        envelope = Envelope(**data)
        assert envelope.priority == 5

    def test_envelope_without_deadline(self):
        """Envelope without optional deadline field."""
        data = self.get_valid_envelope_data()
        del data["deadline"]
        envelope = Envelope(**data)
        assert envelope.deadline is None

    def test_invalid_schema_version(self):
        """Invalid schema_version format raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["schema_version"] = "1.0.0"  # Should be X.Y not X.Y.Z
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_invalid_type(self):
        """Invalid type enum value raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["type"] = "invalid"
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_invalid_name_pattern(self):
        """Name not matching pattern raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["name"] = "process-dataset"  # Hyphens not allowed
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_invalid_name_too_long(self):
        """Name exceeding 64 chars raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["name"] = "A" * 65
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_invalid_message_id_format(self):
        """Non-UUID message_id raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["message_id"] = "not-a-uuid"
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_invalid_priority_range(self):
        """Priority outside 0-9 range raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["priority"] = 10
        with pytest.raises(ValidationError):
            Envelope(**data)

    def test_naive_datetime_rejected(self):
        """Naive datetime (no timezone) raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["sent_at"] = datetime(2025, 1, 15, 12, 34, 56)  # No timezone
        with pytest.raises(ValidationError) as exc_info:
            Envelope(**data)
        assert "timezone-aware" in str(exc_info.value)

    def test_deadline_before_sent_at(self):
        """Deadline before sent_at raises ValidationError."""
        data = self.get_valid_envelope_data()
        data["sent_at"] = "2025-01-15T12:34:56Z"
        data["deadline"] = "2025-01-15T12:30:00Z"  # Before sent_at
        with pytest.raises(ValidationError) as exc_info:
            Envelope(**data)
        assert "must be after sent_at" in str(exc_info.value)


class TestMessages:
    """Test message type models (Command, Response, Status, Error)."""

    def get_golden_command(self) -> dict:
        """Return golden Command JSON from client_facing spec."""
        return {
            "envelope": {
                "schema_version": "1.0",
                "type": "command",
                "name": "ProcessDataset",
                "message_version": "1.0",
                "message_id": "2f8da6a4-3f0c-4e2e-8a2a-9f7b8d1a7a11",
                "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "sent_at": "2025-01-15T12:34:56Z",
                "deadline": "2025-01-15T12:39:56Z",
                "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                "reply_to": "resp.dev.kebormed_orchestrator.v1",
            },
            "payload": {
                "dataset_uri": "azblob://dev/input/dataset-123.json?versionId=abc",
                "options": {"normalize": True},
            },
        }

    def get_golden_response_success(self) -> dict:
        """Return golden Response (success) JSON from client_facing spec."""
        return {
            "envelope": {
                "schema_version": "1.0",
                "type": "response",
                "name": "ProcessDataset",
                "message_version": "1.0",
                "message_id": "1f2a0c73-f0a5-4c06-92e3-7df30a0e8c1d",
                "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "sent_at": "2025-01-15T12:35:41Z",
                "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-11f067aa0ba902b7-01",
                "reply_to": "resp.dev.kebormed_orchestrator.v1",
            },
            "payload": {"result_uri": "azblob://dev/results/report-123.json?versionId=def"},
        }

    def get_golden_response_failure(self) -> dict:
        """Return golden Response (failure) JSON from client_facing spec."""
        return {
            "envelope": {
                "schema_version": "1.0",
                "type": "response",
                "name": "ProcessDataset",
                "message_version": "1.0",
                "message_id": "6b40c1a6-4c98-4595-9c41-cbfde5d7c5b3",
                "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "sent_at": "2025-01-15T12:35:41Z",
                "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-22f067aa0ba902b7-01",
                "reply_to": "resp.dev.kebormed_orchestrator.v1",
            },
            "payload": {
                "error": {
                    "code": "BadInput",
                    "message": "Input blob is missing or empty",
                    "retryable": False,
                }
            },
        }

    def get_golden_status(self) -> dict:
        """Return golden Status JSON from client_facing spec."""
        return {
            "envelope": {
                "schema_version": "1.0",
                "type": "status",
                "name": "ProcessDataset",
                "message_version": "1.0",
                "message_id": "ff5df37d-1a6e-4c53-b3b9-4a2a335a2e55",
                "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "sent_at": "2025-01-15T12:35:10Z",
                "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-33f067aa0ba902b7-01",
                "reply_to": "resp.dev.kebormed_orchestrator.v1",
            },
            "payload": {"stage": "downloading", "progress": 0.25},
        }

    def test_golden_command(self):
        """Golden Command JSON parses correctly."""
        data = self.get_golden_command()
        cmd = Command(**data)
        assert cmd.envelope.type == "command"
        assert cmd.envelope.name == "ProcessDataset"
        assert cmd.payload["dataset_uri"].startswith("azblob://")

    def test_golden_response_success(self):
        """Golden Response (success) JSON parses correctly."""
        data = self.get_golden_response_success()
        resp = Response(**data)
        assert resp.envelope.type == "response"
        assert "result_uri" in resp.payload
        assert "error" not in resp.payload

    def test_golden_response_failure(self):
        """Golden Response (failure) JSON parses correctly."""
        data = self.get_golden_response_failure()
        resp = Response(**data)
        assert resp.envelope.type == "response"
        assert "error" in resp.payload
        assert resp.payload["error"]["retryable"] is False

    def test_golden_status(self):
        """Golden Status JSON parses correctly."""
        data = self.get_golden_status()
        status = Status(**data)
        assert status.envelope.type == "status"
        assert status.payload["stage"] == "downloading"
        assert status.payload["progress"] == 0.25

    def test_command_repr(self):
        """Command repr is informative."""
        cmd = Command(**self.get_golden_command())
        repr_str = repr(cmd)
        assert "Command" in repr_str
        assert "ProcessDataset" in repr_str

    def test_response_repr_success(self):
        """Response repr shows has_error=False for success."""
        resp = Response(**self.get_golden_response_success())
        repr_str = repr(resp)
        assert "Response" in repr_str
        assert "has_error=False" in repr_str

    def test_response_repr_failure(self):
        """Response repr shows has_error=True for failure."""
        resp = Response(**self.get_golden_response_failure())
        repr_str = repr(resp)
        assert "Response" in repr_str
        assert "has_error=True" in repr_str

    def test_status_repr(self):
        """Status repr shows stage and progress."""
        status = Status(**self.get_golden_status())
        repr_str = repr(status)
        assert "Status" in repr_str
        assert "downloading" in repr_str
        assert "0.25" in repr_str


class TestJSONSerialization:
    """Test JSON serialization/deserialization round-trips."""

    def test_command_roundtrip(self):
        """Command serializes and deserializes correctly."""
        original_data = {
            "envelope": {
                "schema_version": "1.0",
                "type": "command",
                "name": "ProcessDataset",
                "message_version": "1.0",
                "message_id": "2f8da6a4-3f0c-4e2e-8a2a-9f7b8d1a7a11",
                "correlation_id": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "idempotency_key": "a6f6d1e7-6c8a-4f6b-9c4b-4c2f8dbf1d99",
                "sent_at": "2025-01-15T12:34:56+00:00",
                "trace_id": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                "reply_to": "resp.dev.kebormed_orchestrator.v1",
            },
            "payload": {"test": "data"},
        }

        # Parse
        cmd = Command(**original_data)

        # Serialize
        serialized = cmd.model_dump(mode="json")

        # Deserialize again
        cmd2 = Command(**serialized)

        assert cmd2.envelope.name == cmd.envelope.name
        assert cmd2.envelope.message_id == cmd.envelope.message_id
        assert cmd2.payload == cmd.payload
