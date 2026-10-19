"""
Message types exchanged between a caller and a service.

All messages follow the same structure:
- envelope: Fixed metadata wrapper
- payload: Message-specific data (dict[str, Any])

Message types:
- Command: caller → service (trigger processing)
- Response: service → caller (success or failure result; handler
  failures come back here with an ``error`` object in the payload)
- Status: service → caller (progress updates)
"""

from typing import Any, ClassVar

from pydantic import ConfigDict, Field, model_validator

from .envelope import Envelope, _ValidatedCopyModel


def _check_envelope_type(message: Any, expected: str) -> None:
    """Refuse a message whose envelope ``type`` is not ``expected`` (a
    Response built around a command's envelope, ...)."""
    actual = message.envelope.type
    if actual != expected:
        raise ValueError(
            f"envelope.type is {actual!r}, not {expected!r} (a "
            f"{type(message).__name__} carries a {expected} envelope)"
        )


class Command(_ValidatedCopyModel):
    """
    Command message from the caller to the service.

    Commands trigger processing in the service. The SDK delivers
    the command to the registered handler matching envelope.name and
    envelope.message_version.

    Example:
        {
          "envelope": {
            "type": "command",
            "name": "ProcessDataset",
            "message_version": "1.0",
            ...
          },
          "payload": {
            "dataset_uri": "azblob://dev/input.json",
            "options": {"normalize": true}
          }
        }
    """

    # Frozen (fields cannot be reassigned; the payload dict itself is
    # the handler's to read).
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ..., description="Command-specific payload (deserialized to handler's request model)"
    )

    @model_validator(mode="after")
    def _envelope_type_matches(self) -> "Command":
        _check_envelope_type(self, "command")
        return self

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"Command(name={self.envelope.name!r}, message_id={self.envelope.message_id!r})"


class Response(_ValidatedCopyModel):
    """
    Response message from the service to the caller.

    Responses indicate success or failure of command processing.
    - Success: payload contains result data (often a blob URI)
    - Failure: payload.error contains error details

    Success Example:
        {
          "envelope": {
            "type": "response",
            "name": "ProcessDataset",
            ...
          },
          "payload": {
            "result_uri": "azblob://dev/results/output.json?versionId=abc"
          }
        }

    Failure Example:
        {
          "envelope": {
            "type": "response",
            ...
          },
          "payload": {
            "error": {
              "code": "Permanent",
              "message": "Missing required field",
              "retryable": false
            }
          }
        }
    """

    # Frozen (fields cannot be reassigned; the payload dict itself is
    # the handler's to read).
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ..., description="Response payload (success result or error details)"
    )

    @model_validator(mode="after")
    def _envelope_type_matches(self) -> "Response":
        _check_envelope_type(self, "response")
        return self

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        has_error = "error" in self.payload
        return f"Response(name={self.envelope.name!r}, has_error={has_error}, message_id={self.envelope.message_id!r})"


class Status(_ValidatedCopyModel):
    """
    Status update message from the service to the caller.

    Status messages provide progress updates during long-running processing.
    They are informational and do not affect command/response correlation.

    Example:
        {
          "envelope": {
            "type": "status",
            "name": "ProcessDataset",
            ...
          },
          "payload": {
            "stage": "downloading",
            "progress": 0.25,
            "note": "Downloading input dataset (15 MB)"
          }
        }

    Status stages (typical values):
        - queued: Message received, waiting to start
        - initializing: Handler starting up
        - downloading: Downloading input data from Blob
        - processing: Executing business logic
        - uploading: Uploading results to Blob
        - finalizing: Cleanup and finalization
        - done: Processing complete
        - error: Error occurred
    """

    # Frozen (fields cannot be reassigned; the payload dict itself is
    # the handler's to read).
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ...,
        description="Status payload with stage (str), progress (float 0-1), and optional note (str)",
    )

    @model_validator(mode="after")
    def _envelope_type_matches(self) -> "Status":
        _check_envelope_type(self, "status")
        return self

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        stage = self.payload.get("stage", "unknown")
        progress = self.payload.get("progress", 0.0)
        return f"Status(name={self.envelope.name!r}, stage={stage!r}, progress={progress})"
