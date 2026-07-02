"""
Message types for LEDSAS communication.

All messages follow the same structure:
- envelope: Fixed metadata wrapper
- payload: Message-specific data (dict[str, Any])

Message types:
- Command: Orchestrator → LEDSAS (trigger processing)
- Response: LEDSAS → Orchestrator (success or failure result; handler
  failures come back here with an ``error`` object in the payload)
- Status: LEDSAS → Orchestrator (progress updates)
"""

from typing import Any

from pydantic import BaseModel, Field

from .envelope import Envelope


class Command(BaseModel):
    """
    Command message from Orchestrator to LEDSAS.

    Commands trigger processing in the LEDSAS service. The SDK delivers
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

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ...,
        description="Command-specific payload (deserialized to handler's request model)",
    )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return f"Command(name={self.envelope.name!r}, message_id={self.envelope.message_id!r})"


class Response(BaseModel):
    """
    Response message from LEDSAS to Orchestrator.

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
              "code": "BadInput",
              "message": "Missing required field",
              "retryable": false
            }
          }
        }
    """

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ..., description="Response payload (success result or error details)"
    )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        has_error = "error" in self.payload
        return f"Response(name={self.envelope.name!r}, has_error={has_error}, message_id={self.envelope.message_id!r})"


class Status(BaseModel):
    """
    Status update message from LEDSAS to Orchestrator.

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

    envelope: Envelope = Field(..., description="Message envelope metadata")
    payload: dict[str, Any] = Field(
        ...,
        description="Status payload with stage (str), progress (float 0-1), and optional note (str)",
    )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        stage = self.payload.get("stage", "unknown")
        progress = self.payload.get("progress", 0.0)
        return f"Status(name={self.envelope.name!r}, stage={stage!r}, progress={progress})"
