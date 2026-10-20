"""Error types of the KeborMed LEDSAS SDK.

``from kbm_ledsas_sdk.errors import Permanent`` and
``from kbm_ledsas_sdk import errors`` give the same classes (defined in
``kbm_ledsas_sdk.models.errors``):

- ``Retryable``: a transient error, retried with exponential backoff;
- ``Permanent``: not retried, replied to the caller and dead-lettered;
- ``DeadlineExceeded``: the command's deadline passed (not retried);
- ``SDKError``: the base class of the three;
- ``ContainerNotAllowed``: a blob operation on a container that
  ``KBM_LEDSAS_ALLOWED_CONTAINERS`` does not list (not retried);
- ``ArgumentTypeError``: an argument of the wrong type given to an SDK call
  (a ``TypeError`` that is also a ``ValueError``).
"""

from .models.errors import (
    ArgumentTypeError,
    ContainerNotAllowed,
    DeadlineExceeded,
    Permanent,
    Retryable,
    SDKError,
)

__all__ = [
    "SDKError",
    "Retryable",
    "Permanent",
    "DeadlineExceeded",
    "ContainerNotAllowed",
    "ArgumentTypeError",
]
