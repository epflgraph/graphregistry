# graphregistry/domain/exceptions.py
"""Domain-level exceptions used across the application and entrypoints."""
from __future__ import annotations

#==================#
# Class Definition #
#==================#
class DisallowedTypeError(ValueError):
    """Raised when a node or edge type is not allowed by API configuration."""

#==================#
# Class Definition #
#==================#
class PersistenceError(RuntimeError):
    """Base class for failures originating from persistence adapters."""

    # Internal Method: Initialize the error with an optional MySQL error code and message.
    def __init__(self, message: str, *, dbapi_code: int | None = None, dbapi_msg: str | None = None) -> None:
        super().__init__(message)
        self.dbapi_code = dbapi_code
        self.dbapi_msg = dbapi_msg

#==================#
# Class Definition #
#==================#
class TransientPersistenceError(PersistenceError):
    """Base class for transient persistence failures that may succeed on retry.

    The application-layer retry policy keys off this marker, so every transient
    condition classified by a persistence adapter is retried automatically.
    """

#==================#
# Class Definition #
#==================#
class ConnectionExhaustedError(TransientPersistenceError):
    """Raised when the database rejects a new connection (e.g. MySQL 1040)."""

#==================#
# Class Definition #
#==================#
class LockWaitTimeoutError(TransientPersistenceError):
    """Raised when a lock wait timeout occurs (e.g. MySQL 1205)."""

#==================#
# Class Definition #
#==================#
class DeadlockError(TransientPersistenceError):
    """Raised when a transaction deadlocks and is rolled back as victim (e.g. MySQL 1213)."""

#==================#
# Class Definition #
#==================#
class RecordChangedError(TransientPersistenceError):
    """Raised when a record changed since it was last read in the transaction (e.g. MySQL 1020)."""

#==================#
# Class Definition #
#==================#
class DuplicateKeyError(PersistenceError):
    """Raised when a unique constraint is violated (e.g. MySQL 1062)."""

#==================#
# Class Definition #
#==================#
class UnitOfWorkError(PersistenceError):
    """Raised when a unit of work cannot be committed or rolled back."""
