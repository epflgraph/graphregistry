# graphregistry/domain/exceptions.py
"""Domain-level exceptions used across the application and entrypoints."""

from __future__ import annotations


class DisallowedTypeError(ValueError):
    """Raised when a node or edge type is not allowed by API configuration."""


class PersistenceError(RuntimeError):
    """Base class for failures originating from persistence adapters."""

    def __init__(self, message: str, *, dbapi_code: int | None = None, dbapi_msg: str | None = None) -> None:
        super().__init__(message)
        self.dbapi_code = dbapi_code
        self.dbapi_msg = dbapi_msg


class ConnectionExhaustedError(PersistenceError):
    """Raised when the database rejects a new connection (e.g. MySQL 1040)."""


class LockWaitTimeoutError(PersistenceError):
    """Raised when a lock wait timeout occurs (e.g. MySQL 1205)."""


class DuplicateKeyError(PersistenceError):
    """Raised when a unique constraint is violated (e.g. MySQL 1062)."""


class UnitOfWorkError(PersistenceError):
    """Raised when a unit of work cannot be committed or rolled back."""
