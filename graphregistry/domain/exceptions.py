# graphregistry/domain/exceptions.py
"""Domain-level exceptions used across the application and entrypoints."""

from __future__ import annotations


#================================================================#
# Class Definition                                               #
#================================================================#
class DisallowedTypeError(ValueError):
    """Raised when a node or edge type is not allowed by API configuration."""


#================================================================#
# Class Definition                                               #
#================================================================#
class PersistenceError(RuntimeError):
    """Base class for failures originating from persistence adapters."""

    # Class initialization and dependency injection
    def __init__(self, message: str, *, dbapi_code: int | None = None, dbapi_msg: str | None = None) -> None:
        super().__init__(message)
        self.dbapi_code = dbapi_code
        self.dbapi_msg = dbapi_msg


#================================================================#
# Class Definition                                               #
#================================================================#
class ConnectionExhaustedError(PersistenceError):
    """Raised when the database rejects a new connection (e.g. MySQL 1040)."""


#================================================================#
# Class Definition                                               #
#================================================================#
class LockWaitTimeoutError(PersistenceError):
    """Raised when a lock wait timeout occurs (e.g. MySQL 1205)."""


#================================================================#
# Class Definition                                               #
#================================================================#
class DuplicateKeyError(PersistenceError):
    """Raised when a unique constraint is violated (e.g. MySQL 1062)."""


#================================================================#
# Class Definition                                               #
#================================================================#
class UnitOfWorkError(PersistenceError):
    """Raised when a unit of work cannot be committed or rolled back."""
