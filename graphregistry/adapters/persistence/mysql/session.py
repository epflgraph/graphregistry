# graphregistry/adapters/persistence/mysql/session.py
"""Transactional session wrapper around a SQLAlchemy connection.

A session corresponds to one database connection and one transaction. It is
used by the MySQL Unit of Work adapter so that several repositories can share
the same connection/transaction for an atomic business operation.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.exc import DataError, IntegrityError, OperationalError, SQLAlchemyError

from graphregistry.domain.exceptions import (
    ConnectionExhaustedError,
    DuplicateKeyError,
    LockWaitTimeoutError,
    PersistenceError,
)

if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB


def _map_sqlalchemy_error(exc: SQLAlchemyError) -> PersistenceError:
    """Translate SQLAlchemy errors into typed domain persistence errors."""
    dbapi_code: int | None = None
    dbapi_msg: str | None = None
    if hasattr(exc, "orig") and exc.orig is not None:
        dbapi_msg = str(exc.orig)
        args = getattr(exc.orig, "args", [None])
        if args and isinstance(args[0], int):
            dbapi_code = args[0]

    if dbapi_code == 1040:
        return ConnectionExhaustedError(
            "Too many database connections.",
            dbapi_code=dbapi_code,
            dbapi_msg=dbapi_msg,
        )
    if dbapi_code == 1205:
        return LockWaitTimeoutError(
            "Lock wait timeout exceeded.",
            dbapi_code=dbapi_code,
            dbapi_msg=dbapi_msg,
        )
    if dbapi_code == 1062:
        return DuplicateKeyError(
            "Duplicate key violation.",
            dbapi_code=dbapi_code,
            dbapi_msg=dbapi_msg,
        )
    if isinstance(exc, IntegrityError):
        return DuplicateKeyError(
            f"Integrity error: {exc}",
            dbapi_code=dbapi_code,
            dbapi_msg=dbapi_msg,
        )

    return PersistenceError(
        f"Database error: {exc}",
        dbapi_code=dbapi_code,
        dbapi_msg=dbapi_msg,
    )


class MySQLSession:
    """One connection, one transaction.

    The session is intentionally low-level: it executes raw SQL and returns
    tuples. Higher-level mapping happens in repositories.
    """

    def __init__(self, db: "GraphDB", engine_name: str) -> None:
        self.db = db
        self.engine_name = engine_name
        self._connection: Any | None = None
        self._transaction: Any | None = None

    def begin(self) -> MySQLSession:
        """Open a connection and start a transaction."""
        engine = self.db.engine[self.engine_name]
        self._connection = engine.connect()
        self._transaction = self._connection.begin()
        return self

    def execute(
        self,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a query inside the bound transaction and return result rows."""
        if self._connection is None:
            raise PersistenceError("Session is not open. Call begin() before execute().")

        try:
            result = self._connection.execute(text(query), parameters=params or {})
            if result.returns_rows:
                return list(result.fetchall())
            return []
        except (DataError, IntegrityError, OperationalError, SQLAlchemyError) as exc:
            raise _map_sqlalchemy_error(exc) from exc

    def commit(self) -> None:
        """Commit the bound transaction."""
        if self._transaction is not None:
            self._transaction.commit()

    def rollback(self) -> None:
        """Roll back the bound transaction."""
        if self._transaction is not None:
            self._transaction.rollback()

    def close(self) -> None:
        """Close the bound connection and clear state."""
        try:
            if self._connection is not None:
                self._connection.close()
        finally:
            self._connection = None
            self._transaction = None
