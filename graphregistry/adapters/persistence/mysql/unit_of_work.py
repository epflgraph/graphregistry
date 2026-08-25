# graphregistry/adapters/persistence/mysql/unit_of_work.py
"""MySQL implementation of the Unit of Work port.

A unit of work corresponds to one business operation. It lazily opens one
transactional session per database engine used during the operation and
commits/rolls back all of them together.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from graphregistry.adapters.persistence.mysql.repositories.rpo_edgerepo import MySQLEdgeRepository
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_edge import EdgeRepository
from graphregistry.application.ports.repositories.prt_node import NodeRepository
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.domain.exceptions import PersistenceError

if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.repositories.resolvers import SchemaResolver


#================================================================#
# Class Definition                                               #
#================================================================#
class MySQLUnitOfWork(UnitOfWork):
    """MySQL Unit of Work.

    Repositories obtained from this instance execute all writes inside the
    same per-engine transaction.
    """

    # Class initialization and dependency injection
    def __init__(self, db: "GraphDB", schema_resolver: "SchemaResolver") -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self._sessions: dict[str, MySQLSession] = {}
        self._node_repo = MySQLNodeRepository(uow=self)
        self._edge_repo = MySQLEdgeRepository(uow=self)

    @property
    def nodes(self) -> NodeRepository:
        return self._node_repo

    @property
    def edges(self) -> EdgeRepository:
        return self._edge_repo

    def get_session(self, engine_name: str) -> MySQLSession:
        """Return (creating if needed) a transactional session for engine_name."""
        if engine_name not in self._sessions:
            session = MySQLSession(self.db, engine_name)
            session.begin()
            self._sessions[engine_name] = session
        return self._sessions[engine_name]

    def commit(self) -> None:
        """Commit every open session."""
        last_error: BaseException | None = None
        for session in self._sessions.values():
            try:
                session.commit()
            except Exception as exc:  # pragma: no cover - defensive
                last_error = exc
        if last_error is not None:
            raise PersistenceError(f"Failed to commit unit of work: {last_error}") from last_error

    def rollback(self) -> None:
        """Roll back every open session, swallowing errors on cleanup."""
        for session in self._sessions.values():
            try:
                session.rollback()
            except Exception:  # pragma: no cover - best effort cleanup
                pass

    def close(self) -> None:
        """Close all sessions."""
        for session in self._sessions.values():
            try:
                session.close()
            except Exception:  # pragma: no cover - best effort cleanup
                pass
        self._sessions.clear()

    def __enter__(self) -> MySQLUnitOfWork:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        try:
            if exc_val is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
