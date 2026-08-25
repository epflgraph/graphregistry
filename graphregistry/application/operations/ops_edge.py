# graphregistry/application/operations/ops_edge.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable

from graphregistry.application.ports.repositories.prt_edge import EdgeRepository
from graphregistry.application.ports.unit_of_work import UnitOfWork
from graphregistry.application.resilience import retry_on_transient_db_error
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.types import ActionSet


@dataclass(frozen=True)
class EdgeUpsertResult:
    success: bool
    created: bool


#================================================================#
# Class Definition                                               #
#================================================================#
class _RepoAsEdgeUoW(UnitOfWork):
    """Backward-compat wrapper that exposes a single repository as a UoW."""

    # Class initialization and dependency injection
    def __init__(self, repo: EdgeRepository) -> None:
        self._repo = repo

    @property
    def nodes(self) -> Any:
        raise NotImplementedError("Nodes are not available in this backward-compat wrapper.")

    @property
    def edges(self) -> EdgeRepository:
        return self._repo

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def __enter__(self) -> UnitOfWork:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        pass


#================================================================#
# Class Definition                                               #
#================================================================#
class EdgeOperations:
    """Application service for edge-related use cases."""

    # Class initialization and dependency injection
    def __init__(
        self,
        uow_factory: Callable[[], UnitOfWork] | None = None,
        *,
        repo: EdgeRepository | None = None,
    ) -> None:
        if repo is not None and uow_factory is not None:
            raise ValueError("Provide either uow_factory= or repo=, not both.")
        if repo is not None:
            self.uow_factory = lambda: _RepoAsEdgeUoW(repo)
        elif uow_factory is not None:
            self.uow_factory = uow_factory
        else:
            raise ValueError("EdgeOperations requires either uow_factory= or repo=.")

    #================================================================#
    # Function Group: Internal helpers                               #
    #================================================================#

    # Function: Return the edge repository from a new unit of work.
    def _repo(self) -> EdgeRepository:
        return self.uow_factory().edges

    #================================================================#
    # Method Group: Basic Edge CRUD/persistence operations           #
    #================================================================#

    # Method: List edges of a given object-type pair and optional ID pattern.
    def list(self, object_type: tuple[str, str], id_pattern: str | None = None) -> list[tuple[str, str, str, str, str]]:
        with self.uow_factory() as uow:
            return uow.edges.list(object_type=object_type, id_pattern=id_pattern)

    # Method: Check whether a single edge exists.
    def exists(self, key: EdgeKey) -> bool:
        with self.uow_factory() as uow:
            return uow.edges.exists(key)

    # Method: Check whether a list of edges exist.
    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        with self.uow_factory() as uow:
            return uow.edges.exists_many(key_list)

    # Method: Retrieve a single edge by key.
    def get(self, key: EdgeKey) -> Edge | None:
        with self.uow_factory() as uow:
            return uow.edges.get(key)

    # Method: Retrieve a list of edges by key.
    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        with self.uow_factory() as uow:
            return uow.edges.get_many(key_list)

    # Method: Save a single edge, retrying transient database errors.
    @retry_on_transient_db_error()
    def save(self, edge: Edge, actions: ActionSet = ('commit',)) -> Edge:
        with self.uow_factory() as uow:
            return uow.edges.save(edge, actions=actions)

    # Method: Save a list of edges, retrying transient database errors.
    @retry_on_transient_db_error()
    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ('commit',)) -> EdgeList:
        with self.uow_factory() as uow:
            return uow.edges.save_many(edge_list, actions=actions)

    # Method: Backward-compatible alias for save/upsert semantics.
    def insert(self, edge: Edge, actions: ActionSet = ('commit',)) -> bool:
        """Backward-compatible alias for save/upsert semantics."""
        return bool(self.save(edge, actions=actions))

    # Method: Backward-compatible alias for save/upsert semantics.
    def update(self, edge: Edge, actions: ActionSet = ('commit',)) -> bool:
        """Backward-compatible alias for save/upsert semantics."""
        return bool(self.save(edge, actions=actions))

    # Method: Upsert an edge and report whether it was newly created.
    def upsert(self, edge: Edge, actions: ActionSet = ('commit',)) -> EdgeUpsertResult:
        with self.uow_factory() as uow:
            created = not uow.edges.exists(edge.key)
            success = bool(uow.edges.save(edge, actions=actions))
            return EdgeUpsertResult(success=success, created=created)

    # Method: Delete a single edge, retrying transient database errors.
    @retry_on_transient_db_error()
    def delete(self, key: EdgeKey, actions: ActionSet = ('commit',)) -> bool | None:
        with self.uow_factory() as uow:
            return uow.edges.delete(key, actions=actions)

    # Method: Delete a list of edges, retrying transient database errors.
    @retry_on_transient_db_error()
    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        with self.uow_factory() as uow:
            return uow.edges.delete_many(key_list, actions=actions)

    #================================================================#
    # Method Group: Backward-compatible repo accessor                #
    #================================================================#

    # Method: Expose the edge repository for callers that still expect it.
    @property
    def repo(self) -> EdgeRepository:
        """Expose the edge repository for callers that still expect it.

        Deprecated: prefer to obtain repositories through a UnitOfWork.
        """
        return self._repo()
