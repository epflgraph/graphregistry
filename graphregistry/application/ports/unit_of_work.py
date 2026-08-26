# graphregistry/application/ports/unit_of_work.py
"""Unit of Work port.

The Unit of Work is the single persistence boundary used by application
services. It owns repository instances, coordinates transactions, and ensures
that all writes within a business operation commit or roll back together.

Dependencies point inward: the application layer depends only on this port.
Concrete adapters implement it.
"""
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.application.ports.repositories.prt_edge import EdgeRepository
from graphregistry.application.ports.repositories.prt_node import NodeRepository

#================================================================#
# Class Definition                                               #
#================================================================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class UnitOfWork(Protocol):
    """Coordinate a set of persistence operations as one atomic transaction."""

    # Public Method: Return the node repository participating in this unit of work.
    @property
    def nodes(self) -> NodeRepository:
        """Return the node repository participating in this unit of work."""
        ...

    # Public Method: Return the edge repository participating in this unit of work.
    @property
    def edges(self) -> EdgeRepository:
        """Return the edge repository participating in this unit of work."""
        ...

    # Public Method: Commit all changes made inside this unit of work.
    def commit(self) -> None:
        """Commit all changes made inside this unit of work."""
        ...

    # Public Method: Roll back all changes made inside this unit of work.
    def rollback(self) -> None:
        """Roll back all changes made inside this unit of work."""
        ...

    # Internal Function: Enter the unit-of-work context.
    def __enter__(self) -> UnitOfWork:
        ...

    # Internal Function: Exit the unit-of-work context.
    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: object | None) -> None:
        ...
