# graphregistry/domain/interfaces/repositories/__init__.py
from graphregistry.domain.interfaces.repositories.rpo_edge import (
    EdgeRepository,
)
from graphregistry.domain.interfaces.repositories.rpo_node import (
    NodeRepository,
)
__all__ = [
    "NodeRepository",
    "EdgeRepository",
]
