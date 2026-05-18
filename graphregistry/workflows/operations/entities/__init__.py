# graphregistry/workflows/operations/entities/__init__.py
from graphregistry.workflows.operations.entities.ops_node import (
    NodeOperations,
)
from graphregistry.workflows.operations.entities.ops_edge import (
    EdgeOperations,
)
from graphregistry.workflows.operations.entities.ops_text import (
    GeneratedTextOperations,
)
__all__ = [
    "NodeOperations",
    "EdgeOperations",
    "GeneratedTextOperations",
]