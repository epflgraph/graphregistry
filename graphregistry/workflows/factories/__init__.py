# graphregistry/workflows/factories/__init__.py
from graphregistry.workflows.factories.fct_node import (
    NodeFactory,
)
from graphregistry.workflows.factories.fct_text import (
    MultilingualGeneratedTextFactory,
)
__all__ = [
    "NodeFactory",
    "MultilingualGeneratedTextFactory",
]