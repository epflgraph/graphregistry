# graphregistry/workflows/factories/__init__.py
from graphregistry.application.factories.fct_node import (
    NodeFactory,
)
from graphregistry.application.factories.fct_text import (
    MultilingualGeneratedTextFactory,
)
__all__ = [
    "NodeFactory",
    "MultilingualGeneratedTextFactory",
]