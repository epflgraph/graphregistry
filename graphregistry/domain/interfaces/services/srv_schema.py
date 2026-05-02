# graphregistry/domain/interfaces/services/srv_schema.py
from __future__ import annotations
from typing import Protocol, TypeAlias
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.entities.mdl_edge import EdgeKey

# Type alias for engine and schema tuple
EngineSchema: TypeAlias = tuple[str, str]

# Class definition
class SchemaResolver(Protocol):
    """
    Resolves where data should be persisted/read from.

    Returns:
        (engine_name, schema_name)
    """

    def for_node(self, key: NodeKey) -> EngineSchema:
        ...

    def for_edge(self, key: EdgeKey) -> EngineSchema:
        ...

    def for_object_type(self, object_type: str | tuple[str, str]) -> EngineSchema:
        ...