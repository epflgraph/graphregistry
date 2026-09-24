# graphregistry/application/ports/repositories/resolvers.py
from __future__ import annotations
from typing import Protocol, TypeAlias, runtime_checkable
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
from graphregistry.domain.models.entities.mdl_node import NodeKey

# Type alias for engine and schema tuple
EngineSchema: TypeAlias = tuple[str, str]

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class SchemaResolver(Protocol):
    """
    Resolves where data should be persisted/read from.

    Returns:
        (engine_name, schema_name)
    """

    # Public Method: Resolve the engine and schema of the airflow tracking tables.
    def for_airflow(self) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of a node by its key.
    def for_node(self, key: NodeKey) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of an edge by its key.
    def for_edge(self, key: EdgeKey) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of an object type, given either a
    # node type string or an edge type pair.
    def for_object_type(self, object_type: str | tuple[str, str]) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of the graph cache.
    def for_graph_cache(self) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of the test graphsearch schema.
    def for_graphsearch_test(self) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of the prod-mirror graphsearch schema.
    def for_graphsearch_prod_mirror(self) -> EngineSchema:
        ...

    # Public Method: Resolve the engine and schema of the Elasticsearch cache.
    def for_es_cache(self) -> EngineSchema:
        ...
