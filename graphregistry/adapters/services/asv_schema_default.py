# graphregistry/adapters/services/schema/asv_schema_default.py
from __future__ import annotations
from graphregistry.application.services.srv_schema import SchemaResolver, EngineSchema
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.entities.mdl_edge import EdgeKey
from graphregistry.common.config import GlobalConfig

# Class definition
class DefaultSchemaResolver(SchemaResolver):

    # Initialisation method
    def __init__(self, engine_name: str, glbcfg: GlobalConfig):
        self.engine_name = engine_name
        self.glbcfg = glbcfg

    # Class method: Get engine identifier and schema name for airflow
    def for_airflow(self) -> EngineSchema:
        schema = self.glbcfg.schema_airflow
        return (self.engine_name, schema)

    # Class method: Get engine identifier and schema name for a node
    def for_node(self, key: NodeKey) -> EngineSchema:
        schema = self.glbcfg.object_type_to_schema[key.object_type]
        return (self.engine_name, schema)

    # Class method: Get engine identifier and schema name for an edge
    def for_edge(self, key: EdgeKey) -> EngineSchema:
        a = key.from_object_type
        b = key.to_object_type
        edge_type = (a, b) if a <= b else (b, a)
        schema = self.glbcfg.object2object_type_to_schema[edge_type]
        return (self.engine_name, schema)

    # Class method: Get engine identifier and schema name for an object type (node or edge)
    def for_object_type(self, object_type: str | tuple[str, str]) -> EngineSchema:
        if type(object_type) is str:
            schema = self.glbcfg.object_type_to_schema[object_type]
            return (self.engine_name, schema)
        elif type(object_type) is tuple:
            a,b = object_type
            edge_type = (a, b) if a <= b else (b, a)
            schema = self.glbcfg.object2object_type_to_schema[edge_type]
            return (self.engine_name, schema)
        else:
            raise ValueError(f"Invalid object_type: {object_type}")

    # Class method: Get engine identifier and schema name for graph cache
    def for_graph_cache(self) -> EngineSchema:
        schema = self.glbcfg.schema_graph_cache_test
        return (self.engine_name, schema)

    # Class method: Get engine identifier and schema name for graph search (test)
    def for_graphsearch_test(self) -> EngineSchema:
        schema = self.glbcfg.schema_graphsearch_test
        return (self.engine_name, schema)

    # Class method: Get engine identifier and schema name for graph search (prod mirror)
    def for_graphsearch_prod_mirror(self) -> EngineSchema:
        schema = self.glbcfg.schema_graphsearch_prod_mirror
        return (self.engine_name, schema)
