# graphregistry/adapters/services/schema/asv_schema_default.py
from __future__ import annotations
from graphregistry.domain.interfaces.services.srv_schema import SchemaResolver, EngineSchema
from graphregistry.domain.models.mdl_node import NodeKey
from graphregistry.domain.models.mdl_edge import EdgeKey
from graphregistry.common.config import GlobalConfig

# Class definition
class DefaultSchemaResolver(SchemaResolver):

    # Initialisation method
    def __init__(self, engine_name: str, glbcfg: GlobalConfig):
        self.engine_name = engine_name
        self.glbcfg = glbcfg

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