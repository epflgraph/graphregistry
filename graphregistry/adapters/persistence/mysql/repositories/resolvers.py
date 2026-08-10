# graphregistry/adapters/persistence/mysql/repositories/resolvers.py
from __future__ import annotations
from graphregistry.application.ports.repositories.resolvers import SchemaResolver, EngineSchema
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

# Class definition
class MultiTenantSchemaResolver(SchemaResolver):
    """
    SchemaResolver implementation supporting multi-tenant deployments.

    Responsibilities:
    - Route persistence operations based on institution (tenant)
    - Resolve the correct (engine_name, schema_name) pair for nodes and edges
    - Provide a central place for environment routing and future sharding logic

    This adapter isolates infrastructure concerns (multi-tenancy, routing)
    from the domain and repository layers.
    """

    # Initialization with tenant configuration
    def __init__(self, tenant_config: dict[str, dict]):
        """
        Initialize the resolver with a tenant configuration.

        Expected structure:

        {
            "EPFL": {
                "engine_name": "xaas_coresrv",
                "node_schema_map": {
                    "Person": "graph_person",
                    "Course": "graph_course",
                    ...
                },
                "edge_schema_map": {
                    ("Course", "Person"): "graph_course_person",
                    ("Person", "Person"): "graph_person_person",
                    ...
                }
            },
            "ETHZ": {
                "engine_name": "ethz_cluster",
                "node_schema_map": {...},
                "edge_schema_map": {...}
            }
        }

        Notes:
        - `engine_name` must match a configured GraphDB engine
        - `node_schema_map` maps object_type -> schema name
        - `edge_schema_map` maps (object_type_a, object_type_b) -> schema name
          (keys are expected to be sorted tuples)
        """
        self.tenant_config = tenant_config

    # Helper method: Retrieve the default tenant config
    def _get_tenant(self) -> dict:
        """
        Retrieve the default tenant configuration.

        Raises:
            ValueError: if no tenant configuration is available
        """
        if not self.tenant_config:
            raise ValueError("No tenant configuration available")
        return next(iter(self.tenant_config.values()))

    # Class method: Resolve schema for a Node
    def for_node(self, key: NodeKey) -> EngineSchema:
        """
        Resolve persistence target for a Node.

        Args:
            key: NodeKey identifying the node

        Returns:
            (engine_name, schema_name)

        Resolution logic:
        - Select the default tenant
        - Map key.object_type to a schema via node_schema_map
        """
        tenant = self._get_tenant()
        engine_name = tenant["engine_name"]
        node_type = key.object_type
        schema = tenant["node_schema_map"][node_type]
        return engine_name, schema

    # Class method: Resolve schema for an Edge
    def for_edge(self, key: EdgeKey) -> EngineSchema:
        """
        Resolve persistence target for an Edge.

        Args:
            key: EdgeKey identifying the edge

        Returns:
            (engine_name, schema_name)

        Resolution logic:
        - Select the default tenant
        - Normalize edge type as a sorted tuple of object types
        - Map edge type to a schema via edge_schema_map
        """
        tenant = self._get_tenant()
        engine_name = tenant["engine_name"]
        a = key.from_object_type
        b = key.to_object_type
        edge_type = (a, b) if a <= b else (b, a)
        schema = tenant["edge_schema_map"][edge_type]
        return engine_name, schema
