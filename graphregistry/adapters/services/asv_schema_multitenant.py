# graphregistry/adapters/services/schema/asv_schema_multitenant.py
from __future__ import annotations
from graphregistry.application.services.srv_schema import SchemaResolver, EngineSchema
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.entities.mdl_edge import EdgeKey

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

    # Helper method: Retrieve tenant config
    def _get_tenant(self, institution_id: str) -> dict:
        """
        Retrieve tenant configuration for a given institution_id.

        Raises:
            ValueError: if the institution_id is not configured
        """
        if institution_id not in self.tenant_config:
            raise ValueError(f"Unknown tenant: {institution_id}")
        return self.tenant_config[institution_id]

    # Class method: Resolve schema for a Node
    def for_node(self, key: NodeKey) -> EngineSchema:
        """
        Resolve persistence target for a Node.

        Args:
            key: NodeKey identifying the node

        Returns:
            (engine_name, schema_name)

        Resolution logic:
        - Select tenant using key.institution_id
        - Map key.object_type to a schema via node_schema_map
        """
        tenant = self._get_tenant(key.institution_id)
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
        - Select tenant using key.from_institution_id
        - Normalize edge type as a sorted tuple of object types
        - Map edge type to a schema via edge_schema_map
        """
        tenant = self._get_tenant(key.from_institution_id)
        engine_name = tenant["engine_name"]
        a = key.from_object_type
        b = key.to_object_type
        edge_type = (a, b) if a <= b else (b, a)
        schema = tenant["edge_schema_map"][edge_type]
        return engine_name, schema
