#!/usr/bin/env python3
from __future__ import annotations
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway
import rich

# Initialize node operations with the repository and gateways
node_ops = NodeOperations(
    repo = MySQLNodeRepository(
        db = GraphDB(),
        schema_resolver = DefaultSchemaResolver(engine_name='xaas_coresrv', glbcfg=GlobalConfig())
    ),
    ai_gateways = {
        "concept_detection": GraphAIConceptDetectionGateway()
    }
)

# Example node to enrich with concept detection
node = Node(
    key=NodeKey(
        institution_id="EPFL",
        object_type="Course",
        object_id="MATH-101",
    ),
    title="Introduction to Geometry",
    raw_text="""
    To draw a straight line from any point to any point.
    To produce a finite straight line continuously in a straight line.
    To describe a circle with any center and radius.
    That all right angles equal one another.
    That, if a straight line falling on two straight lines makes the
    interior angles on the same side less than two right angles,
    the two straight lines, if produced indefinitely, meet on that side
    on which are the angles less than the two right angles.
    """,
)

# Enrich the node with detected concepts using the NodeOperations class
node_list = node_ops.get_with_no_concepts()
for node in node_list.item_list:
    rich.print(node.key)

# Enrich the node with detected concepts using the NodeOperations class
enriched_node_list = node_ops.enrich_with_concepts(node_list)

# Print the detected concepts
print("\nDetected concepts:")
if isinstance(enriched_node_list, NodeList):
    for node in enriched_node_list.item_list:
        for c in node.detected_concepts.item_list:
            rich.print(c)
else:
    for c in enriched_node_list.detected_concepts.item_list:
        rich.print(c)
