# graphregistry/entrypoints/cli/cmd_ai.py
from __future__ import annotations
from typing import cast
import rich
from graphdb.core.graphdb import GraphDB
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.application.operations.ops_lecture import LectureOperations
from graphregistry.adapters.services.asv_schema_default import DefaultSchemaResolver
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.adapters.gateways.graphai.agt_conceptdet import GraphAIConceptDetectionGateway

# Support function: Initialize node operations with the repository and gateways
def _get_node_ops() -> NodeOperations:
    return NodeOperations(
        repo = MySQLNodeRepository(
            db = GraphDB(),
            schema_resolver = DefaultSchemaResolver(engine_name='xaas_coresrv', glbcfg=GlobalConfig())
        ),
        ai_gateways = {
            "concept_detection": GraphAIConceptDetectionGateway()
        }
    )

# Support function: Initialize lecture operations with the repository and gateways
def _get_lecture_ops() -> LectureOperations:
    return LectureOperations(
        repo = MySQLLectureRepository(
            db = GraphDB(),
            schema_resolver = DefaultSchemaResolver(engine_name='xaas_coresrv', glbcfg=GlobalConfig())
        ),
        ai_gateways = {
            "concept_detection": GraphAIConceptDetectionGateway()
        }
    )

#-----------------------------------#
# Handler: Detect concepts in nodes #
#-----------------------------------#
def cmd_ai_detect_concepts(args) -> None:

    # Initialize node operations with the repository and gateways
    node_ops = _get_node_ops()

    # Get list of nodes without detected concepts
    node_list = node_ops.get_with_no_concepts()

    # Detect concepts for the returned list of nodes
    enriched_node_list = cast(NodeList, node_ops.enrich_with_concepts(node_list))

    # Save the enriched nodes back to the repository
    node_ops.save_many(node_list=enriched_node_list)

#--------------------------------------------------#
# Handler: Launch interation of lecture processing #
#--------------------------------------------------#
# def cmd_ai_process_lectures(args) -> None: