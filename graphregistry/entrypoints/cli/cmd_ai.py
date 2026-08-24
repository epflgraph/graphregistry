# graphregistry/entrypoints/cli/cmd_ai.py
from __future__ import annotations
from typing import cast
import rich
from graphregistry.domain.models.entities.mdl_node import NodeList
from graphregistry.entrypoints.cli.dependencies import build_node_operations_with_concept_detection_from_args

# Support function: Initialize node operations with the repository and concept-detection gateway
def _get_node_ops(args):
    return build_node_operations_with_concept_detection_from_args(args)

#-----------------------------------#
# Handler: Detect concepts in nodes #
#-----------------------------------#
def cmd_ai_detect_concepts(args) -> None:

    # Initialize node operations with the repository and gateways
    node_ops = _get_node_ops(args)

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