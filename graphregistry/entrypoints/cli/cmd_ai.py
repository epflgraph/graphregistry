# graphregistry/entrypoints/cli/cmd_ai.py
from __future__ import annotations
from typing import cast
import rich
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from graphregistry.entrypoints.cli.dependencies import build_node_operations_with_concept_detection_from_args

# Internal Function: Initialize node operations with the repository and concept-detection gateway.
def _get_node_ops(args):
    return build_node_operations_with_concept_detection_from_args(args)

#-----------------------------------#
# Handler: Detect concepts in nodes #
#-----------------------------------#
# Public Method: Detect concepts in nodes without detected concepts.
def cmd_ai_detect_concepts(args) -> None:

    # Initialize node operations with the repository and gateways
    node_ops = _get_node_ops(args)

    # Resolve the object types to process. Default to Course for backward compatibility.
    object_types = [t.strip() for t in args.types.split(',') if t.strip()] if args.types else ["Course"]

    # Show which object types will be processed.
    print(f"Detecting concepts for object types: {', '.join(object_types)}")

    # Collect nodes without detected concepts for each requested object type.
    candidate_nodes: list[Node] = []
    for object_type in object_types:
        node_list = node_ops.get_with_no_concepts(object_type=object_type)
        candidate_nodes.extend(node_list.item_list)

    # Loop through the collected nodes and detect concepts for each node.
    for node in candidate_nodes:

        # Detect concepts for the node
        enriched_node = node_ops.enrich_with_concepts(node)

        ###############################################
        ################## TO DELETE ##################
        ###############################################
        # import pickle
        # # # save as pickle file
        # # with open(f"enriched_node_{enriched_node.key}.pkl", "wb") as f:
        # #     pickle.dump(enriched_node, f)

        # # Load from pickle file
        # with open(f"enriched_node_{node.key}.pkl", "rb") as f:
        #     enriched_node = pickle.load(f)

        # rich.print(enriched_node)
        # exit()
        ###############################################
        ###############################################

        # Save the enriched node back to the repository
        node_ops.save(enriched_node)

#--------------------------------------------------#
# Handler: Launch interation of lecture processing #
#--------------------------------------------------#
# def cmd_ai_process_lectures(args) -> None: