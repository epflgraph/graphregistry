# graphregistry/application/operations/ops_node.py
from __future__ import annotations
from dataclasses import dataclass
from graphregistry.domain.types import ActionSet
from graphregistry.domain.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.types import ConceptMapType

# Class definition
class NodeOperations:

    # Class constructor
    def __init__(
        self,
        repo: NodeRepository,
        *,
        concept_detection_gateway: ConceptDetectionGateway | None = None,
    ) -> None:
        self.repo = repo
        self.concept_detection_gateway = concept_detection_gateway
        self.msg = GraphLogger()

    #----------------------------------------#
    # Basic Node CRUD/persistence operations #
    #----------------------------------------#

    # Method: List nodes by object type and optional ID pattern, returning a list of (object_type, id, title) tuples
    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str]]:
        return self.repo.list(object_type=object_type, id_pattern=id_pattern)

    # Method: Check if a node exists by its key
    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    # Method: Check if multiple nodes exist by their keys, returning a list of booleans corresponding to the input keys
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        return self.repo.exists_many(key_list)

    # Method: Get a node by its key, returning the Node instance or None if not found
    def get(self, key: NodeKey) -> Node | None:
        return self.repo.get(key)

    # Method: Get multiple nodes by their keys, returning a list of Node instances corresponding to the input keys (with None for keys that are not found)
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        return self.repo.get_many(key_list)

    # Method: Save a node, with optional actions to perform (default is ('commit',)), returning the saved Node instance
    def save(self, node: Node, actions: ActionSet = ('commit',)) -> Node:
        return self.repo.save(node, actions=actions)

    # Method: Save multiple nodes, with optional actions to perform (default is ('commit',)), returning a list of the saved Node instances
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ('commit',)) -> NodeList:
        return self.repo.save_many(node_list, actions=actions)

    # Method: Delete a node by its key, with optional actions to perform (default is ('commit',)), returning True if the node was deleted, False if it was not found, or None if the deletion was not performed due to the actions
    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:
        return self.repo.delete(key, actions=actions)

    # Method: Delete multiple nodes by their keys, with optional actions to perform (default is ('commit',)), returning a list of booleans corresponding to the input keys indicating whether each node was deleted (True), not found (False), or not deleted due to the actions (None)
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        return self.repo.delete_many(key_list, actions=actions)

    #--------------------------------------------------#
    # Node diagnostics and special get/save operations #
    #--------------------------------------------------#

    # Method: Check if a node has detected concepts by its key or Node instance
    def has_concepts(self, node_or_key: Node | NodeKey, map_type: ConceptMapType) -> bool:
        if isinstance(node_or_key, NodeKey):
            node = self.repo.get(node_or_key)
            if not node:
                raise ValueError(f"Node with key {node_or_key} not found")
        else:
            node = node_or_key
        if   not getattr(node.concepts, map_type):
            return False
        elif not getattr(node.concepts, map_type).item_list:
            return False
        elif len(getattr(node.concepts, map_type).item_list) == 0:
            return False
        else:
            return True

    # Method: Get nodes that have no detected concepts, optionally filtered by object type and ID pattern
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        return self.repo.get_with_no_concepts(object_type=object_type, id_pattern=id_pattern)

    #----------------------------------#
    # Node field enrichment operations #
    #----------------------------------#

    # Method: Enrich a node with detected concepts using the concept detection gateway, returning the enriched Node instance
    def enrich_with_concepts(self, nodes: Node | NodeList) -> Node | NodeList:

        # Get gateway for concept detection
        gateway = self.concept_detection_gateway
        if gateway is None:
            raise ValueError("Concept detection gateway not configured")

        # Perform concept detection using the gateway and populate the concepts.detected field
        if isinstance(nodes, NodeList):
            for node in nodes.item_list:
                concepts = gateway.detect_concepts(f"{node.title}. {node.raw_text}" or "")
                node.concepts.detected = concepts
                self.msg.concepts_detected(node.key)
        else:
            concepts = gateway.detect_concepts(f"{nodes.title}. {nodes.raw_text}" or "")
            nodes.concepts.detected = concepts
            self.msg.concepts_detected(nodes.key)

        # Return the node(s) with detected concepts
        return nodes
