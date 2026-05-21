# graphregistry/application/operations/ops_node.py
from __future__ import annotations
from typing import Any
from dataclasses import dataclass
from graphregistry.domain.types import ActionSet
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList
from graphregistry.domain.interfaces.gateways.types import GatewayDict

# Class definition
class NodeOperations:

    # Class constructor
    def __init__(self, repo: NodeRepository, ai_gateways: GatewayDict | None = None) -> None:
        self.repo = repo
        self.ai_gateways = ai_gateways or {}

    #----------------------------------------#
    # Basic Node CRUD/persistence operations #
    #----------------------------------------#

    # Method: List nodes by object type and optional ID pattern, returning a list of (object_type, id, title) tuples
    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str, str]]:
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

    # Method: Save a node, with optional actions to perform (default is ("eval",)), returning the saved Node instance
    def save(self, node: Node, actions: ActionSet = ("eval",)) -> Node:
        return self.repo.save(node, actions=actions)

    # Method: Save multiple nodes, with optional actions to perform (default is ("eval",)), returning a list of the saved Node instances
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("eval",)) -> NodeList:
        return self.repo.save_many(node_list, actions=actions)

    # Method: Delete a node by its key, with optional actions to perform (default is ("eval",)), returning True if the node was deleted, False if it was not found, or None if the deletion was not performed due to the actions
    def delete(self, key: NodeKey, actions: ActionSet = ("eval",)) -> bool | None:
        return self.repo.delete(key, actions=actions)

    # Method: Delete multiple nodes by their keys, with optional actions to perform (default is ("eval",)), returning a list of booleans corresponding to the input keys indicating whether each node was deleted (True), not found (False), or not deleted due to the actions (None)
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        return self.repo.delete_many(key_list, actions=actions)

    #----------------------------------#
    # Node field enrichment operations #
    #----------------------------------#

    # Method: Enrich a node with detected concepts using the concept detection gateway, returning the enriched Node instance
    def enrich_with_concepts(self, node: Node) -> Node:

        # Get gateway for concept detection
        gateway = self.ai_gateways.get("concept_detection")
        if not gateway:
            raise ValueError("Concept detection gateway not configured")

        # Perform concept detection using the gateway and populate the detected_concepts field
        concepts = gateway.detect_concepts(node.raw_text or "")
        node.detected_concepts = concepts

        # Return the node with detected concepts
        return node
