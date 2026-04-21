# graphregistry/workflows/factories/fct_node.py
from __future__ import annotations
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway

# Factory definition
class NodeFactory:
    """Factory for creating Node instances, with optional concept detection.
    If a ConceptGateway is provided and detect_concepts is True, the factory
    will use the gateway to detect concepts from the node's raw text and
    populate the detected_concepts field.
    """
    # Class constructor
    def __init__(self, concept_gateway: ConceptGateway | None = None) -> None:
        self.concept_gateway = concept_gateway

    # Method: Create a Node instance with optional concept detection
    def create(self, *, detect_concepts: bool = False, **node_data) -> Node:

        # Create the Node instance from the provided data
        node = Node(**node_data)

        # If concept detection is not requested, return the node as is
        if not detect_concepts:
            return node

        # If the node has no raw text, skip concept detection and return the node as is
        if not node.raw_text.strip():
            return node

        # If concept detection is requested, ensure that a ConceptGateway is configured
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")

        # Perform concept detection using the gateway and populate the detected_concepts field
        concepts = self.concept_gateway.detect_concepts(node.raw_text)
        node.detected_concepts = concepts

        # Return the node with detected concepts
        return node
