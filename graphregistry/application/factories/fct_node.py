# graphregistry/application/factories/fct_node.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.application.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.entrypoints.mappers import SpecMapper
from graphregistry.entrypoints.schemas import NodeSpec

# Factory definition
class NodeFactory:
    """Factory for creating Node instances, with optional concept detection.
    If a ConceptDetectionGateway is provided and detect_concepts is True, the factory
    will use the gateway to detect concepts from the node's raw text and
    populate the concepts.detected field.
    """
    # Class constructor
    def __init__(self, concept_gateway: ConceptDetectionGateway | None = None) -> None:
        self.concept_gateway = concept_gateway

    # Method: Create a Node instance with optional concept detection
    def create(self, *, detect_concepts: bool = False, **node_data) -> Node:

        # Create the Node instance from the provided data
        node = Node(**node_data)

        # If concept detection is not requested, return the node as is
        if not detect_concepts:
            return node

        # If the node has no raw text, skip concept detection and return the node as is
        if not (node.raw_text or "").strip():
            return node

        # If concept detection is requested, ensure that a ConceptDetectionGateway is configured
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")

        # Perform concept detection using the gateway and populate the concepts.detected field
        concepts = self.concept_gateway.detect_concepts(node.raw_text or "")
        node.concepts.detected = concepts

        # Return the node with detected concepts
        return node

    # Method: Create a Node with the equivalent of SpecMapper.from_node_spec(node_spec)
    def from_node_spec(self, node_spec: NodeSpec | dict[str, Any], detect_concepts: bool = False) -> Node:
        node = SpecMapper.from_node_spec(node_spec)
        return self.create(
                key             = node.key,
                title           = node.title,
                text_source     = node.text_source,
                raw_text        = node.raw_text,
                field_list      = node.field_list,
                page_profile    = node.page_profile,
                detect_concepts = detect_concepts
            )