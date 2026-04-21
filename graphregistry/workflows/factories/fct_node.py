# graphregistry/workflows/factories/fct_node.py
from __future__ import annotations
from graphregistry.domain.models.entities.mdl_node import Node
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway


class NodeFactory:
    def __init__(self, concept_gateway: ConceptGateway | None = None) -> None:
        self.concept_gateway = concept_gateway

    def create(self, *, detect_concepts: bool = False, **node_data) -> Node:

        node = Node(**node_data)

        if not detect_concepts:
            return node

        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")

        if not node.raw_text.strip():
            return node

        concepts = self.concept_gateway.detect_concepts(node.raw_text)
        node.detected_concepts = concepts.link_to_node(node.key)
        return node