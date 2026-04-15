# graphregistry/workflows/operations/ops_node.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.mdl_node import Node, NodeKey, NodeList

# Class definition
@dataclass(frozen=True)
class UpsertResult:
    success: bool
    created: bool

# Class definition
class NodeOperations:
    def __init__(self, repo: NodeRepository, concept_gateway: ConceptGateway | None = None):
        self.repo = repo
        self.concept_gateway = concept_gateway

    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    def get(self, key: NodeKey) -> Node | None:
        return self.repo.get(key)

    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        return self.repo.get_many(key_list)

    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Node:
        return self.repo.save(node, actions=actions)

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return self.repo.save_many(node_list, actions=actions)

    def insert(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(node, actions=actions))

    def update(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(node, actions=actions))

    def upsert(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> UpsertResult:
        created = not self.repo.exists(node.key)
        success = bool(self.repo.save(node, actions=actions))
        return UpsertResult(success=success, created=created)

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        return bool(self.repo.delete(key, actions=actions))

    def detect_concepts(self, text: str):
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")
        return self.concept_gateway.detect_concepts(text)
