# graphregistry/workflows/operations/entities/ops_node.py
from __future__ import annotations
from dataclasses import dataclass
from graphregistry.domain.types import ActionSet
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList

# Class definition
@dataclass(frozen=True)
class NodeUpsertResult:
    success: bool
    created: bool

# Class definition
class NodeOperations:

    def __init__(self, repo: NodeRepository, concept_gateway: ConceptGateway | None = None):
        self.repo = repo
        self.concept_gateway = concept_gateway

    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str, str]]:
        return self.repo.list(object_type=object_type, id_pattern=id_pattern)

    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        return self.repo.exists_many(key_list)

    def get(self, key: NodeKey) -> Node | None:
        return self.repo.get(key)

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        return self.repo.get_many(key_list)

    def save(self, node: Node, actions: ActionSet = ("eval",)) -> Node:
        return self.repo.save(node, actions=actions)

    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("eval",)) -> NodeList:
        return self.repo.save_many(node_list, actions=actions)

    def insert(self, node: Node, actions: ActionSet = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(node, actions=actions))

    def update(self, node: Node, actions: ActionSet = ("eval",)) -> bool:
        """
        Backward-compatible alias for save/upsert semantics.
        """
        return bool(self.repo.save(node, actions=actions))

    def upsert(self, node: Node, actions: ActionSet = ("eval",)) -> NodeUpsertResult:
        created = not self.repo.exists(node.key)
        success = bool(self.repo.save(node, actions=actions))
        return NodeUpsertResult(success=success, created=created)

    def delete(self, key: NodeKey, actions: ActionSet = ("eval",)) -> bool | None:
        return self.repo.delete(key, actions=actions)

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        return self.repo.delete_many(key_list, actions=actions)

    def detect_concepts(self, text: str) -> ConceptDetectionResultList:
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")
        return self.concept_gateway.detect_concepts(text)
