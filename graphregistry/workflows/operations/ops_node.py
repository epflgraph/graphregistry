from dataclasses import dataclass

from graphregistry.domain.models.mdl_node import Node, NodeKey
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway


@dataclass(frozen=True)
class UpsertResult:
    success: bool
    created: bool


class NodeOperations:

    def __init__(self, repo: NodeRepository, concept_gateway: ConceptGateway | None = None):
        self.repo = repo
        self.concept_gateway = concept_gateway

    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    def insert(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
        if self.repo.exists(node.key):
            raise ValueError("Node already exists")
        return bool(self.repo.save(node, actions=actions))

    def update(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
        if not self.repo.exists(node.key):
            raise ValueError("Node does not exist")
        return bool(self.repo.save(node, actions=actions))

    def upsert(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> UpsertResult:
        created = not self.repo.exists(node.key)
        success = bool(self.repo.save(node, actions=actions))
        return UpsertResult(success=success, created=created)

    def delete(self, key: NodeKey) -> bool:
        return bool(self.repo.delete(key))

    def detect_concepts(self, text: str):
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")
        return self.concept_gateway.detect_concepts(text)
