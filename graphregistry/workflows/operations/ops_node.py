from dataclasses import dataclass
from typing import Any

from graphregistry.domain.models.mdl_node import Node, NodeKey, NodeList
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
        return bool(self.repo.save(node, actions=actions))

    def update(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
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

    # Draft lifecycle/use-case methods migrated from legacy model-centric flow.
    def get_by_key(self, key: NodeKey) -> Node | None:
        return self.repo.get_by_key(key)

    def get_by_keys(self, key_list: list[NodeKey]) -> NodeList:
        return self.repo.get_by_keys(key_list)

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return self.repo.save_many(node_list, actions=actions)

    def commit_node_object(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Use-case draft: map legacy node-object commit to repository save semantics.")

    def commit_custom_fields(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Use-case draft: map legacy custom-fields commit to repository save semantics.")

    def commit_page_profile(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:
        raise NotImplementedError("Use-case draft: integrate page-profile persistence via dedicated repository/gateway.")

    def commit_concepts(self, node: Node, actions: tuple[str, ...] = ("eval",), delete_existing: bool = False) -> Any:
        raise NotImplementedError("Use-case draft: persist detected concepts via concept repository adapter.")

    def commit_manual_mapping(
        self,
        node: Node,
        actions: tuple[str, ...] = ("eval",),
        delete_existing: bool = False,
    ) -> Any:
        raise NotImplementedError("Use-case draft: persist manual concept mappings via dedicated adapter.")

    def refine_concepts(self, node: Node) -> Any:
        raise NotImplementedError("Use-case draft: implement concept post-processing rules here.")
