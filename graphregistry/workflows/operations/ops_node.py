from graphregistry.domain.models.mdl_node import NodeKey
from graphregistry.domain.interfaces.repositories.rpo_node import NodeRepository
from graphregistry.domain.interfaces.gateways.conceptgateway import ConceptGateway


class NodeOperations:

    def __init__(self, repo: NodeRepository):
        self.repo = repo
        self.concept_gateway = concept_gateway

    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    def insert(self, key: NodeKey) -> bool:
        if self.repo.exists(key):
            raise ValueError("Node already exists")
        return self.repo.insert(key)

    def update(self, key: NodeKey) -> bool:
        return self.repo.update(key)

    def upsert(self, key: NodeKey) -> bool:
        if self.repo.exists(key):
            return self.repo.update(key)
        return self.repo.insert(key)

    def delete(self, key: NodeKey) -> bool:
        return self.repo.delete(key)

    def detect_concepts(self, text: str):
        if self.concept_gateway is None:
            raise ValueError("No concept gateway configured")
        return self.concept_gateway.detect_concepts(text)