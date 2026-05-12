# # graphregistry/workflows/operations/ops_Concept.py
# from __future__ import annotations
# from dataclasses import dataclass
# from graphregistry.domain.types import ActionSet
# from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptGateway
# from graphregistry.domain.interfaces.repositories.rpo_concept import ConceptRepository
# from graphregistry.domain.models.tasks.mdl_conceptdet import Concept, ConceptKey, ConceptList
# from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList

# # Class definition
# @dataclass(frozen=True)
# class ConceptUpsertResult:
#     success: bool
#     created: bool

# # Class definition
# class ConceptOperations:
#     def __init__(self, repo: ConceptRepository, concept_gateway: ConceptGateway | None = None):
#         self.repo = repo
#         self.concept_gateway = concept_gateway

#     def exists(self, key: ConceptKey) -> bool:
#         return self.repo.exists(key)

#     def get(self, key: ConceptKey) -> Concept | None:
#         return self.repo.get(key)

#     def get_many(self, key_list: list[ConceptKey]) -> ConceptList:
#         return self.repo.get_many(key_list)

#     def save(self, Concept: Concept, actions: ActionSet = ("eval",)) -> Concept:
#         return self.repo.save(Concept, actions=actions)

#     def save_many(self, Concept_list: ConceptList, actions: ActionSet = ("eval",)) -> list[Concept]:
#         return self.repo.save_many(Concept_list, actions=actions)

#     def insert(self, Concept: Concept, actions: ActionSet = ("eval",)) -> bool:
#         """
#         Backward-compatible alias for save/upsert semantics.
#         """
#         return bool(self.repo.save(Concept, actions=actions))

#     def update(self, Concept: Concept, actions: ActionSet = ("eval",)) -> bool:
#         """
#         Backward-compatible alias for save/upsert semantics.
#         """
#         return bool(self.repo.save(Concept, actions=actions))

#     def upsert(self, Concept: Concept, actions: ActionSet = ("eval",)) -> ConceptUpsertResult:
#         created = not self.repo.exists(Concept.key)
#         success = bool(self.repo.save(Concept, actions=actions))
#         return ConceptUpsertResult(success=success, created=created)

#     def delete(self, key: ConceptKey, actions: ActionSet = ("eval",)) -> bool:
#         return bool(self.repo.delete(key, actions=actions))

#     def detect_concepts(self, text: str) -> ConceptDetectionResultList:
#         if self.concept_gateway is None:
#             raise ValueError("No concept gateway configured")
#         return self.concept_gateway.detect_concepts(text)
