# graphregistry/domain/interfaces/gateways/gtw_conceptdet.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList

# Model definition
class ConceptDetectionGateway(Protocol):

    def extract_keywords(self, text: str) -> list[str]:
        ...

    def detect_concepts(self, text: str | list[str]) -> ConceptDetectionResultList:
        ...
