# graphregistry/application/ports/gateways/prt_conceptdet.py
from __future__ import annotations
from typing import Protocol, Any
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList

# Model definition
class ConceptDetectionGateway(Protocol):

    def wiki_search(self, search_term: str) -> list[dict[str, Any]]:
        ...

    def extract_keywords(self, text: str) -> list[str]:
        ...

    def detect_concepts(self, text: str | list[str]) -> ScoredConceptList:
        ...
