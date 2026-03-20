from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.mdl_concept import DetectedConceptList

# Model definition
class ConceptGateway(Protocol):
    def detect_concepts(self, text: str) -> DetectedConceptList:
        ...
