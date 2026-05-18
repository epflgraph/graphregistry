# graphregistry/domain/interfaces/gateways/gtw_conceptdet.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList

# Model definition
class ConceptGateway(Protocol):
    def detect_concepts(self, text: str) -> ConceptDetectionResultList:
        ...
