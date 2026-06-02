# graphregistry/application/gateways/gtw_lectureenrich.py
from __future__ import annotations
from typing import Protocol
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask

# Model definition
class LectureEnrichmentGateway(Protocol):
    """Gateway protocol for lecture enrichment operations
    """

    def enrich(self, task: LectureEnrichmentTask) -> LectureEnrichmentResult:
        ...