# graphregistry/domain/repositories/rpo_lecture.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_node import NodeKey
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.types import ActionSet

# Class definition
@runtime_checkable
class LectureRepository(Protocol):

    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:
        ...

    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        ...
