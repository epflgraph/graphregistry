# graphregistry/application/ports/repositories/prt_lecture.py
from __future__ import annotations
from typing import Protocol, runtime_checkable

from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.types import ActionSet


# Class definition
@runtime_checkable
class LectureRepository(Protocol):
    """Port for persisting and retrieving lecture enrichment data.

    This protocol is intentionally narrow: it only deals with the lecture
    enrichment task/result lifecycle. Workflow state for media processing
    (video download, audio extraction, slide detection) lives in
    ``LectureProcessingStatePort``.
    """

    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:
        ...

    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        ...
