# graphregistry/domain/interfaces/repositories/rpo_lecture.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask
from graphregistry.domain.types import ActionSet

# Class definition
@runtime_checkable
class LectureRepository(Protocol):

    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str, str]]:
        ...

    def exists(self, key: NodeKey) -> bool:
        ...

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        ...

    def get(self, key: NodeKey) -> Lecture | None:
        ...

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> LectureList:
        ...

    def save(self, lecture: Lecture, actions: ActionSet = ('commit',)) -> Lecture:
        ...

    def save_many(self, lecture_list: LectureList | list[Lecture], actions: ActionSet = ('commit',)) -> LectureList:
        ...

    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:
        ...

    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        ...

    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> LectureList:
        ...

    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:
        ...

    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        ...