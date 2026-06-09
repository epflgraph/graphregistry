# graphregistry/domain/repositories/rpo_lecture.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentResult, LectureEnrichmentTask
from graphregistry.domain.types import ActionSet

# Class definition
@runtime_checkable
class LectureRepository(Protocol):

    #===============================#
    # Content processing operations #
    #===============================#

    #-----------------------------------------#
    # METHOD GROUP: Video download operations #
    #-----------------------------------------#

    def get_undownloaded(self, limit: int | None = 16) -> NodeKeyList:
        ...

    def get_file_url(self, lecture_key: NodeKey) -> str:
        ...

    def save_video_download_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        ...

    def get_video_download_task_id(self, lecture_key: NodeKey) -> str:
        ...

    def get_unfinished_video_tasks(self, limit: int | None = 16) -> NodeKeyList:
        ...

    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> NodeKey:
        ...

    def get_video_token(self, lecture_key: NodeKey) -> str:
        ...

    #-------------------------------------------#
    # METHOD GROUP: Audio extraction operations #
    #-------------------------------------------#

    def get_with_unextracted_audio(self, limit: int | None = 16) -> NodeKeyList:
        ...

    #=====================================#
    # Lecture field enrichment operations #
    #=====================================#

    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:
        ...

    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        ...
