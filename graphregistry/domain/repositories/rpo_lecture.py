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

    def get_unfinished_video_download_tasks(self, limit: int | None = 16) -> NodeKeyList:
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
        
    def save_audio_extraction_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        ...
        
    def get_audio_extraction_task_id(self, lecture_key: NodeKey) -> str:
        ...
        
    def get_unfinished_audio_extraction_tasks(self, limit: int | None = 16) -> NodeKeyList:
        ...
        
    def save_audio_token(self, lecture_key: NodeKey, audio_token: str) -> NodeKey:
        ...
        
    def get_audio_token(self, lecture_key: NodeKey) -> str:
        ...
        
    #------------------------------------------#
    # METHOD GROUP: Slide detection operations #
    #------------------------------------------#
    
    def get_with_undetected_slides(self, limit: int | None = 16) -> NodeKeyList:
        ...
        
    def save_slide_detection_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        ...
        
    def get_slide_detection_task_id(self, lecture_key: NodeKey) -> str:
        ...
        
    def get_unfinished_slide_detection_tasks(self, limit: int | None = 16) -> NodeKeyList:
        ...

    def save_slide_tokens(self, lecture_key: NodeKey, slide_tokens: list[str]) -> NodeKey:
        ...

    def get_slide_tokens(self, lecture_key: NodeKey) -> list[str]:
        ...

    #=====================================#
    # Lecture field enrichment operations #
    #=====================================#

    def get_enrichment_task(self, key: NodeKey) -> LectureEnrichmentTask | None:
        ...

    def save_enrichment_result(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        ...
