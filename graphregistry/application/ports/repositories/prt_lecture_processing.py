# graphregistry/application/ports/repositories/prt_lecture_processing.py
from __future__ import annotations
from typing import Protocol, runtime_checkable

from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList


# Class definition
@runtime_checkable
class LectureProcessingStatePort(Protocol):
    """Port for tracking asynchronous lecture media-processing state.

    This port is separate from ``LectureRepository`` because it does not
    persist the ``Lecture`` aggregate itself; it tracks workflow state such
    as video download tasks, audio extraction tasks, and slide detection tasks.
    """

    # =============================== #
    # Video download operations       #
    # =============================== #

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

    # =============================== #
    # Audio extraction operations     #
    # =============================== #

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

    # =============================== #
    # Slide detection operations      #
    # =============================== #

    def get_with_undetected_slides(self, limit: int | None = 16) -> NodeKeyList:
        ...

    def save_slide_detection_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        ...

    def get_slide_detection_task_id(self, lecture_key: NodeKey) -> str:
        ...

    def get_unfinished_slide_detection_tasks(self, limit: int | None = 16) -> NodeKeyList:
        ...

    def save_slide_tokens(self, lecture_key: NodeKey, slide_num_and_tokens: list[tuple[int, str]]) -> NodeKey:
        ...

    def get_slide_tokens(self, lecture_key: NodeKey) -> list[str]:
        ...
