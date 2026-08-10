# graphregistry/application/gateways/gtw_video.py
from __future__ import annotations
from typing import Any, Protocol

from graphregistry.domain.models.entities.mdl_lecture import SlideList, Video, Voice


# Model definition
class VideoProcessingGateway(Protocol):

    def launch_video_download(self, video_url: str, no_cache: bool = False) -> str:
        ...

    def get_video_download_result(self, task_id: str) -> dict | None:
        ...

    def get_video(self, file_url: str) -> Video | str | None:
        ...

    def launch_audio_extraction(self, video_token: str, no_cache: bool = False) -> str:
        ...

    def get_audio_extraction_result(self, task_id: str) -> dict | None:
        ...

    def extract_audio(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
    ) -> Voice | str | None:
        ...

    def launch_slide_detection(self, video_token: str, no_cache: bool = False) -> str:
        ...

    def get_slide_detection_result(self, task_id: str) -> dict | None:
        ...

    def extract_slides(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
    ) -> SlideList | str | None:
        ...

    def process_slides(
        self,
        input: Any | None = None,
        *,
        video_token: str | None = None,
        slides_language: str | None = None,
        destination_languages: tuple[str, ...] | None = None,
        translation_gateway: Any | None = None,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 6000,
        ocr_model: str = "google",
        google_api_token: str | None = None,
        **extract_kwargs: Any,
    ) -> tuple[str | None, SlideList]:
        ...
