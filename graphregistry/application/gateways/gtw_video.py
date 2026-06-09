# graphregistry/application/gateways/gtw_video.py
from __future__ import annotations
from typing import Protocol

# Model definition
class VideoProcessingGateway(Protocol):

    def launch_video_download(self, video_url: str) -> str:
        ...

    def get_video_download_result(self, task_id: str) -> dict | None:
        ...

    def launch_audio_extraction(self, video_token: str) -> str:
        ...

    def get_audio_extraction_result(self, task_id: str) -> dict | None:
        ...
