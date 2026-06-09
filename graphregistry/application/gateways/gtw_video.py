# graphregistry/domain/interfaces/gateways/gtw_video.py
from __future__ import annotations
from typing import Protocol, Any
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList

# Model definition
class VideoProcessingGateway(Protocol):

    def launch_video_download(self, video_url: str) -> str:
        ...

    def get_video_download_result(self, task_id: str) -> dict | None:
        ...
