# graphregistry/application/gateways/gtw_audio.py
from __future__ import annotations
from typing import Protocol

# Model definition
class AudioProcessingGateway(Protocol):

    def launch_audio_extraction(self, video_token: str) -> str:
        ...

    def get_audio_extraction_result(self, task_id: str) -> dict | None:
        ...
