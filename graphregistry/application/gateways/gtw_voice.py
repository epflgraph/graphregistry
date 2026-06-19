# graphregistry/application/gateways/gtw_voice.py
from __future__ import annotations
from typing import Any, Protocol

from graphregistry.domain.models.entities.mdl_lecture import Transcript, Voice


# Model definition
class VoiceProcessingGateway(Protocol):

    def transcribe_audio(
        self,
        input: Voice | str | None = None,
        *,
        audio_token: str | None = None,
        force: bool = False,
        force_lang: str | None = None,
        strict: bool = False,
        destination_languages: tuple[str, ...] | None = None,
        translation_gateway: Any | None = None,
        max_tries: int = 5,
        max_processing_time_s: int = 7200,
        launch_only: bool = False,
    ) -> Transcript | str | None:
        ...

    def detect_language(
        self,
        audio_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 3600,
        launch_only: bool = False,
    ) -> str | None:
        ...

    def fingerprint(
        self,
        input: Voice | str | None = None,
        *,
        audio_token: str | None = None,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 300,
        launch_only: bool = False,
    ) -> str | None:
        ...
