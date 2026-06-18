# graphregistry/application/gateways/gtw_voice.py
from __future__ import annotations
from typing import Protocol

from graphregistry.domain.models.entities.mdl_lecture import Transcript, Voice


# Model definition
class VoiceProcessingGateway(Protocol):

    def transcribe_audio(
        self,
        input: Voice | str | None = None,
        *,
        audio_token: str | None = None,
    ) -> Transcript | str | None:
        ...
