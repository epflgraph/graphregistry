# graphregistry/adapters/gateways/graphai/agt_voice.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_lecture import Voice, Transcript
from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
import rich

#==================================#
# GraphAI Gateway: Voice endpoints #
#==================================#
class GraphAIVoiceGateway(GraphAIBaseGateway):

    #---------------------------------------------------------------------------#
    # Gateway method: Transcribe audio from a given Voice object or audio token #
    #---------------------------------------------------------------------------#
    def transcribe_audio(
        self,
        input: Voice | str,
        *,
        force: bool = False,
        force_lang: str | None = None,
        strict: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 7200,
    ) -> Transcript | None:
    #---------------------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Get audio token depending on whether we received a Voice object or a token string
        audio_token = input.token if isinstance(input, Voice) else input

        # Prepare payload for the request to the GraphAI endpoint to transcribe the audio
        # corresponding to the given audio token and get the transcription results
        payload: dict[str, Any] = {"token": audio_token, "force": force, "strict": strict}
        if force_lang is not None:
            payload["force_lang"] = force_lang

        # Make the request to the GraphAI endpoint to transcribe the audio corresponding to
        # the given audio token and get the transcription results
        task_result = self._call_async_endpoint(
            endpoint   = "/voice/transcribe",
            payload    = payload,
            login_info = login_info,
            max_processing_time_s = max_processing_time_s,
            max_tries  = max_tries,
        )

        # If the task result is None, it means the request failed and the
        # transcription results could not be obtained, so we return None
        if task_result is None:
            return None

        rich.print(task_result)
        return None

        language = task_result.get("language")
        subtitle_results = task_result.get("subtitle_results")
        if not isinstance(subtitle_results, list):
            return (str(language) if language is not None else None), None

        if not isinstance(language, str) or not language:
            language = "text"

        segments: list[dict[str, Any]] = []
        for segment in subtitle_results:
            if not isinstance(segment, dict):
                continue
            segments.append(
                {
                    "start": segment.get("start"),
                    "end": segment.get("end"),
                    language: str(segment.get("text", "")).strip(),
                }
            )

        return language, segments

    def detect_language(
        self,
        audio_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 3600,
    ) -> str | None:
        login_info = self._ensure_login_info()

        task_result = self._call_async_endpoint(
            endpoint="/voice/detect_language",
            payload={"token": audio_token, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
        )
        if task_result is None:
            return None

        language = task_result.get("language")
        return str(language) if language is not None else None

    def fingerprint(
        self,
        input: Voice | str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 300,
    ) -> str | None:
        login_info = self._ensure_login_info()

        # Get audio token depending on whether we received a Voice object or a token string
        audio_token = input.token if isinstance(input, Voice) else input

        task_result = self._call_async_endpoint(
            endpoint="/voice/calculate_fingerprint",
            payload={"token": audio_token, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
        )
        if task_result is None:
            return None

        result = task_result.get("result")
        return str(result) if result is not None else None
