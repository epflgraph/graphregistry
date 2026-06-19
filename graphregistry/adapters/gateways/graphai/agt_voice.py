# graphregistry/adapters/gateways/graphai/agt_voice.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_lecture import TranscriptSegment, Voice, Transcript
from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
import rich

#==================================#
# GraphAI Gateway: Voice endpoints #
#==================================#
class GraphAIVoiceGateway(GraphAIBaseGateway):

    #===================#
    # Top level methods #
    #===================#

    #---------------------------------------------------------------------------#
    # Gateway method: Transcribe audio from a given Voice object or audio token #
    #---------------------------------------------------------------------------#
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
    #---------------------------------------------------------------------------#

        # Resolve audio token from explicit token param or from input object/string
        if audio_token is None:
            if input is None:
                raise ValueError("Either input or audio_token must be provided")
            audio_token = input.token if isinstance(input, Voice) else input

        if launch_only:
            # launch_only is not supported for the high-level orchestrated API.
            return None

        # 1. Detect language unless the caller forces one.
        detected_language: str | None = force_lang
        if detected_language is None:
            detected_language = self.detect_language(
                audio_token,
                force=force,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
            )

        # 2. Only a small set of languages is supported for transcription fallback.
        supported_languages = {"en", "fr", "de", "it"}
        if detected_language not in supported_languages:
            detected_language = None

        # 3. Try transcription with detected/forced language, then English, then French.
        languages_to_try = []
        if detected_language is not None:
            languages_to_try.append(detected_language)
        if "en" not in languages_to_try:
            languages_to_try.append("en")
        if "fr" not in languages_to_try:
            languages_to_try.append("fr")

        task_result: dict[str, Any] | None = None
        final_language: str | None = None
        for lang in languages_to_try:
            task_result = self._transcribe_audio_raw(
                audio_token=audio_token,
                language=lang,
                force=force,
                strict=strict,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
            )
            if task_result is None:
                continue
            segments = task_result.get("subtitle_results") or []
            if segments:
                final_language = lang
                break

        if task_result is None or final_language is None:
            return None

        # 4. Build domain Transcript.
        transcript = Transcript(
            language  = final_language,
            full_text = task_result.get("transcript_results"),
        )
        for transcript_segment_json in task_result["subtitle_results"]:
            transcript.item_list.append(
                TranscriptSegment(
                    start = transcript_segment_json["start"],
                    end   = transcript_segment_json["end"],
                    text  = transcript_segment_json["text"],
                )
            )

        # 5. Optionally translate subtitle text into destination languages.
        if destination_languages and translation_gateway is not None and transcript.item_list:
            self._translate_subtitles(
                transcript,
                source_language=final_language,
                destination_languages=destination_languages,
                translation_gateway=translation_gateway,
            )

        return transcript

    def _transcribe_audio_raw(
        self,
        audio_token: str,
        language: str,
        *,
        force: bool,
        strict: bool,
        max_tries: int,
        max_processing_time_s: int,
    ) -> dict[str, Any] | None:
        """Call /voice/transcribe with a forced language."""
        login_info = self._ensure_login_info()
        payload: dict[str, Any] = {
            "token": audio_token,
            "force": force,
            "strict": strict,
            "force_lang": language,
        }
        return self._call_async_endpoint(
            endpoint="/voice/transcribe",
            payload=payload,
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=True,
        )

    @staticmethod
    def _translate_subtitles(
        transcript: Transcript,
        *,
        source_language: str,
        destination_languages: tuple[str, ...],
        translation_gateway: Any,
    ) -> None:
        """Translate each segment's text into the requested destination languages."""
        source_texts = [segment.text for segment in transcript.item_list]

        for target_language in destination_languages:
            if target_language == source_language:
                continue
            translated = translation_gateway.translate_text_list(
                source_texts,
                source_language=source_language,
                target_language=target_language,
            )
            if translated is None or len(translated) != len(source_texts):
                continue
            for segment, translated_text in zip(transcript.item_list, translated):
                if translated_text is None:
                    continue
                if segment.translations is None:
                    segment.translations = {}
                segment.translations[target_language] = translated_text.strip()

        # Source language is always available implicitly.
        for segment in transcript.item_list:
            if segment.translations is None:
                segment.translations = {}
            segment.translations.setdefault(source_language, segment.text)

    def detect_language(
        self,
        audio_token: str,
        *,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 3600,
        launch_only: bool = False,
    ) -> str | None:
        login_info = self._ensure_login_info()

        task_result = self._call_async_endpoint(
            endpoint="/voice/detect_language",
            payload={"token": audio_token, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        assert isinstance(task_result, dict)
        language = task_result.get("language")
        return str(language) if language is not None else None

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
        login_info = self._ensure_login_info()

        # Resolve audio token from explicit token param or from input object/string
        if audio_token is None:
            if input is None:
                raise ValueError("Either input or audio_token must be provided")
            audio_token = input.token if isinstance(input, Voice) else input

        task_result = self._call_async_endpoint(
            endpoint="/voice/calculate_fingerprint",
            payload={"token": audio_token, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        assert isinstance(task_result, dict)
        result = task_result.get("result")
        return str(result) if result is not None else None
