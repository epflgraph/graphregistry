# graphregistry/adapters/gateways/graphai/agt_video.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from requests import post

from graphregistry.adapters.gateways.graphai.agt_base import GraphAIBaseGateway
from graphregistry.domain.models.entities.mdl_lecture import Slide, SlideList, Video, Voice


class GraphAIVideoGateway(GraphAIBaseGateway):
    """GraphAI adapter for video download, fingerprinting, audio extraction, and slide detection."""

    def __init__(
        self,
        graph_api_json: str | Path | None = None,
        login_info: dict[str, Any] | None = None,
        debug: bool = False,
        *,
        voice_gateway: Any | None = None,
        image_gateway: Any | None = None,
    ) -> None:
        super().__init__(graph_api_json, login_info, debug)
        self._voice_gateway = voice_gateway
        self._image_gateway = image_gateway

    def _get_voice_gateway(self) -> Any:
        """Lazy getter for the voice gateway used to fingerprint audio tokens."""
        if self._voice_gateway is None:
            from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
            self._voice_gateway = GraphAIVoiceGateway(
                graph_api_json=self.graph_api_json,
                login_info=self._login_info,
                debug=self.debug,
            )
        return self._voice_gateway

    def _get_image_gateway(self) -> Any:
        """Lazy getter for the image gateway used to process slides."""
        if self._image_gateway is None:
            from graphregistry.adapters.gateways.graphai.agt_image import GraphAIImageGateway
            self._image_gateway = GraphAIImageGateway(
                graph_api_json=self.graph_api_json,
                login_info=self._login_info,
                debug=self.debug,
            )
        return self._image_gateway


    # Gateway method: Launch asynchronous video download and processing task on GraphAI
    def launch_video_download(self, video_url: str, no_cache: bool = False) -> str:
        task_id = self.get_video(file_url=video_url, force=no_cache, launch_only=True)
        assert isinstance(task_id, str), "Expected task_id to be a string when launch_only is True"
        return task_id

    # Gateway method: Get the result of an asynchronous video download and processing task from GraphAI using the task ID
    def get_video_download_result(self, task_id: str) -> dict | None:

        # Make the request to the GraphAI endpoint to get the result of the video download and processing task
        task_result = self.get_async_task_result(
            endpoint = "/video/retrieve_url",
            task_id  = task_id,
            wait_for_result = False
        )

        # If the task result is None, it means the request failed or the video could not be processed
        if task_result is None:
            return None

        # Return the task result, which should contain the video token and metadata if the processing was successful
        return task_result

    # Gateway method: Launch audio extraction from a video token and get the corresponding task ID
    def launch_audio_extraction(self, video_token: str, no_cache: bool = False) -> str:
        task_id = self.extract_audio(
            video_token=video_token,
            recalculate_cached=False,
            force=no_cache,
            launch_only=True,
        )
        assert isinstance(task_id, str), "Expected task_id to be a string when launch_only is True"
        return task_id

    # Gateway method: Get the result of an audio extraction task by its task ID
    def get_audio_extraction_result(self, task_id: str) -> dict | None:

        # Make the request to the GraphAI endpoint to get the result of the audio extraction task
        task_result = self.get_async_task_result(
            endpoint = "/video/extract_audio",
            task_id  = task_id,
            wait_for_result = False
        )

        # If the task result is None, it means the request failed or the audio could not be extracted
        if task_result is None:
            return None

        # Return the task result, which should contain the audio token and metadata if the extraction was successful
        return task_result

    # Gateway method: Launch slide detection from a video token and get the corresponding task ID
    def launch_slide_detection(self, video_token: str, no_cache: bool = False) -> str:
        task_id = self.extract_slides(
            video_token=video_token,
            recalculate_cached=False,
            force=no_cache,
            launch_only=True,
        )
        assert isinstance(task_id, str), "Expected task_id to be a string when launch_only is True"
        return task_id

    # Gateway method: Get the result of a slide detection task by its task ID
    def get_slide_detection_result(self, task_id: str) -> dict | None:
        
        # Make the request to the GraphAI endpoint to get the result of the slide detection task
        task_result = self.get_async_task_result(
            endpoint = "/video/detect_slides",
            task_id  = task_id,
            wait_for_result = False,
            return_status_payload = True
        )

        # If the task result is None, it means the request failed or the slides could not be extracted
        if task_result is None:
            return None

        # Return the task result, which should contain the slide tokens and metadata if the detection was successful
        return task_result
    
    #----------------------------------------------------------------#
    # Gateway method: Download video from URL and create video token #
    #----------------------------------------------------------------#
    def get_video(
        self,
        file_url: str,
        *,
        playlist: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 900,
        launch_only: bool = False,
    ) -> Video | str | None:
        """Download a video from URL and return a Video domain object."""
        login_info = self._ensure_login_info()

        task_result = self._call_async_endpoint(
            endpoint="/video/retrieve_url",
            payload={"url": file_url, "playlist": playlist, "force": force},
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )

        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        must_retry, retry_kwargs = self._requires_media_retry(
            task_result,
            media_label=f"video from {file_url}",
            force=force,
            retry_mode="force",
        )
        if must_retry:
            return self.get_video(
                file_url=file_url,
                playlist=playlist,
                force=retry_kwargs.get("force", force),
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                launch_only=launch_only,
            )

        token = task_result.get("token")
        if token is None:
            return None
        token = str(token)

        token_status = task_result.get("token_status")
        streams = token_status.get("streams") if isinstance(token_status, dict) else None
        first_stream = streams[0] if isinstance(streams, list) and streams else None
        first_stream = first_stream if isinstance(first_stream, dict) else {}

        try:
            fingerprint = self.fingerprint(input=token)
        except Exception:
            fingerprint = None

        codec = first_stream.get("codec_name")
        duration = first_stream.get("duration")
        bit_rate = first_stream.get("bit_rate")
        sample_rate = first_stream.get("sample_rate")
        resolution = first_stream.get("resolution")

        return Video(
            token=token,
            file_url=file_url,
            fingerprint=fingerprint,
            codec=codec,
            duration=duration,
            bit_rate=bit_rate,
            sample_rate=sample_rate,
            resolution=resolution,
        )

    #---------------------------------------------------------------------------------#
    # Gateway method: Fingerprint video (generate unique identifier based on content) #
    #---------------------------------------------------------------------------------#
    def fingerprint(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 900,
        launch_only: bool = False,
    ) -> str | None:
    #---------------------------------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Resolve video token from explicit token param or from input object/string
        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        # Make the request to the GraphAI endpoint to calculate the fingerprint for the given video token
        task_result = self._call_async_endpoint(
            endpoint   = "/video/calculate_fingerprint",
            payload    = {"token": video_token, "force": force},
            login_info = login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries  = max_tries,
            wait_for_result = not launch_only,
        )

        # If the task result is None, it means the request failed or the fingerprint could not be calculated
        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        # Get the fingerprint result from the task result
        result = task_result.get("result")

        # Return the fingerprint as a string if it exists, otherwise return None
        return str(result) if result is not None else None

    #-----------------------------------------------------------------#
    # Gateway method: Extract audio from video and create audio token #
    #-----------------------------------------------------------------#
    def extract_audio(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        recalculate_cached: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 300,
        launch_only: bool = False,
    ) -> Voice | str | None:
        """Extract audio from a video token and return a Voice domain object."""
        login_info = self._ensure_login_info()

        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        task_result = self._call_async_endpoint(
            endpoint="/video/extract_audio",
            payload={
                "token": video_token,
                "recalculate_cached": recalculate_cached,
                "force": force,
            },
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )

        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        must_retry, retry_kwargs = self._requires_media_retry(
            task_result,
            media_label=f"audio from video {video_token}",
            force=force,
            recalculate_cached=recalculate_cached,
            retry_mode="recalculate",
        )
        if must_retry:
            return self.extract_audio(
                video_token=video_token,
                recalculate_cached=retry_kwargs.get("recalculate_cached", recalculate_cached),
                force=force,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                launch_only=launch_only,
            )

        token = task_result.get("token")
        duration = task_result.get("duration")
        if token is None:
            return None

        fingerprint = self._get_voice_gateway().fingerprint(input=str(token))

        return Voice(
            token=str(token),
            fingerprint=fingerprint,
            duration=duration,
        )

    #-------------------------------------------------------------------#
    # Gateway method: Extract slides from video and create slide tokens #
    #-------------------------------------------------------------------#
    def extract_slides(
        self,
        input: Video | str | None = None,
        *,
        video_token: str | None = None,
        recalculate_cached: bool = False,
        force: bool = False,
        max_tries: int = 5,
        max_processing_time_s: int = 6000,
        hash_thresh: float = 0.95,
        multiplier: int = 5,
        default_threshold: float = 0.05,
        include_first: bool = True,
        include_last: bool = True,
        launch_only: bool = False,
    ) -> SlideList | str | None:
        """Extract slide keyframes from a video token and return a SlideList."""
        login_info = self._ensure_login_info()

        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if isinstance(input, Video) else input

        task_result = self._call_async_endpoint(
            endpoint="/video/detect_slides",
            payload={
                "token": video_token,
                "recalculate_cached": recalculate_cached,
                "force": force,
                "parameters": {
                    "hash_thresh": hash_thresh,
                    "multiplier": multiplier,
                    "default_threshold": default_threshold,
                    "include_first": include_first,
                    "include_last": include_last,
                },
            },
            login_info=login_info,
            max_processing_time_s=max_processing_time_s,
            max_tries=max_tries,
            wait_for_result=not launch_only,
        )

        if task_result is None:
            return None

        if launch_only:
            return str(task_result)

        must_retry, retry_kwargs = self._requires_media_retry(
            task_result,
            media_label=f"slides from video {video_token}",
            force=force,
            recalculate_cached=recalculate_cached,
            retry_mode="recalculate",
        )
        if must_retry:
            return self.extract_slides(
                video_token=video_token,
                recalculate_cached=retry_kwargs.get("recalculate_cached", recalculate_cached),
                force=force,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                hash_thresh=hash_thresh,
                multiplier=multiplier,
                default_threshold=default_threshold,
                include_first=include_first,
                include_last=include_last,
                launch_only=launch_only,
            )

        slide_tokens = task_result.get("slide_tokens")
        if not isinstance(slide_tokens, dict):
            return SlideList()

        slide_list = SlideList()
        for slide_number in sorted(slide_tokens.keys(), key=lambda k: int(k)):
            slide_data = slide_tokens[slide_number]
            if not isinstance(slide_data, dict):
                continue
            token = slide_data.get("token")
            timestamp = slide_data.get("timestamp")
            if token is None or timestamp is None:
                continue
            slide_list.item_list.append(
                Slide(
                    token=str(token),
                    timestamp=int(timestamp),
                    fingerprint=None,
                )
            )

        return slide_list

    #----------------------------------------------------------------#
    # Gateway method: Process slides (fingerprint, OCR, translate)   #
    #----------------------------------------------------------------#
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
        """
        Extract slides, fingerprint them, run OCR, detect language, and translate.

        Mirrors the legacy ``process_slides`` orchestration.
        """
        if video_token is None:
            if input is None:
                raise ValueError("Either input or video_token must be provided")
            video_token = input.token if hasattr(input, "token") else input
        if video_token is None:
            raise ValueError("video_token is required")

        slide_list = self.extract_slides(
            video_token=video_token,
            force=force,
            max_tries=max_tries,
            max_processing_time_s=max_processing_time_s,
            **extract_kwargs,
        )
        if slide_list is None or not slide_list.item_list:
            return None, SlideList()

        image_gateway = self._get_image_gateway()
        supported_languages = {"en", "fr", "de", "it"}

        # 1. Fingerprint and OCR each slide.
        ocr_results: list[dict[str, str] | None] = []
        for slide in slide_list.item_list:
            slide.fingerprint = image_gateway.calculate_fingerprint(
                slide.token,
                force=force,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
            )
            ocr = image_gateway.extract_text_from_slide(
                slide.token,
                force=force,
                max_tries=max_tries,
                max_processing_time_s=max_processing_time_s,
                ocr_model=ocr_model,
                google_api_token=google_api_token,
            )
            ocr_results.append(ocr)

        # 2. Determine dominant language unless caller forced one.
        final_language = slides_language
        if final_language is None:
            language_counts: dict[str, int] = {}
            for ocr in ocr_results:
                if ocr is None:
                    continue
                lang = ocr.get("language", "")
                if lang:
                    language_counts[lang] = language_counts.get(lang, 0) + 1
            filtered_counts = {
                lang: count
                for lang, count in language_counts.items()
                if lang in supported_languages
            }
            if filtered_counts:
                final_language = max(filtered_counts, key=filtered_counts.get)
            elif language_counts:
                final_language = max(language_counts, key=language_counts.get)

        # 3. Discard unsupported languages and force English fallback.
        if final_language not in supported_languages:
            final_language = None
        if final_language is None:
            final_language = "en"
            # Re-run OCR with English if the detected language was unsupported or missing.
            if slides_language is None:
                for idx, slide in enumerate(slide_list.item_list):
                    ocr = image_gateway.extract_text_from_slide(
                        slide.token,
                        force=force,
                        max_tries=max_tries,
                        max_processing_time_s=max_processing_time_s,
                        ocr_model=ocr_model,
                        google_api_token=google_api_token,
                    )
                    ocr_results[idx] = ocr

        # 4. Attach text/language to slides.
        for slide, ocr in zip(slide_list.item_list, ocr_results):
            slide.language = final_language
            if ocr is not None:
                slide.text = ocr.get("text", "")
            else:
                slide.text = None

        # 5. Optionally translate slide text to destination languages.
        if destination_languages and translation_gateway is not None:
            self._translate_slide_texts(
                slide_list,
                source_language=final_language,
                destination_languages=destination_languages,
                translation_gateway=translation_gateway,
            )

        return final_language, slide_list

    @staticmethod
    def _translate_slide_texts(
        slide_list: SlideList,
        *,
        source_language: str,
        destination_languages: tuple[str, ...],
        translation_gateway: Any,
    ) -> None:
        """Translate each slide's text into the requested destination languages."""
        source_texts = [slide.text or "" for slide in slide_list.item_list]

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
            for slide, translated_text in zip(slide_list.item_list, translated):
                if translated_text is None:
                    continue
                if slide.translations is None:
                    slide.translations = {}
                slide.translations[target_language] = translated_text.strip()

        for slide in slide_list.item_list:
            if slide.translations is None:
                slide.translations = {}
            if slide.text:
                slide.translations.setdefault(source_language, slide.text)

    #---------------------------------------------------------#
    # Gateway method: Download video file given a video token #
    #---------------------------------------------------------#
    def download_file(
        self,
        token: str,
        file_path: str | Path,
        *,
        max_tries: int = 5,
        timeout: int = 60,
    ) -> Path | None:
    #---------------------------------------------------------#

        # Ensure we have valid login information before making the request
        login_info = self._ensure_login_info()

        # Create output path object
        output_path = Path(file_path)

        # Make the request to the GraphAI endpoint to download the video file corresponding to the given video token and save it to the specified file path
        response = self._request(
            url          = "/video/get_file",
            login_info   = login_info,
            request_func = post,
            headers      = {"Content-Type": "application/json"},
            json         = {"token": token},
            max_tries    = max_tries,
            timeout      = timeout,
        )

        # If the response is None, it means the request failed and the file could not be downloaded
        if response is None:
            return None

        output_path.write_bytes(response.content)

        # Return the output path where the video file was saved
        return output_path
