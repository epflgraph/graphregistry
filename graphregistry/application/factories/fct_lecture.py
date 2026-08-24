# graphregistry/application/factories/fct_lecture.py
from __future__ import annotations
from typing import Any

from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.gateways.prt_video import VideoProcessingGateway
from graphregistry.application.ports.gateways.prt_voice import VoiceProcessingGateway
from graphregistry.domain.models.entities.mdl_lecture import (
    Lecture,
    SlideList,
    Transcript,
    Video,
    Voice,
)


# Factory definition
class LectureFactory:
    """Factory for creating Lecture instances from video/voice processing gateways.

    The factory depends on the ``VideoProcessingGateway`` and ``VoiceProcessingGateway"
    ports (defined in the application layer) and uses concrete adapters only through
    dependency injection. This keeps the application layer independent of adapter
    implementations.
    """

    # Class constructor
    def __init__(
        self,
        video_gateway: VideoProcessingGateway,
        voice_gateway: VoiceProcessingGateway,
        concept_gateway: ConceptDetectionGateway | None = None,
    ) -> None:
        self.video_gateway = video_gateway
        self.voice_gateway = voice_gateway
        self.concept_gateway = concept_gateway

    # Method: Create a Lecture instance from a video file URL
    def create(self, *, file_url: str, **lecture_data: Any) -> Lecture:

        # Create the Lecture instance from the provided data
        lecture = Lecture(**lecture_data)

        # Download/process video to obtain a video object
        video_result = self.video_gateway.get_video(file_url=file_url)
        if video_result is None or isinstance(video_result, str):
            raise ValueError(f"Failed to get video object for URL: {file_url}")
        video = video_result

        # Extract audio from video to obtain a voice object
        voice_result = self.video_gateway.extract_audio(input=video)
        if voice_result is None or isinstance(voice_result, str):
            raise ValueError(f"Failed to extract audio from video token: {video.token}")
        voice = voice_result

        # Extract slides from video
        slides_result = self.video_gateway.extract_slides(input=video)
        if slides_result is None or isinstance(slides_result, str):
            raise ValueError(f"Failed to extract slides from video token: {video.token}")
        slides = slides_result

        # Transcribe audio from voice
        transcript_result = self.voice_gateway.transcribe_audio(input=voice)
        if transcript_result is None or isinstance(transcript_result, str):
            raise ValueError(f"Failed to transcribe audio from voice token: {voice.token}")
        transcript = transcript_result

        # Attach processed media to the lecture
        lecture.video = video
        lecture.voice = voice
        lecture.slides = slides
        lecture.transcript = transcript

        return lecture
