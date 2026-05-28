# graphregistry/application/factories/fct_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.entities.mdl_lecture import Lecture
from graphregistry.domain.interfaces.gateways.gtw_conceptdet import ConceptDetectionGateway
from graphregistry.entrypoints.mappers import SpecMapper
from graphregistry.entrypoints.schemas import LectureSpec
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
from graphregistry.domain.models.entities.mdl_lecture import Lecture
import rich

# Factory definition
class LectureFactory:
    """Factory for creating Lecture instances, with optional concept detection.
    If a ConceptDetectionGateway is provided and detect_concepts is True, the factory
    will use the gateway to detect concepts from the lecture's raw text and
    populate the detected_concepts field.
    """
    # Class constructor
    def __init__(self, concept_gateway: ConceptDetectionGateway | None = None) -> None:
        self.concept_gateway = concept_gateway

    # Method: Create a Lecture instance with optional concept detection
    def create(self, *, detect_concepts: bool = False, **lecture_data) -> Lecture:

        # Create the Lecture instance from the provided data
        lecture = Lecture(**lecture_data)

        # Initialize the gateway
        gtw_video = GraphAIVideoGateway(debug=False)
        gtw_voice = GraphAIVoiceGateway(debug=False)

        #-------------------------------------------------------#

        # Get video object
        video = gtw_video.get_video(file_url=lecture_data.file_url)

        # Ensure we got a valid video object before proceeding
        assert video is not None, "Failed to get video object"

        #-------------------------------------------------------#

        # Extract audio from video and get audio token
        voice = gtw_video.extract_audio(input=video)

        # Ensure we got a valid voice object before proceeding
        assert voice is not None, "Failed to extract audio from video"

        #-------------------------------------------------------#

        # Extract slides from video and get slide list
        slides = gtw_video.extract_slides(input=video)

        # Ensure we got a valid slide list before proceeding
        assert slides is not None, "Failed to extract slides from video"

        #-------------------------------------------------------#

        # Transcribe audio from video and get transcription results
        transcript = gtw_voice.transcribe_audio(input=voice)

        # Ensure we got valid transcription results before proceeding
        assert transcript is not None, "Failed to transcribe audio from video"

        #-------------------------------------------------------#

        # Create lecture object to hold all the information about the lecture,
        # including the video, audio, slides, and transcript
        lecture.video      = video,
        lecture.voice      = voice,
        lecture.slides     = slides,
        lecture.transcript = transcript
        rich.print(lecture)

        # # If concept detection is not requested, return the lecture as is
        # if not detect_concepts:
        #     return lecture

        # # If the lecture has no raw text, skip concept detection and return the lecture as is
        # if not (lecture.raw_text or "").strip():
        #     return lecture

        # # If concept detection is requested, ensure that a ConceptDetectionGateway is configured
        # if self.concept_gateway is None:
        #     raise ValueError("No concept gateway configured")

        # # Perform concept detection using the gateway and populate the detected_concepts field
        # concepts = self.concept_gateway.detect_concepts(lecture.raw_text or "")
        # lecture.detected_concepts = concepts

        # # Return the lecture with detected concepts
        # return lecture

    # Method: Create a Lecture with the equivalent of SpecMapper.from_lecture_spec(lecture_spec)
    def from_lecture_spec(self, lecture_spec: LectureSpec | dict[str, Any], detect_concepts: bool = False) -> Lecture:
        lecture = SpecMapper.from_lecture_spec(lecture_spec)
        return self.create(
                key             = lecture.key,
                title           = lecture.title,
                text_source     = lecture.text_source,
                raw_text        = lecture.raw_text,
                field_list      = lecture.field_list,
                page_profile    = lecture.page_profile,
                detect_concepts = detect_concepts
            )