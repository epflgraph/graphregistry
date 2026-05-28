# graphregistry/application/operations/ops_lecture.py
from __future__ import annotations
from typing import Any
from dataclasses import dataclass
from graphregistry.domain.types import ActionSet
from graphregistry.domain.interfaces.repositories.rpo_lecture import LectureRepository
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_lecture import Lecture, NodeKey, LectureList, Video
from graphregistry.domain.models.entities.mdl_lecture import Lecture
from graphregistry.domain.models.tasks.mdl_conceptdet import ConceptDetectionResultList
from graphregistry.domain.interfaces.gateways.types import GatewayDict
from graphregistry.common.logger import GraphLogger


from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
from graphregistry.domain.models.entities.mdl_lecture import Lecture
import rich

# Class definition
class LectureOperations:

    # Class constructor
    def __init__(self, repo: LectureRepository, ai_gateways: GatewayDict | None = None) -> None:
        self.repo = repo
        self.ai_gateways = ai_gateways or {}
        self.msg = GraphLogger()

    #-------------------------------------------#
    # Basic Lecture CRUD/persistence operations #
    #-------------------------------------------#

    # Method: List lectures by object type and optional ID pattern, returning a list of (object_type, id, title) tuples
    def list(self, object_type: str, id_pattern: str | None = None) -> list[tuple[str, str, str]]:
        return self.repo.list(object_type=object_type, id_pattern=id_pattern)

    # Method: Check if a lecture exists by its key
    def exists(self, key: NodeKey) -> bool:
        return self.repo.exists(key)

    # Method: Check if multiple lectures exist by their keys, returning a list of booleans corresponding to the input keys
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        return self.repo.exists_many(key_list)

    # Method: Get a lecture by its key, returning the Lecture instance or None if not found
    def get(self, key: NodeKey) -> Lecture | None:
        return self.repo.get(key)

    # Method: Get multiple lectures by their keys, returning a list of Lecture instances corresponding to the input keys (with None for keys that are not found)
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> LectureList:
        return self.repo.get_many(key_list)

    # Method: Save a lecture, with optional actions to perform (default is ('commit',)), returning the saved Lecture instance
    def save(self, lecture: Lecture, actions: ActionSet = ('commit',)) -> Lecture:
        return self.repo.save(lecture, actions=actions)

    # Method: Save multiple lectures, with optional actions to perform (default is ('commit',)), returning a list of the saved Lecture instances
    def save_many(self, lecture_list: LectureList | list[Lecture], actions: ActionSet = ('commit',)) -> LectureList:
        return self.repo.save_many(lecture_list, actions=actions)

    # Method: Delete a lecture by its key, with optional actions to perform (default is ('commit',)), returning True if the lecture was deleted, False if it was not found, or None if the deletion was not performed due to the actions
    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:
        return self.repo.delete(key, actions=actions)

    # Method: Delete multiple lectures by their keys, with optional actions to perform (default is ('commit',)), returning a list of booleans corresponding to the input keys indicating whether each lecture was deleted (True), not found (False), or not deleted due to the actions (None)
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        return self.repo.delete_many(key_list, actions=actions)

    #-------------------------------#
    # Content processing operations #
    #-------------------------------#

    def run_processing_iteration(self) -> dict[str, int]:

        # Initialize the gateway (TODO: is there an abstraction of these?)
        gtw_video = GraphAIVideoGateway(debug=False)
        gtw_voice = GraphAIVoiceGateway(debug=False)

        #----------------#
        # Video download #
        #----------------#

        # Get lecture keys for which video has not been downloaded
        lecture_keys_undownloaded = self.get_undownloaded()

        # Loop over undownloaded videos and launch download tasks
        for lecture_key in lecture_keys_undownloaded:
            file_url = self.get_file_url(lecture_key)
            video_token = gtw_video.get_video(file_url=file_url, launch_only=True)
            self.save_video_token(lecture_key, video_token)

        #------------------#
        # Slide extraction #
        #------------------#

        # Get video tokens for which slides have not been extracted
        video_tokens_no_slides = self.get_video_tokens_no_slides()

        # Loop over video tokens and launch slide extraction tasks
        for video_token in video_tokens_no_slides:
            slide_tokens = gtw_video.extract_slides(video_token=video_token, launch_only=True)
            self.save_slide_tokens(lecture_key, slide_tokens)

        #-----------#
        # Slide OCR #
        #-----------#

        # Get slide tokens for which slides have not been extracted
        slide_tokens_no_ocr = self.get_slide_tokens_no_ocr()

        # Loop over video tokens and launch slide extraction tasks
        for slide_token in slide_tokens_no_ocr:
            gtw_slide.extract_text(slide_token=slide_token, launch_only=True)
            self.set_slide_ocr_done(lecture_key, slide_token)

        #------------------#
        # Audio extraction #
        #------------------#

        # Get video tokens for which audio has not been extracted
        video_tokens_no_audio = self.get_video_tokens_no_audio()

        # Loop over video tokens and launch slide extraction tasks
        for video_token in video_tokens_no_audio:
            audio_token = gtw_video.extract_audio(video_token=video_token, launch_only=True)

        #---------------------#
        # Audio transcription #
        #---------------------#

        # Get video tokens for which audio has not been extracted
        audio_tokens_no_transcript = self.get_audio_tokens_no_transcript()

        # Loop over audio tokens and launch audio transcription tasks
        for video_token in video_tokens_no_audio:
            transcript_token = gtw_voice.transcribe_audio(video_token=video_token, launch_only=True)
            self.save_transcript_token(lecture_key, transcript_token)


    #-----------------------------------------------------#
    # Lecture diagnostics and special get/save operations #
    #-----------------------------------------------------#

    # Method: Check if a lecture has detected concepts by its key or Lecture instance
    def has_concepts(self, lecture_or_key: Lecture | NodeKey) -> bool:
        if isinstance(lecture_or_key, NodeKey):
            lecture = self.repo.get(lecture_or_key)
            if not lecture:
                raise ValueError(f"Lecture with key {lecture_or_key} not found")
        else:
            lecture = lecture_or_key
        if not lecture.detected_concepts:
            return False
        elif not lecture.detected_concepts.item_list:
            return False
        elif len(lecture.detected_concepts.item_list) == 0:
            return False
        else:
            return True

    # Method: Get lectures that have no detected concepts, optionally filtered by object type and ID pattern
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> LectureList:
        return self.repo.get_with_no_concepts(object_type=object_type, id_pattern=id_pattern)

    #----------------------------------#
    # Lecture field enrichment operations #
    #----------------------------------#

    # Method: Enrich a lecture with detected concepts using the concept detection gateway, returning the enriched Lecture instance
    def enrich_with_concepts(self, lectures: Lecture | LectureList) -> Lecture | LectureList:

        # Get gateway for concept detection
        gateway = self.ai_gateways.get("concept_detection")
        if not gateway:
            raise ValueError("Concept detection gateway not configured")

        # Perform concept detection using the gateway and populate the detected_concepts field
        if isinstance(lectures, LectureList):
            for lecture in lectures.item_list:
                concepts = gateway.detect_concepts(lecture.raw_text or "")
                lecture.detected_concepts = concepts
                self.msg.concepts_detected(lecture.key)
        else:
            concepts = gateway.detect_concepts(lectures.raw_text or "")
            lectures.detected_concepts = concepts
            self.msg.concepts_detected(lectures.key)

        # Return the lecture(s) with detected concepts
        return lectures

