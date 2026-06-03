# graphregistry/application/operations/ops_lecture.py
from __future__ import annotations
from typing import Any
from dataclasses import dataclass
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_lecture import Lecture
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList, Video, Voice
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureEnrichmentResult
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.domain.types import ActionSet
from graphregistry.common.auxfcn import normalized_levenshtein
import rich, pickle

# Class definition
class LectureOperations(NodeOperations):

    # Class constructor
    def __init__(self, repo: LectureRepository, ai_gateways: GatewayDict | None = None) -> None:
        self.repo = repo
        self.ai_gateways = ai_gateways or {}
        self.msg = GraphLogger()

    #-------------------------------#
    # Content processing operations #
    #-------------------------------#

    def get_undownloaded(self) -> list[NodeKey]:
        raise NotImplementedError("Method get_undownloaded not implemented")

    def get_file_url(self, lecture_key: NodeKey) -> str:
        raise NotImplementedError("Method get_file_url not implemented")

    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> None:
        raise NotImplementedError("Method save_video_token not implemented")

    def get_video_tokens_no_slides(self) -> list[str]:
        raise NotImplementedError("Method get_video_tokens_no_slides not implemented")

    def save_slide_tokens(self, lecture_key: NodeKey, slide_tokens: list[str]) -> None:
        raise NotImplementedError("Method save_slide_tokens not implemented")

    def get_slide_tokens_no_ocr(self) -> list[str]:
        raise NotImplementedError("Method get_slide_tokens_no_ocr not implemented")

    def set_slide_ocr_done(self, lecture_key: NodeKey, slide_token: str) -> None:
        raise NotImplementedError("Method set_slide_ocr_done not implemented")

    def get_video_tokens_no_audio(self) -> list[str]:
        raise NotImplementedError("Method get_video_tokens_no_audio not implemented")

    def get_audio_tokens_no_transcript(self) -> list[str]:
        raise NotImplementedError("Method get_audio_tokens_no_transcript not implemented")

    def save_transcript_token(self, lecture_key: NodeKey, transcript_token: str) -> None:
        raise NotImplementedError("Method save_transcript_token not implemented")

    def run_processing_iteration(self) -> dict[str, int] | None:

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

        return None

    #-------------------------------------#
    # Lecture field enrichment operations #
    #-------------------------------------#

    # # Method: Enrich lectures with missing descriptions and refined concepts
    # def enrich(self, lectures: Lecture | LectureList) -> Lecture | LectureList:

    #     # Handle both single Lecture input and list of Lectures input
    #     single_input = isinstance(lectures, Lecture)
    #     lecture_list = [lectures] if single_input else list(lectures)

    #     # Get the enrichment gateway
    #     gtw = self.ai_gateways.get("lecture_enrichment")
    #     if gtw is None:
    #         raise ValueError("Missing gateway: lecture_enrichment")

    #     # Initialize list to hold enriched lectures
    #     enriched: list[Lecture] = []

    #     # Loop over lectures and enrich each one
    #     for lecture in lecture_list:

    #         # Normalize possible (key, lecture) tuple entries
    #         lecture_obj = lecture[1] if isinstance(lecture, tuple) else lecture

    #         # Get the enrichment task for the lecture
    #         task = self.repo.get_enrichment_task(lecture_obj.node.key)

    #         # Verify that the enrichment task was found
    #         if task is None:
    #             print(f"Enrichment task not found for lecture key: {lecture_obj.node.key}")
    #             continue

    #         # Run the enrichment task through the gateway to get the enrichment result
    #         result = gtw.enrich(task)

    #         # Save the enrichment result back to the repository and get the saved lecture key
    #         saved_lecture_key = self.repo.save_enrichment_result(result)

    #         # Load the saved lecture and add it to the enriched list
    #         saved_lecture = self.repo.get(saved_lecture_key)
    #         if saved_lecture is None:
    #             raise ValueError(f"Saved lecture not found for key: {saved_lecture_key}")
    #         enriched.append(saved_lecture)

    #     # Return the enriched lecture(s) in the same format as the input (single Lecture or LectureList)
    #     return enriched[0] if single_input else LectureList(item_list=enriched)


    # Method: Enrich one lecture by lecture_id
    def enrich(self, lecture_id: str) -> LectureEnrichmentResult | None:

        # Get the enrichment gateway
        gtw = self.ai_gateways.get("lecture_enrichment")
        if gtw is None:
            raise ValueError("Missing gateway: lecture_enrichment")

        # Get the enrichment task for the lecture
        task = self.repo.get_enrichment_task(NodeKey(
            institution_id = 'EPFL',
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # Verify that the enrichment task was found
        assert isinstance(task, LectureEnrichmentTask), \
            f"Expected enrichment task for lecture_id {lecture_id}, but got {task}"

        # Run the enrichment task through the gateway to get the enrichment result
        result = gtw.enrich(task)

        # Print status
        self.msg.enriched(NodeKey(
            institution_id = 'EPFL',
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # Check that the result is of the expected type
        assert isinstance(result, LectureEnrichmentResult), \
            f"Expected enrichment result for lecture_id {lecture_id}, but got {result}"

        # # Load enrichment result from pickle for testing
        # with open(f"enrichment_result_{lecture_id}.pkl", "rb") as f:
        #     result = pickle.load(f)

        # # Save to pickle
        # with open(f"enrichment_result_{lecture_id}.pkl", "wb") as f:
        #     pickle.dump(result, f)

        #------------------------------#
        # Concept list post-validation #
        #------------------------------#

        # Loop over keyframes and remove those that do not have any AI-refined concepts
        for k in [-1] + list(range(len(result.keyframes))):

            # Loop over concepts and remove those that are not Wikipedia pages
            # For the keyframe-level concepts (k=-1), we check the top concepts,
            # while for the keyframe-specific concepts (k>=0) we check the refined concepts for each keyframe
            if k == -1:
                ai_refined_list = result.top_concepts.ai_refined_list
            else:
                ai_refined_list = result.keyframes[k].refined_concepts.ai_refined_list

            # Get concept detection gateway
            gtw_conceptdet = self.ai_gateways.get("concept_detection")
            if gtw_conceptdet is None:
                raise ValueError("Missing gateway: concept_detection")

            # Initialise post-validation list
            post_validated_list = []

            # Loop over AI-refined concepts and check if they are valid Wikipedia concepts using the concept detection gateway
            for ai_refined_concept in ai_refined_list:

                # Execute wiki search for the AI-refined concept
                wiki_suggestions = gtw_conceptdet.wiki_search(search_term=ai_refined_concept)

                # Loop over wiki search suggestions and calculate similarity with the AI-refined concept,
                # keeping those above a certain similarity threshold (e.g., 0.75)
                for suggestion in wiki_suggestions:

                    # Calculate similarity between the AI-refined concept and the wiki search suggestions
                    similarity = normalized_levenshtein(ai_refined_concept, suggestion['concept_name'])

                    # If the similarity is above the threshold, add the suggestion to the post-validation list
                    if similarity >= 0.75:
                        post_validated_list.append(suggestion['concept_name'])

            # Assign the post-validated list to the result (for now, we overwrite the AI-refined list, but in the future we could keep both)
            if k == -1:
                result.top_concepts.post_validated_list = post_validated_list
            else:
                result.keyframes[k].refined_concepts.post_validated_list = post_validated_list

        # Print status
        self.msg.concepts_validated(NodeKey(
            institution_id = 'EPFL',
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # Return None for now, as the enrichment result saving and lecture updating is not yet implemented
        return result
