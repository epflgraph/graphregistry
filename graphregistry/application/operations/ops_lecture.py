# graphregistry/application/operations/ops_lecture.py
from __future__ import annotations
from typing import Any
from dataclasses import dataclass
from graphregistry.adapters.gateways.graphai.agt_video import GraphAIVideoGateway
from graphregistry.adapters.gateways.graphai.agt_voice import GraphAIVoiceGateway
from graphregistry.application.gateways.types import GatewayDict
from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.common.auxfcn import normalized_levenshtein
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList
from graphregistry.domain.models.entities.mdl_lecture import Lecture
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList, Video, Voice
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureEnrichmentResult
from graphregistry.domain.repositories.rpo_lecture import LectureRepository
from graphregistry.domain.types import ActionSet
import rich, pickle
from loguru import logger as sysmsg

# Class definition
class LectureOperations(NodeOperations):

    # Class constructor
    def __init__(self, repo: LectureRepository, ai_gateways: GatewayDict | None = None) -> None:
        self.repo = repo
        self.ai_gateways = ai_gateways or {}
        self.msg = GraphLogger()

    #===============================#
    # Content processing operations #
    #===============================#

    #-----------------------------------------#
    # METHOD GROUP: Video download operations #
    #-----------------------------------------#

    # Method: Get list of undownloaded lectures, returning a list of NodeKey objects for the undownloaded lectures
    def get_undownloaded(self, limit: int | None = 16) -> NodeKeyList:
        return self.repo.get_undownloaded(limit)

    # Method: Get file URL for a lecture based on the lecture key, returning the file URL as a string
    def get_file_url(self, lecture_key: NodeKey) -> str:
        return self.repo.get_file_url(lecture_key)

    # Method: Launch asynchronous video download and processing task for a lecture, returning the task ID immediately
    def launch_video_download(self, video_url: str) -> str:

        # Get the enrichment gateway
        gtw = self.ai_gateways.get("video_processing")
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Run the video processing gateway to generate the video token
        task_id = gtw.launch_video_download(video_url)

        # Return the task ID immediately without waiting for the processing to complete
        return task_id

    # Method: Save the video download task ID for a lecture (this can be used later to check the status of the download or retrieve the downloaded video)
    def save_video_download_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        return self.repo.save_video_download_task_id(lecture_key, task_id)

    # Method: Get the video download task ID for a lecture (this can be used to check the status of the download or retrieve the downloaded video)
    def get_video_download_task_id(self, lecture_key: NodeKey) -> str:
        return self.repo.get_video_download_task_id(lecture_key)

    # Method: Get list of lectures for which video download tasks have been launched but not yet completed, returning a list of NodeKey objects for the lectures with unfinished video download tasks
    def get_unfinished_video_tasks(self, limit: int | None = 16) -> NodeKeyList:
        return self.repo.get_unfinished_video_tasks(limit)

    # Method: Launch asynchronous video download and processing task for a lecture, returning the task ID immediately
    def get_video_download_result(self, lecture_key: NodeKey) -> dict | None:

        # Get the enrichment gateway
        gtw = self.ai_gateways.get("video_processing")
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Get the video download result from the gateway using the task ID
        result = gtw.get_video_download_result(task_id=self.get_video_download_task_id(lecture_key))

        # Return the task ID immediately without waiting for the processing to complete
        return result

    # Method: Save the video token for a lecture (this can be used later to check the status of the download or retrieve the downloaded video)
    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> NodeKey:
        return self.repo.save_video_token(lecture_key, video_token)

    # Method: Get the video token for a lecture (this can be used to check the status of the download or retrieve the downloaded video)
    def get_video_token(self, lecture_key: NodeKey) -> str:
        return self.repo.get_video_token(lecture_key)

    #-------------------------------------------#
    # METHOD GROUP: Audio extraction operations #
    #-------------------------------------------#

    # Method: Get list of lectures for which video has been downloaded but audio has not yet been extracted
    def get_with_unextracted_audio(self, limit: int | None = 16) -> NodeKeyList:
        return self.repo.get_with_unextracted_audio(limit)

    # Method: Launch asynchronous audio extraction task for a lecture based on the video token
    def launch_audio_extraction(self, video_token: str) -> str:

        # Get the enrichment gateway
        gtw = self.ai_gateways.get("video_processing")
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Run the audio extraction gateway to generate the audio token
        task_id = gtw.launch_audio_extraction(video_token=video_token)

        # Return the audio token immediately without waiting for the processing to complete
        return task_id


    #-------------------------------------------#
    #-------------------------------------------#

    def get_video_tokens_no_slides(self) -> NodeKeyList:
        raise NotImplementedError("Method get_video_tokens_no_slides not implemented")

    def save_slide_tokens(self, lecture_key: NodeKey, slide_tokens: list[str]) -> None:
        raise NotImplementedError("Method save_slide_tokens not implemented")

    def get_slide_tokens_no_ocr(self) -> NodeKeyList:
        raise NotImplementedError("Method get_slide_tokens_no_ocr not implemented")

    def set_slide_ocr_done(self, lecture_key: NodeKey, slide_token: str) -> None:
        raise NotImplementedError("Method set_slide_ocr_done not implemented")

    def get_video_tokens_no_audio(self) -> NodeKeyList:
        raise NotImplementedError("Method get_video_tokens_no_audio not implemented")

    def get_audio_tokens_no_transcript(self) -> NodeKeyList:
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
            transcript_token = gtw_audio.transcribe_audio(video_token=video_token, launch_only=True)
            self.save_transcript_token(lecture_key, transcript_token)

        return None

    #=====================================#
    # Lecture field enrichment operations #
    #=====================================#

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
        if task is None:
            return None

        # Run the enrichment task through the gateway to get the enrichment result
        result = gtw.enrich(task, verbose=True)

        # # Load enrichment result from pickle for testing
        # with open(f"enrichment_result_{lecture_id}.pkl", "rb") as f:
        #     result = pickle.load(f)

        if result is None:
            sysmsg.warning("Skipping lecture_id={} because enrichment produced no result.", lecture_id)
            return None

        # Print status
        self.msg.enriched(NodeKey(
            institution_id = 'EPFL',
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

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
            post_validated_list = ScoredConceptList()

            # Initialise cache for wiki search results to avoid redundant calls for the same concept
            # (this shortens the processing time by half)
            wiki_search_cache: dict[str, list[dict[str, Any]]] = {}

            # Loop over AI-refined concepts and check if they are valid Wikipedia concepts using the concept detection gateway
            for ai_refined_concept in ai_refined_list:

                # Execute wiki search for the AI-refined concept
                if ai_refined_concept in wiki_search_cache:
                    wiki_suggestions = wiki_search_cache[ai_refined_concept]
                else:
                    wiki_suggestions = gtw_conceptdet.wiki_search(search_term=ai_refined_concept or "")
                    wiki_search_cache[ai_refined_concept] = wiki_suggestions

                # Loop over wiki search suggestions and calculate similarity with the AI-refined concept,
                # keeping those above a certain similarity threshold (e.g., 0.75)
                for suggestion in wiki_suggestions:

                    # Calculate similarity between the AI-refined concept and the wiki search suggestions
                    similarity = normalized_levenshtein(ai_refined_concept or "", suggestion['concept_name'])

                    # If the similarity is above the threshold, add the suggestion to the post-validation list
                    if similarity >= 0.75:
                        post_validated_list.item_list.append(
                            ScoredConcept(
                                concept = Concept(
                                    id   = str(suggestion['concept_id']),
                                    name = str(suggestion['concept_name'])
                                ),
                                score = 1
                            )
                        )

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

    # Method: Save enrichment result for a lecture to persistence and return the saved lecture key
    def save_enrichment(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        return self.repo.save_enrichment_result(result, actions)
