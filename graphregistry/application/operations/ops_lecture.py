# graphregistry/application/operations/ops_lecture.py
from __future__ import annotations
from typing import Any
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.gateways.prt_lectureenrich import LectureEnrichmentGateway
from graphregistry.application.ports.gateways.prt_video import VideoProcessingGateway
from graphregistry.common.auxfcn import normalized_levenshtein
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList
from graphregistry.domain.models.entities.mdl_lecture import Lecture, LectureList, Video, Voice
from graphregistry.domain.models.tasks.mdl_lectureenrich import LectureEnrichmentTask, LectureEnrichmentResult
from graphregistry.application.ports.repositories.prt_lecture import LectureRepository
from graphregistry.application.ports.repositories.prt_lecture_processing import LectureProcessingStatePort
from graphregistry.domain.types import ActionSet
from loguru import logger as sysmsg

# Class definition
class LectureOperations:

    # Class constructor
    def __init__(
        self,
        repo: LectureRepository,
        processing_state: LectureProcessingStatePort | None = None,
        *,
        video_processing_gateway: VideoProcessingGateway | None = None,
        concept_detection_gateway: ConceptDetectionGateway | None = None,
        lecture_enrichment_gateway: LectureEnrichmentGateway | None = None,
    ) -> None:
        self.repo = repo
        if processing_state is not None:
            self.processing_state = processing_state
        elif isinstance(repo, LectureProcessingStatePort):
            self.processing_state = repo
        else:
            raise TypeError(
                "repo must implement LectureProcessingStatePort when processing_state is omitted"
            )
        self.video_processing_gateway = video_processing_gateway
        self.concept_detection_gateway = concept_detection_gateway
        self.lecture_enrichment_gateway = lecture_enrichment_gateway
        self.msg = GraphLogger()

    #===============================#
    # Content processing operations #
    #===============================#

    #-----------------------------------------#
    # METHOD GROUP: Video download operations #
    #-----------------------------------------#

    # Method: Get list of undownloaded lectures, returning a list of NodeKey objects for the undownloaded lectures
    def get_undownloaded(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_undownloaded(limit)

    # Method: Get file URL for a lecture based on the lecture key, returning the file URL as a string
    def get_file_url(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_file_url(lecture_key)

    # Method: Launch asynchronous video download and processing task for a lecture, returning the task ID immediately
    def launch_video_download(self, video_url: str, no_cache: bool = False) -> str:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Run the video processing gateway to generate the video token
        task_id = gtw.launch_video_download(video_url, no_cache=no_cache)

        # Return the task ID immediately without waiting for the processing to complete
        return task_id

    # Method: Save the video download task ID for a lecture (this can be used later to check the status of the download or retrieve the downloaded video)
    def save_video_download_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        return self.processing_state.save_video_download_task_id(lecture_key, task_id)

    # Method: Get the video download task ID for a lecture (this can be used to check the status of the download or retrieve the downloaded video)
    def get_video_download_task_id(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_video_download_task_id(lecture_key)

    # Method: Get list of lectures for which video download tasks have been launched but not yet completed, returning a list of NodeKey objects for the lectures with unfinished video download tasks
    def get_unfinished_video_download_tasks(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_unfinished_video_download_tasks(limit)

    # Method: Launch asynchronous video download and processing task for a lecture, returning the task ID immediately
    def get_video_download_result(self, lecture_key: NodeKey) -> dict | None:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Get the video download result from the gateway using the task ID
        result = gtw.get_video_download_result(task_id=self.get_video_download_task_id(lecture_key))

        # Return the task ID immediately without waiting for the processing to complete
        return result

    # Method: Save the video token for a lecture (this can be used later to check the status of the download or retrieve the downloaded video)
    def save_video_token(self, lecture_key: NodeKey, video_token: str) -> NodeKey:
        return self.processing_state.save_video_token(lecture_key, video_token)

    # Method: Get the video token for a lecture (this can be used to check the status of the download or retrieve the downloaded video)
    def get_video_token(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_video_token(lecture_key)

    #-------------------------------------------#
    # METHOD GROUP: Audio extraction operations #
    #-------------------------------------------#

    # Method: Get list of lectures for which video has been downloaded but audio has not yet been extracted
    def get_with_unextracted_audio(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_with_unextracted_audio(limit)

    # Method: Launch asynchronous audio extraction task for a lecture based on the video token
    def launch_audio_extraction(self, video_token: str, no_cache: bool = False) -> str:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Run the video processing gateway to generate the audio token
        task_id = gtw.launch_audio_extraction(video_token=video_token, no_cache=no_cache)

        # Return the audio token immediately without waiting for the processing to complete
        return task_id

    # Method: Save the audio extraction task ID for a lecture (this can be used later to check the status of the extraction or retrieve the extracted audio)
    def save_audio_extraction_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        return self.processing_state.save_audio_extraction_task_id(lecture_key, task_id)

    # Method: Get the audio extraction task ID for a lecture (this can be used to check the status of the extraction or retrieve the extracted audio)
    def get_audio_extraction_task_id(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_audio_extraction_task_id(lecture_key)

    # Method: Get list of lectures for which audio extraction tasks have been launched but not yet completed, returning a list of NodeKey objects for the lectures with unfinished audio extraction tasks
    def get_unfinished_audio_extraction_tasks(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_unfinished_audio_extraction_tasks(limit)

    # Method: Get the audio extraction result for a lecture using the audio extraction task ID (this can be used to retrieve the extracted audio once the extraction is complete)
    def get_audio_extraction_result(self, lecture_key: NodeKey) -> dict | None:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Get the audio extraction result from the gateway using the task ID
        result = gtw.get_audio_extraction_result(task_id=self.get_audio_extraction_task_id(lecture_key))

        # Return the task ID immediately without waiting for the processing to complete
        return result

    # Method: Save the audio token for a lecture (this can be used later to check the status of the extraction or retrieve the extracted audio)
    def save_audio_token(self, lecture_key: NodeKey, audio_token: str) -> NodeKey:
        return self.processing_state.save_audio_token(lecture_key, audio_token)

    # Method: Get the audio token for a lecture (this can be used to check the status of the extraction or retrieve the extracted audio)
    def get_audio_token(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_audio_token(lecture_key)

    #------------------------------------------#
    # METHOD GROUP: Slide detection operations #
    #------------------------------------------#

    # Method: Get list of lectures for which video has been downloaded but slides have not yet been detected
    def get_with_undetected_slides(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_with_undetected_slides(limit)

    # Method: Launch asynchronous slide detection task for a lecture based on the video token, returning the list of slide tokens immediately
    def launch_slide_detection(self, video_token: str, no_cache: bool = False) -> str:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Run the video processing gateway to generate the slide tokens
        task_id = gtw.launch_slide_detection(video_token=video_token, no_cache=no_cache)

        # Return the slide tokens immediately without waiting for the processing to complete
        return task_id

    # Method: Save the slide detection task ID for a lecture (this can be used later to check the status of the detection or retrieve the detected slides)
    def save_slide_detection_task_id(self, lecture_key: NodeKey, task_id: str) -> NodeKey:
        return self.processing_state.save_slide_detection_task_id(lecture_key, task_id)

    # Method: Get the slide detection task ID for a lecture (this can be used to check the status of the detection or retrieve the detected slides)
    def get_slide_detection_task_id(self, lecture_key: NodeKey) -> str:
        return self.processing_state.get_slide_detection_task_id(lecture_key)

    # Method: Get list of lectures for which slide detection tasks have been launched but not yet completed, returning a list of NodeKey objects for the lectures with unfinished slide detection tasks
    def get_unfinished_slide_detection_tasks(self, limit: int | None = 16) -> NodeKeyList:
        return self.processing_state.get_unfinished_slide_detection_tasks(limit)

    # Method: Get the slide detection result for a lecture using the slide detection task ID (this can be used to retrieve the detected slides once the detection is complete)
    def get_slide_detection_result(self, lecture_key: NodeKey) -> dict | None:

        # Get the video processing gateway
        gtw = self.video_processing_gateway
        if gtw is None:
            raise ValueError("Missing gateway: video_processing")

        # Get the slide detection result from the gateway using the task ID
        result = gtw.get_slide_detection_result(task_id=self.get_slide_detection_task_id(lecture_key))

        # Return the task ID immediately without waiting for the processing to complete
        return result

    # Method: Save the slide tokens for a lecture (this can be used later to check the status of the detection or retrieve the detected slides)
    def save_slide_tokens(self, lecture_key: NodeKey, slide_num_and_tokens: list[tuple[int, str]]) -> NodeKey:
        return self.processing_state.save_slide_tokens(lecture_key, slide_num_and_tokens)

    # Method: Get the slide tokens for a lecture (this can be used to check the status of the detection or retrieve the detected slides)
    def get_slide_tokens(self, lecture_key: NodeKey) -> list[str]:
        return self.processing_state.get_slide_tokens(lecture_key)

    #=====================================#
    # Lecture field enrichment operations #
    #=====================================#

    # Method: Enrich one lecture by lecture_id
    def enrich(self, lecture_id: str, verbose: bool = False) -> LectureEnrichmentResult | None:

        # Get the lecture enrichment gateway
        gtw = self.lecture_enrichment_gateway
        if gtw is None:
            raise ValueError("Missing gateway: lecture_enrichment")

        # Get the enrichment task for the lecture
        task = self.repo.get_enrichment_task(NodeKey(
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # Verify that the enrichment task was found
        if task is None:
            return None

        # Run the enrichment task through the gateway to get the enrichment result,
        # passing verbose through so debugging can print the full LLM prompts.
        result = gtw.enrich(task, verbose=verbose)

        # # Load enrichment result from pickle for testing
        # with open(f"enrichment_result_{lecture_id}.pkl", "rb") as f:
        #     result = pickle.load(f)

        if result is None:
            sysmsg.warning("Skipping lecture_id={} because enrichment produced no result.", lecture_id)
            return None

        # Print status
        self.msg.enriched(NodeKey(
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # # Save to pickle
        # with open(f"enrichment_result_{lecture_id}.pkl", "wb") as f:
        #     pickle.dump(result, f)

        #---------------------------------------#
        # Ontology strict and fuzzy sub-algorithms #
        #---------------------------------------#

        # Loop over the concept lists: the lecture-level top concepts (k=-1)
        # and the extracted concepts of every keyframe (k>=0).
        for k in [-1] + list(range(len(result.keyframes))):

            # Select the concept list to fill: the lecture-level top concepts
            # for k=-1, the keyframe-specific extracted concepts for k>=0.
            if k == -1:
                concept_list = result.top_concepts
            else:
                concept_list = result.keyframes[k].refined_concepts
            ai_keywords = concept_list.ai_top_keywords if k == -1 else concept_list.ai_extracted_keywords

            # Get concept detection gateway
            gtw_conceptdet = self.concept_detection_gateway
            if gtw_conceptdet is None:
                raise ValueError("Missing gateway: concept_detection")

            # Ontology STRICT matching: wiki search per keyword, keeping only
            # unambiguous semantic matches (similarity >= 0.75), all scored 1.
            ontology_strict_list = ScoredConceptList()

            # Initialise cache for wiki search results to avoid redundant calls for the same concept
            # (this shortens the processing time by half)
            wiki_search_cache: dict[str, list[dict[str, Any]]] = {}

            # Loop over the AI keywords and check if they are valid Wikipedia concepts using the concept detection gateway
            for keyword in ai_keywords:

                # Execute wiki search for the keyword
                if keyword in wiki_search_cache:
                    wiki_suggestions = wiki_search_cache[keyword]
                else:
                    wiki_suggestions = gtw_conceptdet.wiki_search(search_term=keyword or "")
                    wiki_search_cache[keyword] = wiki_suggestions

                # Loop over wiki search suggestions and calculate similarity with the keyword,
                # keeping those above a certain similarity threshold (e.g., 0.75)
                for suggestion in wiki_suggestions:

                    # Calculate similarity between the keyword and the wiki search suggestions
                    similarity = normalized_levenshtein(keyword or "", suggestion['concept_name'])

                    # If the similarity is above the threshold, add the suggestion to the strict list
                    if similarity >= 0.75:
                        ontology_strict_list.item_list.append(
                            ScoredConcept(
                                concept = Concept(
                                    id   = str(suggestion['concept_id']),
                                    name = str(suggestion['concept_name'])
                                ),
                                score = 1
                            )
                        )

            # Assign the strict list to the result
            concept_list.ontology_strict_list = ontology_strict_list

            # Ontology FUZZY matching: run the keywords through concept detection
            # (wikify) and keep the suggested scores, minus the strict set.
            ontology_fuzzy_list = ScoredConceptList()
            if ai_keywords:
                fuzzy_suggestions = gtw_conceptdet.detect_concepts(text=ai_keywords)
                strict_ids = {scored.concept.id for scored in ontology_strict_list.item_list}
                ontology_fuzzy_list.item_list = [
                    scored
                    for scored in fuzzy_suggestions.item_list
                    if scored.concept.id not in strict_ids
                ]

            # Assign the fuzzy list to the result
            concept_list.ontology_fuzzy_list = ontology_fuzzy_list

        # Print status
        self.msg.concepts_validated(NodeKey(
            object_type    = 'Lecture',
            object_id      = lecture_id,
        ))

        # Return None for now, as the enrichment result saving and lecture updating is not yet implemented
        return result

    # Method: Save enrichment result for a lecture to persistence and return the saved lecture key
    def save_enrichment(self, result: LectureEnrichmentResult, actions: ActionSet = ("commit",)) -> NodeKey:
        return self.repo.save_enrichment_result(result, actions)
