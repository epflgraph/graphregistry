# graphregistry/adapters/gateways/mappers/agm_lectureenrich.py
"""Maps domain lecture enrichment models to GenAI prompt payloads and back.

Builds the chunked map payload (one chunk of keyframes carrying only their ids
and OCR text; concepts are detected from the OCR alone), the reduce payload
(per-chunk reports only, no raw OCR), and normalizes the LLM responses into
the domain models.
"""
from __future__ import annotations
from typing import Any
from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentChunkResult,
    LectureEnrichmentResult,
    LectureKeyframe,
    LectureKeyframeConceptList,
    LectureKeyframeRefinedConcepts,
)

#==================#
# Class Definition #
#==================#
class GenAILectureEnrichmentMapper:

    # Public Function: Build the chunk map payload for one chunk of keyframes.
    @staticmethod
    def to_chunk_prompt_dict(
        lecture_id      : str,
        keyframes_slice : list[LectureKeyframe],
        chunk_index     : int,
        chunk_count     : int,
    ) -> dict[str, Any]:
        # Keyframes carry only their id and OCR text; the model detects the
        # concepts from the OCR alone, with no candidate anchors.
        return {
            "lecture_id"  : lecture_id,
            "chunk_index" : chunk_index,
            "chunk_count" : chunk_count,
            "keyframes"   : [
                {
                    "keyframe_id" : keyframe.keyframe_id,
                    "ocr_content" : keyframe.ocr_content,
                }
                for keyframe in keyframes_slice
            ],
        }

    # Public Function: Build the reduce payload from the successful chunk results.
    @staticmethod
    def to_reduce_prompt_dict(
        lecture_id    : str,
        chunk_results : dict[int, LectureEnrichmentChunkResult],
    ) -> dict[str, Any]:
        # Only successful chunks contribute reports; the reduce model sees no
        # raw OCR and trusts the per-chunk drafts and concept rankings.
        return {
            "lecture_id"    : lecture_id,
            "chunk_reports" : [
                {
                    "chunk_index"        : chunk_index,
                    "chunk_title_draft"  : chunk_result.chunk_title_draft,
                    "chunk_top_concepts" : chunk_result.chunk_top_concepts,
                }
                for chunk_index, chunk_result in sorted(chunk_results.items())
            ],
        }

    # Public Function: Normalize the reduce response into lecture-level metadata.
    @staticmethod
    def normalize(result: LectureEnrichmentResult) -> LectureEnrichmentResult:
        # Strip the title but keep its natural length; the prompt asks for a
        # concise title and explicitly not to crop longer ones.
        result.title = result.title.strip()

        # Collapse whitespace runs so the descriptions never carry line breaks.
        result.long_description = " ".join(result.long_description.split())
        result.medium_description = " ".join(result.medium_description.split())
        result.short_description = " ".join(result.short_description.split())

        # Clean the lecture-level top keywords so empty entries cannot reach
        # downstream ontology matching.
        result.top_concepts.ai_top_keywords = (
            GenAILectureEnrichmentMapper._normalize_keywords(result.top_concepts.ai_top_keywords)
        )

        # Clean each keyframe's extracted keywords the same way, when present.
        for keyframe in result.keyframes:
            keyframe.refined_concepts.ai_extracted_keywords = (
                GenAILectureEnrichmentMapper._normalize_keywords(
                    keyframe.refined_concepts.ai_extracted_keywords
                )
            )

        # Return the normalized result so callers can chain if needed.
        return result

    # Public Function: Normalize a chunk map result in place before reassembly.
    @staticmethod
    def normalize_chunk_result(result: LectureEnrichmentChunkResult) -> LectureEnrichmentChunkResult:
        # Keep the draft as a one-line summary, without surrounding whitespace.
        result.chunk_title_draft = result.chunk_title_draft.strip()

        # Drop empty concept names so the reduce input only carries real candidates.
        result.chunk_top_concepts = [
            concept.strip()
            for concept in result.chunk_top_concepts
            if concept and concept.strip()
        ]

        # Extracted keyframe concepts go through the same cleanup as final results.
        for keyframe in result.keyframes:
            keyframe.refined_concepts.ai_extracted_keywords = (
                GenAILectureEnrichmentMapper._normalize_keywords(
                    keyframe.refined_concepts.ai_extracted_keywords
                )
            )

        # Return the normalized chunk result so callers can chain if needed.
        return result

    # Public Function: Build the degraded refined concepts of an unprocessed keyframe.
    @staticmethod
    def degraded_refined_concepts(keyframe: LectureKeyframe) -> LectureKeyframeRefinedConcepts:
        # With no candidate concepts in the flow, a keyframe missing from a
        # map result degrades to an EMPTY refined list: it stays present in
        # the mapping (totality) but contributes no concepts until reprocessed.
        return LectureKeyframeRefinedConcepts(
            keyframe_id      = keyframe.keyframe_id,
            refined_concepts = LectureKeyframeConceptList(),
        )

    # Internal Function: Strip entries and drop empties from a keyword list.
    @staticmethod
    def _normalize_keywords(keywords: list[str]) -> list[str]:
        return [
            keyword.strip()
            for keyword in keywords
            if keyword and keyword.strip()
        ]
