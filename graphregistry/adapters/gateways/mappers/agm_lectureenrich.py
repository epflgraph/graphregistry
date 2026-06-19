# graphregistry/adapters/gateways/genai/mappers/agm_lectureenrich.py
from __future__ import annotations

from typing import Any

from graphregistry.domain.models.tasks.mdl_lectureenrich import (
    LectureEnrichmentTask,
    LectureEnrichmentResult,
    LectureConceptTitleList,
)


class GenAILectureEnrichmentMapper:
    """Maps domain LectureEnrichmentTask to the LLM prompt payload."""

    @staticmethod
    def to_prompt_dict(task: LectureEnrichmentTask) -> dict[str, Any]:
        return {
            "lecture_id": task.lecture_id,
            "keyframes": [
                {
                    "keyframe_id": keyframe.keyframe_id,
                    "ocr_content": keyframe.ocr_content,
                    "candidate_concepts": keyframe.concepts.raw_unrefined_list,
                }
                for keyframe in task.keyframes
            ],
        }

    @staticmethod
    def normalize(result: LectureEnrichmentResult) -> LectureEnrichmentResult:
        result.title = result.title.strip()[:60]

        result.long_description = " ".join(result.long_description.split())
        result.medium_description = " ".join(result.medium_description.split())
        result.short_description = " ".join(result.short_description.split())

        result.top_concepts = GenAILectureEnrichmentMapper._normalize_concept_list(
            result.top_concepts
        )

        for keyframe in result.keyframes:
            keyframe.refined_concepts = (
                GenAILectureEnrichmentMapper._normalize_concept_list(
                    keyframe.refined_concepts
                )
            )

        return result

    @staticmethod
    def _normalize_concept_list(concepts: LectureConceptTitleList) -> LectureConceptTitleList:
        concepts.ai_refined_list = [
            concept.strip()
            for concept in concepts.ai_refined_list
            if concept and concept.strip()
        ]
        return concepts