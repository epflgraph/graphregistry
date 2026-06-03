# graphregistry/domain/models/tasks/mdl_lectureenrich.py
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field

# Model definition
class LectureConceptTitleList(BaseModel):
    """Model representing a list of concepts detected in a lecture, along with OCR content for each keyframe
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    raw_unrefined_list  : list[str] = Field(default_factory=list)
    ai_refined_list     : list[str] = Field(default_factory=list)
    post_validated_list : list[str] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureConceptTitleList":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class LectureKeyframeOCTandConcepts(BaseModel):
    """Model representing OCR content and detected concepts for a lecture keyframe
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    keyframe_id: str
    ocr_content: str
    concepts: LectureConceptTitleList

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureKeyframeOCTandConcepts":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class LectureKeyframeRefinedConcepts(BaseModel):
    """Model representing refined concepts for a lecture keyframe
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    keyframe_id: str
    refined_concepts: LectureConceptTitleList

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureKeyframeRefinedConcepts":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class LectureEnrichmentTask(BaseModel):
    """Task model representing a lecture enrichment operation
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id: str
    keyframes: list[LectureKeyframeOCTandConcepts] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureEnrichmentTask":
        return cls.model_validate(json_input)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

# Model definition
class LectureEnrichmentResult(BaseModel):
    """Model representing an enrichment result for a lecture
    """
    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id: str
    title: str
    long_description: str
    medium_description: str
    short_description: str
    top_concepts: LectureConceptTitleList
    keyframes: list[LectureKeyframeRefinedConcepts] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "LectureEnrichmentResult":
        return cls.model_validate(input_json)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
