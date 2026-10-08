# graphregistry/domain/models/tasks/mdl_lectureenrich.py
"""Domain models for the lecture enrichment task, its chunked map results and the final result."""
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList

#==================#
# Class Definition #
#==================#
class LectureTopConceptList(BaseModel):
    """Lecture-level top concepts from the reduce call.

    ai_top_keywords holds the LLM's keyword ranking; ontology_strict_list and
    ontology_fuzzy_list are filled downstream: strict, unambiguous semantic
    matches against ontology concepts (all scored 1), and the fuzzy wikify
    suggestions (with their suggested scores) minus the strict set.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    ai_top_keywords      : list[str] = Field(default_factory=list)
    ontology_strict_list : ScoredConceptList = Field(default_factory=ScoredConceptList)
    ontology_fuzzy_list  : ScoredConceptList = Field(default_factory=ScoredConceptList)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the top concept list from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureTopConceptList":
        return cls.model_validate(json_input)

    # Public Method: Serialize the top concept list to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureKeyframeConceptList(BaseModel):
    """Keyframe-level concepts from the map calls.

    ai_extracted_keywords holds the concepts the model extracted from the
    keyframe's OCR; ontology_strict_list and ontology_fuzzy_list are filled
    downstream with the same strict/fuzzy semantics as the lecture level.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    ai_extracted_keywords : list[str] = Field(default_factory=list)
    ontology_strict_list  : ScoredConceptList = Field(default_factory=ScoredConceptList)
    ontology_fuzzy_list   : ScoredConceptList = Field(default_factory=ScoredConceptList)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the keyframe concept list from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureKeyframeConceptList":
        return cls.model_validate(json_input)

    # Public Method: Serialize the keyframe concept list to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureKeyframe(BaseModel):
    """One lecture keyframe: its identifier and OCR text."""

    #--------------------#
    # Internal variables #
    #--------------------#
    keyframe_id: str
    ocr_content: str

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the keyframe from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureKeyframe":
        return cls.model_validate(json_input)

    # Public Method: Serialize the keyframe to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureKeyframeRefinedConcepts(BaseModel):
    """Detected concepts for one lecture keyframe, keyed by keyframe_id."""

    #--------------------#
    # Internal variables #
    #--------------------#
    keyframe_id: str
    refined_concepts: LectureKeyframeConceptList

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the refined keyframe from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureKeyframeRefinedConcepts":
        return cls.model_validate(json_input)

    # Public Method: Serialize the refined keyframe to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureEnrichmentTask(BaseModel):
    """Task model representing a lecture enrichment operation."""

    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id: str
    keyframes: list[LectureKeyframe] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the enrichment task from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureEnrichmentTask":
        return cls.model_validate(json_input)

    # Public Method: Serialize the enrichment task to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureEnrichmentChunkResult(BaseModel):
    """Map-call result for one keyframe chunk.

    Carries the detected keyframe concepts of the chunk plus its chunk report
    (chunk_title_draft, chunk_top_concepts), which the reduce call merges
    into the lecture-level metadata. Every field is required so that the
    strict json response schema is fully specified.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id         : str
    chunk_index        : int
    chunk_count        : int
    keyframes          : list[LectureKeyframeRefinedConcepts]
    chunk_title_draft  : str
    chunk_top_concepts : list[str]

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the chunk result from its json representation.
    @classmethod
    def from_json(cls, json_input: dict[str, Any]) -> "LectureEnrichmentChunkResult":
        return cls.model_validate(json_input)

    # Public Method: Serialize the chunk result to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class LectureEnrichmentResult(BaseModel):
    """Enrichment result for a whole lecture.

    The lecture-level metadata comes from the reduce call; the per-keyframe
    detected concepts are reassembled from the per-chunk map calls. The
    ontology lists are filled downstream by the strict/fuzzy sub-algorithms.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    lecture_id: str
    title: str
    long_description: str
    medium_description: str
    short_description: str
    top_concepts: LectureTopConceptList
    keyframes: list[LectureKeyframeRefinedConcepts] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the enrichment result from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "LectureEnrichmentResult":
        return cls.model_validate(input_json)

    # Public Method: Serialize the enrichment result to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
