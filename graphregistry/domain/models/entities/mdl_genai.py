# graphregistry/domain/models/entities/mdl_genai.py
"""Domain models for the graph_genai outputs database.

These models represent the records persisted by the graph_genai layer: one
cached LLM completion per call, the keyword lists attached to objects, the
translated presentation fields, and the raw composed input of one enrichment
task. The hash lineage ties the derived records (keywords, translations) back
to the exact completion that produced them.
"""
from __future__ import annotations
from typing import Any
from pydantic import BaseModel, Field
from graphregistry.domain.types import CallRole, KeywordCoverage

#==================#
# Class Definition #
#==================#
class GenAIRawContent(BaseModel):
    """The raw composed input stored for one object before LLM processing.

    Carries the composed task (keyframe ids and OCR text as sent) rather than
    the raw per-slide OCR, which already lives in the registry custom fields.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type : str
    object_id   : str
    raw_content : str

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the raw content record from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAIRawContent":
        return cls.model_validate(input_json)

    # Public Method: Serialize the raw content record to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class GenAICompletion(BaseModel):
    """One cached LLM completion: the record of exactly one LLM call.

    The dedup key is (object_type, object_id, model_used, input_hash); the
    call role and chunk number are run metadata carried alongside, not part
    of the identity. input_hash is the md5 of the exact prompt text and
    output_text is the verbatim structured response.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type : str
    object_id   : str
    model_used  : str
    call_role   : CallRole
    chunk_num   : int | None = None
    input_hash  : str
    input_text  : str
    output_text : str

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the completion record from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAICompletion":
        return cls.model_validate(input_json)

    # Public Method: Serialize the completion record to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class GenAIKeywords(BaseModel):
    """One stored keyword list attached to an object.

    The coverage records what the prompt requested: top lists are the ranked
    bounded set of most important keywords, full lists are exhaustive. The
    level is carried by object_type (lecture-level top lists, per-keyframe
    full lists) and the completion hash ties the row to the exact call that
    extracted the keywords.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type    : str
    object_id      : str
    completion_hash: str
    coverage       : KeywordCoverage
    keywords_list  : list[str] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the keywords record from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAIKeywords":
        return cls.model_validate(input_json)

    # Public Method: Serialize the keywords record to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class GenAIWikiSearchLookup(BaseModel):
    """One cached wiki_search lookup, keyed by the md5 of the keyword.

    The keyword is free-form LLM output, so the dedup key is its hash; the
    keyword itself is stored for readability. The suggestions list carries
    the raw GraphAI response items (concept_id, concept_name, ...).
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    keyword_hash    : str
    keyword         : str
    suggestions_list: list[dict[str, Any]] = Field(default_factory=list)

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the wiki_search lookup from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAIWikiSearchLookup":
        return cls.model_validate(input_json)

    # Public Method: Serialize the wiki_search lookup to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class GenAIWikifyLookup(BaseModel):
    """One cached wikify lookup, keyed by the md5 of the exact input.

    The input is the exact text or keyword list as sent (json-serialized),
    so identical batches hit the cache regardless of which object asked;
    the output is the returned ScoredConceptList as json.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    input_hash : str
    input_text : str
    output_text: str

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the wikify lookup from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAIWikifyLookup":
        return cls.model_validate(input_json)

    # Public Method: Serialize the wikify lookup to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')

#==================#
# Class Definition #
#==================#
class GenAITranslation(BaseModel):
    """One stored translation of the generated presentation fields.

    One row per target language: the completion hash references the generation
    whose content was translated, translated_from the source language, and the
    translated values carry the name and the three description levels.
    """

    #--------------------#
    # Internal variables #
    #--------------------#
    object_type              : str
    object_id                : str
    completion_hash          : str
    translated_from          : str
    translated_to            : str
    name_value               : str | None = None
    description_short_value  : str | None = None
    description_medium_value : str | None = None
    description_long_value   : str | None = None

    #-----------------------#
    # Serialization methods #
    #-----------------------#

    # Public Method: Rebuild the translation record from its json representation.
    @classmethod
    def from_json(cls, input_json: dict[str, Any]) -> "GenAITranslation":
        return cls.model_validate(input_json)

    # Public Method: Serialize the translation record to a json dictionary.
    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode='json')
