# graphregistry/adapters/persistence/mysql/mappers/map_genai.py
"""Maps the graph_genai domain records to MySQL row shapes and back.

The row dictionaries feed the shared batch upsert helper; list payloads such
as the keyword lists are serialized to json strings, matching the longtext
columns of the graph_genai tables.
"""
from __future__ import annotations
import json
from typing import Any
from graphregistry.domain.models.entities.mdl_genai import (
    GenAICompletion,
    GenAIKeywords,
    GenAIRawContent,
    GenAITranslation,
    GenAIWikiSearchLookup,
    GenAIWikifyLookup,
)

#==================#
# Class Definition #
#==================#
class MySQLGenAIRawContentMapper:
    """Maps between MySQL row shapes and the GenAIRawContent model."""

    # Public Function: Build the upsert row of one raw content record.
    @staticmethod
    def to_row(raw: GenAIRawContent) -> dict[str, Any]:
        return {
            "object_type" : raw.object_type,
            "object_id"   : raw.object_id,
            "raw_content" : raw.raw_content,
        }

#==================#
# Class Definition #
#==================#
class MySQLGenAICompletionMapper:
    """Maps between MySQL row shapes and the GenAICompletion model."""

    # Public Function: Build the upsert row of one cached completion.
    @staticmethod
    def to_row(completion: GenAICompletion) -> dict[str, Any]:
        return {
            "object_type" : completion.object_type,
            "object_id"   : completion.object_id,
            "model_used"  : completion.model_used,
            "call_role"   : completion.call_role,
            "chunk_num"   : completion.chunk_num,
            "input_hash"  : completion.input_hash,
            "input_text"  : completion.input_text,
            "output_text" : completion.output_text,
        }

#==================#
# Class Definition #
#==================#
class MySQLGenAIKeywordsMapper:
    """Maps between MySQL row shapes and the GenAIKeywords model."""

    # Public Function: Build the upsert rows of one batch of keyword records.
    @staticmethod
    def to_rows(rows: list[GenAIKeywords]) -> list[dict[str, Any]]:
        return [
            {
                "object_type"    : row.object_type,
                "object_id"      : row.object_id,
                "completion_hash": row.completion_hash,
                "coverage"       : row.coverage,
                "keywords_list"  : json.dumps(row.keywords_list, ensure_ascii=False),
            }
            for row in rows
        ]

#==================#
# Class Definition #
#==================#
class MySQLGenAIWikiSearchMapper:
    """Maps between MySQL row shapes and the GenAIWikiSearchLookup model."""

    # Public Function: Build the upsert row of one wiki_search lookup.
    @staticmethod
    def to_row(lookup: GenAIWikiSearchLookup) -> dict[str, Any]:
        return {
            "keyword_hash"    : lookup.keyword_hash,
            "keyword"         : lookup.keyword,
            "suggestions_list": json.dumps(lookup.suggestions_list, ensure_ascii=False),
        }

#==================#
# Class Definition #
#==================#
class MySQLGenAIWikifyMapper:
    """Maps between MySQL row shapes and the GenAIWikifyLookup model."""

    # Public Function: Build the upsert row of one wikify lookup.
    @staticmethod
    def to_row(lookup: GenAIWikifyLookup) -> dict[str, Any]:
        return {
            "input_hash" : lookup.input_hash,
            "input_text" : lookup.input_text,
            "output_text": lookup.output_text,
        }

#==================#
# Class Definition #
#==================#
class MySQLGenAITranslationMapper:
    """Maps between MySQL row shapes and the GenAITranslation model."""

    # Public Function: Build the upsert rows of one batch of translation records.
    @staticmethod
    def to_rows(rows: list[GenAITranslation]) -> list[dict[str, Any]]:
        return [
            {
                "object_type"             : row.object_type,
                "object_id"               : row.object_id,
                "completion_hash"         : row.completion_hash,
                "translated_from"         : row.translated_from,
                "translated_to"           : row.translated_to,
                "name_value"              : row.name_value,
                "description_short_value" : row.description_short_value,
                "description_medium_value": row.description_medium_value,
                "description_long_value"  : row.description_long_value,
            }
            for row in rows
        ]
