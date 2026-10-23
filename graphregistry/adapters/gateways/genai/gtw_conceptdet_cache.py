# graphregistry/adapters/gateways/genai/gtw_conceptdet_cache.py
"""Caching decorator for the concept-detection gateway.

Wraps any ConceptDetectionGateway and serves wiki_search and detect_concepts
from the graph_genai lookup caches when their inputs were seen before, so
ontology re-validation costs no network. The unit of caching is the lookup
input, not the object: the same keyword or the same keyword list returns the
same result regardless of which lecture asked, which makes the cache globally
reusable across lectures. extract_keywords passes through uncached.
"""
from __future__ import annotations
import hashlib
import json
from typing import Any
from loguru import logger as sysmsg
from graphregistry.application.ports.gateways.prt_conceptdet import ConceptDetectionGateway
from graphregistry.application.ports.repositories.prt_genai import GenAIRepository
from graphregistry.domain.models.entities.mdl_conceptmap import ScoredConceptList
from graphregistry.domain.models.entities.mdl_genai import GenAIWikiSearchLookup, GenAIWikifyLookup

#==================#
# Class Definition #
#==================#
class CachedConceptDetectionGateway(ConceptDetectionGateway):
    """Concept-detection gateway decorator with graph_genai-backed lookup caches."""

    # Public Method: Initialise the decorator with the concrete gateway and the genai repository.
    def __init__(self, gateway: ConceptDetectionGateway, genai_repository: GenAIRepository) -> None:
        self._gateway = gateway
        self._repo = genai_repository

    # Internal Function: Compute the md5 hex digest used for lookup cache keys.
    @staticmethod
    def _input_hash(text: str) -> str:
        """Compute the md5 hex digest used for lookup cache keys."""
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    # Public Method: Serve wiki_search from the keyword cache, falling back to the concrete gateway.
    def wiki_search(self, search_term: str) -> list[dict[str, Any]]:
        """Serve wiki_search from the keyword cache, falling back to the concrete gateway."""

        # Cache lookups are best-effort: a failure degrades to a cache miss.
        keyword_hash = self._input_hash(search_term or "")
        try:
            cached = self._repo.get_wiki_search(keyword_hash=keyword_hash)
            if cached is not None:
                return cached.suggestions_list
        except Exception as exc:
            sysmsg.warning("Wiki-search cache lookup failed for '{}...': {}", (search_term or "")[:40], exc)

        # Call the concrete gateway and store the result; a failed store keeps the result.
        suggestions = self._gateway.wiki_search(search_term=search_term)
        try:
            self._repo.save_wiki_search(GenAIWikiSearchLookup(
                keyword_hash     = keyword_hash,
                keyword          = search_term or "",
                suggestions_list = suggestions,
            ))
        except Exception as exc:
            sysmsg.warning("Wiki-search cache store failed for '{}...': {}", (search_term or "")[:40], exc)
        return suggestions

    # Public Method: Serve detect_concepts from the input cache, falling back to the concrete gateway.
    def detect_concepts(self, text: str | list[str]) -> ScoredConceptList:
        """Serve detect_concepts from the input cache, falling back to the concrete gateway."""

        # The cache key is the md5 of the exact json-serialized input.
        input_text = json.dumps(text, ensure_ascii=False)
        input_hash = self._input_hash(input_text)
        try:
            cached = self._repo.get_wikify(input_hash=input_hash)
            if cached is not None:
                return ScoredConceptList.model_validate_json(cached.output_text)
        except Exception as exc:
            sysmsg.warning("Wikify cache lookup failed: {}", exc)

        # Call the concrete gateway and store the result; a failed store keeps the result.
        scored = self._gateway.detect_concepts(text=text)
        try:
            self._repo.save_wikify(GenAIWikifyLookup(
                input_hash  = input_hash,
                input_text  = input_text,
                output_text = scored.model_dump_json(),
            ))
        except Exception as exc:
            sysmsg.warning("Wikify cache store failed: {}", exc)
        return scored

    # Public Method: Pass extract_keywords through to the concrete gateway uncached.
    def extract_keywords(self, text: str) -> list[str]:
        """Pass extract_keywords through to the concrete gateway uncached."""
        return self._gateway.extract_keywords(text=text)
