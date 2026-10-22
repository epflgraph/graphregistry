# graphregistry/application/ports/repositories/prt_genai.py
from __future__ import annotations
from typing import Protocol, runtime_checkable
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
@runtime_checkable

#==================#
# Class Definition #
#==================#
class GenAIRepository(Protocol):
    """Persistence port for the graph_genai outputs database.

    The graph_genai layer stores every GenAI output as provenance: one cached
    completion per LLM call, the keyword lists attached to objects, the raw
    composed input of one enrichment task, and the translated presentation
    fields. The completion hash ties the derived records back to the exact
    call that produced them.
    """

    # Public Method: Store the raw composed input of one enrichment task.
    def save_raw_content(self, raw: GenAIRawContent) -> None:
        ...

    # Public Method: Store one cached LLM completion.
    def save_completion(self, completion: GenAICompletion) -> None:
        ...

    # Public Method: Fetch one cached LLM completion by its dedup key, or None when absent.
    def get_completion(self, object_type: str, object_id: str, model_used: str, input_hash: str) -> GenAICompletion | None:
        ...

    # Public Method: Store one batch of keyword list records.
    def save_keywords(self, rows: list[GenAIKeywords]) -> None:
        ...

    # Public Method: Store one batch of translation records.
    def save_translations(self, rows: list[GenAITranslation]) -> None:
        ...

    # Public Method: Fetch one cached wiki_search lookup by keyword hash, or None when absent.
    def get_wiki_search(self, keyword_hash: str) -> GenAIWikiSearchLookup | None:
        ...

    # Public Method: Store one cached wiki_search lookup.
    def save_wiki_search(self, lookup: GenAIWikiSearchLookup) -> None:
        ...

    # Public Method: Fetch one cached wikify lookup by input hash, or None when absent.
    def get_wikify(self, input_hash: str) -> GenAIWikifyLookup | None:
        ...

    # Public Method: Store one cached wikify lookup.
    def save_wikify(self, lookup: GenAIWikifyLookup) -> None:
        ...
