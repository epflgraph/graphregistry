# graphregistry/application/ports/repositories/prt_searchindexexport.py
from __future__ import annotations
from typing import Protocol, runtime_checkable

#==================#
# Class Definition #
#==================#
@runtime_checkable

#==================#
# Class Definition #
#==================#
class SearchIndexExportRepository(Protocol):
    """Persistence port for the Elasticsearch index export: the local cache
    generation from the es_cache MySQL tables and the import-folder
    preparation consumed by the search index import.

    The local cache streams one JSONL.gz file per doc type, merging the doc
    rows with their link rows in a single ordered pass. The import folder
    carries the index settings and mappings plus one documents.jsonl with
    every document in the importer shape. The doc and link field lists are
    index configuration, loaded by the adapter exactly as the legacy export
    loads them.

    One deliberate deviation from the legacy: this port never prompts
    interactively. Replacing an existing output requires replace_existing
    together with force_replace; without force_replace the output is
    skipped with a warning, and the caller decides whether to ask the user.
    """

    # Public Method: Generate the per-doc-type JSONL.gz local cache files
    # from the es_cache tables, returning the written file paths.
    def generate_local_cache(self, index_date: str, ignore_warnings: bool = True, replace_existing: bool = False, force_replace: bool = False) -> list[str]:
        ...

    # Public Method: Generate the Elasticsearch import folder (settings,
    # mappings, and documents.jsonl) from the local cache files, returning
    # the folder path.
    def generate_index_folder(self, index_date: str, ignore_warnings: bool = True, replace_existing: bool = False, force_replace: bool = False) -> str:
        ...
