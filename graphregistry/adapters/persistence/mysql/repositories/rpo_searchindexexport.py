# graphregistry/adapters/persistence/mysql/repositories/rpo_searchindexexport.py
from __future__ import annotations
from decimal import Decimal
import gzip
import json
import os
from typing import TYPE_CHECKING, Any, Iterator
from loguru import logger as sysmsg
from tqdm import tqdm
from graphregistry.adapters.clients.elasticsearch import es_degree_score_factors
from graphregistry.application.ports.repositories.prt_searchindexexport import SearchIndexExportRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import IndexConfig
from graphregistry.common.paths import REPO_ROOT

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Width of the progress bar.
PBWIDTH = 92

# Default column lists of the doc and link export rows.
DEFAULT_DOC_COLUMNS = [
    "doc_type", "doc_id", "degree_score", "short_code", "subtype_en", "subtype_fr",
    "name_en", "name_fr", "short_description_en", "short_description_fr",
    "long_description_en", "long_description_fr",
]
DEFAULT_LINK_COLUMNS = [
    "doc_type", "doc_id", "link_type", "link_subtype", "link_id", "link_rank",
    "link_name_en", "link_name_fr", "link_short_description_en", "link_short_description_fr",
]

#==================#
# Class Definition #
#==================#
class MySQLSearchIndexExportRepository(SearchIndexExportRepository):
    """MySQL adapter for the SearchIndexExportRepository port.

    The SQL statements, query ids, and the merge-join structure are
    extracted verbatim from the legacy
    GraphRegistry.IndexES.generate_local_cache_streaming and
    generate_index_from_local_cache methods, including the per-doc-type
    degree-score factors and the static Elasticsearch settings/mappings
    payload. The legacy non-streaming generate_local_cache variant has no
    callers and is not ported.
    """

    # Public Method: Initialize the repository with an injected database
    # client, schema resolver, index configuration, and export path.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, index_config: IndexConfig, export_path: str, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.index_config = index_config
        self.verbose = verbose

        # Resolve the export path against the repository root when relative,
        # as the legacy module-level constant does.
        export_path = os.fspath(export_path)
        self.export_path = export_path if os.path.isabs(export_path) else os.path.join(REPO_ROOT, export_path)

    #================================================================#
    # Method Group: Local cache generation                           #
    #================================================================#

    # Public Method: Generate the per-doc-type JSONL.gz local cache files
    # from the es_cache tables.
    def generate_local_cache(self, index_date: str, ignore_warnings: bool = True, replace_existing: bool = False, force_replace: bool = False) -> list[str]:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, es_cache_schema = self.schema_resolver.for_es_cache()
        written_paths = []
        sysmsg.info(f"🐙 📝 Generate local JSON cache for ElasticSearch index creation (index date: {index_date}).")

        # The target folder is shared by all doc types of the index date.
        target_folder = os.path.join(self.export_path, index_date)
        os.makedirs(target_folder, exist_ok=True)

        # One output file per doc type, streaming the docs and their links.
        list_of_doc_types = self.index_config.settings.get("doc_types", [])
        for doc_type in tqdm(list_of_doc_types, unit="doc type"):
            tqdm.write(f"⚙️ [GLC-ES] Processing doc type: {doc_type}".ljust(PBWIDTH)[:PBWIDTH])
            target_output_path = os.path.join(target_folder, f"es_splitindex_{index_date}_{doc_type}.jsonl.gz")
            if os.path.exists(target_output_path):
                # State the outcome up front: this file will either be
                # replaced or skipped, depending on the caller's flags.
                if replace_existing and force_replace:
                    sysmsg.warning(f"♻️ Replacing existing file: {target_output_path}")
                elif not ignore_warnings:
                    sysmsg.warning(f"File already exists: {target_output_path}")
                if not replace_existing:
                    sysmsg.warning(f"⚠️ File already exists and replace_existing is False; skipping: {target_output_path}")
                    continue

                # Without force_replace the output is skipped rather than
                # replaced; the caller decides whether to confirm.
                if not force_replace:
                    sysmsg.warning(f"⚠️ File already exists and force_replace is False; skipping: {target_output_path}")
                    continue
                os.remove(target_output_path)

            # -------------------------
            # 1) Stream DOC rows to JSONL (temp)
            # -------------------------
            custom_doc_cols = self.index_config.settings.get("elasticsearch", {}).get("fields", {}).get("docs", {}).get(doc_type, [])
            column_names_doc = DEFAULT_DOC_COLUMNS + custom_doc_cols
            docs_jsonl = os.path.join(target_folder, f".tmp_docs_{index_date}_{doc_type}.jsonl")

            # The doc table may not exist for every configured type.
            if not self.db.table_exists(engine_name=engine_name, schema_name=es_cache_schema, table_name=f"Index_D_{doc_type}"):
                if not ignore_warnings:
                    sysmsg.warning(f"Table '{es_cache_schema}.Index_D_{doc_type}' does not exist. Skipping doc type '{doc_type}'.")
                continue
            self.db.execute_query_stream_to_file(
                engine_name = engine_name,
                query       = f"""
                    SELECT {', '.join(column_names_doc)}
                    FROM {es_cache_schema}.Index_D_{doc_type}
                ORDER BY doc_id ASC
                """,
                fetch_size   = 2000,
                output_file  = docs_jsonl,
                query_id     = 'wsg7uZ1k',
            )

            # Empty doc streams are skipped, cleaning up their temp files.
            if os.path.getsize(docs_jsonl) == 0:
                if not ignore_warnings:
                    sysmsg.warning(f"No docs found for doc type '{doc_type}'. Skipping.")
                try:
                    os.remove(docs_jsonl)
                except Exception:
                    pass
                continue

            # -------------------------
            # 2) Stream each LINK table to JSONL (temp), only when present
            # -------------------------
            link_files = {}
            link_custom_cols = {}
            for link_type in list_of_doc_types:
                if not self.db.table_exists(engine_name=engine_name, schema_name=es_cache_schema, table_name=f"Index_D_{doc_type}_L_{link_type}"):
                    continue
                custom_link_cols = self.index_config.settings.get("elasticsearch", {}).get("fields", {}).get("links", {}).get(link_type, [])
                column_names_link = DEFAULT_LINK_COLUMNS + custom_link_cols
                links_jsonl = os.path.join(target_folder, f".tmp_links_{index_date}_{doc_type}_L_{link_type}.jsonl")
                self.db.execute_query_stream_to_file(
                    engine_name = engine_name,
                    query       = f"""
                        SELECT {', '.join(column_names_link)}
                        FROM {es_cache_schema}.Index_D_{doc_type}_L_{link_type}
                    ORDER BY doc_id ASC, link_rank ASC
                    """,
                    fetch_size   = 5000,
                    output_file  = links_jsonl,
                    query_id     = 'fhLp0sNg',
                )
                link_files[link_type] = links_jsonl
                link_custom_cols[link_type] = custom_link_cols

            # -------------------------
            # 3) Write the final JSONL.GZ (one doc per line)
            # -------------------------
            written_paths.append(target_output_path)
            self._write_split_index(target_output_path, docs_jsonl, link_files, link_custom_cols, doc_type)

            # Cleanup the temp files of this doc type.
            try:
                os.remove(docs_jsonl)
            except Exception:
                pass
            for temp_path in link_files.values():
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

        # Report the completion with the written file paths.
        sysmsg.success("🐙 ✅ Done generating local JSON cache.")
        return written_paths

    # Internal Method: Write one split-index file by merge-joining the doc
    # stream with the link streams, both ordered by doc id.
    def _write_split_index(self, target_output_path: str, docs_jsonl: str, link_files: dict[str, str], link_custom_cols: dict[str, list[str]], doc_type: str) -> None:

        # Internal Function: Iterate the JSON objects of a JSONL file.
        def _iter_jsonl(path: str) -> Iterator[dict]:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

        # Prepare the link iterators and their current-row pointers; both
        # streams are ordered by doc id, so one pass merges every link into
        # its document.
        link_iters = {link_type: _iter_jsonl(path) for link_type, path in link_files.items()}
        link_curr = {link_type: next(it, None) for link_type, it in link_iters.items()}

        # Internal Function: Collect the links of one doc id from every link
        # stream, advancing the pointers past the consumed rows.
        def collect_links_for_doc_id(doc_id_value: Any) -> list[dict]:
            out = []
            for link_type, it in link_iters.items():
                curr = link_curr[link_type]
                while curr is not None and curr.get("doc_id") == doc_id_value:
                    json_link = {
                        "doc_type"               : curr["doc_type"],
                        "doc_id"                 : curr["doc_id"],
                        "link_type"              : curr["link_type"],
                        "link_subtype"           : curr["link_subtype"],
                        "link_id"                : curr["link_id"],
                        "link_rank"              : curr["link_rank"],
                        "link_name"              : {"en": curr["link_name_en"], "fr": curr["link_name_fr"]},
                        "link_short_description" : {
                            "en": curr["link_short_description_en"],
                            "fr": curr["link_short_description_fr"],
                        },
                    }
                    for custom_col in link_custom_cols.get(link_type, []):
                        if custom_col in curr:
                            json_link[custom_col] = curr[custom_col]
                    out.append(json_link)
                    curr = next(it, None)
                    link_curr[link_type] = curr
            return out

        # Write the merged documents, serialising the whole dict to
        # guarantee the escaping of newlines and quotes.
        custom_doc_cols = self.index_config.settings.get("elasticsearch", {}).get("fields", {}).get("docs", {}).get(doc_type, [])
        with gzip.open(target_output_path, "wt", encoding="utf-8") as out_fp:
            for doc in _iter_jsonl(docs_jsonl):
                doc_id = doc["doc_id"]
                doc_json = {
                    "doc_type"            : doc["doc_type"],
                    "doc_id"              : doc_id,
                    "degree_score"        : doc["degree_score"],
                    "degree_score_factor" : es_degree_score_factors[doc_type] * doc["degree_score"],
                    "short_code"          : doc["short_code"],
                    "subtype"             : {"en": doc["subtype_en"], "fr": doc["subtype_fr"]},
                    "name"                : {"en": doc["name_en"], "fr": doc["name_fr"]},
                    "short_description"   : {"en": doc["short_description_en"], "fr": doc["short_description_fr"]},
                    "long_description"    : {"en": doc["long_description_en"], "fr": doc["long_description_fr"]},
                    "links"               : collect_links_for_doc_id(doc_id),
                }
                for custom_col in custom_doc_cols:
                    if custom_col in doc:
                        doc_json[custom_col] = doc[custom_col]
                out_fp.write(json.dumps(doc_json, ensure_ascii=False, default=str))
                out_fp.write("\n")

    #================================================================#
    # Method Group: Import folder generation                         #
    #================================================================#

    # Public Method: Generate the Elasticsearch import folder (settings,
    # mappings, and documents.jsonl) from the local cache files.
    def generate_index_folder(self, index_date: str, ignore_warnings: bool = True, replace_existing: bool = False, force_replace: bool = False) -> str:
        sysmsg.info(f"🐙 📝 Generate ElasticSearch import folder from local JSON cache (index date: {index_date}).")

        # The folder layout expected by the index importer.
        output_folder = os.path.join(self.export_path, index_date, f"es_fullindex_{index_date}")
        os.makedirs(output_folder, exist_ok=True)
        docs_path = os.path.join(output_folder, "documents.jsonl")
        settings_path = os.path.join(output_folder, "settings_mappings.json")

        # Existing outputs are skipped unless replacement is forced; the
        # caller decides whether to confirm, as with the local cache. The
        # outcome is stated up front so the operator can tell replacement
        # from a silently skipped regeneration.
        existing_files = [p for p in (docs_path, settings_path) if os.path.exists(p)]
        if existing_files:
            if replace_existing and force_replace:
                sysmsg.warning(f"♻️ Replacing existing output in: {output_folder}")
            elif not ignore_warnings:
                for path in existing_files:
                    sysmsg.warning(f"File already exists: {path}")
            if not replace_existing:
                sysmsg.error(f"❌ Failed. Output already exists in: {output_folder}")
                return output_folder
            if not force_replace:
                sysmsg.warning(f"⚠️ Output already exists and force_replace is False; skipping: {output_folder}")
                return output_folder
            for path in existing_files:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass

        # Write the static settings and mappings payload.
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(SETTINGS_MAPPINGS_PAYLOAD, f, ensure_ascii=False, indent=4)

        # Stream the documents of every doc type into one JSONL file, in the
        # importer shape: one {"_id", "_source"} object per line.
        list_of_doc_types = self.index_config.settings.get("doc_types", [])
        with open(docs_path, "w", encoding="utf-8") as out:
            for doc_type in tqdm(list_of_doc_types, unit="doc type"):
                source_file_path = os.path.join(self.export_path, index_date, f"es_splitindex_{index_date}_{doc_type}.jsonl.gz")
                if not os.path.exists(source_file_path):
                    if not ignore_warnings:
                        sysmsg.warning(f"Source file does not exist: {source_file_path}. Skipping doc type '{doc_type}'.")
                    continue
                with gzip.open(source_file_path, "rt", encoding="utf-8", errors="strict") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        doc = json.loads(line)

                        # Cache format violations are skipped, as the legacy
                        # does.
                        doc_id = doc.get("doc_id") or doc.get("_id")
                        if doc_id is None:
                            continue
                        if isinstance(doc, dict) and "_source" in doc:
                            obj = doc
                            obj.setdefault("_id", doc_id)
                        else:
                            obj = {"_id": doc_id, "_source": doc}
                        out.write(json.dumps(obj, ensure_ascii=False, default=_json_default))
                        out.write("\n")
        sysmsg.success(f"🐙 ✅ Done generating import folder:\n  {output_folder}")
        return output_folder

# Internal Function: Serialize Decimal objects as floats for JSON.
def _json_default(o):
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

# Static Elasticsearch index settings and mappings of the export, verbatim
# from the legacy generate_index_from_local_cache method.
SETTINGS_MAPPINGS_PAYLOAD = {
    "aliases"  : {},
    "settings" : {
        "index": {
            "number_of_shards"   : 1,
            "number_of_replicas" : 1,
            "max_ngram_diff"     : 2,
            "analysis"           : {
                "char_filter": {
                    "strip_html": {"type": "html_strip"}
                },
                "normalizer": {
                    "lc_fold": {
                        "type"   : "custom",
                        "filter" : ["lowercase", "asciifolding"]
                    }
                },
                "filter": {
                    "stemmer_en"  : {"type": "stemmer", "language": "light_english"},
                    "stemmer_fr"  : {"type": "stemmer", "language": "light_french"},
                    "shingle_2_3" : {
                        "type"             : "shingle",
                        "min_shingle_size" : 2,
                        "max_shingle_size" : 3,
                        "output_unigrams"  : True
                    },
                    "synonym_en": {
                        "type"     : "synonym_graph",
                        "synonyms" : ["computational complexity, algorithmic complexity"]
                    },
                    "edge_2_20": {"type": "edge_ngram", "min_gram": 2, "max_gram": 20},
                    "ngram_3_5": {"type": "ngram", "min_gram": 3, "max_gram": 5}
                },
                "analyzer": {
                    "raw_lc": {
                        "tokenizer" : "keyword",
                        "filter"    : ["lowercase", "asciifolding"]
                    },
                    "base_en": {
                        "type"        : "custom",
                        "char_filter" : ["strip_html"],
                        "tokenizer"   : "standard",
                        "filter"      : ["lowercase", "asciifolding", "stemmer_en"]
                    },
                    "base_fr": {
                        "type"        : "custom",
                        "char_filter" : ["strip_html"],
                        "tokenizer"   : "standard",
                        "filter"      : ["lowercase", "asciifolding", "stemmer_fr"]
                    },
                    "search_en": {
                        "type"        : "custom",
                        "char_filter" : ["strip_html"],
                        "tokenizer"   : "standard",
                        "filter"      : ["lowercase", "asciifolding", "stemmer_en", "synonym_en"]
                    },
                    "search_fr": {
                        "type"        : "custom",
                        "char_filter" : ["strip_html"],
                        "tokenizer"   : "standard",
                        "filter"      : ["lowercase", "asciifolding", "stemmer_fr"]
                    },
                    "autocomplete_en": {
                        "type"      : "custom",
                        "tokenizer" : "standard",
                        "filter"    : ["lowercase", "asciifolding", "edge_2_20"]
                    },
                    "autocomplete_fr": {
                        "type"      : "custom",
                        "tokenizer" : "standard",
                        "filter"    : ["lowercase", "asciifolding", "edge_2_20"]
                    },
                    "trigram": {
                        "type"      : "custom",
                        "tokenizer" : "standard",
                        "filter"    : ["lowercase", "asciifolding", "shingle_2_3"]
                    },
                    "typo_ngram": {
                        "type"      : "custom",
                        "tokenizer" : "standard",
                        "filter"    : ["lowercase", "asciifolding", "ngram_3_5"]
                    },
                    "typo_search": {
                        "type"      : "custom",
                        "tokenizer" : "standard",
                        "filter"    : ["lowercase", "asciifolding"]
                    }
                }
            }
        }
    },
    "mappings": {
        "dynamic"    : True,
        "properties" : {
            "name": {
                "properties": {
                    "en": {
                        "type"            : "text",
                        "analyzer"        : "base_en",
                        "search_analyzer" : "search_en",
                        "fields"          : {
                            "raw"  : {"type": "keyword", "normalizer": "lc_fold"},
                            "sayt" : {
                                "type"             : "search_as_you_type",
                                "analyzer"         : "base_en",
                                "doc_values"       : False,
                                "max_shingle_size" : 3
                            },
                            "ac": {
                                "type"            : "text",
                                "analyzer"        : "autocomplete_en",
                                "search_analyzer" : "search_en"
                            },
                            "trigram" : {"type": "text", "analyzer": "trigram"},
                            "typo"    : {
                                "type"            : "text",
                                "analyzer"        : "typo_ngram",
                                "search_analyzer" : "typo_search"
                            }
                        }
                    },
                    "fr": {
                        "type"            : "text",
                        "analyzer"        : "base_fr",
                        "search_analyzer" : "search_fr",
                        "fields"          : {
                            "raw"  : {"type": "keyword", "normalizer": "lc_fold"},
                            "sayt" : {
                                "type"             : "search_as_you_type",
                                "analyzer"         : "base_fr",
                                "doc_values"       : False,
                                "max_shingle_size" : 3
                            },
                            "ac": {
                                "type"            : "text",
                                "analyzer"        : "autocomplete_fr",
                                "search_analyzer" : "search_fr"
                            },
                            "trigram" : {"type": "text", "analyzer": "trigram"},
                            "typo"    : {
                                "type"            : "text",
                                "analyzer"        : "typo_ngram",
                                "search_analyzer" : "typo_search"
                            }
                        }
                    }
                }
            },
            "long_description": {
                "properties": {
                    "en": {
                        "type"            : "text",
                        "analyzer"        : "base_en",
                        "search_analyzer" : "search_en",
                        "fields"          : {
                            "typo": {
                                "type"            : "text",
                                "analyzer"        : "typo_ngram",
                                "search_analyzer" : "typo_search"
                            }
                        }
                    },
                    "fr": {
                        "type"            : "text",
                        "analyzer"        : "base_fr",
                        "search_analyzer" : "search_fr",
                        "fields"          : {
                            "typo": {
                                "type"            : "text",
                                "analyzer"        : "typo_ngram",
                                "search_analyzer" : "typo_search"
                            }
                        }
                    }
                }
            }
        }
    }
}
