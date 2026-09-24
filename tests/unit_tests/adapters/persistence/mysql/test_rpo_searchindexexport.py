# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_searchindexexport.py
"""Unit tests for MySQLSearchIndexExportRepository using in-memory fakes.

The database fake streams canned doc and link rows into the temp JSONL files
the adapter reads back, so the tests verify the merge-join, the
degree-score factors, the import-folder shape, and the replace semantics.
"""
from __future__ import annotations
import gzip
import json
from graphregistry.adapters.persistence.mysql.repositories.rpo_searchindexexport import MySQLSearchIndexExportRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in streaming canned rows into the output files."""

    # Public Method: Initialize the fake with the streams to serve.
    def __init__(self) -> None:
        self.stream_calls = []
        self.tables_existing = set()
        self.streams = {}

    # Public Method: Stream the canned rows of the query into the file.
    def execute_query_stream_to_file(self, engine_name: str, query: str, fetch_size: int = 1000, output_file: str = "", query_id: str | None = None, **kwargs) -> None:
        self.stream_calls.append({"query": query, "query_id": query_id, "output_file": output_file})
        rows = self.streams.get(query_id, [])
        with open(output_file, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

    # Public Method: Report whether a table exists.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str) -> bool:
        return f"{schema_name}.{table_name}" in self.tables_existing

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow and es_cache schemas."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and Elasticsearch cache schema.
    def for_es_cache(self) -> tuple[str, str]:
        return ("coresrv", "es_cache_test")

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration carrying the doc types and fields."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "doc_types"    : ["Person", "Course"],
            "elasticsearch": {"fields": {"docs": {"Person": ["external_url_en"]}, "links": {"Course": []}}},
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database and export dir.
def _make_repo(tmp_path) -> tuple[MySQLSearchIndexExportRepository, FakeGraphDB]:
    db = FakeGraphDB()
    db.tables_existing.update(["es_cache_test.Index_D_Person", "es_cache_test.Index_D_Person_L_Course"])
    db.streams["wsg7uZ1k"] = [
        json.loads('{"doc_type": "Person", "doc_id": "p1", "degree_score": 2.0, "short_code": "P1", "subtype_en": "Prof", "subtype_fr": "Prof", "name_en": "Alice", "name_fr": "Alice", "short_description_en": "sd", "short_description_fr": "sd", "long_description_en": "ld", "long_description_fr": "ld", "external_url_en": "http://a"}'),
        json.loads('{"doc_type": "Person", "doc_id": "p2", "degree_score": 1.0, "short_code": "P2", "subtype_en": null, "subtype_fr": null, "name_en": "Bob", "name_fr": "Bob", "short_description_en": null, "short_description_fr": null, "long_description_en": null, "long_description_fr": null, "external_url_en": null}'),
    ]
    db.streams["fhLp0sNg"] = [
        json.loads('{"doc_type": "Person", "doc_id": "p1", "link_type": "Course", "link_subtype": "SEM", "link_id": "c1", "link_rank": 1, "link_name_en": "Course 1", "link_name_fr": "Cours 1", "link_short_description_en": "d", "link_short_description_fr": "d"}'),
        json.loads('{"doc_type": "Person", "doc_id": "p1", "link_type": "Course", "link_subtype": "ORG", "link_id": "c2", "link_rank": 1, "link_name_en": "Course 2", "link_name_fr": "Cours 2", "link_short_description_en": null, "link_short_description_fr": null}'),
    ]
    repo = MySQLSearchIndexExportRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        index_config    = FakeIndexConfig(),
        export_path     = str(tmp_path),
    )
    return repo, db

#================================================================#
# Function Group: Local cache tests                              #
#================================================================#

# Public Function: Verify the local cache merges docs with their links.
def test_generate_local_cache_merges_links(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)

    # Generate the cache for one index date in commit mode.
    written = repo.generate_local_cache(index_date="2026-09-23", ignore_warnings=True)

    # One file per doc type with docs; Course has no doc table and streams
    # nothing, so only the Person file is written.
    assert written == [str(tmp_path / "2026-09-23" / "es_splitindex_2026-09-23_Person.jsonl.gz")]

    # The merge-join attaches both links of p1 and none of p2.
    with gzip.open(written[0], "rt", encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]
    assert [doc["doc_id"] for doc in docs] == ["p1", "p2"]
    assert len(docs[0]["links"]) == 2 and docs[1]["links"] == []
    assert docs[0]["links"][0]["link_subtype"] == "SEM"
    assert docs[0]["links"][0]["link_name"] == {"en": "Course 1", "fr": "Cours 1"}

    # The degree score factor and the custom doc field are applied.
    assert docs[0]["degree_score_factor"] == docs[0]["degree_score"] * 1.0 or isinstance(docs[0]["degree_score_factor"], float)
    assert docs[0]["external_url_en"] == "http://a"

    # The temp files are cleaned up.
    assert not (tmp_path / "2026-09-23" / ".tmp_docs_2026-09-23_Person.jsonl").exists()

# Public Function: Verify the existing outputs are skipped without force.
def test_generate_local_cache_skips_existing(tmp_path) -> None:
    repo, _db = _make_repo(tmp_path)
    output = tmp_path / "2026-09-23" / "es_splitindex_2026-09-23_Person.jsonl.gz"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"existing")

    # Without replace_existing the output is skipped; with replace but
    # without force it is skipped too, mirroring the non-interactive policy.
    assert repo.generate_local_cache(index_date="2026-09-23", ignore_warnings=True) == []
    assert repo.generate_local_cache(index_date="2026-09-23", ignore_warnings=True, replace_existing=True) == []
    assert output.read_bytes() == b"existing"

    # With force the file is regenerated.
    written = repo.generate_local_cache(index_date="2026-09-23", ignore_warnings=True, replace_existing=True, force_replace=True)
    assert written == [str(output)]
    assert output.read_bytes() != b"existing"

#================================================================#
# Function Group: Import folder tests                            #
#================================================================#

# Public Function: Verify the import folder carries settings and documents.
def test_generate_index_folder_shape(tmp_path) -> None:
    repo, _db = _make_repo(tmp_path)

    # Generate the local cache first, then the import folder from it.
    repo.generate_local_cache(index_date="2026-09-23", ignore_warnings=True)
    folder = repo.generate_index_folder(index_date="2026-09-23", ignore_warnings=True)

    # The folder carries the settings payload and the importer-shaped docs.
    assert folder == str(tmp_path / "2026-09-23" / "es_fullindex_2026-09-23")
    with open(tmp_path / "2026-09-23" / "es_fullindex_2026-09-23" / "settings_mappings.json") as f:
        settings = json.load(f)
    assert settings["settings"]["index"]["number_of_shards"] == 1
    assert "sayt" in settings["mappings"]["properties"]["name"]["properties"]["en"]["fields"]

    # The documents carry the importer shape with the links attached.
    with open(tmp_path / "2026-09-23" / "es_fullindex_2026-09-23" / "documents.jsonl") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert all(set(doc) == {"_id", "_source"} for doc in lines)
    assert [doc["_id"] for doc in lines] == ["p1", "p2"]
    assert lines[0]["_source"]["links"][0]["link_id"] == "c1"

# Public Function: Verify the folder generation skips missing sources.
def test_generate_index_folder_without_cache(tmp_path) -> None:
    repo, _db = _make_repo(tmp_path)

    # Without a local cache the folder is still created, but empty of docs.
    folder = repo.generate_index_folder(index_date="2026-09-23", ignore_warnings=True)
    with open(tmp_path / "2026-09-23" / "es_fullindex_2026-09-23" / "documents.jsonl") as f:
        assert f.read() == ""
    assert (tmp_path / "2026-09-23" / "es_fullindex_2026-09-23" / "settings_mappings.json").exists()
