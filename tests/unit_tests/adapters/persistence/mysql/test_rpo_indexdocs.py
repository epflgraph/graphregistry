# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_indexdocs.py
"""Unit tests for MySQLIndexDocRepository using an in-memory GraphDB fake.

The fake records every query, write, and chunked safe-insert so the tests
verify the legacy query sequences, the two evaluation slices, the
presentation-column mapping of the es_cache patch, and the settle and flag
cleanup updates.
"""
from __future__ import annotations
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexdocs import MySQLIndexDocRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and canned results.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.safe_inserts = []
        self.results_by_query_id = {}

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

    # Public Method: Record a chunked safe-insert upsert.
    def execute_query_as_safe_inserts_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str,
                                                 key_column_names: list[str], upd_column_names: list[str],
                                                 eval_column_names: list[str], actions: tuple = (),
                                                 table_to_chunk: str | None = None, chunk_filter: str | None = None,
                                                 chunk_size: int | None = None, row_id_name: str | None = None,
                                                 query_id: str | None = None, **kwargs) -> None:
        self.safe_inserts.append({
            "schema"           : schema_name,
            "table"            : table_name,
            "query"            : query,
            "upd_column_names" : upd_column_names,
            "table_to_chunk"   : table_to_chunk,
            "chunk_filter"     : chunk_filter,
            "row_id_name"      : row_id_name,
            "query_id"         : query_id,
        })

    # Public Method: Report that the databases always exist, so the table
    # creation helper short-circuits without exercising DDL.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        return True

    # Public Method: Report that the tables always exist.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str) -> bool:
        return True

    # Public Method: Return the query ids of all captured writes in order.
    def write_query_ids(self) -> list[str | None]:
        return [call["query_id"] for call in self.write_calls]

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow, cache, search, and es schemas."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and graph cache schema.
    def for_graph_cache(self) -> tuple[str, str]:
        return ("coresrv", "graph_cache")

    # Public Method: Return the canned engine and graphsearch test schema.
    def for_graphsearch_test(self) -> tuple[str, str]:
        return ("coresrv", "graphsearch_test")

    # Public Method: Return the canned engine and Elasticsearch cache schema.
    def for_es_cache(self) -> tuple[str, str]:
        return ("coresrv", "es_cache_test")

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration carrying the doc fields."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "graphsearch"  : {"fields" : {"docs" : {"Person" : ["supervisor_en", "supervisor_fr"]}}},
            "elasticsearch": {"fields" : {"docs" : {"Person" : ["external_url_en"]}}},
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo() -> tuple[MySQLIndexDocRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLIndexDocRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        index_config    = FakeIndexConfig(),
    )
    return repo, db

#================================================================#
# Function Group: Graphsearch patch tests                        #
#================================================================#

# Public Function: Verify the graphsearch patch evaluation and commit slices.
def test_patch_graphsearch_eval_and_commit() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["Rj0R4w2q[1/2]"] = [(10, 4)]
    db.results_by_query_id["Rj0R4w2q[2/2]"] = [(5, 2)]

    # Patch the Person docs in eval and commit modes.
    stats = repo.patch(doc_type="Person", actions=('eval', 'commit'))

    # The two evaluation slices sum into the returned stats.
    assert stats.rows_flagged == 6
    assert stats.target == "graphsearch_test.Index_D_Person"

    # The two commit slices run as chunked safe inserts with the legacy ids
    # and the page-profile chunk filters.
    assert [call["query_id"] for call in db.safe_inserts] == ["T4VTvBv6[1/2]", "T4VTvBv6[2/2]"]
    first = db.safe_inserts[0]
    assert first["table_to_chunk"] == "graph_cache.Data_N_Object_T_PageProfile"
    assert first["chunk_filter"] == "object_type = 'Person' AND to_process = 1"
    assert first["row_id_name"] == 'p.row_id'
    assert first["upd_column_names"] == ['include_code_in_name', 'degree_score', 'supervisor_en', 'supervisor_fr']
    assert "n.supervisor_en AS supervisor_en" in first["query"]

# Public Function: Verify the graphsearch patch skips commits without work.
def test_patch_graphsearch_eval_only() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["Rj0R4w2q[1/2]"] = [(10, 0)]
    db.results_by_query_id["Rj0R4w2q[2/2]"] = [(5, 0)]

    # Evaluate the Person docs without drift.
    stats = repo.patch(doc_type="Person", actions=('eval',))

    # Evaluation without drift performs no safe inserts.
    assert stats.rows_flagged == 0
    assert db.safe_inserts == [] and db.write_calls == []

#================================================================#
# Function Group: es_cache patch tests                           #
#================================================================#

# Public Function: Verify the es_cache patch maps the presentation columns.
def test_patch_es_cache_presentation_columns() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["4KpdVwsE[1/2]"] = [(8, 3)]
    db.results_by_query_id["4KpdVwsE[2/2]"] = [(2, 1)]

    # Patch the Person docs in the Elasticsearch cache.
    stats = repo.patch_es_cache(doc_type="Person", actions=('eval', 'commit'))

    # The cache patch targets the es_cache schema with the legacy ids.
    assert stats.rows_flagged == 4
    assert stats.target == "es_cache_test.Index_D_Person"
    assert [call["query_id"] for call in db.safe_inserts] == ["vdEk9bpn[1/2]", "vdEk9bpn[2/2]"]

    # The commit maps the page-profile values onto the presentation columns,
    # applying the include-code option to the names.
    first = db.safe_inserts[0]
    assert "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_en_value), p.name_en_value) AS name_en" in first["query"]
    assert "p.description_long_fr_value AS long_description_fr" in first["query"]
    assert first["upd_column_names"][-1] == "external_url_en"

    # The evaluation compares against the same presentation expressions.
    assert "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_fr_value), p.name_fr_value)" in db.read_calls[0]["query"]

#================================================================#
# Function Group: Settle and cleanup tests                       #
#================================================================#

# Public Function: Verify the settle updates stamp the change records.
def test_settle_updates_change_records() -> None:
    repo, db = _make_repo()

    # Settle the Person change records.
    repo.settle(doc_type="Person", actions=('commit',))

    # Both settle slices run with the legacy ids and the stamping SET clause.
    assert db.write_query_ids() == ["42vKAJcy[1/2]", "42vKAJcy[2/2]"]
    assert "SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0" in db.write_calls[0]["query"]
    assert "INNER JOIN graph_cache.IndexBuildup_Fields_Docs_Person n" in db.write_calls[1]["query"]
    second_filter = "p.to_process = 0" in db.write_calls[1]["query"] and "n.to_process = 1" in db.write_calls[1]["query"]
    assert second_filter

# Public Function: Verify the flag cleanup resets the upstream flags.
def test_cleanup_flags_resets_upstream() -> None:
    repo, db = _make_repo()

    # Clean up the Person flags.
    repo.cleanup_flags(doc_type="Person", actions=('commit',))

    # The page profile flags reset per doc type, then the buildup flags.
    assert db.write_query_ids() == ["P9Caiq8w", "yJ74cRvU"]
    assert "UPDATE graph_cache.Data_N_Object_T_PageProfile" in db.write_calls[0]["query"]
    assert "WHERE object_type = 'Person'" in db.write_calls[0]["query"]
    assert "UPDATE graph_cache.IndexBuildup_Fields_Docs_Person" in db.write_calls[1]["query"]
