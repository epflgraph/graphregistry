# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_indexintegrity.py
"""Unit tests for MySQLIndexIntegrityRepository using an in-memory GraphDB fake.

The fake records every query, write, and file execution so the tests verify
the five-step pipeline order, the union-find component computation, the
table classification of the cleanup, and the cache invalidation.
"""
from __future__ import annotations
import os
import graphregistry.adapters.persistence.mysql.repositories.rpo_indexintegrity as integrity_module
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexintegrity import MySQLIndexIntegrityRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty logs and canned results.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.file_calls   = []
        self.tables_by_key     = {}
        self.columns_by_table  = {}
        self.results_by_query_id = {}
        self.written_files     = {}

    # Public Method: Record a read query and return the canned rows; the
    # verification counts carry no query id and default to a zero row, as
    # the real database always returns one row for COUNT(*).
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, schema_name: str | None = None, verbose: bool = False, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        if query_id is None:
            return [(0,)]
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

    # Public Method: Record a file-executed query batch and capture its file.
    def execute_query_from_file(self, engine_name: str, file_path: str, verbose: bool = False, **kwargs) -> None:
        with open(file_path, 'r') as f:
            self.written_files[file_path] = f.read()
        self.file_calls.append({"engine": engine_name, "file_path": file_path})

    # Public Method: Return the canned table list of a schema, by regex key.
    def get_tables_in_schema(self, engine_name: str, schema_name: str, use_regex: list | bool = False, include_views: bool = True, filter_by: bool = False) -> list[str]:
        key = (schema_name, tuple(use_regex) if use_regex else ())
        return self.tables_by_key.get(key, [])

    # Public Method: Return the canned column list of a table.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list[str]:
        return self.columns_by_table.get(f"{schema_name}.{table_name}", [])

    # Public Method: Report whether a table exists.
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
class FakeGlobalConfig:
    """Fixed configuration carrying the data schema names."""

    # Public Method: Initialize the fake with the registry schemas.
    def __init__(self) -> None:
        self.schema_registry = "graph_registry"
        self.schema_lectures = "graph_lectures"
        self.schema_ontology = "graph_ontology"

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo(tmp_path) -> tuple[MySQLIndexIntegrityRepository, FakeGraphDB]:
    db = FakeGraphDB()

    # Redirect the pickle and SQL file paths into the test's temp directory
    # so the tests never touch the real /tmp locations.
    integrity_module.PICKLE_DIR = str(tmp_path / "pickles")
    integrity_module.LARGEST_COMPONENT_SQL_PATH = str(tmp_path / "component.sql")

    # Step 0: one SEM table with overflow rows.
    db.tables_by_key[("graphsearch_test", (r'^Index_D_[^_]*_L_[^_]*_T_SEM+$',))] = ["Index_D_Person_L_Course_T_SEM"]
    db.results_by_query_id['rr99cnt'] = [("Person", "Course", 4)]

    # Step 1: two deleted nodes in the page profile.
    db.columns_by_table["graphsearch_test.Data_N_Object_T_PageProfile"] = ["object_type", "object_id", "deleted"]
    db.results_by_query_id['ppdleval'] = [("Person", 2)]

    # Step 2: the graph cache is missing, so the component is computed from
    # a two-node, one-edge graph.
    db.results_by_query_id['ndmap'] = [("Person|p1", 1), ("Course|c1", 2)]
    db.tables_by_key[("graphsearch_test", (r'^Index_D_[^_]*_L_[^_]*',))] = ["Index_D_Person_L_Course_T_ORG"]
    db.results_by_query_id['edtbl'] = [("Course|c1", "Person|p1")]

    # Step 4: one doc table with loose rows in graphsearch.
    db.tables_by_key[("graphsearch_test", ())] = ["Index_D_Person", "_private", "Data_N_Object_T_PageProfile"]
    db.columns_by_table["graphsearch_test.Index_D_Person"] = ["doc_type", "doc_id", "row_id"]
    db.results_by_query_id['DFSHG4tf'] = [("Person", 3)]

    # Step 5: one doclink table with a dangling reference.
    db.tables_by_key[("graphsearch_test", (r'^Index_D_[^_]+_L_[^_]+',))] = ["Index_D_Person_L_Course_T_ORG"]
    db.tables_by_key[("graphsearch_test", (r'^Index_D_[^_]*$',))] = ["Index_D_Person", "Index_D_Course"]
    db.results_by_query_id['dicidxeval'] = [(1,)]

    # The es_cache schema has no tables, so its steps are no-ops.
    repo = MySQLIndexIntegrityRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        global_config   = FakeGlobalConfig(),
    )
    return repo, db

#================================================================#
# Function Group: Pipeline tests                                 #
#================================================================#

# Public Function: Verify the five steps run in the legacy order.
def test_delete_loose_ends_pipeline_order(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)

    # Run the full pruning pipeline in commit mode.
    stats = repo.delete_loose_ends(refresh_graph=True, actions=('commit',))

    # The steps run in order: overflow delete, profile temp table, profile
    # delete, cache invalidation, component write, schema cleanup, and the
    # dangling reference delete. The cleanup covers both the doc table and
    # the page profile itself, which classifies as an object table.
    ids = db.write_query_ids()
    assert ids[0] == 'rr99del'
    assert 'vnsrctbl' in ids and 'ppdlcommit' in ids and 'vnsrcdrp' in ids
    assert 'lccinv' in ids
    assert ids[-3:] == ['s5DfH2Lk', 's5DfH2Lk', 'dicidxcommit']

    # The stats aggregate the counted rows across the steps.
    assert stats.rows_flagged == 4 + 2 + 3 + 3 + 1

    # The component write goes through the SQL file in chunks.
    assert len(db.file_calls) == 1
    component_sql = db.written_files[db.file_calls[0]["file_path"]]
    assert "TRUNCATE TABLE graph_cache.Operations_N_Object_T_LargestConnectedGraph;" in component_sql
    assert "('Person', 'p1')" in component_sql and "('Course', 'c1')" in component_sql

# Public Function: Verify the no-op commit does not invalidate the caches.
def test_delete_loose_ends_noop_skips_invalidation(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)

    # With no page-profile rows to delete, the graph caches stay untouched.
    db.results_by_query_id['ppdleval'] = []
    repo.delete_loose_ends(refresh_graph=True, actions=('commit',))
    assert 'lccinv' not in db.write_query_ids()

# Public Function: Verify the cached graph is reused when allowed.
def test_delete_loose_ends_reuses_cached_graph(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)

    # With a non-empty cached component table and no refresh, the node
    # mapping and edge fetches never run.
    db.tables_by_key[("graph_cache", (r'^Operations_N_Object_T_LargestConnectedGraph$',))] = ["Operations_N_Object_T_LargestConnectedGraph"]
    db.results_by_query_id['cache_chk'] = [("Person", "p1")]
    repo.delete_loose_ends(refresh_graph=False, actions=('commit',))
    assert 'ndmap' not in [call["query_id"] for call in db.read_calls]
    assert 'edtbl' not in [call["query_id"] for call in db.read_calls]
