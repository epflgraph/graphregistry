# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_indexbuildup.py
"""Unit tests for MySQLIndexBuildupRepository using an in-memory GraphDB fake.

The fake records every query and write so the tests verify the query helper
construction with per-language field suffixes, the degree-score coalescing,
the canonical pair and context resolution, and the table-creation asymmetry
between the two builds.
"""
from __future__ import annotations
from graphregistry.adapters.persistence.mysql.repositories.rpo_indexbuildup import MySQLIndexBuildupRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs.
    def __init__(self) -> None:
        self.read_calls  = []
        self.write_calls = []
        self.missing_tables = set()
        self.results_by_query_id = {}

    # Public Method: Report that the databases always exist.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        return True

    # Public Method: Serve a canned create-statement for the GraphTable.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        if "CREATE TABLE" in query.upper():
            import re as _re
            match = _re.search(r"TABLE IF NOT EXISTS (\S+)\.(\S+)", query)
            if match:
                self.missing_tables.discard(f"{match.group(1)}.{match.group(2)}")

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

    # Public Method: Report that the databases always exist.
    def database_exists(self, engine_name: str, schema_name: str) -> bool:
        return True

    # Public Method: Report whether a table exists.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str) -> bool:
        return f"{schema_name}.{table_name}" not in self.missing_tables

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow and cache schemas."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and graph cache schema.
    def for_graph_cache(self) -> tuple[str, str]:
        return ("coresrv", "graph_cache")

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration carrying the raw field lists."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "options"     : {"include_code_in_name": {"Course": 1}},
            "graphsearch" : {"fields" : {
                "docs_raw"         : {"Course": [["en", "supervisor"], "acronym", ["fr", "resume"]]},
                "links"            : {"parent_child_raw": {"Person": {"Course": [["en", "context_note"]]}}},
            }},
            "edge_selection_contexts" : {("Course", "Person"): "teacher"},
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo(*_args) -> tuple[MySQLIndexBuildupRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLIndexBuildupRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        index_config    = FakeIndexConfig(),
    )
    return repo, db

#================================================================#
# Function Group: Docs build tests                               #
#================================================================#

# Public Function: Verify the docs build joins the per-language fields.
def test_build_docs_fields_language_suffixes() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["hpFZ8RAT"] = [("Course", 12)]

    # Build the Course docs in eval and commit modes.
    stats = repo.build_docs_fields(doc_type="Course", actions=('eval', 'commit'))

    # The eval counts the flagged profiles and the commit replaces the rows.
    assert stats.rows_flagged == 12
    assert stats.target == "graph_cache.IndexBuildup_Fields_Docs_Course"
    assert [call["query_id"] for call in db.write_calls] == ['F1ArYGKd']
    commit = db.write_calls[0]["query"]

    # The raw fields carry their language suffixes in names and joins.
    assert "supervisor_en" in commit and "acronym" in commit and "resume_fr" in commit
    assert "t1.field_value AS supervisor_en" in commit
    assert "LEFT JOIN graph_cache.Data_N_Object_T_AllFields t1" in commit
    assert "t1.field_name) = ('Course', p.object_id, 'en', 'supervisor')" in commit

    # The include-code option and the degree-score coalescing are applied.
    assert "1 AS include_code_in_name" in commit
    assert "COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score" in commit
    assert "1 AS to_process" in commit

# Public Function: Verify the docs build creates its target table.
def test_build_docs_fields_creates_target_table(monkeypatch) -> None:
    repo, db = _make_repo()
    creation_calls = []

    # Spy on the shared creation helper; the docs build must ensure its
    # target exists, compensating for the legacy's constructor DDL.
    import graphregistry.adapters.persistence.mysql.repositories.rpo_indexbuildup as buildup_module
    monkeypatch.setattr(
        buildup_module,
        "create_table_if_not_exists",
        lambda db, engine, schema, table: creation_calls.append((engine, schema, table)),
    )

    # Build the Course docs in commit mode.
    repo.build_docs_fields(doc_type="Course", actions=('commit',))

    # The target table creation precedes the replace.
    assert ("coresrv", "graph_cache", "IndexBuildup_Fields_Docs_Course") in creation_calls
    assert [call["query_id"] for call in db.write_calls] == ['F1ArYGKd']
    assert "REPLACE INTO graph_cache.IndexBuildup_Fields_Docs_Course" in db.write_calls[0]["query"]

#================================================================#
# Function Group: Links build tests                              #
#================================================================#

# Public Function: Verify the links build canonicalises and joins symmetric.
def test_build_links_parentchild_canonical_and_context() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["6D05nXQL"] = [("Course", "Person", 7)]

    # Build the flipped Person-Course pair in eval and commit modes.
    stats = repo.build_links_parentchild(doc_type="Person", link_type="Course", actions=('eval', 'commit'))

    # The eval counts the flagged edges and the commit replaces the rows.
    assert stats.rows_flagged == 7
    assert stats.target == "graph_cache.IndexBuildup_Fields_Links_ParentChild_Course_Person"
    assert [call["query_id"] for call in db.write_calls] == ['gEzB7UwD']
    commit = db.write_calls[0]["query"]

    # The canonical pair and its context drive the edge selection.
    assert "(s.from_object_type, s.to_object_type) = ('Course', 'Person')" in commit
    assert "s.context = 'teacher'" in commit
    assert "Edges_N_Object_N_Object_T_ParentChildSymmetric s" in commit

    # The custom fields join the symmetric all-fields table.
    assert "Data_N_Object_N_Object_T_AllFieldsSymmetric t1" in commit
    assert "t1.field_value AS context_note_en" in commit

# Public Function: Verify the links build skips pairs without a context.
def test_build_links_parentchild_no_context_skips() -> None:
    repo, db = _make_repo()

    # Build the Exercise-Person pair without a context.
    stats = repo.build_links_parentchild(doc_type="Exercise", link_type="Person", actions=('commit',))
    assert stats.rows_flagged == 0
    assert db.write_calls == []
