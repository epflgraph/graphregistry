# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_cacheprojection.py
"""Unit tests for MySQLCacheProjectionRepository using an in-memory GraphDB fake.

The fake records every query, write, and chunked update so the tests verify
the legacy query sequences, the chunk-filter derivation, the per-type batch
fallback, and the scratch-table lifecycle without a database connection.
"""
from __future__ import annotations
from graphregistry.adapters.persistence.mysql.repositories.rpo_cacheprojection import MySQLCacheProjectionRepository
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and canned results.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.chunked_calls = []
        self.tables_by_key    = {}
        self.tables_existing  = set()
        self.columns_by_table = {}
        self.results_by_query_id  = {}

        # Queue of row batches returned per query id, consumed one batch per
        # call; needed for the per-table row_id existence checks.
        self.sequential_results = {}

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        if query_id in self.sequential_results:
            return self.sequential_results[query_id].pop(0)
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

    # Public Method: Record a chunked update and its derived chunk filter.
    def execute_query_in_chunks(self, engine_name: str, schema_name: str, table_name: str, query: str, chunk_filter: str | None = None, row_id_name: str | None = None, chunk_size: int | None = None, show_progress: bool = False, verbose: bool = False, query_id: str | None = None, desc: str | None = None, **kwargs) -> None:
        self.chunked_calls.append({
            "schema"       : schema_name,
            "table"        : table_name,
            "query"        : query,
            "chunk_filter" : chunk_filter,
            "query_id"     : query_id,
        })

    # Public Method: Return the canned table list of a schema, by regex key.
    def get_tables_in_schema(self, engine_name: str, schema_name: str, use_regex: list | None = None) -> list[str]:
        key = (schema_name, tuple(use_regex) if use_regex else ())
        return self.tables_by_key.get(key, [])

    # Public Method: Report whether a schema-qualified table exists.
    def table_exists(self, engine_name: str, schema_name: str, table_name: str) -> bool:
        return f"{schema_name}.{table_name}" in self.tables_existing

    # Public Method: Report whether a column exists on a table.
    def has_column(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        return column_name in self.columns_by_table.get(f"{schema_name}.{table_name}", [])

    # Public Method: Return the query ids of all captured writes in order.
    def write_query_ids(self) -> list[str | None]:
        return [call["query_id"] for call in self.write_calls]

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow and cache schemas of the test engine."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and graph cache schema.
    def for_graph_cache(self) -> tuple[str, str]:
        return ("coresrv", "graph_cache")

#==================#
# Class Definition #
#==================#
class FakeGlobalConfig:
    """Fixed configuration carrying the traversals schema name."""

    # Public Method: Initialize the fake with the traversals schema.
    def __init__(self) -> None:
        self.schema_traversals = "graph_traversals"

#==================#
# Class Definition #
#==================#
class FakeIndexConfig:
    """Fixed index configuration carrying the doc types and parent-child links."""

    # Public Method: Initialize the fake with the index settings.
    def __init__(self) -> None:
        self.settings = {
            "doc_types"   : ["Person", "Course", "Publication"],
            "graphsearch" : {"fields": {"links": {"parent_child": {"Person": ["Course"], "Course": ["Person"]}}}},
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo() -> tuple[MySQLCacheProjectionRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLCacheProjectionRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        global_config   = FakeGlobalConfig(),
        index_config    = FakeIndexConfig(),
    )

    # Register the buildup tables that exist; Publication has none yet.
    db.tables_existing = {
        "graph_cache.IndexBuildup_Fields_Docs_Person",
        "graph_cache.IndexBuildup_Fields_Docs_Course",
        "graph_cache.IndexBuildup_Fields_Links_ParentChild_Course_Person",
    }

    # Register the discovered scores tables of the cache schema.
    db.tables_by_key[("graph_cache", (r'^Edges_N_Object_N_Object_T_ScoresMatrix_.*_AS$',))] = [
        "Edges_N_Object_N_Object_T_ScoresMatrix_Education_AS",
    ]
    db.tables_by_key[("graph_cache", (r'^Edges_N_Object_N_.*_T_FinalScores$',))] = [
        "Edges_N_Object_N_Concept_T_FinalScores",
    ]
    return repo, db

# Public Function: Build a processing scope covering both flag families.
def _make_scope() -> ProcessingScope:
    return ProcessingScope(
        node_fields_types = ["Person", "Course"],
        node_scores_types = ["Person"],
        edge_type_pairs   = [EdgeTypePair(from_object_type="Person", to_object_type="Course")],
    )

#================================================================#
# Function Group: Propagation tests                              #
#================================================================#

# Public Function: Verify the fields-side query sequence over chunked updates.
def test_propagate_fields_only_chunked_sequence() -> None:
    repo, db = _make_repo()

    # Every propagated table carries a row_id, so all updates run chunked:
    # two page-profile tables, two parent-child directions, two existing
    # buildup doc tables, and one buildup link table.
    db.sequential_results["has-row-id"] = [[1]] * 7
    repo.propagate(scope=_make_scope(), include_fields=True, include_scores=False, actions=('commit',))

    # The page-profile section runs under include_fields but is pre-filtered
    # by the scores-active types, a legacy quirk preserved verbatim.
    assert [call["query_id"] for call in db.chunked_calls] == [
        "zv9J4K0r", "zv9J4K0r", "ct6y8Gz2", "ct6y8Gz2", "RjDjz3fW", "RjDjz3fW", "J4Djz3fW",
    ]

    # No shell writes happen on the fully chunked path.
    assert db.write_calls == []

    # The chunk filter preserves the update conditions as a row_id subquery;
    # the regex substitution keeps the original query's line breaks.
    first_filter = db.chunked_calls[0]["chunk_filter"]
    assert first_filter.startswith("row_id IN (")
    assert "SELECT p.row_id FROM graph_cache.Data_N_Object_T_PageProfile p" in first_filter
    assert "FROM graph_airflow.Operations_N_Object_T_ScoresExpired" in first_filter

    # The edge clause covers both stored directions of the undirected family.
    assert "('Course', 'Person'), ('Person', 'Course')" in db.chunked_calls[2]["query"]

# Public Function: Verify the scores-side scratch table and per-type batches.
def test_propagate_scores_only_temp_table_and_batches() -> None:
    repo, db = _make_repo()

    # The final scores table carries no row_id, so it falls back to batches.
    db.sequential_results["has-row-id"] = [[]]
    repo.propagate(scope=_make_scope(), include_fields=False, include_scores=True, actions=('commit',))

    # The scratch table is created before and dropped after the matrix batches.
    assert db.write_query_ids() == [
        "PropScoresTmpCreate",
        "yzm93BqQ-from-Person",
        "yzm93BqQ-to-Person",
        "PropScoresTmpDrop",
        "q7n3P9xY-Person",
    ]

    # The matrix batches substitute the fixed per-type predicate into the
    # placeholder, implementing the intent the legacy no-op replace missed.
    from_side = next(c for c in db.write_calls if c["query_id"] == "yzm93BqQ-from-Person")
    assert "p.from_object_type = 'Person'" in from_side["query"]
    assert "_tmp_prop_scores_expired" in from_side["query"]
    to_side = next(c for c in db.write_calls if c["query_id"] == "yzm93BqQ-to-Person")
    assert "p.to_object_type = 'Person'" in to_side["query"]

    # The final scores fallback batches on the scores-active type.
    final_batch = next(c for c in db.write_calls if c["query_id"] == "q7n3P9xY-Person")
    assert "WHERE p.object_type = 'Person' AND" in final_batch["query"]

# Public Function: Verify that eval mode counts without committing updates.
def test_propagate_eval_mode_counts_and_skips_writes() -> None:
    repo, db = _make_repo()

    # Queue the eval counts: page profile twice, parent-child twice,
    # buildup docs twice, links once.
    db.sequential_results["zv9J4K0r"] = [[(6,)], [(8,)]]
    db.sequential_results["ct6y8Gz2"] = [[(5,)], [(3,)]]
    db.sequential_results["RjDjz3fW"] = [[(2,)], [(4,)]]
    db.sequential_results["J4Djz3fW"] = [[(1,)]]
    stats = repo.propagate(scope=_make_scope(), include_fields=True, include_scores=False, actions=('eval',))

    # No chunked or shell writes happen for the fields side in eval mode.
    assert db.chunked_calls == []
    assert db.write_calls == []

    # The stats carry the counted rows per target and direction.
    assert len(stats) == 7
    assert [s.rows_flagged for s in stats if "ParentChildSymmetric" in s.target] == [5, 3]
    counted = {s.target: s.rows_flagged for s in stats}
    assert counted["graph_cache.Data_N_Object_T_PageProfile"] == 6
    assert counted["graph_cache.IndexBuildup_Fields_Docs_Person"] == 2

# Public Function: Verify that eval mode still manages the scores scratch table.
def test_propagate_scores_eval_creates_scratch_table() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["yzm93BqQ"] = [(7,)]
    stats = repo.propagate(scope=_make_scope(), include_fields=False, include_scores=True, actions=('eval',))

    # The legacy quirk is preserved: the scratch table is materialized and
    # dropped even in eval mode, because the eval counts read from it.
    assert db.write_query_ids() == ["PropScoresTmpCreate", "PropScoresTmpDrop"]
    assert stats[0].rows_flagged == 7

# Public Function: Verify the per-type fallback when a table has no row_id.
def test_propagate_fallback_batches_without_row_id() -> None:
    repo, db = _make_repo()

    # No propagated table carries a row_id, so all updates run batched.
    db.sequential_results["has-row-id"] = [[]] * 7
    repo.propagate(scope=_make_scope(), include_fields=True, include_scores=False, actions=('commit',))

    # The batched writes carry per-type or per-pair query ids.
    assert db.chunked_calls == []
    batch_ids = db.write_query_ids()

    # Page profile batches per scores-active type, over two tables.
    assert batch_ids[:2] == ["zv9J4K0r-Person", "zv9J4K0r-Person"]

    # Parent-child and buildup links batch over both stored directions.
    assert "ct6y8Gz2-Course-Person" in batch_ids and "ct6y8Gz2-Person-Course" in batch_ids
    assert "J4Djz3fW-Course-Person" in batch_ids and "J4Djz3fW-Person-Course" in batch_ids

    # Buildup docs batch per doc type.
    assert "RjDjz3fW-Person" in batch_ids and "RjDjz3fW-Course" in batch_ids

    # The batch predicate is inserted before the original conditions.
    type_batch = next(c for c in db.write_calls if c["query_id"] == "RjDjz3fW-Person")
    assert "WHERE p.doc_type = 'Person' AND" in type_batch["query"]

# Public Function: Verify that empty scopes skip their sections entirely.
def test_propagate_empty_scope_skips_everything() -> None:
    repo, db = _make_repo()
    empty = ProcessingScope()
    stats = repo.propagate(scope=empty, include_fields=True, include_scores=True, actions=('commit',))
    assert stats == []
    assert db.write_calls == [] and db.chunked_calls == []

#================================================================#
# Function Group: Reset tests                                    #
#================================================================#

# Public Function: Verify the cache and traversals resets over discovered tables.
def test_reset_flags_cache_and_traversals() -> None:
    repo, db = _make_repo()

    # Register the flaggable tables of both schemas; _hidden is skipped by
    # name and PageProfileExtra lacks the to_process column.
    db.tables_by_key[("graph_cache", ())] = [
        "Data_N_Object_T_PageProfile", "_hidden_table", "Nodes_N_Object_T_DegreeScores", "PageProfileExtra",
    ]
    db.columns_by_table["graph_cache.Data_N_Object_T_PageProfile"] = ["to_process"]
    db.columns_by_table["graph_cache.Nodes_N_Object_T_DegreeScores"] = ["to_process"]
    db.tables_by_key[("graph_traversals", ())] = ["Traversal_Doc_Ranks"]
    db.columns_by_table["graph_traversals.Traversal_Doc_Ranks"] = ["to_process"]

    # Reset both projection families.
    repo.reset_flags()

    # Two cache tables and one traversals table are reset with legacy ids.
    assert db.write_query_ids() == ["DFEkXX4A", "DFEkXX4A", "X7vYqZ3A"]
    assert "UPDATE graph_cache.Data_N_Object_T_PageProfile SET to_process = 0 WHERE to_process = 1;" in [c["query"] for c in db.write_calls][0]

# Public Function: Verify the selective reset of a single schema family.
def test_reset_flags_traversals_only() -> None:
    repo, db = _make_repo()
    db.tables_by_key[("graph_traversals", ())] = ["Traversal_Doc_Ranks"]
    db.columns_by_table["graph_traversals.Traversal_Doc_Ranks"] = ["to_process"]

    # Only the traversals schema is reset when the cache is excluded.
    repo.reset_flags(include_cache=False)
    assert db.write_query_ids() == ["X7vYqZ3A"]
