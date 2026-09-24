# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_scoresmatrix.py
"""Unit tests for MySQLScoresMatrixRepository using an in-memory GraphDB fake.

The fake records every query and write so the tests verify the legacy query
sequences, the threshold substitution, the branch selection of the
consolidation, and the pair derivation of the combined update.
"""
from __future__ import annotations
import pytest
from graphregistry.adapters.persistence.mysql.repositories.rpo_scoresmatrix import MySQLScoresMatrixRepository
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.pipeline.mdl_scores import ScoreConsolidationParams
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and canned results.
    def __init__(self) -> None:
        self.read_calls  = []
        self.write_calls = []
        self.results_by_query_id = {}

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

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
    """Fixed configuration carrying the ontology schema name."""

    # Public Method: Initialize the fake with the ontology schema.
    def __init__(self) -> None:
        self.schema_ontology = "graph_ontology"

#==================#
# Class Definition #
#==================#
class FakeScoresConfig:
    """Fixed scores configuration carrying the matrix mapping and tuples."""

    # Public Method: Initialize the fake with the scores settings.
    def __init__(self) -> None:
        self.settings = {
            'scored_edge_tuple_to_class_mapping' : {
                ('Course', 'Person')         : 'education',
                ('Person', 'Publication')     : 'research',
                ('Category', 'Category')      : 'education',
            },
            'scored_edge_tuples' : {
                'education' : [['Course', 'Person'], ['Category', 'Category']],
                'research'  : [['Person', 'Publication']],
            },
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo() -> tuple[MySQLScoresMatrixRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLScoresMatrixRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        global_config   = FakeGlobalConfig(),
        scores_config   = FakeScoresConfig(),
    )
    return repo, db

#================================================================#
# Function Group: Table naming tests                             #
#================================================================#

# Public Function: Verify the matrix table naming across the domains.
def test_matrix_table_names() -> None:
    repo, _db = _make_repo()

    # Education and research families resolve through the config mapping.
    assert repo._matrix_table_name(EdgeTypePair(from_object_type="Person", to_object_type="Course"), 'GBC') == "Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC"
    assert repo._matrix_table_name(EdgeTypePair(from_object_type="Person", to_object_type="Publication"), 'AS') == "Edges_N_Object_N_Object_T_ScoresMatrix_Research_AS"

    # Ontology families always use the Ontology tables, including same-type.
    assert repo._matrix_table_name(EdgeTypePair(from_object_type="Person", to_object_type="Category"), 'GBC') == "Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_GBC"
    assert repo._matrix_table_name(EdgeTypePair(from_object_type="Category", to_object_type="Category"), 'AS') == "Edges_N_Object_N_Object_T_ScoresMatrix_Ontology_AS"

    # Unknown families raise the legacy ValueError.
    with pytest.raises(ValueError, match="No corresponding scores matrix table"):
        repo._matrix_table_name(EdgeTypePair(from_object_type="Lecture", to_object_type="Person"), 'GBC')

#================================================================#
# Function Group: Calculation tests                              #
#================================================================#

# Public Function: Verify the calculation queries and threshold substitution.
def test_calculate_matrix_queries_and_thresholds() -> None:
    repo, db = _make_repo()
    params = ScoreConsolidationParams(score_threshold=0.2, min_shared_concepts=6)
    db.results_by_query_id["f3LmCRzV"] = [("Course", "Person", 42)]

    # Calculate with the custom thresholds in eval and commit modes.
    stats = repo.calculate_matrix(
        type_pair = EdgeTypePair(from_object_type="Person", to_object_type="Course"),
        params    = params,
        actions   = ('eval', 'commit'),
    )

    # The eval count feeds the returned stats.
    assert stats is not None and stats.rows_flagged == 42
    assert stats.target == "graph_cache.Edges_N_Object_N_Object_T_ScoresMatrix_Education_GBC"

    # The commit query carries the substituted thresholds, the FORCE INDEX
    # hint, and the double-quoted type predicates of the legacy query.
    commit = db.write_calls[0]
    assert commit["query_id"] == 'wAbL4D8i'
    assert "e1.score >= 0.2" in commit["query"] and "e2.score >= 0.2" in commit["query"]
    assert "COUNT(DISTINCT e1.concept_id) >= 6" in commit["query"]
    assert "FORCE INDEX (idx_concept_type_proc_score)" in commit["query"]
    assert 'e1.object_type = "Course"' in commit["query"]

    # The eval query estimates the cross-type pair counts from both sides.
    assert "t1.n_to_process * t2.n_count + t1.n_count * t2.n_to_process" in db.read_calls[0]["query"]

# Public Function: Verify the ontology exclusion of the calculation.
def test_calculate_matrix_ontology_exclusion() -> None:
    repo, db = _make_repo()

    # Object-to-ontology families are excluded from calculation.
    assert repo.calculate_matrix(EdgeTypePair(from_object_type="Concept", to_object_type="Person"), ScoreConsolidationParams(), actions=('commit',)) is None
    assert db.read_calls == [] and db.write_calls == []

    # Category-to-Category is the single ontology exception that runs.
    stats = repo.calculate_matrix(EdgeTypePair(from_object_type="Category", to_object_type="Category"), ScoreConsolidationParams(), actions=('commit',))
    assert stats is not None and db.write_query_ids() == ['wAbL4D8i']

# Public Function: Verify that eval mode calculates without writing.
def test_calculate_matrix_eval_mode_writes_nothing() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["f3LmCRzV"] = [("Course", "Person", 7)]
    stats = repo.calculate_matrix(EdgeTypePair(from_object_type="Person", to_object_type="Course"), ScoreConsolidationParams(), actions=('eval',))
    assert stats is not None and stats.rows_flagged == 7
    assert db.write_calls == []

#================================================================#
# Function Group: Consolidation tests                            #
#================================================================#

# Public Function: Verify the ontology branch copies the pre-calculated scores.
def test_consolidate_matrix_ontology_branch() -> None:
    repo, db = _make_repo()
    repo.consolidate_matrix(
        type_pair = EdgeTypePair(from_object_type="Concept", to_object_type="Person"),
        params    = ScoreConsolidationParams(),
        actions   = ('commit',),
    )

    # The ontology type names the final scores table, the id column, and the
    # constant to_object_type of the copied rows.
    query = db.write_calls[0]["query"]
    assert db.write_query_ids() == ['qHE7tP6J']
    assert "Edges_N_Object_N_Concept_T_FinalScores" in query
    assert "concept_id AS to_object_id" in query
    assert "'Concept' AS to_object_type" in query

# Public Function: Verify the concept-to-concept branch joins both endpoints.
def test_consolidate_matrix_concept_concept_branch() -> None:
    repo, db = _make_repo()
    repo.consolidate_matrix(
        type_pair = EdgeTypePair(from_object_type="Concept", to_object_type="Concept"),
        params    = ScoreConsolidationParams(score_threshold=0.3),
        actions   = ('commit',),
    )

    # The undirected concept edges are joined with the scores expiry state of
    # both endpoints, filtered by the substituted threshold.
    query = db.write_calls[0]["query"]
    assert db.write_query_ids() == ['qHE7tP6J']
    assert "graph_ontology.Edges_N_Concept_N_Concept_T_Undirected" in query
    assert "s1.to_process OR s2.to_process" in query
    assert "normalised_score >= 0.3" in query

# Public Function: Verify the adjusted-scores branch with average refresh.
def test_consolidate_matrix_adjusted_with_averages() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["fTaH8sTj"] = [("Course", "Person", 0.05, 900)]
    repo.consolidate_matrix(
        type_pair       = EdgeTypePair(from_object_type="Person", to_object_type="Course"),
        params          = ScoreConsolidationParams(),
        update_averages = True,
        actions         = ('commit',),
    )

    # The averages refresh, the existence check, and the sigmoid-normalised
    # adjusted scores run in the legacy order.
    assert db.write_query_ids() == ['gs1ieZYM', 'qHE7tP6J']
    assert db.read_calls[0]["query_id"] == 'fTaH8sTj'
    assert "AVG(score) AS avg_score" in db.write_calls[0]["query"]
    assert "2/(1 + EXP(-gb.score/(4 * av.avg_score))) - 1" in db.write_calls[1]["query"]

# Public Function: Verify the average refresh is optional.
def test_consolidate_matrix_adjusted_without_averages() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["fTaH8sTj"] = [("Course", "Person", 0.05, 900)]
    repo.consolidate_matrix(
        type_pair       = EdgeTypePair(from_object_type="Person", to_object_type="Course"),
        params          = ScoreConsolidationParams(),
        update_averages = False,
        actions         = ('commit',),
    )

    # Without the averages refresh only the adjusted scores are written.
    assert db.write_query_ids() == ['qHE7tP6J']

# Public Function: Verify the missing-average guard skips the family.
def test_consolidate_matrix_missing_average_skips() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["fTaH8sTj"] = []
    repo.consolidate_matrix(
        type_pair       = EdgeTypePair(from_object_type="Person", to_object_type="Course"),
        params          = ScoreConsolidationParams(),
        update_averages = True,
        actions         = ('commit',),
    )

    # The averages are refreshed but the adjusted scores are skipped.
    assert db.write_query_ids() == ['gs1ieZYM']

#================================================================#
# Function Group: Combined update tests                          #
#================================================================#

# Public Function: Verify the pair derivation and pass order of the update.
def test_update_matrix_pair_derivation() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["fTaH8sTj"] = [("Course", "Person", 0.05, 900)]
    scope = ProcessingScope(node_scores_types=["Course", "Person", "Category"])

    # Run the combined update over the three scores-active types.
    repo.update_matrix(scope=scope, params=ScoreConsolidationParams(), actions=('commit',))

    # Active families: combinations of the three scores-active types,
    # restricted to the configured and ontology families: the four pairs
    # below. Person-Publication is configured but not active.
    # Calculation runs only for the non-ontology pairs (Category-Category and
    # Course-Person); consolidation runs for all four.
    assert db.write_query_ids() == [
        'wAbL4D8i',              # Category-Category calculation
        'wAbL4D8i',              # Course-Person calculation
        'gs1ieZYM', 'qHE7tP6J',  # Category-Category consolidation (adjusted)
        'qHE7tP6J',              # Category-Course consolidation (ontology copy)
        'qHE7tP6J',              # Category-Person consolidation (ontology copy)
        'gs1ieZYM', 'qHE7tP6J',  # Course-Person consolidation (adjusted)
    ]

    # The average existence check runs once per adjusted family.
    assert [call["query_id"] for call in db.read_calls] == ['fTaH8sTj', 'fTaH8sTj']

# Public Function: Verify that an empty scope does nothing.
def test_update_matrix_empty_scope() -> None:
    repo, db = _make_repo()
    repo.update_matrix(scope=ProcessingScope(), params=ScoreConsolidationParams(), actions=('commit',))
    assert db.read_calls == [] and db.write_calls == []
