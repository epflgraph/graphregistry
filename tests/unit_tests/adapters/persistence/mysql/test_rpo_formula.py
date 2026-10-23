# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_formula.py
"""Unit tests for MySQLFormulaRepository using in-memory fakes.

The fakes record every query, write, and safe-insert so the tests verify the
template resolution, the execution-type dispatch, the folder families, and
the materialised views.
"""
from __future__ import annotations
from pathlib import Path
import graphregistry.adapters.persistence.mysql.repositories.rpo_formula as formula_module
from graphregistry.adapters.persistence.mysql.repositories.rpo_formula import MySQLFormulaRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty logs.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.safe_inserts = []
        self.chunked_calls = []
        self.columns_existing = set()

    # Public Method: Record a read query and return no rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"query": query, "query_id": query_id})
        return []

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"query": query, "query_id": query_id})

    # Public Method: Record a safe-insert upsert.
    def execute_query_as_safe_inserts(self, engine_name: str, schema_name: str, table_name: str, query: str,
                                       key_column_names: list[str], upd_column_names: list[str],
                                       eval_column_names: list[str], actions: tuple = (),
                                       verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.safe_inserts.append({"table": table_name, "query_id": query_id, "key_columns": key_column_names})

    # Public Method: Record a chunked statement execution.
    def execute_sql_statements_in_chunks(self, engine_name: str, sql: str, chunk_size: int = 100000,
                                          show_progress: bool = True, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.chunked_calls.append({"query_id": query_id, "chunk_size": chunk_size})

    # Public Method: Report whether a column exists.
    def column_exists(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        return (schema_name, table_name, column_name) in self.columns_existing

    # Public Method: Return empty column lists.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list[str]:
        return ["object_type", "object_id", "row_id"]

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
class FakeGlobalConfig:
    """Fixed configuration carrying the schema names."""

    # Public Method: Initialize the fake with the schema map.
    def __init__(self) -> None:
        self.schema_registry = "graph_registry"
        self.schema_lectures = "graph_lectures"
        self.schema_ontology = "graph_ontology"
        self.mysql_schema_names = {"coresrv": {
            "ontology": "graph_ontology", "registry": "graph_registry", "lectures": "graph_lectures",
            "airflow": "graph_airflow", "traversals": "graph_traversals", "es_cache": "es_cache_test",
            "graph_cache": "graph_cache", "graphsearch": "graphsearch_test", "prod_mirror": "prod_mirror_test",
        }}

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database and formula tree.
def _make_repo(tmp_path) -> tuple[MySQLFormulaRepository, FakeGraphDB]:
    db = FakeGraphDB()
    for schema in ("graph_registry", "graph_lectures", "graph_ontology", "graph_cache"):
        db.columns_existing.update([
            (schema, "Data_N_Object_N_Object_T_CustomFields", "record_deleted"),
            (schema, "Data_N_Object_T_PageProfile", "record_deleted"),
            (schema, "Edges_N_Object_N_Object_T_ChildToParent", "record_deleted"),
        ])
    db.columns_existing.update([
        ("graph_cache", "Data_N_Object_N_Object_T_CalculatedFields", "deleted"),
        ("graph_cache", "Data_N_Object_T_CalculatedFields", "deleted"),
    ])

    # Redirect the formulas root into the test tree.
    formula_module.SQL_FORMULAS_PATH = tmp_path
    repo = MySQLFormulaRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        global_config   = FakeGlobalConfig(),
    )
    return repo, db

# Public Function: Write one formula file into the test tree.
def _write_formula(folder: str, name: str, sql: str, tmp_path) -> None:
    folder_path = tmp_path / folder
    folder_path.mkdir(parents=True, exist_ok=True)
    (folder_path / f"formula.{name}.sql").write_text(sql)

#================================================================#
# Function Group: Formula execution tests                        #
#================================================================#

# Public Function: Verify the SELECT templates upsert as safe inserts.
def test_select_formula_runs_as_safe_inserts(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)
    _write_formula("calculated_fields/obj", "person.node_degree",
                   "SELECT '[[registry]]' AS object_type, 'p1' AS object_id, 'n/a' AS field_language, 'node_degree' AS field_name, 5 AS field_value", tmp_path)

    applied = repo.apply_formulas_from_folder("calculated_fields/obj", actions=('commit',))

    # The schema placeholder is resolved and the upsert targets the object
    # calculated-fields table with the legacy query id.
    assert applied == ["person.node_degree"]
    assert len(db.safe_inserts) == 1
    insert = db.safe_inserts[0]
    assert insert["query_id"] == 'D9NxAGY2'
    assert insert["table"] == 'Data_N_Object_T_CalculatedFields'
    assert insert["key_columns"] == ['object_type', 'object_id']

# Public Function: Verify the write templates execute directly.
def test_update_formula_runs_directly(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)
    _write_formula("graph_traversals", "001.unit-person.affiliation",
                   "UPDATE [[graph_cache]].Data_N_Object_T_PageProfile SET to_process = 1;", tmp_path)

    repo.apply_formulas_from_folder("graph_traversals", actions=('commit',))

    # The write template executes in the shell with the legacy query id.
    assert db.safe_inserts == []
    assert [call["query_id"] for call in db.write_calls] == ['Neg00cQJ']
    assert "UPDATE graph_cache.Data_N_Object_T_PageProfile" in db.write_calls[0]["query"]

# Public Function: Verify the by-path resolution applies the aliases.
def test_apply_formula_by_path_resolves_aliases(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)
    _write_formula("graph_traversals", "x.y", "UPDATE [[graph_cache]].t SET c = 1;", tmp_path)

    # The 'traversals' alias resolves to 'graph_traversals'.
    repo.apply_formula_by_path("traversals/formula.x.y", actions=('commit',))
    assert [call["query_id"] for call in db.write_calls] == ['Neg00cQJ']

# Public Function: Verify the named families apply their folders.
def test_named_families_apply_folders(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)
    for folder in ("calculated_fields/obj", "calculated_fields/obj2obj", "graph_traversals",
                   "calculated_scores/obj2ontology/concepts", "calculated_scores/obj2ontology/concepts_union",
                   "calculated_scores/obj2ontology/categories", "calculated_scores/obj2ontology/categories_union",
                   "calculated_scores/degree_scores", "data_reset"):
        _write_formula(folder, "f", "UPDATE [[graph_cache]].t SET c = 1;", tmp_path)

    # The calculated-field, traversal, and scoring families run their folders.
    repo.apply_calculated_field_formulas(actions=('commit',))
    repo.apply_traversals(actions=('commit',))
    repo.apply_scoring_formulas(actions=('commit',))
    assert len(db.write_calls) == 2 + 1 + 5

    # The reset family runs its formulas in chunks.
    repo.apply_data_reset_formulas(actions=('commit',))
    assert len(db.chunked_calls) == 1
    assert db.chunked_calls[0]["chunk_size"] == 100000

#================================================================#
# Function Group: View tests                                     #
#================================================================#

# Public Function: Verify the views materialize with the legacy ids.
def test_materialize_views_runs_all_four(tmp_path) -> None:
    repo, db = _make_repo(tmp_path)

    repo.materialize_views(actions=('eval', 'commit'))

    # The four views evaluate and commit with the legacy query ids.
    assert [call["query_id"] for call in db.read_calls] == ['8nZVFGbc'] * 4
    assert [call["query_id"] for call in db.write_calls] == ['Mn0to7TQ'] * 4

    # The symmetric view unions both directions and joins the type flags.
    first_commit = db.write_calls[0]["query"]
    assert "REPLACE INTO graph_cache.Data_N_Object_N_Object_T_AllFieldsSymmetric" in first_commit
    assert "UNION ALL" in first_commit
    assert "Operations_N_Object_N_Object_T_TypeFlags" in first_commit

    # The page-profile view selects the presentation columns.
    second_commit = db.write_calls[1]["query"]
    assert "REPLACE INTO graph_cache.Data_N_Object_T_PageProfile" in second_commit
    assert "pp.external_url_it, pp.is_visible" in second_commit
    assert "tf.flag_type = 'fields'" in second_commit

    # The parent-child view unions both edge directions.
    fourth_commit = db.write_calls[3]["query"]
    assert "REPLACE INTO graph_cache.Edges_N_Object_N_Object_T_ParentChildSymmetric" in fourth_commit
    assert "'Child-to-Parent'" in fourth_commit and "'Parent-to-Child'" in fourth_commit
