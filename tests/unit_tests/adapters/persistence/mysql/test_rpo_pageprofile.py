# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_pageprofile.py
"""Unit tests for MySQLPageProfileRepository using an in-memory GraphDB fake."""
from __future__ import annotations
from graphregistry.adapters.persistence.mysql.repositories.rpo_pageprofile import MySQLPageProfileRepository

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs.
    def __init__(self) -> None:
        self.read_calls     = []
        self.write_calls    = []
        self.safe_inserts   = []
        self.columns        = ["object_type", "object_id", "row_id", "to_process", "deleted",
                               "name_en_value", "name_fr_value", "degree_score"]
        self.missing_tables = set()

    # Public Method: Record a read query and return no rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return []

    # Public Method: Record a shell-executed write query, applying the DDL
    # side effects of table creation.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        if "CREATE TABLE IF NOT EXISTS" in query:
            self.missing_tables.discard("graphsearch_test.Data_N_Object_T_PageProfile")

    # Public Method: Record a safe-insert upsert.
    def execute_query_as_safe_inserts(self, engine_name: str, schema_name: str, table_name: str, query: str,
                                       key_column_names: list[str], upd_column_names: list[str],
                                       eval_column_names: list[str], actions: tuple = (),
                                       verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.safe_inserts.append({
            "schema"           : schema_name,
            "table"            : table_name,
            "query"            : query,
            "upd_column_names" : upd_column_names,
            "query_id"         : query_id,
        })

    # Public Method: Return the canned column list of any table.
    def get_column_names(self, engine_name: str, schema_name: str, table_name: str) -> list[str]:
        return list(self.columns)

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
    """Fixed resolver pointing at the airflow, cache, and search schemas."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

    # Public Method: Return the canned engine and graph cache schema.
    def for_graph_cache(self) -> tuple[str, str]:
        return ("coresrv", "graph_cache")

    # Public Method: Return the canned engine and graphsearch test schema.
    def for_graphsearch_test(self) -> tuple[str, str]:
        return ("coresrv", "graphsearch_test")

#================================================================#
# Function Group: Patch tests                                    #
#================================================================#

# Public Function: Verify the patch copies the flagged rows with the live
# table columns.
def test_patch_copies_flagged_rows() -> None:
    db = FakeGraphDB()
    repo = MySQLPageProfileRepository(db=db, schema_resolver=FakeSchemaResolver())

    # Patch the flagged page-profile rows in commit mode.
    stats = repo.patch(actions=('commit',))

    # The upsert carries the discovered columns, excluding the keys and the
    # control columns, with the legacy query id.
    assert len(db.safe_inserts) == 1
    insert = db.safe_inserts[0]
    assert insert["query_id"] == 'Mp7U7rMW'
    assert insert["schema"] == "graphsearch_test"
    assert insert["upd_column_names"] == ["name_en_value", "name_fr_value", "degree_score"]

    # The select is driven by the fields-changed flags and the typeflags.
    assert "INNER JOIN graph_airflow.Operations_N_Object_T_FieldsChanged fc" in insert["query"]
    assert "tf.flag_type  = 'fields'" in insert["query"]
    assert stats.target == "graphsearch_test.Data_N_Object_T_PageProfile"

# Public Function: Verify the patch creates the target from the template.
def test_patch_creates_target_from_template() -> None:
    db = FakeGraphDB()
    db.missing_tables.add("graphsearch_test.Data_N_Object_T_PageProfile")
    repo = MySQLPageProfileRepository(db=db, schema_resolver=FakeSchemaResolver())

    # Patch with a missing target table.
    repo.patch(actions=('commit',))
    assert db.write_calls[0]["query_id"] == 'pageprofile-create-target'
    assert "CREATE TABLE IF NOT EXISTS graphsearch_test.Data_N_Object_T_PageProfile LIKE graph_cache.Data_N_Object_T_PageProfile" in db.write_calls[0]["query"]
    assert len(db.safe_inserts) == 1

# Public Function: Verify the patch without actions does nothing.
def test_patch_without_actions_does_nothing() -> None:
    db = FakeGraphDB()
    repo = MySQLPageProfileRepository(db=db, schema_resolver=FakeSchemaResolver())

    # Patch without actions.
    stats = repo.patch(actions=())
    assert stats.rows_flagged == 0
    assert db.safe_inserts == [] and db.write_calls == []
