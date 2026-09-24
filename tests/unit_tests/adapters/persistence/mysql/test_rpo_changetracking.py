# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_changetracking.py
"""Unit tests for MySQLChangeTrackingRepository using an in-memory GraphDB fake.

The fake records every query, write, and safe-insert so the tests verify the
legacy query sequences, the scope-driven conditions, and the model mapping
without a database connection.
"""
from __future__ import annotations
from datetime import date
from pathlib import Path
import pytest
from graphregistry.adapters.persistence.mysql.repositories.rpo_changetracking import (
    MySQLChangeTrackingRepository,
    PAGE_PROFILE_CHECKSUM_COLUMNS,
)
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.pipeline.mdl_policies import ExpirationPolicy, ProcessingScope
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and canned results.
    def __init__(self) -> None:
        self.read_calls       = []
        self.write_calls      = []
        self.cell_results     = {}
        self.safe_inserts     = []
        self.columns_by_table = {}
        self.results_by_query_id = {}

        # Queue of row batches returned per query id, consumed one batch per
        # call; needed because the legacy reuses query ids across tables.
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

    # Public Method: Return the canned rows of a get_cells read.
    def get_cells(self, engine_name: str, schema_name: str, table_name: str, select: list[str], where: list, verbose: bool = False) -> list[tuple]:
        return self.cell_results.get(table_name, [])

    # Public Method: Report whether a column exists on a table.
    def has_column(self, engine_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
        return column_name in self.columns_by_table.get(table_name, [])

    # Public Method: Record a safe-insert upsert driven by a checksum query.
    def execute_query_as_safe_inserts(self, engine_name: str, schema_name: str, table_name: str, query: str,
                                      key_column_names: list[str], upd_column_names: list[str],
                                      eval_column_names: list[str], actions: tuple = (), verbose: bool = False,
                                      query_id: str | None = None) -> None:
        self.safe_inserts.append({
            "table"             : table_name,
            "query"             : query,
            "query_id"          : query_id,
            "key_column_names"  : key_column_names,
            "actions"           : actions,
        })

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
    """Fixed configuration carrying the data schema names and type mapping."""

    # Public Method: Initialize the fake with the registry schema layout.
    def __init__(self) -> None:
        self.schema_registry   = "graph_registry"
        self.schema_lectures   = "graph_lectures"
        self.schema_ontology   = "graph_ontology"
        self.schema_to_object_types = {
            "graph_registry" : ["Course", "Person", "Publication"],
            "graph_lectures" : ["Lecture", "Exercise"],
            "graph_ontology" : ["Category", "Concept", "Curated area"],
        }

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database.
def _make_repo() -> tuple[MySQLChangeTrackingRepository, FakeGraphDB]:
    db = FakeGraphDB()
    repo = MySQLChangeTrackingRepository(
        db              = db,
        schema_resolver = FakeSchemaResolver(),
        global_config   = FakeGlobalConfig(),
    )
    return repo, db

# Public Function: Build a processing scope covering both families.
def _make_scope() -> ProcessingScope:
    return ProcessingScope(
        node_fields_types = ["Course", "Person"],
        node_scores_types = ["Person"],
        edge_type_pairs   = [EdgeTypePair(from_object_type="Person", to_object_type="Course")],
    )

#================================================================#
# Function Group: Checksum expression parity                     #
#================================================================#

# Public Function: Verify the generated page-profile checksum matches the legacy source.
def test_page_profile_checksum_expression_matches_legacy() -> None:
    legacy_path = Path(__file__).resolve().parents[5] / "graphregistry" / "application" / "core" / "cor_registry.py"
    if not legacy_path.exists():
        pytest.skip("Legacy cor_registry.py is gone; the parity anchor is no longer needed.")
    legacy_text = legacy_path.read_text(encoding="utf-8")

    # Locate the hand-written page-profile checksum chain inside the legacy
    # update_checksums_v2 query by its unique first column.
    start = legacy_text.find('MD5(CONCAT(MD5(COALESCE(numeric_id_en')
    assert start != -1, "Legacy page-profile checksum expression not found."
    end = legacy_text.find(' AS checksum_val', start)
    legacy_expression = legacy_text[start:end]
    assert MySQLChangeTrackingRepository._page_profile_checksum_expression() == legacy_expression

# Public Function: Verify the checksum column inventory of the page profile.
def test_page_profile_checksum_columns() -> None:
    # 4 numeric ids + short code + 4 subtypes + 4 fields x 4 langs x 5 parts
    # + 4 external keys + 4 external urls + visibility flag.
    assert len(PAGE_PROFILE_CHECKSUM_COLUMNS) == 4 + 1 + 4 + 80 + 4 + 4 + 1
    assert PAGE_PROFILE_CHECKSUM_COLUMNS[0] == "numeric_id_en"
    assert PAGE_PROFILE_CHECKSUM_COLUMNS[4] == "short_code"
    assert PAGE_PROFILE_CHECKSUM_COLUMNS[-1] == "is_visible"
    assert "name_fr_translated_from" in PAGE_PROFILE_CHECKSUM_COLUMNS
    assert "description_long_it_value" in PAGE_PROFILE_CHECKSUM_COLUMNS

#================================================================#
# Function Group: Sync tests                                     #
#================================================================#

# Public Function: Verify that sync replays the legacy query sequence per schema.
def test_sync_issues_legacy_query_sequence() -> None:
    repo, db = _make_repo()
    db.results_by_query_id["DY3x5PC8"]  = [("Course", 3), ("Person", 5)]
    db.results_by_query_id["Gk7dDRC0"]  = [("Course", "Person", 2)]
    db.results_by_query_id["7RNfE1fF"]  = [("Person", 4)]

    # Run the sync over the default schema set.
    stats = repo.sync_new_records()

    # Both families sync per schema, in the legacy query order.
    assert [call["query_id"] for call in db.write_calls] == [
        "2PbejfUm", "x5BdjGfN", "s1gXyPYb", "dEE3eDPD", "5mhz4Uwr", "n2TKRWNV",
    ]

    # The returned stats aggregate the count queries per target.
    assert [s.rows_flagged for s in stats] == [8, 2, 4]

# Public Function: Verify that eval mode plans the sync without writing.
def test_sync_eval_mode_writes_nothing() -> None:
    repo, db = _make_repo()
    repo.sync_new_records(actions=('eval',))
    assert db.write_calls == []

#================================================================#
# Function Group: Refresh tests                                  #
#================================================================#

# Public Function: Verify the refresh query sequence and the stats mapping.
def test_refresh_flags_query_sequence_and_stats() -> None:
    repo, db = _make_repo()

    # The stats query id is shared by both FieldsChanged tables, so the node
    # and edge row batches are queued for sequential consumption.
    db.sequential_results["4QF4Lh4y"] = [
        [("Course", 1, 2, 3, 4)],
        [("Course", "Person", 5, 6, 7, 8)],
    ]
    db.results_by_query_id["JH9iFxCF"] = [("Person", 9, 10)]

    # Run the refresh over both families.
    stats = repo.refresh_flags(scope=_make_scope())

    # The has_changed derivation runs per table with the legacy query ids,
    # joining the correct typeflags table per family (the edge query joins
    # the edge typeflags — the legacy node-table join was a bug).
    assert db.write_query_ids() == ["iGojjBW7", "Hy3LQ6tJ", "MsnEuv05", "ye472zFQ", "MsnEuv05", "ye472zFQ", "q4L84LJy", "tBKyps8J"]
    assert "INNER JOIN graph_airflow.Operations_N_Object_T_TypeFlags t" in db.write_calls[0]["query"]
    assert "INNER JOIN graph_airflow.Operations_N_Object_N_Object_T_TypeFlags t" in db.write_calls[1]["query"]

    # The node and edge conditions are embedded in the to_process queries.
    select_queries = [c["query"] for c in db.write_calls if c["query_id"] == "ye472zFQ"]
    assert "object_type IN ('Course', 'Person') AND deleted = 0" in select_queries[0]
    assert "('Course', 'Person')" in select_queries[1]

    # The stats rows map onto the typed models, split by grouping key.
    assert len(stats) == 3
    node_stats   = [s for s in stats if s.__class__.__name__ == "NodeRefreshStats"]
    edge_stats   = [s for s in stats if s.__class__.__name__ == "EdgeRefreshStats"]
    assert node_stats[0].object_type == "Course" and node_stats[0].checksum_changed == 2
    assert edge_stats[0].edge_type_pair.as_tuple == ("Course", "Person")
    assert node_stats[1].new_or_never_cached == 9 and node_stats[1].cache_expired == 10

#================================================================#
# Function Group: Expiration tests                               #
#================================================================#

# Public Function: Verify that count_only expiration returns stats and still resets.
def test_apply_expiration_count_only() -> None:
    repo, db = _make_repo()

    # The count query id is shared by both FieldsChanged tables; the edge
    # table batch is empty so only the node table reports counts.
    db.sequential_results["d5GKbPVP"] = [
        [("Course", 7)],
        [],
    ]
    db.results_by_query_id["46PNmQvh"] = [("Person", 3)]

    # Expire rows cached more than thirty days, counting instead of writing.
    policy = ExpirationPolicy(
        older_than      = __import__("datetime").timedelta(days=30),
        limit_per_type  = 100,
        include_fields  = True,
        include_scores  = True,
    )
    stats = repo.apply_expiration(policy=policy, scope=_make_scope(), count_only=True)

    # The reset runs before counting, exactly as the legacy command does.
    assert db.write_query_ids() == ["MY52N1XY", "MY52N1XY", "Zsz9iF13"]

    # The date filter and the per-type budget appear in the count queries.
    count_queries = [c["query"] for c in db.read_calls if c["query_id"] in ("d5GKbPVP", "46PNmQvh")]
    assert all("INTERVAL 30 DAY" in q for q in count_queries)
    assert all("rn <= 100" in q for q in count_queries)

    # The counts come back as propagation stats per table and type.
    assert {(s.target, s.rows_flagged) for s in stats} == {
        ("Operations_N_Object_T_FieldsChanged:Course", 7),
        ("Operations_N_Object_T_ScoresExpired:Person", 3),
    }

# Public Function: Verify that an empty scope expires nothing.
def test_apply_expiration_empty_scope() -> None:
    repo, db = _make_repo()
    empty = ProcessingScope()
    stats = repo.apply_expiration(policy=ExpirationPolicy(), scope=empty)
    assert stats == [] and db.write_calls == []

#================================================================#
# Function Group: Rollover and dates tests                       #
#================================================================#

# Public Function: Verify the rollover and update-dates query sequences.
def test_rollover_and_update_dates_sequences() -> None:
    repo, db = _make_repo()
    scope = _make_scope()

    # Rollover both FieldsChanged tables.
    repo.rollover_checksums(scope=scope, actions=('commit',))
    assert db.write_query_ids() == ["ht5AZcsE", "ht5AZcsE"]
    assert all("checksum_previous = checksum_current" in c["query"] for c in db.write_calls)

    # Stamp the cache dates on both families.
    repo.update_cache_dates(scope=scope, actions=('commit',))
    assert db.write_query_ids() == ["ht5AZcsE", "ht5AZcsE", "Q2dracb0", "Q2dracb0", None]
    assert all("last_date_cached = DATE(NOW())" in c["query"] for c in db.write_calls[2:])

#================================================================#
# Function Group: Reset tests                                    #
#================================================================#

# Public Function: Verify that clear_all_flags builds the per-table SET clauses.
def test_clear_all_flags_builds_set_clauses() -> None:
    repo, db = _make_repo()
    db.columns_by_table["Operations_N_Object_T_FieldsChanged"] = ["to_process", "has_changed", "has_expired"]
    db.columns_by_table["Operations_N_Object_N_Object_T_FieldsChanged"] = ["to_process", "has_changed", "has_expired"]
    db.columns_by_table["Operations_N_Object_T_ScoresExpired"] = ["to_process", "has_expired"]

    # Clear every flag across the three tracking tables.
    repo.clear_all_flags(clear_has_expired=True)

    # The legacy reset order is edge, node, scores, with the legacy query id.
    assert db.write_query_ids() == ["5LEjczg5", "5LEjczg5", "5LEjczg5"]
    edge_query = db.write_calls[0]["query"]
    node_query = db.write_calls[1]["query"]
    scores_query = db.write_calls[2]["query"]
    assert "SET to_process = 0, has_changed = 0, has_expired = 0" in edge_query
    assert "WHERE to_process = 1 OR has_changed = 1 OR has_expired = 1" in node_query
    assert "has_changed" not in scores_query

# Public Function: Verify that the conditional reset uses the scoped conditions.
def test_reset_flags_uses_scoped_conditions() -> None:
    repo, db = _make_repo()
    repo.reset_flags(scope=_make_scope())
    assert db.write_query_ids() == ["RWCE1vkr", "RWCE1vkr", "AhJepYi8"]
    assert "object_type IN ('Person')" in db.write_calls[2]["query"]

#================================================================#
# Function Group: Record read tests                              #
#================================================================#

# Public Function: Verify the record mapping of the three read methods.
def test_get_records_map_rows_to_models() -> None:
    repo, db = _make_repo()
    db.cell_results["Operations_N_Object_T_FieldsChanged"] = [(
        "Course", "c1", "abc123", "abc123", 0, date(2026, 9, 1), 1, 1, 0,
    )]
    db.cell_results["Operations_N_Object_N_Object_T_FieldsChanged"] = [(
        "Course", "c1", "Person", "p1", "default", "aaa", "bbb", 1, date(2026, 9, 2), 0, 1, 0,
    )]
    db.cell_results["Operations_N_Object_T_ScoresExpired"] = [(
        "Person", "p1", date(2026, 8, 1), 1, 0, 0,
    )]

    # Read the node record and verify the mapped state.
    node_record = repo.get_node_record(key=NodeKey(object_type="Course", object_id="c1"))
    assert node_record is not None
    assert node_record.checksums.has_changed is False
    assert node_record.state.last_date_cached == date(2026, 9, 1)
    assert node_record.state.has_expired and node_record.state.to_process
    assert node_record.requires_processing

    # Read the edge record and verify the mapped state.
    edge_record = repo.get_edge_record(
        key = EdgeKey(
            from_object_type = "Course",
            from_object_id   = "c1",
            to_object_type   = "Person",
            to_object_id     = "p1",
            context          = "default",
        )
    )
    assert edge_record is not None
    assert edge_record.checksums.has_changed is True
    assert edge_record.state.to_process and not edge_record.state.has_expired

    # Read the score-expiry record and verify the mapped state.
    score_record = repo.get_score_expiry_record(key=NodeKey(object_type="Person", object_id="p1"))
    assert score_record is not None
    assert score_record.state.is_stale and not score_record.state.to_process

# Public Function: Verify that missing records map onto None.
def test_get_records_return_none_when_absent() -> None:
    repo, _db = _make_repo()
    assert repo.get_node_record(key=NodeKey(object_type="Course", object_id="nope")) is None
    assert repo.get_score_expiry_record(key=NodeKey(object_type="Course", object_id="nope")) is None

#================================================================#
# Function Group: Checksum update tests                          #
#================================================================#

# Public Function: Verify the checksum upsert sequence over the active schemas.
def test_update_current_checksums_query_sequence() -> None:
    repo, db = _make_repo()
    scope = ProcessingScope(
        node_fields_types = ["Course", "Person", "Category"],
        edge_type_pairs   = [EdgeTypePair(from_object_type="Person", to_object_type="Course")],
    )
    repo.update_current_checksums(scope=scope, actions=('commit',))

    # Node checksums: registry general rows, ontology concept and category
    # rows, then page profiles and custom fields over the active schemas. The
    # lectures schema is skipped throughout: it holds no active types.
    insert_ids = [call["query_id"] for call in db.safe_inserts]
    assert insert_ids == [
        "VBk3hp3Z",              # registry general objects
        "CmNTYc97", "XUiwHdd6",  # ontology concept and category
        "5nVWX6nk", "5nVWX6nk",  # page profiles: registry and ontology
        "oTWu6bBL", "oTWu6bBL",  # custom fields: registry and ontology
        "y0yFAafh",              # final node checksums
        "LnzeNnx1",              # edge general rows (registry only)
        "WZ4gEw01",              # edge custom fields (registry only)
        "JJQ2pj3y",              # final edge checksums
    ]

    # The lectures schema is skipped for node rows: no active lectures types.
    assert not any("graph_lectures" in call["table"] for call in db.safe_inserts)

    # The airflow apply queries run in commit mode with the legacy query ids.
    assert db.write_query_ids() == ["j65waWD2", "Fpas6ysH"]

    # The page-profile query embeds the generated checksum expression.
    page_profile_call = next(call for call in db.safe_inserts if call["query_id"] == "5nVWX6nk")
    assert "numeric_id_en" in str(page_profile_call)


#================================================================#
# Function Group: Status tests                                   #
#================================================================#

# Public Function: Verify the status overview aggregates the three tables.
def test_get_status_aggregates_tables() -> None:
    repo, db = _make_repo()
    db.results_by_query_id['TDgw7fYz'] = [("Course", 3), ("Person", 5)]
    db.results_by_query_id['EqpDtL34'] = [("Course", "Person", 2)]
    db.results_by_query_id['ts8NQExF'] = [("Person", 4)]

    status = repo.get_status()

    assert status.node_fields_counts == [("Course", 3), ("Person", 5)]
    assert status.edge_fields_counts == [("Course", "Person", 2)]
    assert status.scores_counts == [("Person", 4)]
    assert [call["query_id"] for call in db.read_calls] == ['TDgw7fYz', 'EqpDtL34', 'ts8NQExF']
