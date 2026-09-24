# graphregistry/tests/unit_tests/adapters/persistence/mysql/test_rpo_typeflags.py
"""Unit tests for MySQLTypeFlagsRepository using an in-memory GraphDB fake.

The fake records every query and cell update issued by the repository so the
tests verify both the model mapping and the legacy query sequence without a
database connection.
"""
from __future__ import annotations
from graphregistry.adapters.persistence.mysql.repositories.rpo_typeflags import MySQLTypeFlagsRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.domain.models.pipeline.mdl_typeflags import TypeFlagConfig

#==================#
# Class Definition #
#==================#
class FakeGraphDB:
    """In-memory stand-in for the GraphDB client, capturing all calls."""

    # Public Method: Initialize the fake with empty call logs and results.
    def __init__(self) -> None:
        self.read_calls   = []
        self.write_calls  = []
        self.cell_calls   = []
        self.results_by_query_id = {}

    # Public Method: Record a read query and return the canned rows.
    def execute_query(self, engine_name: str, query: str, query_id: str | None = None, **kwargs) -> list[tuple]:
        self.read_calls.append({"engine": engine_name, "query": query, "query_id": query_id})
        return self.results_by_query_id.get(query_id, [])

    # Public Method: Record a shell-executed write query.
    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False, query_id: str | None = None, **kwargs) -> None:
        self.write_calls.append({"engine": engine_name, "query": query, "query_id": query_id})

    # Public Method: Record a cell update issued by the node-flag activation.
    def set_cells(self, engine_name: str, schema_name: str, table_name: str, set, where, verbose: bool = False) -> None:
        self.cell_calls.append({
            "engine"     : engine_name,
            "schema"     : schema_name,
            "table"      : table_name,
            "set"        : list(set),
            "where"      : list(where),
        })

#==================#
# Class Definition #
#==================#
class FakeSchemaResolver:
    """Fixed resolver pointing at the airflow schema of the test engine."""

    # Public Method: Return the canned engine and airflow schema.
    def for_airflow(self) -> tuple[str, str]:
        return ("coresrv", "graph_airflow")

#================================================================#
# Function Group: Shared fixtures                                #
#================================================================#

# Public Function: Build a repository over a fake database with canned rows.
def _make_repo() -> tuple[MySQLTypeFlagsRepository, FakeGraphDB]:
    db = FakeGraphDB()
    db.results_by_query_id["4bcoW1KT"] = [
        ("Course",   1, 0),
        ("Category", 1, 1),
        ("Person",   0, 1),
    ]
    db.results_by_query_id["9K34TTeQ"] = [
        ("Concept", "Lecture"),
        ("Course",  "Person"),
    ]
    repo = MySQLTypeFlagsRepository(db=db, schema_resolver=FakeSchemaResolver())
    return repo, db

#================================================================#
# Function Group: Load tests                                     #
#================================================================#

# Public Function: Verify that load maps table rows onto the typed model.
def test_load_maps_rows_to_model() -> None:
    repo, _db = _make_repo()
    config = repo.load()

    # The node flags carry both families per type, including inactive ones.
    assert {(f.object_type, f.flag_type, f.to_process) for f in config.nodes} == {
        ("Course", "fields", True), ("Course", "scores", False),
        ("Category", "fields", True), ("Category", "scores", True),
        ("Person", "fields", False), ("Person", "scores", True),
    }

    # The edge flags are activated and canonicalised.
    assert {f.pair.as_tuple for f in config.edges} == {("Concept", "Lecture"), ("Course", "Person")}
    assert all(f.to_process for f in config.edges)

# Public Function: Verify that the loaded model serialises to the legacy JSON format.
def test_load_to_json_matches_legacy_format() -> None:
    repo, _db = _make_repo()
    legacy_json = repo.load().to_json()

    # Node rows keep the (type, fields, scores) column order of the legacy format.
    assert sorted(legacy_json["nodes"]) == sorted([
        ["Course", True, False],
        ["Category", True, True],
        ["Person", False, True],
    ])

    # Edge rows carry the canonical pair and the always-True activation column.
    assert sorted(legacy_json["edges"]) == sorted([
        ["Concept", "Lecture", True],
        ["Course", "Person", True],
    ])

# Public Function: Verify the round-trip fixed point through the exchange format.
def test_load_round_trip_fixed_point() -> None:
    repo, _db = _make_repo()
    once = repo.load().to_json()

    # Re-parsing the serialised form must reproduce it exactly.
    assert TypeFlagConfig.from_json(once).to_json() == once

#================================================================#
# Function Group: Save and reset tests                           #
#================================================================#

# Public Function: Verify that save replays the legacy query sequence.
def test_save_issues_legacy_query_sequence() -> None:
    repo, db = _make_repo()
    config = TypeFlagConfig.from_json({
        "nodes": [["Course", True, False], ["Person", False, True]],
        "edges": [["Person", "Course", True]],
    })
    repo.save(config)

    # The reset runs first over both flag tables with the legacy query id.
    assert [call["query_id"] for call in db.write_calls[:2]] == ["AUzikHX5", "AUzikHX5"]
    for call in db.write_calls[:2]:
        assert "SET to_process = 0" in call["query"]
        assert "WHERE to_process = 1" in call["query"]

    # Active node flags are activated through cell updates, one per family.
    assert sorted(
        (call["where"][0][1], call["where"][1][1]) for call in db.cell_calls
    ) == [("Course", "fields"), ("Person", "scores")]
    for call in db.cell_calls:
        assert call["set"] == [("to_process", 1)]
        assert call["table"] == "Operations_N_Object_T_TypeFlags"
        assert call["engine"] == "coresrv"

    # The edge family is upserted in both directions with the legacy query id.
    edge_calls = [call for call in db.write_calls if call["query_id"] == "typeflags-edge-upsert"]
    assert len(edge_calls) == 1
    assert "('Course', 'Person', 1)" in edge_calls[0]["query"]
    assert "('Person', 'Course', 1)" in edge_calls[0]["query"]
    assert "ON DUPLICATE KEY UPDATE to_process = 1" in edge_calls[0]["query"]

# Public Function: Verify that eval mode plans without writing.
def test_save_eval_mode_writes_nothing() -> None:
    repo, db = _make_repo()
    config = TypeFlagConfig.from_json({
        "nodes": [["Course", True, True]],
        "edges": [["Person", "Course", True]],
    })
    repo.save(config, actions=('eval',))

    # No reset, no cell update, and no edge upsert may be issued.
    assert db.write_calls == []
    assert db.cell_calls == []

# Public Function: Verify that reset clears both flag tables with the legacy SQL.
def test_reset_issues_legacy_updates() -> None:
    repo, db = _make_repo()
    repo.reset()

    # Both flag tables are reset once, on the airflow schema of the engine.
    assert len(db.write_calls) == 2
    tables = sorted(call["query"].split("UPDATE graph_airflow.")[1].split("\n")[0].strip() for call in db.write_calls)
    assert tables == [
        "Operations_N_Object_N_Object_T_TypeFlags",
        "Operations_N_Object_T_TypeFlags",
    ]
    for call in db.write_calls:
        assert call["query_id"] == "AUzikHX5"
        assert call["engine"] == "coresrv"

# Public Function: Verify that print mode reports the queries without writing.
def test_reset_print_mode_writes_nothing() -> None:
    repo, db = _make_repo()
    repo.reset(actions=('print',))

    # Printing the statements must not execute any of them.
    assert db.write_calls == []
