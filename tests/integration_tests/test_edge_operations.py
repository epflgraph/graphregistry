# graphregistry/tests/integration_tests/test_edge_operations.py
"""Integration tests for MySQLEdgeRepository against a real database.

A dedicated `_0_PYTESTS_*` schema is used to avoid collisions with dev data.
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, cast
import pytest
from graphdb.core.graphdb import GraphDB
from tests.helpers.db_checks import db_field_map, field_map
from tests.helpers.fixtures import FixedTestSchemaResolver, get_test_schema_name, load_json_fixture
from graphregistry.adapters.persistence.mysql.repositories.rpo_edgerepo import MySQLEdgeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.domain.models.entities.mdl_base import EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList, EdgeList

# Engine name used for all integration tests in this module.
ENGINE_NAME = "xaas_coresrv"
# Path to the JSON fixture that drives the edge operation tests.
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "integration_tests" / "edge_operations_sample.json"

#================================================================#
# Function Group: Pytest fixtures                                #
#================================================================#

# Public Method: Return the dedicated test schema name for this module.
@pytest.fixture(scope="module")
def schema_name() -> str:
    return get_test_schema_name()

# Public Method: Build a fixed schema resolver pointing at the test schema.
@pytest.fixture(scope="module")
def schema_resolver(schema_name: str) -> FixedTestSchemaResolver:
    return FixedTestSchemaResolver(engine_name=ENGINE_NAME, schema_name=schema_name)

# Public Method: Build a real MySQL edge repository for the test schema.
@pytest.fixture
def real_repo(schema_resolver: FixedTestSchemaResolver) -> MySQLEdgeRepository:
    db = GraphDB()
    return MySQLEdgeRepository(
        db              = db,
        schema_resolver = cast(SchemaResolver, schema_resolver),
    )

# Public Method: Load the JSON fixture for edge operations.
@pytest.fixture
def sample_data() -> dict[str, Any]:
    return load_json_fixture(FIXTURE_PATH)

# Public Method: Build the primary edge key used across tests.
@pytest.fixture
def edge_key(sample_data: dict[str, Any]) -> EdgeKey:
    return EdgeKey(
        from_object_type = sample_data["from_object_type"],
        from_object_id   = sample_data["from_object_id"],
        to_object_type   = sample_data["to_object_type"],
        to_object_id     = sample_data["to_object_id"],
        context          = sample_data["context"],
    )

# Public Method: Build the primary edge entity used across tests.
@pytest.fixture
def edge(sample_data: dict[str, Any]) -> Edge:
    key = EdgeKey(
        from_object_type = sample_data["from_object_type"],
        from_object_id   = sample_data["from_object_id"],
        to_object_type   = sample_data["to_object_type"],
        to_object_id     = sample_data["to_object_id"],
        context          = sample_data["context"],
    )
    field_list = EdgeFieldList(
        item_list=[
            EdgeField(
                key=EdgeFieldKey(
                    key            = key,
                    field_language = row["field_language"],
                    field_name     = row["field_name"],
                ),
                field_value=row["field_value"],
            )
            for row in sample_data["field_list"]
        ]
    )
    return Edge(key=key, field_list=field_list)

#================================================================#
# Test Group: Single-edge CRUD cycle                             #
#================================================================#

# Test: Verify save/get/delete round-trip for a single edge.
# Public Method: test mysql edge repository real crud cycle
@pytest.mark.integration
def test_mysql_edge_repository_real_crud_cycle(real_repo: MySQLEdgeRepository, sample_data: dict[str, Any], edge_key: EdgeKey, edge: Edge) -> None:
    # Defensive cleanup
    if real_repo.exists(edge_key):
        real_repo.delete(edge_key, actions=("eval", "commit"))

    # Run the CRUD cycle inside a try block so cleanup always happens.
    try:
        # 1) Initial state
        assert real_repo.exists(edge_key) is False
        assert real_repo.get(edge_key) is None

        # 2) Save
        saved = real_repo.save(edge, actions=("eval", "commit"))
        assert saved.key == edge_key

        # 3) Exists after save
        assert real_repo.exists(edge_key) is True

        # 4) Get after save
        loaded = real_repo.get(edge_key)
        assert loaded is not None
        assert loaded.key == edge_key

        # 5) Custom fields verification
        assert len(loaded.field_list.item_list) == len(sample_data["field_list"])
        actual_field_map = db_field_map([
            (field.key.field_language, field.key.field_name, field.field_value)
            for field in loaded.field_list.item_list
        ])
        expected_field_map = field_map(sample_data["field_list"])
        assert actual_field_map == expected_field_map

        # 6) Delete
        assert real_repo.delete(edge_key, actions=("eval", "commit")) is True

        # 7) Final state
        assert real_repo.exists(edge_key) is False
        assert real_repo.get(edge_key) is None

    # Clean up any leftover edge, even if an assertion failed.
    finally:
        if real_repo.exists(edge_key):
            real_repo.delete(edge_key, actions=("eval", "commit"))

#================================================================#
# Test Group: Batch edge CRUD cycle                              #
#================================================================#

# Test: Verify that save_many persists and reloads multiple edges atomically.
# Public Method: test mysql edge repository real batch save cycle
@pytest.mark.integration
def test_mysql_edge_repository_real_batch_save_cycle(real_repo: MySQLEdgeRepository, sample_data: dict[str, Any]) -> None:
    edges: list[Edge] = []
    keys: list[EdgeKey] = []
    for i in range(3):
        key = EdgeKey(
            from_object_type = sample_data["from_object_type"],
            from_object_id   = f"{sample_data['from_object_id']}-BATCH-{i}",
            to_object_type   = sample_data["to_object_type"],
            to_object_id     = f"{sample_data['to_object_id']}-BATCH-{i}",
            context          = sample_data["context"],
        )
        keys.append(key)
        edges.append(Edge(
            key        = key,
            field_list = EdgeFieldList(item_list=[
                EdgeField(
                    key         = EdgeFieldKey(key=key, field_language="en", field_name="weight"),
                    field_value = str(i),
                ),
            ]),
        ))

    # Remove any pre-existing batch edges before the test run.
    for key in keys:
        if real_repo.exists(key):
            real_repo.delete(key, actions=("commit",))

    # Persist all batch edges and verify the returned count.
    try:
        saved = real_repo.save_many(EdgeList(item_list=edges), actions=("commit",))
        assert len(saved.item_list) == 3

        # Verify every edge was saved and can be reloaded.
        for key in keys:
            assert real_repo.exists(key) is True
            loaded = real_repo.get(key)
            assert loaded is not None
            assert loaded.key == key

        # Delete the batch and confirm every key reported success.
        deleted = real_repo.delete_many(keys, actions=("commit",))
        assert all(result is True for result in deleted)

        # Confirm every batch edge was removed.
        for key in keys:
            assert real_repo.exists(key) is False

    # Clean up any remaining batch edges after the test.
    finally:
        for key in keys:
            if real_repo.exists(key):
                real_repo.delete(key, actions=("commit",))
