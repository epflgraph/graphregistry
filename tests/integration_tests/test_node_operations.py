# graphregistry/tests/integration_tests/test_node_operations.py
"""Integration tests for MySQLNodeRepository against a real database.

A dedicated `_0_PYTESTS_*` schema is used to avoid collisions with dev data.
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, cast
import pytest
from graphdb.core.graphdb import GraphDB
from tests.helpers.db_checks import field_map
from tests.helpers.fixtures import FixedTestSchemaResolver, get_test_schema_name, load_json_fixture
from graphregistry.adapters.persistence.mysql.mappers.map_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories.rpo_noderepo import MySQLNodeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.models.entities.mdl_node import NodeList

# Engine name used for all integration tests in this module.
ENGINE_NAME = "xaas_coresrv"
# Path to the JSON fixture that drives the node operation tests.
FIXTURE_PATH = Path(__file__).resolve().parents[1] / "fixtures" / "integration_tests" / "node_operations_sample.json"

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

# Public Method: Build a real MySQL node repository for the test schema.
@pytest.fixture
def real_repo(schema_resolver: FixedTestSchemaResolver) -> MySQLNodeRepository:
    db = GraphDB()
    return MySQLNodeRepository(
        db              = db,
        schema_resolver = cast(SchemaResolver, schema_resolver),
    )

# Public Method: Load the JSON fixture for node operations.
@pytest.fixture
def sample_data() -> dict[str, Any]:
    return load_json_fixture(FIXTURE_PATH)

# Public Method: Build the primary node key used across tests.
@pytest.fixture
def node_key(sample_data: dict[str, Any]) -> NodeKey:
    return NodeKey(
        object_type = sample_data["object_type"],
        object_id   = sample_data["object_id"],
    )

# Public Method: Build the primary node entity used across tests.
@pytest.fixture
def node(sample_data: dict[str, Any]) -> Any:
    return MySQLNodeMapper.from_simplified_dict(sample_data)

#================================================================#
# Test Group: Single-node CRUD cycle                             #
#================================================================#

# Test: Verify save/get/delete round-trip for a single node.
# Public Method: test mysql node repository real crud cycle
@pytest.mark.integration
def test_mysql_node_repository_real_crud_cycle(real_repo: MySQLNodeRepository, sample_data: dict[str, Any], node_key: NodeKey, node: Any) -> None:
    # Defensive cleanup before test in case a prior failed run left state behind
    if real_repo.exists(node_key):
        real_repo.delete(node_key, actions=("eval", "commit"))

    # Run the CRUD cycle inside a try block so cleanup always happens.
    try:
        # 1) Initial state
        assert real_repo.exists(node_key) is False
        assert real_repo.get(node_key) is None

        # 2) Save
        saved = real_repo.save(node, actions=("eval", "commit"))
        assert saved.key == node_key

        # 3) Exists after save
        assert real_repo.exists(node_key) is True

        # 4) Get after save
        loaded = real_repo.get(node_key)
        assert loaded is not None
        assert loaded.key == node_key
        assert loaded.title == sample_data["object_title"]
        assert loaded.text_source == sample_data["text_source"]
        assert loaded.raw_text == sample_data["raw_text"]

        # 5) Custom fields verification
        assert len(loaded.field_list.item_list) == len(sample_data["custom_fields"])
        actual_field_map = field_map(loaded.field_list.to_list())
        expected_field_map = field_map(sample_data["custom_fields"])
        assert actual_field_map == expected_field_map

        # 6) Page profile verification
        assert loaded.page_profile is not None
        assert loaded.page_profile.key == node_key
        assert loaded.page_profile.short_code == sample_data["page_profile"]["short_code"]
        assert loaded.page_profile.name.en.value == sample_data["page_profile"]["name_en_value"]
        assert loaded.page_profile.name.fr.value == sample_data["page_profile"]["name_fr_value"]
        assert loaded.page_profile.description.short.en.value == sample_data["page_profile"]["description_short_en_value"]
        assert loaded.page_profile.description.medium.en.value == sample_data["page_profile"]["description_medium_en_value"]
        assert loaded.page_profile.description.long.en.value == sample_data["page_profile"]["description_long_en_value"]
        assert loaded.page_profile.is_visible is True

        # 7) Serialization round-trip check through mapper
        simplified = MySQLNodeMapper.to_simplified_dict(loaded)
        rehydrated = MySQLNodeMapper.from_simplified_dict(simplified)

        # Verify the simplified-dict serialization round-trip preserves the node.
        assert rehydrated.key == loaded.key
        assert rehydrated.title == loaded.title
        assert rehydrated.text_source == loaded.text_source
        assert rehydrated.raw_text == loaded.raw_text
        assert len(rehydrated.field_list.item_list) == len(loaded.field_list.item_list)
        assert rehydrated.page_profile is not None
        assert loaded.page_profile is not None
        assert rehydrated.page_profile.short_code == loaded.page_profile.short_code
        assert rehydrated.page_profile.name.get_value("en") == loaded.page_profile.name.get_value("en")
        assert rehydrated.page_profile.external_url.get_value("en") == loaded.page_profile.external_url.get_value("en")

        # 8) Delete
        assert real_repo.delete(node_key, actions=("eval", "commit")) is True

        # 9) Final state
        assert real_repo.exists(node_key) is False
        assert real_repo.get(node_key) is None

    # Clean up any leftover node, even if an assertion failed.
    finally:
        # Always clean up even if an assertion fails
        if real_repo.exists(node_key):
            real_repo.delete(node_key, actions=("eval", "commit"))

#================================================================#
# Test Group: Batch node CRUD cycle                              #
#================================================================#

# Test: Verify that save_many persists and reloads multiple nodes atomically.
# Public Method: test mysql node repository real batch save cycle
@pytest.mark.integration
def test_mysql_node_repository_real_batch_save_cycle(real_repo: MySQLNodeRepository, sample_data: dict[str, Any]) -> None:
    nodes = [
        MySQLNodeMapper.from_simplified_dict({**sample_data, "object_id": f"TEST-BATCH-{i}"})
        for i in range(3)
    ]
    keys = [node.key for node in nodes]

    # Defensive cleanup
    for key in keys:
        if real_repo.exists(key):
            real_repo.delete(key, actions=("commit",))

    # Persist all batch nodes and verify the returned count.
    try:
        saved = real_repo.save_many(NodeList(item_list=nodes), actions=("commit",))
        assert len(saved.item_list) == 3

        # Verify every batch node was saved and reloads with expected data.
        for key in keys:
            assert real_repo.exists(key) is True
            loaded = real_repo.get(key)
            assert loaded is not None
            assert loaded.title == sample_data["object_title"]
            assert loaded.page_profile is not None
            assert loaded.page_profile.short_code == sample_data["page_profile"]["short_code"]

        # Delete the batch and confirm every key reported success.
        deleted = real_repo.delete_many(keys, actions=("commit",))
        assert all(result is True for result in deleted)

        # Confirm every batch node was removed.
        for key in keys:
            assert real_repo.exists(key) is False

    # Clean up any remaining batch nodes after the test.
    finally:
        for key in keys:
            if real_repo.exists(key):
                real_repo.delete(key, actions=("commit",))
