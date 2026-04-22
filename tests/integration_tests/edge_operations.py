# tests/integration_tests/edge_operations.py
from __future__ import annotations
from pathlib import Path
from typing import Any
from graphregistry.common.config import GlobalConfig
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.domain.models.entities.mdl_base import EdgeKey
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList
import pytest, json

# Adjust this import if your actual DB class lives elsewhere
from graphdb.core.graphdb import GraphDB

# Initialize global config to ensure schema names are available
glbcfg = GlobalConfig()

# Resolve the fixture path relative to this test file, going up to the project root and then into tests/fixtures
FIXTURE_PATH = Path("./tests/fixtures/edge_operations_sample.json")

# Define the engine name constant used in the schema resolver
ENGINE_NAME = 'xaas_coresrv'

# Use a dedicated schema for integration tests to avoid conflicts with other data
SCHEMA_NAME = "_0_PYTESTS_"+glbcfg.schema_registry.replace('_1_DEV_','')

# Class definition for a fixed schema resolver that always returns the same schema for nodes and edges
class FixedTestSchemaResolver:
    def for_node(self, key):
        return (ENGINE_NAME, SCHEMA_NAME)

    def for_edge(self, key):
        return (ENGINE_NAME, SCHEMA_NAME)

# Helper function to load the JSON fixture data
def load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

# Pytest fixture to provide a real repository instance for integration testing
@pytest.fixture
def real_repo() -> MySQLEdgeRepository:
    db = GraphDB()
    resolver = FixedTestSchemaResolver()

    return MySQLEdgeRepository(
        db=db,
        schema_resolver=resolver
    )

# Helper function to build an Edge domain object from the simplified JSON fixture data
def build_edge(data: dict[str, Any]) -> Edge:
    key = EdgeKey(
        from_institution_id=data["from_institution_id"],
        from_object_type=data["from_object_type"],
        from_object_id=data["from_object_id"],
        to_institution_id=data["to_institution_id"],
        to_object_type=data["to_object_type"],
        to_object_id=data["to_object_id"],
        context=data["context"],
    )

    field_list = EdgeFieldList(
        item_list=[
            EdgeField(
                key=EdgeFieldKey(
                    key=key,
                    field_language=row["field_language"],
                    field_name=row["field_name"],
                ),
                field_value=row["field_value"],
            )
            for row in data["field_list"]
        ]
    )

    return Edge(key=key, field_list=field_list)

# The actual test function that performs a full CRUD cycle on the MySQLEdgeRepository using the fixture data
@pytest.mark.integration
def test_mysql_edge_repository_real_crud_cycle(real_repo: MySQLEdgeRepository) -> None:
    data = load_fixture()

    key = EdgeKey(
        from_institution_id=data["from_institution_id"],
        from_object_type=data["from_object_type"],
        from_object_id=data["from_object_id"],
        to_institution_id=data["to_institution_id"],
        to_object_type=data["to_object_type"],
        to_object_id=data["to_object_id"],
        context=data["context"],
    )

    edge = build_edge(data)

    # Defensive cleanup
    if real_repo.exists(key):
        print('⚠️ Warning: Edge already exists before test. Deleting it to ensure clean state.')
        real_repo.delete(key, actions=("eval", "commit"))

    try:
        # 1) Initial state
        print("")
        assert real_repo.exists(key) is False
        assert real_repo.get(key) is None

        # 2) Save
        print("\nSaving edge to repository...")
        saved = real_repo.save(edge, actions=("eval", "commit"))
        assert saved.key == key

        # 3) Exists after save
        assert real_repo.exists(key) is True

        # 4) Get after save
        loaded = real_repo.get(key)
        assert loaded is not None

        import rich
        print("\nEdge object content:")
        rich.print_json(data=loaded.model_dump(mode="json"))

        assert loaded.key == key

        # 5) Custom fields verification
        assert len(loaded.field_list.item_list) == len(data["field_list"])

        field_map = {
            (field.key.field_language, field.key.field_name): field.field_value
            for field in loaded.field_list.item_list
        }

        for row in data["field_list"]:
            k = (row["field_language"], row["field_name"])
            assert k in field_map
            assert field_map[k] == row["field_value"]

        # 6) Delete
        assert real_repo.delete(key, actions=("eval", "commit")) is True

        # 7) Final state
        assert real_repo.exists(key) is False
        assert real_repo.get(key) is None

    finally:
        if real_repo.exists(key):
            real_repo.delete(key, actions=("eval", "commit"))
