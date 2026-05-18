# tests/integration_tests/test_node_operations.py
from __future__ import annotations
from pathlib import Path
from typing import Any, cast
from graphregistry.common.config import GlobalConfig
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.domain.models.entities.mdl_base import NodeKey
from graphregistry.domain.interfaces.services.srv_schema import SchemaResolver
import pytest, json

# Adjust this import if your actual DB class lives elsewhere
from graphdb.core.graphdb import GraphDB

# Initialize global config to ensure schema names are available
glbcfg = GlobalConfig()

# Resolve the fixture path relative to this test file, going up to the project root and then into tests/fixtures
FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "integration_tests"
    / "node_operations_sample.json"
)

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
def real_repo() -> MySQLNodeRepository:
    db = GraphDB()
    resolver = FixedTestSchemaResolver()

    return MySQLNodeRepository(
        db=db,
        schema_resolver=cast(SchemaResolver, resolver)
    )

# The actual test function that performs a full CRUD cycle on the MySQLNodeRepository using the fixture data
@pytest.mark.integration
def test_mysql_node_repository_real_crud_cycle(real_repo: MySQLNodeRepository) -> None:
    data = load_fixture()

    key = NodeKey(
        institution_id=data["institution_id"],
        object_type=data["object_type"],
        object_id=data["object_id"],
    )

    # Build the domain object from the simplified JSON fixture
    node = MySQLNodeMapper.from_simplified_dict(data)

    # Defensive cleanup before test in case a prior failed run left state behind
    if real_repo.exists(key):
        real_repo.delete(key, actions=("eval", "commit"))

    try:
        # 1) Initial state
        print('')
        assert real_repo.exists(key) is False
        assert real_repo.get(key) is None

        # 2) Save
        print('\nSaving node to repository...')
        saved = real_repo.save(node, actions=("eval", "commit"))
        assert saved.key == key

        # 3) Exists after save
        assert real_repo.exists(key) is True

        # 4) Get after save
        loaded = real_repo.get(key)
        assert loaded is not None
        import rich
        print("\nNode object content:")
        rich.print_json(data=loaded.model_dump(mode="json"))
        assert loaded is not None
        assert loaded.key == key
        assert loaded.title == data["object_title"]
        assert loaded.text_source == data["text_source"]
        assert loaded.raw_text == data["raw_text"]

        # 5) Custom fields verification
        assert len(loaded.field_list.item_list) == len(data["custom_fields"])
        field_map = {
            (field.key.field_language, field.key.field_name): field.field_value
            for field in loaded.field_list.item_list
        }

        for row in data["custom_fields"]:
            k = (row["field_language"], row["field_name"])
            assert k in field_map
            assert field_map[k] == row["field_value"]

        # 6) Page profile verification
        assert loaded.page_profile is not None
        assert loaded.page_profile.key == key
        assert loaded.page_profile.short_code == data["page_profile"]["short_code"]
        assert loaded.page_profile.name.en.value == data["page_profile"]["name_en_value"]
        assert loaded.page_profile.name.fr.value == data["page_profile"]["name_fr_value"]
        assert loaded.page_profile.description.short.en.value == data["page_profile"]["description_short_en_value"]
        assert loaded.page_profile.description.medium.en.value == data["page_profile"]["description_medium_en_value"]
        assert loaded.page_profile.description.long.en.value == data["page_profile"]["description_long_en_value"]
        assert loaded.page_profile.external_key['en'] == data["page_profile"]["external_key_en"]
        assert loaded.page_profile.external_url['en'] == data["page_profile"]["external_url_en"]
        assert loaded.page_profile.is_visible is True

        # 7) Serialization round-trip check through mapper
        simplified = MySQLNodeMapper.to_simplified_dict(loaded)
        rehydrated = MySQLNodeMapper.from_simplified_dict(simplified)

        assert rehydrated.key == loaded.key
        assert rehydrated.title == loaded.title
        assert rehydrated.text_source == loaded.text_source
        assert rehydrated.raw_text == loaded.raw_text
        assert len(rehydrated.field_list.item_list) == len(loaded.field_list.item_list)

        assert rehydrated.page_profile is not None
        assert loaded.page_profile is not None
        assert rehydrated.page_profile.short_code == loaded.page_profile.short_code
        assert rehydrated.page_profile.name['en'].value == loaded.page_profile.name['en'].value
        assert rehydrated.page_profile.external_url['en'] == loaded.page_profile.external_url['en']

        # 8) Delete
        assert real_repo.delete(key, actions=("eval", "commit")) is True

        # 9) Final state
        assert real_repo.exists(key) is False
        assert real_repo.get(key) is None

    finally:
        # Always clean up even if an assertion fails
        if real_repo.exists(key):
            real_repo.delete(key, actions=("eval", "commit"))
