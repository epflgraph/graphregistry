# test/integration_tests/edge_operations.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.mdl_base import EdgeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList

# Adjust if needed
from graphdb.core.graphdb import GraphDB

# Resolve the fixture path relative to this test file, going up to the project root and then into tests/fixtures
FIXTURE_PATH = Path("./tests/fixtures/edge_course_person_test.json")

# Use a dedicated schema for integration tests to avoid conflicts with other data
SCHEMA_NAME = "_0_integration_tests_graph_registry"

def load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def real_repo() -> MySQLEdgeRepository:
    glbcfg = GlobalConfig()
    db = GraphDB()

    return MySQLEdgeRepository(
        engine_name="xaas_coresrv",
        db=db,
        glbcfg=glbcfg,
    )


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
        field_list=[
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
    if real_repo.exists(key, schema_override=SCHEMA_NAME):
        print('⚠️ Warning: Edge already exists before test. Deleting it to ensure clean state.')
        real_repo.delete(key, actions=("eval", "commit"), schema_override=SCHEMA_NAME)

    try:
        # 1) Initial state
        print("")
        assert real_repo.exists(key, schema_override=SCHEMA_NAME) is False
        assert real_repo.get(key, schema_override=SCHEMA_NAME) is None

        # 2) Save
        print("\nSaving edge to repository...")
        saved = real_repo.save(edge, actions=("eval", "commit"), schema_override=SCHEMA_NAME)
        assert saved.key == key

        # 3) Exists after save
        assert real_repo.exists(key, schema_override=SCHEMA_NAME) is True

        # 4) Get after save
        loaded = real_repo.get(key, schema_override=SCHEMA_NAME)
        assert loaded is not None

        import rich
        print("\nEdge object content:")
        rich.print_json(data=loaded.model_dump(mode="json"))

        assert loaded.key == key

        # 5) Custom fields verification
        assert len(loaded.field_list.field_list) == len(data["field_list"])

        field_map = {
            (field.key.field_language, field.key.field_name): field.field_value
            for field in loaded.field_list.field_list
        }

        for row in data["field_list"]:
            k = (row["field_language"], row["field_name"])
            assert k in field_map
            assert field_map[k] == row["field_value"]

        # 6) Delete
        assert real_repo.delete(key, actions=("eval", "commit"), schema_override=SCHEMA_NAME) is True

        # 7) Final state
        assert real_repo.exists(key, schema_override=SCHEMA_NAME) is False
        assert real_repo.get(key, schema_override=SCHEMA_NAME) is None

    finally:
        # Always clean up
        if real_repo.exists(key, schema_override=SCHEMA_NAME):
            real_repo.delete(key, actions=("eval", "commit"), schema_override=SCHEMA_NAME)
