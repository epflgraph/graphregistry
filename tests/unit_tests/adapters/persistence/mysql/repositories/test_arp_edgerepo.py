# tests/unit_tests/adapters/persistence/mysql/repositories/test_arp_edgerepo.py
from __future__ import annotations
from copy import deepcopy
from typing import Any, cast
import pytest
from graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo import MySQLEdgeRepository
from graphregistry.domain.models.mdl_base import EdgeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList

EDGE_JSON_FIXTURE: dict[str, Any] = {
    "from_institution_id": "EPFL",
    "from_object_type": "Course",
    "from_object_id": "TEST-101",
    "to_institution_id": "EPFL",
    "to_object_type": "Person",
    "to_object_id": "01010101",
    "context": "teacher",
    "field_list": [
        {
            "field_language": "n/a",
            "field_name": "teaching_assignment_year",
            "field_value": "2023-2024",
        }
    ],
}

class FakeSchemaResolver:
    def for_node(self, key) -> tuple[str, str]:
        return ("test_engine", "schema_course")

    def for_edge(self, key: EdgeKey) -> tuple[str, str]:
        return ("test_engine", "schema_course_person")

class FakeGraphDB:
    def __init__(self) -> None:
        self.edges: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}
        self.custom_fields: dict[tuple[str, str, str, str, str, str, str], list[dict[str, Any]]] = {}
        self.deleted_keys: list[tuple[str, str, str, str, str, str, str]] = []

    def execute_query(self, engine_name: str, query: str) -> list[Any]:
        parts = query.split("|")
        op = parts[0]
        key = tuple(parts[2:9])

        if op == "edge_exists":
            return [[1 if key in self.edges else 0]]

        if op == "edge_get_custom":
            rows = self.custom_fields.get(cast(tuple[str, str, str, str, str, str, str], key), [])
            return [[r["field_language"], r["field_name"], r["field_value"]] for r in rows]

        raise AssertionError(f"Unexpected query: {query}")

    def execute_upsert_row(
        self,
        engine_name: str,
        schema_name: str,
        table_name: str,
        key_column_names: list[str],
        key_column_values: list[Any],
        upd_column_names: list[str] | tuple[str, ...],
        upd_column_values: list[Any] | tuple[Any, ...],
        actions: tuple[str, ...],
    ) -> None:
        payload = dict(zip(key_column_names, key_column_values))
        payload.update(dict(zip(upd_column_names, upd_column_values)))

        if table_name == "Edges_N_Object_N_Object_T_ChildToParent":
            key = (
                payload["from_institution_id"],
                payload["from_object_type"],
                payload["from_object_id"],
                payload["to_institution_id"],
                payload["to_object_type"],
                payload["to_object_id"],
                payload["context"],
            )
            self.edges[key] = {
                "from_institution_id": payload["from_institution_id"],
                "from_object_type": payload["from_object_type"],
                "from_object_id": payload["from_object_id"],
                "to_institution_id": payload["to_institution_id"],
                "to_object_type": payload["to_object_type"],
                "to_object_id": payload["to_object_id"],
                "context": payload["context"],
            }
            return

        if table_name == "Data_N_Object_N_Object_T_CustomFields":
            key = (
                payload["from_institution_id"],
                payload["from_object_type"],
                payload["from_object_id"],
                payload["to_institution_id"],
                payload["to_object_type"],
                payload["to_object_id"],
                payload["context"],
            )
            rows = self.custom_fields.setdefault(key, [])
            match = None
            for row in rows:
                if row["field_language"] == payload["field_language"] and row["field_name"] == payload["field_name"]:
                    match = row
                    break

            if match is None:
                rows.append(
                    {
                        "field_language": payload["field_language"],
                        "field_name": payload["field_name"],
                        "field_value": payload["field_value"],
                    }
                )
            else:
                match["field_value"] = payload["field_value"]
            return

        raise AssertionError(f"Unexpected table_name: {table_name}")

    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False) -> None:
        parts = query.split("|")
        op = parts[0]
        key = tuple(parts[2:9])

        if op != "edge_delete":
            raise AssertionError(f"Unexpected shell query: {query}")

        self.edges.pop(cast(tuple[str, str, str, str, str, str, str], key), None)
        self.custom_fields.pop(cast(tuple[str, str, str, str, str, str, str], key), None)
        self.deleted_keys.append(cast(tuple[str, str, str, str, str, str, str], key))

@pytest.fixture
def repo(monkeypatch: pytest.MonkeyPatch) -> MySQLEdgeRepository:
    import graphregistry.adapters.persistence.mysql.repositories.arp_edgerepo as repo_module

    fake_paths = {
        "registry": {
            "commit": {
                "edge_exists": "edge_exists",
                "edge_get_custom": "edge_get_custom",
                "edge_delete": "edge_delete",
            }
        }
    }

    def fake_resolve_sql_query(file_path: str, **kwargs: Any) -> str:
        return "|".join(
            [
                file_path,
                kwargs["registry"],
                kwargs["from_institution_id"],
                kwargs["from_object_type"],
                kwargs["from_object_id"],
                kwargs["to_institution_id"],
                kwargs["to_object_type"],
                kwargs["to_object_id"],
                kwargs["context"],
            ]
        )

    monkeypatch.setattr(repo_module, "sql_queries_paths", fake_paths)
    monkeypatch.setattr(repo_module, "resolve_sql_query", fake_resolve_sql_query)

    return MySQLEdgeRepository(
        db=FakeGraphDB(), # type: ignore
        schema_resolver=FakeSchemaResolver()
    )

def make_edge_from_fixture(data: dict[str, Any]) -> Edge:
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
                    field_language=field["field_language"],
                    field_name=field["field_name"],
                ),
                field_value=field["field_value"],
            )
            for field in data["field_list"]
        ]
    )

    return Edge(key=key, field_list=field_list)

def test_mysql_edge_repository_full_crud_cycle(repo: MySQLEdgeRepository) -> None:
    data = deepcopy(EDGE_JSON_FIXTURE)
    key = EdgeKey(
        from_institution_id=data["from_institution_id"],
        from_object_type=data["from_object_type"],
        from_object_id=data["from_object_id"],
        to_institution_id=data["to_institution_id"],
        to_object_type=data["to_object_type"],
        to_object_id=data["to_object_id"],
        context=data["context"],
    )

    print("")
    assert repo.exists(key) is False
    assert repo.get(key) is None

    edge = make_edge_from_fixture(data)

    assert edge.key == key
    assert len(edge.field_list.field_list) == len(data["field_list"])
    assert edge.field_list.field_list[0].key.field_language == "n/a"
    assert edge.field_list.field_list[0].key.field_name == "teaching_assignment_year"
    assert edge.field_list.field_list[0].field_value == "2023-2024"

    saved = repo.save(edge, actions=("eval", "commit"))
    assert saved.key == key

    assert repo.exists(key) is True

    loaded = repo.get(key)
    assert loaded is not None
    assert loaded.key == key
    assert len(loaded.field_list.field_list) == len(data["field_list"])

    field_map = {
        (field.key.field_language, field.key.field_name): field.field_value
        for field in loaded.field_list.field_list
    }
    assert field_map[("n/a", "teaching_assignment_year")] == "2023-2024"

    assert repo.delete(key, actions=("eval",)) is False
    assert repo.exists(key) is True

    assert repo.delete(key, actions=("eval", "commit")) is True
    assert repo.exists(key) is False
    assert repo.get(key) is None

    fake_db = repo.db
    assert isinstance(fake_db, FakeGraphDB)
    assert key.to_tuple() in fake_db.deleted_keys
    assert key.to_tuple() not in fake_db.edges
    assert key.to_tuple() not in fake_db.custom_fields
