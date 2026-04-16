# tests/unit_tests/adapters/persistence/mysql/repositories/test_arp_noderepo.py
from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories.arp_noderepo import MySQLNodeRepository
from graphregistry.domain.models.mdl_base import NodeKey


NODE_JSON_FIXTURE: dict[str, Any] = {
    "institution_id": "EPFL",
    "object_type": "Course",
    "object_id": "TEST-101",
    "object_title": "Introduction to Autonomous Systems Design",
    "text_source": "course page description",
    "raw_text": 'This course introduces the principles behind autonomous systems, including perception, decision-making, and control. Students will explore how intelligent agents interact with dynamic environments.\n\nTopics include:\n1. Foundations of autonomous agents\n2. Sensor fusion and perception pipelines\n3. Planning under uncertainty\n4. Reinforcement learning for control\n5. Multi-agent coordination\n6. Safety, ethics, and human-in-the-loop systems\n\nApplications in robotics, self-driving vehicles, and smart infrastructure are discussed. The course combines theoretical lectures with practical projects.\n\nPrerequisites include probability theory, linear algebra, and basic programming in Python.\n\nRecommended reading:\n"Autonomous Systems: Principles and Practice / A. Kumar"\n"Reinforcement Learning: An Introduction / Sutton & Barto"\n\nThe course prepares students for advanced topics in robotics and AI systems engineering.',
    "custom_fields": [
        {
            "field_language": "en",
            "field_name": "bibliography",
            "field_value": '"Autonomous Systems: Principles and Practice / A. Kumar"\n"Reinforcement Learning: An Introduction / Sutton & Barto"',
        },
        {
            "field_language": "en",
            "field_name": "content",
            "field_value": "Foundations of autonomous agents, perception systems, planning algorithms, reinforcement learning, and safety considerations.",
        },
        {
            "field_language": "en",
            "field_name": "evaluation_method",
            "field_value": "Project (40%), midterm exam (30%), final exam (30%).",
        },
        {
            "field_language": "en",
            "field_name": "teaching_method",
            "field_value": "Lectures, hands-on labs, and a semester-long group project.",
        },
        {
            "field_language": "en",
            "field_name": "summary",
            "field_value": "An introduction to the design and implementation of autonomous intelligent systems.",
        },
        {
            "field_language": "en",
            "field_name": "required_courses_obl",
            "field_value": "Linear Algebra, Probability and Statistics, Programming",
        },
        {
            "field_language": "en",
            "field_name": "required_courses_ind",
            "field_value": "Machine Learning, Control Systems",
        },
        {
            "field_language": "en",
            "field_name": "prepares_for_courses",
            "field_value": "Advanced Robotics, Multi-Agent Systems",
        },
        {
            "field_language": "fr",
            "field_name": "summary",
            "field_value": "Introduction à la conception et à l’implémentation de systèmes autonomes intelligents.",
        },
        {
            "field_language": "n/a",
            "field_name": "course_code",
            "field_value": "TEST-101",
        },
        {
            "field_language": "n/a",
            "field_name": "exam_type",
            "field_value": "Winter session\nWritten + Project",
        },
    ],
    "page_profile": {
        "short_code": "TEST-101",
        "name_en_value": "Introduction to Autonomous Systems Design",
        "name_fr_value": "Introduction à la conception de systèmes autonomes",
        "description_short_en_value": "Learn the fundamentals of autonomous systems, including perception, planning, and control.",
        "description_medium_en_value": "This course provides a structured introduction to autonomous systems, combining theoretical foundations with practical applications in robotics and AI.",
        "description_long_en_value": "This course explores the design of autonomous systems capable of perceiving, reasoning, and acting in complex environments. Students will study perception pipelines, planning under uncertainty, reinforcement learning, and system safety. Applications include robotics, autonomous vehicles, and smart infrastructure.",
        "external_key_en": "intro-autonomous-systems-TEST-101",
        "external_url_en": "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101",
        "is_visible": True,
    },
}


class FakeGlobalConfig:
    def __init__(self) -> None:
        self.object_type_to_schema = {"Course": "schema_course"}
        self.page_profile_columns = tuple(NODE_JSON_FIXTURE["page_profile"].keys())


class FakeGraphDB:
    def __init__(self) -> None:
        self.nodes: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.custom_fields: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self.page_profiles: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.deleted_keys: list[tuple[str, str, str]] = []

    def execute_query(self, engine_name: str, query: str) -> list[Any]:
        parts = query.split("|")
        op = parts[0]
        key = tuple(parts[2:5])

        if op == "node_exists":
            return [[1 if key in self.nodes else 0]]

        if op == "node_get_basic":
            if key not in self.nodes:
                return []
            node = self.nodes[key]
            return [[node["object_title"], node["text_source"], node["raw_text"]]]

        if op == "node_get_custom":
            rows = self.custom_fields.get(key, [])
            return [[r["field_language"], r["field_name"], r["field_value"]] for r in rows]

        if op == "node_get_profile":
            row = self.page_profiles.get(key)
            if row is None:
                return []
            return [[row.get(col) for col in NODE_JSON_FIXTURE["page_profile"].keys()]]

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

        if table_name == "Nodes_N_Object":
            key = (
                payload["institution_id"],
                payload["object_type"],
                payload["object_id"],
            )
            self.nodes[key] = {
                "institution_id": payload["institution_id"],
                "object_type": payload["object_type"],
                "object_id": payload["object_id"],
                "object_title": payload["object_title"],
                "text_source": payload["text_source"],
                "raw_text": payload["raw_text"],
            }
            return

        if table_name == "Data_N_Object_T_CustomFields":
            key = (
                payload["institution_id"],
                payload["object_type"],
                payload["object_id"],
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

        if table_name == "Data_N_Object_T_PageProfile":
            key = (
                payload["institution_id"],
                payload["object_type"],
                payload["object_id"],
            )
            row = self.page_profiles.setdefault(key, {})
            row.update({k: v for k, v in payload.items() if k not in {"institution_id", "object_type", "object_id"}})
            return

        raise AssertionError(f"Unexpected table_name: {table_name}")

    def execute_query_in_shell(self, engine_name: str, query: str, verbose: bool = False) -> None:
        parts = query.split("|")
        op = parts[0]
        key = tuple(parts[2:5])

        if op != "node_delete":
            raise AssertionError(f"Unexpected shell query: {query}")

        self.nodes.pop(key, None)
        self.custom_fields.pop(key, None)
        self.page_profiles.pop(key, None)
        self.deleted_keys.append(key)


@pytest.fixture
def repo(monkeypatch: pytest.MonkeyPatch) -> MySQLNodeRepository:
    import graphregistry.adapters.persistence.mysql.repositories.arp_noderepo as repo_module

    fake_paths = {
        "registry": {
            "commit": {
                "node_exists": "node_exists",
                "node_get_basic": "node_get_basic",
                "node_get_custom": "node_get_custom",
                "node_get_profile": "node_get_profile",
                "node_delete": "node_delete",
            }
        }
    }

    def fake_resolve_sql_query(file_path: str, **kwargs: Any) -> str:
        return "|".join(
            [
                file_path,
                kwargs["registry"],
                kwargs["institution_id"],
                kwargs["object_type"],
                kwargs["object_id"],
            ]
        )

    monkeypatch.setattr(repo_module, "sql_queries_paths", fake_paths)
    monkeypatch.setattr(repo_module, "resolve_sql_query", fake_resolve_sql_query)

    return MySQLNodeRepository(
        engine_name="test_engine",
        db=FakeGraphDB(),
        glbcfg=FakeGlobalConfig(),
    )


def test_mysql_node_repository_full_crud_cycle(repo: MySQLNodeRepository) -> None:
    data = deepcopy(NODE_JSON_FIXTURE)
    key = NodeKey(
        institution_id=data["institution_id"],
        object_type=data["object_type"],
        object_id=data["object_id"],
    )

    print('')
    assert repo.exists(key) is False
    assert repo.get(key) is None

    node = MySQLNodeMapper.from_simplified_dict(data)

    assert node.key == key
    assert node.title == data["object_title"]
    assert node.text_source == data["text_source"]
    assert node.raw_text == data["raw_text"]
    assert len(node.field_list.field_list) == len(data["custom_fields"])
    assert node.page_profile is not None
    assert node.page_profile.short_code == "TEST-101"
    assert node.page_profile.name.en.value == "Introduction to Autonomous Systems Design"
    assert node.page_profile.name.fr.value == "Introduction à la conception de systèmes autonomes"
    assert node.page_profile.description.short.en.value.startswith("Learn the fundamentals of autonomous systems")
    assert node.page_profile.external_key.en == "intro-autonomous-systems-TEST-101"
    assert node.page_profile.external_url.en == "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101"
    assert node.page_profile.is_visible is True

    saved = repo.save(node, actions=("eval", "commit"))
    assert saved.key == key

    assert repo.exists(key) is True

    loaded = repo.get(key)
    assert loaded is not None
    assert loaded.key == key
    assert loaded.title == data["object_title"]
    assert loaded.text_source == data["text_source"]
    assert loaded.raw_text == data["raw_text"]

    assert len(loaded.field_list.field_list) == len(data["custom_fields"])
    field_map = {
        (field.key.field_language, field.key.field_name): field.field_value
        for field in loaded.field_list.field_list
    }
    assert field_map[("en", "bibliography")] == data["custom_fields"][0]["field_value"]
    assert field_map[("en", "content")] == data["custom_fields"][1]["field_value"]
    assert field_map[("fr", "summary")] == "Introduction à la conception et à l’implémentation de systèmes autonomes intelligents."
    assert field_map[("n/a", "course_code")] == "TEST-101"
    assert field_map[("n/a", "exam_type")] == "Winter session\nWritten + Project"

    assert loaded.page_profile is not None
    assert loaded.page_profile.key == key
    assert loaded.page_profile.short_code == "TEST-101"
    assert loaded.page_profile.name.en.value == "Introduction to Autonomous Systems Design"
    assert loaded.page_profile.name.fr.value == "Introduction à la conception de systèmes autonomes"
    assert loaded.page_profile.description.short.en.value == data["page_profile"]["description_short_en_value"]
    assert loaded.page_profile.description.medium.en.value == data["page_profile"]["description_medium_en_value"]
    assert loaded.page_profile.description.long.en.value == data["page_profile"]["description_long_en_value"]
    assert loaded.page_profile.external_key.en == data["page_profile"]["external_key_en"]
    assert loaded.page_profile.external_url.en == data["page_profile"]["external_url_en"]
    assert loaded.page_profile.is_visible is True

    simplified = MySQLNodeMapper.to_simplified_dict(loaded)
    assert simplified["institution_id"] == data["institution_id"]
    assert simplified["object_type"] == data["object_type"]
    assert simplified["object_id"] == data["object_id"]
    assert simplified["object_title"] == data["object_title"]
    assert simplified["text_source"] == data["text_source"]
    assert simplified["raw_text"] == data["raw_text"]
    assert len(simplified["custom_fields"]) == len(data["custom_fields"])
    assert simplified["page_profile"]["short_code"] == "TEST-101"
    assert simplified["page_profile"]["name_en_value"] == "Introduction to Autonomous Systems Design"
    assert simplified["page_profile"]["name_fr_value"] == "Introduction à la conception de systèmes autonomes"
    assert simplified["page_profile"]["description_short_en_value"] == data["page_profile"]["description_short_en_value"]
    assert simplified["page_profile"]["external_key_en"] == data["page_profile"]["external_key_en"]
    assert simplified["page_profile"]["external_url_en"] == data["page_profile"]["external_url_en"]
    assert simplified["page_profile"]["is_visible"] == 1

    rehydrated = MySQLNodeMapper.from_simplified_dict(simplified)
    assert rehydrated.key == key
    assert rehydrated.title == loaded.title
    assert rehydrated.text_source == loaded.text_source
    assert rehydrated.raw_text == loaded.raw_text
    assert len(rehydrated.field_list.field_list) == len(loaded.field_list.field_list)
    assert rehydrated.page_profile is not None
    assert rehydrated.page_profile.short_code == loaded.page_profile.short_code
    assert rehydrated.page_profile.name.en.value == loaded.page_profile.name.en.value
    assert rehydrated.page_profile.name.fr.value == loaded.page_profile.name.fr.value
    assert rehydrated.page_profile.description.short.en.value == loaded.page_profile.description.short.en.value
    assert rehydrated.page_profile.external_key.en == loaded.page_profile.external_key.en
    assert rehydrated.page_profile.external_url.en == loaded.page_profile.external_url.en
    assert rehydrated.page_profile.is_visible is True

    assert repo.delete(key, actions=("eval",)) is False
    assert repo.exists(key) is True

    assert repo.delete(key, actions=("eval", "commit")) is True
    assert repo.exists(key) is False
    assert repo.get(key) is None

    fake_db = repo.db
    assert isinstance(fake_db, FakeGraphDB)
    assert key.to_tuple() in fake_db.deleted_keys
    assert key.to_tuple() not in fake_db.nodes
    assert key.to_tuple() not in fake_db.custom_fields
    assert key.to_tuple() not in fake_db.page_profiles
