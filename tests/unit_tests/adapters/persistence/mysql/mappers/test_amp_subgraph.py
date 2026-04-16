# tests/unit_tests/adapters/persistence/mysql/mappers/test_amp_subgraph.py
from __future__ import annotations

from graphregistry.adapters.persistence.mysql.mappers.amp_subgraph import MySQLSubGraphMapper
from graphregistry.domain.models.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeFieldList, EdgeList
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldKey, NodeFieldList, NodeList
from graphregistry.domain.models.mdl_subgraph import SubGraph


NODE_1 = {
    "institution_id": "EPFL",
    "object_type": "Course",
    "object_id": "TEST-101",
    "object_title": "Introduction to Autonomous Systems Design",
    "text_source": "course page description",
    "raw_text": "Autonomous systems course raw text.",
    "custom_fields": [
        {
            "field_language": "en",
            "field_name": "summary",
            "field_value": "An introduction to autonomous systems.",
        },
        {
            "field_language": "n/a",
            "field_name": "course_code",
            "field_value": "TEST-101",
        },
    ],
    "page_profile": {
        "short_code": "TEST-101",
        "name_en_value": "Introduction to Autonomous Systems Design",
        "description_short_en_value": "Learn the fundamentals of autonomous systems.",
        "external_key_en": "intro-autonomous-systems-TEST-101",
        "external_url_en": "https://edu.epfl.ch/coursebook/en/intro-autonomous-systems-TEST-101",
        "is_visible": True,
    },
}

NODE_2 = {
    "institution_id": "EPFL",
    "object_type": "Person",
    "object_id": "01010101",
    "object_title": "Alice Example",
    "text_source": "people page",
    "raw_text": "Professor in robotics and AI.",
    "custom_fields": [
        {
            "field_language": "en",
            "field_name": "position",
            "field_value": "Professor",
        }
    ],
    "page_profile": {
        "short_code": "ALICE",
        "name_en_value": "Alice Example",
        "description_short_en_value": "Professor in robotics and AI.",
        "external_key_en": "alice-example",
        "external_url_en": "https://people.epfl.ch/alice-example",
        "is_visible": True,
    },
}

EDGE_1 = {
    "from_institution_id": "EPFL",
    "from_object_type": "Course",
    "from_object_id": "TEST-101",
    "to_institution_id": "EPFL",
    "to_object_type": "Person",
    "to_object_id": "01010101",
    "context": "teacher",
    "custom_fields": [
        {
            "field_language": "n/a",
            "field_name": "teaching_assignment_year",
            "field_value": "2023-2024",
        }
    ],
}

SUBGRAPH_DICT = {
    "nodes": [NODE_1, NODE_2],
    "edges": [EDGE_1],
}


def test_from_dict_builds_subgraph() -> None:
    subgraph = MySQLSubGraphMapper.from_dict(SUBGRAPH_DICT)

    assert isinstance(subgraph, SubGraph)
    assert isinstance(subgraph.nodes, NodeList)
    assert isinstance(subgraph.edges, EdgeList)

    assert len(subgraph.nodes.node_list) == 2
    assert len(subgraph.edges.edge_list) == 1

    node_1 = subgraph.nodes.node_list[0]
    node_2 = subgraph.nodes.node_list[1]
    edge_1 = subgraph.edges.edge_list[0]

    assert node_1.key == NodeKey(institution_id="EPFL", object_type="Course", object_id="TEST-101")
    assert node_1.title == "Introduction to Autonomous Systems Design"
    assert node_1.page_profile is not None
    assert node_1.page_profile.short_code == "TEST-101"
    assert node_1.page_profile.name.en.value == "Introduction to Autonomous Systems Design"

    assert node_2.key == NodeKey(institution_id="EPFL", object_type="Person", object_id="01010101")
    assert node_2.title == "Alice Example"
    assert node_2.page_profile is not None
    assert node_2.page_profile.short_code == "ALICE"

    assert edge_1.key == EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="01010101",
        context="teacher",
    )
    assert len(edge_1.field_list.field_list) == 1
    assert edge_1.field_list.field_list[0].key.field_name == "teaching_assignment_year"
    assert edge_1.field_list.field_list[0].field_value == "2023-2024"


def test_to_dict_round_trip() -> None:
    subgraph = MySQLSubGraphMapper.from_dict(SUBGRAPH_DICT)
    out = MySQLSubGraphMapper.to_dict(subgraph)

    assert set(out.keys()) == {"nodes", "edges"}
    assert len(out["nodes"]) == 2
    assert len(out["edges"]) == 1

    assert out["nodes"][0]["institution_id"] == "EPFL"
    assert out["nodes"][0]["object_type"] == "Course"
    assert out["nodes"][0]["object_id"] == "TEST-101"
    assert out["nodes"][0]["object_title"] == "Introduction to Autonomous Systems Design"
    assert out["nodes"][0]["page_profile"]["short_code"] == "TEST-101"
    assert out["nodes"][0]["page_profile"]["name_en_value"] == "Introduction to Autonomous Systems Design"

    assert out["nodes"][1]["object_type"] == "Person"
    assert out["nodes"][1]["object_id"] == "01010101"

    assert out["edges"][0]["from_object_type"] == "Course"
    assert out["edges"][0]["to_object_type"] == "Person"
    assert out["edges"][0]["context"] == "teacher"
    assert out["edges"][0]["custom_fields"][0]["field_name"] == "teaching_assignment_year"
    assert out["edges"][0]["custom_fields"][0]["field_value"] == "2023-2024"


def test_from_parts_accepts_lists() -> None:
    node_key = NodeKey(institution_id="EPFL", object_type="Course", object_id="TEST-101")
    edge_key = EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id="TEST-101",
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id="01010101",
        context="teacher",
    )

    node = Node(
        key=node_key,
        title="Course title",
        text_source="source",
        raw_text="raw text",
        field_list=NodeFieldList(
            field_list=[
                NodeField(
                    key=NodeFieldKey(
                        key=node_key,
                        field_language="en",
                        field_name="summary",
                    ),
                    field_value="Summary",
                )
            ]
        ),
    )

    edge = Edge(
        key=edge_key,
        field_list=EdgeFieldList(
            field_list=[
                EdgeField(
                    key=EdgeFieldKey(
                        key=edge_key,
                        field_language="n/a",
                        field_name="role",
                    ),
                    field_value="teacher",
                )
            ]
        ),
    )

    subgraph = MySQLSubGraphMapper.from_parts(nodes=[node], edges=[edge])

    assert isinstance(subgraph, SubGraph)
    assert len(subgraph.nodes.node_list) == 1
    assert len(subgraph.edges.edge_list) == 1
    assert subgraph.nodes.node_list[0] == node
    assert subgraph.edges.edge_list[0] == edge


def test_from_parts_accepts_nodelist_and_edgelist() -> None:
    node = Node(key=NodeKey(institution_id="EPFL", object_type="Course", object_id="TEST-101"))
    edge = Edge(
        key=EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="TEST-101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="01010101",
            context="teacher",
        )
    )

    node_list = NodeList(node_list=[node])
    edge_list = EdgeList(edge_list=[edge])

    subgraph = MySQLSubGraphMapper.from_parts(nodes=node_list, edges=edge_list)

    assert subgraph.nodes is node_list
    assert subgraph.edges is edge_list
    assert len(subgraph.nodes.node_list) == 1
    assert len(subgraph.edges.edge_list) == 1


def test_from_parts_defaults_to_empty_subgraph() -> None:
    subgraph = MySQLSubGraphMapper.from_parts()

    assert isinstance(subgraph, SubGraph)
    assert isinstance(subgraph.nodes, NodeList)
    assert isinstance(subgraph.edges, EdgeList)
    assert len(subgraph.nodes.node_list) == 0
    assert len(subgraph.edges.edge_list) == 0


def test_from_dict_rejects_invalid_inputs() -> None:
    try:
        MySQLSubGraphMapper.from_dict(None)  # type: ignore[arg-type]
        assert False, "Expected AssertionError"
    except AssertionError as e:
        assert str(e) == "Input data must be a dictionary"

    try:
        MySQLSubGraphMapper.from_dict({"edges": []})
        assert False, "Expected AssertionError"
    except AssertionError as e:
        assert str(e) == "Input data must contain a 'nodes' list"

    try:
        MySQLSubGraphMapper.from_dict({"nodes": [], "edges": {}})
        assert False, "Expected AssertionError"
    except AssertionError as e:
        assert str(e) == "Input data must contain an 'edges' list"
