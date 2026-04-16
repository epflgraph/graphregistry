# tests/unit_tests/domain/models/test_mdl_subgraph.py
from __future__ import annotations

from graphregistry.domain.models.mdl_base import EdgeKey, NodeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldKey
from graphregistry.domain.models.mdl_subgraph import SubGraph


def make_node_key(object_id: str) -> NodeKey:
    return NodeKey(
        institution_id="EPFL",
        object_type="Course",
        object_id=object_id,
    )


def make_edge_key(from_id: str = "CS101", to_id: str = "alice") -> EdgeKey:
    return EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Course",
        from_object_id=from_id,
        to_institution_id="EPFL",
        to_object_type="Person",
        to_object_id=to_id,
        context="teaches",
    )


def make_node(object_id: str, title: str | None = None) -> Node:
    key = make_node_key(object_id)
    node = Node(
        key=key,
        title=title or f"Title {object_id}",
        text_source="catalog",
        raw_text=f"Raw text for {object_id}",
    )
    node.set_field_value("summary", f"Summary {object_id}", field_language="en")
    return node


def make_edge(from_id: str = "CS101", to_id: str = "alice") -> Edge:
    key = make_edge_key(from_id=from_id, to_id=to_id)
    return Edge(
        key=key,
        field_list={
            "field_list": [
                EdgeField(
                    key=EdgeFieldKey(
                        key=key,
                        field_language="n/a",
                        field_name="role",
                    ),
                    field_value="lecturer",
                )
            ]
        },
    )


def test_subgraph_defaults_are_empty() -> None:
    subgraph = SubGraph()

    assert subgraph.nodes.node_list == []
    assert subgraph.edges.edge_list == []
    assert bool(subgraph) is False
    assert subgraph.node_count() == 0
    assert subgraph.edge_count() == 0
    assert list(subgraph.iter_nodes()) == []
    assert list(subgraph.iter_edges()) == []
    assert subgraph.node_keys() == []
    assert subgraph.edge_keys() == []


def test_subgraph_from_json_and_to_json_round_trip() -> None:
    node = make_node("CS101", title="Intro to Robotics")
    edge = make_edge("CS101", "alice")

    json_data = {
        "nodes": [node.to_json()],
        "edges": [edge.to_json()],
    }

    subgraph = SubGraph.from_json(json_data)
    out = subgraph.to_json()

    assert subgraph.node_count() == 1
    assert subgraph.edge_count() == 1
    assert subgraph.nodes.node_list[0].key == node.key
    assert subgraph.nodes.node_list[0].title == "Intro to Robotics"
    assert subgraph.edges.edge_list[0].key == edge.key
    assert subgraph.edges.edge_list[0].get_field("n/a", "role") is not None
    assert subgraph.edges.edge_list[0].get_field("n/a", "role").field_value == "lecturer"

    assert out["nodes"][0]["key"]["object_id"] == "CS101"
    assert out["nodes"][0]["title"] == "Intro to Robotics"
    assert out["edges"][0]["key"]["from_object_id"] == "CS101"
    assert out["edges"][0]["key"]["to_object_id"] == "alice"


def test_subgraph_append_and_extend_methods() -> None:
    node1 = make_node("CS101")
    node2 = make_node("CS102")
    edge1 = make_edge("CS101", "alice")
    edge2 = make_edge("CS102", "bob")

    subgraph = SubGraph()

    subgraph.append_node(node1)
    subgraph.append_edge(edge1)
    subgraph.extend_nodes([node2])
    subgraph.extend_edges([edge2])

    assert subgraph.node_count() == 2
    assert subgraph.edge_count() == 2
    assert bool(subgraph) is True

    node_ids = [node.key.object_id for node in subgraph.iter_nodes()]
    edge_pairs = [(edge.key.from_object_id, edge.key.to_object_id) for edge in subgraph.iter_edges()]

    assert node_ids == ["CS101", "CS102"]
    assert edge_pairs == [("CS101", "alice"), ("CS102", "bob")]


def test_subgraph_get_and_has_node() -> None:
    node1 = make_node("CS101")
    node2 = make_node("CS102")
    subgraph = SubGraph()

    subgraph.extend_nodes([node1, node2])

    key_existing = make_node_key("CS101")
    key_missing = make_node_key("CS999")

    found = subgraph.get_node(key_existing)

    assert found is not None
    assert found.key == key_existing
    assert found.title == node1.title
    assert subgraph.has_node(key_existing) is True
    assert subgraph.get_node(key_missing) is None
    assert subgraph.has_node(key_missing) is False


def test_subgraph_get_and_has_edge() -> None:
    edge1 = make_edge("CS101", "alice")
    edge2 = make_edge("CS102", "bob")
    subgraph = SubGraph()

    subgraph.extend_edges([edge1, edge2])

    key_existing = make_edge_key("CS101", "alice")
    key_missing = make_edge_key("CS999", "nobody")

    found = subgraph.get_edge(key_existing)

    assert found is not None
    assert found.key == key_existing
    assert found.get_field("n/a", "role") is not None
    assert found.get_field("n/a", "role").field_value == "lecturer"
    assert subgraph.has_edge(key_existing) is True
    assert subgraph.get_edge(key_missing) is None
    assert subgraph.has_edge(key_missing) is False


def test_subgraph_node_keys_and_edge_keys() -> None:
    node1 = make_node("CS101")
    node2 = make_node("CS102")
    edge1 = make_edge("CS101", "alice")
    edge2 = make_edge("CS102", "bob")

    subgraph = SubGraph()
    subgraph.extend_nodes([node1, node2])
    subgraph.extend_edges([edge1, edge2])

    node_keys = subgraph.node_keys()
    edge_keys = subgraph.edge_keys()

    assert node_keys == [node1.key, node2.key]
    assert edge_keys == [edge1.key, edge2.key]
