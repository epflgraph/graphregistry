from __future__ import annotations

from graphregistry.domain.models.mdl_base import EdgeKey, NodeKey


class DummySchemaResolver:
    def for_node(self, key: NodeKey) -> tuple[str, str]:
        return ("engine", "node_schema")

    def for_edge(self, key: EdgeKey) -> tuple[str, str]:
        return ("engine", "edge_schema")


def test_schema_resolver_shape_is_usable() -> None:
    resolver = DummySchemaResolver()
    node_out = resolver.for_node(NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"))
    edge_out = resolver.for_edge(
        EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS-101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="123",
            context="teacher",
        )
    )

    assert node_out == ("engine", "node_schema")
    assert edge_out == ("engine", "edge_schema")
