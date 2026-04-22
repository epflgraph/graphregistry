from __future__ import annotations

from types import SimpleNamespace

from graphregistry.adapters.services.schema.asv_schema_default import DefaultSchemaResolver
from graphregistry.domain.models.entities.mdl_base import EdgeKey, NodeKey


def test_default_schema_resolver_for_node_uses_object_type_mapping() -> None:
    glbcfg = SimpleNamespace(
        object_type_to_schema={"Course": "schema_registry"},
        object2object_type_to_schema={},
    )
    resolver = DefaultSchemaResolver(engine_name="engine_a", glbcfg=glbcfg)  # type: ignore[arg-type]

    out = resolver.for_node(NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"))

    assert out == ("engine_a", "schema_registry")


def test_default_schema_resolver_for_edge_normalizes_edge_type_order() -> None:
    glbcfg = SimpleNamespace(
        object_type_to_schema={},
        object2object_type_to_schema={("Course", "Person"): "schema_course_person"},
    )
    resolver = DefaultSchemaResolver(engine_name="engine_a", glbcfg=glbcfg)  # type: ignore[arg-type]

    key = EdgeKey(
        from_institution_id="EPFL",
        from_object_type="Person",
        from_object_id="123",
        to_institution_id="EPFL",
        to_object_type="Course",
        to_object_id="CS-101",
        context="teacher",
    )

    out = resolver.for_edge(key)

    assert out == ("engine_a", "schema_course_person")
