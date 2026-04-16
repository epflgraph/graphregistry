from __future__ import annotations

import pytest

from graphregistry.adapters.services.schema.asv_schema_multitenant import MultiTenantSchemaResolver
from graphregistry.domain.models.mdl_base import EdgeKey, NodeKey


def _tenant_config() -> dict[str, dict]:
    return {
        "EPFL": {
            "engine_name": "engine_epfl",
            "node_schema_map": {"Course": "schema_course"},
            "edge_schema_map": {("Course", "Person"): "schema_course_person"},
        }
    }


def test_multitenant_resolver_for_node_returns_engine_and_schema() -> None:
    resolver = MultiTenantSchemaResolver(tenant_config=_tenant_config())

    out = resolver.for_node(NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"))

    assert out == ("engine_epfl", "schema_course")


def test_multitenant_resolver_for_edge_normalizes_pair_order() -> None:
    resolver = MultiTenantSchemaResolver(tenant_config=_tenant_config())

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

    assert out == ("engine_epfl", "schema_course_person")


def test_multitenant_resolver_unknown_tenant_raises() -> None:
    resolver = MultiTenantSchemaResolver(tenant_config=_tenant_config())

    with pytest.raises(ValueError, match="Unknown tenant: ETHZ"):
        resolver.for_node(NodeKey(institution_id="ETHZ", object_type="Course", object_id="X"))
