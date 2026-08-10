# tests/unit_tests/application/validation/test_allowed_types.py
"""Unit tests for the allowed-types application policy."""
from __future__ import annotations

import pytest

from graphregistry.application.policies.pol_graphunits import GraphUnitsValidator
from graphregistry.domain.exceptions import DisallowedTypeError
from tests.conftest import make_edge, make_node


class TestAllowedTypesValidator:
    @pytest.fixture
    def validator(self) -> GraphUnitsValidator:
        return GraphUnitsValidator(
            allowed_node_types={"Course", "Person"},
            allowed_edge_tuples={("Course", "Person", "teacher")},
        )

    def test_validate_node_accepts_allowed(self, validator: GraphUnitsValidator) -> None:
        node = make_node(object_type="Course")
        validator.validate_node(node)  # should not raise

    def test_validate_node_rejects_disallowed(self, validator: GraphUnitsValidator) -> None:
        node = make_node(object_type="Slide")
        with pytest.raises(DisallowedTypeError, match="not an allowed type"):
            validator.validate_node(node)

    def test_validate_nodes_checks_all_items(self, validator: GraphUnitsValidator) -> None:
        nodes = [make_node(object_type="Course"), make_node(object_type="Slide")]
        with pytest.raises(DisallowedTypeError, match="Slide"):
            validator.validate_nodes(nodes)

    def test_validate_edge_accepts_allowed(self, validator: GraphUnitsValidator) -> None:
        edge = make_edge(
            from_object_type="Course",
            to_object_type="Person",
            context="teacher",
        )
        validator.validate_edge(edge)  # should not raise

    def test_validate_edge_rejects_disallowed_context(self, validator: GraphUnitsValidator) -> None:
        edge = make_edge(
            from_object_type="Course",
            to_object_type="Person",
            context="taught_by",
        )
        with pytest.raises(DisallowedTypeError, match="not an allowed type"):
            validator.validate_edge(edge)

    def test_validate_edges_checks_all_items(self, validator: GraphUnitsValidator) -> None:
        edges = [
            make_edge(
                from_object_type="Course",
                to_object_type="Person",
                context="teacher",
            ),
            make_edge(
                from_object_type="Course",
                to_object_type="Person",
                context="taught_by",
            ),
        ]
        with pytest.raises(DisallowedTypeError, match="not an allowed type"):
            validator.validate_edges(edges)
