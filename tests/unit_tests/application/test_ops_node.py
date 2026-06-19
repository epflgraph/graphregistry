# tests/unit_tests/application/test_ops_node.py
"""Unit tests for NodeOperations using a fake repository adapter."""
from __future__ import annotations

import pytest

from graphregistry.application.operations.ops_node import NodeOperations
from graphregistry.domain.models.entities.mdl_base import NodeKey, NodeKeyList
from graphregistry.domain.models.entities.mdl_conceptmap import Concept, ScoredConcept, ScoredConceptList
from graphregistry.domain.models.entities.mdl_node import Node, NodeList
from tests.conftest import FakeNodeRepository, make_node


class TestNodeOperationsCrud:
    def test_save_and_get(self, node_ops: NodeOperations, fake_node_repo: FakeNodeRepository) -> None:
        node = make_node(object_id="CS-433", title="ML")
        saved = node_ops.save(node)
        assert saved.key == node.key

        loaded = node_ops.get(node.key)
        assert loaded is not None
        assert loaded.title == "ML"

    def test_exists_and_exists_many(self, node_ops: NodeOperations) -> None:
        node1 = make_node(object_id="CS-433")
        node2 = make_node(object_id="MATH-203")
        node_ops.save_many(NodeList(item_list=[node1, node2]))

        assert node_ops.exists(node1.key) is True
        assert node_ops.exists(make_node(object_id="MISSING").key) is False
        assert node_ops.exists_many(NodeKeyList(item_list=[node1.key, node2.key])) == [True, True]

    def test_get_many_omits_missing(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433")
        node_ops.save(node)

        missing_key = make_node(object_id="MISSING").key
        results = node_ops.get_many(NodeKeyList(item_list=[node.key, missing_key]))
        assert len(results.item_list) == 1
        assert results.item_list[0].key == node.key

    def test_list_with_pattern(self, node_ops: NodeOperations) -> None:
        node_ops.save(make_node(object_id="CS-433"))
        node_ops.save(make_node(object_id="CS-250"))
        node_ops.save(make_node(object_id="MATH-203"))

        rows = node_ops.list(object_type="Course", id_pattern="CS*")
        assert len(rows) == 2

    def test_delete(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433")
        node_ops.save(node)
        assert node_ops.exists(node.key) is True

        deleted = node_ops.delete(node.key)
        assert deleted is True
        assert node_ops.exists(node.key) is False

    def test_delete_many(self, node_ops: NodeOperations) -> None:
        node1 = make_node(object_id="CS-433")
        node2 = make_node(object_id="MATH-203")
        node_ops.save_many(NodeList(item_list=[node1, node2]))

        results = node_ops.delete_many(NodeKeyList(item_list=[node1.key, make_node(object_id="MISSING").key]))
        assert results == [True, False]

    def test_eval_action_does_not_persist(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433")
        node_ops.save(node, actions=("eval",))
        assert node_ops.exists(node.key) is False


class TestNodeOperationsConcepts:
    def test_has_concepts_false_for_empty_detected(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433")
        node_ops.save(node)
        assert node_ops.has_concepts(node.key, "detected") is False

    def test_has_concepts_true_after_population(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433")
        node.concepts.detected = ScoredConceptList(item_list=[
            ScoredConcept(concept=Concept(id="c1", name="ML"), score=0.9),
        ])
        node_ops.save(node)
        assert node_ops.has_concepts(node.key, "detected") is True

    def test_has_concepts_raises_for_missing_node(self, node_ops: NodeOperations) -> None:
        with pytest.raises(ValueError, match="not found"):
            node_ops.has_concepts(make_node(object_id="MISSING").key, "detected")

    def test_get_with_no_concepts(self, node_ops: NodeOperations) -> None:
        empty = make_node(object_id="CS-433")
        enriched = make_node(object_id="MATH-203")
        enriched.concepts.detected = ScoredConceptList(item_list=[
            ScoredConcept(concept=Concept(id="c1", name="Math"), score=0.8),
        ])
        node_ops.save_many(NodeList(item_list=[empty, enriched]))

        results = node_ops.get_with_no_concepts(object_type="Course")
        assert len(results.item_list) == 1
        assert results.item_list[0].key.object_id == "CS-433"


class TestNodeOperationsEnrich:
    def test_enrich_with_concepts_requires_gateway(self, node_ops: NodeOperations) -> None:
        node = make_node(object_id="CS-433", raw_text="Machine learning")
        with pytest.raises(ValueError, match="gateway not configured"):
            node_ops.enrich_with_concepts(node)

    def test_enrich_with_concepts_populates_detected(self) -> None:
        class FakeConceptGateway:
            def detect_concepts(self, text: str) -> ScoredConceptList:
                return ScoredConceptList(item_list=[
                    ScoredConcept(concept=Concept(id="c1", name="ML"), score=0.95),
                ])

        repo = FakeNodeRepository()
        ops = NodeOperations(repo=repo, ai_gateways={"concept_detection": FakeConceptGateway()})
        node = make_node(object_id="CS-433", raw_text="Machine learning")
        result = ops.enrich_with_concepts(node)
        assert result.concepts.detected.item_list[0].concept.name == "ML"

    def test_enrich_with_concepts_list(self) -> None:
        class FakeConceptGateway:
            def detect_concepts(self, text: str) -> ScoredConceptList:
                return ScoredConceptList(item_list=[])

        repo = FakeNodeRepository()
        ops = NodeOperations(repo=repo, ai_gateways={"concept_detection": FakeConceptGateway()})
        node_list = NodeList(item_list=[make_node(object_id="CS-433", raw_text="ML")])
        result = ops.enrich_with_concepts(node_list)
        assert len(result.item_list) == 1
