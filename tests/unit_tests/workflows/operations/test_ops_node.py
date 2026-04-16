from __future__ import annotations

from graphregistry.domain.models.mdl_base import NodeKey
from graphregistry.domain.models.mdl_concept import DetectedConcept, DetectedConceptList
from graphregistry.domain.models.mdl_node import Node, NodeList
from graphregistry.workflows.operations.ops_node import NodeOperations


class FakeNodeRepo:
    def __init__(self) -> None:
        self.exists_value = False
        self.saved_actions: tuple[str, ...] | None = None
        self.deleted_actions: tuple[str, ...] | None = None
        self.saved_node: Node | None = None

    def exists(self, key: NodeKey) -> bool:
        return self.exists_value

    def get(self, key: NodeKey) -> Node | None:
        return Node(key=key, title="x")

    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        return NodeList(node_list=[Node(key=k, title="x") for k in key_list])

    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Node:
        self.saved_node = node
        self.saved_actions = actions
        return node

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Node]:
        self.saved_actions = actions
        return node_list.node_list

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        self.deleted_actions = actions
        return True


class FakeConceptGateway:
    def detect_concepts(self, text: str) -> DetectedConceptList:
        return DetectedConceptList(concept_list=[DetectedConcept(concept_id="c1", score=1.0, text_source=text)])


def _node() -> Node:
    return Node(key=NodeKey(institution_id="EPFL", object_type="Course", object_id="CS-101"), title="Course")


def test_node_operations_upsert_created_flag_when_missing() -> None:
    repo = FakeNodeRepo()
    repo.exists_value = False
    ops = NodeOperations(repo=repo)

    result = ops.upsert(_node(), actions=("commit",))

    assert result.success is True
    assert result.created is True
    assert repo.saved_actions == ("commit",)


def test_node_operations_upsert_created_false_when_existing() -> None:
    repo = FakeNodeRepo()
    repo.exists_value = True
    ops = NodeOperations(repo=repo)

    result = ops.upsert(_node())

    assert result.success is True
    assert result.created is False


def test_node_operations_insert_and_update_are_backward_compatible_aliases() -> None:
    repo = FakeNodeRepo()
    ops = NodeOperations(repo=repo)

    assert ops.insert(_node(), actions=("eval", "commit")) is True
    assert repo.saved_actions == ("eval", "commit")

    assert ops.update(_node()) is True


def test_node_operations_detect_concepts_requires_gateway() -> None:
    ops = NodeOperations(repo=FakeNodeRepo(), concept_gateway=None)

    try:
        ops.detect_concepts("hello")
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "No concept gateway configured" in str(exc)


def test_node_operations_detect_concepts_uses_gateway_when_available() -> None:
    ops = NodeOperations(repo=FakeNodeRepo(), concept_gateway=FakeConceptGateway())

    out = ops.detect_concepts("text")

    assert len(out.concept_list) == 1
    assert out.concept_list[0].concept_id == "c1"
