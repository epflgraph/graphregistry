from __future__ import annotations

from graphregistry.domain.models.mdl_base import EdgeKey
from graphregistry.domain.models.mdl_edge import Edge, EdgeList
from graphregistry.workflows.operations.ops_edge import EdgeOperations


class FakeEdgeRepo:
    def __init__(self) -> None:
        self.exists_value = False
        self.saved_actions: tuple[str, ...] | None = None
        self.deleted_actions: tuple[str, ...] | None = None

    def exists(self, key: EdgeKey) -> bool:
        return self.exists_value

    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        return [self.exists(k) for k in key_list]

    def get(self, key: EdgeKey) -> Edge | None:
        return Edge(key=key)

    def get_many(self, key_list: list[EdgeKey]) -> EdgeList:
        return EdgeList(edge_list=[Edge(key=k) for k in key_list])

    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Edge:
        self.saved_actions = actions
        return edge

    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Edge]:
        self.saved_actions = actions
        return edge_list.edge_list

    def delete(self, key: EdgeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        self.deleted_actions = actions
        return True

    def delete_many(self, key_list: list[EdgeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool | None]:
        self.deleted_actions = actions
        return [True, None]


def _edge() -> Edge:
    return Edge(
        key=EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS-101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="123",
            context="teacher",
        )
    )


def test_edge_operations_upsert_sets_created_true_when_missing() -> None:
    repo = FakeEdgeRepo()
    repo.exists_value = False
    ops = EdgeOperations(repo=repo)

    result = ops.upsert(_edge(), actions=("commit",))

    assert result.success is True
    assert result.created is True
    assert repo.saved_actions == ("commit",)


def test_edge_operations_upsert_sets_created_false_when_existing() -> None:
    repo = FakeEdgeRepo()
    repo.exists_value = True
    ops = EdgeOperations(repo=repo)

    result = ops.upsert(_edge())

    assert result.success is True
    assert result.created is False


def test_edge_operations_delete_many_normalizes_truthy_values() -> None:
    ops = EdgeOperations(repo=FakeEdgeRepo())
    edge_key = _edge().key

    out = ops.delete_many([edge_key, edge_key], actions=("eval", "commit"))

    assert out == [True, False]
