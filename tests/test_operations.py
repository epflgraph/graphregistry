import unittest
from dataclasses import dataclass

from graphregistry.domain.models.mdl_edge import Edge, EdgeKey
from graphregistry.domain.models.mdl_node import Node, NodeKey
from graphregistry.workflows.operations.ops_edge import EdgeOperations
from graphregistry.workflows.operations.ops_node import NodeOperations


@dataclass
class FakeNodeRepo:
    existing_keys: set[tuple[str, str, str]]
    save_calls: int = 0

    def exists(self, key: NodeKey) -> bool:
        return (key.institution_id, key.object_type, key.object_id) in self.existing_keys

    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> bool:
        self.save_calls += 1
        self.existing_keys.add((node.key.institution_id, node.key.object_type, node.key.object_id))
        return True

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        return self.existing_keys.discard((key.institution_id, key.object_type, key.object_id)) is None


@dataclass
class FakeEdgeRepo:
    existing_keys: set[tuple[str, str, str, str, str, str, str]]
    save_calls: int = 0

    def exists(self, key: EdgeKey) -> bool:
        return (
            key.from_institution_id,
            key.from_object_type,
            key.from_object_id,
            key.to_institution_id,
            key.to_object_type,
            key.to_object_id,
            key.context,
        ) in self.existing_keys

    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> bool:
        self.save_calls += 1
        self.existing_keys.add(
            (
                edge.key.from_institution_id,
                edge.key.from_object_type,
                edge.key.from_object_id,
                edge.key.to_institution_id,
                edge.key.to_object_type,
                edge.key.to_object_id,
                edge.key.context,
            )
        )
        return True

    def delete(self, key: EdgeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        return self.existing_keys.discard(
            (
                key.from_institution_id,
                key.from_object_type,
                key.from_object_id,
                key.to_institution_id,
                key.to_object_type,
                key.to_object_id,
                key.context,
            )
        ) is None


class NodeOperationsTests(unittest.TestCase):
    def test_insert_uses_save(self) -> None:
        key = NodeKey(institution_id="epfl", object_type="Course", object_id="CS101")
        repo = FakeNodeRepo(existing_keys=set())
        ops = NodeOperations(repo=repo)
        node = Node(key=key)

        self.assertTrue(ops.insert(node))
        self.assertEqual(repo.save_calls, 1)

    def test_update_requires_existing(self) -> None:
        key = NodeKey(institution_id="epfl", object_type="Course", object_id="CS101")
        repo = FakeNodeRepo(existing_keys=set())
        ops = NodeOperations(repo=repo)
        node = Node(key=key)

        with self.assertRaises(ValueError):
            ops.update(node)

    def test_upsert_reports_created(self) -> None:
        key = NodeKey(institution_id="epfl", object_type="Course", object_id="CS101")
        node = Node(key=key)

        repo_missing = FakeNodeRepo(existing_keys=set())
        created_result = NodeOperations(repo=repo_missing).upsert(node)
        self.assertTrue(created_result.success)
        self.assertTrue(created_result.created)

        repo_existing = FakeNodeRepo(existing_keys={("epfl", "Course", "CS101")})
        updated_result = NodeOperations(repo=repo_existing).upsert(node)
        self.assertTrue(updated_result.success)
        self.assertFalse(updated_result.created)


class EdgeOperationsTests(unittest.TestCase):
    def test_insert_uses_save(self) -> None:
        key = EdgeKey(
            from_institution_id="epfl",
            from_object_type="Course",
            from_object_id="CS101",
            to_institution_id="epfl",
            to_object_type="Person",
            to_object_id="alice",
            context="teaches",
        )
        repo = FakeEdgeRepo(existing_keys=set())
        ops = EdgeOperations(repo=repo)
        edge = Edge(key=key)

        self.assertTrue(ops.insert(edge))
        self.assertEqual(repo.save_calls, 1)

    def test_update_requires_existing(self) -> None:
        key = EdgeKey(
            from_institution_id="epfl",
            from_object_type="Course",
            from_object_id="CS101",
            to_institution_id="epfl",
            to_object_type="Person",
            to_object_id="alice",
            context="teaches",
        )
        repo = FakeEdgeRepo(existing_keys=set())
        ops = EdgeOperations(repo=repo)
        edge = Edge(key=key)

        with self.assertRaises(ValueError):
            ops.update(edge)

    def test_upsert_reports_created(self) -> None:
        key = EdgeKey(
            from_institution_id="epfl",
            from_object_type="Course",
            from_object_id="CS101",
            to_institution_id="epfl",
            to_object_type="Person",
            to_object_id="alice",
            context="teaches",
        )
        edge = Edge(key=key)

        repo_missing = FakeEdgeRepo(existing_keys=set())
        created_result = EdgeOperations(repo=repo_missing).upsert(edge)
        self.assertTrue(created_result.success)
        self.assertTrue(created_result.created)

        repo_existing = FakeEdgeRepo(
            existing_keys={("epfl", "Course", "CS101", "epfl", "Person", "alice", "teaches")}
        )
        updated_result = EdgeOperations(repo=repo_existing).upsert(edge)
        self.assertTrue(updated_result.success)
        self.assertFalse(updated_result.created)


if __name__ == "__main__":
    unittest.main()
