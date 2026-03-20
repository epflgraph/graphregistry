import unittest
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

from graphregistry.adapters.mysql.adp_edgerepo import MySQLEdgeRepository
from graphregistry.adapters.mysql.adp_noderepo import MySQLNodeRepository
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeKey
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldKey, NodeKey


@dataclass
class FakeDB:
    counts: dict[str, int] = field(default_factory=dict)
    query_log: list[dict[str, Any]] = field(default_factory=list)

    def execute_query(self, engine_name: str, query: str, params: dict[str, Any] | None = None, **kwargs):
        self.query_log.append({"engine_name": engine_name, "query": query, "params": params, "kwargs": kwargs})
        if "SELECT COUNT(*)" in query and params is not None:
            key = str(sorted(params.items()))
            return [(self.counts.get(key, 0),)]
        if "Data_N_Object_T_CustomFields" in query:
            return [("en", "title", "Graph Theory")]
        if "Data_N_Object_N_Object_T_CustomFields" in query:
            return [("en", "weight", "0.8")]
        return []


@dataclass
class FakeRegistryDB:
    inserts: list[dict[str, Any]] = field(default_factory=list)
    delete_nodes_calls: list[dict[str, Any]] = field(default_factory=list)

    def registry_insert(self, **kwargs):
        self.inserts.append(kwargs)
        return [{"column": "primary_key", "result": "key is new"}]

    def delete_nodes_by_ids(self, **kwargs):
        self.delete_nodes_calls.append(kwargs)
        return {"ok": True}


def _fake_config() -> Any:
    return SimpleNamespace(
        object_type_to_schema={"Course": "graph_registry", "Person": "graph_registry", "Lecture": "graph_lectures"},
        schema_registry="graph_registry",
        schema_lectures="graph_lectures",
    )


class MySQLNodeRepositoryTests(unittest.TestCase):
    def test_save_writes_node_and_custom_fields(self) -> None:
        db = FakeDB()
        bridge = FakeRegistryDB()
        repo = MySQLNodeRepository(db=db, registry_db=bridge, glbcfg=_fake_config())
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS101")
        field = NodeField(key=NodeFieldKey(key=key, field_language="en", field_name="title"), field_value="Graph Theory")
        node = Node(key=key, field_list={"field_list": [field]})

        repo.save(node, actions=("eval",))

        self.assertEqual(len(bridge.inserts), 2)
        self.assertEqual(bridge.inserts[0]["table_name"], "Nodes_N_Object")
        self.assertEqual(bridge.inserts[1]["table_name"], "Data_N_Object_T_CustomFields")

    def test_delete_uses_dbbridge_delete_nodes(self) -> None:
        key = NodeKey(institution_id="EPFL", object_type="Course", object_id="CS101")
        exists_key = str(sorted(key.model_dump(mode="python").items()))
        db = FakeDB(counts={exists_key: 1})
        bridge = FakeRegistryDB()
        repo = MySQLNodeRepository(db=db, registry_db=bridge, glbcfg=_fake_config())

        ok = repo.delete(key, actions=("eval", "commit"))

        self.assertTrue(ok)
        self.assertEqual(len(bridge.delete_nodes_calls), 1)


class MySQLEdgeRepositoryTests(unittest.TestCase):
    def test_save_writes_edge_and_custom_fields(self) -> None:
        db = FakeDB()
        bridge = FakeRegistryDB()
        repo = MySQLEdgeRepository(db=db, registry_db=bridge, glbcfg=_fake_config())
        key = EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="alice",
            context="teaches",
        )
        field = EdgeField(key=EdgeFieldKey(key=key, field_language="en", field_name="weight"), field_value="0.8")
        edge = Edge(key=key, field_list={"field_list": [field]})

        repo.save(edge, actions=("eval",))

        self.assertEqual(len(bridge.inserts), 2)
        self.assertEqual(bridge.inserts[0]["table_name"], "Edges_N_Object_N_Object_T_ChildToParent")
        self.assertEqual(bridge.inserts[1]["table_name"], "Data_N_Object_N_Object_T_CustomFields")

    def test_get_by_key_loads_custom_fields(self) -> None:
        key = EdgeKey(
            from_institution_id="EPFL",
            from_object_type="Course",
            from_object_id="CS101",
            to_institution_id="EPFL",
            to_object_type="Person",
            to_object_id="alice",
            context="teaches",
        )
        exists_key = str(sorted(key.model_dump(mode="python").items()))
        db = FakeDB(counts={exists_key: 1})
        repo = MySQLEdgeRepository(db=db, registry_db=FakeRegistryDB(), glbcfg=_fake_config())

        edge = repo.get_by_key(key)

        self.assertIsNotNone(edge)
        assert edge is not None
        self.assertEqual(len(edge.field_list.field_list), 1)
        self.assertEqual(edge.field_list.field_list[0].key.field_name, "weight")


if __name__ == "__main__":
    unittest.main()
