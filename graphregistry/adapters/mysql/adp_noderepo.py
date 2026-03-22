from __future__ import annotations

from typing import Any

from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.mdl_node import Node, NodeField, NodeFieldKey, NodeKey, NodeList


class MySQLNodeRepository:
    def __init__(self, db=None, registry_db=None, glbcfg: GlobalConfig | None = None, engine_name: str = "xaas_coresrv") -> None:
        if db is None or registry_db is None:
            from graphregistry.core.dbbridge import RegistryDB, db as default_db

            db = db or default_db
            registry_db = registry_db or RegistryDB()

        self.db = db
        self.registry_db = registry_db
        self.glbcfg = glbcfg or GlobalConfig()
        self.engine_name = engine_name

    def _get_schema(self, object_type: str) -> str:
        return self.glbcfg.object_type_to_schema.get(object_type, self.glbcfg.schema_registry)

    def exists(self, key: NodeKey) -> bool:
        schema = self._get_schema(key.object_type)
        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT COUNT(*)
                FROM {schema}.Nodes_N_Object
                WHERE (institution_id, object_type, object_id)
                    = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        return bool(isinstance(out, list) and len(out) > 0 and out[0][0] > 0)

    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        return [self.exists(key) for key in key_list]

    def get_by_key(self, key: NodeKey) -> Node | None:
        if not self.exists(key):
            return None

        schema = self._get_schema(key.object_type)
        rows = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT field_language, field_name, field_value
                FROM {schema}.Data_N_Object_T_CustomFields
                WHERE (institution_id, object_type, object_id)
                    = (:institution_id, :object_type, :object_id);
            """,
            params=key.model_dump(mode="python"),
        )
        field_list = []
        for field_language, field_name, field_value in rows:
            field_key = NodeFieldKey(key=key, field_language=field_language, field_name=field_name)
            field_list.append(NodeField(key=field_key, field_value=field_value))
        return Node(key=key, field_list={"field_list": field_list})

    def get_by_keys(self, key_list: list[NodeKey]) -> NodeList:
        out = [node for node in (self.get_by_key(key) for key in key_list) if node is not None]
        return NodeList(node_list=out)

    def save(self, node: Node, actions: tuple[str, ...] = ("eval",)) -> Any:
        print('HERE')
        return
        key = node.key
        schema = self._get_schema(key.object_type)
        eval_results = {
            "node_object": self.registry_db.registry_insert(
                schema_name=schema,
                table_name="Nodes_N_Object",
                key_column_names=["institution_id", "object_type", "object_id"],
                key_column_values=[key.institution_id, key.object_type, key.object_id],
                upd_column_names=[],
                upd_column_values=[],
                actions=actions,
                engine_name=self.engine_name,
            ),
            "custom_fields": [],
        }
        for field in node.field_list.field_list:
            eval_results["custom_fields"].append(
                self.registry_db.registry_insert(
                    schema_name=schema,
                    table_name="Data_N_Object_T_CustomFields",
                    key_column_names=[
                        "institution_id",
                        "object_type",
                        "object_id",
                        "field_language",
                        "field_name",
                    ],
                    key_column_values=[
                        key.institution_id,
                        key.object_type,
                        key.object_id,
                        field.key.field_language,
                        field.key.field_name,
                    ],
                    upd_column_names=["field_value"],
                    upd_column_values=[field.field_value],
                    actions=actions,
                    engine_name=self.engine_name,
                )
            )
        return eval_results

    def save_many(self, node_list: NodeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return [self.save(node, actions=actions) for node in node_list.node_list]

    def delete(self, key: NodeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        if not self.exists(key):
            return False

        if "commit" in actions:
            self.registry_db.delete_nodes_by_ids(
                institution_id=key.institution_id,
                object_type=key.object_type,
                nodes_id=[key.object_id],
                engine_name=self.engine_name,
                actions=actions,
            )
        return True

    def delete_many(self, key_list: list[NodeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        return [self.delete(key, actions=actions) for key in key_list]
