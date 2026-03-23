from __future__ import annotations
from typing import Any, TYPE_CHECKING
from graphregistry.common.config import GlobalConfig
from graphregistry.domain.models.mdl_edge import Edge, EdgeField, EdgeFieldKey, EdgeKey, EdgeList

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

class MySQLEdgeRepository:

    def __init__(self, engine_name: str, db: GraphDB, glbcfg: GlobalConfig | None = None) -> None:
        self.engine_name = engine_name
        self.db = db
        self.glbcfg = glbcfg or GlobalConfig()

    def _get_schema(self, key: EdgeKey) -> str:
        schema_from = self.glbcfg.object_type_to_schema.get(key.from_object_type, self.glbcfg.schema_registry)
        schema_to = self.glbcfg.object_type_to_schema.get(key.to_object_type, self.glbcfg.schema_registry)
        if schema_from == self.glbcfg.schema_lectures or schema_to == self.glbcfg.schema_lectures:
            return self.glbcfg.schema_lectures
        if schema_from == schema_to:
            return schema_from
        return self.glbcfg.schema_registry

    def exists(self, key: EdgeKey) -> bool:
        schema_name = self.glbcfg.schema_registry
        out = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT COUNT(*) FROM {schema_name}.Edges_N_Object_N_Object_T_ChildToParent
                WHERE (
                    from_institution_id, from_object_type, from_object_id,
                    to_institution_id, to_object_type, to_object_id, context
                ) = (
                    :from_institution_id, :from_object_type, :from_object_id,
                    :to_institution_id, :to_object_type, :to_object_id, :context
                );
            """,
            params=key.model_dump(mode="python"),
        )
        return bool(isinstance(out, list) and len(out) > 0 and out[0][0] > 0)

    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        return [self.exists(key) for key in key_list]

    def get(self, key: EdgeKey) -> Edge | None:
        if not self.exists(key):
            return None

        schema_name = self._get_schema(key)
        rows = self.db.execute_query(
            engine_name=self.engine_name,
            query=f"""
                SELECT field_language, field_name, field_value
                FROM {schema_name}.Data_N_Object_N_Object_T_CustomFields
                WHERE (
                    from_institution_id, from_object_type, from_object_id,
                    to_institution_id, to_object_type, to_object_id, context
                ) = (
                    :from_institution_id, :from_object_type, :from_object_id,
                    :to_institution_id, :to_object_type, :to_object_id, :context
                );
            """,
            params=key.model_dump(mode="python"),
        )
        field_list = []
        for field_language, field_name, field_value in rows:
            field_key = EdgeFieldKey(key=key, field_language=field_language, field_name=field_name)
            field_list.append(EdgeField(key=field_key, field_value=field_value))
        return Edge(key=key, field_list={"field_list": field_list})

    def get_many(self, key_list: list[EdgeKey]) -> EdgeList:
        out = [edge for edge in (self.get_by_key(key) for key in key_list) if edge is not None]
        return EdgeList(edge_list=out)

    # Method: Save (insert or update) edge data to persistence
    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Any:

        # Get schema name based on edge key
        schema_name = self.glbcfg.schema_registry

        # Upsert edge record
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_name,
            table_name        = "Edges_N_Object_N_Object_T_ChildToParent",
            key_column_names  = ["from_institution_id", "from_object_type", "from_object_id", "to_institution_id", "to_object_type", "to_object_id", "context"],
            key_column_values = [edge.key.from_institution_id, edge.key.from_object_type, edge.key.from_object_id, edge.key.to_institution_id, edge.key.to_object_type, edge.key.to_object_id, edge.key.context],
            upd_column_names  = [],
            upd_column_values = [],
            actions           = actions
        )

        # Upsert custom field records
        for field in edge.field_list.field_list:
            self.db.execute_upsert_row(
                engine_name       = self.engine_name,
                schema_name       = schema_name,
                table_name        = "Data_N_Object_N_Object_T_CustomFields",
                key_column_names  = ["from_institution_id", "from_object_type", "from_object_id", "to_institution_id", "to_object_type", "to_object_id", "field_language", "field_name", "context"],
                key_column_values = [edge.key.from_institution_id, edge.key.from_object_type, edge.key.from_object_id, edge.key.to_institution_id, edge.key.to_object_type, edge.key.to_object_id, field.key.field_language, field.key.field_name, edge.key.context],
                upd_column_names  = ["field_value"],
                upd_column_values = [field.field_value],
                actions           = actions
            )

    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return [self.save(edge, actions=actions) for edge in edge_list.edge_list]

    def delete(self, key: EdgeKey, actions: tuple[str, ...] = ("eval",)) -> bool:
        if not self.exists(key):
            return False

        schema_name = self._get_schema(key)
        query_where = """
            (
                from_institution_id = :from_institution_id
                AND from_object_type = :from_object_type
                AND from_object_id = :from_object_id
                AND to_institution_id = :to_institution_id
                AND to_object_type = :to_object_type
                AND to_object_id = :to_object_id
                AND context = :context
            )
        """
        tables = [
            f"{schema_name}.Edges_N_Object_N_Object_T_ChildToParent",
            f"{schema_name}.Data_N_Object_N_Object_T_CustomFields",
        ]

        if "commit" in actions:
            for table in tables:
                self.db.execute_query(
                    engine_name=self.engine_name,
                    query=f"DELETE FROM {table} WHERE {query_where};",
                    params=key.model_dump(mode="python"),
                    commit=True,
                )
        return True

    def delete_many(self, key_list: list[EdgeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        return [self.delete(key, actions=actions) for key in key_list]
