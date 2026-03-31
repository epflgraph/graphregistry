# graphregistry/adapters/mysql/adp_edgerepo.py
from __future__ import annotations
from typing import Any, TYPE_CHECKING
from graphregistry.common.config import GlobalConfig
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.common.auxfcn import sysmsg
from graphregistry.domain.models.mdl_edge import EdgeKey, EdgeFieldKey, EdgeField, EdgeFieldList, Edge, EdgeList
from graphdb.models.sqlquery import print_sql

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLEdgeRepository:

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, engine_name: str, db: GraphDB, glbcfg: GlobalConfig | None = None) -> None:
        self.engine_name = engine_name
        self.db = db
        self.glbcfg = glbcfg or GlobalConfig()

    # Method: Check if an edge exists in persistence based on the edge key
    def exists(self, key: EdgeKey) -> bool:

        # Get schema name from object-to-object type
        schema_name = self.glbcfg.object2object_type_to_schema[tuple(sorted([key.from_object_type, key.to_object_type]))]  # type: ignore[index]

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['edge_exists'],
            registry       = schema_name,
            from_institution_id = key.from_institution_id,
            from_object_type    = key.from_object_type,
            from_object_id      = key.from_object_id,
            to_institution_id   = key.to_institution_id,
            to_object_type      = key.to_object_type,
            to_object_id        = key.to_object_id,
            context             = key.context
        )

        # Execute commit query
        edge_exists = bool(self.db.execute_query(engine_name=self.engine_name, query=sql_query)[0][0])

        # Return True if count is greater than 0, indicating that the edge exists, otherwise return False
        return edge_exists

    # Method: Check if multiple edges exist in persistence from a list of edge keys
    def exists_many(self, key_list: list[EdgeKey]) -> list[bool]:
        return [self.exists(key) for key in key_list]

    # Method: Fetch edge data and construct Edge object
    def get(self, key: EdgeKey) -> Edge | None:

        # First check if the edge exists, if not return None
        if not self.exists(key):
            print(f"❌ Edge ~ ({key.from_institution_id}, {key.from_object_type}, {key.from_object_id}, {key.to_institution_id}, {key.to_object_type}, {key.to_object_id}, {key.context}) not found.")
            return None

        # Get schema name from object-to-object type
        schema_name = self.glbcfg.object2object_type_to_schema[tuple(sorted([key.from_object_type, key.to_object_type]))]  # type: ignore[index]

        # Fetch custom fields for the edge
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

        # If no custom fields are found, return an Edge object with an empty field list
        if not isinstance(rows, list) or len(rows) == 0:
            return Edge(key=key)

        # Construct EdgeField objects from the query results and assemble the Edge object
        field_list = []
        for field_language, field_name, field_value in rows:
            field_key = EdgeFieldKey(key=key, field_language=field_language, field_name=field_name)
            field_list.append(EdgeField(key=field_key, field_value=field_value))

        # Return the constructed Edge object with the key and list of custom fields
        return Edge(key=key, field_list=EdgeFieldList(field_list=field_list))

    # Method: Fetch multiple edges data and construct EdgeList object from a list of edge keys
    def get_many(self, key_list: list[EdgeKey]) -> EdgeList:
        out = [edge for edge in (self.get_by_key(key) for key in key_list) if edge is not None]
        return EdgeList(edge_list=out)

    # Method: Save (insert or update) edge data to persistence
    def save(self, edge: Edge, actions: tuple[str, ...] = ("eval",)) -> Any:

        # Get schema name from object-to-object type
        schema_name = self.glbcfg.object2object_type_to_schema[tuple(sorted([edge.key.from_object_type, edge.key.to_object_type]))]  # type: ignore[index]

        # Upsert basic edge data (without custom fields)
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_name,
            table_name        = 'Edges_N_Object_N_Object_T_ChildToParent',
            key_column_names  = ['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id', 'to_object_type', 'to_object_id', 'context'],
            key_column_values = [edge.key.from_institution_id, edge.key.from_object_type, edge.key.from_object_id, edge.key.to_institution_id, edge.key.to_object_type, edge.key.to_object_id, edge.key.context],
            upd_column_names  = [],
            upd_column_values = [],
            actions           = actions
        )

        # Upsert custom fields for the edge
        for field in edge.field_list.field_list:
            self.db.execute_upsert_row(
                engine_name       = self.engine_name,
                schema_name       = schema_name,
                table_name        = 'Data_N_Object_N_Object_T_CustomFields',
                key_column_names  = ['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id', 'to_object_type', 'to_object_id', 'field_language', 'field_name', 'context'],
                key_column_values = [edge.key.from_institution_id, edge.key.from_object_type, edge.key.from_object_id, edge.key.to_institution_id, edge.key.to_object_type, edge.key.to_object_id, field.key.field_language, field.key.field_name, edge.key.context],
                upd_column_names  = ['field_value'],
                upd_column_values = [field.field_value],
                actions           = actions
            )

    # Method: Save (insert or update) multiple edges data to persistence from an EdgeList object
    def save_many(self, edge_list: EdgeList, actions: tuple[str, ...] = ("eval",)) -> list[Any]:
        return [self.save(edge, actions=actions) for edge in edge_list.edge_list]

    # Method: Delete edge data from persistence based on the edge key
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

    # Method: Delete multiple edges data from persistence based on a list of edge keys
    def delete_many(self, key_list: list[EdgeKey], actions: tuple[str, ...] = ("eval",)) -> list[bool]:
        return [self.delete(key, actions=actions) for key in key_list]
