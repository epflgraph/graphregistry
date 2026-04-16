# graphregistry/adapters/persistence/mysql/repositories/arp_edgerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
from graphregistry.common.config import GlobalConfig
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.domain.models.mdl_edge import EdgeKey, EdgeFieldKey, EdgeField, EdgeFieldList, Edge, EdgeList
from graphregistry.domain.interfaces.types import ActionSet
from graphregistry.common.logger import GraphLogger
from graphdb.models.sqlquery import print_sql
import rich

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
        self.msg = GraphLogger()

    # Method: Check if an edge exists in persistence based on the edge key
    def exists(self, key: EdgeKey, schema_override: str | None = None) -> bool:

        # Get schema name from object-to-object type
        # (use override if provided, otherwise look up from global config)
        schema_name = (
            schema_override if schema_override is not None
            else self.glbcfg.object2object_type_to_schema[tuple(sorted([key.from_object_type, key.to_object_type]))]  # type: ignore[index]
        )

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path           = sql_queries_paths['registry']['commit']['edge_exists'],
            registry            = schema_name,
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
    def exists_many(self, key_list: list[EdgeKey], schema_override: str | None = None) -> list[bool]:
        return [self.exists(key, schema_override=schema_override) for key in key_list]

    # Method: Fetch edge data and construct Edge object
    def get(self, key: EdgeKey, schema_override: str | None = None) -> Edge | None:

        # Check if edge exists first (return None if not found)
        if not self.exists(key, schema_override=schema_override):
            self.msg.not_found(key)
            return None

        # Get schema name from object-to-object type
        # (use override if provided, otherwise look up from global config)
        schema_name = (
            schema_override if schema_override is not None
            else self.glbcfg.object2object_type_to_schema[tuple(sorted([key.from_object_type, key.to_object_type]))]  # type: ignore[index]
        )

        #--------------------------#
        # Get edge's custom fields #
        #--------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path           = sql_queries_paths['registry']['commit']['edge_get_custom'],
            registry            = schema_name,
            from_institution_id = key.from_institution_id,
            from_object_type    = key.from_object_type,
            from_object_id      = key.from_object_id,
            to_institution_id   = key.to_institution_id,
            to_object_type      = key.to_object_type,
            to_object_id        = key.to_object_id,
            context             = key.context
        )

        # Execute query and fetch result
        custom_fields = cast(
            list[tuple[str, str, Any]],
            self.db.execute_query(engine_name=self.engine_name, query=sql_query)
        )

        # If no custom fields are found, return an Edge object with an empty field list
        if len(custom_fields) == 0:
            return Edge(key=key)

        # Construct EdgeField objects from the query results and assemble the Edge object
        field_list = []
        for field_language, field_name, field_value in custom_fields:
            field_key = EdgeFieldKey(
                key=key,
                field_language=str(field_language),
                field_name=str(field_name),
            )
            field_list.append(EdgeField(key=field_key, field_value=field_value))

        # Return the constructed Edge object with the key and list of custom fields
        return Edge(key=key, field_list=EdgeFieldList(field_list=field_list))

    # Method: Fetch multiple edges data and construct EdgeList object from a list of edge keys
    def get_many(self, key_list: list[EdgeKey], schema_override: str | None = None) -> EdgeList:
        out = [edge for edge in (self.get(key, schema_override=schema_override) for key in key_list) if edge is not None]
        return EdgeList(edge_list=out)

    # Method: Save (insert or update) edge data to persistence
    def save(self, edge: Edge, actions: ActionSet = ("eval",), schema_override: str | None = None) -> Edge:

        # Get schema name from object-to-object type
        # (use override if provided, otherwise look up from global config)
        schema_name = (
            schema_override if schema_override is not None
            else self.glbcfg.object2object_type_to_schema[tuple(sorted([edge.key.from_object_type, edge.key.to_object_type]))] # type: ignore[index]
        )

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
                key_column_names  = ['from_institution_id', 'from_object_type', 'from_object_id', 'to_institution_id', 'to_object_type', 'to_object_id', 'context', 'field_language', 'field_name'],
                key_column_values = [field.key.key.from_institution_id, field.key.key.from_object_type, field.key.key.from_object_id, field.key.key.to_institution_id, field.key.key.to_object_type, field.key.key.to_object_id, field.key.key.context, field.key.field_language, field.key.field_name],
                upd_column_names  = ['field_value'],
                upd_column_values = [field.field_value],
                actions           = actions
            )

        # Print status message
        self.msg.saved(edge.key)

        # Return edge for chaining
        return edge

    # Method: Save (insert or update) multiple edges data to persistence from an EdgeList object
    def save_many(self, edge_list: EdgeList, actions: ActionSet = ("eval",), schema_override: str | None = None) -> list[Edge]:
        return [self.save(edge, actions=actions, schema_override=schema_override) for edge in edge_list.edge_list]

    # Method: Delete edge data from persistence based on the edge key
    def delete(self, key: EdgeKey, actions: ActionSet = ("eval",), schema_override: str | None = None) -> bool | None:

        # Check if edge exists first (return None if not found)
        if not self.exists(key, schema_override=schema_override):
            self.msg.not_found(key)
            return None

        # Get schema name from object-to-object type
        # (use override if provided, otherwise look up from global config)
        schema_name = (
            schema_override if schema_override is not None
            else self.glbcfg.object2object_type_to_schema[tuple(sorted([key.from_object_type, key.to_object_type]))]  # type: ignore[index]
        )

        # Execute in commit mode
        if 'commit' in actions:

            # Resolve placeholdes in template query
            sql_query = resolve_sql_query(
                file_path           = sql_queries_paths['registry']['commit']['edge_delete'],
                registry            = schema_name,
                from_institution_id = key.from_institution_id,
                from_object_type    = key.from_object_type,
                from_object_id      = key.from_object_id,
                to_institution_id   = key.to_institution_id,
                to_object_type      = key.to_object_type,
                to_object_id        = key.to_object_id,
                context             = key.context
            )

            # Execute commit query
            self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query, verbose='print' in actions)

            # Print status message
            self.msg.deleted(key)

            # Return True if edge existed and was deleted
            return True

        # Return False if edge exists but was not deleted
        return False

    # Method: Delete multiple edges data from persistence based on a list of edge keys
    def delete_many(self, key_list: list[EdgeKey], actions: ActionSet = ("eval",), schema_override: str | None = None) -> list[bool | None]:
        return [self.delete(key, actions=actions, schema_override=schema_override) for key in key_list]
