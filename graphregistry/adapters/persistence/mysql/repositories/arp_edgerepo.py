# graphregistry/adapters/persistence/mysql/repositories/arp_edgerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import EdgeKey, Edge, EdgeList
from graphregistry.domain.types import ActionSet
from graphregistry.domain.repositories.rpo_edge import EdgeRepository
from graphregistry.application.services.srv_schema import SchemaResolver
from graphregistry.adapters.persistence.mysql.mappers.amp_edge import MySQLEdgeMapper
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.common.logger import GraphLogger

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLEdgeRepository(EdgeRepository):

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, db: GraphDB, schema_resolver: SchemaResolver) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.msg = GraphLogger()

    # Method: Get list of existing nodes given an object type and id string pattern
    def list(self, object_type: tuple[str, str], id_pattern: str | None) -> list[tuple[str, str, str, str, str, str, str]]:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)

        # Get from and to object types
        from_object_type, to_object_type = object_type

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path        = sql_queries_paths['registry']['commit']['edge_list'],
            registry         = schema_name,
            from_object_type = from_object_type,
            to_object_type   = to_object_type,
            id_pattern       = id_pattern.replace('*', '%') if id_pattern is not None else "%"
        )

        # Execute SQL query
        edge_list = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Return edge list
        return cast(list[tuple[str, str, str, str, str, str, str]], edge_list)

    # Method: Check if an edge exists in persistence based on the edge key
    def exists(self, key: EdgeKey) -> bool:

        # Get schema name for object-to-object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_edge(key)

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
        edge_exists = bool(self.db.execute_query(engine_name=engine_name, query=sql_query)[0][0])

        # Return True if count is greater than 0, indicating that the edge exists, otherwise return False
        return edge_exists

    # Method: Check if multiple edges exist in persistence from a list of edge keys
    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        return [self.exists(key) for key in key_list]

    # Method: Fetch edge data and construct Edge object
    def get(self, key: EdgeKey) -> Edge | None:

        # Check if edge exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name for object-to-object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_edge(key)

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
        custom_fields = cast(list[tuple[str, str, Any]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Construct edge object from fetched data
        edge = MySQLEdgeMapper.from_parts(key=key, custom_field_rows=custom_fields)

        # Return edge object
        return edge

    # Method: Fetch multiple edges data and construct EdgeList object from a list of edge keys
    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        out = [edge for edge in (self.get(key) for key in key_list) if edge is not None]
        return EdgeList(item_list=out)

    # Method: Save (insert or update) edge data to persistence
    def save(self, edge: Edge, actions: ActionSet = ('commit',)) -> Edge:

        # Get schema name for object-to-object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_edge(edge.key)

        #-------------------#
        # Upsert edge shell #
        #-------------------#

        # Resolve placeholders in template query and execute upsert query for the edge shell (basic fields)
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Edges_N_Object_N_Object_T_ChildToParent",
            key_column_names  = [
                "from_institution_id",
                "from_object_type",
                "from_object_id",
                "to_institution_id",
                "to_object_type",
                "to_object_id",
                "context",
            ],
            key_column_values = [
                edge.key.from_institution_id,
                edge.key.from_object_type,
                edge.key.from_object_id,
                edge.key.to_institution_id,
                edge.key.to_object_type,
                edge.key.to_object_id,
                edge.key.context,
            ],
            upd_column_names  = [],
            upd_column_values = [],
            actions           = actions,
        )

        #----------------------#
        # Upsert custom fields #
        #----------------------#

        # Remove any previously persisted custom fields so that the edge's field_list
        # becomes the authoritative set. Without this, fields deleted from the domain
        # object would remain in the database.
        # if 'commit' in actions:
        #     sql_query = resolve_sql_query(
        #         file_path           = sql_queries_paths['registry']['commit']['edge_delete_custom'],
        #         registry            = schema_name,
        #         from_institution_id = edge.key.from_institution_id,
        #         from_object_type    = edge.key.from_object_type,
        #         from_object_id      = edge.key.from_object_id,
        #         to_institution_id   = edge.key.to_institution_id,
        #         to_object_type      = edge.key.to_object_type,
        #         to_object_id        = edge.key.to_object_id,
        #         context             = edge.key.context
        #     )
        #     self.db.execute_query(engine_name=engine_name, query=sql_query, commit=True)

        # Convert Edge object to a list of dicts representing the custom fields rows, then upsert each row
        for row in MySQLEdgeMapper.to_custom_field_rows(edge):
            self.db.execute_upsert_row(
                engine_name       = engine_name,
                schema_name       = schema_name,
                table_name        = "Data_N_Object_N_Object_T_CustomFields",
                key_column_names  = [
                    "from_institution_id",
                    "from_object_type",
                    "from_object_id",
                    "to_institution_id",
                    "to_object_type",
                    "to_object_id",
                    "context",
                    "field_language",
                    "field_name",
                ],
                key_column_values = [
                    row["from_institution_id"],
                    row["from_object_type"],
                    row["from_object_id"],
                    row["to_institution_id"],
                    row["to_object_type"],
                    row["to_object_id"],
                    row["context"],
                    row["field_language"],
                    row["field_name"],
                ],
                upd_column_names  = ["field_value"],
                upd_column_values = [row["field_value"]],
                actions           = actions,
            )

        # Print status message
        self.msg.saved(edge.key)

        # Return edge for chaining
        return edge

    # Method: Save (insert or update) multiple edges data to persistence from an EdgeList object
    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ('commit',)) -> EdgeList:
        if isinstance(edge_list, EdgeList):
            edge_list = edge_list.item_list
        return EdgeList(item_list=[self.save(edge, actions=actions) for edge in edge_list])

    # Method: Delete edge data from persistence based on the edge key
    def delete(self, key: EdgeKey, actions: ActionSet = ('commit',)) -> bool | None:

        # Check if edge exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name for object-to-object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_edge(key)

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
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose='print' in actions)

            # Print status message
            self.msg.deleted(key)

            # Return True if edge existed and was deleted
            return True

        # Return False if edge exists but was not deleted
        return False

    # Method: Delete multiple edges data from persistence based on a list of edge keys
    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        if isinstance(key_list, EdgeKeyList):
            key_list = key_list.item_list
        return [self.delete(key, actions=actions) for key in key_list]
