# graphregistry/adapters/mysql/adp_noderepo.py
from __future__ import annotations
from typing import TYPE_CHECKING
from graphregistry.common.config import GlobalConfig
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.domain.models.mdl_node import NodeKey, NodeFieldList, Node, NodeList
from graphregistry.domain.models.mdl_pageprofile import PageProfile
from graphregistry.domain.interfaces.types import ActionSet
from graphregistry.common.logger import GraphLogger
from graphdb.models.sqlquery import print_sql
import rich

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLNodeRepository:

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, engine_name: str, db: GraphDB, glbcfg: GlobalConfig | None = None) -> None:
        self.engine_name = engine_name
        self.db = db
        self.glbcfg = glbcfg or GlobalConfig()
        self.msg = GraphLogger()

    # Method: Check if a node exists in persistence from the node key
    def exists(self, key: NodeKey) -> bool:

        # Get schema name from object type
        schema_name = self.glbcfg.object_type_to_schema[key.object_type]  # type: ignore[index]

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_exists'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute commit query
        node_exists = bool(self.db.execute_query(engine_name=self.engine_name, query=sql_query)[0][0])

        # Return True if count is greater than 0, indicating that the node exists, otherwise return False
        return node_exists

    # Method: Check if multiple nodes exist in persistence from a list of node keys
    def exists_many(self, key_list: list[NodeKey]) -> list[bool]:
        return [self.exists(key) for key in key_list]

    # Method: Fetch node data and construct Node object
    def get(self, key: NodeKey) -> Node | None:

        # Check if node exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type
        schema_name = self.glbcfg.object_type_to_schema[key.object_type]

        #-------------------------#
        # Get node's basic fields #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_basic'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        basic_data = self.db.execute_query(engine_name=self.engine_name, query=sql_query)

        # Any rows returned?
        if len(basic_data) > 0:
            # Get fields from query output
            object_title, text_source, raw_text = basic_data[0]
        else:
            # -> raise error
            return None

        #--------------------------#
        # Get node's custom fields #
        #--------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_custom'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        custom_fields = self.db.execute_query(engine_name=self.engine_name, query=sql_query)

        # Build custom fields list of dics from query output
        custom_fields_dict = [
            {
                "field_language" : field_language,
                "field_name"     : field_name,
                "field_value"    : field_value
            }
            for field_language, field_name, field_value in custom_fields]

        #-------------------------#
        # Get node's page profile #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_profile'],
            registry       = schema_name,
            institution_id = key.institution_id,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        page_profile = self.db.execute_query(engine_name=self.engine_name, query=sql_query)

        # Any rows returned?
        if len(page_profile) > 0:
            page_profile_dict = dict(zip(self.glbcfg.page_profile_columns, page_profile[0]))
        else:
            # -> raise warning
            page_profile_dict = {}

        # Construct Node object from fetched data
        node = Node(
            key          = key,
            title        = object_title,
            text_source  = text_source,
            raw_text     = raw_text,
            field_list   = NodeFieldList.from_json(data=custom_fields_dict, key=key),
            page_profile = PageProfile.from_json(data=page_profile_dict, key=key),
        )

        # Return the constructed Node object
        return node

    # Method: Fetch multiple nodes data and construct NodeList object from a list of node keys
    def get_many(self, key_list: list[NodeKey]) -> NodeList:
        out = [node for node in (self.get(key) for key in key_list) if node is not None]
        return NodeList(node_list=out)

    # Method: Save (insert or update) node data to persistence
    def save(self, node: Node, actions: ActionSet = ("eval",)) -> Node:

        # Get schema name from object type
        schema_name = self.glbcfg.object_type_to_schema[node.key.object_type]  # type: ignore[index]

        # Upsert basic node data (without custom fields)
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_name,
            table_name        = 'Nodes_N_Object',
            key_column_names  = ['institution_id', 'object_type', 'object_id'],
            key_column_values = [node.key.institution_id, node.key.object_type, node.key.object_id],
            upd_column_names  = ['object_title', 'text_source', 'raw_text'],
            upd_column_values = [node.title, node.text_source, node.raw_text],
            actions           = actions
        )

        # Upsert custom fields for the node
        for field in node.field_list.field_list:
            self.db.execute_upsert_row(
                engine_name       = self.engine_name,
                schema_name       = schema_name,
                table_name        = 'Data_N_Object_T_CustomFields',
                key_column_names  = ['institution_id', 'object_type', 'object_id', 'field_language', 'field_name'],
                key_column_values = [field.key.key.institution_id, field.key.key.object_type, field.key.key.object_id, field.key.field_language, field.key.field_name],
                upd_column_names  = ['field_value'],
                upd_column_values = [field.field_value],
                actions           = actions
            )

        # Build simplified page profile dict for the node (only include fields that are present in the page profile JSON)
        page_profile_json = node.page_profile.to_simplified_dict()
        page_profile_json_keys, page_profile_json_values = zip(*page_profile_json.items()) if page_profile_json else ([], [])

        # Upsert page profile data for the node
        self.db.execute_upsert_row(
            engine_name       = self.engine_name,
            schema_name       = schema_name,
            table_name        = 'Data_N_Object_T_PageProfile',
            key_column_names  = ['institution_id', 'object_type', 'object_id'],
            key_column_values = [node.key.institution_id, node.key.object_type, node.key.object_id],
            upd_column_names  = page_profile_json_keys,
            upd_column_values = page_profile_json_values,
            actions           = actions
        )

        # Print status message
        self.msg.saved(node.key)

        # Return node for chaining
        return node

    # Method: Save (insert or update) multiple nodes data to persistence from a NodeList object
    def save_many(self, node_list: NodeList, actions: ActionSet = ("eval",)) -> list[bool]:
        return [self.save(node, actions=actions) for node in node_list.node_list]

    # Method: Delete node data from persistence based on the node key
    def delete(self, key: NodeKey, actions: ActionSet = ("eval",)) -> bool | None:

        # Check if node exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type
        schema_name = self.glbcfg.object_type_to_schema[key.object_type]

        # Execute in commit mode
        if 'commit' in actions:

            # Resolve placeholdes in template query
            sql_query = resolve_sql_query(
                file_path      = sql_queries_paths['registry']['commit']['node_delete'],
                registry       = schema_name,
                institution_id = key.institution_id,
                object_type    = key.object_type,
                object_id      = key.object_id
            )

            # Execute commit query
            self.db.execute_query_in_shell(engine_name=self.engine_name, query=sql_query, verbose='print' in actions)

            # Print status message
            self.msg.deleted(key)

            # Return True if node existed and was deleted
            return True

        # Return False if node exists but was not deleted
        return False

    # Method: Delete multiple nodes data from persistence based on a list of node keys
    def delete_many(self, key_list: list[NodeKey], actions: ActionSet = ("eval",)) -> list[bool | None]:
        return [self.delete(key, actions=actions) for key in key_list]
