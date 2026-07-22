# graphregistry/adapters/persistence/mysql/repositories/arp_noderepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, get_args
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import NodeKey, Node, NodeList
from graphregistry.domain.types import ActionSet
from graphregistry.domain.repositories.rpo_node import NodeRepository
from graphregistry.application.services.srv_schema import SchemaResolver
from graphregistry.adapters.persistence.mysql.mappers.amp_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.schemas.asc_pageprofile import PAGE_PROFILE_COLUMNS
from graphregistry.common.dbstruct import sql_queries_paths, resolve_sql_query
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.types import ObjectType
from graphregistry.domain.models.entities.types import ConceptMapType
import rich

# If TYPE_CHECKING is True, these imports are only for type checking and will not be executed at runtime
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Class definition
class MySQLNodeRepository(NodeRepository):

    # Method: Initialize the repository with database connection and global configuration
    def __init__(self, db: GraphDB, schema_resolver: SchemaResolver) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.msg = GraphLogger()

    #----------------------------------------#
    # Basic Node CRUD/persistence operations #
    #----------------------------------------#

    # Method: Get list of existing nodes given an object type and id string pattern
    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str]]:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['node_list'],
            registry    = schema_name,
            object_type = object_type,
            id_pattern  = id_pattern.replace('*', '%') if id_pattern is not None else "%"
        )

        # Execute SQL query
        node_list = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Return node list
        return cast(list[tuple[str, str]], node_list)

    # Method: Check if a node exists in persistence from the node key
    def exists(self, key: NodeKey) -> bool:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_exists'],
            registry       = schema_name,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute commit query
        node_exists = bool(self.db.execute_query(engine_name=engine_name, query=sql_query)[0][0])

        # Return True if count is greater than 0, indicating that the node exists, otherwise return False
        return node_exists

    # Method: Check if multiple nodes exist in persistence from a list of node keys
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        if isinstance(key_list, NodeKeyList):
            return [self.exists(key) for key in key_list.item_list]
        else:
            return [self.exists(key) for key in key_list]

    # Method: Fetch node data and construct Node object
    def get(self, key: NodeKey) -> Node | None:

        # Check if node exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        #-------------------------#
        # Get node's basic fields #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_basic'],
            registry       = schema_name,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        basic_data = cast(list[tuple[Any, ...]], self.db.execute_query(engine_name=engine_name, query=sql_query))
        basic_row = basic_data[0] if len(basic_data) > 0 else None

        # Any rows returned?
        if basic_row is None:
            self.msg.not_found(key)
            return None

        #--------------------------#
        # Get node's custom fields #
        #--------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_custom'],
            registry       = schema_name,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        custom_fields = cast(list[tuple[str, str, Any]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        #-------------------------#
        # Get node's page profile #
        #-------------------------#

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_get_profile'],
            registry       = schema_name,
            object_type    = key.object_type,
            object_id      = key.object_id
        )

        # Execute query and fetch result
        page_profile = self.db.execute_query(engine_name=engine_name, query=sql_query)

        # Any rows returned?
        if len(page_profile) > 0:
            page_profile_dict = dict(zip(PAGE_PROFILE_COLUMNS, page_profile[0]))
        else:
            page_profile_dict = {}

        #------------------------------#
        # Get node's detected concepts #
        #------------------------------#

        # Init concept map structure
        # TODO: replace this with the ConceptMapType enum once it's implemented in the domain models
        concepts: dict[ConceptMapType, list[tuple[str, float]]] = {
            'detected'       : [],
            'ai_validated'   : [],
            'manually_mapped': []
        }

        # Loop over concept mapping types
        for map_type in get_args(ConceptMapType):

            # TODO: Skip detected concepts update [TEMPORARY]
            if map_type == 'detected':
                continue

            # Resolve placeholdes in template query
            sql_query = resolve_sql_query(
                file_path      = sql_queries_paths['registry']['commit'][f'node_get_concepts_{map_type}'],
                registry       = schema_name,
                object_type    = key.object_type,
                object_id      = key.object_id
            )

            # Execute query and fetch result
            concepts[map_type] = cast(list[tuple[str, float]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Construct Node object from fetched data
        node = MySQLNodeMapper.from_parts(
            key               = key,
            basic_row         = basic_row,
            custom_field_rows = custom_fields,
            page_profile_row  = page_profile_dict,
            detected_concept_rows     = concepts['detected'],
            ai_validated_concept_rows = concepts['ai_validated'],
            manually_mapped_rows      = concepts['manually_mapped']
        )

        # Return the constructed Node object
        return node

    # Method: Fetch multiple nodes data and construct NodeList object from a list of node keys
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        if isinstance(key_list, NodeKeyList):
            key_list = key_list.item_list
        out = [node for node in (self.get(key) for key in key_list) if node is not None]
        return NodeList(item_list=out)

    # Method: Save (insert or update) node data to persistence
    def save(self, node: Node, actions: ActionSet = ('commit',)) -> Node:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(node.key)

        #---------------------#
        # Upsert basic fields #
        #---------------------#

        # Convert Node object to a dict representing the basic fields row
        basic_row = MySQLNodeMapper.to_basic_row(node)

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Nodes_N_Object",
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [node.key.object_type, node.key.object_id],
            upd_column_names  = list(basic_row.keys()),
            upd_column_values = list(basic_row.values()),
            actions           = actions,
        )

        #----------------------#
        # Upsert custom fields #
        #----------------------#

        # Delete any previously persisted custom fields so that the node's field_list becomes the authoritative set.
        # Without this, fields deleted from the domain object would remain in the database.
        sql_query = resolve_sql_query(
            file_path      = sql_queries_paths['registry']['commit']['node_delete_custom'],
            registry       = schema_name,
            object_type    = node.key.object_type,
            object_id      = node.key.object_id
        )
        # !TEMPORARY: Commenting out the deletion of custom fields to avoid accidental data loss during development. Uncomment in production.
        # self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query)

        # Convert Node object to a list of dicts representing the custom fields rows, then upsert each row
        for row in MySQLNodeMapper.to_custom_field_rows(node):
            self.db.execute_upsert_row(
                engine_name       = engine_name,
                schema_name       = schema_name,
                table_name        = "Data_N_Object_T_CustomFields",
                key_column_names  = ["object_type", "object_id", "field_language", "field_name"],
                key_column_values = [
                    row["object_type"],
                    row["object_id"],
                    row["field_language"],
                    row["field_name"],
                ],
                upd_column_names  = ["field_value"],
                upd_column_values = [row["field_value"]],
                actions           = actions,
            )

        #---------------------#
        # Upsert page profile #
        #---------------------#

        # Convert Node object to a dict representing the page profile row
        page_profile_row = MySQLNodeMapper.to_page_profile_row(node)

        # Resolve placeholders in template query
        self.db.execute_upsert_row(
            engine_name       = engine_name,
            schema_name       = schema_name,
            table_name        = "Data_N_Object_T_PageProfile",
            key_column_names  = ["object_type", "object_id"],
            key_column_values = [node.key.object_type, node.key.object_id],
            upd_column_names  = list(page_profile_row.keys()),
            upd_column_values = list(page_profile_row.values()),
            actions           = actions,
        )

        #--------------------------#
        # Upsert detected concepts #
        #--------------------------#

        # Loop over concept mapping types
        for map_type, table_name in zip(
            get_args(ConceptMapType),
            [
                "Edges_N_Object_N_Concept_T_ConceptDetection",
                "Edges_N_Object_N_Concept_T_LLMPostValidated",
                "Edges_N_Object_N_Concept_T_ManualMapping",
            ],
            strict=True,
        ):
            # Convert Node object to a list of dicts representing the custom fields rows, then upsert each row
            for row in MySQLNodeMapper.to_scored_concepts_rows(node, map_to=map_type):

                # TODO: Skip detected concepts update [TEMPORARY]
                if map_type == 'detected':
                    continue

                # Resolve placeholders in template query
                self.db.execute_upsert_row(
                    engine_name       = engine_name,
                    schema_name       = schema_name,
                    table_name        = table_name,
                    key_column_names  = ["object_type", "object_id", "concept_id", "text_source"],
                    key_column_values = [
                        row["object_type"],
                        row["object_id"],
                        row["concept_id"],
                        row["text_source"]
                    ],
                    upd_column_names  = ["score"],
                    upd_column_values = [row["score"]],
                    actions           = actions,
                )

        #---------------------#

        # Print status message
        self.msg.saved(node.key)

        # Return node for chaining
        return node

    # Method: Save (insert or update) multiple nodes data to persistence from a NodeList object
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ('commit',)) -> NodeList:
        if isinstance(node_list, NodeList):
            node_list = node_list.item_list
        return NodeList(item_list=[self.save(node, actions=actions) for node in node_list])

    # Method: Delete node data from persistence based on the node key
    def delete(self, key: NodeKey, actions: ActionSet = ('commit',)) -> bool | None:

        # Check if node exists first (return None if not found)
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        # Execute in commit mode
        if 'commit' in actions:

            # Resolve placeholdes in template query
            sql_query = resolve_sql_query(
                file_path      = sql_queries_paths['registry']['commit']['node_delete'],
                registry       = schema_name,
                object_type    = key.object_type,
                object_id      = key.object_id
            )

            # Execute commit query
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query, verbose='print' in actions)

            # Print status message
            self.msg.deleted(key)

            # Return True if node existed and was deleted
            return True

        # Return False if node exists but was not deleted
        return False

    # Method: Delete multiple nodes data from persistence based on a list of node keys
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ('commit',)) -> list[bool | None]:
        if isinstance(key_list, NodeKeyList):
            return [self.delete(key, actions=actions) for key in key_list.item_list]
        else:
            return [self.delete(key, actions=actions) for key in key_list]

    #--------------------------------------------------#
    # Node diagnostics and special get/save operations #
    #--------------------------------------------------#

    # Method: Get nodes with no detected concepts based on optional object type and id pattern filters, returning a NodeList of the matching nodes
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:

        # Get schema name from object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type if object_type is not None else "Course")

        # Get airflow schema name from object type using the schema resolver
        _, airflow_schema_name = self.schema_resolver.for_airflow()

        # Resolve placeholdes in template query
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths['registry']['commit']['node_get_with_no_concepts'],
            registry    = schema_name,
            airflow     = airflow_schema_name,
            object_type = object_type if object_type is not None else "%",
            id_pattern  = id_pattern.replace('*', '%') if id_pattern is not None else "%"
        )

        # Execute SQL query and fetch result
        node_keys_data = cast(list[tuple[str, str]], self.db.execute_query(engine_name=engine_name, query=sql_query))

        # Construct NodeKey objects from fetched data
        node_keys = [
            NodeKey(
                object_type    = cast(ObjectType, row[0]),
                object_id      = row[1]
            ) for row in node_keys_data
        ]

        # Fetch full Node objects for the NodeKeys and return as a NodeList
        return self.get_many(node_keys)
