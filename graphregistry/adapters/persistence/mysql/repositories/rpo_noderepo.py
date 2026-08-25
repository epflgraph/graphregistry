# graphregistry/adapters/persistence/mysql/repositories/rpo_noderepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, get_args
from graphregistry.adapters.persistence.mysql.mappers.map_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories._helpers import qualified_table, soft_delete_by_key_tuples, upsert_rows
from graphregistry.adapters.persistence.mysql.repositories.schemas import PAGE_PROFILE_COLUMNS
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_node import NodeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.exceptions import PersistenceError
from graphregistry.domain.models.entities.mdl_base import NodeKeyList
from graphregistry.domain.models.entities.mdl_node import Node, NodeKey, NodeList
from graphregistry.domain.models.entities.types import ConceptMapType
from graphregistry.domain.types import ActionSet

# If TYPE_CHECKING is True, import GraphDB and MySQLUnitOfWork for type hints only.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork

#==================#
# Class Definition #
#==================#
class MySQLNodeRepository(NodeRepository):
    """MySQL adapter for the NodeRepository port.

    The repository is bound to a UnitOfWork when used inside application
    services so that all writes for a business operation share one transaction.

    For backward compatibility it can also be constructed directly with a
    GraphDB client and schema resolver; in that case each public method
    manages its own short-lived session.
    """

    # Default table names for concept edges, keyed by ConceptMapType.
    _CONCEPT_TABLE_NAMES: dict[ConceptMapType, str] = {
        "detected"        : "Edges_N_Object_N_Concept_T_ConceptDetection",
        "ai_validated"    : "Edges_N_Object_N_Concept_T_LLMPostValidated",
        "manually_mapped" : "Edges_N_Object_N_Concept_T_ManualMapping",
    }

    # Class initialization and dependency injection
    def __init__(self, db: "GraphDB | None" = None, schema_resolver: "SchemaResolver | None" = None, *, uow: "MySQLUnitOfWork | None" = None) -> None:

        # Validate that either a UnitOfWork is provided, or both a GraphDB and SchemaResolver are provided, but not both.
        if uow is not None and (db is not None or schema_resolver is not None):
            raise ValueError("Provide either uow= or (db=, schema_resolver=), not both.")

        # If a UnitOfWork is provided, use its db and schema_resolver; otherwise, use the provided db and schema_resolver.
        if uow is not None:
            self._uow = uow
            self.db = uow.db
            self.schema_resolver = uow.schema_resolver

        # If a UnitOfWork is not provided, ensure that both db and schema_resolver are provided; otherwise, raise an error.
        elif db is not None and schema_resolver is not None:
            self._uow = None
            self.db = db
            self.schema_resolver = schema_resolver
        else:
            raise ValueError("MySQLNodeRepository requires either uow= or (db=, schema_resolver=).")

        # Initialize a GraphLogger instance for logging messages.
        self.msg = GraphLogger()

    #================================================================#
    # Function Group: Internal helpers                               #
    #================================================================#

    # Function: Helper to get a session for a given engine name, creating a standalone session if not in a UnitOfWork.
    def _session(self, engine_name: str) -> MySQLSession:
        """Return a session for engine_name, creating a standalone one if needed."""
        if self._uow is not None:
            return self._uow.get_session(engine_name)

        session = MySQLSession(self.db, engine_name)
        session.begin()
        return session

    # Function: Helper to close a standalone session if not in a UnitOfWork.
    def _close_standalone_session(self, session: MySQLSession) -> None:
        """Close a session created outside a UnitOfWork."""
        if self._uow is None:
            session.close()

    # Function: Helper to execute a read query and return the results as a list of tuples.
    def _execute_read(self, engine_name: str, query: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a read query."""
        session = self._session(engine_name)
        try:
            return session.execute(query, params)
        finally:
            self._close_standalone_session(session)

    # Function: Helper to get the qualified table name for a given schema and table
    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return qualified_table(schema_name, table_name)

    # Function: Helper to upsert rows into a table using the provided session
    @staticmethod
    def _upsert_rows(session: MySQLSession, table_path: str, key_column_names: list[str], upd_column_names: list[str], rows: list[dict[str, Any]]) -> None:
        upsert_rows(session, table_path, key_column_names, upd_column_names, rows)

    # Function: Helper to soft-delete rows by keys in a given table using the provided session
    @staticmethod
    def _soft_delete_by_keys(session: MySQLSession, schema_name: str, table_name: str, keys: list[NodeKey]) -> None:
        key_tuples = [(key.object_type, key.object_id) for key in keys]
        soft_delete_by_key_tuples(
            session,
            schema_name,
            table_name,
            ["object_type", "object_id"],
            key_tuples,
        )

    # Function: Helper to generate a SQL predicate for checking if keys are in a list
    @staticmethod
    def _key_in_list_predicate(keys: list[NodeKey], prefix: str = "key") -> tuple[str, dict[str, Any]]:
        key_tuples = [(key.object_type, key.object_id) for key in keys]
        from graphregistry.adapters.persistence.mysql.repositories._helpers import key_tuple_in_list_predicate
        return key_tuple_in_list_predicate(key_tuples, ["object_type", "object_id"], prefix=prefix)

    #================================================================#
    # Method Group: Basic Node CRUD/persistence operations           #
    #================================================================#

    # Method: List nodes of a given object type and optional ID pattern
    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str]]:

        # Determine the engine name and schema name for the given object type using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)

        # Resolve the SQL query for listing nodes based on the provided object type and ID pattern
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths["registry"]["commit"]["node_list"],
            registry    = schema_name,
            object_type = object_type,
            id_pattern  = id_pattern.replace("*", "%") if id_pattern is not None else "%",
        )

        # Execute the read query and return the results as a list of tuples containing object type and object ID
        return cast(list[tuple[str, str]], self._execute_read(engine_name=engine_name, query=sql_query))

    # Method: Check if a node with the given key exists in the database
    def exists(self, key: NodeKey) -> bool:

        # Determine the engine name and schema name for the given node key using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        # Resolve the SQL query for checking if the node exists based on the provided key
        sql_query = resolve_sql_query(
            file_path   = sql_queries_paths["registry"]["commit"]["node_exists"],
            registry    = schema_name,
            object_type = key.object_type,
            object_id   = key.object_id,
        )

        # Execute the read query and return True if the node exists, False otherwise
        result = self._execute_read(engine_name=engine_name, query=sql_query)

        # Return True if the result is not empty and the first element of the first row is truthy, otherwise return False
        return bool(result[0][0]) if result else False

    # Method: Check if multiple nodes with the given keys exist in the database
    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        keys = key_list.item_list if isinstance(key_list, NodeKeyList) else key_list
        return [self.exists(key) for key in keys]

    # Method: Retrieve a node with the given key from the database
    def get(self, key: NodeKey) -> Node | None:

        # Check if the node with the given key exists in the database; if not, log a "not found" message and return None
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Determine the engine name and schema name for the given node key using the schema resolver
        engine_name, schema_name = self.schema_resolver.for_node(key)

        # Resolve the SQL query for retrieving the basic information of the node based on the provided key
        basic_query = resolve_sql_query(
            file_path   = sql_queries_paths["registry"]["commit"]["node_get_basic"],
            registry    = schema_name,
            object_type = key.object_type,
            object_id   = key.object_id,
        )

        # Execute the basic query and retrieve the first row of the result, if available
        basic_data = cast(list[tuple[Any, ...]], self._execute_read(engine_name=engine_name, query=basic_query))

        # If no basic data is found, log a "not found" message and return None
        basic_row = basic_data[0] if basic_data else None

        # If no basic row is found, log a "not found" message and return None
        if basic_row is None:
            self.msg.not_found(key)
            return None

        # Resolve the SQL query for retrieving custom fields of the node based on the provided key
        custom_query = resolve_sql_query(
            file_path   = sql_queries_paths["registry"]["commit"]["node_get_custom"],
            registry    = schema_name,
            object_type = key.object_type,
            object_id   = key.object_id,
        )

        # Execute the custom query and retrieve the results as a list of tuples containing field information
        custom_fields = cast(list[tuple[str, str, Any]], self._execute_read(engine_name=engine_name, query=custom_query))

        # Resolve the SQL query for retrieving the page profile of the node based on the provided key
        profile_query = resolve_sql_query(
            file_path   = sql_queries_paths["registry"]["commit"]["node_get_profile"],
            registry    = schema_name,
            object_type = key.object_type,
            object_id   = key.object_id,
        )

        # Execute the profile query and retrieve the first row of the result, if available
        page_profile = self._execute_read(engine_name=engine_name, query=profile_query)
        page_profile_dict = dict(zip(PAGE_PROFILE_COLUMNS, page_profile[0])) if page_profile else {}

        # Initialize a dictionary to hold concepts for different mapping types
        concepts: dict[ConceptMapType, list[tuple[str, float]]] = {
            "detected"        : [],
            "ai_validated"    : [],
            "manually_mapped" : [],
        }

        # Loop through each mapping type (excluding "detected") and retrieve the corresponding concepts for the node
        for map_type in get_args(ConceptMapType):

            # Skip the "detected" mapping type as it is not needed for this operation
            if map_type == "detected":
                continue

            # Resolve the SQL query for retrieving concepts of the node based on the provided key and mapping type
            sql_query = resolve_sql_query(
                file_path=sql_queries_paths["registry"]["commit"][f"node_get_concepts_{map_type}"],
                registry=schema_name,
                object_type=key.object_type,
                object_id=key.object_id,
            )

            # Execute the concept query and store the results in the concepts dictionary for the current mapping type
            concepts[map_type] = cast(list[tuple[str, float]], self._execute_read(engine_name=engine_name, query=sql_query))

        # Use the MySQLNodeMapper to construct a Node object from the retrieved data and return it
        return MySQLNodeMapper.from_parts(
            key                       = key,
            basic_row                 = basic_row,
            custom_field_rows         = custom_fields,
            page_profile_row          = page_profile_dict,
            detected_concept_rows     = concepts["detected"],
            ai_validated_concept_rows = concepts["ai_validated"],
            manually_mapped_rows      = concepts["manually_mapped"],
        )

    # Method: Retrieve multiple nodes with the given keys from the database
    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        keys = key_list.item_list if isinstance(key_list, NodeKeyList) else key_list
        out = [node for node in (self.get(key) for key in keys) if node is not None]
        return NodeList(item_list=out)

    #================================================================#
    # Function Group: Internal helpers for node persistence          #
    #================================================================#

    # Function: Persist a single node within an already-open session
    def _persist_node(self, session: MySQLSession, schema_name: str, node: Node) -> None:
        """Write one node inside an already-open session."""

        # Get the key of the node to be persisted
        key = node.key

        # Get the qualified table path for the "Nodes_N_Object" table in the specified schema
        table_path = self._qt(schema_name, "Nodes_N_Object")

        # Convert the node to a basic row representation using the MySQLNodeMapper
        basic_row = MySQLNodeMapper.to_basic_row(node)

        # Upsert the basic row into the "Nodes_N_Object" table using the provided session
        self._upsert_rows(
            session          = session,
            table_path       = table_path,
            key_column_names = ["object_type", "object_id"],
            upd_column_names = list(basic_row.keys()),
            rows             = [{"object_type": key.object_type, "object_id": key.object_id, **basic_row}],
        )

        # Soft-delete existing custom fields so the domain field_list is authoritative.
        self._soft_delete_by_keys(
            session     = session,
            schema_name = schema_name,
            table_name  = "Data_N_Object_T_CustomFields",
            keys        = [key],
        )

        # Convert the node to custom field rows using the MySQLNodeMapper
        custom_rows = MySQLNodeMapper.to_custom_field_rows(node)

        # If there are custom rows, upsert them into the "Data_N_Object_T_CustomFields" table using the provided session
        if custom_rows:
            self._upsert_rows(
                session=session,
                table_path       = self._qt(schema_name, "Data_N_Object_T_CustomFields"),
                key_column_names = ["object_type", "object_id", "field_language", "field_name"],
                upd_column_names = ["field_value", "record_deleted"],
                rows             = [{**row, "record_deleted": 0} for row in custom_rows],
            )

        # Convert the node to a page profile row using the MySQLNodeMapper
        page_profile_row = MySQLNodeMapper.to_page_profile_row(node)

        # If there is a page profile row, upsert it into the "Data_N_Object_T_PageProfile" table using the provided session
        self._upsert_rows(
            session          = session,
            table_path       = self._qt(schema_name, "Data_N_Object_T_PageProfile"),
            key_column_names = ["object_type", "object_id"],
            upd_column_names = list(page_profile_row.keys()),
            rows             = [{"object_type": key.object_type, "object_id": key.object_id, **page_profile_row}],
        )

        # Upsert concept edges for each mapping type (excluding "detected") using the MySQLNodeMapper
        for map_type, table_name in zip(get_args(ConceptMapType), self._CONCEPT_TABLE_NAMES.values(), strict=True):

            # Skip the "detected" mapping type as it is not needed for this operation
            if map_type == "detected":
                continue

            # Convert the node to scored concept rows for the current mapping type using the MySQLNodeMapper
            concept_rows = MySQLNodeMapper.to_scored_concepts_rows(node, map_to=map_type)
            if not concept_rows:
                continue

            # Upsert the concept rows into the corresponding concept edge table using the provided session
            self._upsert_rows(
                session          = session,
                table_path       = self._qt(schema_name, table_name),
                key_column_names = ["object_type", "object_id", "concept_id", "text_source"],
                upd_column_names = ["score", "record_deleted"],
                rows             = [{**row, "record_deleted": 0} for row in concept_rows],
            )

    # Function: Persist a group of nodes that share the same schema in a batched fashion
    def _persist_node_group(self, session: MySQLSession, schema_name: str, nodes: list[Node]) -> None:
        """Write a group of nodes that share one schema in a batched fashion."""

        # If the list of nodes is empty, return early as there is nothing to persist
        if not nodes:
            return

        #--------------------#
        # Process basic rows #
        #--------------------#

        # Convert each node to a basic row representation using the MySQLNodeMapper and
        # prepare a list of dictionaries for upserting
        basic_rows: list[dict[str, Any]] = []
        for node in nodes:
            basic_row = MySQLNodeMapper.to_basic_row(node)
            basic_rows.append({
                "object_type" : node.key.object_type,
                "object_id"   : node.key.object_id,
                **basic_row,
            })

        # Upsert the basic rows into the "Nodes_N_Object" table using the provided session
        self._upsert_rows(
            session          = session,
            table_path       = self._qt(schema_name, "Nodes_N_Object"),
            key_column_names = ["object_type", "object_id"],
            upd_column_names = list(MySQLNodeMapper.to_basic_row(nodes[0]).keys()),
            rows             = basic_rows,
        )

        #---------------------------------------#
        # Custom fields: delete old, insert new #
        #---------------------------------------#

        # Soft-delete existing custom fields for all nodes in the group so that the
        # domain field_list is authoritative.
        self._soft_delete_by_keys(
            session     = session,
            schema_name = schema_name,
            table_name  = "Data_N_Object_T_CustomFields",
            keys        = [node.key for node in nodes],
        )

        # Convert each node to custom field rows using the MySQLNodeMapper and prepare a
        # list of dictionaries for upserting
        custom_field_rows: list[dict[str, Any]] = []

        # Loop through each node in the group and convert it to custom field rows using the MySQLNodeMapper
        for node in nodes:
            for row in MySQLNodeMapper.to_custom_field_rows(node):
                custom_field_rows.append({**row, "record_deleted": 0})

        # If there are custom field rows, upsert them into the "Data_N_Object_T_CustomFields"
        # table using the provided session
        if custom_field_rows:
            self._upsert_rows(
                session          = session,
                table_path       = self._qt(schema_name, "Data_N_Object_T_CustomFields"),
                key_column_names = ["object_type", "object_id", "field_language", "field_name"],
                upd_column_names = ["field_value", "record_deleted"],
                rows             = custom_field_rows,
            )

        #-----------------------#
        # Process page profiles #
        #-----------------------#

        # Initialize lists and sets to hold page profile rows and columns for upserting
        page_profile_rows: list[dict[str, Any]] = []
        page_profile_cols: set[str] = set()

        # Loop through each node in the group and convert it to a page profile row using the MySQLNodeMapper
        for node in nodes:

            # If the node does not have a page profile, skip it and continue to the next node
            if node.page_profile is None:
                continue

            # Convert the node to a page profile row using the MySQLNodeMapper
            row = MySQLNodeMapper.to_page_profile_row(node)

            # Update the set of page profile columns with the keys from the current row to
            # ensure all columns are captured for normalization
            page_profile_cols.update(row.keys())

            # Append the current row to the list of page profile rows, including the
            # object type and object ID for identification
            page_profile_rows.append({
                "object_type" : node.key.object_type,
                "object_id"   : node.key.object_id,
                **row,
            })

        # If there are page profile rows to upsert, normalize them to ensure all rows have the same shape
        if page_profile_rows:

            # Normalize every row to the union of columns; missing values become
            # None so the multi-row INSERT can use a single shape.
            sorted_cols = sorted(page_profile_cols)

            # Create a list of normalized rows where each row contains the object type, object ID,
            # and all columns from the union of page profile columns. Missing values are filled with None.
            normalized_rows = [
                {"object_type": row["object_type"], "object_id": row["object_id"], **{col: row.get(col) for col in sorted_cols}}
                for row in page_profile_rows
            ]

            # Upsert the normalized page profile rows into the "Data_N_Object_T_PageProfile"
            # table using the provided session
            self._upsert_rows(
                session          = session,
                table_path       = self._qt(schema_name, "Data_N_Object_T_PageProfile"),
                key_column_names = ["object_type", "object_id"],
                upd_column_names = sorted_cols,
                rows             = normalized_rows,
            )

        #-----------------------#
        # Process concept edges #
        #-----------------------#

        # Loop through each mapping type (excluding "detected") and upsert the corresponding
        # concept edges for all nodes in the group
        for map_type, table_name in zip(get_args(ConceptMapType), self._CONCEPT_TABLE_NAMES.values(), strict=True):

            # Skip the "detected" mapping type as it is not needed for this operation
            if map_type == "detected":
                continue

            # Convert each node to scored concept rows for the current mapping type using the MySQLNodeMapper
            concept_rows: list[dict[str, Any]] = []

            # Loop through each node in the group and convert it to scored concept rows for the current mapping type
            for node in nodes:
                for row in MySQLNodeMapper.to_scored_concepts_rows(node, map_to=map_type):
                    concept_rows.append({**row, "record_deleted": 0})

            # If there are concept rows to upsert, perform the upsert operation into the corresponding concept edge table
            if concept_rows:
                self._upsert_rows(
                    session          = session,
                    table_path       = self._qt(schema_name, table_name),
                    key_column_names = ["object_type", "object_id", "concept_id", "text_source"],
                    upd_column_names = ["score", "record_deleted"],
                    rows             = concept_rows,
                )

    #================================================================#
    # Method Group: Basic Node CRUD/persistence operations           #
    #================================================================#

    # Method: Save a single node to the database, optionally committing the transaction
    def save(self, node: Node, actions: ActionSet = ("commit",)) -> Node:
        engine_name, schema_name = self.schema_resolver.for_node(node.key)
        do_commit = "commit" in actions

        if do_commit:
            session = self._session(engine_name)
            try:
                self._persist_node(session, schema_name, node)
                if self._uow is None:
                    session.commit()
            except Exception:
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        self.msg.saved(node.key)
        return node

    # Method: Save a list of nodes to the database, optionally committing per schema group
    def save_many(self, node_list: NodeList | list[Node], actions: ActionSet = ("commit",)) -> NodeList:
        nodes = node_list.item_list if isinstance(node_list, NodeList) else list(node_list)
        do_commit = "commit" in actions

        if not do_commit:
            return NodeList(item_list=nodes)

        # Group nodes by (engine_name, schema_name) so each group gets one
        # transaction and one batch of statements.
        groups: dict[tuple[str, str], list[Node]] = {}
        for node in nodes:
            engine_name, schema_name = self.schema_resolver.for_node(node.key)
            groups.setdefault((engine_name, schema_name), []).append(node)

        for (engine_name, schema_name), group_nodes in groups.items():
            session = self._session(engine_name)
            try:
                self._persist_node_group(session, schema_name, group_nodes)
                if self._uow is None:
                    session.commit()
            except Exception:
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        for node in nodes:
            self.msg.saved(node.key)

        return NodeList(item_list=nodes)

    #================================================================#
    # Method Group: Delete / Delete many                             #
    #================================================================#

    # Method: Delete a single node from the database
    def delete(self, key: NodeKey, actions: ActionSet = ("commit",)) -> bool | None:
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        engine_name, schema_name = self.schema_resolver.for_node(key)
        do_commit = "commit" in actions

        if do_commit:
            session = self._session(engine_name)
            try:
                for table_name in [
                    "Nodes_N_Object",
                    "Data_N_Object_T_PageProfile",
                    "Data_N_Object_T_CustomFields",
                    "Edges_N_Object_N_Concept_T_ConceptDetection",
                ]:
                    self._soft_delete_by_keys(session, schema_name, table_name, [key])
                if self._uow is None:
                    session.commit()
            except Exception:
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        self.msg.deleted(key)
        return True

    # Method: Delete a list of nodes from the database
    def delete_many(self, key_list: NodeKeyList | list[NodeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        keys = key_list.item_list if isinstance(key_list, NodeKeyList) else list(key_list)
        do_commit = "commit" in actions

        if not do_commit:
            return [None] * len(keys)

        # Group keys by schema and soft-delete in batches.
        groups: dict[tuple[str, str], list[NodeKey]] = {}
        for key in keys:
            engine_name, schema_name = self.schema_resolver.for_node(key)
            groups.setdefault((engine_name, schema_name), []).append(key)

        results: dict[NodeKey, bool] = {}
        for (engine_name, schema_name), group_keys in groups.items():
            # Determine which keys actually exist before deleting, so we can
            # preserve the per-key boolean/None semantics of the port.
            existing_keys = self._filter_existing_keys(engine_name, schema_name, group_keys)

            if existing_keys:
                session = self._session(engine_name)
                try:
                    for table_name in [
                        "Nodes_N_Object",
                        "Data_N_Object_T_PageProfile",
                        "Data_N_Object_T_CustomFields",
                        "Edges_N_Object_N_Concept_T_ConceptDetection",
                    ]:
                        self._soft_delete_by_keys(session, schema_name, table_name, existing_keys)
                    if self._uow is None:
                        session.commit()
                except Exception:
                    if self._uow is None:
                        session.rollback()
                    raise
                finally:
                    self._close_standalone_session(session)

                for key in existing_keys:
                    results[key] = True
                    self.msg.deleted(key)

        return [results.get(key) for key in keys]

    # Function: Return the subset of keys that currently exist and are not soft-deleted.
    def _filter_existing_keys(
        self,
        engine_name: str,
        schema_name: str,
        keys: list[NodeKey],
    ) -> list[NodeKey]:
        """Return the subset of keys that currently exist and are not soft-deleted."""
        if not keys:
            return []

        placeholders, params = self._key_in_list_predicate(keys, prefix="ex")
        sql = f"""
            SELECT object_type, object_id
              FROM {self._qt(schema_name, "Nodes_N_Object")}
             WHERE (object_type, object_id) IN ({placeholders})
               AND record_deleted = 0
        """
        session = self._session(engine_name)
        try:
            rows = session.execute(sql, params)
        finally:
            self._close_standalone_session(session)

        existing = {(row[0], row[1]) for row in rows}
        return [key for key in keys if (key.object_type, key.object_id) in existing]

    #================================================================#
    # Method Group: Node diagnostics and special get/save operations #
    #================================================================#

    # Method: Return nodes that have no concepts attached
    def get_with_no_concepts(self, object_type: str | None = None, id_pattern: str | None = None) -> NodeList:
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type if object_type is not None else "Course")
        _, airflow_schema_name = self.schema_resolver.for_airflow()

        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_get_with_no_concepts"],
            registry=schema_name,
            airflow=airflow_schema_name,
            object_type=object_type if object_type is not None else "%",
            id_pattern=id_pattern.replace("*", "%") if id_pattern is not None else "%",
        )
        node_keys_data = cast(list[tuple[str, str]], self._execute_read(engine_name=engine_name, query=sql_query))
        node_keys = [
            NodeKey(object_type=cast(Any, row[0]), object_id=row[1])
            for row in node_keys_data
        ]
        return self.get_many(node_keys)

    #================================================================#
    # Method Group: Retry-aware persistence                          #
    #================================================================#

    # TODO: add retry with exponential back-off around session-level batch
    # operations for transient lock-wait timeouts (DBAPI code 1205). The retry
    # boundary must be the whole unit of work, not an individual statement,
    # because MySQL rolls back the current transaction on lock wait timeout.
