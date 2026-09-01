# graphregistry/adapters/persistence/mysql/repositories/rpo_edgerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
from graphregistry.adapters.persistence.mysql.mappers.map_edge import MySQLEdgeMapper
from graphregistry.adapters.persistence.mysql.repositories.helpers import key_tuple_in_list_predicate, qualified_table, soft_delete_by_key_tuples, upsert_rows
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_edge import EdgeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.types import ActionSet

# If TYPE_CHECKING is True, import GraphDB and MySQLUnitOfWork for type hints only.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork

#==================#
# Class Definition #
#==================#
class MySQLEdgeRepository(EdgeRepository):
    """MySQL adapter for the EdgeRepository port.

    The repository is bound to a UnitOfWork when used inside application
    services so that all writes for a business operation share one transaction.

    For backward compatibility it can also be constructed directly with a
    GraphDB client and schema resolver; in that case each public method
    manages its own short-lived session.
    """

    # Key columns that uniquely identify an edge row.
    _EDGE_KEY_COLUMNS: list[str] = [
        "from_object_type",
        "from_object_id",
        "to_object_type",
        "to_object_id",
        "context",
    ]

    # Class initialization and dependency injection
    def __init__(self, db: "GraphDB | None" = None, schema_resolver: "SchemaResolver | None" = None, *, uow: "MySQLUnitOfWork | None" = None) -> None:

        # Validate that either a UnitOfWork is provided, or both a GraphDB and
        # SchemaResolver are provided, but not both.
        if uow is not None and (db is not None or schema_resolver is not None):
            raise ValueError("Provide either uow= or (db=, schema_resolver=), not both.")

        # If a UnitOfWork is provided, use its db and schema_resolver; otherwise,
        # use the provided db and schema_resolver.
        if uow is not None:
            self._uow = uow
            self.db = uow.db
            self.schema_resolver = uow.schema_resolver

        # If a UnitOfWork is not provided, ensure that both db and schema_resolver
        # are provided; otherwise, raise an error.
        elif db is not None and schema_resolver is not None:
            self._uow = None
            self.db = db
            self.schema_resolver = schema_resolver
        else:
            raise ValueError("MySQLEdgeRepository requires either uow= or (db=, schema_resolver=).")

        # Initialize a GraphLogger instance for logging messages.
        self.msg = GraphLogger()

    #================================================================#
    # Function Group: Internal helpers                               #
    #================================================================#

    # Internal Function: Return a session for engine_name, creating one if needed.
    def _session(self, engine_name: str) -> MySQLSession:
        """Return a session for engine_name, creating a standalone one if needed."""

        # If a UnitOfWork is active, return its session for the given engine.
        if self._uow is not None:
            return self._uow.get_session(engine_name)

        # Else, no UnitOfWork is active, so create and begin a standalone session.
        session = MySQLSession(self.db, engine_name)
        session.begin()
        return session

    # Internal Function: Close a session created outside a UnitOfWork.
    def _close_standalone_session(self, session: MySQLSession) -> None:
        """Close a session created outside a UnitOfWork."""
        if self._uow is None:
            session.close()

    # Internal Function: Execute a read query and return the results as a list of tuples.
    def _execute_read(self, engine_name: str, query: str, params: dict[str, Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a read query."""
        session = self._session(engine_name)
        try:
            return session.execute(query, params)
        finally:
            self._close_standalone_session(session)

    # Internal Function: Return a safely quoted schema-qualified table name.
    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return qualified_table(schema_name, table_name)

    # Internal Function: Convert an EdgeKey into the tuple used by the database key columns.
    @staticmethod
    def _edge_key_tuple(key: EdgeKey) -> tuple[str, str, str, str, str]:
        return (
            key.from_object_type,
            key.from_object_id,
            key.to_object_type,
            key.to_object_id,
            key.context,
        )

    # Internal Function: Upsert rows using the shared batch upsert helper.
    @staticmethod
    def _upsert_rows(session: MySQLSession, table_path: str, key_column_names: list[str], upd_column_names: list[str], rows: list[dict[str, Any]]) -> None:
        upsert_rows(session, table_path, key_column_names, upd_column_names, rows)

    # Internal Function: Soft-delete rows matching the given edge keys in a table.
    @staticmethod
    def _soft_delete_by_keys(session: MySQLSession, schema_name: str, table_name: str, keys: list[EdgeKey]) -> None:
        key_tuples = [MySQLEdgeRepository._edge_key_tuple(key) for key in keys]
        soft_delete_by_key_tuples(
            session,
            schema_name,
            table_name,
            MySQLEdgeRepository._EDGE_KEY_COLUMNS,
            key_tuples,
        )

    # Internal Function: Build a SQL IN-list predicate for a list of edge keys.
    @staticmethod
    def _key_in_list_predicate(keys: list[EdgeKey], prefix: str = "key") -> tuple[str, dict[str, Any]]:
        key_tuples = [MySQLEdgeRepository._edge_key_tuple(key) for key in keys]
        return key_tuple_in_list_predicate(
            key_tuples,
            MySQLEdgeRepository._EDGE_KEY_COLUMNS,
            prefix=prefix,
        )

    #================================================================#
    # Method Group: Basic Edge CRUD/persistence operations           #
    #================================================================#

    # Public Method: List edges of a given object-type pair and optional ID pattern.
    def list(self, object_type: tuple[str, str], id_pattern: str | None) -> list[tuple[str, str, str, str, str]]:

        # Determine the engine name and schema name for the given object-type pair
        # using the schema resolver.
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)
        from_object_type, to_object_type = object_type

        # Resolve the SQL query for listing edges based on the provided object types
        # and ID pattern.
        sql_query = resolve_sql_query(
            file_path       = sql_queries_paths["registry"]["commit"]["edge_list"],
            registry        = schema_name,
            from_object_type= from_object_type,
            to_object_type  = to_object_type,
            id_pattern      = id_pattern.replace("*", "%") if id_pattern is not None else "%",
        )

        # Execute the read query and return the results as a list of edge key tuples.
        return cast(list[tuple[str, str, str, str, str]], self._execute_read(engine_name=engine_name, query=sql_query))

    # Public Method: Check whether a single edge exists.
    def exists(self, key: EdgeKey) -> bool:

        # Determine the engine name and schema name for the given edge key using
        # the schema resolver.
        engine_name, schema_name = self.schema_resolver.for_edge(key)

        # Resolve the SQL query for checking if the edge exists based on the
        # provided key.
        sql_query = resolve_sql_query(
            file_path       = sql_queries_paths["registry"]["commit"]["edge_exists"],
            registry        = schema_name,
            from_object_type= key.from_object_type,
            from_object_id  = key.from_object_id,
            to_object_type  = key.to_object_type,
            to_object_id    = key.to_object_id,
            context         = key.context,
        )

        # Execute the read query and return True if the edge exists, False otherwise.
        result = self._execute_read(engine_name=engine_name, query=sql_query)
        return bool(result[0][0]) if result else False

    # Public Method: Check whether a list of edges exist.
    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else key_list
        return [self.exists(key) for key in keys]

    # Public Method: Retrieve a single edge by key.
    def get(self, key: EdgeKey) -> Edge | None:

        # Check if the edge exists in the database; if not, log a "not found"
        # message and return None.
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Determine the engine name and schema name for the given edge key using
        # the schema resolver.
        engine_name, schema_name = self.schema_resolver.for_edge(key)

        # Resolve the SQL query for retrieving custom fields of the edge based on
        # the provided key.
        sql_query = resolve_sql_query(
            file_path       = sql_queries_paths["registry"]["commit"]["edge_get_custom"],
            registry        = schema_name,
            from_object_type= key.from_object_type,
            from_object_id  = key.from_object_id,
            to_object_type  = key.to_object_type,
            to_object_id    = key.to_object_id,
            context         = key.context,
        )

        # Execute the custom-field query and reconstruct the edge domain model.
        custom_fields = cast(list[tuple[str, str, Any]], self._execute_read(engine_name=engine_name, query=sql_query))
        return MySQLEdgeMapper.from_parts(key=key, custom_field_rows=custom_fields)

    # Public Method: Retrieve a list of edges by key.
    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else key_list
        out = [edge for edge in (self.get(key) for key in keys) if edge is not None]
        return EdgeList(item_list=out)

    #================================================================#
    # Function Group: Save helpers                                   #
    #================================================================#

    # Internal Function: Persist a single edge inside an already-open session.
    def _persist_edge(self, session: MySQLSession, schema_name: str, edge: Edge) -> None:
        """Write one edge inside an already-open session."""

        # Extract the edge key and basic row data using the mapper.
        key = edge.key
        basic_row = MySQLEdgeMapper.to_basic_row(edge)

        # Upsert the basic edge row into the child-to-parent edge table.
        self._upsert_rows(
            session          = session,
            table_path       = self._qt(schema_name, "Edges_N_Object_N_Object_T_ChildToParent"),
            key_column_names = self._EDGE_KEY_COLUMNS,
            upd_column_names = list(basic_row.keys()),
            rows             = [{
                "from_object_type" : key.from_object_type,
                "from_object_id"   : key.from_object_id,
                "to_object_type"   : key.to_object_type,
                "to_object_id"     : key.to_object_id,
                "context"          : key.context,
                **basic_row,
            }],
        )

        # Soft-delete any existing custom fields so the domain field_list is
        # authoritative. This ensures that any removed fields are deleted from the database.
        self._soft_delete_by_keys(
            session     = session,
            schema_name = schema_name,
            table_name  = "Data_N_Object_N_Object_T_CustomFields",
            keys        = [key],
        )

        # Convert the edge to custom field rows and upsert them if any exist.
        custom_rows = MySQLEdgeMapper.to_custom_field_rows(edge)
        if custom_rows:
            self._upsert_rows(
                session          = session,
                table_path       = self._qt(schema_name, "Data_N_Object_N_Object_T_CustomFields"),
                key_column_names = self._EDGE_KEY_COLUMNS + ["field_language", "field_name"],
                upd_column_names = ["field_value", "record_deleted"],
                rows             = [{**row, "record_deleted": 0} for row in custom_rows],
            )

    # Internal Function: Persist edges sharing one schema in batch.
    def _persist_edge_group(self, session: MySQLSession, schema_name: str, edges: list[Edge]) -> None:
        """Write a group of edges that share one schema in a batched fashion."""

        # If the list of edges is empty, return early as there is nothing to persist.
        if not edges:
            return

        #--------------------#
        # Process basic rows #
        #--------------------#

        # Convert each edge to a basic row representation using the MySQLEdgeMapper and
        # prepare a list of dictionaries for upserting.
        basic_rows: list[dict[str, Any]] = []
        for edge in edges:
            basic_row = MySQLEdgeMapper.to_basic_row(edge)
            basic_rows.append({
                "from_object_type" : edge.key.from_object_type,
                "from_object_id"   : edge.key.from_object_id,
                "to_object_type"   : edge.key.to_object_type,
                "to_object_id"     : edge.key.to_object_id,
                "context"          : edge.key.context,
                **basic_row,
            })

        # Upsert the basic edge rows into the child-to-parent edge table.
        self._upsert_rows(
            session          = session,
            table_path       = self._qt(schema_name, "Edges_N_Object_N_Object_T_ChildToParent"),
            key_column_names = self._EDGE_KEY_COLUMNS,
            upd_column_names = list(MySQLEdgeMapper.to_basic_row(edges[0]).keys()),
            rows             = basic_rows,
        )

        #---------------------------------------#
        # Custom fields: delete old, insert new #
        #---------------------------------------#

        # Soft-delete existing custom fields for all edges in the group so that the
        # domain field_list is authoritative.
        self._soft_delete_by_keys(
            session     = session,
            schema_name = schema_name,
            table_name  = "Data_N_Object_N_Object_T_CustomFields",
            keys        = [edge.key for edge in edges],
        )

        # Convert each edge to custom field rows using the MySQLEdgeMapper and prepare a
        # list of dictionaries for upserting.
        custom_field_rows: list[dict[str, Any]] = []
        for edge in edges:
            for row in MySQLEdgeMapper.to_custom_field_rows(edge):
                custom_field_rows.append({**row, "record_deleted": 0})

        # If there are custom field rows, upsert them into the custom fields table.
        if custom_field_rows:
            self._upsert_rows(
                session          = session,
                table_path       = self._qt(schema_name, "Data_N_Object_N_Object_T_CustomFields"),
                key_column_names = self._EDGE_KEY_COLUMNS + ["field_language", "field_name"],
                upd_column_names = ["field_value", "record_deleted"],
                rows             = custom_field_rows,
            )

    #================================================================#
    # Method Group: Save / Save many                                 #
    #================================================================#

    # Public Method: Save a single edge, committing if requested.
    def save(self, edge: Edge, actions: ActionSet = ("commit",)) -> Edge:

        # Determine the engine name and schema name for the edge using the schema resolver.
        engine_name, schema_name = self.schema_resolver.for_edge(edge.key)

        # Check whether the caller requested an explicit commit.
        do_commit = "commit" in actions

        # If committing, open a session, persist the edge, and manage the transaction.
        if do_commit:
            session = self._session(engine_name)
            try:
                self._persist_edge(session, schema_name, edge)

                # Commit the standalone session if not inside a UnitOfWork.
                if self._uow is None:
                    session.commit()
            except Exception:
                # Roll back the standalone session if not inside a UnitOfWork.
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        # Log the saved edge and return it for chaining.
        self.msg.saved(edge.key)
        return edge

    # Public Method: Save a list of edges, committing per schema group if requested.
    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ("commit",)) -> EdgeList:

        # Normalize the input to a plain list of edges.
        edges = edge_list.item_list if isinstance(edge_list, EdgeList) else list(edge_list)

        # Check whether the caller requested an explicit commit.
        do_commit = "commit" in actions

        # If not committing, return the edges without touching persistence.
        if not do_commit:
            return EdgeList(item_list=edges)

        # Group edges by (engine_name, schema_name) so each group gets one
        # transaction and one batch of statements.
        groups: dict[tuple[str, str], list[Edge]] = {}
        for edge in edges:
            engine_name, schema_name = self.schema_resolver.for_edge(edge.key)
            groups.setdefault((engine_name, schema_name), []).append(edge)

        # Persist each group inside its own session.
        for (engine_name, schema_name), group_edges in groups.items():
            session = self._session(engine_name)
            try:
                self._persist_edge_group(session, schema_name, group_edges)

                # Commit the standalone session if not inside a UnitOfWork.
                if self._uow is None:
                    session.commit()
            except Exception:
                # Roll back the standalone session if not inside a UnitOfWork.
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        # Log every saved edge.
        for edge in edges:
            self.msg.saved(edge.key)

        # Return the saved edge list.
        return EdgeList(item_list=edges)

    #================================================================#
    # Method Group: Delete / Delete many                             #
    #================================================================#

    # Public Method: Delete a single edge.
    def delete(self, key: EdgeKey, actions: ActionSet = ("commit",)) -> bool | None:

        # Check if the edge exists; if not, log and return None.
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        # Resolve the schema for the edge and decide whether to commit.
        engine_name, schema_name = self.schema_resolver.for_edge(key)
        do_commit = "commit" in actions

        # Persist the deletion inside a new session when requested.
        if do_commit:
            session = self._session(engine_name)
            try:
                for table_name in [
                    "Edges_N_Object_N_Object_T_ChildToParent",
                    "Data_N_Object_N_Object_T_CustomFields",
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

        # Log the deletion and report success to the caller.
        self.msg.deleted(key)
        return True

    # Public Method: Delete a list of edges.
    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:

        # Normalize the input to a plain list of keys.
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else list(key_list)

        # Determine whether the caller requested an explicit commit.
        do_commit = "commit" in actions

        # Skip persistence when the caller did not request a commit.
        if not do_commit:
            return [None] * len(keys)

        # Group keys by their resolved engine/schema for batched deletes.
        groups: dict[tuple[str, str], list[EdgeKey]] = {}
        for key in keys:
            engine_name, schema_name = self.schema_resolver.for_edge(key)
            groups.setdefault((engine_name, schema_name), []).append(key)

        # Track deletion results for each input key.
        results: dict[EdgeKey, bool] = {}
        for (engine_name, schema_name), group_keys in groups.items():

            # Determine which keys in this group still exist in the database.
            existing_keys = self._filter_existing_keys(engine_name, schema_name, group_keys)

            # Delete only the keys that still exist in the database.
            if existing_keys:
                session = self._session(engine_name)
                try:
                    for table_name in [
                        "Edges_N_Object_N_Object_T_ChildToParent",
                        "Data_N_Object_N_Object_T_CustomFields",
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

                # Record successful deletions and emit log messages.
                for key in existing_keys:
                    results[key] = True
                    self.msg.deleted(key)

        # Return deletion results aligned with the original key order.
        return [results.get(key) for key in keys]

    # Internal Function: Filter keys that exist and are not soft-deleted.
    def _filter_existing_keys(self, engine_name: str, schema_name: str, keys: list[EdgeKey]) -> list[EdgeKey]:
        """Return the subset of keys that currently exist and are not soft-deleted."""

        # If the list of keys is empty, return an empty list immediately.
        if not keys:
            return []

        # Build an IN-list predicate and query the database for the given keys.
        placeholders, params = self._key_in_list_predicate(keys, prefix="ex")

        # Query the database for the given keys, filtering out soft-deleted records.
        sql = f"""
            SELECT from_object_type, from_object_id, to_object_type, to_object_id, context
              FROM {self._qt(schema_name, "Edges_N_Object_N_Object_T_ChildToParent")}
             WHERE (from_object_type, from_object_id, to_object_type, to_object_id, context) IN ({placeholders})
               AND record_deleted = 0
        """
        session = self._session(engine_name)
        try:
            rows = session.execute(sql, params)
        finally:
            self._close_standalone_session(session)

        # Determine which input keys match existing rows.
        existing = {tuple(row) for row in rows}
        return [key for key in keys if self._edge_key_tuple(key) in existing]
