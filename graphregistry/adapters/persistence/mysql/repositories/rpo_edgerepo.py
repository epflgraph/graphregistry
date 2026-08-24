# graphregistry/adapters/persistence/mysql/repositories/rpo_edgerepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast

from graphregistry.adapters.persistence.mysql.mappers.map_edge import MySQLEdgeMapper
from graphregistry.adapters.persistence.mysql.repositories._helpers import (
    qualified_table,
    soft_delete_by_key_tuples,
    upsert_rows,
)
from graphregistry.adapters.persistence.mysql.session import MySQLSession
from graphregistry.application.ports.repositories.prt_edge import EdgeRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.dbstruct import resolve_sql_query, sql_queries_paths
from graphregistry.common.logger import GraphLogger
from graphregistry.domain.models.entities.mdl_base import EdgeKeyList
from graphregistry.domain.models.entities.mdl_edge import Edge, EdgeKey, EdgeList
from graphregistry.domain.types import ActionSet

if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork


class MySQLEdgeRepository(EdgeRepository):
    """MySQL adapter for the EdgeRepository port.

    The repository is bound to a UnitOfWork when used inside application
    services so that all writes for a business operation share one transaction.

    For backward compatibility it can also be constructed directly with a
    GraphDB client and schema resolver; in that case each public method
    manages its own short-lived session.
    """

    _EDGE_KEY_COLUMNS: list[str] = [
        "from_object_type",
        "from_object_id",
        "to_object_type",
        "to_object_id",
        "context",
    ]

    def __init__(
        self,
        db: "GraphDB | None" = None,
        schema_resolver: "SchemaResolver | None" = None,
        *,
        uow: "MySQLUnitOfWork | None" = None,
    ) -> None:
        if uow is not None and (db is not None or schema_resolver is not None):
            raise ValueError("Provide either uow= or (db=, schema_resolver=), not both.")

        if uow is not None:
            self._uow = uow
            self.db = uow.db
            self.schema_resolver = uow.schema_resolver
        elif db is not None and schema_resolver is not None:
            self._uow = None
            self.db = db
            self.schema_resolver = schema_resolver
        else:
            raise ValueError("MySQLEdgeRepository requires either uow= or (db=, schema_resolver=).")

        self.msg = GraphLogger()

    # ---------------------------------------------------------------------- #
    # Internal helpers                                                       #
    # ---------------------------------------------------------------------- #

    def _session(self, engine_name: str) -> MySQLSession:
        """Return a session for engine_name, creating a standalone one if needed."""
        if self._uow is not None:
            return self._uow.get_session(engine_name)

        session = MySQLSession(self.db, engine_name)
        session.begin()
        return session

    def _close_standalone_session(self, session: MySQLSession) -> None:
        """Close a session created outside a UnitOfWork."""
        if self._uow is None:
            session.close()

    def _execute_read(
        self,
        engine_name: str,
        query: str,
        params: dict[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a read query."""
        session = self._session(engine_name)
        try:
            return session.execute(query, params)
        finally:
            self._close_standalone_session(session)

    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return qualified_table(schema_name, table_name)

    @staticmethod
    def _edge_key_tuple(key: EdgeKey) -> tuple[str, str, str, str, str]:
        return (
            key.from_object_type,
            key.from_object_id,
            key.to_object_type,
            key.to_object_id,
            key.context,
        )

    @staticmethod
    def _upsert_rows(
        session: MySQLSession,
        table_path: str,
        key_column_names: list[str],
        upd_column_names: list[str],
        rows: list[dict[str, Any]],
    ) -> None:
        upsert_rows(session, table_path, key_column_names, upd_column_names, rows)

    @staticmethod
    def _soft_delete_by_keys(
        session: MySQLSession,
        schema_name: str,
        table_name: str,
        keys: list[EdgeKey],
    ) -> None:
        key_tuples = [MySQLEdgeRepository._edge_key_tuple(key) for key in keys]
        soft_delete_by_key_tuples(
            session,
            schema_name,
            table_name,
            MySQLEdgeRepository._EDGE_KEY_COLUMNS,
            key_tuples,
        )

    @staticmethod
    def _key_in_list_predicate(keys: list[EdgeKey], prefix: str = "key") -> tuple[str, dict[str, Any]]:
        from graphregistry.adapters.persistence.mysql.repositories._helpers import key_tuple_in_list_predicate

        key_tuples = [MySQLEdgeRepository._edge_key_tuple(key) for key in keys]
        return key_tuple_in_list_predicate(
            key_tuples,
            MySQLEdgeRepository._EDGE_KEY_COLUMNS,
            prefix=prefix,
        )

    # ---------------------------------------------------------------------- #
    # Basic Edge CRUD/persistence operations                                 #
    # ---------------------------------------------------------------------- #

    def list(self, object_type: tuple[str, str], id_pattern: str | None) -> list[tuple[str, str, str, str, str]]:
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)
        from_object_type, to_object_type = object_type

        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["edge_list"],
            registry=schema_name,
            from_object_type=from_object_type,
            to_object_type=to_object_type,
            id_pattern=id_pattern.replace("*", "%") if id_pattern is not None else "%",
        )
        return cast(list[tuple[str, str, str, str, str]], self._execute_read(engine_name=engine_name, query=sql_query))

    def exists(self, key: EdgeKey) -> bool:
        engine_name, schema_name = self.schema_resolver.for_edge(key)
        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["edge_exists"],
            registry=schema_name,
            from_object_type=key.from_object_type,
            from_object_id=key.from_object_id,
            to_object_type=key.to_object_type,
            to_object_id=key.to_object_id,
            context=key.context,
        )
        result = self._execute_read(engine_name=engine_name, query=sql_query)
        return bool(result[0][0]) if result else False

    def exists_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> list[bool]:
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else key_list
        return [self.exists(key) for key in keys]

    def get(self, key: EdgeKey) -> Edge | None:
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        engine_name, schema_name = self.schema_resolver.for_edge(key)
        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["edge_get_custom"],
            registry=schema_name,
            from_object_type=key.from_object_type,
            from_object_id=key.from_object_id,
            to_object_type=key.to_object_type,
            to_object_id=key.to_object_id,
            context=key.context,
        )
        custom_fields = cast(list[tuple[str, str, Any]], self._execute_read(engine_name=engine_name, query=sql_query))
        return MySQLEdgeMapper.from_parts(key=key, custom_field_rows=custom_fields)

    def get_many(self, key_list: EdgeKeyList | list[EdgeKey]) -> EdgeList:
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else key_list
        out = [edge for edge in (self.get(key) for key in keys) if edge is not None]
        return EdgeList(item_list=out)

    # ---------------------------------------------------------------------- #
    # Save helpers                                                           #
    # ---------------------------------------------------------------------- #

    def _persist_edge(self, session: MySQLSession, schema_name: str, edge: Edge) -> None:
        """Write one edge inside an already-open session."""
        key = edge.key
        basic_row = MySQLEdgeMapper.to_basic_row(edge)

        self._upsert_rows(
            session=session,
            table_path=self._qt(schema_name, "Edges_N_Object_N_Object_T_ChildToParent"),
            key_column_names=self._EDGE_KEY_COLUMNS,
            upd_column_names=list(basic_row.keys()),
            rows=[{
                "from_object_type": key.from_object_type,
                "from_object_id": key.from_object_id,
                "to_object_type": key.to_object_type,
                "to_object_id": key.to_object_id,
                "context": key.context,
                **basic_row,
            }],
        )

        self._soft_delete_by_keys(
            session=session,
            schema_name=schema_name,
            table_name="Data_N_Object_N_Object_T_CustomFields",
            keys=[key],
        )

        custom_rows = MySQLEdgeMapper.to_custom_field_rows(edge)
        if custom_rows:
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, "Data_N_Object_N_Object_T_CustomFields"),
                key_column_names=self._EDGE_KEY_COLUMNS + ["field_language", "field_name"],
                upd_column_names=["field_value", "record_deleted"],
                rows=custom_rows,
            )

    def _persist_edge_group(self, session: MySQLSession, schema_name: str, edges: list[Edge]) -> None:
        """Write a group of edges that share one schema in a batched fashion."""
        if not edges:
            return

        basic_rows: list[dict[str, Any]] = []
        for edge in edges:
            basic_row = MySQLEdgeMapper.to_basic_row(edge)
            basic_rows.append({
                "from_object_type": edge.key.from_object_type,
                "from_object_id": edge.key.from_object_id,
                "to_object_type": edge.key.to_object_type,
                "to_object_id": edge.key.to_object_id,
                "context": edge.key.context,
                **basic_row,
            })

        self._upsert_rows(
            session=session,
            table_path=self._qt(schema_name, "Edges_N_Object_N_Object_T_ChildToParent"),
            key_column_names=self._EDGE_KEY_COLUMNS,
            upd_column_names=list(MySQLEdgeMapper.to_basic_row(edges[0]).keys()),
            rows=basic_rows,
        )

        self._soft_delete_by_keys(
            session=session,
            schema_name=schema_name,
            table_name="Data_N_Object_N_Object_T_CustomFields",
            keys=[edge.key for edge in edges],
        )

        custom_field_rows: list[dict[str, Any]] = []
        for edge in edges:
            custom_field_rows.extend(MySQLEdgeMapper.to_custom_field_rows(edge))
        if custom_field_rows:
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, "Data_N_Object_N_Object_T_CustomFields"),
                key_column_names=self._EDGE_KEY_COLUMNS + ["field_language", "field_name"],
                upd_column_names=["field_value", "record_deleted"],
                rows=custom_field_rows,
            )

    # ---------------------------------------------------------------------- #
    # Save / Save many                                                       #
    # ---------------------------------------------------------------------- #

    def save(self, edge: Edge, actions: ActionSet = ("commit",)) -> Edge:
        engine_name, schema_name = self.schema_resolver.for_edge(edge.key)
        do_commit = "commit" in actions

        if do_commit:
            session = self._session(engine_name)
            try:
                self._persist_edge(session, schema_name, edge)
                if self._uow is None:
                    session.commit()
            except Exception:
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        self.msg.saved(edge.key)
        return edge

    def save_many(self, edge_list: EdgeList | list[Edge], actions: ActionSet = ("commit",)) -> EdgeList:
        edges = edge_list.item_list if isinstance(edge_list, EdgeList) else list(edge_list)
        do_commit = "commit" in actions

        if not do_commit:
            return EdgeList(item_list=edges)

        groups: dict[tuple[str, str], list[Edge]] = {}
        for edge in edges:
            engine_name, schema_name = self.schema_resolver.for_edge(edge.key)
            groups.setdefault((engine_name, schema_name), []).append(edge)

        for (engine_name, schema_name), group_edges in groups.items():
            session = self._session(engine_name)
            try:
                self._persist_edge_group(session, schema_name, group_edges)
                if self._uow is None:
                    session.commit()
            except Exception:
                if self._uow is None:
                    session.rollback()
                raise
            finally:
                self._close_standalone_session(session)

        for edge in edges:
            self.msg.saved(edge.key)

        return EdgeList(item_list=edges)

    # ---------------------------------------------------------------------- #
    # Delete / Delete many                                                   #
    # ---------------------------------------------------------------------- #

    def delete(self, key: EdgeKey, actions: ActionSet = ("commit",)) -> bool | None:
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        engine_name, schema_name = self.schema_resolver.for_edge(key)
        do_commit = "commit" in actions

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

        self.msg.deleted(key)
        return True

    def delete_many(self, key_list: EdgeKeyList | list[EdgeKey], actions: ActionSet = ("commit",)) -> list[bool | None]:
        keys = key_list.item_list if isinstance(key_list, EdgeKeyList) else list(key_list)
        do_commit = "commit" in actions

        if not do_commit:
            return [None] * len(keys)

        groups: dict[tuple[str, str], list[EdgeKey]] = {}
        for key in keys:
            engine_name, schema_name = self.schema_resolver.for_edge(key)
            groups.setdefault((engine_name, schema_name), []).append(key)

        results: dict[EdgeKey, bool] = {}
        for (engine_name, schema_name), group_keys in groups.items():
            existing_keys = self._filter_existing_keys(engine_name, schema_name, group_keys)

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

                for key in existing_keys:
                    results[key] = True
                    self.msg.deleted(key)

        return [results.get(key) for key in keys]

    def _filter_existing_keys(
        self,
        engine_name: str,
        schema_name: str,
        keys: list[EdgeKey],
    ) -> list[EdgeKey]:
        """Return the subset of keys that currently exist and are not soft-deleted."""
        if not keys:
            return []

        placeholders, params = self._key_in_list_predicate(keys, prefix="ex")
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

        existing = {tuple(row) for row in rows}
        return [key for key in keys if self._edge_key_tuple(key) in existing]
