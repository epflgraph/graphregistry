# graphregistry/adapters/persistence/mysql/repositories/rpo_noderepo.py
from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast, get_args

from graphregistry.adapters.persistence.mysql.mappers.map_node import MySQLNodeMapper
from graphregistry.adapters.persistence.mysql.repositories._helpers import (
    qualified_table,
    soft_delete_by_key_tuples,
    upsert_rows,
)
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

if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB
    from graphregistry.adapters.persistence.mysql.unit_of_work import MySQLUnitOfWork


class MySQLNodeRepository(NodeRepository):
    """MySQL adapter for the NodeRepository port.

    The repository is bound to a UnitOfWork when used inside application
    services so that all writes for a business operation share one transaction.

    For backward compatibility it can also be constructed directly with a
    GraphDB client and schema resolver; in that case each public method
    manages its own short-lived session.
    """

    _CONCEPT_TABLE_NAMES: dict[ConceptMapType, str] = {
        "detected"       : "Edges_N_Object_N_Concept_T_ConceptDetection",
        "ai_validated"   : "Edges_N_Object_N_Concept_T_LLMPostValidated",
        "manually_mapped": "Edges_N_Object_N_Concept_T_ManualMapping",
    }

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
            raise ValueError("MySQLNodeRepository requires either uow= or (db=, schema_resolver=).")

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

    # ---------------------------------------------------------------------- #
    # Internal helpers                                                       #
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _qt(schema_name: str, table_name: str) -> str:
        return qualified_table(schema_name, table_name)

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
        keys: list[NodeKey],
    ) -> None:
        key_tuples = [(key.object_type, key.object_id) for key in keys]
        soft_delete_by_key_tuples(
            session,
            schema_name,
            table_name,
            ["object_type", "object_id"],
            key_tuples,
        )

    @staticmethod
    def _key_in_list_predicate(keys: list[NodeKey], prefix: str = "key") -> tuple[str, dict[str, Any]]:
        key_tuples = [(key.object_type, key.object_id) for key in keys]
        from graphregistry.adapters.persistence.mysql.repositories._helpers import key_tuple_in_list_predicate
        return key_tuple_in_list_predicate(key_tuples, ["object_type", "object_id"], prefix=prefix)

    # ---------------------------------------------------------------------- #
    # Basic Node CRUD/persistence operations                                 #
    # ---------------------------------------------------------------------- #

    def list(self, object_type: str, id_pattern: str | None) -> list[tuple[str, str]]:
        engine_name, schema_name = self.schema_resolver.for_object_type(object_type)
        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_list"],
            registry=schema_name,
            object_type=object_type,
            id_pattern=id_pattern.replace("*", "%") if id_pattern is not None else "%",
        )
        return cast(list[tuple[str, str]], self._execute_read(engine_name=engine_name, query=sql_query))

    def exists(self, key: NodeKey) -> bool:
        engine_name, schema_name = self.schema_resolver.for_node(key)
        sql_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_exists"],
            registry=schema_name,
            object_type=key.object_type,
            object_id=key.object_id,
        )
        result = self._execute_read(engine_name=engine_name, query=sql_query)
        return bool(result[0][0]) if result else False

    def exists_many(self, key_list: NodeKeyList | list[NodeKey]) -> list[bool]:
        keys = key_list.item_list if isinstance(key_list, NodeKeyList) else key_list
        return [self.exists(key) for key in keys]

    def get(self, key: NodeKey) -> Node | None:
        if not self.exists(key):
            self.msg.not_found(key)
            return None

        engine_name, schema_name = self.schema_resolver.for_node(key)

        basic_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_get_basic"],
            registry=schema_name,
            object_type=key.object_type,
            object_id=key.object_id,
        )
        basic_data = cast(list[tuple[Any, ...]], self._execute_read(engine_name=engine_name, query=basic_query))
        basic_row = basic_data[0] if basic_data else None
        if basic_row is None:
            self.msg.not_found(key)
            return None

        custom_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_get_custom"],
            registry=schema_name,
            object_type=key.object_type,
            object_id=key.object_id,
        )
        custom_fields = cast(list[tuple[str, str, Any]], self._execute_read(engine_name=engine_name, query=custom_query))

        profile_query = resolve_sql_query(
            file_path=sql_queries_paths["registry"]["commit"]["node_get_profile"],
            registry=schema_name,
            object_type=key.object_type,
            object_id=key.object_id,
        )
        page_profile = self._execute_read(engine_name=engine_name, query=profile_query)
        page_profile_dict = dict(zip(PAGE_PROFILE_COLUMNS, page_profile[0])) if page_profile else {}

        concepts: dict[ConceptMapType, list[tuple[str, float]]] = {
            "detected": [],
            "ai_validated": [],
            "manually_mapped": [],
        }
        for map_type in get_args(ConceptMapType):
            if map_type == "detected":
                continue
            sql_query = resolve_sql_query(
                file_path=sql_queries_paths["registry"]["commit"][f"node_get_concepts_{map_type}"],
                registry=schema_name,
                object_type=key.object_type,
                object_id=key.object_id,
            )
            concepts[map_type] = cast(list[tuple[str, float]], self._execute_read(engine_name=engine_name, query=sql_query))

        return MySQLNodeMapper.from_parts(
            key=key,
            basic_row=basic_row,
            custom_field_rows=custom_fields,
            page_profile_row=page_profile_dict,
            detected_concept_rows=concepts["detected"],
            ai_validated_concept_rows=concepts["ai_validated"],
            manually_mapped_rows=concepts["manually_mapped"],
        )

    def get_many(self, key_list: NodeKeyList | list[NodeKey]) -> NodeList:
        keys = key_list.item_list if isinstance(key_list, NodeKeyList) else key_list
        out = [node for node in (self.get(key) for key in keys) if node is not None]
        return NodeList(item_list=out)

    # ---------------------------------------------------------------------- #
    # Save helpers                                                           #
    # ---------------------------------------------------------------------- #

    def _persist_node(self, session: MySQLSession, schema_name: str, node: Node) -> None:
        """Write one node inside an already-open session."""
        key = node.key
        table_path = self._qt(schema_name, "Nodes_N_Object")

        basic_row = MySQLNodeMapper.to_basic_row(node)
        self._upsert_rows(
            session=session,
            table_path=table_path,
            key_column_names=["object_type", "object_id"],
            upd_column_names=list(basic_row.keys()),
            rows=[{"object_type": key.object_type, "object_id": key.object_id, **basic_row}],
        )

        # Soft-delete existing custom fields so the domain field_list is authoritative.
        self._soft_delete_by_keys(
            session=session,
            schema_name=schema_name,
            table_name="Data_N_Object_T_CustomFields",
            keys=[key],
        )

        custom_rows = MySQLNodeMapper.to_custom_field_rows(node)
        if custom_rows:
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, "Data_N_Object_T_CustomFields"),
                key_column_names=["object_type", "object_id", "field_language", "field_name"],
                upd_column_names=["field_value", "record_deleted"],
                rows=[{**row, "record_deleted": 0} for row in custom_rows],
            )

        page_profile_row = MySQLNodeMapper.to_page_profile_row(node)
        self._upsert_rows(
            session=session,
            table_path=self._qt(schema_name, "Data_N_Object_T_PageProfile"),
            key_column_names=["object_type", "object_id"],
            upd_column_names=list(page_profile_row.keys()),
            rows=[{"object_type": key.object_type, "object_id": key.object_id, **page_profile_row}],
        )

        for map_type, table_name in zip(get_args(ConceptMapType), self._CONCEPT_TABLE_NAMES.values(), strict=True):
            if map_type == "detected":
                continue
            concept_rows = MySQLNodeMapper.to_scored_concepts_rows(node, map_to=map_type)
            if not concept_rows:
                continue
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, table_name),
                key_column_names=["object_type", "object_id", "concept_id", "text_source"],
                upd_column_names=["score", "record_deleted"],
                rows=[{**row, "record_deleted": 0} for row in concept_rows],
            )

    def _persist_node_group(
        self,
        session: MySQLSession,
        schema_name: str,
        nodes: list[Node],
    ) -> None:
        """Write a group of nodes that share one schema in a batched fashion."""
        if not nodes:
            return

        # ------------------------------------------------------------------ #
        # Basic rows                                                         #
        # ------------------------------------------------------------------ #
        basic_rows: list[dict[str, Any]] = []
        for node in nodes:
            basic_row = MySQLNodeMapper.to_basic_row(node)
            basic_rows.append({
                "object_type": node.key.object_type,
                "object_id": node.key.object_id,
                **basic_row,
            })

        self._upsert_rows(
            session=session,
            table_path=self._qt(schema_name, "Nodes_N_Object"),
            key_column_names=["object_type", "object_id"],
            upd_column_names=list(MySQLNodeMapper.to_basic_row(nodes[0]).keys()),
            rows=basic_rows,
        )

        # ------------------------------------------------------------------ #
        # Custom fields: delete old, insert new                              #
        # ------------------------------------------------------------------ #
        self._soft_delete_by_keys(
            session=session,
            schema_name=schema_name,
            table_name="Data_N_Object_T_CustomFields",
            keys=[node.key for node in nodes],
        )

        custom_field_rows: list[dict[str, Any]] = []
        for node in nodes:
            for row in MySQLNodeMapper.to_custom_field_rows(node):
                custom_field_rows.append({**row, "record_deleted": 0})
        if custom_field_rows:
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, "Data_N_Object_T_CustomFields"),
                key_column_names=["object_type", "object_id", "field_language", "field_name"],
                upd_column_names=["field_value", "record_deleted"],
                rows=custom_field_rows,
            )

        # ------------------------------------------------------------------ #
        # Page profiles                                                      #
        # ------------------------------------------------------------------ #
        page_profile_rows: list[dict[str, Any]] = []
        page_profile_cols: set[str] = set()
        for node in nodes:
            if node.page_profile is None:
                continue
            row = MySQLNodeMapper.to_page_profile_row(node)
            page_profile_cols.update(row.keys())
            page_profile_rows.append({
                "object_type": node.key.object_type,
                "object_id": node.key.object_id,
                **row,
            })
        if page_profile_rows:
            # Normalize every row to the union of columns; missing values become
            # None so the multi-row INSERT can use a single shape.
            sorted_cols = sorted(page_profile_cols)
            normalized_rows = [
                {"object_type": row["object_type"], "object_id": row["object_id"], **{col: row.get(col) for col in sorted_cols}}
                for row in page_profile_rows
            ]
            self._upsert_rows(
                session=session,
                table_path=self._qt(schema_name, "Data_N_Object_T_PageProfile"),
                key_column_names=["object_type", "object_id"],
                upd_column_names=sorted_cols,
                rows=normalized_rows,
            )

        # ------------------------------------------------------------------ #
        # Concept edges                                                      #
        # ------------------------------------------------------------------ #
        for map_type, table_name in zip(get_args(ConceptMapType), self._CONCEPT_TABLE_NAMES.values(), strict=True):
            if map_type == "detected":
                continue
            concept_rows: list[dict[str, Any]] = []
            for node in nodes:
                for row in MySQLNodeMapper.to_scored_concepts_rows(node, map_to=map_type):
                    concept_rows.append({**row, "record_deleted": 0})
            if concept_rows:
                self._upsert_rows(
                    session=session,
                    table_path=self._qt(schema_name, table_name),
                    key_column_names=["object_type", "object_id", "concept_id", "text_source"],
                    upd_column_names=["score", "record_deleted"],
                    rows=concept_rows,
                )

    # ---------------------------------------------------------------------- #
    # Save / Save many                                                       #
    # ---------------------------------------------------------------------- #

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

    # ---------------------------------------------------------------------- #
    # Delete / Delete many                                                   #
    # ---------------------------------------------------------------------- #

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

    # ---------------------------------------------------------------------- #
    # Node diagnostics and special get/save operations                       #
    # ---------------------------------------------------------------------- #

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

    # ---------------------------------------------------------------------- #
    # Retry-aware persistence                                                #
    # ---------------------------------------------------------------------- #

    # TODO: add retry with exponential back-off around session-level batch
    # operations for transient lock-wait timeouts (DBAPI code 1205). The retry
    # boundary must be the whole unit of work, not an individual statement,
    # because MySQL rolls back the current transaction on lock wait timeout.
