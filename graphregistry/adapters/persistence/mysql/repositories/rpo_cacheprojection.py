# graphregistry/adapters/persistence/mysql/repositories/rpo_cacheprojection.py
from __future__ import annotations
from contextlib import contextmanager
import re
from typing import TYPE_CHECKING, Iterable
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_cacheprojection import CacheProjectionRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import GlobalConfig, IndexConfig
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

#==================#
# Class Definition #
#==================#
class MySQLCacheProjectionRepository(CacheProjectionRepository):
    """MySQL adapter for the CacheProjectionRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.Orchestration.propagate command and the cache and traversals
    options of its reset command. Where the legacy re-queried the typeflags
    tables for the active types once per section, the adapter consumes the
    processing scope built by the caller, which is equivalent and cheaper.

    One deliberate deviation: the legacy per-type batches of the scores
    matrices substituted the __TYPE_FILTER__ placeholder after it had already
    been replaced by the IN-list, so every batch ran the identical IN-list
    update. The intended per-type batching (documented in the legacy trace
    messages) is implemented here; the resulting row state is identical.
    """

    # Public Method: Initialize the repository with an injected database client,
    # schema resolver, and configurations; no module-level connections.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, global_config: GlobalConfig, index_config: IndexConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.global_config = global_config
        self.index_config = index_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Propagation                                      #
    #================================================================#

    # Public Method: Propagate dirty flags from the airflow tracking tables to
    # the cache projections selected by the processing scope, mirroring the
    # legacy propagate command.
    def propagate(self, scope: ProcessingScope, include_fields: bool = True, include_scores: bool = True, actions: ActionSet = ('commit',)) -> list[PropagationStats]:
        actions = tuple(actions)
        stats = []
        if include_fields:
            stats.extend(self._propagate_page_profile_and_degree_scores(scope, actions))
            stats.extend(self._propagate_parent_child(scope, actions))
            stats.extend(self._propagate_index_buildup_docs(scope, actions))
            stats.extend(self._propagate_index_buildup_links(scope, actions))
        if include_scores:
            stats.extend(self._propagate_scores_matrices(scope, actions))
            stats.extend(self._propagate_final_scores(scope, actions))
        return stats

    # Internal Method: Propagate to the page profile and degree scores tables,
    # which are driven by the scores-expired flags of the active score types.
    def _propagate_page_profile_and_degree_scores(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Skip the section entirely when no scores types are active.
        if len(scope.node_scores_types) == 0:
            sysmsg.trace("  ~ No active scores type flags; skipping page profile / degree scores tables.")
            return []
        active_scores_types = self._sql_type_list(scope.node_scores_types)
        stats = []
        for table_name in ("Data_N_Object_T_PageProfile", "Nodes_N_Object_T_DegreeScores"):
            query_update = f"""
                            UPDATE {cache_schema}.{table_name} p
                               SET p.to_process = 1
                             WHERE p.object_type IN ({active_scores_types})
                               AND (p.object_type, p.object_id) IN (
                                   SELECT se.object_type, se.object_id
                                     FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired se
                                    WHERE se.to_process = 1
                                      AND se.deleted = 0
                               )
                               AND  p.to_process = 0;
                            """
            query_eval = f"""SELECT COUNT(*)
                           FROM {cache_schema}.{table_name} p
                          WHERE p.object_type IN ({active_scores_types})
                            AND (p.object_type, p.object_id) IN (
                                SELECT se.object_type, se.object_id
                                  FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired se
                                 WHERE se.to_process = 1
                                   AND se.deleted = 0
                            )
                            AND  p.to_process = 0;
                            """
            stats.extend(self._execute_propagate_query(
                query_update      = query_update,
                query_eval        = query_eval,
                schema_name       = cache_schema,
                table_name        = table_name,
                query_id          = 'zv9J4K0r',
                actions           = actions,
                batch_types       = scope.node_scores_types,
                batch_type_column = 'p.object_type',
            ))
        return stats

    # Internal Method: Propagate to the symmetric parent-child edge table in
    # both endpoint directions, driven by the fields-changed edge flags.
    def _propagate_parent_child(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Skip the section entirely when no edge families are active.
        if len(scope.edge_type_pairs) == 0:
            sysmsg.trace("  ~ No active edge type flags; skipping parent-child tables.")
            return []
        active_edge_types = self._sql_pairs_list(scope.edge_type_pairs)
        batch_pairs = self._expanded_pairs(scope.edge_type_pairs)
        stats = []
        for table_name in ("Edges_N_Object_N_Object_T_ParentChildSymmetric",):
            for d1, d2 in (('from', 'to'), ('to', 'from')):
                query_update = f"""
                            UPDATE {cache_schema}.{table_name} p
                               SET p.to_process = 1
                             WHERE (p.from_object_type, p.to_object_type) IN ({active_edge_types})
                               AND (p.{d1}_object_type, p.{d1}_object_id, p.{d2}_object_type, p.{d2}_object_id) IN (
                                   SELECT fc.from_object_type, fc.from_object_id, fc.to_object_type, fc.to_object_id
                                     FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged fc
                                    WHERE fc.to_process = 1
                                      AND fc.deleted = 0
                               )
                               AND  p.to_process = 0;
                            """
                query_eval = f"""SELECT COUNT(*)
                           FROM {cache_schema}.{table_name} p
                          WHERE (p.from_object_type, p.to_object_type) IN ({active_edge_types})
                            AND (p.{d1}_object_type, p.{d1}_object_id, p.{d2}_object_type, p.{d2}_object_id) IN (
                                SELECT fc.from_object_type, fc.from_object_id, fc.to_object_type, fc.to_object_id
                                  FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged fc
                                 WHERE fc.to_process = 1
                                   AND fc.deleted = 0
                            )
                            AND  p.to_process = 0;
                            """
                stats.extend(self._execute_propagate_query(
                    query_update       = query_update,
                    query_eval         = query_eval,
                    schema_name        = cache_schema,
                    table_name         = table_name,
                    query_id           = 'ct6y8Gz2',
                    actions            = actions,
                    batch_pairs        = batch_pairs,
                    batch_from_column  = 'p.from_object_type',
                    batch_to_column    = 'p.to_object_type',
                ))
        return stats

    # Internal Method: Propagate to the index buildup doc tables, driven by the
    # fields-changed flags of the active node types.
    def _propagate_index_buildup_docs(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Skip the section entirely when no fields types are active.
        if len(scope.node_fields_types) == 0:
            sysmsg.trace("  ~ No active fields type flags; skipping IndexBuildup Doc tables.")
            return []
        active_fields_types = self._sql_type_list(scope.node_fields_types)
        stats = []
        for doc_type in self.index_config.settings.get("doc_types", []):
            table_name = f"IndexBuildup_Fields_Docs_{doc_type}"

            # Skip doc types whose buildup table does not exist (yet).
            if not self.db.table_exists(engine_name=engine_name, schema_name=cache_schema, table_name=table_name):
                sysmsg.trace(f"  ~ Skipping missing table: {table_name}")
                continue
            query_update = f"""
                            UPDATE {cache_schema}.{table_name} p
                               SET p.to_process = 1
                             WHERE p.doc_type IN ({active_fields_types})
                               AND (p.doc_type, p.doc_id) IN (
                                   SELECT fc.object_type, fc.object_id
                                     FROM {airflow_schema}.Operations_N_Object_T_FieldsChanged fc
                                    WHERE fc.to_process = 1
                                      AND fc.deleted = 0
                               )
                               AND  p.to_process = 0;
                            """
            query_eval = f"""SELECT COUNT(*)
                           FROM {cache_schema}.{table_name} p
                          WHERE p.doc_type IN ({active_fields_types})
                            AND (p.doc_type, p.doc_id) IN (
                                SELECT fc.object_type, fc.object_id
                                  FROM {airflow_schema}.Operations_N_Object_T_FieldsChanged fc
                                 WHERE fc.to_process = 1
                                   AND fc.deleted = 0
                            )
                            AND  p.to_process = 0;
                            """
            stats.extend(self._execute_propagate_query(
                query_update      = query_update,
                query_eval        = query_eval,
                schema_name       = cache_schema,
                table_name        = table_name,
                query_id          = 'RjDjz3fW',
                actions           = actions,
                batch_types       = scope.node_fields_types,
                batch_type_column = 'p.doc_type',
            ))
        return stats

    # Internal Method: Propagate to the index buildup doc-link tables, driven
    # by the fields-changed flags of the active edge families.
    def _propagate_index_buildup_links(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Skip the section entirely when no edge families are active.
        if len(scope.edge_type_pairs) == 0:
            sysmsg.trace("  ~ No active edge type flags; skipping IndexBuildup Doc-Link tables.")
            return []
        active_doclink_types = self._sql_pairs_list(scope.edge_type_pairs)
        batch_pairs = self._expanded_pairs(scope.edge_type_pairs)

        # Derive the parent-child doc-link pairs from the index configuration,
        # deduplicated as sorted tuples and sorted for deterministic order.
        parent_child = self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("links", {}).get("parent_child", {})
        p2c_doclink_types = sorted({
            tuple(sorted([doc_type, link_type]))
            for doc_type in parent_child
            for link_type in parent_child[doc_type]
        })
        stats = []
        for source_doc_type, target_doc_type in p2c_doclink_types:
            table_name = f"IndexBuildup_Fields_Links_ParentChild_{source_doc_type}_{target_doc_type}"

            # Skip doc-link types whose buildup table does not exist (yet).
            if not self.db.table_exists(engine_name=engine_name, schema_name=cache_schema, table_name=table_name):
                sysmsg.trace(f"  ~ Skipping missing table: {table_name}")
                continue
            query_update = f"""
                            UPDATE {cache_schema}.{table_name} p
                               SET p.to_process = 1
                             WHERE (p.doc_type, p.link_type) IN ({active_doclink_types})
                               AND (p.doc_type, p.doc_id, p.link_type, p.link_id) IN (
                                   SELECT fc.from_object_type, fc.from_object_id, fc.to_object_type, fc.to_object_id
                                     FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged fc
                                    WHERE fc.to_process = 1
                                      AND fc.deleted = 0
                               )
                               AND  p.to_process = 0;
                            """
            query_eval = f"""SELECT COUNT(*)
                           FROM {cache_schema}.{table_name} p
                          WHERE (p.doc_type, p.link_type) IN ({active_doclink_types})
                            AND (p.doc_type, p.doc_id, p.link_type, p.link_id) IN (
                                SELECT fc.from_object_type, fc.from_object_id, fc.to_object_type, fc.to_object_id
                                  FROM {airflow_schema}.Operations_N_Object_N_Object_T_FieldsChanged fc
                                 WHERE fc.to_process = 1
                                   AND fc.deleted = 0
                            )
                            AND  p.to_process = 0;
                            """
            stats.extend(self._execute_propagate_query(
                query_update      = query_update,
                query_eval        = query_eval,
                schema_name       = cache_schema,
                table_name        = table_name,
                query_id          = 'J4Djz3fW',
                actions           = actions,
                batch_pairs       = batch_pairs,
                batch_from_column = 'p.doc_type',
                batch_to_column   = 'p.link_type',
            ))
        return stats

    # Internal Method: Propagate to the adjusted-scores matrices, driven by the
    # scores-expired flags, through a scratch table of the expired nodes.
    def _propagate_scores_matrices(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Discover the adjusted-scores matrix tables of the cache schema.
        list_of_tables = sorted([
            table_name for table_name in self.db.get_tables_in_schema(
                engine_name = engine_name,
                schema_name = cache_schema,
                use_regex   = [r'^Edges_N_Object_N_Object_T_ScoresMatrix_.*_AS$'],
            )
            if not table_name.startswith('_')
        ])

        # Skip the section entirely when no scores types are active.
        if len(scope.node_scores_types) == 0:
            sysmsg.trace("  ~ No active scores type flags; skipping score matrix tables.")
            return []
        active_scores_in_list = self._sql_type_list(scope.node_scores_types)

        # Materialize the expired score nodes into a scratch table so each
        # matrix update does not re-scan the airflow table.
        temp_table_path = f"{cache_schema}._tmp_prop_scores_expired"
        temp_table_create = f"""
                    DROP TABLE IF EXISTS {temp_table_path};
                    CREATE TABLE {temp_table_path} (
                        object_type VARCHAR(255) NOT NULL,
                        object_id   VARCHAR(255) NOT NULL,
                        PRIMARY KEY (object_type, object_id)
                    ) AS
                    SELECT se.object_type, se.object_id
                      FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired se
                     WHERE se.to_process = 1
                       AND se.deleted = 0;
                    """
        temp_table_drop = f"DROP TABLE IF EXISTS {temp_table_path};"
        if 'eval' in actions or 'commit' in actions:
            if 'print' in actions:
                print_sql(temp_table_create, title='PropScoresTmpCreate')
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = temp_table_create,
                verbose     = self.verbose,
                query_id    = 'PropScoresTmpCreate',
            )

        # Propagate each matrix table in both endpoint directions.
        stats = []
        for table_name in list_of_tables:

            # FROM-side template with a placeholder so the per-type batches of
            # the commit path can substitute a fixed type predicate.
            where_from_template = f"""WHERE p.from_object_type __TYPE_FILTER__
                                AND (p.from_object_type, p.from_object_id) IN (
                                    SELECT object_type, object_id
                                      FROM {temp_table_path}
                                )
                                AND  p.to_process = 0"""
            where_from = where_from_template.replace('__TYPE_FILTER__', f"IN ({active_scores_in_list})")
            query_update_from = f"""UPDATE {cache_schema}.{table_name} p
                           SET  p.to_process = 1
                         {where_from};
                    """
            query_eval_from = f"""SELECT 1
                         FROM {cache_schema}.{table_name} p
                      {where_from}"""

            # TO-side template, mirroring the from-side for the other endpoint.
            where_to_template = f"""WHERE p.to_object_type __TYPE_FILTER__
                              AND (p.to_object_type, p.to_object_id) IN (
                                  SELECT object_type, object_id
                                    FROM {temp_table_path}
                              )
                              AND  p.to_process = 0"""
            where_to = where_to_template.replace('__TYPE_FILTER__', f"IN ({active_scores_in_list})")
            query_update_to = f"""UPDATE {cache_schema}.{table_name} p
                           SET  p.to_process = 1
                         {where_to};
                    """
            query_eval_to = f"""SELECT 1
                     FROM {cache_schema}.{table_name} p
                  {where_to}"""

            # Eval: union both directions to avoid double-counting edges whose
            # from- and to- endpoints are both expired.
            if 'eval' in actions:
                query_eval = f"""SELECT COUNT(*)
                           FROM (
                                {query_eval_from}
                                UNION
                                {query_eval_to}
                           ) t"""
                rows = self.db.execute_query(engine_name=engine_name, query=query_eval, query_id='yzm93BqQ')
                count = rows[0][0] if rows and len(rows) > 0 and rows[0] else 0
                sysmsg.trace(f"  ~ {count} rows would be flagged")
                stats.append(PropagationStats(target=f"{cache_schema}.{table_name}", rows_flagged=count))

            # Commit: run per-type batch updates for both directions, with the
            # fixed type predicate substituted into the placeholder.
            if 'commit' in actions:
                for object_type in scope.node_scores_types:
                    type_filter = f"= '{self._sql_escape(object_type)}'"
                    sysmsg.trace(f"  ~ {table_name}: from-side batch for {object_type}")
                    q_from = where_from_template.replace('__TYPE_FILTER__', type_filter)
                    query_from = f"""UPDATE {cache_schema}.{table_name} p
                           SET  p.to_process = 1
                         {q_from};
                    """
                    if 'print' in actions:
                        print_sql(query_from, title=f'yzm93BqQ-from-{object_type}[commit]')
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = query_from,
                        verbose     = self.verbose,
                        query_id    = f'yzm93BqQ-from-{object_type}',
                    )
                    sysmsg.trace(f"  ~ {table_name}: to-side batch for {object_type}")
                    q_to = where_to_template.replace('__TYPE_FILTER__', type_filter)
                    query_to = f"""UPDATE {cache_schema}.{table_name} p
                           SET  p.to_process = 1
                         {q_to};
                    """
                    if 'print' in actions:
                        print_sql(query_to, title=f'yzm93BqQ-to-{object_type}[commit]')
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = query_to,
                        verbose     = self.verbose,
                        query_id    = f'yzm93BqQ-to-{object_type}',
                    )
                stats.append(PropagationStats(target=f"{cache_schema}.{table_name}", rows_flagged=0))

        # Drop the scratch table of expired score nodes.
        if 'eval' in actions or 'commit' in actions:
            if 'print' in actions:
                print_sql(temp_table_drop, title='PropScoresTmpDrop')
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = temp_table_drop,
                verbose     = self.verbose,
                query_id    = 'PropScoresTmpDrop',
            )
        return stats

    # Internal Method: Propagate to the final scores tables, driven by the
    # scores-expired flags of the active score types.
    def _propagate_final_scores(self, scope: ProcessingScope, actions: ActionSet) -> list[PropagationStats]:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Discover the final scores tables of the cache schema.
        list_of_tables = sorted([
            table_name for table_name in self.db.get_tables_in_schema(
                engine_name = engine_name,
                schema_name = cache_schema,
                use_regex   = [r'^Edges_N_Object_N_.*_T_FinalScores$'],
            )
            if not table_name.startswith('_')
        ])

        # Skip the section entirely when no scores types are active.
        if len(scope.node_scores_types) == 0:
            sysmsg.trace("  ~ No active scores type flags; skipping final scores tables.")
            return []
        active_scores_types = self._sql_type_list(scope.node_scores_types)
        stats = []
        for table_name in list_of_tables:
            query_update = f"""
                            UPDATE {cache_schema}.{table_name} p
                               SET p.to_process = 1
                             WHERE p.object_type IN ({active_scores_types})
                               AND (p.object_type, p.object_id) IN (
                                   SELECT se.object_type, se.object_id
                                     FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired se
                                    WHERE se.to_process = 1
                                      AND se.deleted = 0
                               )
                               AND  p.to_process = 0;
                            """
            query_eval = f"""SELECT COUNT(*)
                           FROM {cache_schema}.{table_name} p
                          WHERE p.object_type IN ({active_scores_types})
                            AND (p.object_type, p.object_id) IN (
                                SELECT se.object_type, se.object_id
                                  FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired se
                                 WHERE se.to_process = 1
                                   AND se.deleted = 0
                            )
                            AND  p.to_process = 0;
                            """
            stats.extend(self._execute_propagate_query(
                query_update      = query_update,
                query_eval        = query_eval,
                schema_name       = cache_schema,
                table_name        = table_name,
                query_id          = 'q7n3P9xY',
                actions           = actions,
                batch_types       = scope.node_scores_types,
                batch_type_column = 'p.object_type',
            ))
        return stats

    #================================================================#
    # Method Group: Resets                                           #
    #================================================================#

    # Public Method: Clear the to_process flags of the cache and traversals
    # projections, mirroring the cache and traversals options of the legacy
    # reset command.
    def reset_flags(self, include_cache: bool = True, include_traversals: bool = True, actions: ActionSet = ('commit',)) -> None:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()

        # Reset the to_process flags of every flagged table of the cache schema.
        if include_cache:
            _, cache_schema = self.schema_resolver.for_graph_cache()
            sysmsg.info("🧹 📝 Reset 'to_process' flags in graph_cache tables.")
            list_of_tables = sorted([
                table_name for table_name in self.db.get_tables_in_schema(engine_name=engine_name, schema_name=cache_schema)
                if not table_name.startswith('_')
                and self.db.has_column(engine_name=engine_name, schema_name=cache_schema, table_name=table_name, column_name='to_process')
            ])
            for table_name in list_of_tables:
                reset_query = f"UPDATE {cache_schema}.{table_name} SET to_process = 0 WHERE to_process = 1;"
                if 'print' in actions:
                    print_sql(reset_query, title='DFEkXX4A')
                if 'commit' in actions:
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = reset_query,
                        verbose     = self.verbose,
                        query_id    = 'DFEkXX4A',
                    )
            sysmsg.success(f"🧹 ✅ Done resetting 'to_process' flags in '{cache_schema}' tables.")

        # Reset the to_process flags of every flagged table of the traversals
        # schema, which feeds the traversal formulas.
        if include_traversals:
            traversals_schema = self.global_config.schema_traversals
            sysmsg.info("🧹 📝 Reset 'to_process' flags in traversals tables.")
            list_of_tables = sorted([
                table_name for table_name in self.db.get_tables_in_schema(engine_name=engine_name, schema_name=traversals_schema)
                if not table_name.startswith('_')
                and self.db.has_column(engine_name=engine_name, schema_name=traversals_schema, table_name=table_name, column_name='to_process')
            ])
            for table_name in list_of_tables:
                reset_query = f"UPDATE {traversals_schema}.{table_name} SET to_process = 0 WHERE to_process = 1;"
                if 'print' in actions:
                    print_sql(reset_query, title='X7vYqZ3A')
                if 'commit' in actions:
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = reset_query,
                        verbose     = self.verbose,
                        query_id    = 'X7vYqZ3A',
                    )
            sysmsg.success(f"🧹 ✅ Done resetting 'to_process' flags in '{traversals_schema}' tables.")

    #================================================================#
    # Method Group: Propagation execution helpers                    #
    #================================================================#

    # Internal Method: Execute one propagate update/eval step: evaluate the
    # affected row count, then commit either through chunked updates when the
    # table carries a row_id column, or through per-type or per-pair batches.
    def _execute_propagate_query(
        self,
        query_update: str,
        query_eval: str,
        schema_name: str,
        table_name: str,
        query_id: str,
        actions: ActionSet,
        batch_types: list[str] | None = None,
        batch_type_column: str = "p.object_type",
        batch_pairs: list[tuple[str, str]] | None = None,
        batch_from_column: str = "p.from_object_type",
        batch_to_column: str = "p.to_object_type",
    ) -> list[PropagationStats]:
        stats = []
        engine_name = self.schema_resolver.for_airflow()[0]

        # Eval: run the count query once and report the would-be flaggings.
        if 'eval' in actions:
            rows = self.db.execute_query(engine_name=engine_name, query=query_eval, query_id=query_id)
            count = rows[0][0] if rows and len(rows) > 0 and rows[0] else 0
            sysmsg.trace(f"  ~ {count} rows would be flagged")
            stats.append(PropagationStats(target=f"{schema_name}.{table_name}", rows_flagged=count))

        # Commit: chunked updates when row_id exists, per-type batches otherwise.
        if 'commit' not in actions:
            return stats
        has_row_id = self._table_has_row_id(engine_name, schema_name, table_name)

        # Tables carrying a row_id column support dense chunked updates; the
        # others fall back to per-type or per-pair batches.
        if has_row_id:

            # Build a dense chunk filter that selects exactly the row_ids the
            # update would touch: convert the UPDATE into a SELECT p.row_id
            # subquery so all join and where conditions are preserved.
            select_for_chunk = re.sub(
                r'UPDATE\s+(\S+\.\S+)\s+p\s+',
                r'SELECT p.row_id FROM \1 p ',
                query_update,
                count=1,
                flags=re.IGNORECASE,
            )
            select_for_chunk = re.sub(
                r'\s+SET\s+p\.to_process\s*=\s*1\s*',
                ' ',
                select_for_chunk,
                count=1,
                flags=re.IGNORECASE,
            )
            if 'SELECT P.ROW_ID FROM' in select_for_chunk.upper():
                select_for_chunk = select_for_chunk.rstrip().rstrip(';')

                # Use unqualified row_id at the CTE level; the subquery itself
                # aliases the table as p and is fine.
                chunk_filter = f"row_id IN ({select_for_chunk})"
            else:

                # Fallback: scan the whole table by row_id ranges.
                chunk_filter = None
            if 'print' in actions:
                print_sql(query_update, title=f'{query_id}[commit]')
            with self._quiet_chunk_discovery(actions):
                self.db.execute_query_in_chunks(
                    engine_name    = engine_name,
                    schema_name    = schema_name,
                    table_name     = table_name,
                    query          = query_update,
                    chunk_filter   = chunk_filter,
                    row_id_name    = 'p.row_id',
                    chunk_size     = 100000,
                    show_progress  = True,
                    verbose        = self.verbose,
                    query_id       = query_id,
                    desc           = f"{schema_name}.{table_name}",
                )
            stats.append(PropagationStats(target=f"{schema_name}.{table_name}", rows_flagged=0))
        else:

            # No row_id column: batch by the active object types or edge pairs.
            if batch_pairs:
                for from_type, to_type in batch_pairs:
                    q = re.sub(
                        r'\bWHERE\b',
                        f"WHERE {batch_from_column} = '{self._sql_escape(from_type)}' AND {batch_to_column} = '{self._sql_escape(to_type)}' AND ",
                        query_update,
                        count=1,
                        flags=re.IGNORECASE,
                    )
                    if 'print' in actions:
                        print_sql(q, title=f'{query_id}-{from_type}-{to_type}[commit]')
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = q,
                        verbose     = self.verbose,
                        query_id    = f'{query_id}-{from_type}-{to_type}',
                    )
            elif batch_types:
                for object_type in batch_types:
                    q = re.sub(
                        r'\bWHERE\b',
                        f"WHERE {batch_type_column} = '{self._sql_escape(object_type)}' AND ",
                        query_update,
                        count=1,
                        flags=re.IGNORECASE,
                    )
                    if 'print' in actions:
                        print_sql(q, title=f'{query_id}-{object_type}[commit]')
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = q,
                        verbose     = self.verbose,
                        query_id    = f'{query_id}-{object_type}',
                    )
            stats.append(PropagationStats(target=f"{schema_name}.{table_name}", rows_flagged=0))
        return stats

    # Internal Method: Check whether a table carries a row_id column through
    # the information schema.
    def _table_has_row_id(self, engine_name: str, schema_name: str, table_name: str) -> bool:
        query = f"""SELECT 1
                      FROM information_schema.COLUMNS
                     WHERE TABLE_SCHEMA = '{self._sql_escape(schema_name)}'
                       AND TABLE_NAME = '{self._sql_escape(table_name)}'
                       AND COLUMN_NAME = 'row_id'"""
        rows = self.db.execute_query(engine_name=engine_name, query=query, query_id='has-row-id')
        return bool(rows)

    # Internal Method: Suppress the generic chunk-boundary discovery log of
    # graphdb unless the print action was requested.
    @contextmanager
    def _quiet_chunk_discovery(self, actions: ActionSet):
        if 'print' in actions:
            yield
            return
        original_info = sysmsg.info

        # Internal Function: Forward a log message unless it is the chunk
        # boundary discovery notice of graphdb.
        def _filtered_info(message, *args, **kwargs):
            if isinstance(message, str) and message.startswith('Discovering chunk boundaries for'):
                return
            return original_info(message, *args, **kwargs)
        sysmsg.info = _filtered_info
        try:
            yield
        finally:
            sysmsg.info = original_info

    #================================================================#
    # Method Group: SQL rendering helpers                            #
    #================================================================#

    # Internal Method: Render a list of object types as a SQL IN-list, using
    # the repr formatting of the legacy where-condition generators.
    @staticmethod
    def _sql_type_list(types: Iterable[str]) -> str:
        return ", ".join(repr(t) for t in types)

    # Internal Method: Render the active edge pairs as a SQL IN-list covering
    # both stored directions of each undirected family.
    @staticmethod
    def _sql_pairs_list(pairs: Iterable[EdgeTypePair]) -> str:
        expanded = []
        for pair in pairs:
            canonical = pair.canonical_order
            expanded.append(canonical.as_tuple)
            expanded.append((canonical.to_object_type, canonical.from_object_type))
        return ", ".join(repr(t) for t in expanded)

    # Internal Method: Expand the canonical edge pairs into both stored
    # directions for the per-pair batches of the no-row_id fallback.
    @staticmethod
    def _expanded_pairs(pairs: Iterable[EdgeTypePair]) -> list[tuple[str, str]]:
        expanded = []
        for pair in pairs:
            canonical = pair.canonical_order
            expanded.append(canonical.as_tuple)
            expanded.append((canonical.to_object_type, canonical.from_object_type))
        return expanded

    # Internal Method: Escape a value for inline SQL, as the legacy propagate
    # command does; object types are short identifiers so single-quote doubling
    # is sufficient.
    @staticmethod
    def _sql_escape(value) -> str:
        if value is None:
            return 'NULL'
        return str(value).replace("'", "''")
