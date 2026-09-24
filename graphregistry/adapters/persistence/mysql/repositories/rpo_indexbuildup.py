# graphregistry/adapters/persistence/mysql/repositories/rpo_indexbuildup.py
from __future__ import annotations
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.adapters.persistence.mysql.repositories.helpers import create_table_if_not_exists
from graphregistry.application.ports.repositories.prt_indexbuildup import IndexBuildupRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import IndexConfig
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# Language suffixes appended to the field names of the raw field lists.
_LANGUAGE_SUFFIXES = {'n/a': '', 'en': '_en', 'fr': '_fr'}

#==================#
# Class Definition #
#==================#
class MySQLIndexBuildupRepository(IndexBuildupRepository):
    """MySQL adapter for the IndexBuildupRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.IndexDB.CacheBuildup class: the docs build (hpFZ8RAT eval,
    F1ArYGKd commit) and the parent-child links build (6D05nXQL eval,
    gEzB7UwD commit). Both builds create their target tables when missing —
    the legacy achieved this through constructor-time DDL in the IndexDocs
    and IndexDocLinks classes; the typed architecture does it lazily at
    first use instead.
    """

    # Public Method: Initialize the repository with an injected database
    # client, schema resolver, and index configuration.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, index_config: IndexConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.index_config = index_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Query helper construction                        #
    #================================================================#

    # Internal Method: Build the transposed query helper matrix of a raw
    # field list: the suffixed field names, the value selections, and the
    # per-field LEFT JOINs, as the legacy build commands do. Raw entries are
    # plain field names or [language, field] pairs.
    @staticmethod
    def _query_helpers(raw_fields: list, join_template: str) -> tuple[list[str], list[str], list[str]]:
        entries = [tuple(v) if type(v) is list else ('n/a', v) for v in raw_fields]
        field_names = []
        value_selects = []
        joins = []
        for k, (field_language, field_name) in enumerate(entries):
            suffix = _LANGUAGE_SUFFIXES[field_language]
            field_names.append(f"{field_name}{suffix}")
            value_selects.append(f"t{k+1}.field_value AS {field_name}{suffix}")
            joins.append(join_template.format(k=k + 1, field_language=field_language, field_name=field_name))
        return field_names, value_selects, joins

    #================================================================#
    # Method Group: Docs build                                       #
    #================================================================#

    # Public Method: Build the doc-fields staging table of one doc type from
    # the flagged page profiles and degree scores.
    def build_docs_fields(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Fetch the doc options and the raw custom field list.
        include_code_in_name = self.index_config.settings.get("options", {}).get("include_code_in_name", {}).get(doc_type, 0)
        raw_fields = self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("docs_raw", {}).get(doc_type, [])

        # Build the per-field LEFT JOINs against the all-fields table.
        join_template = (
            "      LEFT JOIN {cache_schema}.Data_N_Object_T_AllFields t{k} "
            "ON (t{k}.object_type, t{k}.object_id, t{k}.field_language, t{k}.field_name) "
            "= ('{doc_type}', p.object_id, '{field_language}', '{field_name}')\n"
            "             AND t{k}.deleted = 0"
        ).replace("{cache_schema}", cache_schema).replace("{doc_type}", doc_type)
        field_names, value_selects, joins = self._query_helpers(raw_fields, join_template)
        sql_slice_field_values = (", ".join(value_selects) + ", ") if value_selects else ""
        sql_slice_joins = "\n".join(joins)

        # Select the flagged page profiles with their degree scores and the
        # per-language custom fields.
        sql_query = f"""
                SELECT DISTINCT p.object_type AS doc_type, p.object_id AS doc_id,
                                {include_code_in_name} AS include_code_in_name,
                                {sql_slice_field_values}
                                COALESCE(d.avg_norm_log_degree, 0.001) AS degree_score,
                                1 AS to_process
                           FROM {cache_schema}.Data_N_Object_T_PageProfile p\n{sql_slice_joins}
                      LEFT JOIN {cache_schema}.Nodes_N_Object_T_DegreeScores d
                             ON (p.object_type, p.object_id) = (d.object_type, d.object_id)
                            AND d.deleted = 0
                          WHERE p.object_type = '{doc_type}'
                            AND p.to_process = 1
                            AND p.deleted = 0

                """
        target_table = f"IndexBuildup_Fields_Docs_{doc_type}"

        # Evaluate the row count per doc type when requested.
        # The legacy created the docs buildup tables in the IndexDocs
        # constructor at GraphRegistry construction time; the typed
        # architecture creates them lazily here instead, so a fresh schema
        # bootstraps correctly (E2E finding, 2026-09-23).
        create_table_if_not_exists(self.db, engine_name, cache_schema, target_table)

        # Evaluate the row count per doc type when requested.
        stats = PropagationStats(target=f"{cache_schema}.{target_table}", rows_flagged=0)
        if 'eval' in actions:
            sql_query_eval = f"SELECT doc_type, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY doc_type"
            if 'print' in actions:
                print_sql(sql_query_eval, title='hpFZ8RAT')
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='hpFZ8RAT')
            stats.rows_flagged = sum(row[1] for row in out) if out else 0

        # Commit by replacing the staging rows; the legacy docs build does
        # not create the target table when missing.
        if 'commit' in actions:
            target_table_columns = ['doc_type', 'doc_id', 'include_code_in_name'] + field_names + ['degree_score', 'to_process']
            if 'row_id' in target_table_columns:
                target_table_columns.remove('row_id')
            sql_query_commit = f"\tREPLACE INTO {cache_schema}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"
            if 'print' in actions:
                print_sql(sql_query_commit, title='F1ArYGKd')
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, verbose=('print' in actions), query_id='F1ArYGKd')
        return stats

    #================================================================#
    # Method Group: Links build                                      #
    #================================================================#

    # Public Method: Build the parent-child links staging table of one pair
    # from the flagged symmetric edges, resolving the canonical edge context.
    def build_links_parentchild(self, doc_type: str, link_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Fetch the raw custom field list of the pair, in the requested
        # direction, before canonicalising the pair.
        raw_fields = (
            self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("links", {})
            .get("parent_child_raw", {}).get(doc_type, {}).get(link_type, [])
        )
        doc_type, link_type = sorted([doc_type, link_type])

        # Resolve the canonical context of the pair; without a context there
        # is nothing to build.
        edge_context = self.index_config.settings.get("edge_selection_contexts", {}).get((doc_type, link_type))
        sysmsg.trace(
            "build_links_parentchild: doc_type='{}' link_type='{}' context='{}'",
            doc_type, link_type, edge_context,
        )
        if edge_context is None:
            sysmsg.warning(
                f"No edge context configured for '{doc_type} <-> {link_type}' "
                "in config/application/config_index.json object-selection.edges. Skipping."
            )
            return PropagationStats(target=f"{cache_schema}.IndexBuildup_Fields_Links_ParentChild_{doc_type}_{link_type}", rows_flagged=0)

        # Build the per-field LEFT JOINs against the symmetric all-fields
        # table, keyed by the pair endpoints.
        join_template = (
            "      LEFT JOIN {cache_schema}.Data_N_Object_N_Object_T_AllFieldsSymmetric t{k} "
            "ON (t{k}.from_object_type, t{k}.from_object_id, t{k}.to_object_type, t{k}.to_object_id, t{k}.field_language, t{k}.field_name) "
            "= ('{doc_type}', s.from_object_id, '{link_type}',   s.to_object_id, '{field_language}', '{field_name}')\n"
            "             AND t{k}.deleted = 0"
        ).replace("{cache_schema}", cache_schema).replace("{doc_type}", doc_type).replace("{link_type}", link_type)
        field_names, value_selects, joins = self._query_helpers(raw_fields, join_template)
        sql_slice_field_values = (", ".join(value_selects) + ", ") if value_selects else ""
        sql_slice_joins = "\n".join(joins)

        # Select the flagged symmetric edges with their custom fields.
        sql_query = f"""
                  SELECT s.from_object_type AS  doc_type, s.from_object_id AS doc_id,
                           s.to_object_type AS link_type, s.to_object_id AS link_id,
                         {sql_slice_field_values}
                         1 AS to_process
                     FROM {cache_schema}.Edges_N_Object_N_Object_T_ParentChildSymmetric s\n{sql_slice_joins}
                    WHERE (s.from_object_type, s.to_object_type) = ('{doc_type}', '{link_type}')
                      AND s.context = '{edge_context}'
                      AND s.to_process = 1
                      AND s.deleted = 0

                """
        target_table = f"IndexBuildup_Fields_Links_ParentChild_{doc_type}_{link_type}"

        # The links build creates its target table when missing.
        create_table_if_not_exists(self.db, engine_name, cache_schema, target_table)

        # Evaluate the row count per pair when requested.
        stats = PropagationStats(target=f"{cache_schema}.{target_table}", rows_flagged=0)
        if 'print' in actions:
            print_sql(sql_query, title='sfag24G')
        if 'eval' in actions:
            sql_query_eval = f"SELECT doc_type, link_type, COUNT(*) AS n_to_process FROM ({sql_query}) t GROUP BY doc_type, link_type"
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='6D05nXQL')
            stats.rows_flagged = sum(row[2] for row in out) if out else 0

        # Commit by replacing the staging rows.
        if 'commit' in actions:
            target_table_columns = ['doc_type', 'doc_id', 'link_type', 'link_id'] + field_names + ['to_process']
            if 'row_id' in target_table_columns:
                target_table_columns.remove('row_id')
            sql_query_commit = f"\tREPLACE INTO {cache_schema}.{target_table} ({', '.join(target_table_columns)})\n{sql_query}"
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, verbose=('print' in actions), query_id='gEzB7UwD')
        return stats
