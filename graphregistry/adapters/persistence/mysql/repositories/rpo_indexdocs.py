# graphregistry/adapters/persistence/mysql/repositories/rpo_indexdocs.py
from __future__ import annotations
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.adapters.persistence.mysql.repositories.helpers import create_table_if_not_exists
from graphregistry.application.ports.repositories.prt_indexdocs import IndexDocRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import IndexConfig
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

#==================#
# Class Definition #
#==================#
class MySQLIndexDocRepository(IndexDocRepository):
    """MySQL adapter for the IndexDocRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.IndexDB.IndexDocs class: the graphsearch patch
    (Rj0R4w2q evals, T4VTvBv6 commits), the Elasticsearch cache patch
    (4KpdVwsE evals, vdEk9bpn commits), the settle updates (42vKAJcy), and
    the flag cleanup (P9Caiq8w / yJ74cRvU). The patched field lists are
    loaded from the index configuration exactly as the legacy loads them.
    Snapshots and rollbacks remain unimplemented stubs in the legacy and are
    not ported.
    """

    # Public Method: Initialize the repository with an injected database
    # client, schema resolver, and index configuration; no constructor-time
    # DDL, unlike the legacy IndexDocs class.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, index_config: IndexConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.index_config = index_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Configuration lookups                            #
    #================================================================#

    # Internal Method: Load the configured doc fields of one doc type from
    # the index configuration, per projection family.
    def _doc_fields(self, doc_type: str, family: str) -> list[str]:
        return list(
            self.index_config.settings.get(family, {}).get("fields", {}).get("docs", {}).get(doc_type, [])
        )

    #================================================================#
    # Method Group: Graphsearch patch                                #
    #================================================================#

    # Public Method: Patch the doc projection of one doc type in the
    # graphsearch schema from the page profiles and the buildup rows.
    def patch(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        obj_fields = self._doc_fields(doc_type, "graphsearch")
        buildup_table_name = f"IndexBuildup_Fields_Docs_{doc_type}"
        target_table_name = f"Index_D_{doc_type}"

        # Ensure the buildup and target tables exist before patching.
        create_table_if_not_exists(self.db, engine_name, cache_schema, buildup_table_name)
        create_table_if_not_exists(self.db, engine_name, search_schema, target_table_name)

        # The graphsearch patch compares the degree score and the configured
        # doc fields against the page profile and buildup values.
        compare_pairs = [('t.degree_score', 'n.degree_score')] + [(f't.{c}', f'n.{c}') for c in obj_fields]
        compare_conditions = " OR ".join([
            f'COALESCE({t_col}, "__null__") != COALESCE({src_expr}, "__null__")'
            for t_col, src_expr in compare_pairs
        ])

        # The two evaluation slices: flagged page profiles, and unflagged
        # profiles with flagged buildup rows.
        eval_template = f"""
              SELECT COUNT(*) AS n_total,
                     COALESCE(SUM({compare_conditions}), 0) AS n_patch
                FROM {cache_schema}.Data_N_Object_T_PageProfile p
           LEFT JOIN {search_schema}.{target_table_name} t
                  ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)
          INNER JOIN {cache_schema}.{buildup_table_name} n
                  ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
               WHERE p.object_type = '{doc_type}'
        """
        sql_query_eval_1 = eval_template + "  AND p.to_process = 1"
        sql_query_eval_2 = eval_template + "  AND p.to_process = 0\n                 AND n.to_process = 1"

        # The evaluation queries run for eval and commit alike, so the commit
        # path knows the patch size.
        rows_to_process, rows_to_patch = 0, 0
        if 'commit' in actions or 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_query_eval_1, title='Rj0R4w2q[1/2]')
                print_sql(sql_query_eval_2, title='Rj0R4w2q[2/2]')
            out_1 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_1, query_id='Rj0R4w2q[1/2]')
            out_1 = out_1 if type(out_1) is list else [[0, 0]]
            out_2 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_2, query_id='Rj0R4w2q[2/2]')
            out_2 = out_2 if type(out_2) is list else [[0, 0]]
            rows_to_process = (out_1[0][0] if out_1 else 0) + (out_2[0][0] if out_2 else 0)
            rows_to_patch = (out_1[0][1] if out_1 else 0) + (out_2[0][1] if out_2 else 0)
        if 'eval' in actions and rows_to_patch == 0 and 'print' in actions:
            sysmsg.warning(f"No rows to patch in table '{search_schema}.{target_table_name}'.")

        # The commit upserts the degree score, the include-code flag, and the
        # configured doc fields from the buildup rows.
        upd_column_names = ['include_code_in_name', 'degree_score'] + obj_fields
        upd_column_values = ['n.include_code_in_name', 'n.degree_score'] + [f'n.{c}' for c in obj_fields]
        select_columns = ', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])
        commit_template = f"""
             SELECT n.doc_type, n.doc_id, {select_columns}
               FROM {cache_schema}.Data_N_Object_T_PageProfile p
         INNER JOIN {cache_schema}.{buildup_table_name} n
                 ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
              WHERE p.object_type = '{doc_type}'
        """
        sql_query_commit_1 = commit_template + "  AND p.to_process = 1"
        sql_query_commit_2 = commit_template + "  AND p.to_process = 0\n                AND n.to_process = 1"
        if 'print' in actions:
            print_sql(sql_query_commit_1, title='T4VTvBv6[1/2]')
            print_sql(sql_query_commit_2, title='T4VTvBv6[2/2]')

        # Execute the commits as chunked safe inserts, with the chunk filter
        # scoping boundary discovery to the page-profile rows that satisfy
        # the p-table predicates.
        if 'commit' in actions and rows_to_patch > 0:
            chunk_filter_by_query = {
                1: f"object_type = '{doc_type}' AND to_process = 1",
                2: f"object_type = '{doc_type}' AND to_process = 0",
            }
            for query_number, sql_query_commit in enumerate([sql_query_commit_1, sql_query_commit_2], start=1):
                sysmsg.trace(f"🔥 Executing commit query {query_number}/2 on table: '{search_schema}.{target_table_name}' ...")
                self.db.execute_query_as_safe_inserts_in_chunks(
                    engine_name       = engine_name,
                    schema_name       = search_schema,
                    table_name        = target_table_name,
                    query             = sql_query_commit,
                    key_column_names  = ['doc_type', 'doc_id'],
                    upd_column_names  = upd_column_names,
                    eval_column_names = ['doc_type'],
                    actions           = actions,
                    table_to_chunk    = f"{cache_schema}.Data_N_Object_T_PageProfile",
                    chunk_filter      = chunk_filter_by_query[query_number],
                    chunk_size        = 100000,
                    row_id_name       = 'p.row_id',
                    query_id          = f"T4VTvBv6[{query_number}/2]",
                )
        return PropagationStats(target=f"{search_schema}.{target_table_name}", rows_flagged=rows_to_patch)

    #================================================================#
    # Method Group: Elasticsearch cache patch                        #
    #================================================================#

    # Public Method: Patch the doc projection of one doc type in the
    # Elasticsearch cache schema, feeding the search index export.
    def patch_es_cache(self, doc_type: str, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, es_cache_schema = self.schema_resolver.for_es_cache()
        obj_fields = self._doc_fields(doc_type, "elasticsearch")
        buildup_table_name = f"IndexBuildup_Fields_Docs_{doc_type}"
        target_table_name = f"Index_D_{doc_type}"

        # Ensure the buildup and target tables exist before patching.
        create_table_if_not_exists(self.db, engine_name, cache_schema, buildup_table_name)
        create_table_if_not_exists(self.db, engine_name, es_cache_schema, target_table_name)

        # The cache patch compares the fixed presentation columns and the
        # configured doc fields; the names apply the include-code option.
        compare_pairs = [
            ('t.degree_score', 'n.degree_score'),
            ('t.short_code'  , 'p.short_code'),
            ('t.subtype_en'  , 'p.subtype_en'),
            ('t.subtype_fr'  , 'p.subtype_fr'),
            ('t.name_en', "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_en_value), p.name_en_value)"),
            ('t.name_fr', "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_fr_value), p.name_fr_value)"),
            ('t.short_description_en', 'p.description_short_en_value'),
            ('t.short_description_fr', 'p.description_short_fr_value'),
            ('t.long_description_en' , 'p.description_long_en_value'),
            ('t.long_description_fr' , 'p.description_long_fr_value'),
        ] + [(f't.{c}', f'n.{c}') for c in obj_fields]
        compare_conditions = " OR ".join([
            f'COALESCE({t_col}, "__null__") != COALESCE({src_expr}, "__null__")'
            for t_col, src_expr in compare_pairs
        ])

        # The two evaluation slices, mirroring the graphsearch patch.
        eval_template = f"""
              SELECT COUNT(*) AS n_total,
                     COALESCE(SUM({compare_conditions}), 0) AS n_patch
                FROM {cache_schema}.Data_N_Object_T_PageProfile p
           LEFT JOIN {es_cache_schema}.{target_table_name} t
                  ON (t.doc_type, t.doc_id) = (p.object_type, p.object_id)
          INNER JOIN {cache_schema}.{buildup_table_name} n
                  ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
               WHERE p.object_type = '{doc_type}'
        """
        sql_query_eval_1 = eval_template + "  AND p.to_process = 1"
        sql_query_eval_2 = eval_template + "  AND p.to_process = 0\n                 AND n.to_process = 1"

        # The evaluation queries run for eval and commit alike.
        rows_to_process, rows_to_patch = 0, 0
        if 'commit' in actions or 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_query_eval_1, title='4KpdVwsE[1/2]')
                print_sql(sql_query_eval_2, title='4KpdVwsE[2/2]')
            out_1 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_1, query_id='4KpdVwsE[1/2]')
            out_1 = out_1 if type(out_1) is list else [[0, 0]]
            out_2 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_2, query_id='4KpdVwsE[2/2]')
            out_2 = out_2 if type(out_2) is list else [[0, 0]]
            rows_to_process = (out_1[0][0] if out_1 else 0) + (out_2[0][0] if out_2 else 0)
            rows_to_patch = (out_1[0][1] if out_1 else 0) + (out_2[0][1] if out_2 else 0)
        if 'eval' in actions and rows_to_patch == 0 and 'print' in actions:
            sysmsg.warning(f"No rows to patch in table '{es_cache_schema}.{target_table_name}'.")

        # The commit upserts the fixed presentation columns and the
        # configured doc fields.
        upd_column_names = [
            'degree_score', 'short_code', 'subtype_en', 'subtype_fr',
            'name_en', 'name_fr',
            'short_description_en', 'short_description_fr',
            'long_description_en', 'long_description_fr',
        ] + obj_fields
        upd_column_values = [
            'n.degree_score', 'p.short_code', 'p.subtype_en', 'p.subtype_fr',
            "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_en_value), p.name_en_value)",
            "IF(n.include_code_in_name=1, CONCAT(n.doc_id, ': ', p.name_fr_value), p.name_fr_value)",
            'p.description_short_en_value', 'p.description_short_fr_value',
            'p.description_long_en_value', 'p.description_long_fr_value',
        ] + [f'n.{c}' for c in obj_fields]
        select_columns = ', '.join([f'{v} AS {c}' for c, v in zip(upd_column_names, upd_column_values)])
        commit_template = f"""
             SELECT n.doc_type, n.doc_id, {select_columns}
               FROM {cache_schema}.Data_N_Object_T_PageProfile p
         INNER JOIN {cache_schema}.{buildup_table_name} n
                 ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
              WHERE p.object_type = '{doc_type}'
        """
        sql_query_commit_1 = commit_template + "  AND p.to_process = 1"
        sql_query_commit_2 = commit_template + "  AND p.to_process = 0\n                AND n.to_process = 1"
        if 'print' in actions:
            print_sql(sql_query_commit_1, title='vdEk9bpn[1/2]')
            print_sql(sql_query_commit_2, title='vdEk9bpn[2/2]')

        # Execute the commits as chunked safe inserts.
        if 'commit' in actions and rows_to_patch > 0:
            chunk_filter_by_query = {
                1: f"object_type = '{doc_type}' AND to_process = 1",
                2: f"object_type = '{doc_type}' AND to_process = 0",
            }
            for query_number, sql_query_commit in enumerate([sql_query_commit_1, sql_query_commit_2], start=1):
                sysmsg.trace(f"🔥 Executing commit query {query_number}/2 on table: '{es_cache_schema}.{target_table_name}' ...")
                self.db.execute_query_as_safe_inserts_in_chunks(
                    engine_name       = engine_name,
                    schema_name       = es_cache_schema,
                    table_name        = target_table_name,
                    query             = sql_query_commit,
                    key_column_names  = ['doc_type', 'doc_id'],
                    upd_column_names  = upd_column_names,
                    eval_column_names = ['doc_type'],
                    actions           = actions,
                    table_to_chunk    = f"{cache_schema}.Data_N_Object_T_PageProfile",
                    chunk_filter      = chunk_filter_by_query[query_number],
                    chunk_size        = 100000,
                    row_id_name       = 'p.row_id',
                    query_id          = f"vdEk9bpn[{query_number}/2]",
                )
        return PropagationStats(target=f"{es_cache_schema}.{target_table_name}", rows_flagged=rows_to_patch)

    #================================================================#
    # Method Group: Settle and flag cleanup                          #
    #================================================================#

    # Public Method: Settle the airflow change records of the patched
    # documents, stamping the cache date and clearing the processing flags.
    def settle(self, doc_type: str, actions: ActionSet = ('commit',)) -> None:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        buildup_table_name = f"IndexBuildup_Fields_Docs_{doc_type}"

        # The two settle slices mirror the patch slices: flagged profiles,
        # and unflagged profiles with flagged buildup rows.
        settle_template = f"""
              UPDATE {airflow_schema}.Operations_N_Object_T_FieldsChanged a
          INNER JOIN {cache_schema}.Data_N_Object_T_PageProfile p
                  ON (a.object_type, a.object_id) = (p.object_type, p.object_id)
          INNER JOIN {cache_schema}.{buildup_table_name} n
                  ON (p.object_type, p.object_id) = (n.doc_type, n.doc_id)
                 SET a.last_date_cached = CURDATE(), a.has_expired = 0, a.to_process = 0
               WHERE p.object_type = '{doc_type}'
                 AND a.deleted = 0
        """
        sql_query_commit_1 = settle_template + "  AND p.to_process = 1"
        sql_query_commit_2 = settle_template + "  AND p.to_process = 0\n               AND n.to_process = 1"
        if 'print' in actions:
            print_sql(sql_query_commit_1, title='42vKAJcy[1/2]')
            print_sql(sql_query_commit_2, title='42vKAJcy[2/2]')
        if 'commit' in actions:
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit_1, verbose=self.verbose, query_id='42vKAJcy[1/2]')
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit_2, verbose=self.verbose, query_id='42vKAJcy[2/2]')

    # Public Method: Reset the processing flags of the page profiles and
    # buildup rows of one doc type after the patch cycle completed.
    def cleanup_flags(self, doc_type: str, actions: ActionSet = ('commit',)) -> None:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()

        # Reset the page profile flags of the doc type.
        profile_reset_query = f"""
              UPDATE {cache_schema}.Data_N_Object_T_PageProfile
                 SET to_process = 0
               WHERE object_type = '{doc_type}'
                 AND to_process = 1
        """
        if 'print' in actions:
            print_sql(profile_reset_query, title='P9Caiq8w')
        if 'commit' in actions:
            self.db.execute_query_in_shell(engine_name=engine_name, query=profile_reset_query, verbose=self.verbose, query_id='P9Caiq8w')

        # Reset the buildup flags of the doc type.
        buildup_reset_query = f"""
              UPDATE {cache_schema}.IndexBuildup_Fields_Docs_{doc_type}
                 SET to_process = 0
               WHERE to_process = 1
        """
        if 'print' in actions:
            print_sql(buildup_reset_query, title='yJ74cRvU')
        if 'commit' in actions:
            self.db.execute_query_in_shell(engine_name=engine_name, query=buildup_reset_query, verbose=self.verbose, query_id='yJ74cRvU')
