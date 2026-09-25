# graphregistry/adapters/persistence/mysql/repositories/rpo_indexdoclink.py
from __future__ import annotations
import re
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.adapters.persistence.mysql.repositories.helpers import create_table_if_not_exists
from graphregistry.application.ports.repositories.prt_indexdoclink import IndexDocLinkRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import IndexConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_indexdocs import DocLinkTypeKey
from graphregistry.domain.models.pipeline.mdl_policies import LinkSelectionPolicy
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.types import ActionSet

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

# SQL data type mapping of the index configuration field types, copied verbatim
# from the legacy module constants the ordering rules rely on.
SQL_DATA_TYPE_MAPPING = {
    'char'     : 'VARCHAR(255)',
    'text'     : 'MEDIUMTEXT',
    'longtext' : 'LONGTEXT',
    'int'      : 'MEDIUMINT UNSIGNED',
    'bool'     : 'TINYINT(1)',
    'date'     : 'DATE',
    'datetime' : 'DATETIME',
}

# Mapping from field datatypes onto castable CAST expressions, applied to the
# configuration-driven ordering rules of the horizontal patch.
CAST_MAPPING = {
    "TINYINT(1)"         : "CAST(%s AS UNSIGNED)",
    "SMALLINT UNSIGNED"  : "CAST(%s AS UNSIGNED)",
    "YEAR"               : "CAST(%s AS UNSIGNED)",
    "VARCHAR(16)"        : "CAST(%s AS CHAR)",
    "VARCHAR(255)"       : "CAST(%s AS CHAR)",
    "MEDIUMTEXT"         : "CAST(%s AS CHAR)",
    "LONGTEXT"           : "CAST(%s AS CHAR)",
    "DATE"               : "CAST(%s AS DATE)",
    "DATETIME"           : "CAST(%s AS DATETIME)",
    "MEDIUMINT UNSIGNED" : "CAST(%s AS UNSIGNED)",
}

#==================#
# Class Definition #
#==================#
class MySQLIndexDocLinkRepository(IndexDocLinkRepository):
    """MySQL adapter for the IndexDocLinkRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.IndexDB.IndexDocLinks class: the vertical patch
    (hLdNx8Hb / FCQgBmb2 for the default fields, of8T3uCG / sxUZ7wER for the
    parent-child fields) and the horizontal patch (z0rFNfM5 / oxyoF81R eval,
    Del6hv6h delete, InsFwdKuT / InsFlippedT1B inserts, TmpHpCreate /
    TmpHpDrop scratch table). The patched field lists and ordering rules are
    loaded from the index configuration exactly as the legacy loads them.

    Two legacy simplifications are preserved: the SQLQuery1 / SQLQuery2
    variables of the horizontal patch were initialised to None and never
    assigned, so only the forward and flipped inserts exist; and the
    organisational partition always patches untruncated (the legacy forces
    the rank threshold to a large sentinel regardless of the parameter).
    """

    # Public Method: Initialize the repository with an injected database client,
    # schema resolver, and configurations; no module-level connections and no
    # constructor-time DDL, unlike the legacy IndexDocLinks class.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, index_config: IndexConfig, scores_config: ScoresConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.index_config = index_config
        self.scores_config = scores_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Configuration lookups                            #
    #================================================================#

    # Internal Method: Load the patched field lists of one projection from the
    # index configuration: the default link fields of the link type and the
    # parent-child link fields of the pair, as the legacy constructor does.
    def _field_lists(self, key: DocLinkTypeKey) -> tuple[list[str], list[str]]:
        obj_fields = self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("links", {}).get("default", {}).get(key.link_type, [])
        obj2obj_fields = []
        if key.partition == 'ORG':
            obj2obj_fields = (
                self.index_config.settings.get("graphsearch", {}).get("fields", {}).get("links", {})
                .get("parent_child", {}).get(key.doc_type, {}).get(key.link_type, [])
            )
        return list(obj_fields), list(obj2obj_fields)

    # Internal Method: Build the ORDER BY expression of the horizontal patch
    # from the selection policy: the cast configuration rules first, then the
    # score column and the link id as the legacy default.
    def _order_by_expression(self, key: DocLinkTypeKey, policy: LinkSelectionPolicy) -> str:
        order_by = f"{policy.score_type} DESC, link_id ASC"
        if len(policy.ordering) > 0:
            data_types = self.index_config.settings.get("data_types", {})
            rules = ", ".join(
                f"{CAST_MAPPING[SQL_DATA_TYPE_MAPPING[data_types[rule.field]]] % rule.field} {rule.direction}"
                for rule in policy.ordering
                if rule.field in data_types
            )
            if rules:
                order_by = f"{rules}, {order_by}"
        return order_by

    # Internal Method: Build the IS NOT NULL filters for the configuration
    # ordering columns, prefixed per source table as the legacy helper does.
    def _order_by_null_filter(self, policy: LinkSelectionPolicy, prefix: str, obj2obj_fields: list[str] | None = None, obj2obj_prefix: str | None = None) -> str:
        obj2obj_fields = obj2obj_fields or []
        conditions = []
        for rule in policy.ordering:
            if obj2obj_prefix is not None and rule.field in obj2obj_fields:
                conditions.append(f"{obj2obj_prefix}.{rule.field} IS NOT NULL")
            else:
                conditions.append(f"{prefix}.{rule.field} IS NOT NULL")
        return " AND ".join(conditions) if conditions else "1=1"

    #================================================================#
    # Method Group: Vertical patch                                   #
    #================================================================#

    # Public Method: Ensure the graphsearch doc-link tables of the given keys
    # exist. The legacy IndexDB constructor created every configured
    # doc-link table eagerly; the typed architecture creates them lazily
    # per patched pair, so unpatched configured pairs would miss their
    # table in the schema export (E2E finding, 2026-09-24).
    def ensure_link_tables(self, keys: list[DocLinkTypeKey]) -> None:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        for key in keys:
            create_table_if_not_exists(
                self.db, engine_name, search_schema,
                f"Index_D_{key.doc_type}_L_{key.link_type}_T_{key.partition}",
            )

    # Public Method: Patch the denormalised link fields of one projection from
    # the linked documents' profiles, resolving content drift. The SEM
    # partition refreshes the default link fields; the ORG partition
    # additionally refreshes the parent-child link fields.
    def vertical_patch(self, key: DocLinkTypeKey, actions: ActionSet = ('commit',)) -> PropagationStats:
        if key.partition == 'ORG':
            return self._vertical_patch_parentchild(key, actions)
        return self._vertical_patch_default(key, actions)

    # Internal Method: Vertical patch of the default link fields, joined from
    # the link type's docs buildup table.
    def _vertical_patch_default(self, key: DocLinkTypeKey, actions: ActionSet) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        obj_fields, _obj2obj_fields = self._field_lists(key)
        target_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_{key.partition}"
        target_table_path = f"{search_schema}.{target_table_name}"
        buildup_link_table_name = f"IndexBuildup_Fields_Docs_{key.link_type}"
        buildup_link_table_path = f"{cache_schema}.{buildup_link_table_name}"

        # Without configured fields there is nothing to patch.
        if len(obj_fields) == 0:
            if 'print' in actions:
                sysmsg.trace(f"No fields to patch for doc-link type '{key.doc_type} --> {key.link_type}'.")
            return PropagationStats(target=target_table_path, rows_flagged=0)

        # Ensure the buildup and target tables exist before patching.
        create_table_if_not_exists(self.db, engine_name, cache_schema, buildup_link_table_name)
        create_table_if_not_exists(self.db, engine_name, search_schema, target_table_name)

        # Evaluation counts the flagged buildup rows and the drifted ones.
        drift_conditions = " OR ".join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in obj_fields])
        sql_query_eval = f"""
                    SELECT COUNT(*) AS n_total, COALESCE(SUM({drift_conditions}), 0) AS n_patch
                      FROM {buildup_link_table_path} b
                INNER JOIN {target_table_path} i
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                     WHERE b.to_process = 1;
                """

        # The evaluation query runs for eval and commit alike, so the commit
        # path knows the patch size and can pick the execution strategy.
        rows_to_process, rows_to_patch = 0, 0
        if 'commit' in actions or 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_query_eval, title='hLdNx8Hb')
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='hLdNx8Hb')
            out = out if type(out) is list else [[0, 0]]
            rows_to_process, rows_to_patch = out[0]
        if 'eval' in actions and rows_to_patch == 0 and 'print' in actions:
            sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

        # Commit query: refresh the drifted fields from the buildup rows.
        sql_query_commit = f"""
                    UPDATE {target_table_path} i
                INNER JOIN {buildup_link_table_path} b
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                       SET {', '.join([f'i.{c}  = b.{c}' for c in obj_fields])}
                     WHERE b.to_process = 1
                       AND ({drift_conditions});
                """
        if 'print' in actions:
            print_sql(sql_query_commit, title='FCQgBmb2')

        # Small patches run directly; large patches run chunked, with the
        # chunk filter scoping boundary discovery to rows with a pending
        # buildup counterpart.
        if 'commit' in actions and rows_to_patch > 0:
            if rows_to_patch <= 10000:
                self.db.execute_query_in_shell(
                    engine_name = engine_name,
                    query       = sql_query_commit,
                    query_id    = 'FCQgBmb2',
                    verbose     = 'print' in actions,
                )
            else:
                self.db.execute_query_in_chunks(
                    engine_name   = engine_name,
                    schema_name   = search_schema,
                    table_name    = target_table_name,
                    query         = sql_query_commit,
                    chunk_filter  = f"EXISTS (SELECT 1 FROM {buildup_link_table_path} b WHERE (b.doc_type, b.doc_id) = (link_type, link_id) AND b.to_process = 1)",
                    chunk_size    = 10000,
                    row_id_name   = 'i.row_id',
                    show_progress = False,
                    query_id      = 'FCQgBmb2',
                    verbose       = 'print' in actions,
                )
        return PropagationStats(target=target_table_path, rows_flagged=rows_to_patch)

    # Internal Method: Vertical patch of the ORG partition, additionally
    # joining the parent-child links buildup table for the obj2obj fields.
    def _vertical_patch_parentchild(self, key: DocLinkTypeKey, actions: ActionSet) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        obj_fields, obj2obj_fields = self._field_lists(key)
        target_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_{key.partition}"
        target_table_path = f"{search_schema}.{target_table_name}"

        # The obj2obj buildup table is keyed by the canonical pair; the link
        # direction flag selects which side of the pair this table's rows use.
        src, trg = sorted([key.doc_type, key.link_type])
        flipped = [src, trg] != [key.doc_type, key.link_type]
        buildup_link_table_name_obj = f"IndexBuildup_Fields_Docs_{key.link_type}"
        buildup_link_table_name_obj2obj = f"IndexBuildup_Fields_Links_ParentChild_{src}_{trg}"
        buildup_link_table_path_obj = f"{cache_schema}.{buildup_link_table_name_obj}"
        buildup_link_table_path_obj2obj = f"{cache_schema}.{buildup_link_table_name_obj2obj}"

        # Ensure the buildup and target tables exist before any early return,
        # compensating for the legacy's constructor-time table creation.
        create_table_if_not_exists(self.db, engine_name, cache_schema, buildup_link_table_name_obj)
        create_table_if_not_exists(self.db, engine_name, cache_schema, buildup_link_table_name_obj2obj)
        create_table_if_not_exists(self.db, engine_name, search_schema, target_table_name)

        # Without configured fields there is nothing to patch.
        if len(obj_fields) == 0:
            if 'print' in actions:
                sysmsg.trace(f"No fields to patch for doc-link type '{key.doc_type} --> {key.link_type}'.")
            return PropagationStats(target=target_table_path, rows_flagged=0)

        # The obj2obj buildup join is only built when the table exists and
        # carries configured fields.
        obj2obj_exists = self.db.table_exists(engine_name=engine_name, schema_name=cache_schema, table_name=buildup_link_table_name_obj2obj)
        has_obj2obj_fields = obj2obj_exists and len(obj2obj_fields) > 0
        if not obj2obj_exists:
            sysmsg.warning(f"Source table '{buildup_link_table_name_obj2obj}' does not exist.")
        obj2obj_placeholder = ''
        if obj2obj_exists:
            side = {False: 'doc', True: 'link'}[flipped]
            other = {False: 'link', True: 'doc'}[flipped]
            obj2obj_placeholder = f"""
                 LEFT JOIN {buildup_link_table_path_obj2obj} l
                        ON (i.doc_type, i.doc_id, i.link_type, i.link_id)
                         = (l.{side}_type, l.{side}_id, l.{other}_type, l.{other}_id)
                    """

        # Evaluation counts the flagged rows and the drifted ones across both
        # field sources.
        drift_b = " OR ".join([f'COALESCE(i.{c}, "__null__") != COALESCE(b.{c}, "__null__")' for c in obj_fields])
        drift_l = " OR ".join([f'COALESCE(i.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in obj2obj_fields])
        joiner = " OR " if has_obj2obj_fields else ""
        sql_query_eval = f"""
                    SELECT COUNT(*) AS n_total,
                           COALESCE(SUM({drift_b}), 0){joiner if has_obj2obj_fields else ''}
                        {"COALESCE(SUM(" if has_obj2obj_fields else ""}{drift_l if has_obj2obj_fields else ""}{"), 0)" if has_obj2obj_fields else ""}
                           AS n_patch
                      FROM {buildup_link_table_path_obj} b
                INNER JOIN {target_table_path} i
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                           {obj2obj_placeholder}
                     WHERE b.to_process = 1;
                """

        # The evaluation query runs for eval and commit alike.
        rows_to_process, rows_to_patch = 0, 0
        if 'commit' in actions or 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_query_eval, title='Kwrgj34')
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='of8T3uCG', verbose='print' in actions)
            out = out if type(out) is list else [[0, 0]]
            rows_to_process, rows_to_patch = out[0]
        if 'eval' in actions and rows_to_patch == 0 and 'print' in actions:
            sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

        # Commit query: refresh the drifted fields from both buildup sources.
        set_clause = ", ".join([f'i.{c}  = b.{c}' for c in obj_fields]) + ("," if has_obj2obj_fields else "")
        set_clause += " " + ", ".join([f'i.{c}  = l.{c}' for c in obj2obj_fields])
        sql_query_commit = f"""
                    UPDATE {target_table_path} i
                INNER JOIN {buildup_link_table_path_obj} b
                        ON (i.link_type, i.link_id) = (b.doc_type, b.doc_id)
                           {obj2obj_placeholder}
                       SET {set_clause}
                     WHERE b.to_process = 1
                       AND ({drift_b}){" OR " if has_obj2obj_fields else ""}
                {"(" if has_obj2obj_fields else ""}{drift_l if has_obj2obj_fields else ""}{")" if has_obj2obj_fields else ""};
                """
        if 'print' in actions:
            print_sql(sql_query_commit, title='sxUZ7wER')

        # The ORG patch always runs chunked, with the chunk filter scoping
        # boundary discovery to rows with a pending buildup counterpart.
        if 'commit' in actions and rows_to_patch > 0:
            self.db.execute_query_in_chunks(
                engine_name   = engine_name,
                schema_name   = search_schema,
                table_name    = target_table_name,
                query         = sql_query_commit,
                chunk_filter  = f"EXISTS (SELECT 1 FROM {buildup_link_table_path_obj} b WHERE (b.doc_type, b.doc_id) = (link_type, link_id) AND b.to_process = 1)",
                chunk_size    = 10000,
                row_id_name   = 'i.row_id',
                show_progress = False,
                query_id      = 'sxUZ7wER',
                verbose       = 'print' in actions,
            )
        return PropagationStats(target=target_table_path, rows_flagged=rows_to_patch)

    #================================================================#
    # Method Group: Horizontal patch                                 #
    #================================================================#

    # Internal Method: Check whether the pair is an ontology-object edge, as
    # the legacy helper does: exactly one of the two types is Concept or
    # Category.
    @staticmethod
    def _is_ontology_object_edge(key: DocLinkTypeKey) -> bool:
        ontology_types = {'Concept', 'Category'}
        return (key.doc_type in ontology_types) != (key.link_type in ontology_types)

    # Internal Method: Resolve the final scores source of an ontology-object
    # edge: the table path, the ontology id column, and both type names.
    def _ontology_final_scores_source(self, key: DocLinkTypeKey) -> tuple[str, str, str, str]:
        _, cache_schema = self.schema_resolver.for_graph_cache()
        ontology_types = {'Concept', 'Category'}
        if key.doc_type in ontology_types:
            ontology_type, object_type = key.doc_type, key.link_type
        else:
            ontology_type, object_type = key.link_type, key.doc_type
        if ontology_type == 'Concept':
            final_scores_table = f"{cache_schema}.Edges_N_Object_N_Concept_T_FinalScores"
            ontology_id_col = 'concept_id'
        else:
            final_scores_table = f"{cache_schema}.Edges_N_Object_N_Category_T_FinalScores"
            ontology_id_col = 'category_id'
        return final_scores_table, ontology_id_col, ontology_type, object_type

    # Internal Method: Resolve the adjusted-scores matrix table name of a pair,
    # mirroring the legacy get_scores_matrix_table_name used by the semantic
    # patch branch.
    def _scores_matrix_table_name(self, key: DocLinkTypeKey, kind: str) -> str:
        from graphregistry.domain.models.values.mdl_typepairs import EdgeTypePair
        canonical = EdgeTypePair(from_object_type=key.doc_type, to_object_type=key.link_type).canonical_order
        domain = canonical.matrix_domain(self.scores_config.settings['scored_edge_tuple_to_class_mapping'])
        if domain is None:
            raise ValueError(
                f"Invalid input: ({canonical.from_object_type}, {canonical.to_object_type}, {kind}). "
                f"No corresponding scores matrix table found."
            )
        return f"Edges_N_Object_N_Object_T_ScoresMatrix_{domain.title()}_{kind}"

    # Public Method: Patch and re-rank the links of one projection, adding
    # missing links, replacing stale ones with new scores, and reordering,
    # truncated by the rank threshold; resolves membership and ranking drift.
    def horizontal_patch(self, key: DocLinkTypeKey, policy: LinkSelectionPolicy, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        obj_fields, obj2obj_fields = self._field_lists(key)
        target_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_{key.partition}"
        target_table_path = f"{search_schema}.{target_table_name}"
        buildup_link_table_path = f"{cache_schema}.IndexBuildup_Fields_Docs_{key.link_type}"
        parentchild_table_path = f"{cache_schema}.Edges_N_Object_N_Object_T_ParentChildSymmetric"
        order_by = self._order_by_expression(key, policy)

        # Ensure the target and link-side buildup tables exist before patching,
        # compensating for the legacy's constructor-time table creation: the
        # horizontal patch joins the link type's buildup table even when the
        # link type itself is not active (reached via the link-side rule).
        create_table_if_not_exists(self.db, engine_name, search_schema, target_table_name)
        create_table_if_not_exists(self.db, engine_name, cache_schema, f"IndexBuildup_Fields_Docs_{key.link_type}")

        # Does the parent-child links buildup table exist, in either spelling?
        buildup_table_exists_direct = self.db.table_exists(engine_name=engine_name, schema_name=cache_schema, table_name=f"IndexBuildup_Fields_Links_ParentChild_{key.doc_type}_{key.link_type}")
        buildup_table_exists_flipped = self.db.table_exists(engine_name=engine_name, schema_name=cache_schema, table_name=f"IndexBuildup_Fields_Links_ParentChild_{key.link_type}_{key.doc_type}")
        buildup_table_exists = buildup_table_exists_direct or buildup_table_exists_flipped

        # Initialise the query set; the legacy SQLQuery1/SQLQuery2 variables
        # were never assigned, so only the delete and the two inserts exist.
        delete_query = None
        insert_forward = None
        insert_flipped = None
        uses_temp_affected_docs_table = False
        temp_table_create_query = None
        temp_table_drop_query = None

        # Organisational links are always patched untruncated; semantic links
        # use the policy threshold, mapping an unbounded policy onto the
        # legacy sentinel.
        if key.partition == 'ORG':
            row_rank_thr = 9999999
        else:
            row_rank_thr = policy.rank_threshold if policy.rank_threshold is not None else 9999999

        #---------------#
        # ORG partition #
        #---------------#
        if key.partition == 'ORG':

            # Resolve the canonical context of this edge pair from the index
            # configuration; without a context there is nothing to patch.
            edge_pair_key = tuple(sorted([key.doc_type, key.link_type]))
            edge_context = self.index_config.settings.get('edge_selection_contexts', {}).get(edge_pair_key)
            sysmsg.trace(
                "horizontal_patch_parentchild: doc_type='{}' link_type='{}' edge_pair_key={} context='{}'",
                key.doc_type, key.link_type, edge_pair_key, edge_context,
            )
            if edge_context is None:
                sysmsg.warning(
                    f"No edge context configured for '{key.doc_type} <-> {key.link_type}' "
                    "in config/application/config_index.json object-selection.edges. Skipping."
                )
                return PropagationStats(target=target_table_path, rows_flagged=0)

            # Affected doc ids: the endpoints of flagged parent-child edges.
            doc_id_subquery = f"""
                SELECT DISTINCT IF(from_object_type='{key.doc_type}', from_object_id, to_object_id) AS doc_id
                  FROM {parentchild_table_path}
                 WHERE from_object_type = '{key.doc_type}'
                   AND to_object_type   = '{key.link_type}'
                   AND context          = '{edge_context}'
                   AND to_process = 1
                   AND deleted = 0
            """
            delete_query = f"""
                    DELETE FROM {target_table_path}
                     WHERE doc_id IN ({doc_id_subquery})
                    """

            # Column list of the insert: identity columns, the configured
            # fields of both sources, and the ranking columns.
            columns = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id'] + obj_fields + obj2obj_fields + ['degree_score', 'row_score', 'row_rank']
            select_fields = (
                [f"bd.{c}" for c in obj_fields] + [f"bl.{c}" for c in obj2obj_fields] + ["bd.degree_score"]
            )
            if buildup_table_exists:

                # Insert freshly ranked rows, joining both buildup sources.
                insert_forward = f"""
                        INSERT INTO {target_table_path}
                                     ({', '.join(columns)})
                        WITH affected_docs AS (
                            {doc_id_subquery}
                        ),
                        ranked AS (
                              SELECT p.from_object_type AS  doc_type, p.from_object_id AS doc_id,
                                       p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                      {', '.join(select_fields)},
                                      1/2 + 1/(1+row_number() OVER (PARTITION BY p.from_object_id ORDER BY {order_by})) AS row_score,
                                                 row_number() OVER (PARTITION BY p.from_object_id ORDER BY {order_by})  AS row_rank
                                FROM {parentchild_table_path} p
                          INNER JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                           LEFT JOIN {cache_schema}.IndexBuildup_Fields_Links_ParentChild_{key.doc_type if buildup_table_exists_direct else key.link_type}_{key.link_type if buildup_table_exists_direct else key.doc_type} bl
                                  ON (p.{'from' if buildup_table_exists_direct else 'to'}_object_type, p.{'from' if buildup_table_exists_direct else 'to'}_object_id, p.{'to' if buildup_table_exists_direct else 'from'}_object_type, p.{'to' if buildup_table_exists_direct else 'from'}_object_id) = (bl.doc_type, bl.doc_id, bl.link_type, bl.link_id)
                          INNER JOIN affected_docs ad
                                  ON p.from_object_id = ad.doc_id
                                WHERE p.from_object_type = '{key.doc_type}'
                                  AND p.to_object_type   = '{key.link_type}'
                                  AND p.context          = '{edge_context}'
                                  AND p.deleted = 0
                                  AND bd.degree_score IS NOT NULL
                                  AND p.to_object_id IS NOT NULL
                                  AND {self._order_by_null_filter(policy, 'bd', obj2obj_fields, 'bl')}
                         )
                           SELECT {', '.join(columns)}
                             FROM ranked
                            WHERE row_rank <= {row_rank_thr}
                          """
            else:

                # Insert freshly ranked rows without the links buildup source.
                insert_forward = f"""
                        INSERT INTO {target_table_path}
                                     ({', '.join(columns)})
                        WITH affected_docs AS (
                            {doc_id_subquery}
                        ),
                        ranked AS (
                              SELECT p.from_object_type AS  doc_type, p.from_object_id AS doc_id,
                                       p.to_object_type AS link_type, p.edge_type AS link_subtype, p.to_object_id AS link_id,
                                      {', '.join([f'bd.{c}' for c in obj_fields])}{',' if obj_fields else ''}
                                      bd.degree_score,
                                      1/2 + 1/(1+row_number() OVER (PARTITION BY p.from_object_id ORDER BY {order_by})) AS row_score,
                                                 row_number() OVER (PARTITION BY p.from_object_id ORDER BY {order_by})  AS row_rank
                                FROM {parentchild_table_path} p
                           LEFT JOIN {buildup_link_table_path} bd
                                  ON (p.to_object_type, p.to_object_id) = (bd.doc_type, bd.doc_id)
                          INNER JOIN affected_docs ad
                                  ON p.from_object_id = ad.doc_id
                                WHERE p.from_object_type = '{key.doc_type}'
                                  AND p.to_object_type   = '{key.link_type}'
                                  AND p.context        = '{edge_context}'
                                  AND p.deleted = 0
                                  AND bd.degree_score IS NOT NULL
                                  AND p.to_object_id IS NOT NULL
                                  AND {self._order_by_null_filter(policy, 'bd', obj2obj_fields)}
                         )
                           SELECT {', '.join(columns)}
                             FROM ranked
                            WHERE row_rank <= {row_rank_thr}
                          """

        #---------------------------------#
        # SEM partition, ontology-object #
        #---------------------------------#
        elif self._is_ontology_object_edge(key):
            final_scores_table, ontology_id_col, ontology_type, object_type = self._ontology_final_scores_source(key)

            # Each ontology-object pair has two index tables; each stores only
            # its named forward direction to avoid duplicating reverse rows.
            if key.link_type == object_type:
                link_join_condition = f"(fs.object_type, fs.object_id) = ('{object_type}', i.doc_id)"
            else:
                link_join_condition = f"fs.{ontology_id_col} = i.doc_id"

            # The affected doc ids depend on which side of the pair is the doc.
            if key.doc_type in ('Concept', 'Category'):
                doc_id_subquery = f"""
                    SELECT DISTINCT {ontology_id_col} AS doc_id
                      FROM {final_scores_table}
                     WHERE object_type = '{object_type}'
                       AND to_process = 1
                       AND deleted = 0
                """
            else:
                doc_id_subquery = f"""
                    SELECT DISTINCT object_id AS doc_id
                      FROM {final_scores_table}
                     WHERE object_type = '{object_type}'
                       AND to_process = 1
                       AND deleted = 0
                """
            delete_query = f"""
                    DELETE FROM {target_table_path}
                     WHERE doc_id IN ({doc_id_subquery})
                    """
            columns = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id'] + obj_fields + ['semantic_score', 'row_score', 'row_rank']
            if key.doc_type == ontology_type:

                # Forward: ontology as doc, object as link.
                insert_forward = f"""
                            INSERT INTO {target_table_path}
                                         ({', '.join(columns)})
                            WITH affected_docs AS (
                                {doc_id_subquery}
                            ),
                            ranked AS (
                                  SELECT '{ontology_type}' AS doc_type, fs.{ontology_id_col} AS doc_id,
                                         fs.object_type AS link_type, 'Semantic' AS link_subtype, fs.object_id AS link_id,
                                          {', '.join([f'i.{c}' for c in obj_fields])}{',' if obj_fields else ''}
                                          fs.score AS semantic_score,
                                          1/2 + 1/(1+row_number() OVER (PARTITION BY fs.{ontology_id_col} ORDER BY {order_by})) AS row_score,
                                                     row_number() OVER (PARTITION BY fs.{ontology_id_col} ORDER BY {order_by}) AS row_rank
                                    FROM {final_scores_table} fs
                              INNER JOIN {buildup_link_table_path} i
                                      ON {link_join_condition}
                              INNER JOIN affected_docs ad
                                      ON fs.{ontology_id_col} = ad.doc_id
                                    WHERE fs.object_type = '{object_type}'
                                      AND fs.deleted = 0
                                      AND fs.score IS NOT NULL
                                      AND fs.object_id IS NOT NULL
                                      AND {self._order_by_null_filter(policy, 'i')}
                             )
                              SELECT {', '.join(columns)}
                                FROM ranked
                               WHERE semantic_score >= 0.1
                                 AND row_rank <= {row_rank_thr}
                              """
            else:

                # Forward: object as doc, ontology as link.
                insert_forward = f"""
                            INSERT INTO {target_table_path}
                                         ({', '.join(columns)})
                            WITH affected_docs AS (
                                {doc_id_subquery}
                            ),
                            ranked AS (
                                  SELECT fs.object_type AS doc_type, fs.object_id AS doc_id,
                                         '{ontology_type}' AS link_type, 'Semantic' AS link_subtype, fs.{ontology_id_col} AS link_id,
                                          {', '.join([f'i.{c}' for c in obj_fields])}{',' if obj_fields else ''}
                                          fs.score AS semantic_score,
                                          1/2 + 1/(1+row_number() OVER (PARTITION BY fs.object_id ORDER BY {order_by})) AS row_score,
                                                     row_number() OVER (PARTITION BY fs.object_id ORDER BY {order_by}) AS row_rank
                                    FROM {final_scores_table} fs
                              INNER JOIN {buildup_link_table_path} i
                                      ON {link_join_condition}
                              INNER JOIN affected_docs ad
                                      ON fs.object_id = ad.doc_id
                                    WHERE fs.object_type = '{object_type}'
                                      AND fs.deleted = 0
                                      AND fs.score IS NOT NULL
                                      AND fs.{ontology_id_col} IS NOT NULL
                                      AND {self._order_by_null_filter(policy, 'i')}
                             )
                              SELECT {', '.join(columns)}
                                FROM ranked
                               WHERE semantic_score >= 0.1
                                 AND row_rank <= {row_rank_thr}
                              """

        #------------------------#
        # SEM partition, matrix #
        #------------------------#
        else:

            # Non-ontology SEM edges read the adjusted-scores matrix table.
            scores_matrix_table_name_as = self._scores_matrix_table_name(key, 'AS')
            scoresmatrix_table_path = f"{cache_schema}.{scores_matrix_table_name_as}"

            # Affected doc ids, in both directions of the matrix.
            doc_id_subquery = f"""
                              SELECT DISTINCT IF(from_object_type="{key.doc_type}", from_object_id, to_object_id) AS doc_id
                                         FROM {scoresmatrix_table_path}
                                        WHERE (
                                                    (       from_object_type = "{key.doc_type}"
                                                        AND   to_object_type = "{key.link_type}"
                                                    )
                                                OR
                                                    (
                                                                  to_object_type = "{key.doc_type}"
                                                        AND from_object_type = "{key.link_type}"
                                                    )
                                                  )
                                               AND to_process = 1
                                               AND deleted = 0

                                             UNION

                              SELECT DISTINCT IF(to_object_type="{key.doc_type}", to_object_id, from_object_id) AS doc_id
                                         FROM {scoresmatrix_table_path}
                                        WHERE (
                                                    (       from_object_type = "{key.doc_type}"
                                                        AND   to_object_type = "{key.link_type}"
                                                    )
                                                OR
                                                    (
                                                                  to_object_type = "{key.doc_type}"
                                                        AND from_object_type = "{key.link_type}"
                                                    )
                                                  )
                                               AND to_process = 1
                                               AND deleted = 0
                            """

            # Materialize the affected doc ids into a scratch table so the
            # delete and insert subqueries are simple lookups.
            temp_table_path = f"{cache_schema}._tmp_hp_{key.doc_type}_{key.link_type}"
            temp_table_create_query = f"""
                        DROP TABLE IF EXISTS {temp_table_path};
                        CREATE TABLE {temp_table_path} (doc_id VARCHAR(255) NOT NULL PRIMARY KEY) AS
                        {doc_id_subquery};
                        """
            temp_table_drop_query = f"DROP TABLE IF EXISTS {temp_table_path};"
            doc_id_subquery = f"SELECT doc_id FROM {temp_table_path}"
            uses_temp_affected_docs_table = True
            delete_query = f"""
                    DELETE FROM {target_table_path}
                     WHERE doc_id IN ({doc_id_subquery})
                    """
            columns = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id'] + obj_fields + ['semantic_score', 'row_score', 'row_rank']

            # Insert freshly ranked forward rows from the matrix.
            insert_forward = f"""
                        INSERT INTO {target_table_path}
                                     ({', '.join(columns)})
                        WITH affected_docs AS (
                            {doc_id_subquery}
                        ),
                        ranked AS (
                              SELECT s.from_object_type AS  doc_type, s.from_object_id AS doc_id,
                                       s.to_object_type AS link_type, 'Semantic' AS link_subtype, s.to_object_id AS link_id,
                                      {', '.join([f'i.{c}' for c in obj_fields])}{',' if obj_fields else ''}
                                      s.score AS semantic_score,
                                      1/2 + 1/(1+row_number() OVER (PARTITION BY s.from_object_id ORDER BY {order_by})) AS row_score,
                                                 row_number() OVER (PARTITION BY s.from_object_id ORDER BY {order_by}) AS row_rank
                                FROM {scoresmatrix_table_path} s
                          INNER JOIN {buildup_link_table_path} i
                                  ON (s.from_object_type, s.to_object_type, s.to_object_id) = ("{key.doc_type}", "{key.link_type}", i.doc_id)
                          INNER JOIN affected_docs ad
                                  ON s.from_object_id = ad.doc_id
                                WHERE s.from_object_type = "{key.doc_type}"
                                  AND s.to_object_type   = "{key.link_type}"
                                  AND s.deleted = 0
                                  AND s.score IS NOT NULL
                                  AND s.to_object_id IS NOT NULL
                                  AND {self._order_by_null_filter(policy, 'i')}
                         )
                          SELECT {', '.join(columns)}
                            FROM ranked
                           WHERE semantic_score >= 0.1
                             AND row_rank <= {row_rank_thr}
                          """

            # Insert freshly ranked flipped rows from the matrix.
            insert_flipped = f"""
                        INSERT INTO {target_table_path}
                                     ({', '.join(columns)})
                        WITH affected_docs AS (
                            {doc_id_subquery}
                        ),
                        ranked AS (
                              SELECT   s.to_object_type AS  doc_type, s.to_object_id AS doc_id,
                                     s.from_object_type AS link_type, 'Semantic' AS link_subtype, s.from_object_id AS link_id,
                                      {', '.join([f'i.{c}' for c in obj_fields])}{',' if obj_fields else ''}
                                      s.score AS semantic_score,
                                      1/2 + 1/(1+row_number() OVER (PARTITION BY s.to_object_id ORDER BY {order_by})) AS row_score,
                                                 row_number() OVER (PARTITION BY s.to_object_id ORDER BY {order_by}) AS row_rank
                                FROM {scoresmatrix_table_path} s
                          INNER JOIN {buildup_link_table_path} i
                                  ON (s.to_object_type, s.from_object_type, s.from_object_id) = ("{key.doc_type}", "{key.link_type}", i.doc_id)
                          INNER JOIN affected_docs ad
                                  ON s.to_object_id = ad.doc_id
                                WHERE s.to_object_type   = "{key.doc_type}"
                                  AND s.from_object_type = "{key.link_type}"
                                  AND s.deleted = 0
                                  AND s.score IS NOT NULL
                                  AND s.from_object_id IS NOT NULL
                                  AND {self._order_by_null_filter(policy, 'i')}
                         )
                          SELECT {', '.join(columns)}
                            FROM ranked
                           WHERE semantic_score >= 0.1
                             AND row_rank <= {row_rank_thr}
                          """

            # Self-loop semantic edges have identical forward and flipped
            # inserts; keeping both would violate the unique key.
            if key.doc_type == key.link_type:
                insert_flipped = None

        #---------------------------#
        # Evaluation and execution #
        #---------------------------#

        # The score column and the re-score condition of the evaluation.
        score_col = 'semantic_score' if key.partition == 'SEM' else 'degree_score'
        re_score_condition = (
            f"ABS(e.{score_col} - t.{score_col}) > 0.01 "
            f"OR COALESCE(e.row_rank, -1) != COALESCE(t.row_rank, -1) "
            f"OR ABS(COALESCE(e.row_score, 0) - COALESCE(t.row_score, 0)) > 0.0001"
        )

        # Internal Function: wrap one insert query as an evaluation count.
        def _eval_query(insert_query: str) -> str:
            stripped = re.sub(r'(?:REPLACE|INSERT)\s+INTO[^\(\)]*\([^\(\)]*\)', '', insert_query)
            return f"""
                        SELECT COUNT(*) AS rows_to_insert, COALESCE(SUM(e.{score_col} IS NOT NULL AND ({re_score_condition})),0) AS rows_to_re_score
                        FROM ({stripped}) t LEFT JOIN {target_table_path} e USING (doc_id, link_id)
                    """

        # Build the evaluation queries for the forward and flipped inserts.
        if insert_forward is not None:
            sql_query_eval_1 = _eval_query(insert_forward)
        else:
            sql_query_eval_1 = "SELECT 0 AS rows_to_insert, 0 AS rows_to_re_score"
        if insert_flipped is not None:
            sql_query_eval_2 = _eval_query(insert_flipped)
        else:
            sql_query_eval_2 = "SELECT 0 AS rows_to_insert, 0 AS rows_to_re_score"

        # Print the evaluation queries.
        if 'print' in actions:
            print(f"\n🔍 Evaluation queries for {target_table_path}:")
            print_sql(sql_query_eval_1, title='z0rFNfM5')
            print_sql(sql_query_eval_2, title='oxyoF81R')

        # Create the scratch table for affected doc ids when needed.
        if uses_temp_affected_docs_table and ('eval' in actions or 'commit' in actions):
            self.db.execute_query_in_shell(engine_name=engine_name, query=temp_table_create_query, verbose='print' in actions, query_id='TmpHpCreate')

        # Evaluate the patch operation.
        rows_to_insert, rows_to_re_score = 0, 0
        if 'eval' in actions:
            out_1 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_1, query_id='z0rFNfM5')
            out_2 = self.db.execute_query(engine_name=engine_name, query=sql_query_eval_2, query_id='oxyoF81R')
            rows_to_insert = (out_1[0][0] if out_1 else 0) + (out_2[0][0] if out_2 else 0)
            rows_to_re_score = (out_1[0][1] if out_1 else 0) + (out_2[0][1] if out_2 else 0)

        # Print the commit queries.
        if 'print' in actions:
            if uses_temp_affected_docs_table and temp_table_create_query:
                print_sql(temp_table_create_query, title='EHT42tk[tmp-create]')
            if delete_query:
                print_sql(delete_query, title='EHT42tk[del]')
            if insert_forward:
                print_sql(insert_forward, title='EHT42tk[fwd]')
            if insert_flipped:
                print_sql(insert_flipped, title='EHT42tk[flp]')
            if uses_temp_affected_docs_table and temp_table_drop_query:
                print_sql(temp_table_drop_query, title='EHT42tk[tmp-drop]')

        # Execute the commit queries; the delete must run before any insert
        # for the same doc_id slice, and nothing runs when the evaluation
        # found no work.
        total_rows = rows_to_insert + rows_to_re_score
        if 'commit' in actions and not ('eval' in actions and total_rows == 0):
            if delete_query:
                self.db.execute_query_in_shell(engine_name=engine_name, query=delete_query, verbose='print' in actions, query_id='Del6hv6h')
            if insert_forward:
                self.db.execute_query_in_shell(engine_name=engine_name, query=insert_forward, verbose='print' in actions, query_id='InsFwdKuT')
            if insert_flipped:
                self.db.execute_query_in_shell(engine_name=engine_name, query=insert_flipped, verbose='print' in actions, query_id='InsFlippedT1B')

        # Drop the scratch table for affected doc ids when one was created.
        if uses_temp_affected_docs_table and ('eval' in actions or 'commit' in actions):
            self.db.execute_query_in_shell(engine_name=engine_name, query=temp_table_drop_query, verbose='print' in actions, query_id='TmpHpDrop')
        return PropagationStats(target=target_table_path, rows_flagged=total_rows)

    #================================================================#
    # Method Group: Elasticsearch cache vertical patch               #
    #================================================================#

    # Public Method: Patch the denormalised link fields of the Elasticsearch
    # cache mirror of one projection, resolving content drift for the search
    # index export.
    def vertical_patch_es_cache(self, key: DocLinkTypeKey, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        _, es_cache_schema = self.schema_resolver.for_es_cache()

        # The cache patch refreshes the presentation columns and the
        # Elasticsearch-specific link fields of the link type.
        obj_fields = list(
            self.index_config.settings.get("elasticsearch", {}).get("fields", {}).get("links", {}).get(key.link_type, [])
        )
        target_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}"
        target_table_path = f"{es_cache_schema}.{target_table_name}"
        buildup_link_table_path = f"{cache_schema}.IndexBuildup_Fields_Docs_{key.link_type}"

        # Ensure the buildup and target tables exist before any early return,
        # compensating for the legacy's constructor-time table creation.
        create_table_if_not_exists(self.db, engine_name, cache_schema, f"IndexBuildup_Fields_Docs_{key.link_type}")
        create_table_if_not_exists(self.db, engine_name, es_cache_schema, target_table_name)

        # Without configured fields there is nothing to patch.
        if len(obj_fields) == 0:
            if 'print' in actions:
                sysmsg.trace(f"No fields to patch for doc-link type '{key.doc_type} --> {key.link_type}'.")
            return PropagationStats(target=target_table_path, rows_flagged=0)

        # Evaluation counts the drifted presentation columns against the
        # page profile values, applying the include-code option to the names.
        name_en = "IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value)"
        name_fr = "IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value)"
        compare_conditions = " OR ".join(
            [f'COALESCE(t.link_name_en, "__null__") != COALESCE({name_en}, "__null__")',
             f'COALESCE(t.link_name_fr, "__null__") != COALESCE({name_fr}, "__null__")',
             'COALESCE(t.link_short_description_en, "__null__") != COALESCE(p.description_short_en_value, "__null__")',
             'COALESCE(t.link_short_description_fr, "__null__") != COALESCE(p.description_short_fr_value, "__null__")']
            + [f'COALESCE(t.{c}, "__null__") != COALESCE(l.{c}, "__null__")' for c in obj_fields]
        )
        sql_query_eval = f"""
              SELECT COUNT(*) AS n_total, COALESCE(SUM({compare_conditions}), 0) AS n_patch
                FROM {cache_schema}.Data_N_Object_T_PageProfile p
           LEFT JOIN {target_table_path} t
                  ON (t.link_type, t.link_id) = (p.object_type, p.object_id)
          INNER JOIN {buildup_link_table_path} l
                  ON (t.link_type, t.link_id) = (l.doc_type, l.doc_id)
               WHERE p.object_type = '{key.link_type}'
                 AND (p.to_process = 1 OR l.to_process = 1)
        """

        # The evaluation query runs for eval and commit alike.
        rows_to_process, rows_to_patch = 0, 0
        if 'commit' in actions or 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_query_eval, title='AFSGljr')
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='XL1265bE')
            out = out if type(out) is list else [[0, 0]]
            rows_to_process, rows_to_patch = out[0]
        if 'eval' in actions and rows_to_patch == 0 and 'print' in actions:
            sysmsg.warning(f"No rows to patch in table '{target_table_name}'.")

        # Commit query: refresh the presentation columns from the profile
        # and buildup values.
        set_clause = f"""
                       SET t.link_name_en = {name_en},
                           t.link_name_fr = {name_fr},
                           t.link_short_description_en = p.description_short_en_value,
                           t.link_short_description_fr = p.description_short_fr_value"""
        if len(obj_fields) > 0:
            set_clause += ", " + ", ".join([f't.{c} = l.{c}' for c in obj_fields])
        sql_query_commit = f"""
                    UPDATE {target_table_path} t
                INNER JOIN {cache_schema}.Data_N_Object_T_PageProfile p
                        ON (t.link_type, t.link_id) = (p.object_type, p.object_id)
                INNER JOIN {buildup_link_table_path} l
                        ON (t.link_type, t.link_id) = (l.doc_type, l.doc_id)
                    {set_clause}
                      WHERE p.object_type = '{key.link_type}'
                        AND (p.to_process = 1 OR l.to_process = 1)
                """
        if 'print' in actions:
            print_sql(sql_query_commit, title='Z16jRm9j')

        # The commit runs chunked, with the chunk filter scoping boundary
        # discovery to rows with a pending profile or buildup counterpart.
        if 'commit' in actions and rows_to_patch > 0:
            self.db.execute_query_in_chunks(
                engine_name   = engine_name,
                schema_name   = es_cache_schema,
                table_name    = target_table_name,
                query         = sql_query_commit,
                chunk_filter  = f"EXISTS (SELECT 1 FROM {cache_schema}.Data_N_Object_T_PageProfile p INNER JOIN {buildup_link_table_path} l ON (l.doc_type, l.doc_id) = (p.object_type, p.object_id) WHERE (p.object_type, p.object_id) = (link_type, link_id) AND p.object_type = '{key.link_type}' AND (p.to_process = 1 OR l.to_process = 1))",
                chunk_size    = 10000,
                row_id_name   = 't.row_id',
                query_id      = 'Z16jRm9j',
                verbose       = 'print' in actions,
            )
        return PropagationStats(target=target_table_path, rows_flagged=rows_to_patch)

    #================================================================#
    # Method Group: Elasticsearch cache horizontal patch             #
    #================================================================#

    # Internal Method: Ensure the mixed view of one doc-link pair exists,
    # mirroring the legacy _ensure_mixed_view_exists: only pairs configured
    # for mixed scoring carry a view, stale views of unconfigured pairs are
    # dropped, and the view is created when both source tables exist.
    def _ensure_mixed_view(self, key: DocLinkTypeKey) -> bool:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        table_name_org = f"Index_D_{key.doc_type}_L_{key.link_type}_T_ORG"
        table_name_sem = f"Index_D_{key.doc_type}_L_{key.link_type}_T_SEM"
        table_name_mix = f"Index_D_{key.doc_type}_L_{key.link_type}_T_MIX"

        # Only the pairs configured for mixed scoring carry a view; the
        # configured list covers both directions of each pair.
        configured_mix_pairs = set(self.scores_config.settings.get('mixed_scoring_tuples', []))
        if (key.doc_type, key.link_type) not in configured_mix_pairs:

            # Drop a stale view so the patch falls back to the correct
            # ORG or SEM branch instead of querying an invalid view.
            if self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=table_name_mix, exclude_views=False):
                sysmsg.info(f"🗑️  Dropping stale MIX view for {key.doc_type} --> {key.link_type} (not in configured SEM∩ORG pairs).")
                self.db.execute_query_in_shell(
                    engine_name = engine_name,
                    query       = f"DROP VIEW IF EXISTS {search_schema}.{table_name_mix}",
                    query_id    = 'xY7gHv2K',
                )
            return False

        # The view requires both source tables.
        table_exists_org = self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=table_name_org)
        table_exists_sem = self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=table_name_sem)
        if not (table_exists_org and table_exists_sem):
            return False

        # An existing view is reused.
        if self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=table_name_mix, exclude_views=False):
            return True

        # Create the view: the organisational ranks first, then the semantic
        # ranks offset by the organisational maximum per document.
        sysmsg.info(f"🛠️  Creating missing MIX view for {key.doc_type} --> {key.link_type}.")
        list_of_columns_sem = self.db.get_column_names(engine_name=engine_name, schema_name=search_schema, table_name=table_name_sem)
        if 'row_id' in list_of_columns_sem:
            list_of_columns_sem.remove('row_id')
        list_of_columns_org = ['degree_score' if c == 'semantic_score' else c for c in list_of_columns_sem]
        _, cache_schema = self.schema_resolver.for_graph_cache()
        create_view_query = f"""
            CREATE OR REPLACE VIEW {search_schema}.{table_name_mix} AS

                            SELECT {', '.join(list_of_columns_org)}, (s.row_rank) AS adjusted_row_rank
                              FROM {search_schema}.{table_name_org} s
                        INNER JOIN (SELECT doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                       FROM {search_schema}.{table_name_org}
                                   GROUP BY doc_type, doc_id) o
                              USING (doc_type, doc_id)
                              WHERE doc_id IN (SELECT doc_id FROM {cache_schema}.IndexBuildup_Fields_Docs_{key.doc_type} WHERE to_process = 1)

                          UNION ALL

                            SELECT {', '.join(list_of_columns_sem)}, (s.row_rank + COALESCE(o.max_row_rank, 0)) AS adjusted_row_rank
                              FROM {search_schema}.{table_name_sem} s
                          LEFT JOIN (SELECT doc_type, doc_id, MAX(row_rank) AS max_row_rank
                                       FROM {search_schema}.{table_name_org}
                                   GROUP BY doc_type, doc_id) o
                              USING (doc_type, doc_id)
                              WHERE (s.doc_type, s.doc_id, s.link_type, s.link_id)
                                       NOT IN (SELECT doc_type, doc_id, link_type, link_id FROM {search_schema}.{table_name_org})
                                AND doc_id IN (SELECT doc_id FROM {cache_schema}.IndexBuildup_Fields_Docs_{key.doc_type} WHERE to_process = 1)

                           ORDER BY doc_id ASC, adjusted_row_rank ASC;
            """
        self.db.execute_query_in_shell(engine_name=engine_name, query=create_view_query, query_id='tb1Vdfyq')
        return True

    # Public Method: Patch and re-rank the links of the Elasticsearch cache
    # mirror of one projection, resolving membership and ranking drift for
    # the search index export.
    def horizontal_patch_es_cache(self, key: DocLinkTypeKey, policy: LinkSelectionPolicy, actions: ActionSet = ('commit',)) -> PropagationStats:
        engine_name, _airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        _, search_schema = self.schema_resolver.for_graphsearch_test()
        _, es_cache_schema = self.schema_resolver.for_es_cache()
        es_filters = list(
            self.index_config.settings.get("elasticsearch", {}).get("filters", {}).get("links", {}).get(key.link_type, [])
        )
        obj_fields = list(
            self.index_config.settings.get("elasticsearch", {}).get("fields", {}).get("links", {}).get(key.link_type, [])
        )
        target_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}"
        target_table_path = f"{es_cache_schema}.{target_table_name}"

        # The legacy cache patch truncates at half the SQL-side threshold;
        # the policy carries the caller's threshold, unbounded mapped onto
        # the legacy sentinel.
        row_rank_thr = policy.rank_threshold if policy.rank_threshold is not None else 9999999

        # Ensure the mixed view exists before resolving the source table.
        self._ensure_mixed_view(key)

        # Resolve the source table: the mixed view when present, otherwise
        # the organisational or semantic projection table.
        if self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=f"Index_D_{key.doc_type}_L_{key.link_type}_T_MIX", exclude_views=False):
            if 'print' in actions:
                sysmsg.trace(f"Using MIX table for {key.doc_type} --> {key.link_type}")
            source_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_MIX"
            score_column_name = 'adjusted_row_rank'
        elif self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=f"Index_D_{key.doc_type}_L_{key.link_type}_T_ORG", exclude_views=True):
            source_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_ORG"
            score_column_name = 'row_rank'
        elif self.db.table_exists(engine_name=engine_name, schema_name=search_schema, table_name=f"Index_D_{key.doc_type}_L_{key.link_type}_T_SEM", exclude_views=True):
            source_table_name = f"Index_D_{key.doc_type}_L_{key.link_type}_T_SEM"
            score_column_name = 'row_rank'
        else:
            return PropagationStats(target=target_table_path, rows_flagged=0)

        # Build the affected-doc-ids statement per source family: the mixed
        # and organisational tables read the parent-child edges (and, for the
        # mixed view, the scores matrix); the semantic tables read the final
        # scores or the scores matrix.
        if source_table_name.endswith('_T_ORG'):
            to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {cache_schema}.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{key.doc_type}", "{key.link_type}")
                                    AND to_process = 1
                        """
        elif source_table_name.endswith('_T_SEM'):
            if self._is_ontology_object_edge(key):
                final_scores_table, ontology_id_col, ontology_type, object_type = self._ontology_final_scores_source(key)
                if key.doc_type in ('Concept', 'Category'):
                    to_process_sql_statement = f"""
                                SELECT DISTINCT '{ontology_type}' AS doc_type, {ontology_id_col} AS doc_id
                                               FROM {final_scores_table}
                                              WHERE object_type = '{object_type}'
                                                AND to_process = 1
                                                AND deleted = 0
                            """
                else:
                    to_process_sql_statement = f"""
                                SELECT DISTINCT '{object_type}' AS doc_type, object_id AS doc_id
                                               FROM {final_scores_table}
                                              WHERE object_type = '{object_type}'
                                                AND to_process = 1
                                                AND deleted = 0
                            """
            else:
                try:
                    scores_matrix_table_name_as = self._scores_matrix_table_name(key, 'AS')
                except ValueError:
                    scores_matrix_table_name_as = None
                if scores_matrix_table_name_as is None:
                    sysmsg.warning(
                        f"Skipping ES horizontal patch for {key.doc_type} --> {key.link_type} [SEM]: "
                        f"no scores matrix table mapping found."
                    )
                    return PropagationStats(target=target_table_path, rows_flagged=0)
                to_process_sql_statement = f"""
                            SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                       FROM {cache_schema}.{scores_matrix_table_name_as}
                                      WHERE (from_object_type, to_object_type) = ("{key.doc_type}", "{key.link_type}")
                                        AND to_process = 1
                                      UNION
                            SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                       FROM {cache_schema}.{scores_matrix_table_name_as}
                                      WHERE (to_object_type, from_object_type) = ("{key.doc_type}", "{key.link_type}")
                                        AND to_process = 1
                        """
        else:

            # The mixed view reads both the parent-child edges and the
            # scores matrix, in both directions of the pair.
            try:
                scores_matrix_table_name_as = self._scores_matrix_table_name(key, 'AS')
            except ValueError:
                scores_matrix_table_name_as = None
            to_process_sql_statement = f"""
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {cache_schema}.Edges_N_Object_N_Object_T_ParentChildSymmetric
                                  WHERE (from_object_type, to_object_type) = ("{key.doc_type}", "{key.link_type}")
                                    AND to_process = 1
                        """
            if scores_matrix_table_name_as is not None:
                to_process_sql_statement += f"""
                                  UNION
                        SELECT DISTINCT from_object_type AS doc_type, from_object_id AS doc_id
                                   FROM {cache_schema}.{scores_matrix_table_name_as}
                                  WHERE (from_object_type, to_object_type) = ("{key.doc_type}", "{key.link_type}")
                                     AND to_process = 1
                                  UNION
                        SELECT DISTINCT to_object_type AS doc_type, to_object_id AS doc_id
                                   FROM {cache_schema}.{scores_matrix_table_name_as}
                                  WHERE (to_object_type, from_object_type) = ("{key.doc_type}", "{key.link_type}")
                                     AND to_process = 1
                        """

        # Ensure the target table exists before patching.
        create_table_if_not_exists(self.db, engine_name, es_cache_schema, target_table_name)

        # The link queries join the graphsearch doc tables of both endpoint
        # types. The legacy created every Index_D_* table eagerly at
        # construction, so inactive endpoint types (e.g. Exercise or Startup
        # as link types) never missed their table; create them lazily here
        # instead (E2E finding, 2026-09-24).
        create_table_if_not_exists(self.db, engine_name, search_schema, f"Index_D_{key.doc_type}")
        create_table_if_not_exists(self.db, engine_name, search_schema, f"Index_D_{key.link_type}")

        # Commit query: insert the ranked links with their presentation
        # fields, updating the rank on duplicate keys when it drifted.
        columns = ['doc_type', 'doc_id', 'link_type', 'link_subtype', 'link_id', 'link_rank',
                   'link_name_en', 'link_name_fr', 'link_short_description_en', 'link_short_description_fr'] + obj_fields
        name_en = "IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_en_value), p.name_en_value)"
        name_fr = "IF(l.include_code_in_name=1, CONCAT(l.doc_id, ': ', p.name_fr_value), p.name_fr_value)"
        select_values = [
            'd.doc_type', 'd.doc_id', 'dl.link_type', 'dl.link_subtype', 'dl.link_id',
            f'dl.{score_column_name} AS link_rank',
            f'{name_en} AS link_name_en',
            f'{name_fr} AS link_name_fr',
            'p.description_short_en_value AS link_short_description_en',
            'p.description_short_fr_value AS link_short_description_fr',
        ] + [f'l.{c}' for c in obj_fields]
        filter_clause = ""
        if len(es_filters) > 0:
            filter_clause = "AND " + " AND ".join([f'l.{f}' for f in es_filters])
        sql_query_commit = f"""
                    INSERT INTO {target_table_path}
                                ({', '.join(columns)})
                         SELECT {', '.join(select_values)}
                           FROM {search_schema}.Index_D_{key.doc_type} d
                     INNER JOIN {search_schema}.{source_table_name} dl
                          USING (doc_type, doc_id)
                     INNER JOIN {search_schema}.Index_D_{key.link_type} l
                             ON (dl.link_type, dl.link_id) = (l.doc_type, l.doc_id)
                     INNER JOIN {search_schema}.Data_N_Object_T_PageProfile p
                             ON (p.object_type, p.object_id) = (l.doc_type, l.doc_id)
                     INNER JOIN (
                                {to_process_sql_statement}
                                ) tp
                             ON (dl.doc_type, dl.doc_id) = (tp.doc_type, tp.doc_id)
                          WHERE dl.row_rank <= {row_rank_thr}
                                {filter_clause}
                ON DUPLICATE KEY
                         UPDATE {target_table_path}.link_rank = IF(COALESCE({target_table_path}.link_rank, "__null__") != COALESCE(dl.{score_column_name}, "__null__"), dl.{score_column_name}, {target_table_path}.link_rank);
                """

        # Evaluation query: the optimised straight-join count of the legacy
        # (the earlier regex-derived variant was dead code, immediately
        # overridden); the FORCE INDEX hint applies to the plain tables only.
        force_index = "" if source_table_name.endswith('_T_MIX') else f"FORCE INDEX (idx_doc_rank_link)"
        sql_query_eval = f"""
                    SELECT
                        COALESCE(SUM(e.row_id IS NULL), 0) AS rows_to_insert,
                        COALESCE(
                            SUM(
                                e.row_id IS NOT NULL
                                AND e.link_rank <> dl.{score_column_name}
                            ),
                            0
                        ) AS rows_to_replace
                    FROM (
                        {to_process_sql_statement}
                    ) AS tp

                    STRAIGHT_JOIN {search_schema}.{source_table_name} AS dl
                        {force_index}
                        ON dl.doc_type = tp.doc_type
                    AND dl.doc_id = tp.doc_id
                    AND dl.row_rank <= {row_rank_thr}

                    INNER JOIN {search_schema}.Index_D_{key.doc_type} AS d
                        ON d.doc_type = dl.doc_type
                    AND d.doc_id = dl.doc_id

                    INNER JOIN {search_schema}.Index_D_{key.link_type} AS l
                        ON l.doc_type = dl.link_type
                    AND l.doc_id = dl.link_id

                    INNER JOIN {search_schema}.Data_N_Object_T_PageProfile AS p
                        ON p.object_type = l.doc_type
                    AND p.object_id = l.doc_id

                    LEFT JOIN {target_table_path} AS e
                        ON e.doc_type = dl.doc_type
                    AND e.doc_id = dl.doc_id
                    AND e.link_type = dl.link_type
                    AND e.link_subtype = dl.link_subtype
                    AND e.link_id = dl.link_id

                    WHERE 1 = 1
                        {"AND" if es_filters else ""}
                        {" AND ".join(f'l.{f}' for f in es_filters)}
                """

        # The evaluation query runs for eval and commit alike.
        rows_to_patch = 0
        if 'commit' in actions or 'eval' in actions:
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='FJ9HVCLW')
            out = [int(out[0][0]), int(out[0][1])] if type(out) is list and out else [0, 0]
            rows_to_patch = sum(out)
        if 'eval' in actions:
            if 'print' in actions:
                print(f"\n🔍 Evaluation query for {target_table_path}:")
                print_sql(sql_query_eval, title='LLzeD3NV')
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_eval, query_id='LLzeD3NV')
            if out and sum(int(v) for v in out[0] if v is not None) > 0:
                sysmsg.trace(f"🔍 Evaluation results for {target_table_path}: {out[0]}")

        # Execute the commit query; nothing runs when the evaluation found
        # no work.
        if 'commit' in actions and not ('eval' in actions and rows_to_patch == 0):
            if 'print' in actions:
                print_sql(sql_query_commit, title='zwRx2b8a')
            self.db.execute_query_in_shell(engine_name=engine_name, query=sql_query_commit, query_id='zwRx2b8a')
        return PropagationStats(target=target_table_path, rows_flagged=rows_to_patch)
