# graphregistry/adapters/persistence/mysql/repositories/rpo_scoresmatrix.py
from __future__ import annotations
from typing import TYPE_CHECKING
from loguru import logger as sysmsg
from graphdb.models.sqlquery import print_sql
from graphregistry.application.ports.repositories.prt_scoresmatrix import ScoresMatrixRepository
from graphregistry.application.ports.repositories.resolvers import SchemaResolver
from graphregistry.common.config import GlobalConfig, ScoresConfig
from graphregistry.domain.models.pipeline.mdl_policies import ProcessingScope
from graphregistry.domain.models.pipeline.mdl_scores import ScoreConsolidationParams
from graphregistry.domain.models.pipeline.mdl_stats import PropagationStats
from graphregistry.domain.models.values.mdl_typepairs import ONTOLOGY_OBJECT_TYPES, EdgeTypePair
from graphregistry.domain.types import ActionSet, ScoreMatrixKind

# Import GraphDB only for type checking so importing this module never opens a
# database connection; the client is injected by the entrypoint wiring.
if TYPE_CHECKING:
    from graphdb.core.graphdb import GraphDB

#==================#
# Class Definition #
#==================#
class MySQLScoresMatrixRepository(ScoresMatrixRepository):
    """MySQL adapter for the ScoresMatrixRepository port.

    The SQL statements and query ids are extracted verbatim from the legacy
    GraphRegistry.CacheManagement calculate_scores_matrix and
    consolidate_scores_matrix methods and its update_scores_matrix command.
    The legacy hardcoded calculation thresholds (0.1 score, 4 shared concepts)
    are substituted from the ScoreConsolidationParams; with the default
    params the generated SQL is byte-identical.
    """

    # Public Method: Initialize the repository with an injected database client,
    # schema resolver, and configurations; no module-level connections.
    def __init__(self, db: "GraphDB", schema_resolver: SchemaResolver, global_config: GlobalConfig, scores_config: ScoresConfig, verbose: bool = False) -> None:
        self.db = db
        self.schema_resolver = schema_resolver
        self.global_config = global_config
        self.scores_config = scores_config
        self.verbose = verbose

    #================================================================#
    # Method Group: Table naming                                     #
    #================================================================#

    # Internal Method: Resolve the scores matrix table name of an edge family
    # and matrix kind, mirroring the legacy get_scores_matrix_table_name:
    # ontology tuples always use the Ontology tables, the others resolve the
    # education or research domain from the scores configuration mapping.
    def _matrix_table_name(self, type_pair: EdgeTypePair, kind: ScoreMatrixKind) -> str:
        canonical = type_pair.canonical_order
        domain = canonical.matrix_domain(self.scores_config.settings['scored_edge_tuple_to_class_mapping'])
        if domain is None:
            raise ValueError(
                f"Invalid input: ({canonical.from_object_type}, {canonical.to_object_type}, {kind}). "
                f"No corresponding scores matrix table found."
            )
        return f"Edges_N_Object_N_Object_T_ScoresMatrix_{domain.title()}_{kind}"

    #================================================================#
    # Method Group: Calculation                                      #
    #================================================================#

    # Public Method: Rebuild the group-by-concepts matrix of one edge family
    # from the object-to-concept final scores, applying the consolidation
    # thresholds. Returns None when the pair is excluded from calculation, as
    # the ontology tuples are.
    def calculate_matrix(self, type_pair: EdgeTypePair, params: ScoreConsolidationParams, actions: ActionSet = ('commit',)) -> PropagationStats | None:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        canonical = type_pair.canonical_order
        from_type = canonical.from_object_type
        to_type = canonical.to_object_type

        # Ignore edge types Object-to-Concept/Category/Curated area (not
        # including Category-to-Category): these are consolidated from the
        # pre-calculated object-to-ontology final scores instead.
        if (from_type in ONTOLOGY_OBJECT_TYPES or to_type in ONTOLOGY_OBJECT_TYPES) and (from_type, to_type) != ('Category', 'Category'):
            return None

        # Without actions there is nothing to do, as the legacy command warns.
        if len(actions) == 0:
            sysmsg.warning("No actions specified. Supported actions are: 'print', 'eval', 'commit'.")
            sysmsg.info("🚀 📝 Nothing to do.")
            return None
        if 'eval' in actions and 'commit' not in actions:
            sysmsg.warning("Executing in evaluation mode only.")

        # Resolve the group-by-concepts table the calculation writes into.
        gbc_table_name = self._matrix_table_name(canonical, 'GBC')

        # Estimated number of pairs to score: same-type families multiply the
        # flagged rows by the full rows; mixed families count both directions.
        if from_type == to_type:
            sql_eval_query = f"""
                        SELECT object_type AS from_object_type, object_type AS to_object_type, SUM(to_process) * COUNT(*) AS estimated_n_to_process
                          FROM {airflow_schema}.Operations_N_Object_T_ScoresExpired
                         WHERE object_type = '{from_type}'
                           AND deleted = 0
                    """
        else:
            sql_eval_query = f"""
                        SELECT t1.object_type AS from_object_type, t2.object_type AS to_object_type,
                               t1.n_to_process * t2.n_count + t1.n_count * t2.n_to_process AS estimated_n_to_process
                          FROM (SELECT '_' AS id, object_type, SUM(to_process) AS n_to_process, COUNT(*) AS n_count
                                  FROM  {airflow_schema}.Operations_N_Object_T_ScoresExpired
                                 WHERE object_type = '{from_type}'
                                   AND deleted = 0) t1
                    INNER JOIN (SELECT '_' AS id, object_type, SUM(to_process) AS n_to_process, COUNT(*) AS n_count
                                  FROM  {airflow_schema}.Operations_N_Object_T_ScoresExpired
                                 WHERE object_type = '{to_type}'
                                   AND deleted = 0) t2
                         USING (id)
                    """

        # Commit query: aggregate the shared-concept products into the matrix,
        # keeping the FORCE INDEX hint of the legacy query.
        sql_commit_query = f"""
                     REPLACE INTO {cache_schema}.{gbc_table_name}
                                  (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process, deleted)

                           SELECT e1.object_type     AS from_object_type,
                                  e1.object_id       AS from_object_id,
                                  e2.object_type     AS to_object_type,
                                  e2.object_id       AS to_object_id,
                                  SUM(e1.score*e2.score) AS score, 1 AS to_process, 0 AS deleted

                             FROM {cache_schema}.Edges_N_Object_N_Concept_T_FinalScores e1
                       INNER JOIN {cache_schema}.Edges_N_Object_N_Concept_T_FinalScores e2
                      FORCE INDEX (idx_concept_type_proc_score)
                             USING (concept_id)

                              WHERE e1.object_type = "{from_type}"
                                AND e2.object_type = "{to_type}"

                                AND e1.to_process = 1
                                AND e2.to_process = 1

                                AND e1.deleted = 0
                                AND e2.deleted = 0

                                AND e1.score >= {params.score_threshold}
                                AND e2.score >= {params.score_threshold}

                               AND ((e1.object_type = e2.object_type AND e1.object_id < e2.object_id) OR (e1.object_type != e2.object_type))

                          GROUP BY e1.object_type, e1.object_id,
                                   e2.object_type, e2.object_id

                            HAVING COUNT(DISTINCT e1.concept_id) >= {params.min_shared_concepts}
                               AND SUM(e1.score*e2.score) >= {params.score_threshold}
                    """

        # Evaluate the estimated pair count when requested.
        stats = PropagationStats(target=f"{cache_schema}.{gbc_table_name}", rows_flagged=0)
        if 'eval' in actions:
            if 'print' in actions:
                print_sql(sql_eval_query, title='f3LmCRzV')
            rows = self.db.execute_query(engine_name=engine_name, query=sql_eval_query, query_id='f3LmCRzV')
            count = rows[0][2] if rows and len(rows) > 0 and rows[0] else 0
            stats.rows_flagged = count
            sysmsg.trace(f"  ~ {count} pairs estimated for ({from_type}, {to_type})")

        # Print and commit the matrix rebuild when requested.
        if 'print' in actions:
            print_sql(sql_commit_query, title='wAbL4D8i')
        if 'commit' in actions:
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = sql_commit_query,
                verbose     = self.verbose,
                query_id    = 'wAbL4D8i',
            )
        return stats

    #================================================================#
    # Method Group: Consolidation                                    #
    #================================================================#

    # Public Method: Consolidate the adjusted-scores matrix of one edge
    # family, optionally refreshing the rolling averages, mirroring the
    # legacy consolidate_scores_matrix method.
    def consolidate_matrix(self, type_pair: EdgeTypePair, params: ScoreConsolidationParams, update_averages: bool = False, actions: ActionSet = ('commit',)) -> None:
        engine_name, airflow_schema = self.schema_resolver.for_airflow()
        _, cache_schema = self.schema_resolver.for_graph_cache()
        canonical = type_pair.canonical_order
        from_type = canonical.from_object_type
        to_type = canonical.to_object_type

        # Without actions there is nothing to do, as the legacy command warns.
        if len(actions) == 0:
            sysmsg.warning("No actions specified. Nothing to do.")
            return
        if 'eval' in actions and 'commit' not in actions:
            sysmsg.warning("Executing in evaluation mode only.")

        # Resolve both matrix tables the consolidation reads and writes.
        gbc_table_name = self._matrix_table_name(canonical, 'GBC')
        as_table_name = self._matrix_table_name(canonical, 'AS')

        # Ontology object-to-X families (excluding the same-type pairs) copy
        # from the pre-calculated object-to-ontology final scores tables.
        same_type_ontology = (from_type, to_type) in (('Category', 'Category'), ('Concept', 'Concept'), ('Curated area', 'Curated area'))
        if (from_type in ONTOLOGY_OBJECT_TYPES or to_type in ONTOLOGY_OBJECT_TYPES) and not same_type_ontology:

            # The ontology type names the final scores table and its id column.
            ontology_type = from_type if from_type in ONTOLOGY_OBJECT_TYPES else to_type
            id_column = f"{ontology_type.lower().replace(' ', '_')}_id"
            table_suffix = ontology_type.title().replace(' ', '')
            sql_query = f"""
                    REPLACE INTO {cache_schema}.{as_table_name}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process, deleted)
                          SELECT object_type    AS from_object_type,
                                 object_id      AS from_object_id,
                                 '{ontology_type}' AS to_object_type,
                                 {id_column} AS to_object_id,
                                 score, to_process, 0 AS deleted
                            FROM {cache_schema}.Edges_N_Object_N_{table_suffix}_T_FinalScores
                           WHERE to_process = 1
                             AND deleted = 0
                             AND score >= {params.score_threshold}
                """

        # Concept-to-concept families consolidate the undirected concept edges
        # flagged on either endpoint.
        elif (from_type, to_type) == ('Concept', 'Concept'):
            sql_query = f"""
                    REPLACE INTO {cache_schema}.{as_table_name}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process, deleted)
                          SELECT 'Concept'        AS from_object_type,
                                 from_id          AS from_object_id,
                                 'Concept'        AS to_object_type,
                                 to_id            AS to_object_id,
                                 normalised_score AS score,
                                 s1.to_process OR s2.to_process AS to_process,
                                 0 AS deleted
                            FROM {self.global_config.schema_ontology}.Edges_N_Concept_N_Concept_T_Undirected c

                       INNER JOIN {airflow_schema}.Operations_N_Object_T_ScoresExpired s1
                              ON s1.object_id = c.from_id

                       INNER JOIN {airflow_schema}.Operations_N_Object_T_ScoresExpired s2
                              ON s2.object_id = c.to_id

                          WHERE s1.object_type = 'Concept'
                            AND s2.object_type = 'Concept'
                            AND (s1.to_process = 1 OR s2.to_process = 1)
                            AND s1.deleted = 0
                            AND s2.deleted = 0
                            AND c.record_deleted = 0
                            AND normalised_score >= {params.score_threshold}
                """

        # All other families, including Category-to-Category, derive the
        # adjusted scores from the group-by-concepts matrix and its averages.
        else:

            # Refresh the rolling averages first when requested.
            if update_averages:
                sql_query_avg = f"""
                    REPLACE INTO {cache_schema}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                                (from_object_type, to_object_type, avg_score, n_rows)
                           SELECT from_object_type, to_object_type,
                                  AVG(score) AS avg_score, COUNT(*) AS n_rows
                             FROM {cache_schema}.{gbc_table_name}
                            WHERE from_object_type = '{from_type}'
                              AND to_object_type   = '{to_type}'
                              AND deleted = 0
                         GROUP BY from_object_type, to_object_type
                    """
                if 'print' in actions:
                    print_sql(sql_query_avg, title='gs1ieZYM')
                if 'commit' in actions:
                    self.db.execute_query_in_shell(
                        engine_name = engine_name,
                        query       = sql_query_avg,
                        verbose     = 'print' in actions,
                        query_id    = 'gs1ieZYM',
                    )

            # An average score must exist before the adjusted scores can be
            # derived; warn and skip the family otherwise.
            sql_query_check = f"""
                    SELECT * FROM {cache_schema}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG
                     WHERE (from_object_type, to_object_type)
                         = ('{from_type}', '{to_type}');
                """
            out = self.db.execute_query(engine_name=engine_name, query=sql_query_check, query_id='fTaH8sTj')
            if len(out) == 0:
                sysmsg.warning(f'\nNo average score calculation available for ({from_type}, {to_type})')
                return

            # Adjusted scores: the sigmoid normalisation against the average.
            sql_query = f"""
                    REPLACE INTO {cache_schema}.{as_table_name}
                                 (from_object_type, from_object_id, to_object_type, to_object_id, score, to_process, deleted)
                           SELECT t.from_object_type, t.from_object_id, t.to_object_type, t.to_object_id, t.score, t.to_process, 0 AS deleted
                             FROM (SELECT gb.from_object_type, gb.from_object_id,
                                             gb.to_object_type,   gb.to_object_id,
                                           (2/(1 + EXP(-gb.score/(4 * av.avg_score))) - 1) AS score, gb.to_process
                                      FROM {cache_schema}.{gbc_table_name} gb
                                INNER JOIN {cache_schema}.Edges_N_Object_N_Object_T_ScoresMatrix_AVG av
                                        ON gb.from_object_type = av.from_object_type
                                       AND   gb.to_object_type = av.to_object_type
                                      WHERE gb.to_process = 1
                                        AND gb.deleted = 0
                                        AND gb.from_object_type = '{from_type}'
                                        AND gb.to_object_type   = '{to_type}'
                                    ) t
                             WHERE t.score >= {params.score_threshold}
                """

        # Print and commit the consolidation when requested.
        if 'print' in actions:
            print_sql(sql_query, title='qHE7tP6J')
        if 'commit' in actions:
            self.db.execute_query_in_shell(
                engine_name = engine_name,
                query       = sql_query,
                verbose     = 'print' in actions,
                query_id    = 'qHE7tP6J',
            )

    #================================================================#
    # Method Group: Combined update                                  #
    #================================================================#

    # Public Method: Compose calculation and consolidation over the edge
    # families selected by the processing scope and the scores configuration,
    # mirroring the legacy update_scores_matrix command.
    def update_matrix(self, scope: ProcessingScope, params: ScoreConsolidationParams, actions: ActionSet = ('commit',)) -> None:
        sysmsg.info("🧮 📝 Calculate and consolidate scores matrices.")

        # Active families: every combination of the scores-active node types.
        active_pairs = {pair.canonical_order.as_tuple for pair in scope.scores_matrix_pairs()}

        # Configured families from the scores configuration, canonicalised.
        config_pairs = set()
        for edge_class in ('education', 'research'):
            for pair in self.scores_config.settings['scored_edge_tuples'][edge_class]:
                config_pairs.add(EdgeTypePair.from_tuple(tuple(pair)).canonical_order.as_tuple)

        # Ontology families among the active pairs; the legacy command checks
        # Category and Concept only.
        ontology_pairs = {
            pair for pair in active_pairs
            if pair[0] in ('Category', 'Concept') or pair[1] in ('Category', 'Concept')
        }

        # Keep only the families active in the typeflags.
        edge_pairs = sorted((config_pairs | ontology_pairs) & active_pairs)
        if len(edge_pairs) == 0:
            sysmsg.warning("No object-to-object edge types to process for scores matrix calculation. Check TypeFlags configuration.")
            sysmsg.info("🧮 Nothing to do.")
            return

        # Print the list of families that will be rescored.
        print('\n[🐬 GraphSearch DB] [SM-DB] The following edges will be (re)scored:')
        for pair in edge_pairs:
            print(f" - {pair[0]} --> {pair[1]}")
        print('')

        # Calculation pass: the legacy command hardcoded the calculation
        # thresholds, so the default params are used here regardless of the
        # consolidation params requested by the caller.
        sysmsg.trace("⚙️  Calculating scores matrix for object-to-object edge combinations ...")
        default_params = ScoreConsolidationParams()
        for pair in edge_pairs:
            self.calculate_matrix(EdgeTypePair(from_object_type=pair[0], to_object_type=pair[1]), default_params, actions)

        # Consolidation pass with the caller's thresholds and averages.
        sysmsg.trace("⚙️  Consolidating scores matrix (normalising scores and inserting Category/Concept edges) ...")
        for pair in edge_pairs:
            self.consolidate_matrix(
                type_pair       = EdgeTypePair(from_object_type=pair[0], to_object_type=pair[1]),
                params          = params,
                update_averages = True,
                actions         = actions,
            )

        # Report the completion of the combined update.
        sysmsg.success("🧮 ✅ Done updating scores matrices.\n")
